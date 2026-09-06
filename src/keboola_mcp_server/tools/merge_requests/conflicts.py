"""Per-path classification of a conflicting configuration's three-way diff.

Pure functions, a port of the kbagent CLI's `_classify_three_way` / `_envelope_holes` / `_diff_warnings`
(`merge_request_service.py`). A side that is absent, `isDeleted` or key-incomplete produces no path rows at
all (`changes=[]`) — fabricating rows against an empty stand-in would contradict the side flags.
"""

from collections.abc import Mapping
from typing import Any

from keboola_mcp_server.tools.merge_requests.models import (
    ConfigConflict,
    ConfigVersionSnapshot,
    ConflictRef,
    PathChange,
    TakeMode,
)

# Content-bearing keys of a diff side's `diff` envelope. `changeDescription` is excluded: it is a per-version
# commit message, not content to resolve.
DIFF_CONTENT_KEYS: tuple[str, ...] = ('name', 'description', 'configuration', 'isDisabled', 'rows')
# The keys the backend's rebase requires (`description` is genuinely nullable and may be absent on the wire).
REQUIRED_CONTENT_KEYS: tuple[str, ...] = ('name', 'rows', 'configuration', 'isDisabled')

_ABSENT = object()


def envelope_holes(side: Mapping[str, Any]) -> list[str]:
    """Required content keys absent from a (non-null, non-deleted) side's envelope."""
    envelope = side.get('diff') or {}
    return [key for key in REQUIRED_CONTENT_KEYS if key not in envelope]


def diff_warnings(diff: Mapping[str, Any]) -> list[str]:
    """Explains why a side yielded no classification: an empty or holed envelope is a backend contract violation."""
    warnings: list[str] = []
    for label in ('ours', 'theirs'):
        side = diff.get(label)
        if side is None or side.get('isDeleted'):
            continue
        holes = envelope_holes(side)
        if not holes:
            continue
        what = (
            'has an empty content envelope'
            if holes == list(REQUIRED_CONTENT_KEYS)
            else f'carries no {", ".join(holes)}'
        )
        warnings.append(
            f"The diff's {label} side {what}; no per-path classification is possible (backend envelope hole)."
        )
    return warnings


def _classifiable(side: Mapping[str, Any] | None) -> bool:
    return side is not None and not side.get('isDeleted') and not envelope_holes(side)


def _content(side: Mapping[str, Any] | None) -> dict[str, Any]:
    envelope = (side or {}).get('diff') or {}
    return {key: envelope[key] for key in DIFF_CONTENT_KEYS if key in envelope}


def _ids(items: list[Any]) -> dict[str, Any] | None:
    """`{id: item}` when every item is a dict with a unique `id` (configuration rows), else None."""
    if not items or not all(isinstance(item, dict) and item.get('id') is not None for item in items):
        return None
    by_id = {str(item['id']): item for item in items}
    return by_id if len(by_id) == len(items) else None


def _walk(old: Any, new: Any, path: str, out: dict[str, tuple[Any, Any]]) -> None:
    """Records changed leaf paths (JSON pointers) between two JSON values into `out` as `(old, new)`
    where a missing side is `_ABSENT`. Dicts and lists recurse; anything else compares by value."""
    if isinstance(old, dict) and isinstance(new, dict):
        for key in sorted(set(old) | set(new)):
            sub = f'{path}/{key}'
            if key in old and key in new:
                _walk(old[key], new[key], sub, out)
            elif key in old:
                out[sub] = (old[key], _ABSENT)
            else:
                out[sub] = (_ABSENT, new[key])
        return
    if isinstance(old, list) and isinstance(new, list):
        if _ids(old) is not None and _ids(new) is not None:
            # Rows carry ids: align by id so an insertion does not shift every following row into a "change".
            old_by_id, new_by_id = _ids(old), _ids(new)
            assert old_by_id is not None and new_by_id is not None
            for key in sorted(set(old_by_id) | set(new_by_id)):
                sub = f'{path}/{key}'
                if key in old_by_id and key in new_by_id:
                    _walk(old_by_id[key], new_by_id[key], sub, out)
                elif key in old_by_id:
                    out[sub] = (old_by_id[key], _ABSENT)
                else:
                    out[sub] = (_ABSENT, new_by_id[key])
            return
        for i in range(max(len(old), len(new))):
            sub = f'{path}/{i}'
            if i < len(old) and i < len(new):
                _walk(old[i], new[i], sub, out)
            elif i < len(old):
                out[sub] = (old[i], _ABSENT)
            else:
                out[sub] = (_ABSENT, new[i])
        return
    if old != new:
        out[path] = (old, new)


def classify_three_way(diff: Mapping[str, Any]) -> list[PathChange]:
    """
    Intersects the two pairwise diffs (base→ours, base→theirs) per JSON-pointer path. Covers all replaced
    keys: `/name`, `/description`, `/isDisabled` as top-level pseudo-paths, `/configuration/...` and
    `/rows/...` structurally. Returns `[]` when either side is not classifiable.
    """
    if not _classifiable(diff.get('ours')) or not _classifiable(diff.get('theirs')):
        return []

    base = _content(diff.get('base'))
    ours: dict[str, tuple[Any, Any]] = {}
    theirs: dict[str, tuple[Any, Any]] = {}
    _walk(base, _content(diff.get('ours')), '', ours)
    _walk(base, _content(diff.get('theirs')), '', theirs)

    def present(value: Any) -> Any:
        return None if value is _ABSENT else value

    changes: list[PathChange] = []
    for path in sorted(set(ours) | set(theirs)):
        o = ours.get(path)
        t = theirs.get(path)
        reference = o or t
        assert reference is not None
        base_value = present(reference[0])
        ours_value = present(o[1]) if o is not None else base_value
        theirs_value = present(t[1]) if t is not None else base_value
        if o is not None and t is not None:
            changed_by = 'both'
            agreed: bool | None = (o[1] is _ABSENT) == (t[1] is _ABSENT) and ours_value == theirs_value
        else:
            changed_by = 'ours' if o is not None else 'theirs'
            agreed = None
        changes.append(
            PathChange(
                path=path, changed_by=changed_by, agreed=agreed, base=base_value, ours=ours_value, theirs=theirs_value
            )
        )
    return changes


def conflicting_paths(changes: list[PathChange]) -> list[str]:
    """
    Paths the two sides changed differently: `both` rows that are not agreed, plus one-sided rows where one side
    changed a subtree and the other side a path inside it (an ancestor/descendant collision is a conflict too).
    """
    result = {c.path for c in changes if c.changed_by == 'both' and not c.agreed}
    ours = [c.path for c in changes if c.changed_by == 'ours']
    theirs = [c.path for c in changes if c.changed_by == 'theirs']
    for a in ours:
        for b in theirs:
            if a.startswith(b + '/') or b.startswith(a + '/'):
                result.update((a, b))
    return sorted(result)


def suggest_take(changes: list[PathChange]) -> TakeMode | None:
    """`ours`/`theirs` when only that side changed anything; None when paths collide or `changes` is empty."""
    if not changes:
        return None
    sides = {c.changed_by for c in changes}
    if 'both' in sides:
        return None
    if sides == {'ours'}:
        return 'ours'
    if sides == {'theirs'}:
        return 'theirs'
    return None


def build_config_conflict(ref: ConflictRef, diff: Mapping[str, Any]) -> ConfigConflict:
    """Combines a conflict reference with its three-way diff into everything needed to resolve it."""
    ours_raw = diff.get('ours')
    theirs_raw = diff.get('theirs')
    changes = classify_three_way(diff)
    return ConfigConflict(
        **ref.model_dump(),
        base=ConfigVersionSnapshot.from_api(diff.get('base')),
        ours=ConfigVersionSnapshot.from_api(ours_raw),
        theirs=ConfigVersionSnapshot.from_api(theirs_raw),
        ours_deleted=bool(ours_raw.get('isDeleted', False)) if ours_raw is not None else None,
        theirs_deleted=bool(theirs_raw.get('isDeleted', False)) if theirs_raw is not None else None,
        changes=changes,
        conflicting_paths=conflicting_paths(changes),
        suggested_take=suggest_take(changes),
    )
