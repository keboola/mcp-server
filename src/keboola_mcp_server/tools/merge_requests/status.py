"""Derived merge-request status and the deterministic `next_step` guide.

Pure functions, no I/O. This is a port of the kbagent CLI's Layer 2 tables (`merge_request_service.py`)
so the two clients cannot disagree. Every derivation is *server-first*: it prefers the serialized field
when Connection ships it (`derivedState`, `mergeBlockers`, `allowedActions`, `viewer`; DMD-1988,
https://linear.app/keboola/issue/DMD-1988) and falls back to the local table below. Delete the fallbacks
when the backend serializes the fields.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from keboola_mcp_server.tools.merge_requests.models import (
    AllowedAction,
    ConflictRef,
    DerivedState,
    DerivedStatus,
    MergeBlocker,
    Viewer,
)

_DERIVED_STATE_BY_RAW: dict[str, DerivedState] = {
    'development': 'in_development',
    'in_review': 'in_review',
    'approved': 'approved',
    'in_merge': 'in_merge',
    'published': 'merged',
    'canceled': 'closed',
}

_ALLOWED_ACTIONS_BY_STATE: dict[str, tuple[AllowedAction, ...]] = {
    'development': ('request_review', 'merge', 'update', 'resolve_conflicts'),
    'in_review': ('approve', 'request_changes', 'update', 'resolve_conflicts'),
    'approved': ('request_changes', 'merge', 'update', 'resolve_conflicts'),
    'in_merge': ('update',),
    'published': (),
    'canceled': (),
}

_ALLOWED_ACTIONS: frozenset[str] = frozenset(
    {'request_review', 'approve', 'request_changes', 'merge', 'update', 'resolve_conflicts'}
)
_BLOCKERS: frozenset[str] = frozenset({'conflicts', 'approvals', 'state'})
_DERIVED_STATES: frozenset[str] = frozenset(_DERIVED_STATE_BY_RAW.values()) | {'rejected'}


def same_id(a: Any, b: Any) -> bool:
    """Compares two ids that may arrive as int or str (`approverId` is a string, `creator.id` an int)."""
    return a is not None and b is not None and str(a) == str(b)


def _creator_id(mr: Mapping[str, Any]) -> Any:
    return (mr.get('creator') or {}).get('id')


def derive_state(mr: Mapping[str, Any]) -> DerivedState:
    """
    The client-facing state: the UI list badge's decision table, evaluated in order.

    - `rejected`: `development` + a non-creator reviewer with `status=rejected`
    - `closed`: `canceled`, or `development` + the creator's own rejection (the UI "cancel" trick)
    - otherwise the raw state mapped through `_DERIVED_STATE_BY_RAW`

    `reviewers[].status` is populated only inside a review round anchored by a real `request_review` event,
    so with the non-SOX default of 0 approvals the first two rows never fire — the UI badge has the same
    blind spot, and DMD-1988 asks the backend to derive from the activity log instead.
    """
    server = mr.get('derivedState')
    if isinstance(server, str) and server in _DERIVED_STATES:
        return server  # type: ignore[return-value]

    state = mr.get('state') or ''
    creator_id = _creator_id(mr)
    non_creator_rejected = False
    creator_self_rejected = False
    for reviewer in mr.get('reviewers') or []:
        if reviewer.get('status') != 'rejected':
            continue
        if same_id(reviewer.get('id'), creator_id):
            creator_self_rejected = True
        else:
            non_creator_rejected = True

    if state == 'development' and non_creator_rejected:
        return 'rejected'
    if state == 'canceled' or (state == 'development' and creator_self_rejected):
        return 'closed'
    return _DERIVED_STATE_BY_RAW.get(state, 'in_development')


def derive_merge_blockers(mr: Mapping[str, Any], conflicts: Sequence[Any] | None) -> list[MergeBlocker]:
    """
    What currently blocks `merge`, as a list so concurrent blockers do not mask each other. Never `None`.

    - `conflicts`: the live conflicts list is non-empty; `conflicts=None` (not fetched) skips only this check
    - `approvals`: `state == in_review` (the state collapses the requirement; no count until DMD-1969)
    - `state`: `in_merge` / `published` / `canceled`
    """
    server = mr.get('mergeBlockers')
    if isinstance(server, list):
        return [b for b in server if b in _BLOCKERS]  # type: ignore[misc]

    state = mr.get('state') or ''
    blockers: list[MergeBlocker] = []
    if conflicts:
        blockers.append('conflicts')
    if state == 'in_review':
        blockers.append('approvals')
    if state in ('in_merge', 'published', 'canceled'):
        blockers.append('state')
    return blockers


def derive_allowed_actions(mr: Mapping[str, Any]) -> list[AllowedAction]:
    """Actions the state machine mechanically allows; state-only, an unknown state yields `[]`."""
    server = mr.get('allowedActions')
    if isinstance(server, list):
        return [a for a in server if a in _ALLOWED_ACTIONS]  # type: ignore[misc]
    return list(_ALLOWED_ACTIONS_BY_STATE.get(mr.get('state') or '', ()))


def _server_viewer(mr: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """The single predicate for "DMD-1988 landed" on the viewer block."""
    server = mr.get('viewer')
    if isinstance(server, Mapping) and ('isCreator' in server or 'hasApproved' in server):
        return server
    return None


def derive_viewer(mr: Mapping[str, Any], admin_id: Any) -> Viewer:
    """
    The caller's relation to the MR. `admin_id` comes from `verify_token`'s `admin.id`; when it is `None`
    (an OAuth or scoped token) both flags are `None` — an honest "unknown", never `False`.
    """
    server = _server_viewer(mr)
    if server is not None:
        return Viewer(is_creator=server.get('isCreator'), has_approved=server.get('hasApproved'))
    if admin_id is None:
        return Viewer(is_creator=None, has_approved=None)
    return Viewer(
        is_creator=same_id(_creator_id(mr), admin_id),
        has_approved=any(same_id(a.get('approverId'), admin_id) for a in mr.get('approvals') or []),
    )


def approved_by(mr: Mapping[str, Any]) -> list[str]:
    return [str(a.get('approverName') or a.get('approverId') or '') for a in mr.get('approvals') or []]


def pending_reviewers(mr: Mapping[str, Any]) -> list[str]:
    return [str(r.get('name') or r.get('id') or '') for r in mr.get('reviewers') or [] if r.get('status') != 'approved']


def request_changes_reason(mr: Mapping[str, Any]) -> str | None:
    """The note of the latest `changes_requested` activity event, when the activity log was fetched."""
    log = mr.get('activityLog')
    if not isinstance(log, list):
        return None
    for event in reversed(log):
        if isinstance(event, Mapping) and event.get('eventType') == 'changes_requested':
            note = event.get('note')
            return str(note) if note else None
    return None


@dataclass(frozen=True)
class SessionContext:
    """Session facts `next_step` needs: is this a session on the MR's source branch, and may the caller write."""

    on_mr_branch: bool
    can_write: bool


LastRefusal = Literal['conflicts', 'not_ready']


def build_next_step(
    *,
    state: str,
    derived_state: str,
    merge_blockers: Sequence[str],
    conflicts: Sequence[ConflictRef] | None,
    allowed_actions: Sequence[str],
    viewer: Viewer,
    pending: Sequence[str],
    session: SessionContext,
    branch_from_name: str | None,
    remaining_conflicts: Sequence[ConflictRef] | None = None,
    last_refusal: LastRefusal | None = None,
    rejection_reason: str | None = None,
) -> str:
    """The 15-row decision table (RFC "next_step — the deterministic guide"); the first matching row wins."""
    branch = f"branch '{branch_from_name}'" if branch_from_name else "the merge request's source branch"

    if derived_state == 'merged':  # 1
        return (
            'Done: the changes are in production and the source branch is being deleted. '
            'Open a session on the production branch to continue.'
        )
    if derived_state == 'closed':  # 2
        return 'Nothing to do: the merge request is closed.'
    if derived_state == 'in_merge':  # 3
        return (
            'A merge is running. Do not edit this branch until it finishes (edits made now are lost when the '
            'branch is deleted) and do not start a second merge.'
        )
    if remaining_conflicts:  # 4
        first = remaining_conflicts[0]
        return f'Resolve the next conflict: {first.label} ({len(remaining_conflicts)} left).'
    if 'conflicts' in merge_blockers:
        n = len(conflicts) if conflicts is not None else 0
        noun = 'conflict' if n == 1 else 'conflicts'
        if not session.on_mr_branch:  # 5
            return f'Conflicts must be resolved from a session on {branch}; open one and call get_merge_request_conflicts there.'
        return (  # 6
            f'{n} {noun} block the merge; call get_merge_request_conflicts and resolve them one by one '
            'with resolve_merge_request_conflict.'
        )
    if 'approvals' in merge_blockers:
        names = ', '.join(pending) if pending else 'the requested reviewers'
        if viewer.has_approved is True:  # 7
            return f'You already approved; wait for the other reviewers ({names}).'
        if viewer.is_creator is True:  # 8
            return f'Waiting for approval from {names}; you cannot approve your own merge request.'
        if session.can_write:  # 9
            return 'Approve it (approve_merge_request) or request changes (request_merge_request_changes).'
        return "Waiting for a reviewer's approval."  # 10
    if last_refusal == 'not_ready' and state == 'development':  # 11
        return 'This project requires approvals before a merge; request a review (request_merge_request_review).'
    if derived_state == 'rejected':  # 12
        reason = f' ({rejection_reason})' if rejection_reason else ''
        return f'Changes were requested{reason}; address them on the branch, then merge again.'
    if 'merge' in allowed_actions:
        if not session.on_mr_branch:  # 13
            return f'Ready to merge; open a session on {branch} and call merge_merge_request there.'
        if session.can_write:  # 14
            return 'Ready: merge it with merge_merge_request (irreversible; the source branch is deleted afterwards).'
    return 'Show the merge request (read-only).'  # 15


def build_status(
    mr: Mapping[str, Any],
    *,
    conflicts: Sequence[ConflictRef] | None,
    admin_id: Any,
    session: SessionContext,
    branch_from_name: str | None,
    remaining_conflicts: Sequence[ConflictRef] | None = None,
    last_refusal: LastRefusal | None = None,
) -> DerivedStatus:
    """Assembles the derived status of a raw MR payload plus the (optionally fetched) live conflicts."""
    state = mr.get('state') or 'development'
    derived_state = derive_state(mr)
    blockers = derive_merge_blockers(mr, conflicts)
    allowed = derive_allowed_actions(mr)
    viewer = derive_viewer(mr, admin_id)
    pending = pending_reviewers(mr)
    mergeable: bool | None
    if conflicts is None:
        mergeable = None
    else:
        mergeable = not blockers
    return DerivedStatus(
        state=state,
        derived_state=derived_state,
        merge_blockers=blockers,
        mergeable=mergeable,
        allowed_actions=allowed,
        viewer=viewer,
        approved_by=approved_by(mr),
        pending_reviewers=pending,
        conflicts=list(conflicts) if conflicts is not None else None,
        next_step=build_next_step(
            state=state,
            derived_state=derived_state,
            merge_blockers=blockers,
            conflicts=conflicts,
            allowed_actions=allowed,
            viewer=viewer,
            pending=pending,
            session=session,
            branch_from_name=branch_from_name,
            remaining_conflicts=remaining_conflicts,
            last_refusal=last_refusal,
            rejection_reason=request_changes_reason(mr),
        ),
    )
