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
TERMINAL_STATES: frozenset[str] = frozenset({'published', 'canceled'})
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
    # An unrecognized raw state is reported as such, never as a state the agent may act on.
    return _DERIVED_STATE_BY_RAW.get(state, 'unknown')


def derive_merge_blockers(mr: Mapping[str, Any], conflicts: Sequence[Any] | None) -> list[MergeBlocker]:
    """
    What currently blocks `merge`, as a list so concurrent blockers do not mask each other. Never `None`.

    - `conflicts`: the live conflicts list is non-empty; `conflicts=None` (not fetched) skips only this check
    - `approvals`: `state == in_review` (the state collapses the requirement; no count until DMD-1969)
    - `state`: `in_merge` / `published` / `canceled`

    Server-first (`mergeBlockers`, DMD-1988): the backend's list is kept verbatim (unknown kinds included, so an
    unknown blocker never reads as "mergeable"), and the live conflicts the caller just fetched are unioned in.
    """
    server = mr.get('mergeBlockers')
    if isinstance(server, list):
        blockers = [str(b) for b in server if isinstance(b, str) and b]
        if conflicts and 'conflicts' not in blockers:
            blockers.append('conflicts')
        return blockers

    state = mr.get('state') or ''
    blockers = []
    if conflicts:
        blockers.append('conflicts')
    if state == 'in_review':
        blockers.append('approvals')
    if state in ('in_merge', 'published', 'canceled') or state not in _DERIVED_STATE_BY_RAW:
        # Unknown states block too: when unsure, the answer is "no", never "mergeable".
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
    """
    Session facts `next_step` needs: is this a session on the MR's source branch, is it a production session (the
    merge / conflicts / resolve tools work there with an explicit `merge_request_id`), and may the caller write.
    Neither branch flag set means a session on another development branch, which can only hand the user off.
    """

    on_mr_branch: bool
    can_write: bool
    on_production: bool = False


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
    refusal_message: str | None = None,
    rejection_reason: str | None = None,
    merge_request_id: int | None = None,
    branch_url: str | None = None,
    project_url: str | None = None,
) -> str:
    """
    The 15-row decision table (RFC "next_step — the deterministic guide"); the first matching row wins.

    Handoff rows (1, 5, 13) are written for Kai, where a chat is pinned to one branch and kbc-ui blocks the chat
    in a branch whose merge request is `in_review` / `approved`: from a production session they say "do it from
    here with merge_request_id=N"; from another branch they name the MR's branch with its UI link and offer the
    production route. The URLs are in the text on purpose (kai-agent's prompt otherwise hides branch details).
    """
    branch = f"branch '{branch_from_name}'" if branch_from_name else "the merge request's source branch"
    link = f' ({branch_url})' if branch_url else ''
    mr_arg = f'merge_request_id={merge_request_id}' if merge_request_id is not None else 'the merge request id'

    def ui_read_only_note() -> str:
        """Item K1: a reviewed/approved MR makes its branch read-only in kbc-ui, which switches Kai off there."""
        if not session.on_mr_branch or merge_request_id is None:
            return ''
        if last_refusal == 'conflicts':
            why = 'the refused merge left the merge request approved'
        elif state in ('in_review', 'approved'):
            why = f'the merge request is {state}'
        else:
            return ''
        return (
            f' Note: {why}, which makes this branch read-only in the Keboola UI; if Kai becomes unavailable on it, '
            f'continue from a production chat with {mr_arg} (get_merge_request_conflicts, '
            'resolve_merge_request_conflict, merge_merge_request work there).'
        )

    if derived_state == 'unknown':  # 0: an unrecognized raw state; never recommend an action on it
        return (
            f"The merge request is in a state this server does not recognize ('{state}'); do not act on it from "
            'here, check it in the Keboola UI.'
        )
    if derived_state == 'merged':  # 1
        if session.on_mr_branch:
            where = f' ({project_url})' if project_url else ''
            return (
                f'Done: the changes are in production and {branch} is being deleted. This chat is pinned to that '
                'branch: it can still answer questions about production but cannot make changes. Tell the user to '
                f'continue in production{where}; include the link.'
            )
        return f'Done: the changes are in production and {branch} is being deleted; nothing more to do on it.'
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
    if 'conflicts' in merge_blockers or last_refusal == 'conflicts':  # 5/6
        if 'conflicts' not in merge_blockers:  # a conflict refusal without a usable conflict list
            lead = 'The merge was refused because of conflicts'
        elif conflicts is None:  # count unknown (the blocker came from the backend, the list was not fetched)
            lead = 'Conflicts block the merge'
        else:
            n = len(conflicts)
            lead = f'{n} {"conflict" if n == 1 else "conflicts"} block the merge'
        if session.on_mr_branch:  # 6
            return (
                f'{lead}; call get_merge_request_conflicts and resolve them one by one with '
                f'resolve_merge_request_conflict.{ui_read_only_note()}'
            )
        if session.on_production:  # 5, from production: the tools act on the MR's branch by id
            return (
                f'{lead}; call get_merge_request_conflicts({mr_arg}) and resolve them one by one with '
                f'resolve_merge_request_conflict (the resolutions are written to {branch}).'
            )
        return (  # 5, from another branch: hand off
            f'{lead}; they must be handled from a chat on {branch}{link} or from a production chat, calling '
            f'get_merge_request_conflicts({mr_arg}) there. Tell the user, including the link.'
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
    if last_refusal == 'not_ready':  # 11
        backend = f' Backend: {refusal_message}' if refusal_message else ''
        if state == 'development':
            return (
                'This project requires approvals before a merge; request a review (request_merge_request_review).'
                + backend
            )
        return (
            'The merge was refused as not ready (a merge lock is held or another merge request is being processed); '
            'wait for it to clear, then merge again.' + backend
        )
    if derived_state == 'rejected':  # 12
        reason = f' ({rejection_reason})' if rejection_reason else ''
        return f'Changes were requested{reason}; address them on the branch, then merge again.'
    if 'merge' in allowed_actions:
        if session.on_mr_branch:
            if session.can_write:  # 14
                return (
                    'Ready: merge it with merge_merge_request (irreversible; the source branch is deleted '
                    f'afterwards).{ui_read_only_note()}'
                )
        elif session.on_production:  # 13, from production: merge by id
            if session.can_write:
                return (
                    f'Ready to merge; merge it from here with merge_merge_request({mr_arg}) (irreversible; {branch} '
                    'is deleted afterwards).'
                )
        else:  # 13, from another branch: hand off
            return (
                f'Ready to merge; tell the user to open Kai on {branch}{link} or in production and merge it there '
                f'with merge_merge_request({mr_arg}). Include the link.'
            )
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
    refusal_message: str | None = None,
    branch_url: str | None = None,
    project_url: str | None = None,
) -> DerivedStatus:
    """Assembles the derived status of a raw MR payload plus the (optionally fetched) live conflicts."""
    state = mr.get('state') or 'development'
    derived_state = derive_state(mr)
    blockers = derive_merge_blockers(mr, conflicts)
    allowed = derive_allowed_actions(mr)
    viewer = derive_viewer(mr, admin_id)
    pending = pending_reviewers(mr)
    mergeable: bool | None
    if last_refusal is not None:
        mergeable = False  # the backend just refused the merge; that beats any local derivation
    elif conflicts is None:
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
            refusal_message=refusal_message,
            rejection_reason=request_changes_reason(mr),
            merge_request_id=int(mr['id']) if mr.get('id') is not None else None,
            branch_url=branch_url,
            project_url=project_url,
        ),
    )
