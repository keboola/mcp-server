"""Result models of the merge-request tools.

`MergeRequest`, `Reviewer`, `ActivityEvent`, `ConflictRef` and `ConfigVersionSnapshot` map 1:1 onto the
Connection responses (`MergeRequestResponse`, the `/conflicts` action, `ConfigurationDiffResponse`).
`DerivedStatus`, `PathChange` and `suggested_take` have no backend counterpart today; they are computed
by this server exactly as the kbagent CLI computes them (see `status.py` / `conflicts.py`) and will be
read server-first once Connection serializes them (DMD-1988).
"""

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, Field, StrictBool, field_validator

from keboola_mcp_server.links import Link

MergeRequestState = Literal['development', 'in_review', 'approved', 'in_merge', 'published', 'canceled']
DerivedState = Literal['rejected', 'closed', 'in_development', 'in_review', 'approved', 'in_merge', 'merged']
MergeRequestStateFilter = Literal[
    'development',
    'in_review',
    'approved',
    'in_merge',
    'published',
    'canceled',
    'rejected',
    'closed',
    'in_development',
    'merged',
]
MergeBlocker = Literal['conflicts', 'approvals', 'state']
AllowedAction = Literal['request_review', 'approve', 'request_changes', 'merge', 'update', 'resolve_conflicts']
AutoMergeStrategy = Literal['none', 'immediately', 'scheduled']
ReviewerStatus = Literal['approved', 'rejected', 'pending']
TakeMode = Literal['ours', 'theirs', 'delete']
ChangedBy = Literal['ours', 'theirs', 'both']
MergeRefusal = Literal['conflicts', 'not_ready']
ResolutionMode = Literal['ours', 'theirs', 'delete', 'custom']


def _opt_str(value: Any) -> str | None:
    """Normalizes the API's `''` to `None`."""
    if value is None:
        return None
    text = str(value)
    return text if text.strip() else None


class Reviewer(BaseModel):
    id: int = Field(description="The reviewer's Keboola user id.")
    name: str = Field(description="The reviewer's name.")
    status: ReviewerStatus = Field(
        description="The reviewer's decision in the current review round; 'pending' when none."
    )

    @classmethod
    def from_api(cls, raw: Mapping[str, Any]) -> 'Reviewer':
        return cls(id=int(raw['id']), name=str(raw.get('name') or ''), status=raw.get('status') or 'pending')


class Viewer(BaseModel):
    """The caller's relation to the merge request."""

    is_creator: bool | None = Field(
        description='True when the caller created the MR; None when the identity is unknown.'
    )
    has_approved: bool | None = Field(
        description='True when the caller already approved the MR; None when the identity is unknown.'
    )


class ConflictRef(BaseModel):
    """One conflicting configuration, exactly as `GET /merge-request/{id}/conflicts` (and the merge 409) reports it."""

    component_id: str = Field(description='The component id of the conflicting configuration.')
    configuration_id: str = Field(description='The configuration id.')
    message: str = Field(description="The backend's description of the conflict.")
    is_deleted: bool = Field(description='True when the configuration is deleted on the development branch.')
    dev_branch_version_identifier: str = Field(
        description='The version identifier the development branch was created from (evidence only).'
    )
    default_branch_version_identifier: str = Field(
        description="The default branch's current version identifier (evidence only)."
    )

    @classmethod
    def from_api(cls, raw: Mapping[str, Any]) -> 'ConflictRef':
        return cls(
            component_id=str(raw.get('componentId') or ''),
            configuration_id=str(raw.get('configurationId') or ''),
            message=str(raw.get('message') or ''),
            is_deleted=bool(raw.get('isDeleted', False)),
            dev_branch_version_identifier=str(raw.get('devBranchVersionIdentifier') or ''),
            default_branch_version_identifier=str(raw.get('defaultBranchVersionIdentifier') or ''),
        )

    @property
    def label(self) -> str:
        return f'{self.component_id}/{self.configuration_id}'


class DerivedStatus(BaseModel):
    """What the merge request is waiting on and what to do next. Server-first polyfill for DMD-1988."""

    state: MergeRequestState = Field(description='The raw lifecycle state reported by the API.')
    derived_state: DerivedState = Field(description='The client-facing state (what the Keboola UI badge shows).')
    merge_blockers: list[MergeBlocker] = Field(
        description="Everything currently blocking a merge; informational, the backend's merge check is the authority."
    )
    mergeable: bool | None = Field(
        description='True when nothing blocks the merge and conflicts were checked; None when conflicts were not fetched.'
    )
    allowed_actions: list[AllowedAction] = Field(description='Actions the current state permits (state-only).')
    viewer: Viewer = Field(description="The caller's relation to the merge request.")
    approved_by: list[str] = Field(description='Names of the users who approved.')
    pending_reviewers: list[str] = Field(description='Names of the requested reviewers who have not approved yet.')
    conflicts: list[ConflictRef] | None = Field(
        description='The live list of conflicting configurations when fetched; None when not fetched.'
    )
    next_step: str = Field(description='The single recommended next action, in one sentence.')


class ChangedConfig(BaseModel):
    component_id: str = Field(description='The component id.')
    configuration_id: str = Field(description='The configuration id.')
    is_deleted: bool = Field(description='True when the merge request deletes this configuration.')

    @classmethod
    def list_from_change_log(cls, change_log: Any) -> list['ChangedConfig']:
        """Parses `changeLog.configurations` defensively: a missing or malformed key yields `[]`."""
        if not isinstance(change_log, Mapping):
            return []
        configurations = change_log.get('configurations')
        if not isinstance(configurations, list):
            return []
        result: list[ChangedConfig] = []
        for item in configurations:
            if not isinstance(item, Mapping):
                continue
            result.append(
                cls(
                    component_id=str(item.get('componentId') or ''),
                    configuration_id=str(item.get('configurationId') or ''),
                    is_deleted=bool(item.get('isDeleted', False)),
                )
            )
        return result


class ActivityEvent(BaseModel):
    event_type: str = Field(description="The event type, e.g. 'review_requested', 'approved', 'changes_requested'.")
    admin_name: str | None = Field(description='Who did it; None for system events (e.g. auto-merge).')
    note: str | None = Field(description='The note attached to the event, e.g. the reason for requesting changes.')
    created_at: str = Field(description='When the event happened.')

    @classmethod
    def from_api(cls, raw: Mapping[str, Any]) -> 'ActivityEvent':
        admin = raw.get('admin')
        return cls(
            event_type=str(raw.get('eventType') or ''),
            admin_name=str(admin['name']) if isinstance(admin, Mapping) and admin.get('name') else None,
            note=_opt_str(raw.get('note')),
            created_at=str(raw.get('createdAt') or ''),
        )


class MergeRequest(BaseModel):
    """A merge request summary (list rows)."""

    id: int = Field(description='The merge request id.')
    title: str = Field(description='The title.')
    description: str | None = Field(description='The description.')
    state: MergeRequestState = Field(description='The raw lifecycle state reported by the API.')
    derived_state: DerivedState = Field(description='The client-facing state (what the Keboola UI badge shows).')
    branch_from_id: int | None = Field(description='The source development branch id; None once the branch is deleted.')
    branch_from_name: str | None = Field(description='The source development branch name.')
    branch_into_name: str = Field(description='The target branch name (the production/default branch).')
    creator_name: str = Field(description='Who created the merge request.')
    reviewers: list[Reviewer] = Field(description='The requested reviewers and their decisions.')
    auto_merge: AutoMergeStrategy = Field(
        description="Auto-merge strategy: 'none' (off), 'immediately' (merge once approved), 'scheduled' (at auto_merge_at)."
    )
    auto_merge_at: str | None = Field(description="When a 'scheduled' auto-merge runs.")
    created_at: str = Field(description='When the merge request was created.')
    merged_at: str | None = Field(description='When it was merged; None until then.')
    merged_by: str | None = Field(description='Who merged it (the system for auto-merges); None until merged.')
    links: list[Link] = Field(description='Keboola UI links.')

    @classmethod
    def from_api(cls, raw: Mapping[str, Any], *, branch_names: Mapping[str, str], links: list[Link]) -> 'MergeRequest':
        from keboola_mcp_server.tools.merge_requests.status import derive_state

        branches = raw.get('branches') or {}
        branch_from_id = branches.get('branchFromId')
        branch_into_id = branches.get('branchIntoId')
        merge = raw.get('merge') or {}
        creator = raw.get('creator') or {}
        return cls(
            id=int(raw['id']),
            title=str(raw.get('title') or ''),
            description=_opt_str(raw.get('description')),
            state=raw.get('state'),
            derived_state=derive_state(raw),
            branch_from_id=int(branch_from_id) if branch_from_id is not None else None,
            branch_from_name=branch_names.get(str(branch_from_id)) if branch_from_id is not None else None,
            branch_into_name=branch_names.get(str(branch_into_id), 'production')
            if branch_into_id is not None
            else 'production',
            creator_name=str(creator.get('name') or ''),
            reviewers=[Reviewer.from_api(r) for r in raw.get('reviewers') or []],
            auto_merge=raw.get('autoMergeStrategy') or 'none',
            auto_merge_at=_opt_str(raw.get('autoMergeAt')),
            created_at=str(raw.get('createdAt') or ''),
            merged_at=_opt_str(merge.get('mergedAt')),
            merged_by=_opt_str(merge.get('mergerName')),
            links=links,
        )


class MergeRequestDetail(MergeRequest):
    status: DerivedStatus = Field(description='What the merge request is waiting on and what to do next.')
    changed_configurations: list[ChangedConfig] = Field(
        description='Configurations the merge request changes; empty until a review is requested or the MR is merged.'
    )
    activity_log: list[ActivityEvent] | None = Field(
        description='The review history (oldest first); None when the tool did not fetch it.'
    )


class MergeRequestsListOutput(BaseModel):
    merge_requests: list[MergeRequest] = Field(description='The merge requests of the project.')
    links: list[Link] = Field(description='Keboola UI links.')


class MergeRequestsDetailOutput(BaseModel):
    merge_requests: list[MergeRequestDetail] = Field(description='The requested merge requests with their status.')


class ConfigVersionSnapshot(BaseModel):
    """One side of a three-way configuration diff (`ConfigurationVersionResponse` with its `diff` flattened in)."""

    version: int = Field(description='The configuration version on that branch.')
    is_deleted: bool = Field(description='True when the configuration is deleted in this version.')
    name: str | None = Field(description='The configuration name.')
    description: str | None = Field(description='The configuration description.')
    change_description: str | None = Field(description="This version's change description.")
    is_disabled: bool = Field(description='Whether the configuration is disabled.')
    configuration: dict[str, Any] = Field(description='The configuration content.')
    rows: list[dict[str, Any]] = Field(description='The configuration rows (complete set).')

    @classmethod
    def from_api(cls, raw: Mapping[str, Any] | None) -> 'ConfigVersionSnapshot | None':
        if raw is None:
            return None
        diff = raw.get('diff') or {}
        return cls(
            version=int(raw.get('version') or 0),
            is_deleted=bool(raw.get('isDeleted', False)),
            name=diff.get('name'),
            description=diff.get('description'),
            change_description=diff.get('changeDescription'),
            is_disabled=bool(diff.get('isDisabled', False)),
            configuration=diff.get('configuration') if isinstance(diff.get('configuration'), dict) else {},
            rows=list(diff.get('rows') or []),
        )


class PathChange(BaseModel):
    """A change to one path of the configuration, classified by who made it."""

    path: str = Field(
        description="The changed path: '/name', '/description', '/isDisabled', or '/configuration/...', '/rows/...'."
    )
    changed_by: ChangedBy = Field(
        description="'ours' = development branch, 'theirs' = production, 'both' = both sides."
    )
    agreed: bool | None = Field(description="On 'both' rows: True when both sides made the same change.")
    base: Any | None = Field(description='The value the development branch started from.')
    ours: Any | None = Field(description='The value on the development branch.')
    theirs: Any | None = Field(description='The value on production.')


class ConfigConflict(ConflictRef):
    """One conflicting configuration with everything needed to resolve it."""

    base: ConfigVersionSnapshot | None = Field(description='The version the development branch started from.')
    ours: ConfigVersionSnapshot | None = Field(description="The development branch's current version.")
    theirs: ConfigVersionSnapshot | None = Field(
        description="Production's current version (a rebase re-anchors onto it)."
    )
    ours_deleted: bool | None = Field(description='True when the development branch deleted it; None when absent.')
    theirs_deleted: bool | None = Field(description='True when production deleted it; None when absent.')
    changes: list[PathChange] = Field(
        description='Per-path changes of both sides; empty when either side is deleted or absent.'
    )
    conflicting_paths: list[str] = Field(description='Paths both sides changed differently: the actual conflict.')
    suggested_take: TakeMode | None = Field(
        description="'ours'/'theirs' when only that side changed anything; None when the user has to decide."
    )


class MergeRequestConflictsOutput(BaseModel):
    merge_request_id: int = Field(description='The merge request id.')
    conflicts: list[ConfigConflict] = Field(description='The conflicting configurations; empty when nothing blocks.')
    status: DerivedStatus = Field(description='The merge request status.')
    next_step: str = Field(description='The single recommended next action.')


class MergeResult(BaseModel):
    merge_request_id: int = Field(description='The merge request id.')
    merged: bool = Field(description='True when the merge finished successfully.')
    state: MergeRequestState = Field(description='The merge request state after the call.')
    job_id: str | None = Field(description='The Storage job that performed the merge.')
    refusal: MergeRefusal | None = Field(
        description="Why the backend refused: 'conflicts' or 'not_ready' (lock, wrong state, missing approvals)."
    )
    refusal_message: str | None = Field(description="The backend's message.")
    conflicts: list[ConflictRef] | None = Field(description='The conflicting configurations on a conflict refusal.')
    status: DerivedStatus | None = Field(description='The merge request status when the merge did not happen.')
    source_branch_deleting: bool = Field(
        description='True after a successful merge: the source branch is being deleted by a background job.'
    )
    warnings: list[str] = Field(description='Soft failures that did not prevent the merge.')
    next_step: str = Field(description='The single recommended next action.')


class ResolvedConfiguration(BaseModel):
    """The manually merged configuration content. Every key is required because a rebase REPLACES the version."""

    name: str = Field(description='The configuration name (non-empty).')
    description: str | None = Field(
        description='The configuration description (null to clear, but the key is required).'
    )
    is_disabled: StrictBool = Field(description='Whether the configuration is disabled (a JSON boolean).')
    configuration: dict[str, Any] = Field(description='The complete configuration content.')
    rows: list[dict[str, Any]] = Field(description='The complete row set; an empty list deletes all rows.')

    @field_validator('name')
    @classmethod
    def _name_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError('name must be a non-empty string')
        return value


class ResolveConflictResult(BaseModel):
    component_id: str = Field(description='The component id.')
    configuration_id: str = Field(description='The configuration id.')
    resolved: bool = Field(description='True when the configuration was rebased.')
    mode: ResolutionMode = Field(description="How it was resolved: 'ours', 'theirs', 'delete' or 'custom'.")
    rebased_onto_version: int = Field(description='The production version the configuration is now anchored onto.')
    remaining_conflicts: list[ConflictRef] = Field(description='Conflicts still blocking the merge; empty = mergeable.')
    status: DerivedStatus = Field(description='The merge request status.')
    warnings: list[str] = Field(description='Soft issues, e.g. an ignored change description on delete.')
    next_step: str = Field(description='The single recommended next action.')
