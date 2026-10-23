from typing import Any
from unittest.mock import AsyncMock, call

import httpx
import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError

from keboola_mcp_server.clients.client import BranchNotFoundError, KeboolaClient
from keboola_mcp_server.mcp import TOKEN_INFO_VAR
from keboola_mcp_server.tools.merge_requests import tools as mr_tools
from keboola_mcp_server.tools.merge_requests.models import (
    MergeRequestsDetailOutput,
    MergeRequestsListOutput,
)
from keboola_mcp_server.tools.merge_requests.tools import (
    approve_merge_request,
    create_merge_request,
    get_merge_request_conflicts,
    get_merge_requests,
    merge_merge_request,
    request_merge_request_changes,
    request_merge_request_review,
    resolve_merge_request_conflict,
    update_merge_request,
)

DEV_BRANCH_ID = 55
DEFAULT_BRANCH_ID = 1
TOKEN_INFO = {
    'owner': {'id': 123, 'features': ['branches-merge-requests']},
    'admin': {'id': 10, 'name': 'Alice', 'role': 'admin'},
}
BRANCHES = [
    {'id': DEFAULT_BRANCH_ID, 'name': 'Main', 'isDefault': True},
    {'id': DEV_BRANCH_ID, 'name': 'reporting', 'isDefault': False},
    {'id': 77, 'name': 'other', 'isDefault': False},
]
MR_UI_URL = f'https://connection.test.keboola.com/admin/projects/123/branch/{DEV_BRANCH_ID}/development-overview'


def _mr_raw(
    state: str = 'development', *, mr_id: int = 42, branch_from: int | None = DEV_BRANCH_ID, **extra: Any
) -> dict:
    return {
        'id': mr_id,
        'creator': {'id': 10, 'name': 'Alice'},
        'title': 'Add reporting',
        'description': '',
        'state': state,
        'branches': {'branchFromId': branch_from, 'branchIntoId': DEFAULT_BRANCH_ID},
        'merge': {'mergedAt': None, 'mergerId': None, 'mergerName': None},
        'createdAt': '2026-09-07T10:00:00+0200',
        'externalId': None,
        'autoMergeStrategy': 'none',
        'autoMergeAt': None,
        'approvals': [],
        'reviewers': [],
        'changeLog': {'configurations': []},
        **extra,
    }


def _conflict_raw(cid: str = 'cfg-1', component: str = 'keboola.ex-db') -> dict:
    return {
        'componentId': component,
        'configurationId': cid,
        'message': 'Configuration changed in the default branch',
        'isDeleted': False,
        'devBranchVersionIdentifier': 'v-dev',
        'defaultBranchVersionIdentifier': 'v-prod',
    }


def _side(version: int, is_deleted: bool = False, **content: Any) -> dict:
    envelope: dict[str, Any] = {
        'name': 'My config',
        'description': None,
        'changeDescription': 'x',
        'isDisabled': False,
        'configuration': {'parameters': {'query': 'SELECT 1'}},
        'rows': [],
    }
    envelope.update(content)
    return {'version': version, 'isDeleted': is_deleted, 'diff': envelope}


def _diff(**overrides: Any) -> dict:
    diff = {
        'base': _side(1),
        'ours': _side(3, configuration={'parameters': {'query': 'SELECT 2'}}),
        'theirs': _side(9, configuration={'parameters': {'query': 'SELECT 3'}}),
    }
    diff.update(overrides)
    return diff


def _http_error(status: int, body: Any = None) -> httpx.HTTPStatusError:
    request = httpx.Request('PUT', 'https://connection.test.keboola.com/v2/storage/merge-request/42/merge')
    response = (
        httpx.Response(status, json=body, request=request)
        if body is not None
        else httpx.Response(status, request=request)
    )
    return httpx.HTTPStatusError(f'HTTP {status}', request=request, response=response)


@pytest.fixture
def storage(mcp_context_client: Context, keboola_client: KeboolaClient) -> AsyncMock:
    """The mocked storage client of a dev-branch session with the MR feature; tests override what they need."""
    keboola_client.branch_id = str(DEV_BRANCH_ID)
    sc = keboola_client.storage_client
    sc.verify_token.return_value = TOKEN_INFO
    sc.branches_list.return_value = BRANCHES
    sc.merge_requests_list.return_value = [_mr_raw()]
    sc.merge_request_detail.return_value = _mr_raw()
    sc.merge_request_conflicts.return_value = []
    return sc


def _n_calls(storage: AsyncMock) -> int:
    return sum(
        getattr(storage, name).await_count
        for name in (
            'verify_token',
            'branches_list',
            'merge_requests_list',
            'merge_request_detail',
            'merge_request_conflicts',
            'merge_request_create',
            'merge_request_update',
            'merge_request_request_review',
            'merge_request_approve',
            'merge_request_request_changes',
            'merge_request_merge',
            'configuration_diff',
            'configuration_rebase',
            'job_detail',
        )
    )


# ---- get_merge_requests -----------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('state', 'expected_ids'),
    [
        pytest.param(None, [42, 43, 44], id='no_filter'),
        pytest.param('development', [42, 44], id='raw_state'),
        pytest.param('in_development', [42], id='derived_state'),
        pytest.param('rejected', [44], id='derived_rejected_matches_development_mr'),
        pytest.param('merged', [43], id='derived_merged'),
        pytest.param('published', [43], id='raw_published'),
    ],
)
async def test_get_merge_requests_list(
    mcp_context_client: Context, storage: AsyncMock, state: str | None, expected_ids: list[int]
) -> None:
    storage.merge_requests_list.return_value = [
        _mr_raw('development'),
        _mr_raw(
            'published', mr_id=43, branch_from=None, merge={'mergedAt': 't', 'mergerId': 10, 'mergerName': 'Alice'}
        ),
        _mr_raw('development', mr_id=44, reviewers=[{'id': 20, 'name': 'Bob', 'email': 'b@x', 'status': 'rejected'}]),
    ]

    result = await get_merge_requests(mcp_context_client, state=state)

    assert isinstance(result, MergeRequestsListOutput)
    assert [m.id for m in result.merge_requests] == expected_ids
    first = result.merge_requests[0]
    if first.id == 42:
        assert (first.branch_from_name, first.branch_into_name, first.creator_name) == ('reporting', 'Main', 'Alice')
        assert first.description is None  # '' normalized
        assert [link.url for link in first.links] == [MR_UI_URL]
    if 43 in expected_ids:
        merged = next(m for m in result.merge_requests if m.id == 43)
        assert (merged.derived_state, merged.merged_by, merged.branch_from_id, merged.links) == (
            'merged',
            'Alice',
            None,
            [],
        )
    # list = 3 requests: GET /merge-request + branches_list + verify_token; no detail/conflicts fetched
    assert _n_calls(storage) == 3
    storage.merge_request_detail.assert_not_called()
    storage.merge_request_conflicts.assert_not_called()


@pytest.mark.asyncio
async def test_get_merge_requests_detail(mcp_context_client: Context, storage: AsyncMock) -> None:
    storage.merge_request_detail.side_effect = [
        _mr_raw(
            'in_review',
            reviewers=[{'id': 20, 'name': 'Bob', 'email': 'b@x', 'status': None}],
            changeLog={
                'configurations': [{'componentId': 'keboola.ex-db', 'configurationId': 'cfg-1', 'isDeleted': False}]
            },
            activityLog=[
                {
                    'id': 1,
                    'eventType': 'review_requested',
                    'admin': {'id': 10, 'name': 'Alice'},
                    'note': '',
                    'createdAt': 't1',
                },
                {'id': 2, 'eventType': 'auto_merge', 'admin': None, 'note': None, 'createdAt': 't2'},
            ],
        ),
        _mr_raw('approved', mr_id=43),
        _mr_raw(
            'published', mr_id=44, branch_from=None, merge={'mergedAt': 't', 'mergerId': 10, 'mergerName': 'Alice'}
        ),
    ]
    storage.merge_request_conflicts.side_effect = [[_conflict_raw()], []]

    result = await get_merge_requests(mcp_context_client, merge_request_ids=[42, 43, 44])

    assert isinstance(result, MergeRequestsDetailOutput)
    first, second, third = result.merge_requests
    assert first.reviewers[0].status == 'pending'  # null -> pending
    assert [c.configuration_id for c in first.changed_configurations] == ['cfg-1']
    assert first.activity_log is not None and [e.admin_name for e in first.activity_log] == ['Alice', None]
    assert first.activity_log[0].note is None  # '' -> None
    assert first.status.merge_blockers == ['conflicts', 'approvals']
    assert first.status.mergeable is False
    assert first.status.conflicts is not None and first.status.conflicts[0].configuration_id == 'cfg-1'
    assert second.status.mergeable is True
    assert 'merge it' in second.status.next_step
    # a published merge request is never asked for /conflicts (its source branch is gone; the backend answers 500)
    assert third.status.conflicts == [] and third.status.mergeable is False
    assert storage.merge_request_conflicts.await_count == 2
    # detail = 2 + N details + conflicts for the two open ones, activity log requested
    assert _n_calls(storage) == 2 + 3 + 2
    storage.merge_request_detail.assert_has_awaits(
        [call(42, include_activity_log=True), call(43, include_activity_log=True)], any_order=True
    )


@pytest.mark.asyncio
async def test_token_info_cached_by_middleware_is_reused(mcp_context_client: Context, storage: AsyncMock) -> None:
    token = TOKEN_INFO_VAR.set(TOKEN_INFO)
    try:
        await get_merge_requests(mcp_context_client)
    finally:
        TOKEN_INFO_VAR.reset(token)

    storage.verify_token.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('role', 'bearer_token', 'client_readonly', 'header_read_only', 'expect_write_recommended'),
    [
        pytest.param('admin', None, False, False, True, id='admin'),
        pytest.param('readOnly', 'oauth', False, False, False, id='oauth_readonly_role'),
        pytest.param('', 'oauth', False, False, True, id='oauth_regular'),
        pytest.param('admin', None, True, False, False, id='readonly_client'),
        pytest.param('developer', None, False, False, False, id='developer'),
        pytest.param('admin', None, False, True, False, id='x_read_only_mode_header'),
    ],
)
async def test_next_step_never_recommends_a_write_the_session_cannot_do(
    mcp_context_client: Context,
    storage: AsyncMock,
    keboola_client: KeboolaClient,
    mocker,
    role: str,
    bearer_token: str | None,
    client_readonly: bool,
    header_read_only: bool,
    expect_write_recommended: bool,
) -> None:
    keboola_client.bearer_token = bearer_token
    keboola_client.readonly = client_readonly
    mocker.patch.object(
        mr_tools.ToolAuthorizationMiddleware, '_get_authorization_config', return_value=(None, None, header_read_only)
    )
    storage.verify_token.return_value = {'owner': {'id': 123}, 'admin': {'id': 10, 'role': role}}
    storage.merge_request_detail.return_value = _mr_raw('approved')

    result = await get_merge_requests(mcp_context_client, merge_request_ids=[42])

    assert isinstance(result, MergeRequestsDetailOutput)
    next_step = result.merge_requests[0].status.next_step
    assert ('merge it with merge_merge_request' in next_step) is expect_write_recommended


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('session_branch', 'expected_fragment'),
    [
        pytest.param(
            None,
            "merge it from here with merge_merge_request(merge_request_id=42) (irreversible; branch 'reporting' is "
            'deleted afterwards)',
            id='production_merges_by_id',
        ),
        pytest.param(
            '77',
            f"open a chat on branch 'reporting' ({MR_UI_URL}) or in production and merge it there with "
            'merge_merge_request(merge_request_id=42). Include the link.',
            id='other_branch_hands_off_with_link',
        ),
    ],
)
async def test_detail_off_the_mr_branch_points_to_the_merge_route(
    mcp_context_client: Context,
    storage: AsyncMock,
    keboola_client: KeboolaClient,
    session_branch: str | None,
    expected_fragment: str,
) -> None:
    keboola_client.branch_id = session_branch
    storage.merge_request_detail.return_value = _mr_raw('approved')

    result = await get_merge_requests(mcp_context_client, merge_request_ids=[42])

    assert isinstance(result, MergeRequestsDetailOutput)
    assert expected_fragment in result.merge_requests[0].status.next_step


# ---- create -----------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_create_merge_request_from_session_branch(mcp_context_client: Context, storage: AsyncMock) -> None:
    storage.merge_request_create.return_value = _mr_raw()  # the create response carries no changeLog
    storage.merge_request_detail.return_value = _mr_raw(
        changeLog={'configurations': [{'componentId': 'keboola.ex-db', 'configurationId': 'cfg-9', 'isDeleted': False}]}
    )

    result = await create_merge_request(mcp_context_client, title='Add reporting', reviewer_ids=[20])

    storage.merge_request_create.assert_awaited_once_with(
        branch_from_id=DEV_BRANCH_ID,
        branch_into_id=DEFAULT_BRANCH_ID,
        title='Add reporting',
        description=None,
        reviewer_ids=[20],
        auto_merge_strategy='none',
        auto_merge_at=None,
    )
    assert result.branch_from_name == 'reporting'
    assert result.activity_log is None
    assert result.status.mergeable is True
    assert 'merge it' in result.status.next_step
    assert [c.configuration_id for c in result.changed_configurations] == ['cfg-9']  # from the re-read detail
    assert _n_calls(storage) == 5  # POST + GET detail + /conflicts + branches_list + verify_token


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('branch_id', 'kwargs', 'expected_fragment'),
    [
        pytest.param(None, {}, 'development-branch chat', id='production_session'),
        pytest.param(
            str(DEV_BRANCH_ID), {'auto_merge': 'scheduled'}, 'requires auto_merge_at', id='scheduled_without_time'
        ),
        pytest.param(
            str(DEV_BRANCH_ID),
            {'auto_merge_at': '2026-09-08T10:00:00Z'},
            'only meaningful',
            id='time_without_scheduled',
        ),
    ],
)
async def test_create_merge_request_refuses_without_calling(
    mcp_context_client: Context,
    storage: AsyncMock,
    keboola_client: KeboolaClient,
    branch_id: str | None,
    kwargs: dict[str, Any],
    expected_fragment: str,
) -> None:
    keboola_client.branch_id = branch_id

    with pytest.raises(ToolError, match=expected_fragment):
        await create_merge_request(mcp_context_client, title='T', **kwargs)

    storage.merge_request_create.assert_not_called()


@pytest.mark.asyncio
async def test_create_merge_request_names_the_existing_one(mcp_context_client: Context, storage: AsyncMock) -> None:
    storage.merge_request_create.side_effect = _http_error(
        400, {'error': 'There is already a merge request (42) created from branch "55"'}
    )
    storage.merge_request_detail.return_value = _mr_raw('approved')

    with pytest.raises(ToolError, match="already has merge request 42 .*Next step: .*merge it"):
        await create_merge_request(mcp_context_client, title='T')

    storage.merge_request_detail.assert_awaited_once_with(42)
    storage.merge_requests_list.assert_not_called()


@pytest.mark.asyncio
async def test_create_merge_request_passes_other_400s_through(mcp_context_client: Context, storage: AsyncMock) -> None:
    storage.merge_request_create.side_effect = _http_error(400, {'error': 'Invalid autoMergeAt'})

    with pytest.raises(httpx.HTTPStatusError):
        await create_merge_request(mcp_context_client, title='T')

    storage.merge_request_detail.assert_not_called()


# ---- review-state actions ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_merge_request_sends_only_supplied_keys(mcp_context_client: Context, storage: AsyncMock) -> None:
    storage.merge_request_update.return_value = _mr_raw()  # the update response carries no changeLog
    storage.merge_request_detail.return_value = _mr_raw(
        changeLog={'configurations': [{'componentId': 'keboola.ex-db', 'configurationId': 'cfg-9', 'isDeleted': False}]}
    )

    result = await update_merge_request(mcp_context_client, merge_request_id=42, title='New', auto_merge='none')

    storage.merge_request_update.assert_awaited_once_with(42, {'title': 'New', 'autoMergeStrategy': 'none'})
    assert result.id == 42 and result.activity_log is None
    assert [c.configuration_id for c in result.changed_configurations] == ['cfg-9']  # from the re-read detail
    assert (
        _n_calls(storage) == 5
    )  # PUT + GET detail + /conflicts + verify_token + branches_list (names/links); 4 with the token cache


@pytest.mark.asyncio
async def test_update_merge_request_rejects_time_without_scheduled_strategy(
    mcp_context_client: Context, storage: AsyncMock
) -> None:
    """The backend applies auto-merge only when a strategy is sent; a lone time would be silently ignored."""
    with pytest.raises(ToolError, match='only meaningful'):
        await update_merge_request(mcp_context_client, merge_request_id=42, auto_merge_at='2026-10-01T18:00:00+02:00')

    storage.merge_request_update.assert_not_called()


@pytest.mark.asyncio
async def test_update_merge_request_requires_a_change(mcp_context_client: Context, storage: AsyncMock) -> None:
    with pytest.raises(ToolError, match='Nothing to update'):
        await update_merge_request(mcp_context_client, merge_request_id=42)
    storage.merge_request_update.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('tool', 'kwargs', 'method'),
    [
        pytest.param(approve_merge_request, {'merge_request_id': 42}, 'merge_request_approve', id='approve'),
        pytest.param(
            request_merge_request_changes,
            {'merge_request_id': 42, 'reason': 'no'},
            'merge_request_request_changes',
            id='request_changes',
        ),
        pytest.param(update_merge_request, {'merge_request_id': 42, 'title': 'x'}, 'merge_request_update', id='update'),
        pytest.param(
            request_merge_request_review, {'merge_request_id': 42}, 'merge_request_request_review', id='request_review'
        ),
        pytest.param(merge_merge_request, {'merge_request_id': 42}, 'merge_request_merge', id='merge'),
        pytest.param(create_merge_request, {'title': 'T'}, 'merge_request_create', id='create'),
    ],
)
@pytest.mark.parametrize(
    ('status', 'error', 'expected'),
    [
        pytest.param(403, 'Forbidden', 'The backend refused it: Forbidden.*project role', id='403_role'),
        pytest.param(
            422,
            'Operation cannot be performed due: The marking does not enable the transition.',
            'The backend refused it: .*marking does not enable the transition.*not in a state that allows this action',
            id='422_state_machine',
        ),
    ],
)
async def test_backend_403_and_422_map_to_clear_messages(
    mcp_context_client: Context,
    storage: AsyncMock,
    tool: Any,
    kwargs: dict[str, Any],
    method: str,
    status: int,
    error: str,
    expected: str,
) -> None:
    getattr(storage, method).side_effect = _http_error(status, {'error': error})

    with pytest.raises(ToolError, match=expected):
        await tool(mcp_context_client, **kwargs)


@pytest.mark.asyncio
async def test_approve_and_request_changes_pass_through(mcp_context_client: Context, storage: AsyncMock) -> None:
    storage.merge_request_approve.return_value = _mr_raw(
        'approved', approvals=[{'approverId': '10', 'approverName': 'Alice'}]
    )
    storage.merge_request_request_changes.return_value = _mr_raw('development')

    approved = await approve_merge_request(mcp_context_client, merge_request_id=42)
    sent_back = await request_merge_request_changes(mcp_context_client, merge_request_id=42, reason='fix the join')

    storage.merge_request_approve.assert_awaited_once_with(42)
    storage.merge_request_request_changes.assert_awaited_once_with(42, reason='fix the join')
    assert approved.status.approved_by == ['Alice'] and approved.status.viewer.has_approved is True
    assert sent_back.state == 'development'


# ---- MR-id convention (branch-only tools) ------------------------------------------------------------------


_DEV = str(DEV_BRANCH_ID)
_OTHER_BRANCH_URL = 'https://connection.test.keboola.com/admin/projects/123/branch/77/development-overview'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('session_branch', 'merge_request_id', 'listed', 'detail', 'expected_error', 'expected_acted_on'),
    [
        # --- a development-branch session: its own MR, implicitly or by id; never another branch's
        pytest.param(_DEV, None, [_mr_raw(mr_id=42)], None, None, 42, id='omitted_resolves_branch_mr'),
        pytest.param(_DEV, 42, None, _mr_raw(mr_id=42), None, 42, id='given_and_matching'),
        pytest.param(
            _DEV,
            9,
            None,
            _mr_raw(mr_id=9, branch_from=77),
            r"Merge request 9 \('Add reporting'\) belongs to branch 'other' \(id 77\), not to this chat's branch\. "
            rf'Tell the user to open a chat on that branch \({_OTHER_BRANCH_URL}\) or in production and ask again there '
            r'with merge_request_id=9; include the link\.',
            None,
            id='given_other_branch_hands_off_with_link_and_the_production_route',
        ),
        pytest.param(
            _DEV,
            None,
            [_mr_raw(mr_id=9, branch_from=77)],
            None,
            'has no open merge request',
            None,
            id='omitted_none_on_branch',
        ),
        pytest.param(
            _DEV,
            None,
            [_mr_raw('canceled', mr_id=41), _mr_raw('development', mr_id=42)],
            None,
            None,
            42,
            id='omitted_skips_canceled_mr_on_the_branch',
        ),
        pytest.param(
            _DEV,
            None,
            [_mr_raw('canceled', mr_id=41)],
            None,
            'has no open merge request',
            None,
            id='omitted_only_canceled_is_none',
        ),
        pytest.param(
            _DEV,
            43,
            None,
            _mr_raw('published', mr_id=43, branch_from=None),
            'already merged .*nothing to do',
            None,
            id='given_published_says_merged_not_branch_none',
        ),
        # --- a production session: any MR by id; without an id the candidates are listed, never auto-picked
        pytest.param(None, 9, None, _mr_raw(mr_id=9, branch_from=77), None, 9, id='production_given_any_branch'),
        pytest.param(
            None,
            None,
            [_mr_raw(mr_id=42), _mr_raw('approved', mr_id=9, branch_from=77), _mr_raw('canceled', mr_id=3)],
            None,
            r"This chat is on the production branch: pass merge_request_id\. Open merge requests: 42 'Add reporting' "
            r"\(branch 'reporting', in_development\); 9 'Add reporting' \(branch 'other', approved\)\. If the request "
            r'matches more than one, ask the user which one \(by title or branch\); never guess\.',
            None,
            id='production_omitted_lists_open_mrs_newest_first_without_terminal_ones',
        ),
        pytest.param(
            None,
            None,
            [_mr_raw(mr_id=42)],
            None,
            "pass merge_request_id. Open merge requests: 42 'Add reporting'",
            None,
            id='production_omitted_never_auto_picks_even_a_single_candidate',
        ),
        pytest.param(
            None,
            None,
            [_mr_raw('canceled', mr_id=3)],
            None,
            'production branch and the project has no open merge request',
            None,
            id='production_omitted_without_candidates',
        ),
        pytest.param(
            None,
            43,
            None,
            _mr_raw('published', mr_id=43, branch_from=None),
            'already merged .*nothing to do',
            None,
            id='production_given_published',
        ),
    ],
)
async def test_mr_id_convention(
    mcp_context_client: Context,
    storage: AsyncMock,
    keboola_client: KeboolaClient,
    session_branch: str | None,
    merge_request_id: int | None,
    listed: list[dict] | None,
    detail: dict | None,
    expected_error: str | None,
    expected_acted_on: int | None,
) -> None:
    keboola_client.branch_id = session_branch
    if listed is not None:
        storage.merge_requests_list.return_value = listed
    if detail is not None:
        storage.merge_request_detail.return_value = detail
    storage.merge_request_request_review.return_value = _mr_raw('approved', mr_id=expected_acted_on or 0)

    if expected_error:
        with pytest.raises(ToolError, match=expected_error):
            await request_merge_request_review(mcp_context_client, merge_request_id=merge_request_id)
        storage.merge_request_request_review.assert_not_called()
    else:
        result = await request_merge_request_review(mcp_context_client, merge_request_id=merge_request_id)
        storage.merge_request_request_review.assert_awaited_once_with(expected_acted_on)
        assert result.state == 'approved'
        if merge_request_id is None:
            storage.merge_requests_list.assert_awaited_once()  # the +1 of the omitted id
        else:
            storage.merge_requests_list.assert_not_called()


@pytest.mark.asyncio
async def test_omitted_id_after_merge_hands_off_to_production(
    mcp_context_client: Context, storage: AsyncMock, keboola_client: KeboolaClient
) -> None:
    """The dev-branch session that merged is dead: the branch is gone from branches_list and the MR is published."""
    keboola_client.branch_id = '999'
    storage.merge_requests_list.return_value = [_mr_raw('published', branch_from=None)]

    with pytest.raises(ToolError, match='no longer exists.*production branch'):
        await merge_merge_request(mcp_context_client)

    storage.merge_request_merge.assert_not_called()


@pytest.mark.asyncio
async def test_omitted_id_in_the_deletion_window_hands_off_to_production(
    mcp_context_client: Context, storage: AsyncMock
) -> None:
    """Right after a merge the branch is still listed while its MR is already published: no 'create one' advice."""
    storage.merge_requests_list.return_value = [_mr_raw('published')]

    with pytest.raises(
        ToolError,
        match=r'already merged.*continue in production \(https://connection.test.keboola.com/admin/projects/123/\)',
    ):
        await request_merge_request_review(mcp_context_client)

    storage.merge_request_request_review.assert_not_called()


@pytest.mark.asyncio
async def test_merge_on_production_without_id_lists_candidates_and_never_merges(
    mcp_context_client: Context, storage: AsyncMock, keboola_client: KeboolaClient
) -> None:
    keboola_client.branch_id = None

    with pytest.raises(ToolError, match="pass merge_request_id. Open merge requests: 42 'Add reporting'"):
        await merge_merge_request(mcp_context_client)

    storage.merge_request_merge.assert_not_called()
    storage.merge_request_detail.assert_not_called()


# ---- merge ------------------------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('body', 'expected_refusal', 'expected_conflict_ids', 'expected_fragment'),
    [
        pytest.param(
            {
                'code': 'storage.mergeRequests.validation',
                'error': 'Merge request has conflicts',
                'params': {'errors': [_conflict_raw('cfg-1'), _conflict_raw('cfg-2')]},
            },
            'conflicts',
            ['cfg-1', 'cfg-2'],
            '2 conflicts block the merge',
            id='conflict_409_carries_the_conflicts',
        ),
        pytest.param(
            {
                'code': 'storage.mergeRequests.notReadyToMerge',
                'error': 'Cannot merge, branch is in "development" state.',
            },
            'not_ready',
            None,
            'request a review',
            id='not_ready_in_development_means_request_review',
        ),
        pytest.param(
            {'error': 'Conflicts', 'params': {'errors': [_conflict_raw('cfg-1')]}},
            'conflicts',
            ['cfg-1'],
            '1 conflict block',
            id='code_less_409_is_a_conflict',
        ),
    ],
)
async def test_merge_refusals(
    mcp_context_client: Context,
    storage: AsyncMock,
    body: dict,
    expected_refusal: str,
    expected_conflict_ids: list[str] | None,
    expected_fragment: str,
) -> None:
    storage.merge_request_merge.side_effect = _http_error(409, body)

    result = await merge_merge_request(mcp_context_client, merge_request_id=42)

    assert result.merged is False
    assert result.refusal == expected_refusal
    assert result.refusal_message == body['error']
    assert result.state == 'development'
    assert result.source_branch_deleting is False
    assert (
        None if result.conflicts is None else [c.configuration_id for c in result.conflicts]
    ) == expected_conflict_ids
    assert result.status is not None and expected_fragment in result.next_step
    storage.merge_request_conflicts.assert_not_called()  # the 409 already carries them
    storage.job_detail.assert_not_called()


@pytest.mark.asyncio
async def test_merge_code_less_409_without_live_conflicts_is_not_ready(
    mcp_context_client: Context, storage: AsyncMock
) -> None:
    """A 409 without code and without any live conflict is an unknown refusal, never mergeable=True / "merge it"."""
    storage.merge_request_merge.side_effect = _http_error(409, {'error': 'Conflicts'})
    storage.merge_request_conflicts.return_value = []  # even the live list is empty (race / proxy)

    result = await merge_merge_request(mcp_context_client, merge_request_id=42)

    assert result.refusal == 'not_ready'  # the 409 was not a conflict after all
    assert result.conflicts is None
    assert result.status is not None and result.status.mergeable is False
    assert 'Ready: merge it' not in result.next_step
    storage.merge_request_conflicts.assert_awaited_once_with(42)


@pytest.mark.asyncio
async def test_merge_code_less_409_with_live_conflicts(mcp_context_client: Context, storage: AsyncMock) -> None:
    storage.merge_request_merge.side_effect = _http_error(409, {'error': 'Conflicts'})
    storage.merge_request_conflicts.return_value = [_conflict_raw('cfg-1')]

    result = await merge_merge_request(mcp_context_client, merge_request_id=42)

    assert result.refusal == 'conflicts'
    assert result.conflicts is not None and [c.configuration_id for c in result.conflicts] == ['cfg-1']
    assert '1 conflict block' in result.next_step


@pytest.mark.asyncio
async def test_merge_not_ready_on_approved_says_wait(mcp_context_client: Context, storage: AsyncMock) -> None:
    storage.merge_request_detail.return_value = _mr_raw('approved')
    storage.merge_request_merge.side_effect = _http_error(
        409, {'code': 'storage.mergeRequests.notReadyToMerge', 'error': 'Another merge request is being processed.'}
    )

    result = await merge_merge_request(mcp_context_client, merge_request_id=42)

    assert result.refusal == 'not_ready'
    assert result.status is not None and result.status.mergeable is False
    assert 'wait for it to clear' in result.next_step
    assert 'Another merge request is being processed.' in result.next_step
    assert 'Ready: merge it' not in result.next_step


_NOT_READY_IN_MERGE = _http_error(
    409, {'code': 'storage.mergeRequests.notReadyToMerge', 'error': 'Cannot merge, branch is in "in_merge" state.'}
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('merge_error', 'expected_warning'),
    [
        pytest.param(_NOT_READY_IN_MERGE, 'A merge is already running', id='not_ready_409'),
        pytest.param(
            httpx.ReadTimeout('timed out'), 'The merge call failed (ReadTimeout: timed out)', id='read_timeout'
        ),
        pytest.param(httpx.ReadError('connection reset'), 'The merge call failed (ReadError', id='read_error'),
        pytest.param(_http_error(502), 'The merge call failed (HTTPStatusError: HTTP 502)', id='bad_gateway_502'),
        pytest.param(_http_error(504), 'The merge call failed (HTTPStatusError: HTTP 504)', id='gateway_timeout_504'),
    ],
)
async def test_merge_not_ready_while_a_merge_is_running_is_not_a_refusal(
    mcp_context_client: Context, storage: AsyncMock, merge_error: Exception, expected_warning: str
) -> None:
    """
    A timed-out first attempt (or auto-merge) already started the job, or this very call started it and then lost
    the response: neither the 409 nor the lost response may read as 'nothing changed' or as a bare error.
    """
    storage.merge_request_detail.side_effect = [_mr_raw('approved'), _mr_raw('in_merge')]
    storage.merge_request_merge.side_effect = merge_error

    result = await merge_merge_request(mcp_context_client, merge_request_id=42)

    assert result.refusal is None
    assert result.merged is False
    assert result.state == 'in_merge'
    assert expected_warning in result.warnings[0]
    assert storage.merge_request_merge.await_count == 1  # the merge PUT is never retried
    assert 'Do not edit this branch' in result.next_step and 'do not start a second merge' in result.next_step
    assert result.status is not None and result.status.next_step == result.next_step


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('merge_error', 'reread', 'expected_fragment'),
    [
        pytest.param(
            httpx.ReadTimeout('timed out'),
            _mr_raw('approved'),
            "The merge call failed (ReadTimeout: timed out) and merge request 42 is in state 'approved'; it could not "
            'be confirmed whether the merge started.',
            id='lost_response_mr_not_in_merge',
        ),
        pytest.param(
            _http_error(502),
            httpx.ReadError('connection reset'),
            'The merge call failed (HTTPStatusError: HTTP 502) and merge request 42 could not be re-read '
            '(ReadError: connection reset); the merge may have started.',
            id='lost_response_and_reread_fails',
        ),
        pytest.param(
            _NOT_READY_IN_MERGE,
            httpx.ReadTimeout('timed out'),
            'The backend refused the merge as not ready (Cannot merge, branch is in "in_merge" state.) and merge '
            'request 42 could not be re-read (ReadTimeout: timed out); the merge may have started.',
            id='not_ready_and_reread_fails',
        ),
    ],
)
async def test_merge_with_unknown_outcome_is_never_a_bare_error(
    mcp_context_client: Context, storage: AsyncMock, merge_error: Exception, reread: Any, expected_fragment: str
) -> None:
    """When the merge may have started but the MR does not show it, the agent is told to check, not to retry."""
    storage.merge_request_detail.side_effect = [_mr_raw('approved'), reread]
    storage.merge_request_merge.side_effect = merge_error

    with pytest.raises(ToolError) as exc_info:
        await merge_merge_request(mcp_context_client, merge_request_id=42)

    assert expected_fragment in str(exc_info.value)
    assert 'Check get_merge_requests before merging again and do not edit the branch meanwhile.' in str(exc_info.value)
    assert storage.merge_request_merge.await_count == 1
    storage.job_detail.assert_not_called()


@pytest.mark.asyncio
async def test_merge_lost_response_after_the_merge_finished_reports_the_merge(
    mcp_context_client: Context, storage: AsyncMock
) -> None:
    storage.merge_request_detail.side_effect = [_mr_raw('approved'), _mr_raw('published', branch_from=None)]
    storage.merge_request_merge.side_effect = httpx.ReadTimeout('timed out')

    result = await merge_merge_request(mcp_context_client, merge_request_id=42)

    assert result.merged is True
    assert result.state == 'published'
    assert result.job_id is None
    assert result.source_branch_deleting is True
    assert 'The merge call failed (ReadTimeout: timed out)' in result.warnings[0]
    assert "branch 'reporting' is being deleted" in result.next_step
    assert storage.merge_request_merge.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'merge_error',
    [
        pytest.param(httpx.ConnectError('connection refused'), id='connect_error'),
        pytest.param(httpx.ConnectTimeout('connect timed out'), id='connect_timeout_is_not_a_lost_response'),
    ],
)
async def test_merge_request_never_sent_is_reraised(
    mcp_context_client: Context, storage: AsyncMock, merge_error: Exception
) -> None:
    """A connection-phase failure never reached the backend: nothing started, the error passes through unchanged."""
    storage.merge_request_merge.side_effect = merge_error

    with pytest.raises(type(merge_error)):
        await merge_merge_request(mcp_context_client, merge_request_id=42)

    assert storage.merge_request_detail.await_count == 1  # only the pre-merge read, no outcome re-read
    assert storage.merge_request_merge.await_count == 1


@pytest.mark.asyncio
async def test_merge_state_override_drops_server_derived_fields(
    mcp_context_client: Context, storage: AsyncMock, monkeypatch
) -> None:
    """After a timeout the local `in_merge` override must win over DMD-1988 fields still saying 'approved'."""
    monkeypatch.setattr(mr_tools, '_next_poll_interval', lambda _elapsed: 0.0)
    monkeypatch.setattr(mr_tools, 'MERGE_JOB_TIMEOUT_SEC', 0)
    storage.merge_request_detail.return_value = _mr_raw(
        'approved', derivedState='approved', allowedActions=['merge'], mergeBlockers=[]
    )
    storage.merge_request_merge.return_value = {'id': 991}
    storage.job_detail.return_value = {'id': 991, 'status': 'processing'}

    result = await merge_merge_request(mcp_context_client, merge_request_id=42)

    assert result.state == 'in_merge'
    assert result.status is not None
    assert result.status.derived_state == 'in_merge'
    assert result.status.mergeable is not True
    assert 'Ready: merge it' not in result.status.next_step
    assert 'do not start a second merge' in result.status.next_step


@pytest.mark.asyncio
async def test_merge_reraises_unknown_409(mcp_context_client: Context, storage: AsyncMock) -> None:
    storage.merge_request_merge.side_effect = _http_error(409, {'code': 'storage.something.else', 'error': 'nope'})

    with pytest.raises(httpx.HTTPStatusError):
        await merge_merge_request(mcp_context_client, merge_request_id=42)


@pytest.mark.asyncio
async def test_merge_success_awaits_job_and_hands_off(
    mcp_context_client: Context, storage: AsyncMock, monkeypatch
) -> None:
    monkeypatch.setattr(mr_tools, '_next_poll_interval', lambda _elapsed: 0.0)
    storage.merge_request_merge.return_value = {'id': 987, 'status': 'waiting'}  # int on the wire
    storage.job_detail.side_effect = [{'id': 987, 'status': 'processing'}, {'id': 987, 'status': 'success'}]

    result = await merge_merge_request(mcp_context_client, merge_request_id=42)

    assert result.merged is True
    assert result.state == 'published'
    assert result.job_id == '987'
    assert result.source_branch_deleting is True
    assert result.refusal is None and result.status is None
    assert (
        "branch 'reporting' is being deleted. This chat is pinned to that branch: it can still answer questions about "
        'production but cannot make changes. Tell the user to continue in production '
        '(https://connection.test.keboola.com/admin/projects/123/); include the link.'
    ) in result.next_step
    assert [link.url for link in result.links] == ['https://connection.test.keboola.com/admin/projects/123/']
    assert storage.job_detail.await_count == 2
    # branchFromId is read from the MR BEFORE the merge is issued
    assert storage.method_calls.index(call.merge_request_detail(42)) < storage.method_calls.index(
        call.merge_request_merge(42)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('job', 'reread', 'expected_message', 'expected_state'),
    [
        pytest.param(
            {'id': 988, 'status': 'error', 'error': {'message': 'boom'}},
            _mr_raw('approved'),
            'boom',
            'approved',
            id='error_with_message',
        ),
        pytest.param(
            {'id': 988, 'status': 'cancelled'},
            _mr_raw('development'),
            "ended with status 'cancelled'",
            'development',
            id='cancelled_is_terminal_and_the_reread_state_wins',
        ),
        pytest.param(
            {'id': 988, 'status': 'terminated', 'error': 'killed'},
            httpx.ReadTimeout('timed out'),
            'killed',
            'approved',
            id='terminated_reread_fails_falls_back_to_approved',
        ),
    ],
)
async def test_merge_job_failure_rolls_back_to_approved(
    mcp_context_client: Context,
    storage: AsyncMock,
    monkeypatch,
    job: dict,
    reread: Any,
    expected_message: str,
    expected_state: str,
) -> None:
    monkeypatch.setattr(mr_tools, '_next_poll_interval', lambda _elapsed: 0.0)
    storage.merge_request_detail.side_effect = [_mr_raw('approved'), reread]
    storage.merge_request_merge.return_value = {'id': '988'}
    storage.job_detail.return_value = job

    result = await merge_merge_request(mcp_context_client, merge_request_id=42)

    assert result.merged is False
    assert result.state == expected_state
    assert result.status is not None and result.status.state == expected_state
    assert result.refusal is None
    assert result.refusal_message is not None and expected_message in result.refusal_message
    assert expected_message in result.next_step
    assert f'the merge request is now in `{expected_state}`' in result.next_step
    assert storage.job_detail.await_count == 1  # terminal on the first poll, no spinning
    assert storage.merge_request_detail.await_count == 2  # the pre-merge read + the outcome re-read


@pytest.mark.asyncio
async def test_merge_job_non_success_with_a_merged_mr_reports_the_merge(
    mcp_context_client: Context, storage: AsyncMock, monkeypatch
) -> None:
    """The re-read MR, not the job status, decides: a published merge request is reported as merged."""
    monkeypatch.setattr(mr_tools, '_next_poll_interval', lambda _elapsed: 0.0)
    storage.merge_request_detail.side_effect = [_mr_raw('approved'), _mr_raw('published', branch_from=None)]
    storage.merge_request_merge.return_value = {'id': '988'}
    storage.job_detail.return_value = {'id': 988, 'status': 'warning'}

    result = await merge_merge_request(mcp_context_client, merge_request_id=42)

    assert result.merged is True
    assert result.state == 'published'
    assert result.job_id == '988'
    assert result.source_branch_deleting is True
    assert "ended with status 'warning'" in result.warnings[0]


@pytest.mark.asyncio
async def test_merge_poll_failure_is_not_a_merge_failure(
    mcp_context_client: Context, storage: AsyncMock, monkeypatch
) -> None:
    """The merge is irreversible once the job started: a transient error while polling must not read as failed."""
    monkeypatch.setattr(mr_tools, '_next_poll_interval', lambda _elapsed: 0.0)
    storage.merge_request_merge.return_value = {'id': 990}
    storage.job_detail.side_effect = [
        {'id': 990, 'status': 'processing'},
        httpx.ReadTimeout('timed out'),
    ]

    result = await merge_merge_request(mcp_context_client, merge_request_id=42)

    assert result.merged is False
    assert result.state == 'in_merge'
    assert result.job_id == '990'
    assert 'Lost contact with the merge job 990' in result.warnings[0]
    assert 'do not merge again' in result.next_step and 'do not edit the branch' in result.next_step


@pytest.mark.asyncio
async def test_merge_timeout_is_not_a_failure(mcp_context_client: Context, storage: AsyncMock, monkeypatch) -> None:
    monkeypatch.setattr(mr_tools, '_next_poll_interval', lambda _elapsed: 0.0)
    monkeypatch.setattr(mr_tools, 'MERGE_JOB_TIMEOUT_SEC', 0)
    storage.merge_request_merge.return_value = {'id': 989}
    storage.job_detail.return_value = {'id': 989, 'status': 'processing'}

    result = await merge_merge_request(mcp_context_client, merge_request_id=42)

    assert result.merged is False
    assert result.state == 'in_merge'
    assert result.job_id == '989'
    assert 'still running' in result.next_step
    assert result.warnings


# ---- conflicts ---------------------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('session_branch', 'expected_fragment'),
    [
        pytest.param(_DEV, '2 conflicts block the merge; call get_merge_request_conflicts and resolve', id='on_branch'),
        pytest.param(
            None,
            '2 conflicts block the merge; call get_merge_request_conflicts(merge_request_id=42) and resolve them one '
            "by one with resolve_merge_request_conflict (the resolutions are written to branch 'reporting').",
            id='from_production_by_id',
        ),
    ],
)
async def test_get_merge_request_conflicts_fans_out(
    mcp_context_client: Context,
    storage: AsyncMock,
    keboola_client: KeboolaClient,
    session_branch: str | None,
    expected_fragment: str,
) -> None:
    keboola_client.branch_id = session_branch
    storage.merge_request_conflicts.return_value = [_conflict_raw('cfg-1'), _conflict_raw('cfg-2')]
    storage.configuration_diff.side_effect = [
        _diff(),
        _diff(ours=_side(3, name='Renamed'), theirs=_side(9)),
    ]

    result = await get_merge_request_conflicts(mcp_context_client, merge_request_id=42)

    assert result.merge_request_id == 42
    assert [c.configuration_id for c in result.conflicts] == ['cfg-1', 'cfg-2']
    first, second = result.conflicts
    assert first.conflicting_paths == ['/configuration/parameters/query'] and first.suggested_take is None
    assert first.theirs is not None and first.theirs.version == 9
    assert second.changes[0].path == '/name' and second.suggested_take == 'ours'
    assert result.status.merge_blockers == ['conflicts']
    assert expected_fragment in result.status.next_step
    # /diff is branch-scoped: it goes through a client on the MR's source branch (the session client on that branch)
    keboola_client.with_branch_id.assert_awaited_once_with(_DEV)
    storage.configuration_diff.assert_has_awaits(
        [call('keboola.ex-db', 'cfg-1'), call('keboola.ex-db', 'cfg-2')], any_order=True
    )
    assert _n_calls(storage) == 2 + 2 + 2  # detail + /conflicts + N diffs (+ branches_list + verify_token overhead)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('kwargs', 'diff', 'expected_mode', 'expected_diff', 'expected_warning'),
    [
        pytest.param(
            {'take': 'ours', 'change_description': 'keep dev'},
            _diff(),
            'ours',
            {
                'name': 'My config',
                'description': None,
                'configuration': {'parameters': {'query': 'SELECT 2'}},
                'isDisabled': False,
                'rows': [],
                'changeDescription': 'keep dev',
            },
            None,
            id='take_ours',
        ),
        pytest.param(
            {'take': 'theirs'},
            _diff(),
            'theirs',
            {
                'name': 'My config',
                'description': None,
                'configuration': {'parameters': {'query': 'SELECT 3'}},
                'isDisabled': False,
                'rows': [],
            },
            None,
            id='take_theirs',
        ),
        pytest.param(
            {'take': 'delete', 'change_description': 'bye'}, _diff(), 'delete', {}, 'ignored', id='take_delete_warns'
        ),
        pytest.param(
            {'take': 'ours'}, _diff(ours=_side(3, is_deleted=True)), 'delete', {}, None, id='ours_deleted_is_delete'
        ),
        pytest.param(
            {'take': 'theirs'},
            _diff(theirs=_side(9, is_deleted=True)),
            'delete',
            {},
            None,
            id='theirs_deleted_is_delete',
        ),
        pytest.param(
            {
                'resolved': {
                    'name': 'Merged',
                    'description': None,
                    'is_disabled': True,
                    'configuration': {'parameters': {'query': 'SELECT 2 UNION SELECT 3'}},
                    'rows': [{'id': 'r1'}],
                },
                'change_description': 'manual',
            },
            _diff(),
            'custom',
            {
                'name': 'Merged',
                'description': None,
                'isDisabled': True,
                'configuration': {'parameters': {'query': 'SELECT 2 UNION SELECT 3'}},
                'rows': [{'id': 'r1'}],
                'changeDescription': 'manual',
            },
            None,
            id='custom_body',
        ),
    ],
)
async def test_resolve_conflict_modes(
    mcp_context_client: Context,
    storage: AsyncMock,
    kwargs: dict[str, Any],
    diff: dict,
    expected_mode: str,
    expected_diff: dict,
    expected_warning: str | None,
) -> None:
    storage.merge_request_conflicts.side_effect = [
        [_conflict_raw('cfg-1'), _conflict_raw('cfg-2')],
        [_conflict_raw('cfg-2')],
    ]
    storage.configuration_diff.return_value = diff
    storage.configuration_rebase.return_value = {'id': 'cfg-1', 'version': 4}

    result = await resolve_merge_request_conflict(
        mcp_context_client, component_id='keboola.ex-db', configuration_id='cfg-1', merge_request_id=42, **kwargs
    )

    storage.configuration_rebase.assert_awaited_once_with('keboola.ex-db', 'cfg-1', version=9, diff=expected_diff)
    assert result.resolved is True
    assert result.mode == expected_mode
    assert result.rebased_onto_version == 9
    assert [c.configuration_id for c in result.remaining_conflicts] == ['cfg-2']
    assert 'Resolve the next conflict: keboola.ex-db/cfg-2 (1 left)' in result.status.next_step
    assert (expected_warning is None) == (not result.warnings)
    if expected_warning:
        assert expected_warning in result.warnings[0]
    assert _n_calls(storage) == 5 + 2  # detail + /conflicts guard + /diff + rebase + /conflicts re-check (+ overhead)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('session_branch', 'kwargs', 'expected_fragment'),
    [
        pytest.param(_DEV, {}, 'Ready: merge it with merge_merge_request', id='on_branch_implicit_mr'),
        pytest.param(
            None,
            {'merge_request_id': 42},
            'Ready to merge; merge it from here with merge_merge_request(merge_request_id=42)',
            id='from_production_by_id',
        ),
    ],
)
async def test_resolve_last_conflict_recommends_merge(
    mcp_context_client: Context,
    storage: AsyncMock,
    keboola_client: KeboolaClient,
    session_branch: str | None,
    kwargs: dict[str, Any],
    expected_fragment: str,
) -> None:
    keboola_client.branch_id = session_branch
    storage.merge_request_conflicts.side_effect = [[_conflict_raw('cfg-1')], []]
    storage.configuration_diff.return_value = _diff()

    result = await resolve_merge_request_conflict(
        mcp_context_client, component_id='keboola.ex-db', configuration_id='cfg-1', take='ours', **kwargs
    )

    assert result.remaining_conflicts == []
    assert result.status.mergeable is True
    assert expected_fragment in result.status.next_step
    # /diff and /rebase are branch-scoped: both go through a client on the MR's source branch
    keboola_client.with_branch_id.assert_awaited_once_with(_DEV)
    storage.configuration_rebase.assert_awaited_once()


@pytest.mark.asyncio
async def test_resolve_from_production_when_the_branch_is_gone(
    mcp_context_client: Context, storage: AsyncMock, keboola_client: KeboolaClient
) -> None:
    """The MR still lists `branchFromId` (deleted moments ago) but the branch client cannot be built: no rebase."""
    keboola_client.branch_id = None
    storage.merge_request_conflicts.return_value = [_conflict_raw('cfg-1')]
    request = httpx.Request('GET', 'https://connection.test.keboola.com/v2/storage/dev-branches/55')
    keboola_client.with_branch_id.side_effect = BranchNotFoundError(
        'Branch "55" not found', branch_id='55', request=request, response=httpx.Response(404, request=request)
    )

    with pytest.raises(ToolError, match="Branch 'reporting' \\(id 55\\) of merge request 42 no longer exists"):
        await resolve_merge_request_conflict(
            mcp_context_client, component_id='keboola.ex-db', configuration_id='cfg-1', take='ours', merge_request_id=42
        )

    storage.configuration_diff.assert_not_called()
    storage.configuration_rebase.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('kwargs', 'diff', 'expected_fragment', 'expect_network'),
    [
        pytest.param({}, _diff(), 'exactly one of', False, id='neither'),
        pytest.param({'take': 'ours', 'resolved': {'name': 'x'}}, _diff(), 'exactly one of', False, id='both'),
        pytest.param(
            {'resolved': {'name': 'x', 'is_disabled': False, 'configuration': {}, 'rows': []}},
            _diff(),
            'description',
            False,
            id='custom_missing_description_key',
        ),
        pytest.param(
            {'resolved': {'name': '  ', 'description': None, 'is_disabled': False, 'configuration': {}, 'rows': []}},
            _diff(),
            'non-empty',
            False,
            id='custom_blank_name',
        ),
        pytest.param(
            {'resolved': {'name': 'x', 'description': None, 'is_disabled': 'false', 'configuration': {}, 'rows': []}},
            _diff(),
            'is_disabled',
            False,
            id='custom_string_boolean',
        ),
        pytest.param(
            {'take': 'ours'},
            _diff(ours={'version': 3, 'isDeleted': False, 'diff': {}}),
            'envelope hole',
            True,
            id='take_empty_side_is_not_a_delete',
        ),
        pytest.param(
            {'take': 'ours'},
            _diff(ours={'version': 3, 'isDeleted': False, 'diff': {'name': 'n', 'rows': [], 'configuration': {}}}),
            'carries no isDisabled',
            True,
            id='take_holed_side',
        ),
        pytest.param({'take': 'ours'}, _diff(theirs=None), 'no production', True, id='theirs_missing'),
        pytest.param({'take': 'ours'}, _diff(ours=None), 'no ours side', True, id='absent_side_is_not_a_delete'),
    ],
)
async def test_resolve_conflict_refuses_without_rebasing(
    mcp_context_client: Context,
    storage: AsyncMock,
    kwargs: dict[str, Any],
    diff: dict,
    expected_fragment: str,
    expect_network: bool,
) -> None:
    storage.merge_request_conflicts.return_value = [_conflict_raw('cfg-1')]
    storage.configuration_diff.return_value = diff

    with pytest.raises(ToolError, match=expected_fragment):
        await resolve_merge_request_conflict(
            mcp_context_client, component_id='keboola.ex-db', configuration_id='cfg-1', merge_request_id=42, **kwargs
        )

    storage.configuration_rebase.assert_not_called()
    if not expect_network:
        assert _n_calls(storage) == 0  # validated before any request


@pytest.mark.asyncio
async def test_resolve_conflict_guards_the_conflict_set(mcp_context_client: Context, storage: AsyncMock) -> None:
    storage.merge_request_conflicts.return_value = [_conflict_raw('cfg-1')]

    with pytest.raises(ToolError, match='not in merge request 42'):
        await resolve_merge_request_conflict(
            mcp_context_client,
            component_id='keboola.ex-db',
            configuration_id='cfg-other',
            take='ours',
            merge_request_id=42,
        )

    storage.configuration_diff.assert_not_called()
    storage.configuration_rebase.assert_not_called()
