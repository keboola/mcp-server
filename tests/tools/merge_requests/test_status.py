from typing import Any

import pytest

from keboola_mcp_server.tools.merge_requests.models import ConflictRef, Viewer
from keboola_mcp_server.tools.merge_requests.status import (
    SessionContext,
    build_next_step,
    build_status,
    derive_allowed_actions,
    derive_merge_blockers,
    derive_state,
    derive_viewer,
    request_changes_reason,
)


def _mr(state: str = 'development', **extra: Any) -> dict[str, Any]:
    return {'id': 1, 'state': state, 'creator': {'id': 10, 'name': 'Alice'}, 'reviewers': [], 'approvals': [], **extra}


def _conflict(cid: str = 'cfg-1') -> ConflictRef:
    return ConflictRef(
        component_id='keboola.ex-db',
        configuration_id=cid,
        message='changed on both sides',
        is_deleted=False,
        dev_branch_version_identifier='a',
        default_branch_version_identifier='b',
    )


ON_BRANCH_WRITER = SessionContext(on_mr_branch=True, can_write=True)
ON_BRANCH_READER = SessionContext(on_mr_branch=True, can_write=False)
PRODUCTION_WRITER = SessionContext(on_mr_branch=False, can_write=True)
UNKNOWN_VIEWER = Viewer(is_creator=None, has_approved=None)


@pytest.mark.parametrize(
    ('mr', 'expected'),
    [
        pytest.param(_mr('development'), 'in_development', id='development'),
        pytest.param(_mr('in_review'), 'in_review', id='in_review'),
        pytest.param(_mr('approved'), 'approved', id='approved'),
        pytest.param(_mr('in_merge'), 'in_merge', id='in_merge'),
        pytest.param(_mr('published'), 'merged', id='published_is_merged'),
        pytest.param(_mr('canceled'), 'closed', id='canceled_is_closed'),
        pytest.param(
            _mr('development', reviewers=[{'id': 20, 'name': 'Bob', 'status': 'rejected'}]),
            'rejected',
            id='non_creator_rejection',
        ),
        pytest.param(
            _mr('development', reviewers=[{'id': 10, 'name': 'Alice', 'status': 'rejected'}]),
            'closed',
            id='creator_self_rejection_is_closed',
        ),
        pytest.param(
            _mr('in_review', reviewers=[{'id': 20, 'name': 'Bob', 'status': 'rejected'}]),
            'in_review',
            id='rejection_only_counts_in_development',
        ),
        pytest.param(_mr('development', derivedState='rejected'), 'rejected', id='server_first_override'),
        pytest.param(_mr('development', derivedState='nonsense'), 'in_development', id='unknown_server_value_ignored'),
    ],
)
def test_derive_state(mr: dict[str, Any], expected: str) -> None:
    assert derive_state(mr) == expected


@pytest.mark.parametrize(
    ('mr', 'conflicts', 'expected'),
    [
        pytest.param(_mr('development'), None, [], id='development_nothing_fetched'),
        pytest.param(_mr('development'), [], [], id='development_no_conflicts'),
        pytest.param(_mr('development'), [_conflict()], ['conflicts'], id='conflicts'),
        pytest.param(_mr('in_review'), None, ['approvals'], id='approvals_kept_without_conflict_fetch'),
        pytest.param(_mr('in_review'), [_conflict()], ['conflicts', 'approvals'], id='concurrent_blockers'),
        pytest.param(_mr('in_merge'), None, ['state'], id='in_merge'),
        pytest.param(_mr('published'), None, ['state'], id='published'),
        pytest.param(_mr('canceled'), None, ['state'], id='canceled'),
        pytest.param(_mr('approved'), [], [], id='approved_mergeable'),
        pytest.param(_mr('in_review', mergeBlockers=['state']), [_conflict()], ['state'], id='server_first_override'),
    ],
)
def test_derive_merge_blockers(mr: dict[str, Any], conflicts: list[ConflictRef] | None, expected: list[str]) -> None:
    assert derive_merge_blockers(mr, conflicts) == expected


@pytest.mark.parametrize(
    ('mr', 'expected'),
    [
        pytest.param(_mr('development'), ['request_review', 'merge', 'update', 'resolve_conflicts'], id='development'),
        pytest.param(_mr('in_review'), ['approve', 'request_changes', 'update', 'resolve_conflicts'], id='in_review'),
        pytest.param(_mr('approved'), ['request_changes', 'merge', 'update', 'resolve_conflicts'], id='approved'),
        pytest.param(_mr('in_merge'), ['update'], id='in_merge'),
        pytest.param(_mr('published'), [], id='published'),
        pytest.param(_mr('canceled'), [], id='canceled'),
        pytest.param(_mr('weird'), [], id='unknown_state'),
        pytest.param(_mr('published', allowedActions=['update']), ['update'], id='server_first_override'),
    ],
)
def test_derive_allowed_actions(mr: dict[str, Any], expected: list[str]) -> None:
    assert derive_allowed_actions(mr) == expected


@pytest.mark.parametrize(
    ('mr', 'admin_id', 'expected'),
    [
        pytest.param(_mr(), None, Viewer(is_creator=None, has_approved=None), id='no_admin_block_is_unknown'),
        pytest.param(_mr(), 10, Viewer(is_creator=True, has_approved=False), id='creator'),
        pytest.param(_mr(), '10', Viewer(is_creator=True, has_approved=False), id='creator_str_vs_int'),
        pytest.param(
            _mr(approvals=[{'approverId': '20', 'approverName': 'Bob'}]),
            20,
            Viewer(is_creator=False, has_approved=True),
            id='approver_id_string_on_the_wire',
        ),
        pytest.param(_mr(), 99, Viewer(is_creator=False, has_approved=False), id='bystander'),
        pytest.param(
            _mr(viewer={'isCreator': False, 'hasApproved': True}),
            10,
            Viewer(is_creator=False, has_approved=True),
            id='server_first_override',
        ),
        pytest.param(_mr(viewer={}), 10, Viewer(is_creator=True, has_approved=False), id='empty_server_viewer_ignored'),
    ],
)
def test_derive_viewer(mr: dict[str, Any], admin_id: Any, expected: Viewer) -> None:
    assert derive_viewer(mr, admin_id) == expected


@pytest.mark.parametrize(
    ('row', 'kwargs', 'expected_fragment'),
    [
        pytest.param(
            1,
            {'state': 'published', 'derived_state': 'merged', 'allowed_actions': [], 'merge_blockers': ['state']},
            'source branch is being deleted',
            id='1_merged',
        ),
        pytest.param(
            2, {'state': 'canceled', 'derived_state': 'closed', 'merge_blockers': ['state']}, 'closed', id='2_closed'
        ),
        pytest.param(
            3,
            {
                'state': 'in_merge',
                'derived_state': 'in_merge',
                'merge_blockers': ['state'],
                'allowed_actions': ['update'],
            },
            'Do not edit this branch',
            id='3_in_merge_is_a_warning',
        ),
        pytest.param(
            4,
            {
                'merge_blockers': ['conflicts'],
                'conflicts': [_conflict('a'), _conflict('b')],
                'remaining_conflicts': [_conflict('b')],
            },
            'Resolve the next conflict: keboola.ex-db/b (1 left)',
            id='4_remaining_conflicts',
        ),
        pytest.param(
            5,
            {'merge_blockers': ['conflicts'], 'conflicts': [_conflict()], 'session': PRODUCTION_WRITER},
            "session on branch 'reporting'",
            id='5_conflicts_handoff_from_production',
        ),
        pytest.param(
            6,
            {'merge_blockers': ['conflicts'], 'conflicts': [_conflict('a'), _conflict('b')]},
            '2 conflicts block the merge',
            id='6_conflicts_on_branch',
        ),
        pytest.param(
            7,
            {
                'state': 'in_review',
                'derived_state': 'in_review',
                'merge_blockers': ['approvals'],
                'viewer': Viewer(is_creator=False, has_approved=True),
                'pending': ['Bob'],
            },
            'You already approved; wait for the other reviewers (Bob)',
            id='7_already_approved',
        ),
        pytest.param(
            8,
            {
                'state': 'in_review',
                'derived_state': 'in_review',
                'merge_blockers': ['approvals'],
                'viewer': Viewer(is_creator=True, has_approved=False),
                'pending': ['Bob'],
            },
            'cannot approve your own',
            id='8_creator_waits',
        ),
        pytest.param(
            9,
            {'state': 'in_review', 'derived_state': 'in_review', 'merge_blockers': ['approvals']},
            'Approve it (approve_merge_request) or request changes',
            id='9_reviewer_can_write',
        ),
        pytest.param(
            10,
            {
                'state': 'in_review',
                'derived_state': 'in_review',
                'merge_blockers': ['approvals'],
                'session': ON_BRANCH_READER,
            },
            "Waiting for a reviewer's approval",
            id='10_reader_waits',
        ),
        pytest.param(
            11,
            {'last_refusal': 'not_ready'},
            'request a review (request_merge_request_review)',
            id='11_not_ready_in_development_means_request_review',
        ),
        pytest.param(
            12,
            {'derived_state': 'rejected', 'rejection_reason': 'typo in query'},
            'Changes were requested (typo in query); address them',
            id='12_rejected_before_merge',
        ),
        pytest.param(
            13,
            {'session': PRODUCTION_WRITER},
            "open a session on branch 'reporting' and call merge_merge_request",
            id='13_merge_handoff_from_production',
        ),
        pytest.param(14, {}, 'merge it with merge_merge_request (irreversible', id='14_merge_from_development'),
        pytest.param(15, {'session': ON_BRANCH_READER}, 'read-only', id='15_reader_fallback'),
    ],
)
def test_next_step_rows(row: int, kwargs: dict[str, Any], expected_fragment: str) -> None:
    defaults: dict[str, Any] = {
        'state': 'development',
        'derived_state': 'in_development',
        'merge_blockers': [],
        'conflicts': None,
        'allowed_actions': ['request_review', 'merge', 'update', 'resolve_conflicts'],
        'viewer': UNKNOWN_VIEWER,
        'pending': [],
        'session': ON_BRANCH_WRITER,
        'branch_from_name': 'reporting',
    }
    assert expected_fragment in build_next_step(**{**defaults, **kwargs})


@pytest.mark.parametrize(
    ('kwargs', 'expected_fragment', 'reason'),
    [
        pytest.param(
            {
                'state': 'in_review',
                'derived_state': 'in_merge',
                'merge_blockers': ['state', 'conflicts', 'approvals'],
                'conflicts': [_conflict()],
            },
            'A merge is running',
            'state beats conflicts beats approvals',
            id='precedence_state_first',
        ),
        pytest.param(
            {
                'state': 'in_review',
                'derived_state': 'in_review',
                'merge_blockers': ['conflicts', 'approvals'],
                'conflicts': [_conflict()],
            },
            'block the merge',
            'conflicts beat approvals',
            id='precedence_conflicts_before_approvals',
        ),
        pytest.param(
            {},
            'merge it',
            'from development the recommendation is merge, not request review',
            id='development_recommends_merge_not_review',
        ),
        pytest.param(
            {'last_refusal': None},
            'merge it',
            'without last_refusal the table would loop back to merge',
            id='without_refusal_merge_again',
        ),
        pytest.param(
            {
                'state': 'in_review',
                'derived_state': 'in_review',
                'merge_blockers': ['approvals'],
                'viewer': Viewer(is_creator=None, has_approved=None),
            },
            'Approve it',
            'rows 7/8 fire only on True, not on None',
            id='none_viewer_falls_through_to_row_9',
        ),
        pytest.param(
            {'derived_state': 'rejected', 'session': PRODUCTION_WRITER},
            'Changes were requested',
            'rejected is told before the merge handoff',
            id='rejected_before_merge_rows',
        ),
        pytest.param(
            {'session': SessionContext(on_mr_branch=True, can_write=False)},
            'read-only',
            'no write recommended to a session that cannot perform it',
            id='no_write_for_reader',
        ),
        pytest.param(
            {
                'merge_blockers': ['conflicts'],
                'conflicts': [_conflict()],
                'session': PRODUCTION_WRITER,
                'branch_from_name': None,
            },
            "the merge request's source branch",
            'unknown branch name degrades gracefully',
            id='handoff_without_branch_name',
        ),
    ],
)
def test_next_step_rules(kwargs: dict[str, Any], expected_fragment: str, reason: str) -> None:
    defaults: dict[str, Any] = {
        'state': 'development',
        'derived_state': 'in_development',
        'merge_blockers': [],
        'conflicts': None,
        'allowed_actions': ['request_review', 'merge', 'update', 'resolve_conflicts'],
        'viewer': UNKNOWN_VIEWER,
        'pending': [],
        'session': ON_BRANCH_WRITER,
        'branch_from_name': 'reporting',
    }
    assert expected_fragment in build_next_step(**{**defaults, **kwargs}), reason


@pytest.mark.parametrize(
    ('mr', 'conflicts', 'expected_mergeable', 'expected_blockers'),
    [
        pytest.param(_mr('approved'), None, None, [], id='not_fetched_is_none'),
        pytest.param(_mr('approved'), [], True, [], id='fetched_empty_is_true'),
        pytest.param(_mr('approved'), [_conflict()], False, ['conflicts'], id='conflicts_is_false'),
        pytest.param(_mr('in_review'), [], False, ['approvals'], id='approvals_blocker_even_without_conflicts'),
    ],
)
def test_build_status_mergeable_fail_closed(
    mr: dict[str, Any],
    conflicts: list[ConflictRef] | None,
    expected_mergeable: bool | None,
    expected_blockers: list[str],
) -> None:
    status = build_status(mr, conflicts=conflicts, admin_id=10, session=ON_BRANCH_WRITER, branch_from_name='reporting')

    assert status.mergeable is expected_mergeable
    assert status.merge_blockers == expected_blockers
    assert status.conflicts == conflicts
    assert status.state == mr['state']


def test_build_status_names_and_reason() -> None:
    mr = _mr(
        'development',
        reviewers=[{'id': 20, 'name': 'Bob', 'status': 'rejected'}, {'id': 30, 'name': 'Cid', 'status': 'approved'}],
        approvals=[{'approverId': '30', 'approverName': 'Cid'}],
        activityLog=[
            {'eventType': 'review_requested', 'admin': {'id': 10, 'name': 'Alice'}, 'note': '', 'createdAt': 't1'},
            {
                'eventType': 'changes_requested',
                'admin': {'id': 20, 'name': 'Bob'},
                'note': 'fix the join',
                'createdAt': 't2',
            },
        ],
    )

    status = build_status(mr, conflicts=[], admin_id=10, session=ON_BRANCH_WRITER, branch_from_name='reporting')

    assert status.derived_state == 'rejected'
    assert status.approved_by == ['Cid']
    assert status.pending_reviewers == ['Bob']
    assert status.viewer == Viewer(is_creator=True, has_approved=False)
    assert 'fix the join' in status.next_step
    assert request_changes_reason(_mr()) is None
