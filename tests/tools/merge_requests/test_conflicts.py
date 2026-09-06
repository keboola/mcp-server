from typing import Any

import pytest

from keboola_mcp_server.tools.merge_requests.conflicts import (
    build_config_conflict,
    classify_three_way,
    diff_warnings,
    envelope_holes,
    suggest_take,
)
from keboola_mcp_server.tools.merge_requests.models import ConflictRef, PathChange


def _side(version: int = 1, is_deleted: bool = False, **content: Any) -> dict[str, Any]:
    envelope: dict[str, Any] = {
        'name': 'My config',
        'description': None,
        'changeDescription': 'v',
        'isDisabled': False,
        'configuration': {'parameters': {'query': 'SELECT 1', 'limit': 10}},
        'rows': [],
    }
    envelope.update(content)
    return {'version': version, 'isDeleted': is_deleted, 'diff': envelope}


def _ref() -> ConflictRef:
    return ConflictRef(
        component_id='keboola.ex-db',
        configuration_id='42',
        message='changed on both sides',
        is_deleted=False,
        dev_branch_version_identifier='a',
        default_branch_version_identifier='b',
    )


def _by_path(changes: list[PathChange]) -> dict[str, PathChange]:
    return {c.path: c for c in changes}


@pytest.mark.parametrize(
    ('diff', 'expected'),
    [
        pytest.param(
            {
                'base': _side(1),
                'ours': _side(3, configuration={'parameters': {'query': 'SELECT 2', 'limit': 10}}),
                'theirs': _side(5, configuration={'parameters': {'query': 'SELECT 1', 'limit': 20}}),
            },
            {
                '/configuration/parameters/query': ('ours', None, 'SELECT 1', 'SELECT 2', 'SELECT 1'),
                '/configuration/parameters/limit': ('theirs', None, 10, 10, 20),
            },
            id='disjoint_paths',
        ),
        pytest.param(
            {
                'base': _side(1),
                'ours': _side(3, configuration={'parameters': {'query': 'SELECT 2', 'limit': 10}}),
                'theirs': _side(5, configuration={'parameters': {'query': 'SELECT 3', 'limit': 10}}),
            },
            {'/configuration/parameters/query': ('both', False, 'SELECT 1', 'SELECT 2', 'SELECT 3')},
            id='both_disagree',
        ),
        pytest.param(
            {
                'base': _side(1),
                'ours': _side(3, configuration={'parameters': {'query': 'SELECT 2', 'limit': 10}}),
                'theirs': _side(5, configuration={'parameters': {'query': 'SELECT 2', 'limit': 10}}),
            },
            {'/configuration/parameters/query': ('both', True, 'SELECT 1', 'SELECT 2', 'SELECT 2')},
            id='both_agree',
        ),
        pytest.param(
            {
                'base': _side(1),
                'ours': _side(3, name='Renamed', isDisabled=True),
                'theirs': _side(5, description='Documented'),
            },
            {
                '/name': ('ours', None, 'My config', 'Renamed', 'My config'),
                '/isDisabled': ('ours', None, False, True, False),
                '/description': ('theirs', None, None, None, 'Documented'),
            },
            id='pseudo_paths_name_description_isDisabled',
        ),
        pytest.param(
            {
                'base': _side(1),
                'ours': _side(3, configuration={'parameters': {'query': 'SELECT 1'}}),
                'theirs': _side(5, configuration={'parameters': {'query': 'SELECT 1', 'limit': 10, 'extra': 1}}),
            },
            {
                '/configuration/parameters/limit': ('ours', None, 10, None, 10),
                '/configuration/parameters/extra': ('theirs', None, None, None, 1),
            },
            id='removed_and_added_keys',
        ),
        pytest.param(
            {
                'base': _side(1, rows=[{'id': 'r1', 'configuration': {'x': 1}}]),
                'ours': _side(3, rows=[{'id': 'r1', 'configuration': {'x': 2}}]),
                'theirs': _side(5, rows=[{'id': 'r1', 'configuration': {'x': 1}}, {'id': 'r2', 'configuration': {}}]),
            },
            {
                '/rows/0/configuration/x': ('ours', None, 1, 2, 1),
                '/rows/1': ('theirs', None, None, None, {'id': 'r2', 'configuration': {}}),
            },
            id='rows_by_index',
        ),
        pytest.param(
            {'base': None, 'ours': _side(1, name='A'), 'theirs': _side(2, name='A')},
            {
                '/name': ('both', True, None, 'A', 'A'),
                '/isDisabled': ('both', True, None, False, False),
                '/configuration': (
                    'both',
                    True,
                    None,
                    {'parameters': {'query': 'SELECT 1', 'limit': 10}},
                    {'parameters': {'query': 'SELECT 1', 'limit': 10}},
                ),
                '/rows': ('both', True, None, [], []),
                '/description': ('both', True, None, None, None),
            },
            id='null_base_created_on_both_sides',
        ),
    ],
)
def test_classify_three_way(diff: dict[str, Any], expected: dict[str, tuple[Any, ...]]) -> None:
    changes = _by_path(classify_three_way(diff))

    assert set(changes) == set(expected)
    for path, (changed_by, agreed, base, ours, theirs) in expected.items():
        change = changes[path]
        assert (change.changed_by, change.agreed, change.base, change.ours, change.theirs) == (
            changed_by,
            agreed,
            base,
            ours,
            theirs,
        ), path


@pytest.mark.parametrize(
    ('diff', 'expected_ours_deleted', 'expected_theirs_deleted', 'expected_warnings'),
    [
        pytest.param(
            {'base': _side(1), 'ours': _side(2, is_deleted=True), 'theirs': _side(3)}, True, False, 0, id='ours_deleted'
        ),
        pytest.param(
            {'base': _side(1), 'ours': _side(2), 'theirs': _side(3, is_deleted=True)},
            False,
            True,
            0,
            id='theirs_deleted',
        ),
        pytest.param({'base': _side(1), 'ours': None, 'theirs': _side(3)}, None, False, 0, id='ours_absent'),
        pytest.param(
            {'base': _side(1), 'ours': {'version': 2, 'isDeleted': False, 'diff': {}}, 'theirs': _side(3)},
            False,
            False,
            1,
            id='ours_empty_envelope_is_a_hole',
        ),
        pytest.param(
            {'base': _side(1), 'ours': _side(2), 'theirs': {'version': 3, 'isDeleted': False, 'diff': {'name': 'x'}}},
            False,
            False,
            1,
            id='theirs_holed_envelope',
        ),
    ],
)
def test_unclassifiable_sides_yield_no_changes(
    diff: dict[str, Any],
    expected_ours_deleted: bool | None,
    expected_theirs_deleted: bool | None,
    expected_warnings: int,
) -> None:
    conflict = build_config_conflict(_ref(), diff)

    assert conflict.changes == []
    assert conflict.conflicting_paths == []
    assert conflict.suggested_take is None
    assert conflict.ours_deleted is expected_ours_deleted
    assert conflict.theirs_deleted is expected_theirs_deleted
    assert len(diff_warnings(diff)) == expected_warnings


@pytest.mark.parametrize(
    ('changed_by_list', 'expected'),
    [
        pytest.param([], None, id='empty_is_none'),
        pytest.param(['ours', 'ours'], 'ours', id='only_ours'),
        pytest.param(['theirs'], 'theirs', id='only_theirs'),
        pytest.param(['ours', 'theirs'], None, id='disjoint_but_both_sides_changed'),
        pytest.param(['ours', 'both'], None, id='collision'),
    ],
)
def test_suggest_take(changed_by_list: list[str], expected: str | None) -> None:
    changes = [
        PathChange(path=f'/p{i}', changed_by=cb, agreed=False if cb == 'both' else None, base=1, ours=2, theirs=3)
        for i, cb in enumerate(changed_by_list)
    ]
    assert suggest_take(changes) == expected


def test_build_config_conflict_full() -> None:
    diff = {
        'base': _side(1),
        'ours': _side(3, configuration={'parameters': {'query': 'SELECT 2', 'limit': 10}}),
        'theirs': _side(5, configuration={'parameters': {'query': 'SELECT 3', 'limit': 20}}),
    }

    conflict = build_config_conflict(_ref(), diff)

    assert conflict.component_id == 'keboola.ex-db'
    assert conflict.theirs is not None and conflict.theirs.version == 5
    assert conflict.ours is not None and conflict.ours.configuration == {
        'parameters': {'query': 'SELECT 2', 'limit': 10}
    }
    assert conflict.base is not None and conflict.base.rows == []
    assert conflict.conflicting_paths == ['/configuration/parameters/query']
    assert conflict.suggested_take is None
    assert [c.changed_by for c in conflict.changes] == ['theirs', 'both']


def test_envelope_holes() -> None:
    assert envelope_holes(_side()) == []
    assert envelope_holes({'diff': {}}) == ['name', 'rows', 'configuration', 'isDisabled']
    assert envelope_holes({'diff': {'name': 'n', 'rows': [], 'configuration': {}, 'isDisabled': False}}) == []
    assert envelope_holes({'diff': {'name': 'n'}}) == ['rows', 'configuration', 'isDisabled']
