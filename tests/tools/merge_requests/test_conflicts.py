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
                '/rows/r1/configuration/x': ('ours', None, 1, 2, 1),
                '/rows/r2': ('theirs', None, None, None, {'id': 'r2', 'configuration': {}}),
            },
            id='rows_with_ids_align_by_id',
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


def test_rows_with_ids_align_by_id_not_index() -> None:
    base = _side(1, rows=[{'id': 'r1', 'configuration': {'x': 1}}, {'id': 'r2', 'configuration': {'x': 2}}])
    ours = _side(
        3,
        rows=[
            {'id': 'r0', 'configuration': {}},
            {'id': 'r1', 'configuration': {'x': 1}},
            {'id': 'r2', 'configuration': {'x': 2}},
        ],
    )
    theirs = _side(5, rows=[{'id': 'r1', 'configuration': {'x': 1}}, {'id': 'r2', 'configuration': {'x': 99}}])

    conflict = build_config_conflict(_ref(), {'base': base, 'ours': ours, 'theirs': theirs})

    changes = _by_path(conflict.changes)
    assert set(changes) == {'/rows/r0', '/rows/r2/configuration/x'}
    assert changes['/rows/r0'].changed_by == 'ours'
    assert changes['/rows/r2/configuration/x'].changed_by == 'theirs'
    assert conflict.conflicting_paths == []  # an insertion is not a conflict with an edit of another row


@pytest.mark.parametrize(
    ('base_rows', 'ours_rows', 'theirs_rows', 'expected_paths', 'expected_conflicting'),
    [
        pytest.param(
            [{'id': 'r1', 'configuration': {'x': 1}}],
            [{'id': 'r1', 'configuration': {'x': 2}}],
            [],
            {'/rows/r1/configuration/x': 'ours', '/rows/r1': 'theirs'},
            ['/rows/r1', '/rows/r1/configuration/x'],
            id='modify_vs_delete_all_is_a_conflict',
        ),
        pytest.param(
            [],
            [{'id': 'r1', 'configuration': {}}],
            [{'id': 'r2', 'configuration': {}}],
            {'/rows/r1': 'ours', '/rows/r2': 'theirs'},
            [],
            id='two_different_insertions_on_empty_base_are_not_a_conflict',
        ),
    ],
)
def test_empty_rows_side_is_still_aligned_by_id(
    base_rows: list, ours_rows: list, theirs_rows: list, expected_paths: dict[str, str], expected_conflicting: list[str]
) -> None:
    diff = {'base': _side(1, rows=base_rows), 'ours': _side(3, rows=ours_rows), 'theirs': _side(5, rows=theirs_rows)}

    conflict = build_config_conflict(_ref(), diff)

    assert {c.path: c.changed_by for c in conflict.changes} == expected_paths
    assert conflict.conflicting_paths == expected_conflicting


def test_subtree_vs_leaf_edit_is_a_conflict() -> None:
    base = _side(1, configuration={'parameters': {'a': 1}})
    ours = _side(3, configuration={})  # removed the whole subtree
    theirs = _side(5, configuration={'parameters': {'a': 2}})  # edited inside it

    conflict = build_config_conflict(_ref(), {'base': base, 'ours': ours, 'theirs': theirs})

    assert {c.path: c.changed_by for c in conflict.changes} == {
        '/configuration/parameters': 'ours',
        '/configuration/parameters/a': 'theirs',
    }
    assert conflict.conflicting_paths == ['/configuration/parameters', '/configuration/parameters/a']
    assert conflict.suggested_take is None


@pytest.mark.parametrize(
    ('base_items', 'ours_items', 'theirs_items', 'expected_changes', 'expected_conflicting'),
    [
        pytest.param(
            [{'query': 'a'}, {'query': 'b'}],
            [{'query': 'x'}, {'query': 'b'}],
            [{'query': 'y'}, {'query': 'b'}],
            {'/configuration/tables/0/query': ('both', 'x', 'y')},
            ['/configuration/tables/0/query'],
            id='lists_without_ids_walk_by_index',
        ),
        pytest.param(
            [{'id': 5, 'query': 'a'}],
            [{'id': 5, 'query': 'b'}],
            [{'id': 5, 'query': 'c'}, {'id': 6, 'query': 'z'}],
            {
                '/configuration/tables/5/query': ('both', 'b', 'c'),
                '/configuration/tables/6': ('theirs', None, {'id': 6, 'query': 'z'}),
            },
            ['/configuration/tables/5/query'],
            id='id_keyed_on_both_pairs_finds_the_clash',
        ),
        # KNOWN GAP (DMD-2246): keying is decided per pair (base->ours, base->theirs), not once per list. When one
        # side breaks the id rule, the same edited row lands under an id path on one side and an index path on the
        # other: the real clash on `query` is reported as two one-sided rows (each showing the other side unchanged)
        # and `conflicting_paths` comes back empty. Pinned here so a fix is a deliberate, visible behaviour change.
        pytest.param(
            [{'id': 5, 'query': 'a'}],
            [{'id': 5, 'query': 'b'}],
            [{'id': 5, 'query': 'c'}, {'query': 'z'}],
            {
                '/configuration/tables/0/query': ('theirs', 'a', 'c'),
                '/configuration/tables/1': ('theirs', None, {'query': 'z'}),
                '/configuration/tables/5/query': ('ours', 'b', 'a'),
            },
            [],
            id='known_gap_one_side_adds_item_without_id',
        ),
        pytest.param(
            [{'id': 5, 'query': 'a'}],
            [{'id': 5, 'query': 'b'}],
            [{'id': 5, 'query': 'c'}, {'id': 5, 'query': 'z'}],
            {
                '/configuration/tables/0/query': ('theirs', 'a', 'c'),
                '/configuration/tables/1': ('theirs', None, {'id': 5, 'query': 'z'}),
                '/configuration/tables/5/query': ('ours', 'b', 'a'),
            },
            [],
            id='known_gap_one_side_duplicates_an_id',
        ),
        # The other face of the same gap: an id path and an index path of different rows can share a segment.
        pytest.param(
            [{'id': 1, 'query': 'a'}, {'id': 2, 'query': 'b'}],
            [{'id': 1, 'query': 'a'}, {'id': 2, 'query': 'x'}],
            [{'id': 1, 'query': 'a'}, {'id': 2, 'query': 'b'}, {'query': 'new'}],
            {
                '/configuration/tables/2': ('theirs', None, {'query': 'new'}),
                '/configuration/tables/2/query': ('ours', 'x', 'b'),
            },
            ['/configuration/tables/2', '/configuration/tables/2/query'],
            id='known_gap_id_path_collides_with_index_path',
        ),
    ],
)
def test_list_keying_across_sides(
    base_items: list,
    ours_items: list,
    theirs_items: list,
    expected_changes: dict[str, tuple[str, Any, Any]],
    expected_conflicting: list[str],
) -> None:
    """Characterizes how lists inside `configuration` are aligned when the sides disagree on the id rule."""
    diff = {
        'base': _side(1, configuration={'tables': base_items}),
        'ours': _side(3, configuration={'tables': ours_items}),
        'theirs': _side(5, configuration={'tables': theirs_items}),
    }

    conflict = build_config_conflict(_ref(), diff)

    assert {c.path: (c.changed_by, c.ours, c.theirs) for c in conflict.changes} == expected_changes
    assert conflict.conflicting_paths == expected_conflicting


def test_envelope_holes() -> None:
    assert envelope_holes(_side()) == []
    assert envelope_holes({'diff': {}}) == ['name', 'rows', 'configuration', 'isDisabled']
    assert envelope_holes({'diff': {'name': 'n', 'rows': [], 'configuration': {}, 'isDisabled': False}}) == []
    assert envelope_holes({'diff': {'name': 'n'}}) == ['rows', 'configuration', 'isDisabled']
