import pytest

from keboola_mcp_server.cli import parse_args


@pytest.mark.parametrize(
    ('args', 'expected'),
    [
        (['--workspace-id', '123'], '123'),
        ([], None),
    ],
    ids=['workspace_id_set', 'workspace_id_defaults_to_none'],
)
def test_parse_args_workspace_id(args: list[str], expected: str | None) -> None:
    parsed = parse_args(args)
    assert parsed.workspace_id == expected
