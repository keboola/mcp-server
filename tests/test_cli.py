from keboola_mcp_server.cli import parse_args


def test_parse_args_workspace_id() -> None:
    parsed = parse_args(['--workspace-id', '123'])
    assert parsed.workspace_id == '123'


def test_parse_args_workspace_id_defaults_to_none() -> None:
    parsed = parse_args([])
    assert parsed.workspace_id is None
