"""Static checks of the ChatGPT plugin package in `integrations/chatgpt-plugin/`.

The skills name MCP tools by their exact names, so a renamed or removed tool must fail here rather than in
a ChatGPT conversation. Manifest limits are the ones OpenAI documents for plugin submission.
"""

import json
import re
import struct
from pathlib import Path

import pytest

from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.server import create_server

PLUGIN_DIR = Path(__file__).parents[1] / 'integrations' / 'chatgpt-plugin'
SKILL_NAMES = ('explore-project-data', 'build-sql-transformation', 'debug-failed-job', 'build-flow')

# Backticked snake_case words in the skills that are tool arguments or result fields, not tools.
NON_TOOL_IDENTIFIERS = frozenset({'block_id', 'code_id', 'read_only', 'scope_token'})
_SNAKE_CASE_IN_BACKTICKS = re.compile(r'`([a-z]+(?:_[a-z0-9]+)+)`')
_SKILL_REFERENCE = re.compile(r'`([a-z]+(?:-[a-z]+)+)` skill')


def _load_json(name: str) -> dict:
    return json.loads((PLUGIN_DIR / name).read_text(encoding='utf-8'))


def _png_size(path: Path) -> tuple[int, int]:
    data = path.read_bytes()
    assert data[:8] == b'\x89PNG\r\n\x1a\n', f'{path.name} is not a PNG'
    return struct.unpack('>II', data[16:24])


def _split_skill(skill_name: str) -> tuple[dict[str, str], str]:
    text = (PLUGIN_DIR / 'skills' / skill_name / 'SKILL.md').read_text(encoding='utf-8')
    match = re.match(r'---\n(.*?)\n---\n(.*)', text, re.DOTALL)
    assert match, f'{skill_name}/SKILL.md has no frontmatter'
    frontmatter = dict(line.split(': ', 1) for line in match.group(1).splitlines())
    return frontmatter, match.group(2)


async def _registered_tools() -> dict[str, bool]:
    """Maps each registered tool name to whether it is read-only."""
    server = create_server(Config(), runtime_info=ServerRuntimeInfo(transport='stdio'))
    tools = await server.list_tools(run_middleware=False)
    return {t.name: bool(t.annotations and t.annotations.read_only_hint) for t in tools}


def test_plugin_manifest() -> None:
    manifest = _load_json('plugin.json')
    assert manifest['$schema'] == 'https://agent-plugins.org/schemas/1.0.0/plugin.schema.json'
    assert manifest['name'] == 'keboola'
    assert re.fullmatch(r'\d+\.\d+\.\d+', manifest['version'])

    openai = manifest['extensions']['com.openai']
    # Apps and lifecycle hooks make a plugin ineligible for the public directory.
    assert 'apps' not in openai
    assert 'hooks' not in openai
    assert not (PLUGIN_DIR / '.app.json').exists()
    assert not (PLUGIN_DIR / 'hooks').exists()

    interface = openai['interface']
    for key, limit in (('displayName', 30), ('shortDescription', 30), ('longDescription', 4000)):
        assert 0 < len(interface[key]) <= limit, f'interface.{key} must have 1-{limit} characters'
    assert interface['developerName'] == 'Keboola'
    assert interface['category']
    for key in ('websiteURL', 'supportURL', 'privacyPolicyURL', 'termsOfServiceURL'):
        assert interface[key].startswith('https://'), f'interface.{key} must be an HTTPS URL'
    assert re.fullmatch(r'#[0-9A-F]{6}', interface['brandColor'])
    assert len(interface['defaultPrompt']) <= 3
    assert all(len(prompt) <= 128 and '@' not in prompt for prompt in interface['defaultPrompt'])

    for key in ('composerIcon', 'logo'):
        assert interface[key].startswith('./'), f'interface.{key} must be relative to the plugin root'
        width, height = _png_size(PLUGIN_DIR / interface[key])
        assert width == height >= 48, f'interface.{key} must be square and at least 48x48'


def test_mcp_config() -> None:
    config = _load_json('mcp.json')
    assert config['$schema'] == 'https://agent-plugins.org/schemas/1.0.0/mcp.schema.json'
    # A plugin has exactly one MCP server; the public listing freezes its URL (RFC Phase 4 switches it).
    assert list(config['mcpServers']) == ['keboola']
    server = config['mcpServers']['keboola']
    assert server['type'] == 'streamable-http'
    assert server['url'] == 'https://mcp.keboola.com/mcp'
    assert server['extensions']['com.openai']['auth'] == {'type': 'oauth', 'client': {'mode': 'dcr'}}


def test_skill_directories() -> None:
    assert sorted(p.name for p in (PLUGIN_DIR / 'skills').iterdir()) == sorted(SKILL_NAMES)


@pytest.mark.asyncio
@pytest.mark.parametrize('skill_name', SKILL_NAMES)
async def test_skill_names_real_tools_and_scopes_before_writes(skill_name: str) -> None:
    frontmatter, body = _split_skill(skill_name)
    assert frontmatter['name'] == skill_name
    assert frontmatter['description']

    for referenced_skill in _SKILL_REFERENCE.findall(body):
        assert referenced_skill in SKILL_NAMES, f'{skill_name} refers to an unknown skill {referenced_skill}'

    tools = await _registered_tools()
    mentions = [
        (match.start(), match.group(1))
        for match in _SNAKE_CASE_IN_BACKTICKS.finditer(body)
        if match.group(1) not in NON_TOOL_IDENTIFIERS
    ]
    unknown = sorted({name for _, name in mentions if name not in tools})
    assert not unknown, f'{skill_name} names tools the server does not register: {unknown}'

    # The project-less grant reads across projects but writes only to the confirmed one, so every skill
    # lists projects, then scopes the session, before it names any write tool.
    first_mention = {}
    for position, name in mentions:
        first_mention.setdefault(name, position)
    assert 'get_accessible_projects' in first_mention, f'{skill_name} does not call get_accessible_projects'
    assert 'set_project_scope' in first_mention, f'{skill_name} does not call set_project_scope'
    assert first_mention['get_accessible_projects'] < first_mention['set_project_scope']
    writes = [position for name, position in first_mention.items() if not tools[name]]
    if writes:
        assert first_mention['set_project_scope'] < min(writes), f'{skill_name} names a write tool before scoping'
