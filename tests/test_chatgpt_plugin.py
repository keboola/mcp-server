"""Static checks of the ChatGPT plugin package in `integrations/chatgpt-plugin/`.

The skills name MCP tools and their arguments exactly, so a renamed or removed tool must fail here rather than in
a ChatGPT conversation. Manifest limits are the ones OpenAI documents for plugin submission.
"""

import asyncio
import json
import re
import struct
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastmcp.tools import Tool

from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.mcp import ToolsFilteringMiddleware, is_read_only_tool, is_semantic_tool
from keboola_mcp_server.multiproject import MultiProjectMiddleware
from keboola_mcp_server.scope import SCOPE_KEY, SessionScope
from keboola_mcp_server.server import create_server

PLUGIN_DIR = Path(__file__).parents[1] / 'integrations' / 'chatgpt-plugin'
SKILL_NAMES = ('explore-project-data', 'build-sql-transformation', 'debug-failed-job', 'build-flow')

# Backticked lowercase words in the skills that are result fields or operation names, not tools.
NON_TOOL_IDENTIFIERS = frozenset(
    {
        'block_id',
        'code_id',
        'id',
        'name',
        'project_id',
        'read_only',
        'scope_token',
        'set_code',
        'str_replace',
        'toolset_restrictions',
    }
)
_BACKTICK_SPAN = re.compile(r'`[^`]+`')
# A tool is a whole span (`run_job`) or a call written out in one (`run_job(component_id=...)`).
_TOOL_IN_SPAN = re.compile(r'`([a-z]+(?:_[a-z0-9]+)*)[`(\s]')
# An argument starts a span (`component_id=...`) or follows the bracket or a comma of a call in one.
_ARGUMENT_IN_SPAN = re.compile(r'(?:^`|[(,]\s*)(\w+)=')
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


def _oauth_denial(tool: Tool) -> str | None:
    """The reason an OAuth session (the only way ChatGPT connects) cannot call the tool, or None.

    Checked for a write role only: under OAuth the role matters just for `readonly`, where the skills stop. The
    `create_flow` and `create_conditional_flow` tools depend on the project's conditional-flows feature, so a tool
    passes when either feature state allows it.
    """
    denials = [
        ToolsFilteringMiddleware.authorize_tool_call(
            tool_name=tool.name,
            is_read_only=is_read_only_tool(tool),
            # Most projects have no semantic models, so a skill must not depend on semantic tools.
            is_semantic=is_semantic_tool(tool),
            has_semantic_models=False,
            token_role='admin',
            features=features,
            is_oauth=True,
            is_main_branch=True,
        )
        for features in (set(), {'hide-conditional-flows'})
    ]
    return denials[0] if all(denials) else None


@pytest.fixture(scope='module')
def tools() -> dict[str, Tool]:
    server = create_server(Config(), runtime_info=ServerRuntimeInfo(transport='stdio'))
    registered = asyncio.run(server.list_tools(run_middleware=False))

    async def call_next(_context: object) -> list[Tool]:
        return registered

    # With 2+ projects scoped, the middleware adds the `project_ids` filter to fan-out read tools.
    state = {SCOPE_KEY: SessionScope(project_ids=[1, 2], confirmed=True)}
    context = SimpleNamespace(fastmcp_context=SimpleNamespace(session=SimpleNamespace(state=state)))
    listed = asyncio.run(MultiProjectMiddleware().on_list_tools(context, call_next))
    return {t.name: t for t in listed}


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
    # A plugin has exactly one MCP server, and the public listing freezes its URL: the global router serves every stack.
    assert list(config['mcpServers']) == ['keboola']
    server = config['mcpServers']['keboola']
    assert server['type'] == 'streamable-http'
    assert server['url'] == 'https://mcp-router.keboola.com/mcp'
    assert server['extensions']['com.openai']['auth'] == {'type': 'oauth', 'client': {'mode': 'dcr'}}


def test_skill_directories() -> None:
    assert sorted(p.name for p in (PLUGIN_DIR / 'skills').iterdir()) == sorted(SKILL_NAMES)


@pytest.mark.parametrize('skill_name', SKILL_NAMES)
def test_skill_names_callable_tools_and_scopes_before_writes(skill_name: str, tools: dict[str, Tool]) -> None:
    frontmatter, body = _split_skill(skill_name)
    assert frontmatter['name'] == skill_name
    assert frontmatter['description']

    for referenced_skill in _SKILL_REFERENCE.findall(body):
        assert referenced_skill in SKILL_NAMES, f'{skill_name} refers to an unknown skill {referenced_skill}'

    spans = list(_BACKTICK_SPAN.finditer(body))
    mentions = [
        (span.start(), match.group(1))
        for span in spans
        if (match := _TOOL_IN_SPAN.match(span.group())) and match.group(1) not in NON_TOOL_IDENTIFIERS
    ]
    named_tools = {name for _, name in mentions}
    unknown = sorted(named_tools - tools.keys())
    assert not unknown, f'{skill_name} names tools the server does not register: {unknown}'
    denied = {name: reason for name in sorted(named_tools) if (reason := _oauth_denial(tools[name]))}
    assert not denied, f'{skill_name} names tools an OAuth session cannot call: {denied}'

    # A tool named in plain text would skip every check here. Single-word names (`search`) are also English words.
    plain_text = _BACKTICK_SPAN.sub(lambda span: ' ' * len(span.group()), body)
    unquoted = sorted(name for name in tools if '_' in name and re.search(rf'\b{name}\b', plain_text))
    assert not unquoted, f'{skill_name} names tools outside backticks: {unquoted}'

    # The skills write each argument after the tool it belongs to ("call `run_job` with `configuration_id=...`").
    wrong_arguments = []
    arguments_by_tool: dict[str, set[str]] = {}
    for span in spans:
        for match in _ARGUMENT_IN_SPAN.finditer(span.group()):
            owners = [name for position, name in mentions if position <= span.start() + match.start()]
            assert owners, f'{skill_name} names the argument {match.group(1)} before any tool'
            arguments_by_tool.setdefault(owners[-1], set()).add(match.group(1))
            if match.group(1) not in tools[owners[-1]].parameters.get('properties', {}):
                wrong_arguments.append(f'{owners[-1]}({match.group(1)})')
    assert not wrong_arguments, f'{skill_name} names arguments the tool does not take: {wrong_arguments}'

    # Under OAuth the scope is shared by every conversation on the login, so each write a step calls names its
    # target project. Rules only mention tools ("before every `run_job`"), so they are not checked.
    steps_start = body.index('\n## Steps\n')
    steps_end = body.find('\n## ', steps_start + 1)
    steps_end = len(body) if steps_end < 0 else steps_end
    unpinned = sorted(
        {
            name
            for position, name in mentions
            if steps_start < position < steps_end
            and not is_read_only_tool(tools[name])
            and 'project_id' not in arguments_by_tool.get(name, set())
        }
    )
    assert not unpinned, f'{skill_name} names write tools without project_id: {unpinned}'

    # The skills narrow writes to the project the user confirmed (a prompt-level rule, see the package README),
    # so every skill lists projects, then scopes the session, before it names any write tool.
    first_mention: dict[str, int] = {}
    for position, name in mentions:
        first_mention.setdefault(name, position)
    assert 'get_accessible_projects' in first_mention, f'{skill_name} does not call get_accessible_projects'
    assert 'set_project_scope' in first_mention, f'{skill_name} does not call set_project_scope'
    assert first_mention['get_accessible_projects'] < first_mention['set_project_scope']
    writes = [position for name, position in first_mention.items() if not is_read_only_tool(tools[name])]
    if writes:
        assert first_mention['set_project_scope'] < min(writes), f'{skill_name} names a write tool before scoping'
