import logging
import os
import subprocess
import uuid
from collections.abc import AsyncGenerator, Mapping
from pathlib import Path
from typing import Any, cast

import pytest
import pytest_asyncio
import toon_format
from fastmcp import Client, FastMCP

from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID, KeboolaClient, get_metadata_property
from keboola_mcp_server.config import Config, MetadataField, ServerRuntimeInfo
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.data_apps import (
    _DEFAULT_PACKAGES,
    DataApp,
    DataAppSummary,
    GetDataAppsOutput,
    ModifiedDataAppOutput,
    ModifiedPythonJsDataAppOutput,
    _get_query_function_code,
)
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)


@pytest.fixture
def streamlit_app_imports() -> str:
    return 'import streamlit as st\n\n'


@pytest.fixture
def streamlit_app_entrypoint() -> str:
    return (
        'def main():\n'
        "    st.title('Integration Test Data App')\n"
        "    st.write('Hello from integration test')\n"
        '    # Optionally query data (kept commented to avoid side-effects during tests)\n'
        "    # df = query_data('select 1 as col')\n"
        '    # st.dataframe(df)\n\n'
        'if __name__ == "__main__":\n'
        '    main()\n'
    )


@pytest.fixture
def sample_streamlit_app(streamlit_app_imports: str, streamlit_app_entrypoint: str) -> str:
    """Return a minimal Streamlit app template that supports query injection."""
    return f'{streamlit_app_imports}' '{QUERY_DATA_FUNCTION}\n\n' f'{streamlit_app_entrypoint}'


@pytest.fixture
def mcp_server(storage_api_url: str, storage_api_token: str, workspace_schema: str) -> FastMCP:
    config = Config(storage_api_url=storage_api_url, storage_token=storage_api_token, workspace_schema=workspace_schema)
    mcp_server = create_server(config, runtime_info=ServerRuntimeInfo(transport='stdio'))
    assert isinstance(mcp_server, FastMCP)
    return mcp_server


@pytest_asyncio.fixture
async def mcp_client(mcp_server: FastMCP) -> AsyncGenerator[Client, None]:
    async with Client(mcp_server) as client:
        yield client


@pytest.fixture
def app_name() -> str:
    unique_suffix = uuid.uuid4().hex[:8]
    return f'Integration Test Data App {unique_suffix}'


@pytest.fixture
def app_description() -> str:
    return 'Data app created by integration test'


@pytest_asyncio.fixture
async def initial_data_app(
    mcp_client: Client,
    keboola_client: KeboolaClient,
    app_name: str,
    app_description: str,
    sample_streamlit_app: str,
) -> AsyncGenerator[ModifiedDataAppOutput, None]:
    sync_output: ModifiedDataAppOutput | None = None
    try:
        # Create
        created_result = await mcp_client.call_tool(
            name='modify_streamlit_data_app',
            arguments={
                'name': app_name,
                'description': app_description,
                'source_code': sample_streamlit_app,
                'packages': ['numpy', 'streamlit'],
                'authentication_type': 'no-auth',
            },
        )
        assert created_result.structured_content is not None
        sync_output = ModifiedDataAppOutput.model_validate(created_result.structured_content)
        yield sync_output
    finally:
        if sync_output:
            try:
                # Delete the data app from the data science API and the configuration from the storage API as well.
                await keboola_client.data_science_client.delete_data_app(sync_output.data_app.data_app_id)
            except Exception as e:
                LOG.error(f'Error deleting data app: {e}')
        else:
            LOG.error('No data app to delete')


@pytest.mark.asyncio
async def test_get_data_apps_listing(mcp_client: Client, initial_data_app: ModifiedDataAppOutput) -> None:
    """Test listing data apps returns valid TOON formatted output."""
    tool_result = await mcp_client.call_tool(name='get_data_apps', arguments={})

    # Verify structured content
    assert tool_result.structured_content is not None
    apps = GetDataAppsOutput.model_validate(tool_result.structured_content)
    assert len(apps.data_apps) > 0

    # Verify TOON formatted text content matches structured content
    assert len(tool_result.content) == 1
    assert tool_result.content[0].type == 'text'
    toon_decoded = GetDataAppsOutput.model_validate(toon_format.decode(tool_result.content[0].text))
    assert toon_decoded == apps


@pytest.mark.asyncio
async def test_data_app_lifecycle(
    mcp_client: Client,
    keboola_client: KeboolaClient,
    workspace_manager: WorkspaceManager,
    app_name: str,
    app_description: str,
    initial_data_app: ModifiedDataAppOutput,
    streamlit_app_imports: str,
    streamlit_app_entrypoint: str,
) -> None:
    """
    End-to-end lifecycle for data apps:
    Starts with a created app.
    - get details and list of created app
    - update app
    - get details and list of updated app
    Always deletes the data app in teardown.
    """

    # Check created app basic details
    assert initial_data_app.response == 'created'
    data_app_id = initial_data_app.data_app.data_app_id
    configuration_id = initial_data_app.data_app.configuration_id
    assert data_app_id
    assert configuration_id

    # Verify the metadata - check that KBC.MCP.createdBy is set to 'true'
    metadata = await keboola_client.storage_client.configuration_metadata_get(
        component_id=DATA_APP_COMPONENT_ID, configuration_id=configuration_id
    )
    assert isinstance(metadata, list)
    metadata_dict = {item['key']: item['value'] for item in metadata if isinstance(item, dict)}
    assert MetadataField.CREATED_BY_MCP in metadata_dict
    assert metadata_dict[MetadataField.CREATED_BY_MCP] == 'true'

    # Check created app details by configuration_id
    details_result = await mcp_client.call_tool(
        name='get_data_apps', arguments={'configuration_ids': [configuration_id]}
    )
    assert details_result.structured_content is not None
    details = GetDataAppsOutput.model_validate(details_result.structured_content)
    assert len(details.data_apps) == 1
    data_app_details = details.data_apps[0]
    assert isinstance(data_app_details, DataApp)

    assert data_app_details.configuration_id == configuration_id
    assert data_app_details.data_app_id == data_app_id
    assert data_app_details.name == app_name
    assert data_app_details.description == app_description
    # Check code and code injection
    data_app_details_parameters = data_app_details.configuration.get('parameters') or {}
    assert streamlit_app_imports in data_app_details_parameters['script'][0]
    assert streamlit_app_entrypoint in data_app_details_parameters['script'][0]
    sql_dialect = await workspace_manager.get_sql_dialect()
    assert _get_query_function_code(sql_dialect) in data_app_details_parameters['script'][0]
    # Check packages
    assert set(data_app_details_parameters['packages']) == set(['numpy', 'streamlit'] + _DEFAULT_PACKAGES)

    # Check listing contains our app
    # TODO(REMOVE): Set the limit back to the default value once DSAPI is fixed. The limit is temporarily increased to
    # 500 to prevent listing only the leftover data apps from previous tests (100). These apps cannot be deleted
    # because their configurations were removed in SAPI first, causing the DSAPI delete endpoint to return a 500 error
    # afterward.
    listed_result = await mcp_client.call_tool(name='get_data_apps', arguments={'limit': 500})
    assert listed_result.structured_content is not None
    listed = GetDataAppsOutput.model_validate(listed_result.structured_content)
    assert len(listed.data_apps) > 0
    assert all(isinstance(app, DataAppSummary) for app in listed.data_apps)
    assert configuration_id in [a.configuration_id for a in listed.data_apps]
    # TODO(REMOVE): Remove this assertion once DSAPI is fixed. This only checks that we do not leave any data apps
    # in the CI project after test executions except those which are already there and cannot be deleted.
    assert len(listed.data_apps) < 110

    # Update app
    updated_name = f'{app_name} - Updated'
    updated_description = 'Data app updated by integration test'
    updated_source_code = 'import numpy as np\n\n'
    updated_result = await mcp_client.call_tool(
        name='modify_streamlit_data_app',
        arguments={
            'name': updated_name,
            'description': updated_description,
            'source_code': updated_source_code,
            'packages': ['streamlit'],
            'authentication_type': 'no-auth',
            'configuration_id': configuration_id,
            'change_description': 'Update Code',
        },
    )
    # Check updated app basic details
    assert updated_result.structured_content is not None
    updated = ModifiedDataAppOutput.model_validate(updated_result.structured_content)
    assert updated.response == 'updated'
    assert updated.data_app.data_app_id == data_app_id
    assert updated.data_app.configuration_id == configuration_id

    # Check that KBC.MCP.updatedBy.version.{version} is set to 'true'
    metadata = cast(
        list[Mapping[str, Any]],
        await keboola_client.storage_client.configuration_metadata_get(
            component_id=DATA_APP_COMPONENT_ID, configuration_id=configuration_id
        ),
    )
    meta_key = f'{MetadataField.UPDATED_BY_MCP_PREFIX}{updated.data_app.config_version}'
    meta_value = get_metadata_property(metadata, meta_key)
    assert meta_value == 'true'
    # Check that the original creation metadata is still there
    assert get_metadata_property(metadata, MetadataField.CREATED_BY_MCP) == 'true'

    # Check updated app details by configuration_id
    fetched_app = await mcp_client.call_tool(name='get_data_apps', arguments={'configuration_ids': [configuration_id]})
    assert fetched_app.structured_content is not None
    fetched = GetDataAppsOutput.model_validate(fetched_app.structured_content)
    assert len(fetched.data_apps) == 1
    assert isinstance(fetched.data_apps[0], DataApp)
    assert fetched.data_apps[0].name == updated_name
    assert fetched.data_apps[0].description == updated_description
    # Check that the source code is updated
    fetched_data_app_parameters = fetched.data_apps[0].configuration.get('parameters') or {}
    assert _get_query_function_code(sql_dialect) in fetched_data_app_parameters['script'][0]
    assert updated_source_code in fetched_data_app_parameters['script'][0]
    assert streamlit_app_imports not in fetched_data_app_parameters['script'][0]
    assert streamlit_app_entrypoint not in fetched_data_app_parameters['script'][0]
    # Check that the packages are updated
    assert set(fetched_data_app_parameters['packages']) == set(['streamlit'] + _DEFAULT_PACKAGES)


# ===== python-js data app: prod + external-git draft (AI-3005 / AI-3286) =====


@pytest.fixture
def python_js_app_py() -> str:
    """Minimal python-js entrypoint: a tiny HTTP server returning a fixed string."""
    return (
        'from http.server import BaseHTTPRequestHandler, HTTPServer\n'
        'import os\n\n'
        'class H(BaseHTTPRequestHandler):\n'
        '    def do_GET(self):\n'
        '        self.send_response(200)\n'
        '        self.end_headers()\n'
        "        self.wfile.write(b'integration-test-ok')\n\n"
        "if __name__ == '__main__':\n"
        "    port = int(os.environ.get('PORT', '8000'))\n"
        "    HTTPServer(('0.0.0.0', port), H).serve_forever()\n"
    )


def _git(*args: str, cwd: Path) -> None:
    """Run a git subcommand inside `cwd`, failing loudly on non-zero exit."""
    subprocess.run(['git', *args], cwd=cwd, check=True, capture_output=True, text=True)


@pytest.mark.asyncio
async def test_python_js_data_app_prod_and_draft_lifecycle(
    mcp_client: Client,
    keboola_client: KeboolaClient,
    tmp_path: Path,
    python_js_app_py: str,
) -> None:
    """End-to-end on canary-orion: create prod (managed repo), create draft
    (external-git pointing at prod's repo), push branch, deploy draft in mode='dev',
    merge into main, redeploy prod, then delete the draft via the new MCP tool.

    Also asserts that fetching the prod's detail surfaces the draft in `drafts: [...]`
    before deletion and returns an empty `drafts` array after.
    """

    unique = uuid.uuid4().hex[:8]
    prod_slug = f'int-prod-{unique}'
    draft_slug = f'int-draft-{unique}'
    prod_output: ModifiedPythonJsDataAppOutput | None = None
    draft_output: ModifiedPythonJsDataAppOutput | None = None
    draft_deleted_via_tool = False

    try:
        # Step 1: create prod (managed repo).
        prod_result = await mcp_client.call_tool(
            name='modify_python_js_data_app',
            arguments={
                'name': f'Integration prod {unique}',
                'description': 'AI-3286 prod app integration test',
                'slug': prod_slug,
                'authentication_type': 'no-auth',
            },
        )
        assert prod_result.structured_content is not None
        prod_output = ModifiedPythonJsDataAppOutput.model_validate(prod_result.structured_content)
        assert prod_output.response == 'created'
        assert prod_output.repo_url is not None
        assert prod_output.repo_url.startswith('https://')
        assert prod_output.git_clone_url is None
        assert prod_output.branch is None

        # Step 2: create draft pointing at prod's repo. Branch defaults to 'init'.
        draft_result = await mcp_client.call_tool(
            name='modify_python_js_data_app',
            arguments={
                'name': f'Integration draft {unique}',
                'description': 'AI-3286 draft integration test',
                'slug': draft_slug,
                'parent_configuration_id': prod_output.data_app.configuration_id,
                'authentication_type': 'no-auth',
            },
        )
        assert draft_result.structured_content is not None
        draft_output = ModifiedPythonJsDataAppOutput.model_validate(draft_result.structured_content)
        assert draft_output.response == 'created'
        assert draft_output.repo_url == prod_output.repo_url
        assert draft_output.git_clone_url is not None
        assert draft_output.git_clone_url.startswith('https://kai:')
        assert draft_output.branch == 'init'

        # Step 3: clone via the embedded credential. Initialize main if the freshly provisioned
        # repo is empty, then branch off and push the draft branch.
        repo_dir = tmp_path / 'repo'
        env = {**os.environ, 'GIT_TERMINAL_PROMPT': '0'}
        subprocess.run(
            ['git', 'clone', draft_output.git_clone_url, str(repo_dir)],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        _git('config', 'user.email', 'mcp-integration@keboola.com', cwd=repo_dir)
        _git('config', 'user.name', 'MCP Integration Test', cwd=repo_dir)
        # The platform-provisioned repo may be empty. Make sure `main` exists with at least one
        # commit so the post-merge push has somewhere to land.
        has_main = (
            subprocess.run(
                ['git', 'rev-parse', '--verify', 'refs/heads/main'],
                cwd=repo_dir,
                capture_output=True,
                text=True,
                check=False,
            ).returncode
            == 0
        )
        if not has_main:
            _git('checkout', '-b', 'main', cwd=repo_dir)
            (repo_dir / 'README.md').write_text(f'# integration test {unique}\n')
            _git('add', 'README.md', cwd=repo_dir)
            _git('commit', '-m', 'init main', cwd=repo_dir)
            subprocess.run(
                ['git', 'push', '-u', 'origin', 'main'],
                cwd=repo_dir,
                check=True,
                capture_output=True,
                text=True,
                env=env,
            )
        _git('checkout', '-b', draft_output.branch, 'main', cwd=repo_dir)
        (repo_dir / 'app.py').write_text(python_js_app_py)
        _git('add', 'app.py', cwd=repo_dir)
        _git('commit', '-m', f'AI-3286 integration test commit {unique}', cwd=repo_dir)
        subprocess.run(
            ['git', 'push', '-u', 'origin', draft_output.branch],
            cwd=repo_dir,
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )

        # Step 4: deploy draft in mode='dev'.
        draft_deploy = await mcp_client.call_tool(
            name='deploy_data_app',
            arguments={
                'action': 'deploy',
                'configuration_id': draft_output.data_app.configuration_id,
                'mode': 'dev',
            },
        )
        assert draft_deploy.structured_content is not None
        # The data-app runtime is async — we only assert the deploy call was accepted; not its
        # eventual state, since CI cannot afford to poll the full startup loop.

        # Fetching the prod's detail must now list the draft under `drafts`.
        prod_detail_before = await mcp_client.call_tool(
            name='get_data_apps',
            arguments={'configuration_ids': [prod_output.data_app.configuration_id]},
        )
        assert prod_detail_before.structured_content is not None
        prod_apps_before = GetDataAppsOutput.model_validate(prod_detail_before.structured_content)
        assert len(prod_apps_before.data_apps) == 1
        prod_app_before = prod_apps_before.data_apps[0]
        assert isinstance(prod_app_before, DataApp)
        draft_cfg_ids_before = [d.configuration_id for d in prod_app_before.drafts]
        assert draft_output.data_app.configuration_id in draft_cfg_ids_before

        # Confirm the draft's stored config carries the external-git block we sent and the
        # parent linkage we expect.
        draft_detail = await mcp_client.call_tool(
            name='get_data_apps',
            arguments={'configuration_ids': [draft_output.data_app.configuration_id]},
        )
        assert draft_detail.structured_content is not None
        draft_apps = GetDataAppsOutput.model_validate(draft_detail.structured_content)
        assert len(draft_apps.data_apps) == 1
        draft_detail_app = draft_apps.data_apps[0]
        assert isinstance(draft_detail_app, DataApp)
        draft_data_app_block = draft_detail_app.configuration.get('parameters', {}).get('dataApp', {})
        draft_git_block = draft_data_app_block.get('git', {})
        assert draft_git_block.get('repository') == prod_output.repo_url
        assert draft_git_block.get('branch') == draft_output.branch
        assert draft_git_block.get('username') == 'kai'
        encrypted_pw = draft_git_block.get('#password', '')
        assert encrypted_pw.startswith('KBC::'), f'expected encrypted #password, got {encrypted_pw!r}'
        assert draft_data_app_block.get('isDraft') is True
        assert draft_data_app_block.get('parentConfigurationId') == prod_output.data_app.configuration_id
        # Drafts of a draft are always empty.
        assert draft_detail_app.drafts == []

        # Step 4b: repoint the draft's external-git branch via the update path (CFTL-714) and
        # confirm the stored config now pins the new branch while the encrypted credential,
        # repository, and username are preserved verbatim.
        repointed_branch = f'repoint-{unique}'
        repoint_result = await mcp_client.call_tool(
            name='modify_python_js_data_app',
            arguments={
                'name': '',
                'description': '',
                'configuration_id': draft_output.data_app.configuration_id,
                'branch': repointed_branch,
                'change_description': 'CFTL-714 repoint git branch',
            },
        )
        assert repoint_result.structured_content is not None
        repoint_output = ModifiedPythonJsDataAppOutput.model_validate(repoint_result.structured_content)
        assert repoint_output.response.startswith('updated')
        assert repoint_output.change_summary is not None
        assert repointed_branch in repoint_output.change_summary

        repointed_detail = await mcp_client.call_tool(
            name='get_data_apps',
            arguments={'configuration_ids': [draft_output.data_app.configuration_id]},
        )
        assert repointed_detail.structured_content is not None
        repointed_apps = GetDataAppsOutput.model_validate(repointed_detail.structured_content)
        repointed_app = repointed_apps.data_apps[0]
        assert isinstance(repointed_app, DataApp)
        repointed_git_block = repointed_app.configuration.get('parameters', {}).get('dataApp', {}).get('git', {})
        assert repointed_git_block.get('branch') == repointed_branch
        # Untouched fields survive the repoint (only `branch` is rewritten; no re-encryption).
        assert repointed_git_block.get('repository') == prod_output.repo_url
        assert repointed_git_block.get('username') == 'kai'
        assert repointed_git_block.get('#password', '').startswith('KBC::')

        # Step 5: merge into main and push.
        _git('checkout', 'main', cwd=repo_dir)
        _git('merge', '--no-ff', '-m', f'Merge {draft_output.branch}', draft_output.branch, cwd=repo_dir)
        subprocess.run(
            ['git', 'push', 'origin', 'main'],
            cwd=repo_dir,
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )

        # Step 6: redeploy prod (no mode, no branch).
        prod_deploy = await mcp_client.call_tool(
            name='deploy_data_app',
            arguments={
                'action': 'deploy',
                'configuration_id': prod_output.data_app.configuration_id,
            },
        )
        assert prod_deploy.structured_content is not None

        # Step 7: delete the draft via the new MCP tool, then verify it's gone from prod's drafts.
        # Stop the draft first — DSAPI delete requires desiredState == currentState.
        try:
            await keboola_client.data_science_client.suspend_data_app(draft_output.data_app.data_app_id)
        except Exception as exc:
            LOG.info(f'suspend failed for {draft_output.data_app.data_app_id}: {exc}')
        delete_result = await mcp_client.call_tool(
            name='delete_python_js_data_app_draft',
            arguments={'configuration_id': draft_output.data_app.configuration_id},
        )
        assert delete_result.structured_content is not None
        deleted = delete_result.structured_content
        assert deleted['response'] == 'deleted'
        assert deleted['configuration_id'] == draft_output.data_app.configuration_id
        assert deleted['parent_configuration_id'] == prod_output.data_app.configuration_id
        draft_deleted_via_tool = True

        prod_detail_after = await mcp_client.call_tool(
            name='get_data_apps',
            arguments={'configuration_ids': [prod_output.data_app.configuration_id]},
        )
        assert prod_detail_after.structured_content is not None
        prod_apps_after = GetDataAppsOutput.model_validate(prod_detail_after.structured_content)
        prod_app_after = prod_apps_after.data_apps[0]
        assert isinstance(prod_app_after, DataApp)
        draft_cfg_ids_after = [d.configuration_id for d in prod_app_after.drafts]
        assert draft_output.data_app.configuration_id not in draft_cfg_ids_after

    finally:
        # Teardown: best-effort cleanup. Skip draft if the test already deleted it via the tool.
        cleanup_targets = [(prod_output, 'prod')]
        if not draft_deleted_via_tool:
            cleanup_targets.insert(0, (draft_output, 'draft'))
        for app, role in cleanup_targets:
            if app is None:
                continue
            try:
                await keboola_client.data_science_client.suspend_data_app(app.data_app.data_app_id)
            except Exception as exc:
                LOG.info(f'suspend failed for {role} {app.data_app.data_app_id}: {exc}')
            try:
                await keboola_client.data_science_client.delete_data_app(app.data_app.data_app_id)
            except Exception as exc:
                LOG.error(f'delete failed for {role} {app.data_app.data_app_id}: {exc}')
