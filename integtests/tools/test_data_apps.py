import asyncio
import logging
import os
import subprocess
import time
import uuid
from collections.abc import AsyncGenerator, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import pytest
import pytest_asyncio
import toon_format
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID, KeboolaClient, get_metadata_property
from keboola_mcp_server.config import Config, MetadataField, ServerRuntimeInfo
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.data_apps import (
    _DEFAULT_PACKAGES,
    CreatedDraftOutput,
    DataApp,
    DataAppPreviewLinkOutput,
    DataAppSummary,
    DeploymentDataAppOutput,
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
    return f'{streamlit_app_imports}{{QUERY_DATA_FUNCTION}}\n\n{streamlit_app_entrypoint}'


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


async def _wait_until_suspended(keboola_client: KeboolaClient, data_app_id: str, timeout_s: float = 180) -> None:
    """Wait until the data app is suspended, or the timeout passes.

    - A delete issued while the app still changes state deadlocks in DSAPI against its state watcher (500).
    - A suspend changes no deployment and `/v2` lists no stopping state, so only `suspended` tells it finished.
    - Returns on timeout too: the delete that follows reports the real outcome.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        app = await keboola_client.data_science_client.get_data_app(data_app_id)
        if app.state == 'suspended':
            return
        await asyncio.sleep(5)
    LOG.info(f'data app {data_app_id} did not suspend within {timeout_s}s')


@pytest.mark.asyncio
async def test_python_js_data_app_prod_and_draft_lifecycle(
    mcp_client: Client,
    keboola_client: KeboolaClient,
    tmp_path: Path,
    python_js_app_py: str,
) -> None:
    """End-to-end: create prod (managed repo), push a branch, create a draft on it, mint its preview link,
    merge into main, redeploy prod, then delete the draft; the prod detail lists the draft until then."""

    unique = uuid.uuid4().hex[:8]
    branch = f'draft-{unique}'
    prod_output: ModifiedPythonJsDataAppOutput | None = None
    live_draft_id: str | None = None

    try:
        # Step 1: create prod (managed repo).
        prod_result = await mcp_client.call_tool(
            name='modify_python_js_data_app',
            arguments={
                'name': f'Integration prod {unique}',
                'description': 'prod app integration test',
                'slug': f'int-prod-{unique}',
                'authentication_type': 'no-auth',
            },
        )
        assert prod_result.structured_content is not None
        prod_output = ModifiedPythonJsDataAppOutput.model_validate(prod_result.structured_content)
        assert prod_output.response == 'created'
        assert prod_output.repo_url is not None
        assert prod_output.branch is None
        prod_cfg_id = prod_output.data_app.configuration_id

        # Step 2: clone with a fresh credential, make sure `main` exists, push the draft branch.
        credential = await mcp_client.call_tool(
            name='create_python_js_data_app_git_credential', arguments={'configuration_id': prod_cfg_id}
        )
        assert credential.structured_content is not None
        clone_url = credential.structured_content['git_clone_url']
        repo_dir = tmp_path / 'repo'
        env = {**os.environ, 'GIT_TERMINAL_PROMPT': '0'}

        def push(ref: str) -> None:
            subprocess.run(
                ['git', 'push', '-u', 'origin', ref], cwd=repo_dir, check=True, capture_output=True, text=True, env=env
            )

        subprocess.run(['git', 'clone', clone_url, str(repo_dir)], check=True, capture_output=True, text=True, env=env)
        _git('config', 'user.email', 'mcp-integration@keboola.com', cwd=repo_dir)
        _git('config', 'user.name', 'MCP Integration Test', cwd=repo_dir)
        has_main = (
            subprocess.run(
                ['git', 'rev-parse', '--verify', 'refs/heads/main'], cwd=repo_dir, capture_output=True, check=False
            ).returncode
            == 0
        )
        if not has_main:
            _git('checkout', '-b', 'main', cwd=repo_dir)
            _git('commit', '--allow-empty', '-m', 'init main', cwd=repo_dir)
            push('main')
        _git('checkout', '-b', branch, 'main', cwd=repo_dir)
        (repo_dir / 'app.py').write_text(python_js_app_py)
        _git('add', 'app.py', cwd=repo_dir)
        _git('commit', '-m', f'integration test commit {unique}', cwd=repo_dir)
        push(branch)

        # Step 3: create the draft on the pushed branch.
        draft_result = await mcp_client.call_tool(
            name='create_python_js_data_app_draft', arguments={'configuration_id': prod_cfg_id, 'git_branch': branch}
        )
        assert draft_result.structured_content is not None
        draft = CreatedDraftOutput.model_validate(draft_result.structured_content).draft
        assert draft.git_branch == branch
        live_draft_id = draft.draft_id

        # Step 4: mint a preview link. The draft URL can lag on the test stack, so "has no URL yet" passes too.
        try:
            preview_result = await mcp_client.call_tool(
                name='get_data_app_preview_link',
                arguments={'configuration_id': prod_cfg_id, 'draft_id': draft.draft_id},
            )
        except ToolError as e:
            assert 'has no URL yet' in str(e), f'unexpected preview-link refusal: {e}'
        else:
            assert preview_result.structured_content is not None
            preview = DataAppPreviewLinkOutput.model_validate(preview_result.structured_content)
            assert '/_proxy/preview#t=' in preview.url
            assert datetime.fromisoformat(preview.link_expires_at) > datetime.now(timezone.utc)

        async def listed_draft_ids() -> list[str]:
            detail = await mcp_client.call_tool(name='get_data_apps', arguments={'configuration_ids': [prod_cfg_id]})
            assert detail.structured_content is not None
            app = GetDataAppsOutput.model_validate(detail.structured_content).data_apps[0]
            assert isinstance(app, DataApp)
            assert app.drafts is not None
            return [d.draft_id for d in app.drafts]

        assert draft.draft_id in await listed_draft_ids()

        # Step 5: merge into main and redeploy prod.
        _git('checkout', 'main', cwd=repo_dir)
        _git('merge', '--no-ff', '-m', f'Merge {branch}', branch, cwd=repo_dir)
        push('main')
        prod_deploy = await mcp_client.call_tool(
            name='deploy_data_app', arguments={'action': 'deploy', 'configuration_id': prod_cfg_id}
        )
        assert prod_deploy.structured_content is not None

        # Step 6: delete the draft; it is gone from the prod detail.
        delete_result = await mcp_client.call_tool(
            name='delete_python_js_data_app_draft',
            arguments={'configuration_id': prod_cfg_id, 'draft_id': draft.draft_id},
        )
        assert delete_result.structured_content is not None
        assert delete_result.structured_content['response'] == 'deleted'
        live_draft_id = None
        assert draft.draft_id not in await listed_draft_ids()

    finally:
        if prod_output is not None:
            app_id = prod_output.data_app.data_app_id
            if live_draft_id is not None:
                try:
                    await keboola_client.data_science_client.delete_draft(app_id, live_draft_id)
                except Exception as exc:
                    LOG.error(f'delete failed for draft {live_draft_id}: {exc}')
            try:
                await keboola_client.data_science_client.suspend_data_app(app_id)
            except Exception as exc:
                LOG.info(f'suspend failed for prod {app_id}: {exc}')
            try:
                await keboola_client.data_science_client.delete_data_app(app_id)
            except Exception as exc:
                LOG.error(f'delete failed for prod {app_id}: {exc}')


@pytest.mark.asyncio
async def test_python_js_data_app_storage_access_can_be_enabled_after_create(
    mcp_client: Client,
    keboola_client: KeboolaClient,
) -> None:
    """An agent can turn read-only Storage access on for an existing python-js app
    using MCP tools alone, with no UI step.

    Creates an app that explicitly opts out of Storage access, verifies the stored configuration
    carries neither marker, then enables it through `modify_python_js_data_app(storage_access=True)`
    and verifies the marker is there. Which marker is used depends on whether the project has the
    `data-apps-storage-workspace` feature (`runtime.workspace.enabled` if so, the legacy
    `parameters.dataApp.secrets.WORKSPACE_ID` if not), so the assertions accept either -- what
    matters is that the app ends up with a workspace id reaching it as WORKSPACE_ID.

    Whether that environment variable actually lands in the running container is a platform
    concern this test cannot observe; verify it manually on a deployed app.
    """

    def has_storage_access(configuration: Mapping[str, Any]) -> bool:
        workspace = (configuration.get('runtime') or {}).get('workspace') or {}
        secrets = ((configuration.get('parameters') or {}).get('dataApp') or {}).get('secrets') or {}
        return bool(workspace.get('enabled')) or bool(secrets.get('WORKSPACE_ID'))

    async def fetch_configuration(configuration_id: str) -> Mapping[str, Any]:
        detail = await mcp_client.call_tool(name='get_data_apps', arguments={'configuration_ids': [configuration_id]})
        assert detail.structured_content is not None
        app = GetDataAppsOutput.model_validate(detail.structured_content).data_apps[0]
        assert isinstance(app, DataApp)
        return app.configuration

    unique = uuid.uuid4().hex[:8]
    created: ModifiedPythonJsDataAppOutput | None = None
    try:
        # Step 1: create with Storage access explicitly declined.
        create_result = await mcp_client.call_tool(
            name='modify_python_js_data_app',
            arguments={
                'name': f'Integration storage-access {unique}',
                'description': 'storage access integration test',
                'slug': f'int-sa-{unique}',
                'authentication_type': 'basic-auth',
                'storage_access': False,
            },
        )
        assert create_result.structured_content is not None
        created = ModifiedPythonJsDataAppOutput.model_validate(create_result.structured_content)
        assert created.response == 'created'
        assert created.data_app.storage_access_enabled is False
        assert has_storage_access(await fetch_configuration(created.data_app.configuration_id)) is False

        # Step 2: enable it through MCP alone -- the gap this closes.
        enable_result = await mcp_client.call_tool(
            name='modify_python_js_data_app',
            arguments={
                'name': '',
                'description': '',
                'configuration_id': created.data_app.configuration_id,
                'storage_access': True,
                'change_description': 'enable storage access',
            },
        )
        assert enable_result.structured_content is not None
        enabled = ModifiedPythonJsDataAppOutput.model_validate(enable_result.structured_content)
        assert enabled.response.startswith('updated')
        assert enabled.data_app.storage_access_enabled is True
        assert enabled.change_summary is not None
        assert 'Storage access enabled' in enabled.change_summary
        assert has_storage_access(await fetch_configuration(created.data_app.configuration_id)) is True

    finally:
        if created is not None:
            try:
                await keboola_client.data_science_client.suspend_data_app(created.data_app.data_app_id)
            except Exception as exc:
                LOG.info(f'suspend failed for {created.data_app.data_app_id}: {exc}')
            try:
                await keboola_client.data_science_client.delete_data_app(created.data_app.data_app_id)
            except Exception as exc:
                LOG.error(f'delete failed for {created.data_app.data_app_id}: {exc}')


@pytest.mark.asyncio
async def test_python_js_data_app_deploy_publishes_config_only_change(
    mcp_client: Client,
    keboola_client: KeboolaClient,
    tmp_path: Path,
    python_js_app_py: str,
) -> None:
    """A config-only change (no git push) goes live on `deploy_data_app`, and the tool
    reports the version the app actually runs instead of the latest saved one.

    Before the fix a python-js deploy sent no `configVersion`, so the data-science API kept the
    previously published version while `deployment_info.version` echoed the latest config version.
    """

    async def deploy(configuration_id: str) -> DeploymentDataAppOutput:
        result = await mcp_client.call_tool(
            name='deploy_data_app', arguments={'action': 'deploy', 'configuration_id': configuration_id}
        )
        assert result.structured_content is not None
        return DeploymentDataAppOutput.model_validate(result.structured_content)

    async def fetch_detail(configuration_id: str) -> DataApp:
        result = await mcp_client.call_tool(name='get_data_apps', arguments={'configuration_ids': [configuration_id]})
        assert result.structured_content is not None
        app = GetDataAppsOutput.model_validate(result.structured_content).data_apps[0]
        assert isinstance(app, DataApp)
        return app

    unique = uuid.uuid4().hex[:8]
    created: ModifiedPythonJsDataAppOutput | None = None
    try:
        # Step 1: create a prod app and push minimal code to `main`.
        create_result = await mcp_client.call_tool(
            name='modify_python_js_data_app',
            arguments={
                'name': f'Integration publish {unique}',
                'description': 'Config publishing integration test',
                'slug': f'int-pub-{unique}',
                'authentication_type': 'no-auth',
                'auto_suspend_after_seconds': 900,
            },
        )
        assert create_result.structured_content is not None
        created = ModifiedPythonJsDataAppOutput.model_validate(create_result.structured_content)
        configuration_id = created.data_app.configuration_id

        credential_result = await mcp_client.call_tool(
            name='create_python_js_data_app_git_credential', arguments={'configuration_id': configuration_id}
        )
        assert credential_result.structured_content is not None
        git_clone_url = credential_result.structured_content['git_clone_url']
        repo_dir = tmp_path / 'repo'
        env = {**os.environ, 'GIT_TERMINAL_PROMPT': '0'}
        subprocess.run(['git', 'clone', git_clone_url, str(repo_dir)], check=True, capture_output=True, env=env)
        _git('config', 'user.email', 'mcp-integration@keboola.com', cwd=repo_dir)
        _git('config', 'user.name', 'MCP Integration Test', cwd=repo_dir)
        _git('checkout', '-B', 'main', cwd=repo_dir)
        (repo_dir / 'app.py').write_text(python_js_app_py)
        _git('add', 'app.py', cwd=repo_dir)
        _git('commit', '-m', f'Integration test commit {unique}', cwd=repo_dir)
        subprocess.run(['git', 'push', '-u', 'origin', 'main'], cwd=repo_dir, check=True, capture_output=True, env=env)

        # Step 2: the first deploy publishes the current config version.
        first = await deploy(configuration_id)
        assert first.deployment_info is not None
        assert first.deployment_info.version == first.deployment_info.latest_config_version
        assert first.deployment_info.has_unpublished_changes is False
        published_before = first.deployment_info.version

        # Step 3: a config-only change -- nothing is pushed to git. It is saved but not live yet.
        update_result = await mcp_client.call_tool(
            name='modify_python_js_data_app',
            arguments={
                'name': '',
                'description': '',
                'configuration_id': configuration_id,
                'auto_suspend_after_seconds': 1800,
                'change_description': 'Config-only change',
            },
        )
        assert update_result.structured_content is not None
        pending = await fetch_detail(configuration_id)
        assert pending.deployment_info is not None
        assert int(pending.config_version) > int(published_before)
        assert pending.deployment_info.version == published_before
        assert pending.deployment_info.has_unpublished_changes is True

        # Step 4: redeploy publishes it, and the response reports the version actually running.
        second = await deploy(configuration_id)
        assert second.deployment_info is not None
        assert second.deployment_info.version == pending.config_version
        assert second.deployment_info.latest_config_version == pending.config_version
        assert second.deployment_info.has_unpublished_changes is False
        app = await keboola_client.data_science_client.get_data_app(created.data_app.data_app_id)
        assert app.production.desired_config_version == pending.config_version

    finally:
        if created is not None:
            try:
                await keboola_client.data_science_client.suspend_data_app(created.data_app.data_app_id)
            except Exception as exc:
                LOG.info(f'suspend failed for {created.data_app.data_app_id}: {exc}')
            try:
                await keboola_client.data_science_client.delete_data_app(created.data_app.data_app_id)
            except Exception as exc:
                LOG.error(f'delete failed for {created.data_app.data_app_id}: {exc}')
