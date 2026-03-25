from __future__ import annotations

import logging
from urllib.parse import urljoin
from uuid import uuid4

import httpx
import pytest

from keboola_mcp_server.clients.metastore import MetastoreClient

LOG = logging.getLogger(__name__)


async def delete_metastore_object(client: MetastoreClient, object_type: str, uuid: str) -> None:
    try:
        await client.delete_object(object_type, uuid)
        # Delete the soft deleted object
        await client.delete_object(object_type, uuid)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code not in (401, 403, 404):
            raise


@pytest.fixture(scope='session')
def metastore_url(storage_api_url: str) -> str:
    """Derive metastore URL from storage API URL by replacing 'connection.' prefix."""
    return storage_api_url.replace('connection.', 'metastore.', 1)


@pytest.fixture
def metastore_client(storage_api_token: str, metastore_url: str) -> MetastoreClient:
    return MetastoreClient.create(root_url=metastore_url, token=storage_api_token)


@pytest.fixture(scope='module', autouse=True)
def _require_metastore_available(
    storage_api_token: str,
    metastore_url: str,
) -> None:
    try:
        probe_url = urljoin(metastore_url, '/health-check')
        with httpx.Client(
            headers={'X-StorageApi-Token': storage_api_token},
            timeout=httpx.Timeout(3.0, connect=1.0),
        ) as client:
            response = client.get(probe_url)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        _skip_unauthorized(exc)
        raise
    except httpx.ConnectError as exc:
        pytest.skip(f'Metastore endpoint is not reachable in this environment: {exc}')
    except httpx.TimeoutException as exc:
        pytest.skip(f'Metastore endpoint timed out in this environment: {exc}')


def _skip_unauthorized(exc: httpx.HTTPStatusError) -> None:
    if exc.response.status_code == 401:
        details = ''
        try:
            details = exc.response.text[:300]
        except Exception:
            details = '<no response text>'
        LOG.warning(f'Metastore unauthorized (401) for {exc.request.url}: {details}')
        pytest.skip('Token is not authorized for configured Metastore.')


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'object_type',
    [
        'semantic-model',
        'semantic-dataset',
        'semantic-metric',
        'semantic-relationship',
        'semantic-glossary',
        'semantic-constraint',
    ],
)
async def test_get_schema_for_semantic_types(metastore_client: MetastoreClient, object_type: str) -> None:
    schema_doc = await metastore_client.get_schema(object_type)
    assert isinstance(schema_doc, dict)


@pytest.mark.asyncio
async def test_get_schema_specific_version(metastore_client: MetastoreClient) -> None:
    latest = await metastore_client.get_schema('semantic-model')
    version = latest.get('version')
    if not version:
        pytest.skip('Schema version not available in metastore response.')
    specific = await metastore_client.get_schema('semantic-model', version=version)
    assert specific.get('title') == 'semantic-model'
    assert specific.get('version') == version


@pytest.mark.asyncio
async def test_list_semantic_models_and_optional_detail(metastore_client: MetastoreClient) -> None:
    try:
        models = await metastore_client.list_objects('semantic-model')
    except httpx.HTTPStatusError as exc:
        _skip_unauthorized(exc)
        raise
    assert isinstance(models, list)

    if not models:
        pytest.skip('No semantic-model objects available in project scope for this token.')

    model = await metastore_client.get_object('semantic-model', models[0].id)
    assert model.id
    assert model.type == 'semantic-model'
    assert isinstance(model.attributes, dict)


@pytest.mark.asyncio
async def test_get_organization_models_list(metastore_client: MetastoreClient) -> None:
    try:
        models = await metastore_client.list_objects('semantic-model', organization_scope=True)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (401, 403, 404):
            pytest.skip('Organization scope endpoint is not accessible for this token/environment.')
        raise
    assert isinstance(models, list)


@pytest.mark.asyncio
async def test_crud_walkthrough_post_get_put_delete_and_revisions(metastore_client: MetastoreClient) -> None:
    object_type = 'semantic-model'
    model_name = f'ai2607-it-model-{uuid4().hex[:8]}'
    model_uuid: str | None = None

    try:
        try:
            created = await metastore_client.create_object(
                object_type,
                name=model_name,
                data={
                    'name': model_name,
                    'sql_dialect': 'Snowflake',
                    'description': 'Integration test model',
                },
            )
        except httpx.HTTPStatusError as exc:
            _skip_unauthorized(exc)
            raise

        model_uuid = created.id
        assert model_uuid
        assert created.type == object_type
        assert created.attributes.get('name') == model_name

        fetched = await metastore_client.get_object(object_type, model_uuid)
        assert fetched.id == model_uuid
        assert fetched.attributes.get('sql_dialect') == 'Snowflake'

        replaced = await metastore_client.put_object(
            object_type,
            model_uuid,
            name=model_name,
            data={
                'name': model_name,
                'sql_dialect': 'BigQuery',
                'description': 'Replaced via PUT',
            },
        )
        assert replaced.id == model_uuid

        fetched_after_put = await metastore_client.get_object(object_type, model_uuid)
        assert fetched_after_put.attributes.get('sql_dialect') == 'BigQuery'
        assert fetched_after_put.meta.revision >= 2

        revisions = await metastore_client.list_revisions(object_type, filter_by=f'id={model_uuid}')
        assert isinstance(revisions, list)
        assert len(revisions) >= 1

        rev1 = await metastore_client.get_revision(object_type, model_uuid, 1)
        assert rev1.id == model_uuid
        assert rev1.attributes.get('sql_dialect') == 'Snowflake'

        if fetched_after_put.meta.revision >= 2:
            rev2 = await metastore_client.get_revision(object_type, model_uuid, 2)
            assert rev2.id == model_uuid
            assert rev2.attributes.get('sql_dialect') == 'BigQuery'

        await delete_metastore_object(metastore_client, object_type, model_uuid)

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await metastore_client.get_object(object_type, model_uuid)
        assert exc_info.value.response.status_code == 404

    finally:
        if model_uuid:
            await delete_metastore_object(metastore_client, object_type, model_uuid)
