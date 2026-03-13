from __future__ import annotations

import logging
from uuid import uuid4

import httpx
import pytest

from keboola_mcp_server.clients.metastore import MetastoreClient

LOG = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def metastore_url(storage_api_url: str) -> str:
    """Derive metastore URL from storage API URL by replacing 'connection.' prefix."""
    return storage_api_url.replace('connection.', 'metastore.', 1)


@pytest.fixture
def metastore_client(storage_api_token: str, metastore_url: str) -> MetastoreClient:
    return MetastoreClient.create(root_url=metastore_url, token=storage_api_token)


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
async def test_health_check(metastore_client: MetastoreClient) -> None:
    health = await metastore_client.health_check()
    assert health.is_ok is True


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
    assert schema_doc.title == object_type
    assert schema_doc.version


@pytest.mark.asyncio
async def test_get_schema_specific_version(metastore_client: MetastoreClient) -> None:
    latest = await metastore_client.get_schema('semantic-model')
    if not latest.version:
        pytest.skip('Schema version not available in metastore response.')
    specific = await metastore_client.get_schema('semantic-model', version=latest.version)
    assert specific.title == 'semantic-model'
    assert specific.version == latest.version


@pytest.mark.asyncio
async def test_list_semantic_models_and_optional_detail(metastore_client: MetastoreClient) -> None:
    try:
        models = await metastore_client.list_objects('semantic-model', limit=10)
    except httpx.HTTPStatusError as exc:
        _skip_unauthorized(exc)
        raise
    assert isinstance(models, list)

    if not models:
        pytest.skip('No semantic-model objects available in project scope for this token.')

    if not models[0].id:
        pytest.skip('First listed model has no id in response.')

    model = await metastore_client.get_object('semantic-model', models[0].id)
    assert model.id
    assert model.type == 'semantic-model'


@pytest.mark.asyncio
async def test_get_organization_models_list(metastore_client: MetastoreClient) -> None:
    try:
        models = await metastore_client.list_objects('semantic-model', organization_scope=True, limit=10)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in (401, 403, 404):
            pytest.skip('Organization scope endpoint is not accessible for this token/environment.')
        raise
    assert isinstance(models, list)


@pytest.mark.asyncio
async def test_crud_walkthrough_post_get_put_delete_and_revisions(metastore_client: MetastoreClient) -> None:
    object_type = 'semantic-model'
    model_name = f'ai2607-it-model-{uuid4().hex[:8]}'
    model_id: str | None = None

    try:
        try:
            created = await metastore_client.create_object(
                object_type,
                name=model_name,
                data={
                    'name': model_name,
                    'sql_dialect': 'Snowflake',
                    'description': 'Integration test model',
                    'status': 'draft',
                },
            )
        except httpx.HTTPStatusError as exc:
            _skip_unauthorized(exc)
            raise

        model_id = created.id
        assert model_id
        assert created.attributes.get('name') == model_name
        assert created.type == object_type

        fetched = await metastore_client.get_object(object_type, model_id)
        assert fetched.id == model_id
        assert fetched.attributes.get('sql_dialect') == 'Snowflake'
        assert fetched.attributes.get('status') == 'draft'

        replaced = await metastore_client.put_object(
            object_type,
            model_id,
            name=model_name,
            data={
                'name': model_name,
                'sql_dialect': 'BigQuery',
                'description': 'Replaced via PUT',
                'status': 'published',
            },
        )
        assert replaced.id == model_id

        fetched_after_put = await metastore_client.get_object(object_type, model_id)
        assert fetched_after_put.attributes.get('sql_dialect') == 'BigQuery'
        assert fetched_after_put.attributes.get('status') == 'published'
        after_put_revision = fetched_after_put.meta.get('revision')
        assert isinstance(after_put_revision, int)
        assert after_put_revision >= 2

        revisions = await metastore_client.list_revisions(object_type, filter_by=f'id={model_id}')
        assert isinstance(revisions, list)
        assert len(revisions) >= 1

        rev1 = await metastore_client.get_revision(object_type, model_id, 1)
        assert rev1.id == model_id
        assert rev1.attributes.get('sql_dialect') == 'Snowflake'

        if after_put_revision >= 2:
            rev2 = await metastore_client.get_revision(object_type, model_id, 2)
            assert rev2.id == model_id
            assert rev2.attributes.get('sql_dialect') == 'BigQuery'

        await metastore_client.delete_object(object_type, model_id)

        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await metastore_client.get_object(object_type, model_id)
        assert exc_info.value.response.status_code == 404

    finally:
        if model_id:
            try:
                await metastore_client.delete_object(object_type, model_id)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code not in (401, 403, 404):
                    raise
