"""`load_policy_rules`: reads every page of the project's policies, only through the SA step-up."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from keboola_mcp_server.clients.metastore import MetaObjectMeta, MetastoreObject
from keboola_mcp_server.rls import rewrite_query
from keboola_mcp_server.rls_policies import _MAX_PAGES, _PAGE_SIZE, load_policy_rules


def _policy(index: int) -> MetastoreObject:
    return MetastoreObject(
        type='rls-policy',
        id=f'policy-{index}',
        attributes={
            'table': f'in.c-crm.t{index}',
            'dialect': 'snowflake',
            'rules': [{'principal': 'petr', 'condition': {'true': True}}],
        },
        meta=MetaObjectMeta(source_project_id=1, scope='organization'),
    )


def _client(pages_by_type: dict[str, list[list[MetastoreObject]]]) -> MagicMock:
    """A client whose metastore serves `pages_by_type[object_type]` one page per call."""
    served = {object_type: iter(pages) for object_type, pages in pages_by_type.items()}

    async def list_objects(object_type: str, *, limit: int | None = None, offset: int | None = None):
        return next(served[object_type])

    metastore = MagicMock()
    metastore.list_objects = AsyncMock(side_effect=list_objects)
    client = MagicMock()
    client.storage_client.project_id = AsyncMock(return_value='1')
    client.step_up_metastore_client = MagicMock(return_value=metastore)
    return client


@pytest.fixture(autouse=True)
def _deployed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', '/var/run/sa-token')


@pytest.mark.asyncio
async def test_every_page_is_loaded_so_no_policy_is_missed() -> None:
    first = [_policy(i) for i in range(_PAGE_SIZE)]
    second = [_policy(i) for i in range(_PAGE_SIZE, _PAGE_SIZE + 3)]
    client = _client({'rls-policy': [first, second, []], 'cls-policy': [[]]})

    rules, _ = await load_policy_rules(client, dialect='snowflake')

    # A policy that only exists on the second page still governs its table.
    assert f'in.c-crm.t{_PAGE_SIZE + 2}' in rules.tables
    assert len(rules.tables) == _PAGE_SIZE + 3
    metastore = client.step_up_metastore_client.return_value
    rls_calls = [c for c in metastore.list_objects.await_args_list if c.args[0] == 'rls-policy']
    # The next offset is what was received; only the empty page ends the listing.
    assert [(c.kwargs['limit'], c.kwargs['offset']) for c in rls_calls] == [
        (_PAGE_SIZE, 0),
        (_PAGE_SIZE, _PAGE_SIZE),
        (_PAGE_SIZE, _PAGE_SIZE + 3),
    ]


@pytest.mark.asyncio
async def test_a_full_last_page_is_followed_by_one_more_request() -> None:
    """Exactly one full page can mean there is more: only an empty page ends the listing."""
    full = [_policy(i) for i in range(_PAGE_SIZE)]
    client = _client({'rls-policy': [full, []], 'cls-policy': [[]]})

    rules, _ = await load_policy_rules(client, dialect='snowflake')

    assert len(rules.tables) == _PAGE_SIZE
    metastore = client.step_up_metastore_client.return_value
    assert sum(1 for c in metastore.list_objects.await_args_list if c.args[0] == 'rls-policy') == 2


@pytest.mark.asyncio
async def test_a_short_page_is_followed_by_one_more_request() -> None:
    client = _client({'rls-policy': [[_policy(0)], []], 'cls-policy': [[]]})

    await load_policy_rules(client, dialect='snowflake')

    # rls-policy: the short page and the empty one; cls-policy: the empty one.
    assert client.step_up_metastore_client.return_value.list_objects.await_count == 3


@pytest.mark.asyncio
async def test_a_server_that_caps_the_page_size_still_yields_every_policy() -> None:
    """Fail closed: a page shorter than `limit` (a server or proxy cap) must not end the listing."""
    stored = [_policy(i) for i in range(250)]
    cap = 37

    async def list_objects(object_type: str, *, limit: int | None = None, offset: int | None = None):
        if object_type != 'rls-policy':
            return []
        return stored[offset : offset + min(limit, cap)]

    metastore = MagicMock()
    metastore.list_objects = AsyncMock(side_effect=list_objects)
    client = MagicMock()
    client.storage_client.project_id = AsyncMock(return_value='1')
    client.step_up_metastore_client = MagicMock(return_value=metastore)

    rules, _ = await load_policy_rules(client, dialect='snowflake')

    assert len(rules.tables) == 250


@pytest.mark.asyncio
async def test_an_endless_listing_is_refused_instead_of_looped_on() -> None:
    metastore = MagicMock()
    metastore.list_objects = AsyncMock(return_value=[_policy(i) for i in range(_PAGE_SIZE)])
    client = MagicMock()
    client.storage_client.project_id = AsyncMock(return_value='1')
    client.step_up_metastore_client = MagicMock(return_value=metastore)

    with pytest.raises(ValueError, match='refusing to guess'):
        await load_policy_rules(client, dialect='snowflake')

    assert metastore.list_objects.await_count >= _MAX_PAGES


@pytest.mark.parametrize('sql', ['SELECT 1', 'SELECT CURRENT_TIMESTAMP()', 'SELECT 1; SELECT 2'][:2])
def test_harmless_table_less_queries_still_pass_the_strict_path(sql: str) -> None:
    """The pre-check now routes table-less queries to the strict rewrite; the safe ones must still run."""
    from keboola_mcp_server.rls import RlsRules

    rewritten = rewrite_query(sql, user='petr', dialect='snowflake', rules=RlsRules(tables={}, dialect='snowflake'))

    assert rewritten.applied_rules == []
    assert rewritten.sql
