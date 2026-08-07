import pytest

from tests.session_store.conftest import requires_postgres

pytestmark = [pytest.mark.asyncio, requires_postgres]


async def test_upsert_and_get(kai_store) -> None:
    await kai_store.upsert('conv-1', 42, project_ids=[18, 83], read_only=False, confirmed=True)

    scope = await kai_store.get('conv-1', 42)

    assert scope is not None
    assert scope.project_ids == [18, 83]
    assert scope.read_only is False
    assert scope.confirmed is True


async def test_get_unknown_returns_none(kai_store) -> None:
    assert await kai_store.get('does-not-exist', 1) is None


async def test_different_user_id_is_a_different_row(kai_store) -> None:
    # Same conversation_id, different user -- must not collide (the whole point of the
    # composite key, see pat_token_support/RFC.md increment 6).
    await kai_store.upsert('conv-1', 42, project_ids=[18], read_only=False, confirmed=True)

    assert await kai_store.get('conv-1', 999) is None


async def test_upsert_overwrites_existing_row(kai_store) -> None:
    await kai_store.upsert('conv-1', 42, project_ids=[18], read_only=False, confirmed=True)
    await kai_store.upsert('conv-1', 42, project_ids=[18, 83], read_only=True, confirmed=True)

    scope = await kai_store.get('conv-1', 42)

    assert scope.project_ids == [18, 83]
    assert scope.read_only is True


async def test_drop_removes_the_row(kai_store) -> None:
    await kai_store.upsert('conv-1', 42, project_ids=[18], read_only=False, confirmed=True)

    await kai_store.drop('conv-1', 42)

    assert await kai_store.get('conv-1', 42) is None
