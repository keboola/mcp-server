from typing import Any

import httpx
import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.storage.shared_buckets import (
    MAX_SHARED_BUCKETS_LIMIT,
    GetSharedBucketsOutput,
    SharedBucketDetail,
    get_shared_buckets,
    link_shared_bucket,
)
from keboola_mcp_server.tools.storage.tools import BucketDetail


def _linked_bucket_detail_raw(bucket_id: str, **overrides: Any) -> dict[str, Any]:
    raw = {
        'id': bucket_id,
        'name': 'c-linked',
        'displayName': 'Linked bucket',
        'stage': bucket_id.split('.', 1)[0],
        'created': '2026-01-01T00:00:00+0000',
    }
    raw.update(overrides)
    return raw


def _shared_bucket_raw(bucket_id: str, **overrides: Any) -> dict[str, Any]:
    raw = {
        'id': bucket_id,
        'displayName': f'Display {bucket_id}',
        'stage': 'in',
        'description': 'A shared bucket.',
        'project': {'id': 'proj-1', 'name': 'Source Project'},
        'sharing': 'organization',
        'linkedBy': [],
        'tablesCount': 3,
        'rowsCount': 1000,
        'dataSizeBytes': 2048,
    }
    raw.update(overrides)
    return raw


class TestSharedBucketDetail:
    def test_flattens_project_fields(self) -> None:
        bucket = SharedBucketDetail.model_validate(_shared_bucket_raw('in.c-foo'))
        assert bucket.project_id == 'proj-1'
        assert bucket.project_name == 'Source Project'
        assert bucket.sharing == 'organization'
        assert bucket.tables_count == 3
        assert bucket.rows_count == 1000
        assert bucket.data_size_bytes == 2048

    def test_missing_project_key_leaves_project_fields_none(self) -> None:
        # The false branch of `if project := ...` in set_project_fields: no 'project' key at all.
        raw = _shared_bucket_raw('in.c-foo')
        del raw['project']

        bucket = SharedBucketDetail.model_validate(raw)

        assert bucket.project_id is None
        assert bucket.project_name is None

    def test_integer_project_id_is_accepted(self) -> None:
        # Storage API project ids come back as JSON integers on some payloads.
        bucket = SharedBucketDetail.model_validate(_shared_bucket_raw('in.c-foo', project={'id': 123, 'name': 'P'}))

        assert bucket.project_id == 123


@pytest.mark.asyncio
class TestGetSharedBuckets:
    async def test_returns_shared_buckets_sorted_by_id(
        self, mocker: MockerFixture, mcp_context_client: Context
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.shared_bucket_list = mocker.AsyncMock(
            return_value=[_shared_bucket_raw('in.c-zzz'), _shared_bucket_raw('in.c-aaa')]
        )

        result = await get_shared_buckets(mcp_context_client)

        assert isinstance(result, GetSharedBucketsOutput)
        assert [b.id for b in result.shared_buckets] == ['in.c-aaa', 'in.c-zzz']
        assert result.total_count == 2
        assert result.message == 'Returning 2 of 2 shared buckets.'

    async def test_empty_result_has_no_message(self, mocker: MockerFixture, mcp_context_client: Context) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.shared_bucket_list = mocker.AsyncMock(return_value=[])

        result = await get_shared_buckets(mcp_context_client)

        assert result.shared_buckets == []
        assert result.total_count == 0
        assert result.message is None

    @pytest.mark.parametrize(
        ('limit', 'offset', 'expected_ids', 'expected_message'),
        [
            (1, 0, ['in.c-a'], 'Returning 1 of 3 shared buckets. Use offset=1 to see more.'),
            (2, 1, ['in.c-b', 'in.c-c'], 'Returning 2 of 3 shared buckets.'),
            (10, 0, ['in.c-a', 'in.c-b', 'in.c-c'], 'Returning 3 of 3 shared buckets.'),
            (10, 100, [], 'Returning 0 of 3 shared buckets.'),
        ],
        ids=['first-page', 'second-page-exact-fit', 'limit-larger-than-total', 'offset-beyond-total'],
    )
    async def test_pagination(
        self,
        limit: int,
        offset: int,
        expected_ids: list[str],
        expected_message: str,
        mocker: MockerFixture,
        mcp_context_client: Context,
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.shared_bucket_list = mocker.AsyncMock(
            return_value=[_shared_bucket_raw('in.c-a'), _shared_bucket_raw('in.c-b'), _shared_bucket_raw('in.c-c')]
        )

        result = await get_shared_buckets(mcp_context_client, limit=limit, offset=offset)

        assert [b.id for b in result.shared_buckets] == expected_ids
        assert result.total_count == 3
        assert result.message == expected_message

    @pytest.mark.parametrize('bad_limit', [0, -1, MAX_SHARED_BUCKETS_LIMIT + 1])
    async def test_invalid_limit_falls_back_to_default(
        self, bad_limit: int, mocker: MockerFixture, mcp_context_client: Context
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.shared_bucket_list = mocker.AsyncMock(return_value=[_shared_bucket_raw('in.c-a')])

        result = await get_shared_buckets(mcp_context_client, limit=bad_limit)

        assert result.total_count == 1
        assert len(result.shared_buckets) == 1

    async def test_negative_offset_clamped_to_zero(self, mocker: MockerFixture, mcp_context_client: Context) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.shared_bucket_list = mocker.AsyncMock(return_value=[_shared_bucket_raw('in.c-a')])

        result = await get_shared_buckets(mcp_context_client, offset=-5)

        assert len(result.shared_buckets) == 1

    async def test_item_missing_id_raises_clear_error_not_bare_keyerror(
        self, mocker: MockerFixture, mcp_context_client: Context
    ) -> None:
        # Sorting must not crash with a bare KeyError on one malformed item -- SharedBucketDetail.id
        # is required, so validation still fails, but with a Pydantic error naming the field.
        raw_missing_id = _shared_bucket_raw('in.c-a')
        del raw_missing_id['id']
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.shared_bucket_list = mocker.AsyncMock(return_value=[raw_missing_id])

        with pytest.raises(Exception, match='id'):
            await get_shared_buckets(mcp_context_client)


@pytest.mark.asyncio
class TestLinkSharedBucket:
    @staticmethod
    def _mock_link_then_detail(mocker: MockerFixture, keboola_client, bucket_id: str, **detail_overrides: Any) -> None:
        # bucket_link's response shape isn't reliably documented (the reference PHP client only
        # ever reads `id` off it) -- the tool fetches bucket_detail explicitly afterwards, so tests
        # mock both calls rather than assuming bucket_link returns a full BucketDetail payload.
        keboola_client.storage_client.bucket_link = mocker.AsyncMock(return_value={'id': bucket_id})
        keboola_client.storage_client.bucket_detail = mocker.AsyncMock(
            return_value=_linked_bucket_detail_raw(bucket_id, **detail_overrides)
        )

    async def test_links_bucket_and_returns_bucket_detail(
        self, mocker: MockerFixture, mcp_context_client: Context
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        self._mock_link_then_detail(mocker, keboola_client, 'in.c-linked')

        result = await link_shared_bucket(
            mcp_context_client,
            source_project_id='proj-1',
            source_bucket_id='in.c-foo',
            target_bucket_name='linked',
        )

        assert isinstance(result, BucketDetail)
        assert result.id == 'in.c-linked'
        keboola_client.storage_client.bucket_link.assert_called_once_with(
            name='linked',
            stage='in',
            source_project_id='proj-1',
            source_bucket_id='in.c-foo',
            display_name=None,
        )
        keboola_client.storage_client.bucket_detail.assert_called_once_with('in.c-linked')

    async def test_accepts_integer_source_project_id(self, mocker: MockerFixture, mcp_context_client: Context) -> None:
        # Storage API project ids come back as JSON integers; the tool must not require the
        # caller to pre-stringify them.
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        self._mock_link_then_detail(mocker, keboola_client, 'in.c-linked')

        await link_shared_bucket(
            mcp_context_client,
            source_project_id=123,
            source_bucket_id='in.c-foo',
            target_bucket_name='linked',
        )

        assert keboola_client.storage_client.bucket_link.call_args.kwargs['source_project_id'] == '123'

    async def test_stage_derived_from_out_prefix(self, mocker: MockerFixture, mcp_context_client: Context) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        self._mock_link_then_detail(mocker, keboola_client, 'out.c-linked')

        await link_shared_bucket(
            mcp_context_client,
            source_project_id='proj-1',
            source_bucket_id='out.c-foo',
            target_bucket_name='linked',
        )

        assert keboola_client.storage_client.bucket_link.call_args.kwargs['stage'] == 'out'

    async def test_explicit_target_stage_overrides_derived_stage(
        self, mocker: MockerFixture, mcp_context_client: Context
    ) -> None:
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        self._mock_link_then_detail(mocker, keboola_client, 'out.c-linked')

        await link_shared_bucket(
            mcp_context_client,
            source_project_id='proj-1',
            source_bucket_id='in.c-foo',
            target_bucket_name='linked',
            target_stage='out',
        )

        assert keboola_client.storage_client.bucket_link.call_args.kwargs['stage'] == 'out'

    async def test_unparseable_stage_raises(self, mcp_context_client: Context) -> None:
        with pytest.raises(ToolError, match='Could not determine stage'):
            await link_shared_bucket(
                mcp_context_client,
                source_project_id='proj-1',
                source_bucket_id='no-stage-prefix',
                target_bucket_name='linked',
            )

    @pytest.mark.parametrize(
        ('status_code', 'reason'),
        [
            (400, 'already exists'),
            (409, 'already linked'),
            (403, 'forbidden'),
        ],
        ids=['already-exists-400', 'already-linked-409', 'no-access-403'],
    )
    async def test_bucket_link_http_error_propagates(
        self, status_code: int, reason: str, mocker: MockerFixture, mcp_context_client: Context
    ) -> None:
        # The most likely real-world failure: the agent lists shares and links one that's already
        # linked or lacks access. This isn't wrapped into a friendlier message today -- the raw
        # HTTP error propagates -- but it must propagate, not get swallowed.
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        response = httpx.Response(
            status_code, request=httpx.Request('POST', 'https://x/v2/storage/branch/default/buckets')
        )
        keboola_client.storage_client.bucket_link = mocker.AsyncMock(
            side_effect=httpx.HTTPStatusError(reason, request=response.request, response=response)
        )

        with pytest.raises(httpx.HTTPStatusError):
            await link_shared_bucket(
                mcp_context_client,
                source_project_id='proj-1',
                source_bucket_id='in.c-foo',
                target_bucket_name='linked',
            )
