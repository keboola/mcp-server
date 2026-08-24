"""Partition maintenance for oauth_sessions (RFC oauth_session_persistence, "Session expiry /
cleanup"). Two responsibilities, both idempotent and safe to re-run or to have missed a run (each
call computes everything from "now", not from a last-run watermark):

  - Ensure a partition exists for the current month and the next, so writes never fail for lack of
    one -- a RANGE-partitioned INSERT with no matching partition raises immediately, it does not
    fall through to a partition created moments later.
  - Drop partitions whose entire month is older than the retention window.

Intended to run as a recurring job (`keboola-mcp-server gc-sessions`), separate from the
deploy-time `migrate` command -- deploys don't happen on a reliable cadence, so this can't
piggyback on that hook. Safe to run more often than monthly: the exists-check on creation and the
month-boundary check on drops make repeated runs within the same month no-ops.
"""

import logging
import re
from datetime import date, datetime, timezone

import asyncpg

LOG = logging.getLogger(__name__)

DEFAULT_RETENTION_MONTHS = 2

_PARTITION_NAME_RE = re.compile(r'^oauth_sessions_(\d{4})_(\d{2})$')


def _month_start(d: date) -> date:
    return d.replace(day=1)


def _add_months(d: date, n: int) -> date:
    month_index = d.month - 1 + n
    year = d.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def _partition_name(month_start: date) -> str:
    return f'oauth_sessions_{month_start:%Y_%m}'


async def ensure_partitions(
    pool: asyncpg.Pool, *, retention_months: int = DEFAULT_RETENTION_MONTHS
) -> dict[str, list[str]]:
    """Creates this month's + next month's partition if missing; drops partitions entirely older
    than ``retention_months`` back from the current month.

    :return: ``{'created': [...], 'dropped': [...]}`` partition names, for the CLI to report.
    """
    this_month = _month_start(datetime.now(timezone.utc).date())
    # retention_months counts the current month, so keep (retention_months - 1) months before it --
    # e.g. retention_months=2 on an August run keeps July + August, drops June.
    cutoff = _add_months(this_month, -(retention_months - 1))

    created: list[str] = []
    async with pool.acquire() as conn:
        for offset in (0, 1):
            start = _add_months(this_month, offset)
            end = _add_months(this_month, offset + 1)
            name = _partition_name(start)
            exists = await conn.fetchval('SELECT to_regclass($1) IS NOT NULL', name)
            if not exists:
                # DDL bounds can't be bound query parameters -- start/end are computed, not user
                # input, so direct formatting is safe.
                #
                # Postgres refuses to attach a new partition while default holds matching rows
                # (e.g. migration 0002's backlog copy), so move any such rows in first.
                async with conn.transaction():
                    await conn.execute(f'CREATE TABLE {name} (LIKE oauth_sessions INCLUDING ALL)')
                    await conn.execute(
                        f'WITH moved AS ('
                        f'    DELETE FROM oauth_sessions_default '
                        f"    WHERE created_at >= '{start.isoformat()}' AND created_at < '{end.isoformat()}' "
                        f'    RETURNING *'
                        f') INSERT INTO {name} SELECT * FROM moved'
                    )
                    await conn.execute(
                        f'ALTER TABLE oauth_sessions ATTACH PARTITION {name} '
                        f"FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}')"
                    )
                    # The copied (access_token_hash, created_at) index doesn't actually enforce hash
                    # uniqueness (created_at differs per row) -- a plain index on the partition
                    # table itself, without the partition key, does.
                    await conn.execute(
                        f'CREATE UNIQUE INDEX {name}_access_token_hash_uidx ON {name} (access_token_hash)'
                    )
                    await conn.execute(
                        f'CREATE UNIQUE INDEX {name}_refresh_token_hash_uidx ON {name} (refresh_token_hash) '
                        f'WHERE refresh_token_hash IS NOT NULL'
                    )
                LOG.info(f'Created oauth_sessions partition: {name} [{start}, {end})')
                created.append(name)

        rows = await conn.fetch("SELECT tablename FROM pg_tables WHERE tablename LIKE 'oauth_sessions_%'")
        dropped: list[str] = []
        for row in rows:
            name = row['tablename']
            match = _PARTITION_NAME_RE.match(name)
            if match is None:
                continue  # oauth_sessions_default / oauth_sessions_pre_partition -- not a month partition
            partition_month = date(int(match.group(1)), int(match.group(2)), 1)
            if partition_month < cutoff:
                await conn.execute(f'DROP TABLE IF EXISTS {name}')
                LOG.info(f'Dropped expired oauth_sessions partition: {name} (older than {cutoff})')
                dropped.append(name)

    return {'created': created, 'dropped': dropped}
