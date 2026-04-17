"""Migration of local data and component configs to the Keboola platform."""

import logging
from pathlib import Path
from typing import Literal

import httpx
from pydantic import BaseModel, Field

from keboola_mcp_server.tools.local.config import ComponentConfig

LOG = logging.getLogger(__name__)

_TIMEOUT = 60  # seconds per API call


# ---------------------------------------------------------------------------
# Output models
# ---------------------------------------------------------------------------


class TableMigrateResult(BaseModel):
    name: str = Field(description='Table name (CSV stem).')
    status: Literal['uploaded', 'already_exists', 'error'] = Field(description='Result of the upload attempt.')
    table_id: str | None = Field(default=None, description='Keboola Storage table ID after upload.')
    message: str | None = Field(default=None, description='Error message on failure.')


class ConfigMigrateResult(BaseModel):
    config_id: str = Field(description='Local config ID.')
    component_id: str = Field(description='Keboola component ID.')
    status: Literal['created', 'already_exists', 'error'] = Field(description='Result of the config creation.')
    kbc_config_id: str | None = Field(default=None, description='Keboola config ID after creation.')
    message: str | None = Field(default=None, description='Error message on failure.')


class MigrateResult(BaseModel):
    bucket_id: str = Field(description='Keboola Storage bucket where tables were uploaded.')
    tables: list[TableMigrateResult] = Field(default_factory=list, description='Per-table migration results.')
    configs: list[ConfigMigrateResult] = Field(default_factory=list, description='Per-config migration results.')
    tables_ok: int = Field(default=0, description='Number of tables successfully uploaded.')
    tables_error: int = Field(default=0, description='Number of tables that failed to upload.')
    configs_ok: int = Field(default=0, description='Number of configs successfully created.')
    configs_error: int = Field(default=0, description='Number of configs that failed.')


# ---------------------------------------------------------------------------
# Storage API helpers
# ---------------------------------------------------------------------------


def _auth_headers(token: str) -> dict[str, str]:
    return {'X-StorageApi-Token': token}


async def _ensure_bucket(client: httpx.AsyncClient, api_url: str, token: str, bucket_id: str) -> None:
    """Create the bucket if it does not already exist. bucket_id like 'in.c-local'."""
    stage, name = bucket_id.split('.', 1)
    if name.startswith('c-'):
        name = name[2:]
    resp = await client.post(
        f'{api_url}/v2/storage/buckets',
        headers=_auth_headers(token),
        data={'stage': stage, 'name': name},
    )
    # 200 = created, 422 = already exists — both are OK
    if resp.status_code not in (200, 201, 422):
        resp.raise_for_status()


async def _upload_table(
    client: httpx.AsyncClient,
    api_url: str,
    token: str,
    bucket_id: str,
    table_name: str,
    csv_path: Path,
) -> TableMigrateResult:
    """Upload a CSV file as a new table. Detects 'already exists' (422) as its own status."""
    try:
        csv_bytes = csv_path.read_bytes()
        resp = await client.post(
            f'{api_url}/v2/storage/buckets/{bucket_id}/tables',
            headers=_auth_headers(token),
            data={'name': table_name, 'incremental': '0'},
            files={'data': (f'{table_name}.csv', csv_bytes, 'text/csv')},
        )
        if resp.status_code in (200, 201):
            body = resp.json()
            return TableMigrateResult(
                name=table_name,
                status='uploaded',
                table_id=body.get('id'),
            )
        if resp.status_code == 422:
            body = resp.json()
            # Keboola returns 422 with "already exists" message
            return TableMigrateResult(name=table_name, status='already_exists', message=body.get('error'))
        resp.raise_for_status()
        return TableMigrateResult(name=table_name, status='error', message=f'HTTP {resp.status_code}')
    except httpx.HTTPStatusError as exc:
        return TableMigrateResult(name=table_name, status='error', message=str(exc))
    except Exception as exc:
        return TableMigrateResult(name=table_name, status='error', message=str(exc))


async def _create_config(
    client: httpx.AsyncClient,
    api_url: str,
    token: str,
    config: ComponentConfig,
) -> ConfigMigrateResult:
    """Create a component configuration via the Storage API."""
    import json as _json

    try:
        resp = await client.post(
            f'{api_url}/v2/storage/components/{config.component_id}/configs',
            headers=_auth_headers(token),
            data={
                'name': config.name,
                'configuration': _json.dumps({'parameters': config.parameters}),
            },
        )
        if resp.status_code in (200, 201):
            body = resp.json()
            return ConfigMigrateResult(
                config_id=config.config_id,
                component_id=config.component_id,
                status='created',
                kbc_config_id=body.get('id'),
            )
        if resp.status_code == 422:
            body = resp.json()
            return ConfigMigrateResult(
                config_id=config.config_id,
                component_id=config.component_id,
                status='already_exists',
                message=body.get('error'),
            )
        resp.raise_for_status()
        return ConfigMigrateResult(
            config_id=config.config_id,
            component_id=config.component_id,
            status='error',
            message=f'HTTP {resp.status_code}',
        )
    except httpx.HTTPStatusError as exc:
        return ConfigMigrateResult(
            config_id=config.config_id,
            component_id=config.component_id,
            status='error',
            message=str(exc),
        )
    except Exception as exc:
        return ConfigMigrateResult(
            config_id=config.config_id,
            component_id=config.component_id,
            status='error',
            message=str(exc),
        )


# ---------------------------------------------------------------------------
# Main migrate function
# ---------------------------------------------------------------------------


async def migrate_to_keboola(
    data_dir: Path,
    storage_api_url: str,
    storage_token: str,
    table_names: list[str] | None = None,
    config_ids: list[str] | None = None,
    bucket_id: str = 'in.c-local',
) -> MigrateResult:
    """Upload local CSV tables and saved component configs to Keboola Storage.

    Tables are uploaded to *bucket_id* (default ``in.c-local``).
    Component configs are created under their respective component IDs.
    Passing *table_names* / *config_ids* restricts which items are migrated;
    omitting them migrates everything in the catalog.
    """
    from keboola_mcp_server.tools.local.config import list_configs as _list_configs

    result = MigrateResult(bucket_id=bucket_id)
    tables_dir = data_dir / 'tables'
    configs_dir = data_dir / 'configs'

    # Collect tables to migrate
    all_csv = sorted(tables_dir.glob('*.csv')) if tables_dir.exists() else []
    if table_names is not None:
        name_set = set(table_names)
        csv_files = [p for p in all_csv if p.stem in name_set]
    else:
        csv_files = all_csv

    # Collect configs to migrate
    all_configs = _list_configs(configs_dir)
    if config_ids is not None:
        id_set = set(config_ids)
        configs = [c for c in all_configs if c.config_id in id_set]
    else:
        configs = all_configs

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        if csv_files or configs:
            try:
                await _ensure_bucket(client, storage_api_url, storage_token, bucket_id)
            except Exception as exc:
                # If bucket creation fails, mark all items as errors
                for p in csv_files:
                    result.tables.append(
                        TableMigrateResult(name=p.stem, status='error', message=f'Bucket error: {exc}')
                    )
                for cfg in configs:
                    result.configs.append(
                        ConfigMigrateResult(
                            config_id=cfg.config_id,
                            component_id=cfg.component_id,
                            status='error',
                            message=f'Bucket error: {exc}',
                        )
                    )
                result.tables_error = len(csv_files)
                result.configs_error = len(configs)
                return result

        for csv_path in csv_files:
            tr = await _upload_table(client, storage_api_url, storage_token, bucket_id, csv_path.stem, csv_path)
            result.tables.append(tr)
            if tr.status in ('uploaded', 'already_exists'):
                result.tables_ok += 1
            else:
                result.tables_error += 1

        for cfg in configs:
            cr = await _create_config(client, storage_api_url, storage_token, cfg)
            result.configs.append(cr)
            if cr.status in ('created', 'already_exists'):
                result.configs_ok += 1
            else:
                result.configs_error += 1

    return result
