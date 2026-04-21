"""Local-mode MCP tool implementations backed by the local filesystem and DuckDB."""

import asyncio
import logging
from pathlib import Path
from typing import Annotated, Literal

from fastmcp import FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.tools.local.backend import LocalBackend
from keboola_mcp_server.tools.local.config import ComponentConfig, ConfigsOutput
from keboola_mcp_server.tools.local.dataapp import (
    DataAppChartConfig,
    DataAppConfig,
    DataAppInfo,
    DataAppRunResult,
    DataAppsOutput,
    DataAppStopResult,
)
from keboola_mcp_server.tools.local.docker import ComponentRunResult, ComponentSetupResult
from keboola_mcp_server.tools.local.migrate import MigrateResult
from keboola_mcp_server.tools.local.schema import ComponentSchemaResult, ComponentSearchResult

LOG = logging.getLogger(__name__)

LOCAL_TOOLS_TAG = 'local'


# ---------------------------------------------------------------------------
# Output models
# ---------------------------------------------------------------------------


class LocalTableInfo(BaseModel):
    name: str = Field(description='Table name (CSV stem).')
    columns: list[str] = Field(description='Column names read from the CSV header.')
    rows_count: int | None = Field(default=None, description='Row count (None if unreadable).')
    size_bytes: int | None = Field(default=None, description='File size in bytes.')


class LocalTablesOutput(BaseModel):
    tables: list[LocalTableInfo] = Field(description='Local CSV tables.')
    total: int = Field(description='Total number of tables found.')


class LocalBucketInfo(BaseModel):
    id: str = Field(description='Virtual bucket ID.')
    name: str = Field(description='Bucket name.')
    stage: Literal['in'] = Field(description='Bucket stage (always "in" for local mode).')
    tables_count: int = Field(description='Number of CSV tables in the bucket.')
    table_names: list[str] = Field(description='Names of tables in this bucket.')


class LocalBucketsOutput(BaseModel):
    buckets: list[LocalBucketInfo] = Field(description='Virtual local buckets.')


class LocalSearchResult(BaseModel):
    name: str = Field(description='CSV table name (stem).')
    match_type: Literal['filename', 'column'] = Field(description='Where the query matched.')
    matched_value: str = Field(description='The filename or column name that matched.')


class LocalSearchOutput(BaseModel):
    results: list[LocalSearchResult] = Field(description='Search results.')
    query: str = Field(description='The search query used.')


class LocalProjectInfo(BaseModel):
    mode: Literal['local'] = Field(default='local', description='Server mode.')
    data_dir: str = Field(description='Absolute path to the local data directory.')
    table_count: int = Field(description='Number of CSV tables in the catalog.')
    config_count: int = Field(description='Number of saved component configurations.')
    sql_engine: Literal['DuckDB'] = Field(default='DuckDB', description='SQL engine used for local queries.')
    llm_instruction: str = Field(description='Base instructions for working in local mode.')


class LocalComponentSearchOutput(BaseModel):
    results: list[ComponentSearchResult] = Field(description='Matching components from the Developer Portal.')
    query: str = Field(description='The search query used.')


# ---------------------------------------------------------------------------
# Implementation functions (standalone, injectable for tests)
# ---------------------------------------------------------------------------

_LOCAL_PROJECT_INSTRUCTION = (
    'You are working in local mode. Data is stored as CSV files on disk. '
    'Use get_tables to list available tables, query_data to run DuckDB SQL queries, '
    'and search to find tables or columns by name. '
    'Table names in SQL correspond to CSV file stems (e.g. customers.csv → SELECT * FROM customers). '
    'Use write_table to add or overwrite a table and delete_table to remove one. '
    '\n'
    'Component configs: save_config / get_configs / delete_config / run_saved_config. '
    'Components run via Docker: setup_component (clone and build) then run_component (execute). '
    'Discover components with find_component_id; inspect their config schema with get_component_schema. '
    '\n'
    'Dashboards: create_data_app generates a self-contained HTML dashboard from DuckDB SQL queries. '
    'run_data_app serves it on localhost and returns the browser URL. '
    'list_data_apps / stop_data_app / delete_data_app manage the dashboard lifecycle. '
    '\n'
    'Migration: migrate_to_keboola uploads local CSV tables and saved configs to Keboola platform. '
    'Pass storage_api_url and storage_token as arguments, or start the server with '
    '--api-url and --storage-token so the tool can be called without credentials each time. '
    '\n'
    'NOT AVAILABLE in local mode (no platform connection): '
    'run_job / get_jobs — use run_saved_config or run_component instead; '
    'create_flow / update_flow / create_conditional_flow — not available; '
    'create_sql_transformation / update_sql_transformation — use query_data to run SQL directly; '
    'deploy_data_app / modify_data_app — use create_data_app and run_data_app instead; '
    'docs_query / search_semantic_context — no Keboola AI Service in local mode; '
    'create_config / update_config — use save_config instead.'
)


async def get_tables_local(
    local_backend: LocalBackend,
    table_names: list[str] | None = None,
) -> LocalTablesOutput:
    """Implementation of get_tables for local mode."""
    csv_paths = local_backend.list_csv_tables()
    tables: list[LocalTableInfo] = []
    for path in csv_paths:
        stem = path.stem
        if table_names and stem not in table_names:
            continue
        columns = local_backend.read_csv_headers(path)
        try:
            size_bytes = path.stat().st_size
        except OSError:
            size_bytes = None
        try:
            rows_count = _count_csv_rows(path)
        except Exception:
            rows_count = None
        tables.append(LocalTableInfo(name=stem, columns=columns, rows_count=rows_count, size_bytes=size_bytes))
    return LocalTablesOutput(tables=tables, total=len(tables))


async def get_buckets_local(local_backend: LocalBackend) -> LocalBucketsOutput:
    """Implementation of get_buckets for local mode (single virtual bucket)."""
    csv_paths = local_backend.list_csv_tables()
    table_names = [p.stem for p in csv_paths]
    bucket = LocalBucketInfo(
        id='local',
        name='local',
        stage='in',
        tables_count=len(table_names),
        table_names=table_names,
    )
    return LocalBucketsOutput(buckets=[bucket])


async def query_data_local(local_backend: LocalBackend, sql_query: str, query_name: str) -> str:
    """Implementation of query_data for local mode."""
    return local_backend.query_local(sql_query)


async def search_local(
    local_backend: LocalBackend,
    query: str,
    item_types: list[str] | None = None,
) -> LocalSearchOutput:
    """Implementation of search for local mode (filename + column header matching)."""
    q = query.lower()
    results: list[LocalSearchResult] = []
    seen: set[tuple[str, str]] = set()

    for path in local_backend.list_csv_tables():
        stem = path.stem

        # Filename match
        if q in stem.lower():
            key = (stem, 'filename')
            if key not in seen:
                results.append(LocalSearchResult(name=stem, match_type='filename', matched_value=stem))
                seen.add(key)

        # Column header match
        for col in local_backend.read_csv_headers(path):
            if q in col.lower():
                key = (stem, col)
                if key not in seen:
                    results.append(LocalSearchResult(name=stem, match_type='column', matched_value=col))
                    seen.add(key)

    return LocalSearchOutput(results=results, query=query)


async def get_project_info_local(local_backend: LocalBackend) -> LocalProjectInfo:
    """Implementation of get_project_info for local mode."""
    table_count = len(local_backend.list_csv_tables())
    config_count = len(local_backend.list_configs())
    return LocalProjectInfo(
        data_dir=str(local_backend.data_dir.resolve()),
        table_count=table_count,
        config_count=config_count,
        llm_instruction=_LOCAL_PROJECT_INSTRUCTION,
    )


async def setup_component_local(
    local_backend: LocalBackend,
    git_url: str,
    force_rebuild: bool = False,
) -> ComponentSetupResult:
    """Implementation of setup_component for local mode."""
    return await asyncio.to_thread(local_backend.setup_component, git_url, force_rebuild)


async def run_component_local(
    local_backend: LocalBackend,
    parameters: dict,
    component_image: str | None = None,
    git_url: str | None = None,
    input_tables: list[str] | None = None,
    memory_limit: str = '4g',
) -> ComponentRunResult:
    """Implementation of run_component for local mode."""
    if component_image and git_url:
        raise ValueError('Provide either component_image or git_url, not both.')
    if not component_image and not git_url:
        raise ValueError('Provide either component_image (Docker registry) or git_url (source).')
    if component_image:
        return await asyncio.to_thread(
            local_backend.run_docker_component, component_image, parameters, input_tables, memory_limit
        )
    return await asyncio.to_thread(local_backend.run_source_component, git_url, parameters, input_tables, memory_limit)


async def get_component_schema_local(component_id: str) -> ComponentSchemaResult:
    """Implementation of get_component_schema for local mode."""
    from keboola_mcp_server.tools.local.schema import get_component_schema as _fetch_schema

    return await _fetch_schema(component_id)


async def find_component_id_local(query: str, limit: int = 10) -> LocalComponentSearchOutput:
    """Implementation of find_component_id for local mode."""
    from keboola_mcp_server.tools.local.schema import find_component_id as _search_components

    results = await _search_components(query, limit)
    return LocalComponentSearchOutput(results=results, query=query)


async def write_table_local(local_backend: LocalBackend, name: str, csv_content: str) -> LocalTableInfo:
    """Implementation of write_table for local mode."""
    path = local_backend.write_csv_table(name, csv_content)
    columns = local_backend.read_csv_headers(path)
    try:
        size_bytes = path.stat().st_size
    except OSError:
        size_bytes = None
    try:
        rows_count = _count_csv_rows(path)
    except Exception:
        rows_count = None
    return LocalTableInfo(name=path.stem, columns=columns, rows_count=rows_count, size_bytes=size_bytes)


async def delete_table_local(local_backend: LocalBackend, name: str) -> dict:
    """Implementation of delete_table for local mode."""
    deleted = local_backend.delete_csv_table(name)
    return {'deleted': deleted, 'name': name}


async def save_config_local(
    local_backend: LocalBackend,
    config_id: str,
    component_id: str,
    name: str,
    parameters: dict,
    component_image: str | None = None,
    git_url: str | None = None,
) -> ComponentConfig:
    """Implementation of save_config for local mode."""
    config = ComponentConfig(
        config_id=config_id,
        component_id=component_id,
        name=name,
        parameters=parameters,
        component_image=component_image,
        git_url=git_url,
    )
    return local_backend.save_config(config)


async def list_configs_local(local_backend: LocalBackend) -> ConfigsOutput:
    """Implementation of list_configs for local mode."""
    configs = local_backend.list_configs()
    return ConfigsOutput(configs=configs, total=len(configs))


async def delete_config_local(local_backend: LocalBackend, config_id: str) -> dict:
    """Implementation of delete_config for local mode."""
    deleted = local_backend.delete_config(config_id)
    return {'deleted': deleted, 'config_id': config_id}


async def run_saved_config_local(
    local_backend: LocalBackend,
    config_id: str,
    input_tables: list[str] | None = None,
    memory_limit: str = '4g',
) -> ComponentRunResult:
    """Implementation of run_saved_config for local mode."""
    config = local_backend.load_config(config_id)
    if not config.component_image and not config.git_url:
        raise ValueError(f'Config {config_id!r} has neither component_image nor git_url — cannot run.')
    return await run_component_local(
        local_backend,
        config.parameters,
        component_image=config.component_image,
        git_url=config.git_url,
        input_tables=input_tables,
        memory_limit=memory_limit,
    )


async def migrate_to_keboola_local(
    local_backend: LocalBackend,
    storage_api_url: str | None,
    storage_token: str | None,
    table_names: list[str] | None = None,
    config_ids: list[str] | None = None,
    bucket_id: str = 'in.c-local',
) -> MigrateResult:
    """Implementation of migrate_to_keboola for local mode."""
    resolved_url = storage_api_url or local_backend.storage_api_url
    resolved_token = storage_token or local_backend.storage_token
    if not resolved_url or not resolved_token:
        raise ValueError(
            'Storage API URL and token are required. '
            'Pass them as arguments or start the server with --api-url and --storage-token.'
        )
    return await local_backend.migrate_to_keboola(
        storage_api_url=resolved_url,
        storage_token=resolved_token,
        table_names=table_names,
        config_ids=config_ids,
        bucket_id=bucket_id,
    )


# ---------------------------------------------------------------------------
# Data app implementations
# ---------------------------------------------------------------------------


def _validate_app_name(name: str) -> str:
    name = name.strip().lower()
    if not name or '/' in name or '\\' in name or '..' in name:
        raise ValueError(f'Invalid app name: {name!r}. Use lowercase letters, digits, and hyphens.')
    return name


async def create_data_app_local(
    local_backend: LocalBackend,
    name: str,
    title: str,
    charts: list[DataAppChartConfig],
    description: str = '',
    tables: list[str] | None = None,
) -> DataAppInfo:
    """Create or update a local data app dashboard."""
    from datetime import datetime, timezone

    name = _validate_app_name(name)

    # Auto-detect tables from chart SQL if not specified
    if not tables:
        available = {p.stem for p in local_backend.list_csv_tables()}
        tables = sorted(available)

    now = datetime.now(timezone.utc).isoformat()
    existing_created_at = now
    try:
        existing = local_backend.load_data_app(name)
        existing_created_at = existing.created_at or now
    except FileNotFoundError:
        pass

    config = DataAppConfig(
        name=name,
        title=title,
        description=description,
        tables=tables,
        charts=charts,
        created_at=existing_created_at,
        updated_at=now,
    )

    local_backend.save_data_app(config)

    port = local_backend.get_running_port(name)
    return DataAppInfo(
        name=name,
        title=title,
        description=description,
        chart_count=len(charts),
        status='running' if port else 'stopped',
        app_url=f'http://localhost:{port}/apps/{name}/' if port else None,
        port=port,
    )


async def run_data_app_local(local_backend: LocalBackend, name: str) -> DataAppRunResult:
    """Start the HTTP server for a data app."""
    name = _validate_app_name(name)

    port = local_backend.get_running_port(name)
    if port is not None:
        return DataAppRunResult(
            name=name,
            status='already_running',
            app_url=f'http://localhost:{port}/apps/{name}/',
            port=port,
            message=f'Already running on port {port}.',
        )

    try:
        _, port = local_backend.start_data_app(name)
    except FileNotFoundError:
        return DataAppRunResult(
            name=name, status='error', message=f'App {name!r} not found. Call create_data_app first.'
        )
    except RuntimeError as exc:
        return DataAppRunResult(name=name, status='error', message=str(exc))

    url = f'http://localhost:{port}/apps/{name}/'
    return DataAppRunResult(name=name, status='started', app_url=url, port=port, message=f'Open {url} in your browser.')


async def list_data_apps_local(local_backend: LocalBackend) -> DataAppsOutput:
    """List all local data apps with running status."""
    configs = local_backend.list_data_apps()
    apps = []
    for config in configs:
        port = local_backend.get_running_port(config.name)
        apps.append(
            DataAppInfo(
                name=config.name,
                title=config.title,
                description=config.description,
                chart_count=len(config.charts),
                status='running' if port else 'stopped',
                app_url=f'http://localhost:{port}/apps/{config.name}/' if port else None,
                port=port,
            )
        )
    return DataAppsOutput(apps=apps, total=len(apps))


async def stop_data_app_local(local_backend: LocalBackend, name: str) -> DataAppStopResult:
    """Stop the HTTP server for a data app."""
    name = _validate_app_name(name)
    stopped = local_backend.stop_data_app(name)
    if stopped:
        return DataAppStopResult(name=name, stopped=True, message=f'App {name!r} stopped.')
    return DataAppStopResult(name=name, stopped=False, message=f'App {name!r} was not running.')


async def delete_data_app_local(local_backend: LocalBackend, name: str) -> DataAppStopResult:
    """Stop (if running) and delete a data app."""
    name = _validate_app_name(name)
    local_backend.stop_data_app(name)
    deleted = local_backend.delete_data_app(name)
    if deleted:
        return DataAppStopResult(name=name, stopped=True, message=f'App {name!r} deleted.')
    return DataAppStopResult(name=name, stopped=False, message=f'App {name!r} not found.')


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------


def register_local_tools(mcp: FastMCP, local_backend: LocalBackend) -> None:
    """Register all local-mode tools with the MCP server."""

    @tool_errors()
    async def get_tables(
        table_names: Annotated[
            list[str] | None,
            Field(default=None, description='Filter by specific table names (CSV stems). Omit to list all tables.'),
        ] = None,
    ) -> LocalTablesOutput:
        """
        Lists CSV tables in the local data catalog.

        Scans <data-dir>/tables/ for .csv files and returns their name, columns, row count, and size.
        Use table names returned here directly in SQL (e.g. SELECT * FROM customers).
        """
        return await get_tables_local(local_backend, table_names)

    mcp.add_tool(
        FunctionTool.from_function(
            get_tables,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def get_buckets() -> LocalBucketsOutput:
        """
        Returns the virtual local bucket containing all CSV tables.

        In local mode there is always exactly one bucket named "local".
        """
        return await get_buckets_local(local_backend)

    mcp.add_tool(
        FunctionTool.from_function(
            get_buckets,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def query_data(
        sql_query: Annotated[str, Field(description='DuckDB SQL SELECT query to execute against local CSV tables.')],
        query_name: Annotated[
            str,
            Field(
                description=(
                    'A concise, human-readable name for this query based on its purpose. '
                    'Use normal words with spaces (e.g., "Top Customers by Revenue").'
                )
            ),
        ],
    ) -> str:
        """
        Executes a DuckDB SQL query against local CSV files.

        CSV files in <data-dir>/tables/ are auto-registered as tables using their filename stem.
        Example: customers.csv → SELECT * FROM customers

        Use standard SQL with double-quoted identifiers for column names that contain spaces or special characters.
        """
        return await query_data_local(local_backend, sql_query, query_name)

    mcp.add_tool(
        FunctionTool.from_function(
            query_data,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def search(
        query: Annotated[str, Field(description='Search term to match against table names and column headers.')],
        item_types: Annotated[
            list[str] | None,
            Field(default=None, description='Ignored in local mode (all items are CSV tables).'),
        ] = None,
    ) -> LocalSearchOutput:
        """
        Searches local CSV tables by filename and column header.

        Returns tables whose name or column headers contain the query string (case-insensitive).
        """
        return await search_local(local_backend, query, item_types)

    mcp.add_tool(
        FunctionTool.from_function(
            search,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def get_project_info() -> LocalProjectInfo:
        """
        Returns metadata about the local project.

        Always call this at the start of a conversation in local mode to understand
        what data is available and how to query it.
        """
        return await get_project_info_local(local_backend)

    mcp.add_tool(
        FunctionTool.from_function(
            get_project_info,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def setup_component(
        git_url: Annotated[str, Field(description='Git URL of the Keboola component repository to clone and build.')],
        force_rebuild: Annotated[
            bool,
            Field(default=False, description='Force Docker image rebuild even if the sentinel file already exists.'),
        ] = False,
    ) -> ComponentSetupResult:
        """
        Clones a Keboola component repository and builds its Docker image.

        Run this before run_component to prepare a source-based component.
        Skips clone/build if already done (use force_rebuild=True to rebuild).
        Returns the clone path and any schema hints found in component.json / README.
        """
        return await setup_component_local(local_backend, git_url, force_rebuild)

    mcp.add_tool(
        FunctionTool.from_function(
            setup_component,
            annotations=ToolAnnotations(readOnlyHint=False),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def run_component(
        parameters: Annotated[dict, Field(description='Component configuration parameters (JSON object).')],
        component_image: Annotated[
            str | None,
            Field(
                default=None,
                description=(
                    'Docker image tag for a locally available image (e.g. "myimage:local"). '
                    'Only use this for images already present on the host — '
                    'ECR/registry images require AWS credentials not available locally. '
                    'Prefer git_url for open-source Keboola components.'
                ),
            ),
        ] = None,
        git_url: Annotated[
            str | None,
            Field(
                default=None,
                description=(
                    'Git URL of the component source repository (preferred for local execution). '
                    'The component will be cloned, built locally, and run via docker compose. '
                    'Use this for all open-source Keboola components (github.com/keboola). '
                    'Provide this OR component_image, not both.'
                ),
            ),
        ] = None,
        input_tables: Annotated[
            list[str] | None,
            Field(default=None, description='Table name stems to mount into /data/in/tables/ before running.'),
        ] = None,
        memory_limit: Annotated[
            str,
            Field(default='4g', description='Docker memory limit (e.g. "2g", "512m").'),
        ] = '4g',
    ) -> ComponentRunResult:
        """
        Runs a Keboola component via Docker using the Common Interface.

        Prefer git_url (clones and builds from the public GitHub repo) for open-source Keboola
        components — ECR registry images require AWS credentials not available locally.
        Input tables are copied from the local catalog into /data/in/tables/.
        Output tables written to /data/out/tables/ are collected back into the local catalog.
        """
        return await run_component_local(
            local_backend, parameters, component_image, git_url, input_tables, memory_limit
        )

    mcp.add_tool(
        FunctionTool.from_function(
            run_component,
            annotations=ToolAnnotations(readOnlyHint=False),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def get_component_schema(
        component_id: Annotated[str, Field(description='Keboola component ID (e.g. "keboola.ex-http").')],
    ) -> ComponentSchemaResult:
        """
        Fetches the configuration schema for a Keboola component from the Developer Portal.

        Returns the JSON schema for the component's parameters. Use this to understand
        what to pass in the parameters argument of run_component.
        """
        return await get_component_schema_local(component_id)

    mcp.add_tool(
        FunctionTool.from_function(
            get_component_schema,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def find_component_id(
        query: Annotated[str, Field(description='Search term to match against component names and IDs.')],
        limit: Annotated[
            int,
            Field(default=10, description='Maximum number of results to return.'),
        ] = 10,
    ) -> LocalComponentSearchOutput:
        """
        Searches the Keboola Developer Portal for components matching the query.

        Returns component IDs, names, types, and Docker images. Use get_component_schema
        with a returned component_id to fetch full parameter documentation.
        """
        return await find_component_id_local(query, limit)

    mcp.add_tool(
        FunctionTool.from_function(
            find_component_id,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def write_table(
        name: Annotated[
            str, Field(description='Table name (CSV stem, no extension). Used in SQL as: SELECT * FROM <name>.')
        ],
        csv_content: Annotated[str, Field(description='Full CSV content including header row.')],
    ) -> LocalTableInfo:
        """
        Writes a CSV file to the local data catalog.

        Creates or overwrites <data-dir>/tables/<name>.csv. The table is immediately
        available for queries via query_data. Use this to add new datasets, save
        intermediate results, or create test data.
        """
        return await write_table_local(local_backend, name, csv_content)

    mcp.add_tool(
        FunctionTool.from_function(
            write_table,
            annotations=ToolAnnotations(readOnlyHint=False),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def delete_table(
        name: Annotated[str, Field(description='Table name (CSV stem) to delete from the local catalog.')],
    ) -> dict:
        """
        Deletes a CSV table from the local data catalog.

        Removes <data-dir>/tables/<name>.csv permanently. Returns {"deleted": true}
        if the table existed, {"deleted": false} if it did not.
        """
        return await delete_table_local(local_backend, name)

    mcp.add_tool(
        FunctionTool.from_function(
            delete_table,
            annotations=ToolAnnotations(readOnlyHint=False),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def save_config(
        config_id: Annotated[
            str,
            Field(description='Unique identifier for this config (no spaces, e.g. "ex-http-001").'),
        ],
        component_id: Annotated[str, Field(description='Keboola component ID (e.g. "keboola.ex-http").')],
        name: Annotated[str, Field(description='Human-readable name for this configuration.')],
        parameters: Annotated[dict, Field(description='Component parameters object written to config.json.')],
        component_image: Annotated[
            str | None,
            Field(
                default=None,
                description=(
                    'Docker image tag for a locally available image. '
                    'Only for images already on the host — ECR requires AWS credentials. '
                    'Prefer git_url for open-source Keboola components.'
                ),
            ),
        ] = None,
        git_url: Annotated[
            str | None,
            Field(
                default=None,
                description=(
                    'Git URL of the component source repository (preferred). '
                    'Use this for all open-source Keboola components (github.com/keboola). '
                    'Provide this OR component_image.'
                ),
            ),
        ] = None,
    ) -> ComponentConfig:
        """
        Saves a component configuration to disk for later reuse.

        Stored at <data-dir>/configs/<config_id>.json. Use run_saved_config to
        execute it without repeating the parameters. Use migrate_to_keboola to
        push saved configs to the Keboola platform.
        Always use git_url for open-source Keboola components — ECR images require AWS credentials.
        """
        return await save_config_local(
            local_backend, config_id, component_id, name, parameters, component_image, git_url
        )

    mcp.add_tool(
        FunctionTool.from_function(
            save_config,
            annotations=ToolAnnotations(readOnlyHint=False),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def get_configs() -> ConfigsOutput:
        """
        Lists all saved component configurations.

        Returns configs stored under <data-dir>/configs/. Use config_id with
        run_saved_config to execute a saved config, or with migrate_to_keboola
        to push it to the Keboola platform.
        """
        return await list_configs_local(local_backend)

    mcp.add_tool(
        FunctionTool.from_function(
            get_configs,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def delete_config(
        config_id: Annotated[str, Field(description='Config ID to delete.')],
    ) -> dict:
        """
        Deletes a saved component configuration.

        Removes <data-dir>/configs/<config_id>.json. Returns {"deleted": true}
        if it existed, {"deleted": false} if not.
        """
        return await delete_config_local(local_backend, config_id)

    mcp.add_tool(
        FunctionTool.from_function(
            delete_config,
            annotations=ToolAnnotations(readOnlyHint=False),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def run_saved_config(
        config_id: Annotated[str, Field(description='Config ID to run (from list_configs).')],
        input_tables: Annotated[
            list[str] | None,
            Field(default=None, description='Override input table names from the catalog.'),
        ] = None,
        memory_limit: Annotated[
            str,
            Field(default='4g', description='Docker memory limit (e.g. "2g", "512m").'),
        ] = '4g',
    ) -> ComponentRunResult:
        """
        Runs a saved component configuration.

        Loads the config saved by save_config and executes run_component with its
        stored parameters, component_image or git_url. Equivalent to calling
        run_component with the same arguments but without re-specifying them.
        """
        return await run_saved_config_local(local_backend, config_id, input_tables, memory_limit)

    mcp.add_tool(
        FunctionTool.from_function(
            run_saved_config,
            annotations=ToolAnnotations(readOnlyHint=False),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def migrate_to_keboola(
        storage_api_url: Annotated[
            str | None,
            Field(
                default=None,
                description=(
                    'Keboola Storage API URL for your stack '
                    '(e.g. "https://connection.keboola.com" or '
                    '"https://connection.europe-west3.gcp.keboola.com"). '
                    'Omit if the server was started with --api-url.'
                ),
            ),
        ] = None,
        storage_token: Annotated[
            str | None,
            Field(
                default=None,
                description=(
                    'Keboola Storage API token with write access. '
                    'Omit if the server was started with --storage-token.'
                ),
            ),
        ] = None,
        table_names: Annotated[
            list[str] | None,
            Field(default=None, description='Table names to migrate. Omit to migrate all tables.'),
        ] = None,
        config_ids: Annotated[
            list[str] | None,
            Field(default=None, description='Config IDs to migrate. Omit to migrate all saved configs.'),
        ] = None,
        bucket_id: Annotated[
            str,
            Field(default='in.c-local', description='Target Keboola Storage bucket (created if it does not exist).'),
        ] = 'in.c-local',
    ) -> MigrateResult:
        """
        Uploads local CSV tables and saved component configs to the Keboola platform.

        Creates the target bucket if it does not exist, then uploads each CSV table
        and creates each component configuration via the Keboola Storage API.
        Tables already present in Keboola are reported as "already_exists" (not overwritten).

        Credentials can be omitted if the server was started with --api-url and --storage-token.
        """
        return await migrate_to_keboola_local(
            local_backend, storage_api_url, storage_token, table_names, config_ids, bucket_id
        )

    mcp.add_tool(
        FunctionTool.from_function(
            migrate_to_keboola,
            annotations=ToolAnnotations(readOnlyHint=False),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    # ------------------------------------------------------------------
    # Data app tools
    # ------------------------------------------------------------------

    @tool_errors()
    async def create_data_app(
        name: Annotated[
            str,
            Field(description='App identifier (lowercase, hyphens). Used as the directory name.'),
        ],
        title: Annotated[str, Field(description='Dashboard title shown in the browser.')],
        charts: Annotated[
            list[DataAppChartConfig],
            Field(
                description=(
                    'List of chart definitions. Each chart needs: id (slug), title, sql (DuckDB query), '
                    'type (bar/line/scatter/pie/table/heatmap), and optional x_column/y_column/color_column.'
                )
            ),
        ],
        description: Annotated[
            str,
            Field(default='', description='Short subtitle shown below the dashboard title.'),
        ] = '',
        tables: Annotated[
            list[str] | None,
            Field(default=None, description='Table names to load. Defaults to all available tables.'),
        ] = None,
    ) -> DataAppInfo:
        """
        Creates or updates a local HTML dashboard backed by DuckDB SQL queries.

        Generates a self-contained index.html with Apache ECharts visualizations.
        Chart data is pre-computed from your local CSV tables via DuckDB.
        Call run_data_app to serve the dashboard on a local port.

        Supported chart types: bar, line, scatter, pie, table, heatmap.
        """
        return await create_data_app_local(local_backend, name, title, charts, description, tables)

    mcp.add_tool(
        FunctionTool.from_function(
            create_data_app,
            annotations=ToolAnnotations(readOnlyHint=False),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def run_data_app(
        name: Annotated[str, Field(description='App name (as given to create_data_app).')],
    ) -> DataAppRunResult:
        """
        Starts a local HTTP server for the named data app and returns its URL.

        The server runs in the background (port 8101-8199). Open the returned URL
        in a browser to view the dashboard. Call stop_data_app to shut it down.
        """
        return await run_data_app_local(local_backend, name)

    mcp.add_tool(
        FunctionTool.from_function(
            run_data_app,
            annotations=ToolAnnotations(readOnlyHint=False),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def list_data_apps() -> DataAppsOutput:
        """
        Lists all local data apps and their running status.

        Shows app name, title, chart count, and whether an HTTP server is active.
        """
        return await list_data_apps_local(local_backend)

    mcp.add_tool(
        FunctionTool.from_function(
            list_data_apps,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def stop_data_app(
        name: Annotated[str, Field(description='App name to stop.')],
    ) -> DataAppStopResult:
        """
        Stops the HTTP server for the named data app.

        The app files are kept — call run_data_app to restart.
        """
        return await stop_data_app_local(local_backend, name)

    mcp.add_tool(
        FunctionTool.from_function(
            stop_data_app,
            annotations=ToolAnnotations(readOnlyHint=False),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    @tool_errors()
    async def delete_data_app(
        name: Annotated[str, Field(description='App name to permanently delete.')],
    ) -> DataAppStopResult:
        """
        Stops (if running) and permanently deletes a local data app.

        Removes the app directory including app.json and index.html.
        """
        return await delete_data_app_local(local_backend, name)

    mcp.add_tool(
        FunctionTool.from_function(
            delete_data_app,
            annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True),
            tags={LOCAL_TOOLS_TAG},
        )
    )

    LOG.info('Local-mode tools registered.')


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _count_csv_rows(path: Path) -> int:
    """Count data rows in a CSV (excluding header)."""
    with open(path, newline='', encoding='utf-8') as f:
        return sum(1 for _ in f) - 1  # subtract header line
