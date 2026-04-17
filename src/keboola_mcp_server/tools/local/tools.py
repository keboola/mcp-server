"""Local-mode MCP tool implementations backed by the local filesystem and DuckDB."""

import logging
from pathlib import Path
from typing import Annotated, Literal

from fastmcp import FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.tools.local.backend import LocalBackend

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
    sql_engine: Literal['DuckDB'] = Field(default='DuckDB', description='SQL engine used for local queries.')
    llm_instruction: str = Field(description='Base instructions for working in local mode.')


# ---------------------------------------------------------------------------
# Implementation functions (standalone, injectable for tests)
# ---------------------------------------------------------------------------

_LOCAL_PROJECT_INSTRUCTION = (
    'You are working in local mode. Data is stored as CSV files on disk. '
    'Use get_tables to list available tables, query_data to run DuckDB SQL queries, '
    'and search to find tables or columns by name. '
    'Table names in SQL correspond to CSV file stems (e.g. customers.csv → SELECT * FROM customers). '
    'There is no Keboola platform connection — jobs, flows, and transformations are not available.'
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
    return LocalProjectInfo(
        data_dir=str(local_backend.data_dir.resolve()),
        table_count=table_count,
        llm_instruction=_LOCAL_PROJECT_INSTRUCTION,
    )


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

    LOG.info('Local-mode tools registered.')


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _count_csv_rows(path: Path) -> int:
    """Count data rows in a CSV (excluding header)."""
    with open(path, newline='', encoding='utf-8') as f:
        return sum(1 for _ in f) - 1  # subtract header line
