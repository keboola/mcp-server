"""Component schema discovery via the public Keboola Developer Portal API."""

import logging
from typing import Any

import httpx
from pydantic import BaseModel, Field

LOG = logging.getLogger(__name__)

DEVELOPER_PORTAL_BASE = 'https://apps-api.keboola.com'


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class ComponentSchemaResult(BaseModel):
    component_id: str = Field(description='Keboola component ID.')
    name: str | None = Field(default=None, description='Human-readable component name.')
    image: str | None = Field(default=None, description='Docker image used by the component.')
    config_schema: dict | None = Field(default=None, description='JSON Schema for the component configuration.')
    config_row_schema: dict | None = Field(default=None, description='JSON Schema for configuration rows.')
    raw: dict = Field(default_factory=dict, description='Full raw response from the Developer Portal.')


class ComponentSearchResult(BaseModel):
    component_id: str = Field(description='Keboola component ID.')
    name: str | None = Field(default=None, description='Human-readable component name.')
    type: str | None = Field(default=None, description='Component type (extractor, writer, application, …).')
    image: str | None = Field(default=None, description='Docker image tag for this component.')
    description: str | None = Field(default=None, description='Short description of the component.')


# ---------------------------------------------------------------------------
# API functions
# ---------------------------------------------------------------------------


async def get_component_schema(component_id: str) -> ComponentSchemaResult:
    """Fetch a component manifest from the public Keboola Developer Portal.

    Endpoint: GET https://apps-api.keboola.com/apps/{component_id}
    No authentication required.
    """
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(f'{DEVELOPER_PORTAL_BASE}/apps/{component_id}')
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()

    return ComponentSchemaResult(
        component_id=component_id,
        name=data.get('name'),
        image=data.get('imageTag') or data.get('uri'),
        config_schema=data.get('configurationSchema') or data.get('configSchema'),
        config_row_schema=data.get('configurationRowSchema') or data.get('configRowSchema'),
        raw=data,
    )


async def find_component_id(name: str, limit: int = 10) -> list[ComponentSearchResult]:
    """Search for components by name in the public Keboola Developer Portal.

    Fetches all published apps (limit=500 covers the full catalog) and filters
    client-side because the API does not support server-side text search.

    Endpoint: GET https://apps-api.keboola.com/apps?limit=500
    No authentication required.
    """
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(f'{DEVELOPER_PORTAL_BASE}/apps', params={'limit': 500})
        resp.raise_for_status()
        data: Any = resp.json()

    items: list[Any] = data if isinstance(data, list) else data.get('apps', data.get('items', []))

    query = name.lower()
    results: list[ComponentSearchResult] = []
    for item in items:
        item_id = item.get('id') or ''
        if not item_id:
            continue
        item_name = (item.get('name') or '').lower()
        item_desc = (item.get('shortDescription') or item.get('description') or '').lower()
        if query in item_id.lower() or query in item_name or query in item_desc:
            results.append(
                ComponentSearchResult(
                    component_id=item_id,
                    name=item.get('name'),
                    type=item.get('type'),
                    image=item.get('imageTag') or item.get('uri'),
                    description=item.get('shortDescription') or item.get('description'),
                )
            )
            if len(results) >= limit:
                break
    return results
