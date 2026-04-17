"""Component schema discovery via the public Keboola Developer Portal API."""

import logging
from typing import Any

import httpx
from pydantic import BaseModel, Field

LOG = logging.getLogger(__name__)

DEVELOPER_PORTAL_BASE = 'https://components.keboola.com'


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

    Endpoint: GET https://components.keboola.com/components/{component_id}
    No authentication required.
    """
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(f'{DEVELOPER_PORTAL_BASE}/components/{component_id}')
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()

    image = None
    if isinstance(data.get('data'), dict):
        image = data['data'].get('image_tag')
    if not image:
        image = data.get('uri')

    return ComponentSchemaResult(
        component_id=component_id,
        name=data.get('name'),
        image=image,
        config_schema=data.get('configSchema') or data.get('config_schema'),
        config_row_schema=data.get('configRowSchema') or data.get('config_row_schema'),
        raw=data,
    )


async def find_component_id(name: str, limit: int = 10) -> list[ComponentSearchResult]:
    """Search for components by name in the public Keboola Developer Portal.

    Endpoint: GET https://components.keboola.com/components?q=<name>&limit=<n>
    No authentication required.
    """
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f'{DEVELOPER_PORTAL_BASE}/components',
            params={'q': name, 'limit': limit},
        )
        resp.raise_for_status()
        data: Any = resp.json()

    items: list[Any] = data if isinstance(data, list) else data.get('components', data.get('items', []))

    results: list[ComponentSearchResult] = []
    for item in items:
        if not item.get('id'):
            continue
        image = None
        if isinstance(item.get('data'), dict):
            image = item['data'].get('image_tag')
        results.append(
            ComponentSearchResult(
                component_id=item['id'],
                name=item.get('name'),
                type=item.get('type'),
                image=image,
                description=item.get('description'),
            )
        )
    return results
