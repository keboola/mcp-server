"""
Domain Models - Clean business objects representing Keboola concepts.

These models represent business entities with clear responsibilities and no API concerns.
They can optionally carry MCP-specific metadata when needed by tools.
"""

from typing import Any

from pydantic import BaseModel, Field

from keboola_mcp_server.links import Link


class ComponentCapabilities(BaseModel):
    """
    Component capabilities derived from developer portal flags.

    Represents what a component can do in terms of data processing.
    """

    is_row_based: bool = Field(default=False, description='Whether the component supports configuration rows')
    has_table_input: bool = Field(default=False, description='Whether the component can read from tables')
    has_table_output: bool = Field(default=False, description='Whether the component can write to tables')
    has_file_input: bool = Field(default=False, description='Whether the component can read from files')
    has_file_output: bool = Field(default=False, description='Whether the component can write to files')
    requires_oauth: bool = Field(default=False, description='Whether the component requires OAuth authorization')


class Component(BaseModel):
    """
    Business representation of a Keboola component.

    Contains core business data and can optionally carry MCP-specific metadata
    when needed by tools that require additional context.
    """

    # Core business data (always present)
    id: str = Field(description='Component ID')
    name: str = Field(description='Component name')
    type: str = Field(description='Component type')
    categories: list[str] = Field(default_factory=list, description='Component categories')
    capabilities: ComponentCapabilities = Field(description='Component capabilities')

    # Optional MCP-specific metadata (populated when needed)
    documentation_url: str | None = Field(default=None, description='URL to component documentation')
    documentation: str | None = Field(default=None, description='Component documentation text')
    configuration_schema: dict[str, Any] | None = Field(default=None, description='JSON schema for configuration')
    row_schema: dict[str, Any] | None = Field(default=None, description='JSON schema for configuration rows')
    links: list[Link] = Field(default_factory=list, description='MCP-specific links for UI navigation')
