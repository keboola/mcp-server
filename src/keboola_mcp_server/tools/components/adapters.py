"""
Adapters - Conversion logic between raw API responses and Domain models.

These adapters handle the transformation of data between different layers:
- Raw API responses to clean domain models
- Domain models to legacy models (for backward compatibility)
"""

from keboola_mcp_server.tools.components.api_models import APIComponentResponse
from keboola_mcp_server.tools.components.domain_models import Component, ComponentCapabilities
from keboola_mcp_server.tools.components.model import Component as LegacyComponent


class ComponentAdapter:
    """Converts between raw API responses and domain models for components."""

    @staticmethod
    def from_raw_response(raw_response: APIComponentResponse) -> Component:
        """
        Convert raw component response (from either Storage API or AI Service API) to clean domain model.

        :param raw_response: Raw API response (works for both Storage API and AI Service API)
        :return: Clean domain model with derived capabilities and optional metadata
        """
        capabilities = ComponentAdapter._derive_capabilities(raw_response.flags)

        return Component(
            id=raw_response.component_id,
            name=raw_response.component_name,
            type=raw_response.type,
            categories=raw_response.categories,
            capabilities=capabilities,
            documentation_url=raw_response.documentation_url,
            documentation=raw_response.documentation,
            configuration_schema=raw_response.configuration_schema,
            row_schema=raw_response.configuration_row_schema,
        )

    @staticmethod
    def _derive_capabilities(flags: list[str]) -> ComponentCapabilities:
        """
        Derive component capabilities from developer portal flags.

        :param flags: List of developer portal flags
        :return: Component capabilities
        """
        return ComponentCapabilities(
            is_row_based='genericDockerUI-rows' in flags,
            has_table_input=any(flag in flags for flag in [
                'genericDockerUI-tableInput',
                'genericDockerUI-simpleTableInput'
            ]),
            has_table_output='genericDockerUI-tableOutput' in flags,
            has_file_input='genericDockerUI-fileInput' in flags,
            has_file_output='genericDockerUI-fileOutput' in flags,
            requires_oauth='genericDockerUI-authorization' in flags,
        )

    @staticmethod
    def to_legacy_component(component: Component) -> LegacyComponent:
        """
        Convert new domain model to legacy Component model for backward compatibility.

        :param component: New clean domain model
        :return: Legacy Component model with all the complex field mappings
        """
        return LegacyComponent(
            component_id=component.id,
            component_name=component.name,
            component_type=component.type,
            component_categories=component.categories,
            component_flags=[],  # We don't store original flags in domain model

            # Derived capabilities
            is_row_based=component.capabilities.is_row_based,
            has_table_input_mapping=component.capabilities.has_table_input,
            has_table_output_mapping=component.capabilities.has_table_output,
            has_file_input_mapping=component.capabilities.has_file_input,
            has_file_output_mapping=component.capabilities.has_file_output,
            has_oauth=component.capabilities.requires_oauth,

            # Optional metadata
            documentation_url=component.documentation_url,
            documentation=component.documentation,
            configuration_schema=component.configuration_schema,
            configuration_row_schema=component.row_schema,
        )
