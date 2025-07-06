"""
Adapters - Conversion logic between API responses and Domain models.

These adapters handle the transformation of parsed API responses
into clean domain models for business logic.
"""

from keboola_mcp_server.tools.components.api_models import APIComponentResponse
from keboola_mcp_server.tools.components.domain_models import Component, ComponentCapabilities, ComponentSummary


class ComponentAdapter:
    """Converts between API response objects and domain models for components."""

    @staticmethod
    def to_component_detail(api_response: APIComponentResponse) -> Component:
        """
        Convert API component response to detailed domain model.

        :param api_response: Parsed API response (works for both Storage API and AI Service API)
        :return: Clean domain model with derived capabilities and full metadata
        """
        capabilities = ComponentAdapter._derive_capabilities(api_response.flags)

        return Component(
            component_id=api_response.component_id,
            component_name=api_response.component_name,
            component_type=api_response.type,
            component_categories=api_response.categories,
            capabilities=capabilities,
            documentation_url=api_response.documentation_url,
            documentation=api_response.documentation,
            configuration_schema=api_response.configuration_schema,
            configuration_row_schema=api_response.configuration_row_schema,
        )

    @staticmethod
    def to_component_summary(api_response: APIComponentResponse) -> ComponentSummary:
        """
        Convert API component response to summary domain model (for list operations).

        :param api_response: Parsed API response (works for both Storage API and AI Service API)
        :return: Lightweight domain model with essential info only
        """
        capabilities = ComponentAdapter._derive_capabilities(api_response.flags)

        return ComponentSummary(
            component_id=api_response.component_id,
            component_name=api_response.component_name,
            component_type=api_response.type,
            capabilities=capabilities,
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
