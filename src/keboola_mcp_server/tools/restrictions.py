"""Component restriction validations for update operations."""

from keboola_mcp_server.clients.client import (
    DATA_APP_COMPONENT_ID,
    CONDITIONAL_FLOW_COMPONENT_ID,
    ORCHESTRATOR_COMPONENT_ID,
)

SPECIALIZED_COMPONENT_TO_UPDATE_TOOL: dict[str, str] = {
    DATA_APP_COMPONENT_ID: 'modify_data_app',
    CONDITIONAL_FLOW_COMPONENT_ID: 'update_flow',
    ORCHESTRATOR_COMPONENT_ID: 'update_flow',
}


def validate_component_id_for_update(
    component_id: str,
    tool_name: str,
    expected_component_id: str | None = None,
) -> None:
    """
    Validates that a component_id is appropriate for the given update tool.

    For generic update tools (update_config, update_config_row):
    - Pass expected_component_id=None to block specialized components

    For specialized update tools (modify_data_app, update_flow):
    - Pass the expected_component_id to ensure the component matches

    :param component_id: The component ID to validate
    :param tool_name: Name of the update tool being used (for error message context)
    :param expected_component_id: The component ID that this tool expects, or None for generic tools
    :raises ValueError: If the component_id is not appropriate, with suggestion for alternative tool
    """
    if expected_component_id is not None:
        if component_id == expected_component_id:
            return

        if component_id in SPECIALIZED_COMPONENT_TO_UPDATE_TOOL:
            alternative_tool = SPECIALIZED_COMPONENT_TO_UPDATE_TOOL[component_id]
        else:
            alternative_tool = 'update_config/update_config_row'

        raise ValueError(
            f"Component '{component_id}' cannot be used with {tool_name}. " f"Use '{alternative_tool}' instead."
        )

    if component_id in SPECIALIZED_COMPONENT_TO_UPDATE_TOOL:
        alternative_tool = SPECIALIZED_COMPONENT_TO_UPDATE_TOOL[component_id]
        raise ValueError(
            f"Component '{component_id}' cannot be used with {tool_name}. " f"Use '{alternative_tool}' instead."
        )
