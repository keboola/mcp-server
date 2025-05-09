import logging
from typing import Any, Dict, List, Optional, Tuple, Annotated

from jsonschema import Draft7Validator, exceptions
from mcp.server.fastmcp import Context
from pydantic import Field

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.tools.components.utils import _get_component_details

LOG = logging.getLogger(__name__)


async def fetch_component_schema(
        client: KeboolaClient, component_id: str
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Fetch configuration schemas directly from storage API for a specific component.

    Args:
        client: The Keboola client
        component_id: The ID of the component

    Returns:
        A tuple of (root_configuration_schema, row_configuration_schema)
    """
    try:
        endpoint = 'v2/storage/components'
        params = {'include': component_id}

        response = await client.storage_client.get(endpoint, params=params)

        if not response or not isinstance(response, list) or len(response) == 0:
            LOG.warning(f"No component found for ID {component_id}")
            return None, None

        component = response[0]
        if component['id'] != component_id:
            LOG.warning(f"Retrieved component ID {component['id']} doesn't match requested ID {component_id}")
            return None, None

        return component.get('configurationSchema'), component.get('configurationRowSchema')

    except Exception as e:
        LOG.exception(f"Error fetching schema for component {component_id}: {e}")
        return None, None


def validate_against_schema(configuration: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate a configuration against a JSON schema.

    Args:
        configuration: The configuration to validate
        schema: The JSON schema to validate against

    Returns:
        Dictionary with validation results including 'valid' flag and any errors
    """
    try:
        validator = Draft7Validator(schema)
        errors = list(validator.iter_errors(configuration))

        if not errors:
            return {"valid": True, "errors": []}

        formatted_errors = []
        for error in errors:
            path = ".".join(str(p) for p in error.path) if error.path else "root"
            formatted_errors.append({
                "path": path,
                "message": error.message,
                "schema_path": ".".join(str(p) for p in error.schema_path)
            })

        return {"valid": False, "errors": formatted_errors}

    except exceptions.SchemaError as e:
        return {
            "valid": False,
            "errors": [f"Schema error: {str(e)}"]
        }
    except Exception as e:
        return {
            "valid": False,
            "errors": [f"Unexpected error: {str(e)}"]
        }


async def validate_component_configuration(
        ctx: Context,
        component_id: Annotated[
            str,
            Field(
                description='The ID of the component to validate the configuration against.',
            ),
        ],
        configuration: Annotated[
            Dict[str, Any],
            Field(
                description='The configuration parameters to validate.',
            ),
        ],
        is_row_configuration: Annotated[
            bool,
            Field(
                description='Whether this is a row configuration or a root configuration.',
                default=False,
            ),
        ] = False,
) -> Annotated[
    Dict[str, Any],
    Field(
        description='Validation results containing success status and any validation errors.',
    ),
]:
    """
    Validates a component configuration against the component's schema.

    USAGE:
    - Use before creating or updating a component configuration to ensure it adheres to the schema.

    EXAMPLES:
    - user_input: `Is this configuration for JIRA valid?`
        -> set the component_id to the JIRA extractor ID and the configuration to the configuration to validate
        -> returns validation results indicating if the configuration is valid
    """
    client = KeboolaClient.from_state(ctx.session.state)

    component_detail = await _get_component_details(client=client, component_id=component_id)
    schema = component_detail.row_configuration_schema if is_row_configuration else component_detail.root_configuration_schema

    if not schema:
        LOG.info(f"Schema not found in component details, fetching directly from API for {component_id}")
        root_schema, row_schema = await fetch_component_schema(client, component_id)
        schema = row_schema if is_row_configuration else root_schema

    if not schema:
        return {
            "valid": False,
            "errors": [f"No schema found for component {component_id}"]
        }

    LOG.info(f"Validating {'row' if is_row_configuration else 'root'} configuration for component {component_id}")
    return validate_against_schema(configuration, schema)