"""
This test is used to validate both row and root parameter schemas of all components.
- Serves as a sanity check for the schemas validation, identifying invalid schemas and proposing two possible solutions:
    - Fix the json schema to be valid
    - Improve the KeboolaParametersValidator to accept the schema
- Ensures that all parameter schemas are valid and that the MCP server will them correctly to validate the parameters
received from the LLM Agent.
- In case the schema is invalid, we skip the validation, log the schema error but continue with the action (creation or
update of the component) assuming that the validation of json object against the schema had been correct. That is the
reason why we are having those tests.
"""

import logging
from typing import cast

import jsonschema
import pytest

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.storage import ComponentAPIResponse
from keboola_mcp_server.tools.components.model import Component
from keboola_mcp_server.tools.validation import KeboolaParametersValidator

LOG = logging.getLogger(__name__)


def _check_schema(schema: JsonDict, dummy_parameters: JsonDict) -> None:
    try:
        KeboolaParametersValidator.validate(dummy_parameters, schema)
    except jsonschema.ValidationError:
        # We care only about schema errors, ignore validation errors since we are using dummy parameters.
        # The schema itself is checked just before we validate JSON object against it. Hence, we can ignore
        # ValidationError because the schema is valid which is our objective, but our dummy_parameters violates
        # the schema - we are not interested in the dummy parameters.
        pass


@pytest.mark.asyncio
async def test_validate_parameters(keboola_client: KeboolaClient):
    data = cast(JsonDict, await keboola_client.storage_client.get(''))  # get information about current storage stack
    LOG.info(f'Fetched information: {data.keys()}')
    components = cast(list[JsonDict], data['components'])
    components.sort(key=lambda x: (x['type'], x['name']))  # sort by type and then by name
    LOG.info(f'Fetched total of {len(components)} components')

    row_counts, root_counts = 0, 0
    invalid_row_schemas, invalid_root_schemas = [], []
    for raw_component in components:
        api_component = ComponentAPIResponse.model_validate(raw_component)
        component = Component.from_api_response(api_component)
        if component.configuration_schema:
            try:
                root_counts += 1
                _check_schema(component.configuration_schema, dummy_parameters={})
            except jsonschema.SchemaError as e:
                LOG.exception(f'Root schema error for {raw_component["id"]}: {e}')
                invalid_root_schemas.append(raw_component['id'])
        if component.configuration_row_schema:
            try:
                row_counts += 1
                _check_schema(component.configuration_row_schema, dummy_parameters={})
            except jsonschema.SchemaError as e:
                LOG.exception(f'Row schema error for {raw_component["id"]}: {e}')
                invalid_row_schemas.append(raw_component['id'])

    if invalid_root_schemas:
        pytest.fail(f'Invalid root schemas({len(invalid_root_schemas)}): {invalid_root_schemas}')
    if invalid_row_schemas:
        pytest.fail(f'Invalid row schemas({len(invalid_row_schemas)}): {invalid_row_schemas}')

    LOG.info(
        f'Total components: {len(components)}, from which {root_counts} have root configuration schema and '
        f'{row_counts} have row configuration schema. All schemas are valid.'
    )
