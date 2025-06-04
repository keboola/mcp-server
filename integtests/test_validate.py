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

from keboola_mcp_server.client import JsonDict, KeboolaClient
from keboola_mcp_server.tools._validate import KeboolaParametersValidator
from keboola_mcp_server.tools.components.model import Component

LOG = logging.getLogger(__name__)


def _check_schema(schema: JsonDict, dummy_parameters: JsonDict) -> None:
    try:
        KeboolaParametersValidator.validate(dummy_parameters, schema)
    except jsonschema.ValidationError:
        # We care only about schema errors, ignore validation errors since we are using dummy parameters.
        # The schema itself is checked just before we validate json object against it. Hence, we can ingore
        # ValidationError because the schema is valid which is our objective, but our dummy_parameters violates
        # the schema - we are not interested in the dummy parameters.
        pass


@pytest.mark.asyncio
async def test_validate_parameters(keboola_client: KeboolaClient):
    data = cast(JsonDict, await keboola_client.storage_client.get(''))  # get information about current storage stack
    LOG.info(f'Fetched information: {data.keys()}')
    components = cast(list[JsonDict], data['components'])
    components = sorted(components, key=lambda x: (x['type'], x['name']))  # sort by type and then by name
    LOG.info(f'Fetched total of {len(components)} components')
    row_counts, root_counts = 0, 0
    for raw_component in components:
        try:
            component = Component.model_validate(raw_component)
            configuration_schema = component.configuration_schema
            configuration_row_schema = component.configuration_row_schema
            if configuration_schema:
                root_counts += 1
                _check_schema(configuration_schema, dummy_parameters={})
            if configuration_row_schema:
                row_counts += 1
                _check_schema(configuration_row_schema, dummy_parameters={})
        except jsonschema.SchemaError as e:
            pytest.fail(f'Schema error for {raw_component["id"]}: {e}')
    LOG.info(
        f'Total components: {len(components)}, from which {root_counts} have root configuration schema and '
        f'{row_counts} have row configuration schema. All schemas are valid.'
    )
