import logging
from typing import cast

import jsonschema
import pytest

from keboola_mcp_server.client import JsonDict, KeboolaClient
from keboola_mcp_server.tools._validate import validate_parameters
from keboola_mcp_server.tools.components.model import Component

LOG = logging.getLogger(__name__)


def _safe_validate_parameters(schema: JsonDict, dummy_parameters: JsonDict) -> JsonDict:
    """
    Validate parameters against schema but
    """
    try:
        return cast(JsonDict, validate_parameters(dummy_parameters, schema)['parameters'])
    except jsonschema.ValidationError:
        # we care only about schema errors, ignore validation errors since we are using dummy parameters
        return {}


@pytest.mark.asyncio
async def test_validate_parameters(keboola_client: KeboolaClient):
    data = cast(JsonDict, await keboola_client.storage_client.get(''))  # get information about current storage stack
    LOG.info(f'Fetched information: {data.keys()}')
    components = cast(list[JsonDict], data['components'])
    components = sorted(components, key=lambda x: (x['type'], x['name']))  # sort by type and then by name
    LOG.info(f'Fetched total of {len(components)} components')

    row_counts, root_counts = 0, 0
    for raw_component in components:
        component = Component.model_validate(raw_component)
        configuration_schema = component.configuration_schema
        configuration_row_schema = component.configuration_row_schema
        if configuration_schema:
            root_counts += 1
            validated = _safe_validate_parameters(configuration_schema, dummy_parameters={})
            assert (
                validated == {}
            ), f'{component.component_id} has invalid configuration schema'  # expect returned dummy parameters
        if configuration_row_schema:
            row_counts += 1
            validated = _safe_validate_parameters(configuration_row_schema, dummy_parameters={})
            assert (
                validated == {}
            ), f'{component.component_id} has invalid configuration row schema'  # expect returned dummy parameters
    # assert False
    LOG.info(
        f'Total components: {len(components)}, from which {root_counts} have root configuration and {row_counts} '
        'have row configuration'
    )
