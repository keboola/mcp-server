"""
Validator functions for Component Configuration data that are generated by agents.
"""

import json
import logging
from enum import Enum
from importlib import resources
from typing import Callable, Optional, cast

import jsonschema
import jsonschema.validators

from keboola_mcp_server.client import JsonDict, JsonPrimitive, JsonStruct
from keboola_mcp_server.tools.components.model import Component
from keboola_mcp_server.tools.components.utils import BIGQUERY_TRANSFORMATION_ID, SNOWFLAKE_TRANSFORMATION_ID

LOG = logging.getLogger(__name__)

ValidateFunction = Callable[[JsonDict, JsonDict], None]

RESOURCES = 'keboola_mcp_server.resources'


class ConfigurationSchemaResources(str, Enum):
    STORAGE = 'storage-schema.json'
    FLOW = 'flow-schema.json'


class RecoverableValidationError(jsonschema.ValidationError):
    """
    An instance was invalid under a provided schema using a recoverable message for the Agent.
    """

    _RECOVERY_INSTRUCTIONS = (
        'Recovery instructions:\n'
        '- Please check the json schema.\n'
        '- Fix the errors in your input data to follow the schema.\n'
    )

    def __init__(self, *args, invalid_json: Optional[JsonDict] = None, initial_message: Optional[str] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.invalid_json = invalid_json
        self.initial_message = initial_message

    @classmethod
    def create_from_values(
        cls,
        other: jsonschema.ValidationError,
        invalid_json: Optional[JsonDict] = None,
        initial_message: Optional[str] = None,
    ):
        return cls(**other._contents(), invalid_json=invalid_json, initial_message=initial_message)

    def __str__(self) -> str:
        """
        Builds a string representation of the error with recovery instructions.
        Following points describe the order of the error message:
        - message = super().__str__() represents the original error message, so it starts with the error message.
        - message += initial_message if provided, it adds the initial message to the error message.
        - message += recovery instructions from _RECOVERY_INSTRUCTIONS
        - message += invalid_json if provided, it adds the invalid json input data to the error message.

        Example output (init_message="The provided storage configuration is not valid."):
            RecoverableValidationError: 'destination' is a required property
            ...parent error message (containing the violated part of the json schema)...

            The provided storage configuration is not valid.
            Recovery instructions:
            - Please check the json schema.
            - Fix the errors in your input data to follow the schema.

            Invalid input data:
            {
                "storage": {...}
            }
        """
        str_repr = f'{super().__str__()}\n'
        if self.initial_message:
            str_repr += f'{self.initial_message}\n'
        str_repr += f'{self._RECOVERY_INSTRUCTIONS}\n'
        if self.invalid_json:
            str_repr += f'\nInvalid input data:\n{json.dumps(self.invalid_json, indent=2)}\n'
        return str_repr.rstrip()


class KeboolaParametersValidator:
    """
    We use this validator to load parameters' schema that has been fetched from AI service for a given component ID and
    to validate the parameters configuration (json data) received from the Agent against the loaded schema.

    A custom JSON Schema validator that handles UI elements and schema normalization:
    1. Ignores 'button' type (UI-only construct)
    2. Normalizes schema by:
       - Converting boolean 'required' flags to proper list format (propagating the required flag up)
       - Ensuring 'properties' is a dictionary if it is an empty list
    """

    @classmethod
    def validate(cls, instance: JsonDict, schema: JsonDict) -> None:
        """
        Validate the json data instance against the schema.
        :param instance: The json data to validate
        :param schema: The schema to validate against
        """
        sanitized_schema = cls.sanitize_schema(schema)
        base_validator = jsonschema.validators.validator_for(sanitized_schema)
        keboola_validator = jsonschema.validators.extend(
            base_validator, type_checker=base_validator.TYPE_CHECKER.redefine('button', cls.check_button_type)
        )
        return keboola_validator(sanitized_schema).validate(instance)

    @staticmethod
    def check_button_type(checker: jsonschema.TypeChecker, instance: object) -> bool:
        """
        Dummy button type checker.
        We accept button as a type since it is a UI construct and not a data type.
        :returns: True if instance is a dict with a type field with value 'button', False otherwise
        """
        # TODO: We can add a custom pydantic model or json schema for validating button type instances.
        return isinstance(instance, dict) and 'button' == instance.get('type', None)

    @staticmethod
    def sanitize_schema(schema: JsonDict) -> JsonDict:
        """Normalize schema by converting required fields to lists and ensuring properties is a dict"""

        def _sanitize_required_and_properties(
            schema: JsonStruct | JsonPrimitive,
        ) -> tuple[JsonStruct | JsonPrimitive, Optional[bool]]:

            # default returns the element of a schema if we are at the bottom of the tree (not a dict)
            if not isinstance(schema, dict):
                return schema, False

            is_current_required = None
            required = schema.get('required', [])
            if not isinstance(required, list):
                # Convert required field to empty list, and set is_current_required to True/False if the required
                # field is set to true/false and propagate the required flag up to the parent's required list
                is_current_required = str(required).lower() == 'true'
                required = []

            if (properties := schema.get('properties')) is not None:
                if properties == []:
                    properties = {}  # convert empty list to empty dict to avoid AttributeError in jsonschema
                elif not isinstance(properties, dict):
                    # Invalid schema - properties must be a dictionary. SchemaError will be caught and logged
                    # in _validate_json_against_schema but the validation will succeed since we cant use invalid schema
                    raise jsonschema.SchemaError(f'properties must be a dictionary, got {type(properties)}')

                for property_name, subschema in properties.items():
                    # we recursively sanitize the subschemas within the properties
                    properties[property_name], is_child_required = _sanitize_required_and_properties(subschema)
                    # if is_child_required is None, do not propagate - the child has required field correctly set
                    if is_child_required is True and property_name not in required:
                        required.append(property_name)
                    elif is_child_required is False and property_name in required:
                        required.remove(property_name)
                schema['properties'] = properties

            if required:
                schema['required'] = list(required)
            else:
                schema.pop('required', None)

            return schema, is_current_required

        sanitized_schema = cast(JsonDict, _sanitize_required_and_properties(schema)[0])
        return sanitized_schema


def _validate_storage_configuration_against_schema(
    storage: JsonDict, initial_message: Optional[str] = None
) -> JsonDict:
    """Validate the storage configuration using jsonschema.
    :param storage: The storage configuration to validate
    :param initial_message: The initial message to include in the error message
    :returns: The validated storage configuration (json data as the input) if the validation succeeds
    """
    schema = _load_schema(ConfigurationSchemaResources.STORAGE)
    _validate_json_against_schema(
        json_data=storage,
        schema=schema,
        initial_message=initial_message,
    )
    return storage


def _validate_parameters_configuration_against_schema(
    parameters: JsonDict,
    schema: JsonDict,
    initial_message: Optional[str] = None,
) -> JsonDict:
    """
    Validate the parameters configuration using jsonschema.
    :parameters: json data to validate
    :schema: json schema to validate against (root or row parameter configuration schema)
    :initial_message: initial message to include in the error message
    :returns: The validated parameters configuration (json data as the input) if the validation succeeds
    """
    _validate_json_against_schema(
        json_data=parameters,
        schema=schema,
        initial_message=initial_message,
        validate_fn=KeboolaParametersValidator.validate,
    )
    return parameters


def validate_flow_configuration_against_schema(flow: JsonDict, initial_message: Optional[str] = None) -> JsonDict:
    """
    Validate the flow configuration using jsonschema.
    :flow: json data to validate
    :initial_message: initial message to include in the error message
    :returns: The validated flow configuration
    """
    schema = _load_schema(ConfigurationSchemaResources.FLOW)
    _validate_json_against_schema(
        json_data=flow,
        schema=schema,
        initial_message=initial_message,
    )
    return flow


def _validate_json_against_schema(
    json_data: JsonDict,
    schema: JsonDict,
    initial_message: Optional[str] = None,
    validate_fn: Optional[ValidateFunction] = None,
):
    """Validate JSON data against the provided schema."""
    try:
        validate_fn = validate_fn or jsonschema.validate
        validate_fn(json_data, schema)
    except jsonschema.ValidationError as e:
        raise RecoverableValidationError.create_from_values(e, invalid_json=json_data, initial_message=initial_message)
    except jsonschema.SchemaError as e:
        LOG.exception(
            f'The validation schema is not valid: {e}\n'
            f'initial_message: {initial_message}\n'
            f'schema: {schema}\n'
            f'json_data: {json_data}'
        )
        # this is not an Agent error, the schema is not valid and we are unable to validate the json
        # hence we continue with as if it was valid
        return


def _load_schema(json_schema_name: ConfigurationSchemaResources) -> JsonDict:
    with resources.open_text(RESOURCES, json_schema_name.value, encoding='utf-8') as f:
        return json.load(f)


STORAGE_VALIDATION_INITIAL_MESSAGE = 'The provided storage configuration input does not follow the storage schema.\n'
ROOT_PARAMETERS_VALIDATION_INITIAL_MESSAGE = (
    'The provided Root parameters configuration input does not follow the Root parameter json schema for component '
    'id: {component_id}.\n'
)
ROW_PARAMETERS_VALIDATION_INITIAL_MESSAGE = (
    'The provided Row parameters configuration input does not follow the Row parameter json schema for component '
    'id: {component_id}.\n'
)


def validate_root_storage_configuration(
    storage: Optional[JsonDict],
    component: Component,
    initial_message: Optional[str] = None,
) -> JsonDict:
    """
    Utility function to validate the root storage configuration.
    """
    return _validate_storage_configuration(storage, component, initial_message, is_row_storage=False)


def validate_row_storage_configuration(
    storage: Optional[JsonDict],
    component: Component,
    initial_message: Optional[str] = None,
) -> JsonDict:
    """
    Utility function to validate the row storage configuration.
    """
    return _validate_storage_configuration(storage, component, initial_message, is_row_storage=True)


def _validate_storage_configuration(
    storage: Optional[JsonDict],
    component: Component,
    initial_message: Optional[str] = None,
    is_row_storage: bool = False,
) -> JsonDict:
    """
    Validates the storage configuration and checks if it is necessary for the component.
    :param storage: The storage configuration to validate received from the agent.
    :param component: The component for which the storage is provided
    :param initial_message: The initial message to include in the error message.
    :param is_row_storage: Whether the provided storage is for a row configuration. (False for root, True for row)
    :return: The contents of the 'storage' key from the validated configuration,
              or an empty dict if no storage is provided.
    """
    # As expected by the storage schema, we normalize storage to {'storage': storage | {} | None}
    # since the agent bot can input storage as {'storage': storage} or just storage
    storage_cfg = cast(Optional[JsonDict], storage.get('storage', storage) if storage else {})

    # If storage is None, we set it to an empty dict
    if storage_cfg is None:
        LOG.warning(
            f'No "storage" configuration provided for component {component.component_id} of type '
            f'{component.component_type}.'
        )
        storage_cfg = {}
    # Only for SQL transformations - storage must contain either input or output mappings
    if component.component_id in [SNOWFLAKE_TRANSFORMATION_ID, BIGQUERY_TRANSFORMATION_ID]:
        if not storage_cfg.get('input') and not storage_cfg.get('output'):
            raise ValueError(
                f'The "storage" must contain either "input" or "output" mappings in the configuration of the SQL '
                f'transformation "{component.component_id}".'
            )
    # For row-based writers - ROOT must have an empty storage, ROW must have non-empty input in storage
    if component.component_type == 'writer' and component.capabilities.is_row_based:
        if not is_row_storage and storage_cfg != {}:
            # ROOT storage is not empty but the writer is row-based - this is not allowed
            raise ValueError(
                'The "storage" must be empty for root configuration of the writer component '
                f'"{component.component_id}" since it is row-based. In this case, storage should only be defined '
                'in its outgoing row configurations.'
            )
        elif is_row_storage and not storage_cfg.get('input'):
            # ROW storage does not contain input configuration for row-based writer - this is not allowed
            raise ValueError(
                f'The "storage" must contain "input" mappings for the row configuration of the writer component '
                f'"{component.component_id}".'
            )
    # Only for non-row-based writers - ROOT must have non-empty input in storage
    if component.component_type == 'writer' and not component.capabilities.is_row_based:
        if is_row_storage:
            LOG.warning(
                f'Validating "storage" for row configuration of non-row-based writer {component.component_id} is not '
                'semantically correct. Possible cause: agent error or wrong component flag. Proceeding with validation.'
            )
        if not storage_cfg.get('input'):
            # ROOT storage does not contain input configuration for non-row-based writer - this is not allowed
            # (We also can get here when bot tries to create a row config for non-row-based writer {both require input})
            raise ValueError(
                f'The "storage" must contain "input" mappings for the root configuration of the writer component '
                f'"{component.component_id}".'
            )

    initial_message = (initial_message or '') + '\n'
    initial_message += STORAGE_VALIDATION_INITIAL_MESSAGE
    normalized_storage = cast(JsonDict, {'storage': storage_cfg})
    normalized_storage = _validate_storage_configuration_against_schema(normalized_storage, initial_message)
    return cast(JsonDict, normalized_storage['storage'])


def validate_root_parameters_configuration(
    parameters: JsonDict,
    component: Component,
    initial_message: Optional[str] = None,
) -> JsonDict:
    """
    Utility function to validate the root parameters configuration.
    :param parameters: The parameters of the configuration to validate
    :param component: The component for which the configuration is provided
    :param initial_message: The initial message to include in the error message
    :return: The contents of the 'parameters' key from the validated configuration
    """
    initial_message = (initial_message or '') + '\n'
    initial_message += ROOT_PARAMETERS_VALIDATION_INITIAL_MESSAGE.format(component_id=component.component_id)
    return _validate_parameters_configuration(
        parameters, component.configuration_schema, component.component_id, initial_message
    )


def validate_row_parameters_configuration(
    parameters: JsonDict,
    component: Component,
    initial_message: Optional[str] = None,
) -> JsonDict:
    """
    Utility function to validate the row parameters configuration.
    :param parameters: The parameters of the configuration to validate
    :param component: The component for which the configuration is provided
    :param initial_message: The initial message to include in the error message
    :return: The contents of the 'parameters' key from the validated configuration
    """
    initial_message = (initial_message or '') + '\n'
    initial_message += ROW_PARAMETERS_VALIDATION_INITIAL_MESSAGE.format(component_id=component.component_id)
    return _validate_parameters_configuration(
        parameters, component.configuration_row_schema, component.component_id, initial_message
    )


def _validate_parameters_configuration(
    parameters: JsonDict,
    schema: Optional[JsonDict],
    component_id: str,
    initial_message: Optional[str] = None,
) -> JsonDict:
    """
    Utility function to validate the parameters configuration.
    :param parameters: The parameters configuration to validate
    :param schema: The schema to validate against
    :param component_id: The ID of the component
    :param initial_message: The initial message to include in the error message
    :return: The contents of the 'parameters' key from the validated configuration
    """
    # As expected by the component parameter schema, we use only the parameters configurations without the "parameters"
    # key since the agent bot can input parameters as {'parameters': parameters} or just parameters
    expected_parameters = cast(JsonDict, parameters.get('parameters', parameters))

    if not schema:
        LOG.warning(f'No schema provided for component {component_id}, skipping validation.')
        return expected_parameters

    expected_parameters = _validate_parameters_configuration_against_schema(
        expected_parameters, schema, initial_message
    )
    return expected_parameters
