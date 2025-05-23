"""
Exceptions for the validators.
"""

from typing import Optional

from keboola_mcp_server.client import JsonStruct
from keboola_mcp_server.errors import ToolException


class ConfigurationValidationError(ToolException):
    """
    Exception raised when a schema validation error occurs.
    """

    _RECOVERY_INSTRUCTION = 'The provided json is not conforming to the json schema. Please fix the json and try again.'

    _RECOVERY_INSTRUCTION_WITH_INPUT_JSON = '{previous_instructions}\n' 'Your input:\n' '{input}' '\n\n'

    _RECOVERY_INSTRUCTION_WITH_SCHEMA = (
        '{previous_instructions}\n'
        'Please follow the json schema provided below when creating the json.\n'
        'Json schema:\n'
        '{schema}'
        '\n\n'
    )

    def __init__(
        self,
        original_exception: Exception,
        recovery_instruction: Optional[str] = None,
        input_json: Optional[JsonStruct] = None,
        schema: Optional[JsonStruct] = None,
    ):

        recovery_instruction = recovery_instruction or self._RECOVERY_INSTRUCTION
        if input_json:
            recovery_instruction = self._RECOVERY_INSTRUCTION_WITH_INPUT_JSON.format(
                previous_instructions=recovery_instruction, input=input_json
            )

        if schema:
            recovery_instruction = self._RECOVERY_INSTRUCTION_WITH_SCHEMA.format(
                previous_instructions=recovery_instruction, schema=schema
            )

        super().__init__(original_exception, recovery_instruction=recovery_instruction)


class StorageConfigurationValidationError(ConfigurationValidationError):

    _RECOVERY_INSTRUCTION = (
        'The storage configuration does not match the json schema. Please fix the storage configuration and try again.'
        '\n\n'
        'You may use appropriate tools to see examples of the storage configuration.\n'
    )

    def __init__(
        self, original_exception: Exception, input: Optional[JsonStruct] = None, schema: Optional[JsonStruct] = None
    ):
        super().__init__(
            original_exception,
            recovery_instruction=self._RECOVERY_INSTRUCTION,
            input_json=input,
            schema=schema,
        )
