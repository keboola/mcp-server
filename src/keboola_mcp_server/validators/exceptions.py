"""
Exceptions for the validators.
"""

from typing import Optional

from keboola_mcp_server.client import JsonStruct


class ConfigurationValidationError(Exception):
    """
    Exception raised when a schema validation error occurs.
    """

    _ERROR_MESSAGE = (
        'The provided json configuration is not conforming to the corresponding configuration json schema.\n'
    )
    _INPUT_DATA = 'Input json configuration: \n{input_data}\n\n'
    _JSON_SCHEMA = 'Json schema: \n{json_schema}\n\n'

    def __init__(
        self,
        original_exception: Optional[Exception] = None,
        initial_error_message: Optional[str] = None,
        input_data: Optional[JsonStruct] = None,
        json_schema: Optional[JsonStruct] = None,
    ):
        """
        Creates error message as follows:
        If original_exception is provided, it starts with the error message of the original exception.
        If initial_error_message is provided, then it is added to the error message, if not provided, then the default
        error message is used.
        If input_data is provided, then it is added to the error message.
        If json_schema is provided, then it is added to the error message.
        """
        prev_error_message = (str(original_exception) + '\n\n') if original_exception else ''
        error_message = prev_error_message + (initial_error_message or self._ERROR_MESSAGE)
        if input_data:
            error_message += self._INPUT_DATA.format(input_data=input_data)
        if json_schema:
            error_message += self._JSON_SCHEMA.format(json_schema=json_schema)
        super().__init__(error_message)

    @classmethod
    def from_exception(
        cls,
        original_exception: Exception,
        input_data: Optional[JsonStruct] = None,
        json_schema: Optional[JsonStruct] = None,
    ):
        return cls(
            original_exception, initial_error_message=cls._ERROR_MESSAGE, input_data=input_data, json_schema=json_schema
        )

    @staticmethod
    def recovery_instructions(additional_instructions: str = '') -> str:
        return (
            'Please check the configuration json schema.\n'
            'Fix the errors in your configuration to follow the schema.\n'
            f'{additional_instructions}'
        )


class JsonValidationError(ConfigurationValidationError):

    _ERROR_MESSAGE = 'The provided json configuration is not a valid json.\n'

    def __init__(
        self,
        original_exception: Exception,
        input_data: Optional[JsonStruct] = None,
        json_schema: Optional[JsonStruct] = None,
        error_message: Optional[str] = None,
    ):
        super().__init__(
            original_exception,
            initial_error_message=error_message or self._ERROR_MESSAGE,
            input_data=input_data,
            json_schema=json_schema,
        )

    @staticmethod
    def recovery_instructions(additional_instructions: str = '') -> str:
        return '\n' 'Please provide a valid json configuration.\n' f'{additional_instructions}'


class StorageConfigurationValidationError(ConfigurationValidationError):

    _ERROR_MESSAGE = 'The provided storage json configuration is not conforming to the storage json schema.\n'

    def __init__(
        self,
        original_exception: Exception,
        input_data: Optional[JsonStruct] = None,
        json_schema: Optional[JsonStruct] = None,
        error_message: Optional[str] = None,
    ):
        super().__init__(
            original_exception,
            initial_error_message=error_message or self._ERROR_MESSAGE,
            input_data=input_data,
            json_schema=json_schema,
        )

    @staticmethod
    def recovery_instructions(additional_instructions: str = '') -> str:
        return (
            '\n'
            'Please check the storage configuration json schema.\n'
            'Fix the errors in your storage configuration to follow the schema.\n'
            f'{additional_instructions}'
        )
