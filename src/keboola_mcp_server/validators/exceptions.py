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
        original_exception: Optional[Exception],
        initial_error_message: Optional[str],
        input_data: Optional[JsonStruct],
        json_schema: Optional[JsonStruct],
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
        initial_error_message = initial_error_message or self._ERROR_MESSAGE
        error_message = prev_error_message + initial_error_message
        if input_data:
            error_message += self._INPUT_DATA.format(input_data=input_data)
        if json_schema:
            error_message += self._JSON_SCHEMA.format(json_schema=json_schema)
        super().__init__(error_message)

    @classmethod
    def from_exception(
        cls,
        original_exception: Exception,
        input_data: Optional[JsonStruct],
        json_schema: Optional[JsonStruct],
    ):
        return cls(
            original_exception,
            initial_error_message=cls._ERROR_MESSAGE,
            input_data=input_data,
            json_schema=json_schema,
        )

    @staticmethod
    def recovery_instructions(additional_instructions: str = '') -> str:
        return (
            'Please check the configuration json schema.\n'
            'Fix the errors in your configuration to follow the schema.\n'
            f'{additional_instructions}'  # serves for RootConfiguration vs RowConfiguration
        )


class JsonValidationError(ConfigurationValidationError):

    _ERROR_MESSAGE = 'The provided json configuration is not a valid json.\n'

    @staticmethod
    def recovery_instructions(additional_instructions: str = '') -> str:
        return (
            '\n'
            'Please provide a valid json configuration.\n'
            f'{additional_instructions}'
        )


class StorageConfigurationValidationError(ConfigurationValidationError):

    _ERROR_MESSAGE = 'The provided storage json configuration is not conforming to the storage json schema.\n'

    @staticmethod
    def recovery_instructions(additional_instructions: str = '') -> str:
        return (
            '\n'
            'Please check the storage configuration json schema.\n'
            'Fix the errors in your storage configuration to follow the schema.\n'
            f'{additional_instructions}'
        )


class ParameterRootConfigurationValidationError(ConfigurationValidationError):

    _ERROR_MESSAGE_WITH_COMPONENT_ID = (
        'The provided root parameter configuration json is not conforming to the root parameter json schema for '
        'component id: "{component_id}".\n'
    )
    _ERROR_MESSAGE = (
        'The provided root parameter configuration json is not conforming to the root parameter json schema.\n'
    )

    @classmethod
    def from_exception(
        cls,
        original_exception: Exception,
        input_data: Optional[JsonStruct] = None,
        json_schema: Optional[JsonStruct] = None,
        component_id: Optional[str] = None,
    ):
        initial_error_message = (
            cls._ERROR_MESSAGE_WITH_COMPONENT_ID.format(component_id=component_id)
            if component_id
            else cls._ERROR_MESSAGE
        )
        return cls(
            original_exception=original_exception,
            input_data=input_data,
            json_schema=json_schema,
            initial_error_message=initial_error_message,
        )

    @staticmethod
    def recovery_instructions(additional_instructions: str = '') -> str:
        return (
            '\n'
            'Please check the parameter root json schema.\n'
            'Fix the errors in your parameter root configuration to follow the schema.\n'
            f'{additional_instructions}'
        )


class ParameterRowConfigurationValidationError(ParameterRootConfigurationValidationError):

    _ERROR_MESSAGE_WITH_COMPONENT_ID = (
        'The provided row parameter configuration json is not conforming to the row parameter json schema for '
        'component id: "{component_id}".\n'
    )
    _ERROR_MESSAGE = (
        'The provided row parameter configuration json is not conforming to the row parameter json schema.\n'
    )

    @staticmethod
    def recovery_instructions(additional_instructions: str = '') -> str:
        return (
            '\n'
            'Please check the parameter row json schema.\n'
            'Fix the errors in your parameter row configuration to follow the schema.\n'
            f'{additional_instructions}'
        )
