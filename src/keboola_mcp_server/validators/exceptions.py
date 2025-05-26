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
        original_exception: Exception,
        input_data: Optional[JsonStruct] = None,
        json_schema: Optional[JsonStruct] = None,
        error_message: Optional[str] = None,
    ):
        error_message = str(original_exception) + '\n\n' + (error_message or self._ERROR_MESSAGE)
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
        return cls(original_exception, input_data, json_schema, cls._ERROR_MESSAGE)


class StorageConfigurationValidationError(ConfigurationValidationError):

    _ERROR_MESSAGE = 'The provided storage json configuration is not conforming to the storage json schema.\n'

    def __init__(
        self,
        original_exception: Exception,
        input_data: Optional[JsonStruct] = None,
        json_schema: Optional[JsonStruct] = None,
    ):
        super().__init__(original_exception, input_data, json_schema, self._ERROR_MESSAGE)
    

