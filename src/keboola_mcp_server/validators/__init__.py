"""
Validators that validates the Agent generated configurations against the json schema or pydantic model. Since the
corrupted configurations are not handled, and UI can crash, we need to validate the configurations before.
"""

from keboola_mcp_server.validators.validate import validate_storage
