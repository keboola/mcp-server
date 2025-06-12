# Error Handling in Keboola MCP Server

This document provides a comprehensive overview of how error handling is implemented throughout the Keboola MCP Server application. The application employs a multi-layered approach to error handling, ensuring robust operation, meaningful error messages, and recovery instructions for both developers and AI agents.

## Table of Contents

1. [Overview](#overview)
2. [Core Error Handling Architecture](#core-error-handling-architecture)
3. [Tool-Level Error Handling](#tool-level-error-handling)
4. [HTTP Client Error Handling](#http-client-error-handling)
5. [Validation Error Handling](#validation-error-handling)
6. [OAuth Error Handling](#oauth-error-handling)
7. [Server-Level Error Handling](#server-level-error-handling)
8. [Configuration Error Handling](#configuration-error-handling)
9. [Logging and Monitoring](#logging-and-monitoring)
10. [Error Recovery Patterns](#error-recovery-patterns)
11. [Testing Error Handling](#testing-error-handling)

## Overview

The Keboola MCP Server implements a sophisticated error handling system designed to:

- **Provide meaningful error messages** to AI agents with recovery instructions
- **Log errors comprehensively** for debugging and monitoring
- **Handle different types of failures** gracefully (network, validation, authentication, etc.)
- **Maintain system stability** even when individual operations fail
- **Support both human developers and AI agents** with appropriate error context

## Core Error Handling Architecture

### 1. Custom Exception Classes

The application defines custom exception classes to provide structured error handling:

#### `ToolException` Class
```python
class ToolException(Exception):
    """Custom tool exception class that wraps tool execution errors."""

    def __init__(self, original_exception: Exception, recovery_instruction: str):
        super().__init__(f'{str(original_exception)} | Recovery: {recovery_instruction}')
```

**Purpose**: Wraps original exceptions with AI-friendly recovery instructions
**Location**: `src/keboola_mcp_server/errors.py`

#### `RecoverableValidationError` Class
```python
class RecoverableValidationError(jsonschema.ValidationError):
    """An instance was invalid under a provided schema using a recoverable message for the Agent."""
```

**Purpose**: Provides detailed validation error messages with recovery instructions
**Location**: `src/keboola_mcp_server/tools/validation.py`

### 2. Decorator-Based Error Handling

The application uses a decorator pattern for consistent error handling across all tools:

#### `@tool_errors` Decorator
```python
def tool_errors(
    default_recovery: Optional[str] = None,
    recovery_instructions: Optional[dict[Type[Exception], str]] = None,
) -> Callable[[F], F]:
```

**Features**:
- **Automatic exception catching**: Wraps all tool functions in try-catch blocks
- **Recovery instruction mapping**: Maps specific exception types to recovery messages
- **Comprehensive logging**: Logs all exceptions with context
- **Exception transformation**: Converts exceptions to `ToolException` with recovery instructions

**Usage Example**:
```python
@tool_errors(
    default_recovery="Please check your input parameters and try again.",
    recovery_instructions={
        ValueError: "Check that data has valid types.",
        HTTPStatusError: "Verify your authentication credentials and API endpoint."
    }
)
async def my_tool_function(ctx: Context, param: str) -> Result:
    # Tool implementation
    pass
```

## Tool-Level Error Handling

### 1. Tool Function Error Handling

All tool functions in the application are decorated with `@tool_errors` to ensure consistent error handling:

**Components with tool error handling**:
- Component tools (`src/keboola_mcp_server/tools/components/tools.py`)
- Job tools (`src/keboola_mcp_server/tools/jobs.py`)
- Storage tools (`src/keboola_mcp_server/tools/storage.py`)
- Flow tools (`src/keboola_mcp_server/tools/flow.py`)
- SQL tools (`src/keboola_mcp_server/tools/sql.py`)
- Project tools (`src/keboola_mcp_server/tools/project.py`)
- Documentation tools (`src/keboola_mcp_server/tools/doc.py`)

### 2. Error Handling Flow in Tools

1. **Exception occurs** in tool function
2. **Logging**: Exception is logged with full context using `logging.exception()`
3. **Recovery instruction lookup**: System checks for specific recovery instructions based on exception type
4. **Exception transformation**: Original exception is wrapped in `ToolException` with recovery instructions
5. **Propagation**: Transformed exception is raised to the MCP framework

### 3. Example Tool Error Handling

```python
@tool_errors(
    default_recovery="Please verify the component ID and try again.",
    recovery_instructions={
        HTTPStatusError: "Component not found. Please check the component ID.",
        ValueError: "Invalid component ID format."
    }
)
async def get_component_configuration(
    ctx: Context,
    component_id: str,
    configuration_id: str
) -> ComponentConfigurationOutput:
    client = KeboolaClient.from_state(ctx.session.state)
    # Implementation that may raise various exceptions
```

## HTTP Client Error Handling

### 1. Raw HTTP Client Error Handling

The `RawKeboolaClient` class handles HTTP-level errors:

```python
async def get(self, endpoint: str, params: dict[str, Any] | None = None) -> JsonStruct:
    async with httpx.AsyncClient(timeout=self.timeout) as client:
        response = await client.get(f'{self.base_api_url}/{endpoint}', params=params, headers=headers)
        response.raise_for_status()  # Raises HTTPStatusError for 4xx/5xx responses
        return cast(JsonStruct, response.json())
```

**Error handling features**:
- **Automatic status code checking**: `response.raise_for_status()` raises `HTTPStatusError` for error status codes
- **Timeout handling**: Configurable timeouts for different operations
- **Connection error handling**: Network-level errors are propagated up

### 2. Service-Specific Client Error Handling

Different service clients handle errors appropriately:

#### Storage API Client
- Handles 404 errors for missing resources
- Manages authentication errors (401/403)
- Handles rate limiting (429)

#### Jobs Queue Client
- Handles job creation failures
- Manages job status query errors
- Handles invalid job parameters

#### AI Service Client
- Handles component lookup failures
- Manages documentation query errors
- Handles service unavailability

### 3. Fallback Error Handling

The application implements fallback mechanisms for certain operations:

```python
async def _get_component(client: KeboolaClient, component_id: str) -> Component:
    try:
        # Try AI service first
        raw_component = await client.ai_service_client.get_component_detail(component_id=component_id)
        return Component.model_validate(raw_component)
    except HTTPStatusError as e:
        if e.response.status_code == 404:
            # Fallback to Storage API for private components
            endpoint = f'branch/{client.storage_client.branch_id}/components/{component_id}'
            raw_component = await client.storage_client.get(endpoint=endpoint)
            return Component.model_validate(raw_component)
```

## Validation Error Handling

### 1. JSON Schema Validation

The application uses custom JSON schema validation with enhanced error handling:

#### `KeboolaParametersValidator` Class
```python
class KeboolaParametersValidator:
    """Custom JSON Schema validator that handles UI elements and schema normalization."""
```

**Features**:
- **Schema sanitization**: Normalizes schemas for compatibility
- **UI element handling**: Ignores UI-only constructs like 'button' types
- **Required field normalization**: Converts boolean required flags to proper list format
- **Custom type checking**: Handles Keboola-specific data types

### 2. Validation Error Messages

Validation errors include detailed recovery instructions:

```python
class RecoverableValidationError(jsonschema.ValidationError):
    _RECOVERY_INSTRUCTIONS = (
        'Recovery instructions:\n'
        '- Please check the json schema.\n'
        '- Fix the errors in your input data to follow the schema.\n'
    )
```

**Error message structure**:
1. Original validation error message
2. Custom initial message (if provided)
3. Recovery instructions
4. Invalid input data (formatted JSON)

### 3. Configuration Validation

The application validates multiple types of configurations:

- **Storage configurations**: Validated against storage schema
- **Parameter configurations**: Validated against component schemas
- **Flow configurations**: Validated against flow schema
- **Component configurations**: Validated against component-specific schemas

## OAuth Error Handling

### 1. OAuth Callback Error Handling

The OAuth flow includes comprehensive error handling:

```python
async def oauth_callback_handler(request: Request) -> Response:
    code = request.query_params.get('code')
    state = request.query_params.get('state')

    if not code or not state:
        raise HTTPException(400, 'Missing code or state parameter')

    try:
        redirect_uri = await oauth_provider.handle_oauth_callback(code, state)
        return RedirectResponse(status_code=302, url=redirect_uri)
    except HTTPException:
        raise
    except Exception as e:
        LOG.exception(f'Failed to handle OAuth callback: {e}')
        return JSONResponse(status_code=500, content={'message': f'Unexpected error: {e}'})
```

**Error handling features**:
- **Parameter validation**: Checks for required OAuth parameters
- **State validation**: Validates JWT state tokens
- **Token exchange error handling**: Handles OAuth server communication failures
- **Graceful degradation**: Returns appropriate HTTP responses for different error types

### 2. JWT Token Error Handling

```python
try:
    state_data = jwt.decode(state, self._jwt_secret, algorithms=['HS256'])
except jwt.InvalidTokenError:
    LOG.debug(f'[handle_oauth_callback] Invalid state: {state}', exc_info=True)
    raise HTTPException(400, 'Invalid state parameter')
```

**JWT error handling**:
- **Invalid token handling**: Catches malformed JWT tokens
- **Expired token handling**: Validates token expiration
- **Signature verification**: Ensures token authenticity

## Server-Level Error Handling

### 1. Server Initialization Error Handling

```python
def create_server(config: Config) -> FastMCP:
    try:
        # Server initialization logic
        mcp = KeboolaMcpServer(
            name='Keboola Explorer',
            lifespan=create_keboola_lifespan(config),
            auth_server_provider=oauth_provider,
            auth=auth_settings,
        )
        return mcp
    except Exception as e:
        LOG.exception(f'Failed to create server: {e}')
        raise
```

### 2. Session State Error Handling

```python
def _create_session_state(config: Config) -> dict[str, Any]:
    state: dict[str, Any] = {}
    try:
        if not config.storage_token:
            raise ValueError('Storage API token is not provided.')
        if not config.storage_api_url:
            raise ValueError('Storage API URL is not provided.')
        client = KeboolaClient(config.storage_token, config.storage_api_url, bearer_token=config.bearer_token)
        state[KeboolaClient.STATE_KEY] = client
    except Exception as e:
        LOG.error(f'Failed to initialize Keboola client: {e}')
        raise
```

### 3. CLI Error Handling

```python
async def run_server(args: Optional[list[str]] = None) -> None:
    try:
        keboola_mcp_server = create_server(config)
        await keboola_mcp_server.run_async(transport=parsed_args.transport)
    except Exception as e:
        LOG.exception(f'Server failed: {e}')
        sys.exit(1)
```

## Configuration Error Handling

### 1. Configuration Validation

The `Config` class includes validation and error handling:

```python
def __post_init__(self) -> None:
    for f in dataclasses.fields(self):
        if 'url' not in f.name or f.name == 'accept_secrets_in_url':
            continue
        value = getattr(self, f.name)
        if value and not value.startswith(('http://', 'https://')):
            value = f'https://{value}'
            object.__setattr__(self, f.name, value)
```

**Configuration error handling features**:
- **URL normalization**: Automatically adds HTTPS scheme to URLs
- **Environment variable parsing**: Handles various naming conventions
- **Type conversion**: Converts string values to appropriate types
- **Sensitive data masking**: Masks tokens and secrets in string representations

### 2. Configuration Loading Error Handling

```python
@classmethod
def from_dict(cls, d: Mapping[str, str]) -> 'Config':
    """Creates new `Config` instance with values read from the input mapping."""
    return cls(**cls._read_options(d))
```

**Error handling features**:
- **Flexible key matching**: Handles different naming conventions (KBC_, X-, etc.)
- **Type validation**: Ensures proper type conversion
- **Default value handling**: Provides sensible defaults for missing values

## Logging and Monitoring

### 1. Comprehensive Logging

The application implements comprehensive logging throughout the error handling system:

```python
logging.exception(f'Failed to run tool {func.__name__}: {e}')
```

**Logging features**:
- **Exception context**: Includes function names and parameters
- **Stack traces**: Full exception stack traces for debugging
- **Structured logging**: Consistent log format across the application
- **Log levels**: Appropriate log levels (DEBUG, INFO, WARNING, ERROR, EXCEPTION)

### 2. Log Configuration

The application supports configurable logging:

```python
if parsed_args.log_config:
    logging.config.fileConfig(parsed_args.log_config, disable_existing_loggers=False)
elif os.environ.get('LOG_CONFIG'):
    logging.config.fileConfig(os.environ['LOG_CONFIG'], disable_existing_loggers=False)
else:
    logging.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s: %(message)s',
        level=parsed_args.log_level,
        stream=sys.stderr,
    )
```

### 3. User Agent Logging

The application includes user agent information for request tracking:

```python
@classmethod
def _get_user_agent(cls) -> str:
    try:
        version = importlib.metadata.version('keboola-mcp-server')
    except importlib.metadata.PackageNotFoundError:
        version = 'NA'
    app_env = os.getenv('APP_ENV', 'local')
    return f'Keboola MCP Server/{version} app_env={app_env}'
```

## Error Recovery Patterns

### 1. Graceful Degradation

The application implements graceful degradation for non-critical failures:

```python
async def _set_cfg_creation_metadata(client: KeboolaClient, component_id: str, configuration_id: str) -> None:
    try:
        await client.storage_client.configuration_metadata_update(
            component_id=component_id,
            configuration_id=configuration_id,
            metadata={MetadataField.CREATED_BY_MCP: 'true'},
        )
    except HTTPStatusError as e:
        LOG.exception(f'Failed to set "{MetadataField.CREATED_BY_MCP}" metadata for configuration {configuration_id}: {e}')
        # Continue execution even if metadata setting fails
```

### 2. Retry Logic

While not explicitly implemented in the current codebase, the error handling architecture supports retry logic through the decorator pattern.

### 3. Fallback Mechanisms

The application implements fallback mechanisms for certain operations:

- **Component lookup**: Falls back from AI service to Storage API
- **Authentication**: Supports both OAuth and direct token authentication
- **Configuration**: Handles missing configuration with sensible defaults

## Testing Error Handling

### 1. Unit Tests for Error Handling

The application includes comprehensive tests for error handling:

```python
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('function_fixture', 'default_recovery', 'recovery_instructions', 'expected_recovery_message', 'exception_message'),
    [
        # Test cases for different error scenarios
    ],
)
async def test_tool_function_recovery_instructions(
    function_fixture,
    default_recovery,
    recovery_instructions,
    expected_recovery_message,
    exception_message,
    request,
):
    """Test that the appropriate recovery message is applied based on the exception type."""
```

### 2. Test Coverage

**Error handling test coverage includes**:
- **Tool decorator testing**: Verifies `@tool_errors` decorator functionality
- **Recovery instruction testing**: Tests exception type mapping
- **Logging testing**: Verifies proper error logging
- **Exception transformation testing**: Ensures proper exception wrapping

### 3. Integration Testing

The application includes integration tests that verify error handling in real scenarios:

- **HTTP error handling**: Tests network failure scenarios
- **Validation error handling**: Tests invalid input scenarios
- **Authentication error handling**: Tests OAuth flow failures

## Best Practices Implemented

### 1. Exception Hierarchy

The application follows a clear exception hierarchy:
- **Base exceptions**: Standard Python exceptions
- **Custom exceptions**: `ToolException`, `RecoverableValidationError`
- **HTTP exceptions**: `HTTPStatusError`, `HTTPException`

### 2. Error Message Design

Error messages are designed to be:
- **Actionable**: Include specific recovery instructions
- **Contextual**: Include relevant context information
- **AI-friendly**: Structured for AI agent consumption
- **Human-readable**: Clear for human developers

### 3. Logging Strategy

The logging strategy ensures:
- **Comprehensive coverage**: All errors are logged
- **Appropriate levels**: Correct log levels for different error types
- **Structured format**: Consistent log message format
- **Sensitive data protection**: Tokens and secrets are masked

### 4. Error Propagation

Error propagation follows these principles:
- **Preserve context**: Original exceptions are preserved
- **Add value**: Recovery instructions are added
- **Maintain stack traces**: Full exception context is maintained
- **Appropriate abstraction**: Errors are handled at appropriate levels

## Conclusion

The Keboola MCP Server implements a comprehensive and sophisticated error handling system that ensures robust operation, meaningful error messages, and effective recovery mechanisms. The multi-layered approach provides both human developers and AI agents with the information they need to understand and resolve errors effectively.

The system's design principles of graceful degradation, comprehensive logging, and AI-friendly error messages make it well-suited for production use in complex data processing environments where reliability and maintainability are critical. 