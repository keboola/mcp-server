# HTTP 500 Error Handling Improvement Plan

## Overview

This document outlines a comprehensive plan to improve HTTP 500 error handling in the Keboola MCP Server by extracting and including the `exceptionId` parameter from API error responses. **Only HTTP 500 errors require this enhancement** - other HTTP errors (4xx, other 5xx) already provide meaningful error messages and should continue to use the existing error handling.

**This implementation follows Test-Driven Development (TDD) principles: tests are written first to define success criteria, then implementation follows.**

## Current State Analysis

### Current Error Message Format
```
Error calling tool 'query_table': Server error '500 Internal Server Error' for url 'https://connection.us-east4.gcp.keboola.com/v2/storage/branch/default/workspaces/13408634/query' For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/500
```

### Current HTTP Error Handling Flow
1. **HTTP Request**: Made via `RawKeboolaClient` methods (get, post, put, delete)
2. **Status Check**: `response.raise_for_status()` raises `HTTPStatusError` for 4xx/5xx responses
3. **Exception Propagation**: `HTTPStatusError` propagates up through the call stack
4. **Tool Error Handling**: `@tool_errors` decorator catches and transforms exceptions
5. **Final Message**: Generic HTTP error message without API-specific details

### Error Type Analysis

#### HTTP 500 Errors (Target for Enhancement)
- **Problem**: Generic error messages without debugging information
- **Solution**: Extract and include `exceptionId` for server-side debugging
- **Example**: `500 Internal Server Error` → `500 Internal Server Error (Exception ID: abc123-def456-ghi789)`

#### HTTP 4xx Errors (Already Meaningful)
- **Status**: Already provide meaningful error messages
- **Action**: No changes needed
- **Examples**: 
  - `400 Bad Request` - Invalid parameters
  - `401 Unauthorized` - Authentication issues
  - `403 Forbidden` - Permission issues
  - `404 Not Found` - Resource not found

#### Other HTTP 5xx Errors (Already Meaningful)
- **Status**: Already provide meaningful error messages
- **Action**: No changes needed
- **Examples**:
  - `502 Bad Gateway` - Upstream service issues
  - `503 Service Unavailable` - Service temporarily unavailable
  - `504 Gateway Timeout` - Request timeout

### Missing Information (HTTP 500 Only)
- **exceptionId**: Unique identifier for server-side errors (not currently extracted)
- **Error Details**: Specific error information from API response body
- **Context**: Additional debugging information from response headers

## Proposed Solution

### 1. Custom HTTP Exception Class

Create a new exception class that preserves API error details for HTTP 500 errors only:

```python
class KeboolaHTTPException(HTTPStatusError):
    """Enhanced HTTP exception that includes Keboola API error details for HTTP 500 errors."""
    
    def __init__(self, original_exception: HTTPStatusError, exception_id: str | None = None, error_details: dict | None = None):
        self.exception_id = exception_id
        self.error_details = error_details or {}
        self.original_exception = original_exception
        
        # Build enhanced error message
        message = self._build_error_message()
        super().__init__(message, request=original_exception.request, response=original_exception.response)
    
    def _build_error_message(self) -> str:
        """Build a comprehensive error message including exceptionId for HTTP 500 errors."""
        base_message = str(self.original_exception)
        
        # Only enhance HTTP 500 errors with exception ID
        if self.original_exception.response.status_code == 500 and self.exception_id:
            base_message += f" (Exception ID: {self.exception_id})"
        
        if self.error_details:
            # Add relevant error details without exposing sensitive information
            if 'message' in self.error_details:
                base_message += f" - {self.error_details['message']}"
        
        return base_message
```

### 2. Enhanced HTTP Client Error Handling

Modify the `RawKeboolaClient` to extract error details only for HTTP 500 errors:

```python
class RawKeboolaClient:
    async def _handle_http_error(self, response: httpx.Response) -> None:
        """Enhanced error handling that extracts API error details for HTTP 500 errors only."""
        
        # Only enhance HTTP 500 errors with exception ID extraction
        if response.status_code == 500:
            try:
                # Try to parse error response for HTTP 500 errors
                error_data = response.json()
                exception_id = error_data.get('exceptionId')
                error_details = {
                    'message': error_data.get('message'),
                    'error_code': error_data.get('errorCode'),
                    'request_id': error_data.get('requestId')
                }
            except (ValueError, KeyError):
                # Fallback to basic error handling
                exception_id = None
                error_details = None
            
            # Create enhanced exception for HTTP 500 errors
            original_exception = HTTPStatusError(
                f"Server error '{response.status_code} {response.reason_phrase}' for url '{response.url}'",
                request=response.request,
                response=response
            )
            
            raise KeboolaHTTPException(original_exception, exception_id, error_details)
        else:
            # For all other HTTP errors, use standard HTTPStatusError
            response.raise_for_status()
    
    async def get(self, endpoint: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> JsonStruct:
        headers = self.headers | (headers or {})
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f'{self.base_api_url}/{endpoint}',
                params=params,
                headers=headers,
            )
            
            if response.is_error:
                await self._handle_http_error(response)
            
            return cast(JsonStruct, response.json())
```

### 3. Updated Tool Error Handling

Enhance the `@tool_errors` decorator to handle the new exception type for HTTP 500 errors:

```python
def tool_errors(
    default_recovery: Optional[str] = None,
    recovery_instructions: Optional[dict[Type[Exception], str]] = None,
) -> Callable[[F], F]:
    """
    Enhanced MCP tool function decorator with improved HTTP 500 error handling.
    """
    
    def decorator(func: Callable):
        @wraps(func)
        async def wrapped(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logging.exception(f'Failed to run tool {func.__name__}: {e}')
                
                # Enhanced recovery message for HTTP 500 errors
                recovery_msg = default_recovery
                if recovery_instructions:
                    for exc_type, msg in recovery_instructions.items():
                        if isinstance(e, exc_type):
                            recovery_msg = msg
                            break
                
                # Special handling for KeboolaHTTPException (HTTP 500 errors only)
                if isinstance(e, KeboolaHTTPException) and e.original_exception.response.status_code == 500:
                    if e.exception_id:
                        recovery_msg = f"{recovery_msg or 'Please try again later.'} For debugging, reference Exception ID: {e.exception_id}"
                
                if not recovery_msg:
                    raise e
                
                raise ToolException(e, recovery_msg) from e
        
        return cast(F, wrapped)
    
    return decorator
```

### 4. Updated Recovery Instructions

Enhance recovery instructions to include exception ID information only for HTTP 500 errors:

```python
# Example usage in tools
@tool_errors(
    default_recovery="Please check your request parameters and try again.",
    recovery_instructions={
        KeboolaHTTPException: "The request failed due to a server error. Please check the exception ID for debugging.",
        HTTPStatusError: "The request failed. Please verify your authentication and try again.",
        ValueError: "Invalid input parameters. Please check your data format."
    }
)
async def query_table(ctx: Context, sql_query: str) -> QueryResult:
    # Implementation
    pass
```

## TDD Implementation Plan

### Phase 1: Test-Driven Development - Core Infrastructure (Week 1)

#### 1.1 Test-First: Enhanced Exception Classes ✅ COMPLETED
**Test Phase:**
- [x] Write unit tests for `KeboolaHTTPException` class creation and message formatting
- [x] Test exception ID inclusion only for HTTP 500 errors
- [x] Test fallback behavior when exception ID is missing
- [x] Test error details inclusion and filtering
- [x] Test that other HTTP status codes don't get enhanced
- [x] Define success criteria: Exception ID appears in error message only for HTTP 500

**Implementation Phase:**
- [x] Create `KeboolaHTTPException` class in `src/keboola_mcp_server/errors.py`
- [x] Implement `_build_error_message` method with HTTP 500-specific logic
- [x] Add type hints and imports
- [x] Run tests to verify implementation meets criteria

**Results:** All 6 tests passing. The `KeboolaHTTPException` class correctly:
- Includes exception ID in error messages only for HTTP 500 errors
- Preserves standard error messages for other HTTP status codes
- Handles missing exception IDs gracefully
- Filters error details appropriately
- Maintains compatibility with existing `HTTPStatusError` interface

#### 1.2 Test-First: Enhanced HTTP Client Error Handling
**Test Phase:**
- [ ] Write unit tests for `_handle_http_error` method
- [ ] Test HTTP 500 error handling with valid JSON response containing exceptionId
- [ ] Test HTTP 500 error handling with malformed JSON response (fallback)
- [ ] Test HTTP 500 error handling with missing exceptionId
- [ ] Test that other HTTP errors (4xx, other 5xx) continue to use standard HTTPStatusError
- [ ] Test integration with all HTTP methods (get, post, put, delete)
- [ ] Define success criteria: Only HTTP 500 errors get enhanced, others remain unchanged

**Implementation Phase:**
- [ ] Add `_handle_http_error` method to `RawKeboolaClient`
- [ ] Implement HTTP 500-specific error handling logic
- [ ] Update all HTTP methods to use enhanced error handling
- [ ] Ensure other HTTP errors continue to use standard `HTTPStatusError`
- [ ] Run tests to verify implementation meets criteria

#### 1.3 Test-First: Enhanced Tool Error Decorator
**Test Phase:**
- [ ] Write unit tests for enhanced `@tool_errors` decorator
- [ ] Test HTTP 500 error handling with exception ID inclusion in recovery message
- [ ] Test HTTP 500 error handling without exception ID (fallback)
- [ ] Test that other HTTP errors maintain existing behavior
- [ ] Test logging includes exception ID when available
- [ ] Test integration with existing recovery instructions
- [ ] Define success criteria: Recovery messages include exception ID for HTTP 500 errors only

**Implementation Phase:**
- [ ] Enhance `@tool_errors` decorator to handle `KeboolaHTTPException`
- [ ] Add special recovery message formatting for HTTP 500 errors only
- [ ] Update logging to include exception ID when available
- [ ] Ensure other HTTP errors maintain existing behavior
- [ ] Run tests to verify implementation meets criteria

### Phase 2: Test-Driven Development - Integration and Validation (Week 2)

#### 2.1 Test-First: Service Client Integration
**Test Phase:**
- [ ] Write integration tests for all service clients (Storage, Jobs, AI Service)
- [ ] Test HTTP 500 error handling across different API endpoints
- [ ] Test exception ID extraction works for all service clients
- [ ] Test that other HTTP errors maintain existing behavior across all services
- [ ] Test error propagation through service client hierarchy
- [ ] Define success criteria: All service clients support enhanced HTTP 500 error handling

**Implementation Phase:**
- [ ] Ensure all service clients inherit enhanced error handling
- [ ] Test error handling across different API endpoints
- [ ] Verify exception ID extraction works for HTTP 500 errors only
- [ ] Confirm other HTTP errors maintain existing behavior
- [ ] Run integration tests to verify implementation meets criteria

#### 2.2 Test-First: Tool Recovery Instructions
**Test Phase:**
- [ ] Write tests for updated recovery instructions across all tools
- [ ] Test HTTP 500 error handling in component tools
- [ ] Test HTTP 500 error handling in job tools
- [ ] Test HTTP 500 error handling in storage tools
- [ ] Test HTTP 500 error handling in SQL tools
- [ ] Test that other HTTP errors maintain existing recovery messages
- [ ] Define success criteria: All tools include exception ID in recovery messages for HTTP 500 errors

**Implementation Phase:**
- [ ] Review and update recovery instructions for all tools
- [ ] Add specific recovery messages for HTTP 500 errors
- [ ] Ensure exception ID is included in user-facing messages for HTTP 500 only
- [ ] Maintain existing recovery messages for other HTTP errors
- [ ] Run tests to verify implementation meets criteria

#### 2.3 Test-First: End-to-End Validation
**Test Phase:**
- [ ] Write end-to-end tests with mock HTTP 500 responses
- [ ] Test complete error flow from HTTP client to tool decorator to user message
- [ ] Test error message format matches requirements for HTTP 500 errors
- [ ] Test that other HTTP errors maintain existing behavior end-to-end
- [ ] Test logging includes exception ID information for HTTP 500 errors
- [ ] Define success criteria: Complete error flow works correctly for HTTP 500 errors

**Implementation Phase:**
- [ ] Run comprehensive end-to-end tests
- [ ] Validate error message format in actual tool execution for HTTP 500 errors
- [ ] Ensure logging includes exception ID information for HTTP 500 errors
- [ ] Verify other HTTP errors maintain existing behavior
- [ ] Run tests to verify implementation meets criteria

### Phase 3: Test-Driven Development - Documentation and Production Validation (Week 3)

#### 3.1 Test-First: Documentation Validation
**Test Phase:**
- [ ] Write tests to validate documented behavior
- [ ] Test that all documented examples work correctly
- [ ] Test that error handling behavior matches documentation
- [ ] Define success criteria: Documentation accurately reflects implementation

**Implementation Phase:**
- [ ] Update `ERROR_HANDLING.md` with new HTTP 500 error handling details
- [ ] Add examples of enhanced error messages for HTTP 500 errors
- [ ] Document exception ID usage for debugging
- [ ] Clarify that other HTTP errors remain unchanged
- [ ] Run documentation validation tests

#### 3.2 Test-First: Production Readiness
**Test Phase:**
- [ ] Write tests with real API error response formats
- [ ] Test error handling with various exception ID formats
- [ ] Test performance impact of enhanced error handling
- [ ] Test backward compatibility with existing error handling
- [ ] Define success criteria: Production-ready implementation with no regressions

**Implementation Phase:**
- [ ] Test with real API error scenarios
- [ ] Validate error message format matches requirements for HTTP 500 errors
- [ ] Ensure backward compatibility with existing error handling for other HTTP errors
- [ ] Verify no regressions in existing functionality
- [ ] Run production readiness tests

## Test-Driven Success Criteria

### Unit Test Success Criteria
1. **Exception Class Tests**: `KeboolaHTTPException` correctly formats messages with exception ID for HTTP 500 errors only
2. **HTTP Client Tests**: `_handle_http_error` extracts exception ID for HTTP 500 errors and preserves standard behavior for others
3. **Tool Decorator Tests**: `@tool_errors` includes exception ID in recovery messages for HTTP 500 errors only
4. **Fallback Tests**: All components gracefully handle missing or malformed exception IDs

### Integration Test Success Criteria
1. **Service Client Tests**: All service clients support enhanced HTTP 500 error handling
2. **Error Propagation Tests**: Exception ID correctly propagates through the entire error handling chain
3. **Backward Compatibility Tests**: Other HTTP errors maintain existing behavior across all components

### End-to-End Test Success Criteria
1. **Complete Flow Tests**: HTTP 500 errors with exception ID produce correct user-facing messages
2. **Logging Tests**: Exception ID appears in logs for HTTP 500 errors
3. **Performance Tests**: Enhanced error handling has minimal performance impact
4. **Regression Tests**: No existing functionality is broken

## Expected Results

### Enhanced Error Messages (HTTP 500 Only)
**Before:**
```
Error calling tool 'query_table': Server error '500 Internal Server Error' for url 'https://connection.us-east4.gcp.keboola.com/v2/storage/branch/default/workspaces/13408634/query' For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/500
```

**After:**
```
Error calling tool 'query_table': Server error '500 Internal Server Error' for url 'https://connection.us-east4.gcp.keboola.com/v2/storage/branch/default/workspaces/13408634/query' (Exception ID: abc123-def456-ghi789) - Internal server error occurred while processing your request. For debugging, reference Exception ID: abc123-def456-ghi789
```

### Unchanged Error Messages (Other HTTP Errors)
**HTTP 400, 401, 403, 404, 502, 503, 504, etc. remain unchanged:**
```
Error calling tool 'query_table': Server error '404 Not Found' for url 'https://connection.us-east4.gcp.keboola.com/v2/storage/branch/default/workspaces/99999999/query'
```

### Benefits
1. **Improved Debugging**: Exception ID provides direct reference for server-side debugging of HTTP 500 errors
2. **Better User Experience**: More informative error messages for HTTP 500 errors
3. **Enhanced Logging**: Exception ID included in logs for HTTP 500 error correlation
4. **Maintained Compatibility**: Existing error handling patterns preserved for all other HTTP errors
5. **Focused Enhancement**: Only targets the specific problem area (HTTP 500 errors) without unnecessary changes
6. **Test-Driven Quality**: TDD approach ensures comprehensive test coverage and validates success criteria

## Technical Considerations

### Error Response Parsing
- Handle cases where error response is not valid JSON for HTTP 500 errors
- Gracefully fall back to basic error handling for HTTP 500 errors
- Preserve original exception information
- Ensure other HTTP errors continue to use standard error handling

### Security and Privacy
- Ensure sensitive information is not exposed in error messages
- Filter error details to include only debugging-relevant information for HTTP 500 errors
- Maintain existing security practices for all HTTP errors

### Performance Impact
- Minimal overhead from additional error parsing (only for HTTP 500 errors)
- Error handling only occurs on actual errors
- No impact on successful request performance
- No impact on other HTTP error handling performance

### Backward Compatibility
- Existing error handling continues to work for all HTTP errors except 500
- New features are additive, not breaking
- Gradual migration path for existing code
- No changes to existing HTTP 4xx and other 5xx error handling

## TDD Testing Strategy

### Red-Green-Refactor Cycle
1. **Red**: Write failing tests that define the desired behavior
2. **Green**: Implement minimal code to make tests pass
3. **Refactor**: Improve code while keeping tests green

### Test Categories
1. **Unit Tests**: Test individual components in isolation
2. **Integration Tests**: Test component interactions
3. **End-to-End Tests**: Test complete error handling flows
4. **Regression Tests**: Ensure existing functionality remains intact

### Test Data Management
- Mock HTTP responses with various exception ID formats
- Test both valid and invalid JSON error responses
- Test all HTTP status codes to ensure proper behavior
- Use realistic API error response structures

## Success Criteria (Test-Defined)

### Functional Success Criteria
1. **Exception ID Inclusion**: All HTTP 500 errors include the exception ID in the error message (validated by tests)
2. **Enhanced Debugging**: Developers can use exception ID to correlate with server logs for HTTP 500 errors (validated by logging tests)
3. **Improved UX**: AI agents receive more actionable error information for HTTP 500 errors (validated by message format tests)
4. **Maintained Stability**: No regressions in existing error handling functionality for other HTTP errors (validated by regression tests)
5. **Focused Enhancement**: Only HTTP 500 errors are enhanced, other HTTP errors remain unchanged (validated by comprehensive status code tests)
6. **Comprehensive Coverage**: All HTTP client methods support enhanced error handling for HTTP 500 errors (validated by integration tests)

### Quality Success Criteria
1. **Test Coverage**: All new functionality has comprehensive test coverage
2. **Test-Driven**: All features are developed using TDD methodology
3. **Regression Prevention**: Existing functionality is protected by tests
4. **Documentation Accuracy**: Documentation matches actual implementation behavior

## Future Enhancements

### Phase 4: Advanced Features (Future)
- [ ] Add error categorization based on exception ID patterns for HTTP 500 errors
- [ ] Implement error tracking and analytics for HTTP 500 errors
- [ ] Add automatic retry logic for specific HTTP 500 error types
- [ ] Create error reporting dashboard integration for HTTP 500 errors

### Phase 5: Monitoring and Alerting (Future)
- [ ] Add metrics for different HTTP 500 error types
- [ ] Implement alerting for high-frequency HTTP 500 errors
- [ ] Create error trend analysis for HTTP 500 errors
- [ ] Add performance impact tracking for HTTP 500 errors

## Conclusion

This improvement plan will significantly enhance the debugging capabilities of the Keboola MCP Server by providing detailed error information including exception IDs for HTTP 500 errors only. The implementation follows Test-Driven Development principles, ensuring that success criteria are defined by tests before implementation begins.

**Key Focus**: Only HTTP 500 errors are enhanced with exception ID extraction, while all other HTTP errors (4xx, other 5xx) maintain their existing meaningful error messages and handling patterns.

**TDD Approach**: Tests are written first to define success criteria, then implementation follows to meet those criteria. This ensures comprehensive test coverage and validates that the implementation meets all requirements.

The phased TDD approach ensures minimal disruption to existing functionality while delivering targeted improvements for the specific problem area (HTTP 500 errors) that can be validated and refined throughout the implementation process. 