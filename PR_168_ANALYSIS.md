# PR #168 Analysis: Exception ID Forwarding Implementation

## Pull Request Overview
**Title:** KAB-1129 mcp server doesn t forward exceptionids from sapi client  
**URL:** https://github.com/keboola/mcp-server/pull/168  
**Purpose:** Forward exceptionId for HTTP500 errors so LLMs can instruct users to provide exception IDs to support.

## Code Review Comments Analysis

### 1. Architecture Concerns (@odinuv)

**Comment Location:** `src/keboola_mcp_server/client.py`
```python
raise KeboolaHTTPException(original_exception, exception_id, error_details)
else:
    # For all other HTTP errors, use standard HTTPStatusError
    response.raise_for_status()
```

**Reviewer's Concern:**
> "nevím no, asi bych šel už rovnou do Application+UserException konceptu? Pak by se dal udělat někde ten top level try-catch, ale tohle chce asi trošku probrat s nějakým python exception guru."

**Explanation provided:**
- **ApplicationException (5xx):** Doesn't display much information because user can't fix it
- **UserException (4xx):** End-user fixable errors converted to 4xx responses  
- **Top-level try-catch:** Converts UserException to 4xx and everything else to 5xx
- **Context matters:** A workspace creation error in background should be 5xx even if the API returns 4xx

**Analysis:** This suggests a fundamental architectural change to exception handling that goes beyond this PR's scope.

### 2. Import Style Issues (@vita-stejskal)

**Comment Location:** `src/keboola_mcp_server/client.py`
```python
from .errors import KeboolaHTTPException
```

**Issue:** "Please never use relative imports."

### 3. Code Redundancy (@vita-stejskal)

**Comment Location:** `src/keboola_mcp_server/client.py`
```python
if response.is_error:
    self._handle_http_error(response)
```

**Issue:** This check might be redundant after `response.raise_for_status()`

### 4. Message Constants (@mariankrotil & @vita-stejskal)

**Comment Location:** `src/keboola_mcp_server/errors.py`
```python
recovery_msg = f"{recovery_msg or 'Please try again later.'} For support reference Exception ID: {e.exception_id}"
```

**Issues:**
- Use a constant for "Please try again later." 
- Use `default_recovery_message` variable
- Split recovery instruction from support exception ID with newline
- Discuss whether exception ID should come before or after recovery message

### 5. Message Formatting (@mariankrotil)

**Comment Location:** `src/keboola_mcp_server/errors.py`
```python
base_message += f" (Exception ID: {self.exception_id})"
```

**Issue:** Consider dividing individual lines using newline for better readability.

### 6. Test Import Organization (@mariankrotil)

**Comment Location:** `tests/test_errors.py`
```python
def test_create_with_exception_id_for_500_error(self, mock_httpstatus_error_500):
    """Test that KeboolaHTTPException includes exception ID for HTTP 500 errors."""
    from keboola_mcp_server.errors import KeboolaHTTPException
```

**Issue:** Move imports to the top of the file instead of inline imports.

## Proposed Fixes

### Fix 1: Replace Relative Imports

**File:** `src/keboola_mcp_server/client.py`

**Current:**
```python
from .errors import KeboolaHTTPException
```

**Proposed:**
```python
from keboola_mcp_server.errors import KeboolaHTTPException
```

### Fix 2: Remove Code Redundancy

**File:** `src/keboola_mcp_server/client.py`

**Current:**
```python
async def get(self, endpoint: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> JsonStruct:
    # ... code ...
    response = await client.get(f'{self.base_api_url}/{endpoint}', params=params, headers=headers)
    
    if response.is_error:
        self._handle_http_error(response)
    
    return cast(JsonStruct, response.json())
```

**Proposed:**
```python
async def get(self, endpoint: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> JsonStruct:
    # ... code ...
    response = await client.get(f'{self.base_api_url}/{endpoint}', params=params, headers=headers)
    
    # _handle_http_error already calls raise_for_status internally for non-500 errors
    self._handle_http_error(response)
    
    return cast(JsonStruct, response.json())
```

**Note:** Update `_handle_http_error` to handle all error cases:
```python
def _handle_http_error(self, response: httpx.Response) -> None:
    """Enhanced error handling that extracts API error details for HTTP 500 errors."""
    
    if not response.is_error:
        return  # No error, continue normally
    
    if response.status_code == 500:
        # Enhanced handling for HTTP 500 errors
        try:
            error_data = response.json()
            exception_id = error_data.get('exceptionId')
            error_details = {
                'message': error_data.get('message'),
                'error_code': error_data.get('errorCode'),
                'request_id': error_data.get('requestId')
            }
        except (ValueError, KeyError):
            exception_id = None
            error_details = None
        
        original_exception = httpx.HTTPStatusError(
            f"Server error '{response.status_code} {response.reason_phrase}' for url '{response.url}'",
            request=response.request,
            response=response
        )
        
        raise KeboolaHTTPException(original_exception, exception_id, error_details)
    else:
        # For all other HTTP errors, use standard HTTPStatusError
        response.raise_for_status()
```

### Fix 3: Improve Message Constants and Formatting

**File:** `src/keboola_mcp_server/errors.py`

**Add constants at the top:**
```python
# Constants for error messages
DEFAULT_RECOVERY_MESSAGE = "Please try again later."
EXCEPTION_ID_PREFIX = "For support reference Exception ID:"
```

**Update the tool_errors decorator:**
```python
def tool_errors(
    default_recovery: Optional[str] = None,
    recovery_instructions: Optional[dict[Type[Exception], str]] = None,
) -> Callable[[F], F]:
    """Enhanced MCP tool function decorator with improved HTTP 500 error handling."""

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
                
                # Use default recovery message if none specified
                if not recovery_msg:
                    recovery_msg = DEFAULT_RECOVERY_MESSAGE

                # Special handling for KeboolaHTTPException (HTTP 500 errors only)
                if isinstance(e, KeboolaHTTPException) and e.original_exception.response.status_code == 500:
                    if e.exception_id:
                        # Split messages with newline for better readability
                        recovery_msg = f"{recovery_msg}\n{EXCEPTION_ID_PREFIX} {e.exception_id}"

                raise ToolException(e, recovery_msg) from e

        return cast(F, wrapped)
    return decorator
```

**Update KeboolaHTTPException message building:**
```python
def _build_error_message(self) -> str:
    """Build a comprehensive error message including exceptionId for HTTP 500 errors."""
    base_message = str(self.original_exception)
    
    # Only enhance HTTP 500 errors with exception ID
    if self.original_exception.response.status_code == 500 and self.exception_id:
        # Use newline for better readability
        base_message += f"\n(Exception ID: {self.exception_id})"
    
    if self.error_details and 'message' in self.error_details:
        # Add relevant error details without exposing sensitive information
        base_message += f"\n{self.error_details['message']}"
    
    return base_message
```

### Fix 4: Organize Test Imports

**File:** `tests/test_errors.py`

**Move all imports to the top:**
```python
import logging
from unittest.mock import Mock, AsyncMock, patch

import httpx
import pytest

from keboola_mcp_server.errors import ToolException, tool_errors, KeboolaHTTPException
from keboola_mcp_server.client import RawKeboolaClient
```

**Remove inline imports from test methods:**
```python
def test_create_with_exception_id_for_500_error(self, mock_httpstatus_error_500):
    """Test that KeboolaHTTPException includes exception ID for HTTP 500 errors."""
    # Remove: from keboola_mcp_server.errors import KeboolaHTTPException
    
    exception_id = "abc123-def456-ghi789"
    error_details = {"message": "Internal server error occurred"}
    
    keboola_exception = KeboolaHTTPException(
        mock_httpstatus_error_500, 
        exception_id, 
        error_details
    )
    # ... rest of test
```

## Architectural Recommendation

While the current PR addresses the immediate need for exception ID forwarding, consider implementing the **Application/UserException pattern** suggested by @odinuv in a follow-up PR:

### Proposed Exception Hierarchy
```python
class ApplicationException(Exception):
    """5xx errors - system/infrastructure issues user cannot fix"""
    pass

class UserException(Exception):
    """4xx errors - user-fixable issues"""
    pass

class KeboolaApplicationException(ApplicationException):
    """Keboola-specific application exceptions with exception ID support"""
    def __init__(self, message: str, exception_id: str | None = None):
        self.exception_id = exception_id
        super().__init__(message)

class KeboolaUserException(UserException):
    """Keboola-specific user exceptions"""
    pass
```

### Top-Level Exception Handler
```python
def handle_api_exceptions(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except UserException as e:
            # Convert to 4xx response
            raise HTTPException(status_code=400, detail=str(e))
        except ApplicationException as e:
            # Convert to 5xx response with minimal info
            if hasattr(e, 'exception_id') and e.exception_id:
                detail = f"Internal server error. Reference ID: {e.exception_id}"
            else:
                detail = "Internal server error"
            raise HTTPException(status_code=500, detail=detail)
        except Exception as e:
            # Unhandled exceptions become 5xx
            LOG.exception("Unhandled exception")
            raise HTTPException(status_code=500, detail="Internal server error")
    return wrapper
```

This architectural change would provide better separation of concerns and more consistent error handling across the application.

## Summary

The current PR implementation is functional but could benefit from the proposed fixes to improve code quality, maintainability, and user experience. The architectural suggestions should be considered for future iterations to create a more robust error handling system. 