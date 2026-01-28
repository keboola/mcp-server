import asyncio
import logging
from typing import Any, Optional, Union, cast

import httpx

JsonPrimitive = Union[int, float, str, bool, None]
JsonDict = dict[str, Union[JsonPrimitive, 'JsonStruct']]
JsonList = list[Union[JsonPrimitive, 'JsonStruct']]
JsonStruct = Union[JsonDict, JsonList]

LOG = logging.getLogger(__name__)

# Deadlock detection keywords for MySQL errors
DEADLOCK_KEYWORDS = ['deadlock', 'sqlstate[40001]', '1213']

# Retry configuration for deadlock errors
DEADLOCK_RETRY_MAX_ATTEMPTS = 3
DEADLOCK_RETRY_INITIAL_DELAY = 1.0
DEADLOCK_RETRY_MAX_DELAY = 10.0


def _is_deadlock_error(response: httpx.Response) -> bool:
    """
    Checks if the HTTP response indicates a MySQL deadlock error.

    Deadlock errors from the Connection API are returned as HTTP 500 errors
    with error messages containing deadlock-related keywords.

    :param response: The HTTP response to check
    :return: True if the response indicates a deadlock error, False otherwise
    """
    if response.status_code != 500:
        return False

    try:
        error_text = response.text.lower()
        return any(keyword in error_text for keyword in DEADLOCK_KEYWORDS)
    except Exception:
        return False


class RawKeboolaClient:
    """
    Raw async client for Keboola services.

    Implements the basic HTTP methods (GET, POST, PUT, DELETE)
    and can be used to implement high-level functions in clients for individual services.
    """

    def __init__(
        self,
        base_api_url: str,
        api_token: Optional[str],
        headers: dict[str, Any] | None = None,
        timeout: httpx.Timeout | None = None,
        readonly: bool | None = None,
    ) -> None:
        self.base_api_url = base_api_url
        self.headers = {
            'Content-Type': 'application/json',
            'Accept-Encoding': 'gzip',
        }
        if api_token:
            if api_token.startswith('Bearer '):
                self.headers['Authorization'] = api_token
            else:
                self.headers['X-StorageAPI-Token'] = api_token
        self.timeout = timeout or httpx.Timeout(connect=5.0, read=60.0, write=10.0, pool=5.0)
        if headers:
            self.headers.update(headers)
        self.readonly = readonly

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        """
        Checks the HTTP response status code and raises an exception with a detailed message. The message will
        include "error" and "exceptionId" fields if they are present in the response.
        """
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            message_parts = [str(e)]

            try:
                error_data = response.json()
                LOG.error(f'API error data: {error_data}')

                if error_msg := error_data.get('exception'):
                    # Query Service error message
                    message_parts.append(f'API error: {error_msg}')

                elif error_msg := error_data.get('error'):
                    # SAPI error message
                    message_parts.append(f'API error: {error_msg}')

                if exception_id := error_data.get('exceptionId'):
                    message_parts.append(f'Exception ID: {exception_id}')
                    message_parts.append('When contacting Keboola support please provide the exception ID.')

            except ValueError:
                try:
                    if response.text:
                        message_parts.append(f'API error: {response.text}')
                except Exception:
                    pass  # should never get here

            raise httpx.HTTPStatusError('\n'.join(message_parts), request=response.request, response=response) from e

    async def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> JsonStruct:
        """
        Makes a GET request to the service API.

        :param endpoint: API endpoint to call
        :param params: Query parameters for the request
        :param headers: Additional headers for the request
        :return: API response as dictionary
        """
        headers = self.headers | (headers or {})
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f'{self.base_api_url}/{endpoint}',
                params=params,
                headers=headers,
            )
            self._raise_for_status(response)
            return cast(JsonStruct, response.json())

    async def get_text(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> str:
        """
        Makes a GET request to the service API and returns the response as text.

        :param endpoint: API endpoint to call
        :param params: Query parameters for the request
        :param headers: Additional headers for the request
        :return: API response as text
        """
        headers = self.headers | (headers or {})
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f'{self.base_api_url}/{endpoint}',
                params=params,
                headers=headers,
            )
            self._raise_for_status(response)
            return cast(str, response.text)

    async def post(
        self,
        endpoint: str,
        data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> JsonStruct:
        """
        Makes a POST request to the service API.

        Includes retry logic with exponential backoff for MySQL deadlock errors (HTTP 500
        with deadlock-related error messages). This handles concurrent configuration updates
        that may cause serialization failures.

        :param endpoint: API endpoint to call
        :param data: Request payload
        :param params: Query parameters for the request
        :param headers: Additional headers for the request
        :return: API response as dictionary
        """
        if self.readonly:
            raise RuntimeError(f'Forbidden POST operation on a readonly client: {self.base_api_url}')

        headers = self.headers | (headers or {})

        for attempt in range(DEADLOCK_RETRY_MAX_ATTEMPTS + 1):
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f'{self.base_api_url}/{endpoint}',
                    params=params,
                    headers=headers,
                    json=data or {},
                )

                if response.is_success:
                    return cast(JsonStruct, response.json())

                if _is_deadlock_error(response) and attempt < DEADLOCK_RETRY_MAX_ATTEMPTS:
                    delay = min(
                        DEADLOCK_RETRY_INITIAL_DELAY * (2**attempt),
                        DEADLOCK_RETRY_MAX_DELAY,
                    )
                    LOG.warning(
                        f'Deadlock error detected on POST {endpoint}, '
                        f'attempt {attempt + 1}/{DEADLOCK_RETRY_MAX_ATTEMPTS + 1}. '
                        f'Retrying in {delay:.1f}s...'
                    )
                    await asyncio.sleep(delay)
                    continue

                self._raise_for_status(response)
                return cast(JsonStruct, response.json())

        self._raise_for_status(response)
        return cast(JsonStruct, response.json())

    async def put(
        self,
        endpoint: str,
        data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
    ) -> JsonStruct:
        """
        Makes a PUT request to the service API.

        :param endpoint: API endpoint to call
        :param data: Request payload
        :param params: Query parameters for the request
        :param headers: Additional headers for the request
        :return: API response as dictionary
        """
        if self.readonly:
            raise RuntimeError(f'Forbidden PUT operation on a readonly client: {self.base_api_url}')

        headers = self.headers | (headers or {})
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.put(
                f'{self.base_api_url}/{endpoint}',
                params=params,
                headers=headers,
                json=data or {},
            )
            self._raise_for_status(response)
            return cast(JsonStruct, response.json())

    async def delete(
        self,
        endpoint: str,
        headers: dict[str, Any] | None = None,
    ) -> JsonStruct | None:
        """
        Makes a DELETE request to the service API.

        :param endpoint: API endpoint to call
        :param headers: Additional headers for the request
        :return: API response as dictionary
        """
        if self.readonly:
            raise RuntimeError(f'Forbidden DELETE operation on a readonly client: {self.base_api_url}')

        headers = self.headers | (headers or {})
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.delete(
                f'{self.base_api_url}/{endpoint}',
                headers=headers,
            )
            self._raise_for_status(response)

            if response.content:
                return cast(JsonStruct, response.json())

            return None

    async def patch(
        self,
        endpoint: str,
        data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, Any]] = None,
    ) -> JsonStruct:
        """
        Makes a PATCH request to the service API.

        :param endpoint: API endpoint to call
        :param data: Request payload
        :param params: Query parameters for the request
        :param headers: Additional headers for the request
        :return: API response as dictionary
        """
        if self.readonly:
            raise RuntimeError(f'Forbidden PATCH operation on a readonly client: {self.base_api_url}')

        headers = self.headers | (headers or {})
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.patch(
                f'{self.base_api_url}/{endpoint}',
                params=params,
                headers=headers,
                json=data or {},
            )
            self._raise_for_status(response)
            return cast(JsonStruct, response.json())


class KeboolaServiceClient:
    """
    Base class for Keboola service clients.

    Implements the basic HTTP methods (GET, POST, PUT, DELETE)
    and is used as a base class for clients for individual services.
    """

    def __init__(self, raw_client: RawKeboolaClient) -> None:
        """
        Creates a client instance.

        The inherited classes should implement the `create` method
        rather than overriding this constructor.

        :param raw_client: The raw client to use
        """
        self.raw_client = raw_client

    async def get(
        self,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
    ) -> JsonStruct:
        """
        Makes a GET request to the service API.

        :param endpoint: API endpoint to call
        :param params: Query parameters for the request
        :return: API response as dictionary
        """
        return await self.raw_client.get(endpoint=endpoint, params=params)

    async def get_text(
        self,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
    ) -> str:
        """
        Makes a GET request to the service API.

        :param endpoint: API endpoint to call
        :param params: Query parameters for the request
        :return: API response as text
        """
        return await self.raw_client.get_text(endpoint=endpoint, params=params)

    async def post(
        self,
        endpoint: str,
        data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> JsonStruct:
        """
        Makes a POST request to the service API.

        :param endpoint: API endpoint to call
        :param data: Request payload
        :param params: Query parameters for the request
        :return: API response as dictionary
        """
        return await self.raw_client.post(endpoint=endpoint, data=data, params=params)

    async def put(
        self,
        endpoint: str,
        data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> JsonStruct:
        """
        Makes a PUT request to the service API.

        :param endpoint: API endpoint to call
        :param data: Request payload
        :param params: Query parameters for the request
        :return: API response as dictionary
        """
        return await self.raw_client.put(endpoint=endpoint, data=data, params=params)

    async def delete(
        self,
        endpoint: str,
    ) -> JsonStruct | None:
        """
        Makes a DELETE request to the service API.

        :param endpoint: API endpoint to call
        :return: API response as dictionary
        """
        return await self.raw_client.delete(endpoint=endpoint)

    async def patch(
        self,
        endpoint: str,
        data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> JsonStruct:
        """
        Makes a PATCH request to the service API.

        :param endpoint: API endpoint to call
        :param data: Request payload
        :param params: Query parameters for the request
        :return: API response as dictionary
        """
        return await self.raw_client.patch(endpoint=endpoint, data=data, params=params)
