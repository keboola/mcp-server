"""Keboola Manage API client for organization-level operations.

Used by the CLI init command to verify manage tokens, list organization projects,
and create Storage API tokens for MPA configuration.
"""

import logging
from typing import Any

import httpx

LOG = logging.getLogger(__name__)


class ManageClient:
    """Async HTTP client for the Keboola Manage API.

    Uses X-KBC-ManageApiToken for authentication (different from Storage API).
    """

    def __init__(self, stack_url: str, manage_token: str) -> None:
        self._base_url = stack_url.rstrip('/')
        self._headers = {
            'X-KBC-ManageApiToken': manage_token,
            'Content-Type': 'application/json',
            'Accept-Encoding': 'gzip',
        }
        self._timeout = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.request(
                method,
                f'{self._base_url}{path}',
                headers=self._headers,
                **kwargs,
            )
            response.raise_for_status()
            return response.json()

    async def verify_token(self) -> dict[str, Any]:
        """Verify the manage token and return token/user metadata.

        Returns dict with token info including 'user' block (id, name, email).
        """
        return await self._request('GET', '/manage/tokens/verify')

    async def get_project(self, project_id: int) -> dict[str, Any]:
        """Get project details by ID.

        Works with Personal Access Tokens (PAT) for projects where the
        token owner is a member.
        """
        return await self._request('GET', f'/manage/projects/{project_id}')

    async def list_organizations(self) -> list[dict[str, Any]]:
        """List all organizations accessible to the authenticated user."""
        return await self._request('GET', '/manage/organizations')

    async def get_organization(self, org_id: int) -> dict[str, Any]:
        """Get organization details including its projects.

        Returns org dict with 'projects' list containing all projects the user can access.
        """
        return await self._request('GET', f'/manage/organizations/{org_id}')

    async def create_project_token(
        self,
        project_id: int,
        description: str = 'keboola-mcp-server',
        can_manage_buckets: bool = True,
        can_read_all_file_uploads: bool = True,
        can_read_all_project_events: bool = True,
        can_manage_dev_branches: bool = True,
        can_manage_tokens: bool = True,
        expires_in: int | None = None,
    ) -> dict[str, Any]:
        """Create a new Storage API token for a project.

        Returns token dict including the 'token' field (shown only once).
        """
        payload: dict[str, Any] = {
            'description': description,
            'canManageBuckets': can_manage_buckets,
            'canReadAllFileUploads': can_read_all_file_uploads,
            'canReadAllProjectEvents': can_read_all_project_events,
            'canManageDevBranches': can_manage_dev_branches,
            'canManageTokens': can_manage_tokens,
        }
        if expires_in is not None:
            payload['expiresIn'] = expires_in
        return await self._request('POST', f'/manage/projects/{project_id}/tokens', json=payload)
