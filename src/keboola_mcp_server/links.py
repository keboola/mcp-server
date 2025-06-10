from typing import Literal

from pydantic import BaseModel, Field

from keboola_mcp_server.client import KeboolaClient

URLType = Literal['ui-detail', 'ui-dashboard', 'docs']


class Link(BaseModel):
    type: URLType = Field(..., description='The type of the URL.')
    title: str = Field(..., description='The name of the URL.')
    url: str = Field(..., description='The URL.')


class ProjectLinksManager:

    FLOW_DOCUMENTATION_URL = 'https://help.keboola.com/flows/'

    def __init__(self, base_url: str, project_id: str):
        self.base_url = base_url
        self.project_id = project_id

    @classmethod
    async def from_client(cls, client: KeboolaClient) -> 'ProjectLinksManager':
        base_url = client.storage_client.base_api_url
        project_id = await client.storage_client.project_id()
        return ProjectLinksManager(base_url, project_id)

    def get_flow_url(self, flow_id: str | int) -> str:
        """Get the UI detail URL for a specific flow."""
        return f'{self.base_url}/admin/projects/{self.project_id}/flows/{flow_id}'

    def get_flows_dashboard_url(self) -> str:
        """Get the UI dashboard URL for all flows in a project."""
        return f'{self.base_url}/admin/projects/{self.project_id}/flows'

    def get_project_url(self) -> str:
        """Return the UI URL for accessing the project."""
        return f'{self.base_url}/admin/projects/{self.project_id}'

    def get_project_links(self) -> list[Link]:
        """Return a list of relevant links for a project."""
        project_url = self.get_project_url()
        return [Link(type='ui-detail', title='Project Dashboard', url=project_url)]

    def get_flow_links(self, flow_id: str | int, flow_name: str) -> list[Link]:
        """Get a list of relevant links for a flow, including detail, dashboard, and documentation."""
        flow_detail_url = Link(type='ui-detail', title=f'Flow: {flow_name}', url=self.get_flow_url(flow_id))
        flows_dashboard_url = Link(
            type='ui-dashboard', title='Flows in the project', url=self.get_flows_dashboard_url()
        )
        documentation_url = Link(type='docs', title='Documentation for Keboola Flows', url=self.FLOW_DOCUMENTATION_URL)
        return [flow_detail_url, flows_dashboard_url, documentation_url]
