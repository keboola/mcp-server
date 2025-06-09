from typing import Literal

from pydantic import BaseModel, Field

URLType = Literal['ui-detail', 'ui-dashboard', 'docs']


class Link(BaseModel):
    type: URLType = Field(..., description='The type of the URL.')
    title: str = Field(..., description='The name of the URL.')
    url: str = Field(..., description='The URL.')


class LinksManager:

    FLOW_DOCUMENTATION_URL = 'https://help.keboola.com/flows/'

    def __init__(self, base_url: str):
        self.base_url = base_url

    def get_flow_url(self, project_id: str, flow_id: str | int) -> str:
        """Get the UI detail URL for a specific flow."""
        return f'{self.base_url}/admin/projects/{project_id}/flows/{flow_id}'

    def get_flows_dashboard_url(self, project_id: str) -> str:
        """Get the UI dashboard URL for all flows in a project."""
        return f'{self.base_url}/admin/projects/{project_id}/flows'

    def get_project_url(self, project_id: str) -> str:
        """Return the UI URL for accessing the project."""
        return f'{self.base_url}/admin/projects/{project_id}'

    def get_project_links(self, project_id: str) -> list[Link]:
        """Return a list of relevant links for a project."""
        project_url = self.get_project_url(project_id)
        return [Link(type='ui-detail', title='Project Dashboard', url=project_url)]

    def get_flow_links(self, project_id: str, flow_id: str | int, flow_name: str) -> list[Link]:
        """Get a list of relevant links for a flow, including detail, dashboard, and documentation."""
        flow_url = Link(type='ui-detail', title=f'Flow: {flow_name}', url=self.get_flow_url(project_id, flow_id))
        flows_url = Link(
            type='ui-dashboard', title='Flows in the project', url=self.get_flows_dashboard_url(project_id)
        )
        documentation_url = Link(type='docs', title='Documentation for Keboola Flows', url=self.FLOW_DOCUMENTATION_URL)
        return [flow_url, flows_url, documentation_url]
