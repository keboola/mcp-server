from typing import Literal

from pydantic import BaseModel, Field

URLType = Literal['ui-detail', 'ui-dashboard', 'docs']


class Link(BaseModel):
    type: URLType = Field(..., description='The type of the URL.')
    title: str = Field(..., description='The name of the URL.')
    url: str = Field(..., description='The URL.')


class LinksManager:

    FLOW_DOCUMENTATION_URL = 'https://help.keboola.com/flows/'

    def get_flow_url(self, base_url: str, project_id: str, flow_id: str | int) -> str:
        """
        Get the UI detail URL for a specific flow.

        :param base_url: The base URL of the Keboola instance.
        :param project_id: The project ID.
        :param flow_id: The flow ID.
        :return: The URL to the flow detail page.
        """
        return f'{base_url}/admin/projects/{project_id}/flows/{flow_id}'

    def get_flows_dashboard_url(self, base_url: str, project_id: str) -> str:
        """
        Get the UI dashboard URL for all flows in a project.

        :param base_url: The base URL of the Keboola instance.
        :param project_id: The project ID.
        :return: The URL to the flows dashboard.
        """
        return f'{base_url}/admin/projects/{project_id}/flows'

    def get_flow_links(self, base_url: str, project_id: str, flow_id: str | int, flow_name: str) -> list:
        """
        Get a list of relevant links for a flow, including detail, dashboard, and documentation.

        :param base_url: The base URL of the Keboola instance.
        :param project_id: The project ID.
        :param flow_id: The flow ID.
        :param flow_name: The flow name.
        :return: List of Link objects for the flow.
        """
        flow_url = Link(type='ui-detail', title=f'Flow: {flow_name}',
                        url=self.get_flow_url(base_url, project_id, flow_id))
        flows_url = Link(type='ui-dashboard', title='Flows in the project',
                         url=self.get_flows_dashboard_url(base_url, project_id))
        documentation_url = Link(type='docs', title='Documentation for Keboola Flows', url=self.FLOW_DOCUMENTATION_URL)
        return [flow_url, flows_url, documentation_url]
