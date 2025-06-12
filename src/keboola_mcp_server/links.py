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

    def _detail_link(self, label: str, name: str, url: str) -> Link:
        """Builds object detail link."""
        return Link(type='ui-detail', title=f'{label}: {name}', url=url)

    def _dashboard_link(self, title: str, url: str) -> Link:
        """Builds object dashboard link."""
        return Link(type='ui-dashboard', title=title, url=url)

    def _docs_link(self, title: str, url: str) -> Link:
        """Builds object documentation link."""
        return Link(type='docs', title=title, url=url)

    def get_component_configuration_url(self, component_id: str, configuration_id: str) -> str:
        """Get the UI detail URL for a specific component configuration."""
        return f'{self.base_url}/admin/projects/{self.project_id}/components/{component_id}/{configuration_id}'

    def get_component_configurations_dashboard_url(self, component_id: str) -> str:
        """Get the UI dashboard URL for all configurations of a component."""
        return f'{self.base_url}/admin/projects/{self.project_id}/components/{component_id}'

    def get_flow_url(self, flow_id: str | int) -> str:
        """Get the UI detail URL for a specific flow."""
        return f'{self.base_url}/admin/projects/{self.project_id}/flows/{flow_id}'

    def get_flows_dashboard_url(self) -> str:
        """Get the UI dashboard URL for all flows in a project."""
        return f'{self.base_url}/admin/projects/{self.project_id}/flows'

    def get_job_url(self, job_id: str) -> str:
        """Get the UI detail URL for a specific job."""
        return f'{self.base_url}/admin/projects/{self.project_id}/queue/{job_id}'

    def get_jobs_dashboard_url(self) -> str:
        """Get the UI dashboard URL for all jobs in a project."""
        return f'{self.base_url}/admin/projects/{self.project_id}/queue'

    def get_bucket_url(self, bucket_id: str) -> str:
        """Get the UI detail URL for a specific bucket."""
        return f'{self.base_url}/admin/projects/{self.project_id}/storage/{bucket_id}'

    def get_buckets_dashboard_url(self) -> str:
        """Get the UI dashboard URL for all buckets in a project."""
        return f'{self.base_url}/admin/projects/{self.project_id}/storage'

    def get_project_url(self) -> str:
        """Return the UI URL for accessing the project."""
        return f'{self.base_url}/admin/projects/{self.project_id}'
    
    def get_table_url(self, bucket_id: str, table_name: str) -> str:
        """Return the UI URL for accessing the table of a bucket."""
        return f'{self.base_url}/admin/projects/{self.project_id}/storage/{bucket_id}/table/{table_name}'
    
    def get_project_links(self) -> list[Link]:
        """Return a list of relevant links for a project."""
        return [self._detail_link('Project Dashboard', '', self.get_project_url())]

    def get_flow_links(self, flow_id: str | int, flow_name: str) -> list[Link]:
        """Links for flow: detail, dashboard, documentation."""
        return [
            self._detail_link('Flow', flow_name, self.get_flow_url(flow_id)),
            self._dashboard_link('Flows in the project', self.get_flows_dashboard_url()),
            self._docs_link('Documentation for Keboola Flows', self.FLOW_DOCUMENTATION_URL),
        ]

    def get_component_configuration_links(self, component_id: str, configuration_id: str, configuration_name: str) -> list[Link]:
        """Links for component config: detail, dashboard."""
        return [
            self._detail_link('Configuration', configuration_name, self.get_component_configuration_url(component_id, configuration_id)),
            self._dashboard_link('Component Configurations Dashboard', self.get_component_configurations_dashboard_url(component_id)),
        ]

    def get_job_links(self, job_id: str) -> list[Link]:
        """Links for job: detail, dashboard."""
        return [
            self._detail_link('Job', job_id, self.get_job_url(job_id)),
            self._dashboard_link('Jobs Dashboard', self.get_jobs_dashboard_url()),
        ]

    def get_bucket_links(self, bucket_id: str, bucket_name: str) -> list[Link]:
        """Links for bucket: detail, dashboard."""
        return [
            self._detail_link('Bucket', bucket_name, self.get_bucket_url(bucket_id)),
            self._dashboard_link('Buckets in the project', self.get_buckets_dashboard_url()),
        ]

    def get_table_links(self, bucket_id: str, table_name: str) -> list[Link]:
            """Links for table: detail."""
            return [
                self._detail_link('Table', table_name, self.get_table_url(bucket_id, table_name)),
            ]
