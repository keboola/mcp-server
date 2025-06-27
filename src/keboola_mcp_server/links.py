from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from keboola_mcp_server.client import KeboolaClient

URLType = Literal['ui-detail', 'ui-dashboard', 'docs']


class Link(BaseModel):
    model_config = ConfigDict(frozen=True)

    type: URLType = Field(..., description='The type of the URL.')
    title: str = Field(..., description='The name of the URL.')
    url: str = Field(..., description='The URL.')

    @classmethod
    def detail(cls, title: str, url: str) -> 'Link':
        return cls(type='ui-detail', title=title, url=url)

    @classmethod
    def dashboard(cls, title: str, url: str) -> 'Link':
        return cls(type='ui-dashboard', title=title, url=url)

    @classmethod
    def docs(cls, title: str, url: str) -> 'Link':
        return cls(type='docs', title=title, url=url)


class ProjectLinksManager:
    FLOW_DOCUMENTATION_URL = 'https://help.keboola.com/flows/'

    def __init__(self, base_url: str, project_id: str):
        self._base_url = base_url
        self._project_id = project_id

    @classmethod
    async def from_client(cls, client: KeboolaClient) -> 'ProjectLinksManager':
        base_url = client.storage_client.base_api_url
        project_id = await client.storage_client.project_id()
        return cls(base_url=base_url, project_id=project_id)

    def _url(self, path: str) -> str:
        return f'{self._base_url}/admin/projects/{self._project_id}/{path}'

    # --- Project ---
    def get_project_detail_link(self) -> Link:
        return Link.detail(title='Project Dashboard', url=self._url(''))

    def get_project_links(self) -> list[Link]:
        return [self.get_project_detail_link()]

    # --- Flows ---
    def get_flow_detail_link(self, flow_id: str | int, flow_name: str) -> Link:
        return Link.detail(title=f'Flow: {flow_name}', url=self._url(f'flows/{flow_id}'))

    def get_flows_dashboard_link(self) -> Link:
        return Link.dashboard(title='Flows in the project', url=self._url('flows'))

    def get_flows_docs_link(self) -> Link:
        return Link.docs(title='Documentation for Keboola Flows', url=self.FLOW_DOCUMENTATION_URL)

    def get_flow_links(self, flow_id: str | int, flow_name: str) -> list[Link]:
        return [
            self.get_flow_detail_link(flow_id, flow_name),
            self.get_flows_dashboard_link(),
            self.get_flows_docs_link(),
        ]

    # --- Components ---
    def get_component_config_link(self, component_id: str, configuration_id: str, configuration_name: str) -> Link:
        return Link.detail(
            title=f'Configuration: {configuration_name}', url=self._url(f'components/{component_id}/{configuration_id}')
        )

    def get_config_dashboard_link(self, component_id: str, component_name: str) -> Link:
        return Link.dashboard(
            title=f'{component_name} Configurations Dashboard', url=self._url(f'components/{component_id}')
        )

    def get_used_components_link(
        self
    ) -> Link:
        return Link.dashboard(
            title='Used Components Dashboard', url=self._url('components/configurations')
        )

    def get_configuration_links(
        self, component_id: str, configuration_id: str, configuration_name: str
    ) -> list[Link]:
        return [
            self.get_component_config_link(
                component_id=component_id, configuration_id=configuration_id, configuration_name=configuration_name
            ),
            self.get_config_dashboard_link(component_id=component_id, component_name=component_id),
        ]

    # --- Transformations ---
    def get_transformations_dashboard_link(self) -> Link:
        return Link.dashboard(
            title='Transformations dashboard', url=self._url('transformations-v2')
        )

    # --- Jobs ---
    def get_job_detail_link(self, job_id: str) -> Link:
        return Link.detail(title=f'Job: {job_id}', url=self._url(f'queue/{job_id}'))

    def get_jobs_dashboard_link(self) -> Link:
        return Link.dashboard(title='Jobs in the project', url=self._url('queue'))

    def get_job_links(self, job_id: str) -> list[Link]:
        return [self.get_job_detail_link(job_id), self.get_jobs_dashboard_link()]

    # --- Buckets ---
    def get_bucket_detail_link(self, bucket_id: str, bucket_name: str) -> Link:
        return Link.detail(title=f'Bucket: {bucket_name}', url=self._url(f'storage/{bucket_id}'))

    def get_bucket_dashboard_link(self) -> Link:
        return Link.dashboard(title='Buckets in the project', url=self._url('storage'))

    def get_bucket_links(self, bucket_id: str, bucket_name: str) -> list[Link]:
        return [
            self.get_bucket_detail_link(bucket_id, bucket_name),
            self.get_bucket_dashboard_link(),
        ]

    # --- Tables ---
    def get_table_detail_link(self, bucket_id: str, table_name: str) -> Link:
        return Link.detail(title=f'Table: {table_name}', url=self._url(f'storage/{bucket_id}/table/{table_name}'))

    def get_table_links(self, bucket_id: str, table_name: str) -> list[Link]:
        return [
            self.get_table_detail_link(bucket_id, table_name),
            self.get_bucket_detail_link(bucket_id=bucket_id, bucket_name=bucket_id),
        ]
