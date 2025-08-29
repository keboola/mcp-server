from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from keboola_mcp_server.clients.client import (
    CONDITIONAL_FLOW_COMPONENT_ID,
    FlowType,
    KeboolaClient,
)

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

    def __init__(self, *, base_url: str, project_id: str, branch_id: str | None):
        self._base_url = base_url
        self._project_id = project_id
        self._branch_id = branch_id

    @classmethod
    async def from_client(cls, client: KeboolaClient) -> 'ProjectLinksManager':
        project_id = await client.storage_client.project_id()
        return cls(base_url=client.storage_api_url, project_id=project_id, branch_id=client.branch_id)

    def _url(self, path: str) -> str:
        parts = [self._base_url, 'admin/projects', self._project_id]
        if self._branch_id:
            parts += ['branch', self._branch_id]
        parts.append(path)
        return '/'.join(parts)

    # --- Project ---
    def get_project_detail_link(self) -> Link:
        return Link.detail(title='Project Dashboard', url=self._url(''))

    def get_project_links(self) -> list[Link]:
        return [self.get_project_detail_link()]

    # --- Flows ---
    def get_flow_detail_link(self, flow_id: str | int, flow_name: str, flow_type: FlowType) -> Link:
        """Get detail link for a specific flow based on its type."""
        flow_path = 'flows-v2' if flow_type == CONDITIONAL_FLOW_COMPONENT_ID else 'flows'
        return Link.detail(title=f'Flow: {flow_name}', url=self._url(f'{flow_path}/{flow_id}'))

    def get_flows_dashboard_link(self, flow_type: FlowType) -> Link:
        """Get dashboard link for flows based on the flow type."""
        flow_path = 'flows-v2' if flow_type == CONDITIONAL_FLOW_COMPONENT_ID else 'flows'
        flow_label = 'Conditional Flows' if flow_type == CONDITIONAL_FLOW_COMPONENT_ID else 'Flows'
        return Link.dashboard(title=f'{flow_label} in the project', url=self._url(flow_path))

    def get_flows_docs_link(self) -> Link:
        return Link.docs(title='Documentation for Keboola Flows', url=self.FLOW_DOCUMENTATION_URL)

    def get_flow_links(self, flow_id: str | int, flow_name: str, flow_type: FlowType) -> list[Link]:
        """Get all relevant links for a flow based on its type."""
        return [
            self.get_flow_detail_link(flow_id, flow_name, flow_type),
            self.get_flows_dashboard_link(flow_type),
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

    def get_used_components_link(self) -> Link:
        return Link.dashboard(title='Used Components Dashboard', url=self._url('components/configurations'))

    def get_configuration_links(self, component_id: str, configuration_id: str, configuration_name: str) -> list[Link]:
        return [
            self.get_component_config_link(
                component_id=component_id, configuration_id=configuration_id, configuration_name=configuration_name
            ),
            self.get_config_dashboard_link(component_id=component_id, component_name=component_id),
        ]

    # --- Data Apps ---
    def get_data_app_config_link(self, configuration_id: str, configuration_name: str, is_authorized: bool) -> Link:
        title = (
            f'Data App Configuration (To see password, click on "OPEN DATA APP"): {configuration_name}'
            if is_authorized
            else f'Data App Configuration: {configuration_name}'
        )
        return Link.detail(title=title, url=self._url(f'data-apps/{configuration_id}'))

    def get_data_app_dashboard_link(self) -> Link:
        return Link.dashboard(title='Data Apps in the project', url=self._url('data-apps'))

    def get_data_app_deployment_link(self, deployment_link: str) -> Link:
        return Link.detail(title='Data App Deployment', url=deployment_link)

    def get_data_app_links(
        self,
        configuration_id: str,
        configuration_name: str,
        deployment_link: str | None = None,
        is_authorized: bool = False,
    ) -> list[Link]:
        links = [
            self.get_data_app_config_link(
                configuration_id=configuration_id,
                configuration_name=configuration_name,
                is_authorized=is_authorized,
            ),
            self.get_data_app_dashboard_link(),
        ]
        if deployment_link:
            links.append(self.get_data_app_deployment_link(deployment_link))
        return links

    # --- Transformations ---
    def get_transformations_dashboard_link(self) -> Link:
        return Link.dashboard(title='Transformations dashboard', url=self._url('transformations-v2'))

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

    def get_table_links(self, bucket_id: str, bucket_name: str, table_name: str) -> list[Link]:
        return [
            self.get_table_detail_link(bucket_id, table_name),
            self.get_bucket_detail_link(bucket_id=bucket_id, bucket_name=bucket_name),
        ]
