import {
  CONDITIONAL_FLOW_COMPONENT_ID,
  DATA_APP_COMPONENT_ID,
  FLOW_TYPES,
  type FlowType,
} from '@/constants';

/**
 * UI / docs links surfaced to the user alongside tool results. Faithful port of
 * the Python `links.ProjectLinksManager` — pure URL building from the project's
 * base URL, project id, and (optional) dev-branch id.
 */
export type UrlType = 'ui-detail' | 'ui-dashboard' | 'docs';

export type Link = {
  type: UrlType;
  title: string;
  url: string;
};

const detail = (title: string, url: string): Link => ({ type: 'ui-detail', title, url });
const dashboard = (title: string, url: string): Link => ({ type: 'ui-dashboard', title, url });
const docs = (title: string, url: string): Link => ({ type: 'docs', title, url });

const FLOW_DOCUMENTATION_URL = 'https://help.keboola.com/flows/';

const isFlowType = (componentId: string | undefined): componentId is FlowType =>
  componentId !== undefined && (FLOW_TYPES as readonly string[]).includes(componentId);

const isDataAppComponent = (componentId: string | undefined): boolean =>
  componentId === DATA_APP_COMPONENT_ID;

const isTransformationComponent = (componentId: string): boolean =>
  Boolean(componentId) && componentId.includes('transformation');

export class ProjectLinksManager {
  private readonly baseUrl: string;
  private readonly projectId: string;
  private readonly branchId: string | undefined;

  constructor(options: { baseUrl: string; projectId: string; branchId?: string }) {
    this.baseUrl = options.baseUrl;
    this.projectId = options.projectId;
    this.branchId = options.branchId;
  }

  private url(path: string): string {
    const parts = [this.baseUrl, 'admin/projects', this.projectId];
    if (this.branchId) {
      parts.push('branch', this.branchId);
    }
    parts.push(path);
    return parts.join('/');
  }

  private flowPath(flowType: FlowType): string {
    return flowType === CONDITIONAL_FLOW_COMPONENT_ID ? 'flows-v2' : 'flows';
  }

  /** Most relevant links for a Keboola object from mutually-exclusive identifiers. */
  getLinks(opts: {
    bucketId?: string;
    tableId?: string;
    componentId?: string;
    configurationId?: string;
    name?: string;
  }): Link[] {
    const { bucketId, tableId, componentId, configurationId, name } = opts;
    if (componentId && configurationId) {
      return [this.getComponentConfigLink(componentId, configurationId, name ?? '')];
    }
    if (componentId) {
      return [this.getConfigDashboardLink(componentId, name ?? '')];
    }
    if (tableId) {
      return [this.getTableDetailLinkFromTableId(tableId)];
    }
    if (bucketId) {
      return [this.getBucketDetailLink(bucketId, name ?? bucketId)];
    }
    return [];
  }

  // --- Project ---
  getProjectDetailLink(): Link {
    return detail('Project Dashboard', this.url(''));
  }

  getProjectLinks(): Link[] {
    return [this.getProjectDetailLink()];
  }

  // --- Flows ---
  getFlowDetailLink(flowId: string | number, flowName: string, flowType: FlowType): Link {
    return detail(`Flow: ${flowName}`, this.url(`${this.flowPath(flowType)}/${flowId}`));
  }

  getFlowsDashboardLink(flowType: FlowType): Link {
    const label = flowType === CONDITIONAL_FLOW_COMPONENT_ID ? 'Conditional Flows' : 'Flows';
    return dashboard(`${label} in the project`, this.url(this.flowPath(flowType)));
  }

  getFlowsDocsLink(): Link {
    return docs('Documentation for Keboola Flows', FLOW_DOCUMENTATION_URL);
  }

  getFlowLinks(flowId: string | number, flowName: string, flowType: FlowType): Link[] {
    return [
      this.getFlowDetailLink(flowId, flowName, flowType),
      this.getFlowsDashboardLink(flowType),
      this.getFlowsDocsLink(),
    ];
  }

  // --- Schedulers ---
  getSchedulerDetailLink(flowId: string | number, flowType: FlowType): Link {
    return detail('Schedules', this.url(`${this.flowPath(flowType)}/${flowId}/schedules`));
  }

  // --- Components ---
  getComponentConfigLink(
    componentId: string,
    configurationId: string,
    configurationName: string,
  ): Link {
    if (isTransformationComponent(componentId)) {
      return this.getTransformationConfigLink(componentId, configurationId, configurationName);
    }
    if (isDataAppComponent(componentId)) {
      return this.getDataAppConfigLink(configurationId, configurationName, false);
    }
    if (isFlowType(componentId)) {
      return this.getFlowDetailLink(configurationId, configurationName, componentId);
    }
    return detail(
      `Configuration: ${configurationName}`,
      this.url(`components/${componentId}/${configurationId}`),
    );
  }

  getConfigDashboardLink(componentId: string, componentName: string | undefined): Link {
    const label = componentName ? componentName : `Component "${componentId}"`;
    return dashboard(`${label} Configurations Dashboard`, this.url(`components/${componentId}`));
  }

  getUsedComponentsLink(): Link {
    return dashboard('Used Components Dashboard', this.url('components/configurations'));
  }

  getConfigurationLinks(
    componentId: string,
    configurationId: string,
    configurationName: string,
  ): Link[] {
    return [
      this.getComponentConfigLink(componentId, configurationId, configurationName),
      this.getConfigDashboardLink(componentId, undefined),
    ];
  }

  // --- Data Apps ---
  getDataAppConfigLink(
    configurationId: string,
    configurationName: string,
    usesBasicAuthentication: boolean,
  ): Link {
    const title = usesBasicAuthentication
      ? `Data App Configuration (To see password, click on "OPEN DATA APP"): ${configurationName}`
      : `Data App Configuration: ${configurationName}`;
    return detail(title, this.url(`data-apps/${configurationId}`));
  }

  getDataAppDashboardLink(): Link {
    return dashboard('Data Apps in the project', this.url('data-apps'));
  }

  getDataAppDeploymentLink(deploymentLink: string): Link {
    return detail('Data App Deployment', deploymentLink);
  }

  getDataAppLinks(
    configurationId: string,
    configurationName: string,
    deploymentLink?: string,
    usesBasicAuthentication = false,
  ): Link[] {
    const links = [
      this.getDataAppConfigLink(configurationId, configurationName, usesBasicAuthentication),
      this.getDataAppDashboardLink(),
    ];
    if (deploymentLink) {
      links.push(this.getDataAppDeploymentLink(deploymentLink));
    }
    return links;
  }

  // --- Transformations ---
  getTransformationsDashboardLink(): Link {
    return dashboard('Transformations dashboard', this.url('transformations-v2'));
  }

  getTransformationConfigLink(
    transformationType: string,
    transformationId: string,
    transformationName: string,
  ): Link {
    return detail(
      `Transformation: ${transformationName}`,
      this.url(`transformations-v2/${transformationType}/${transformationId}`),
    );
  }

  getTransformationLinks(
    transformationType: string,
    transformationId: string,
    transformationName: string,
  ): Link[] {
    return [
      this.getTransformationConfigLink(transformationType, transformationId, transformationName),
      this.getTransformationsDashboardLink(),
    ];
  }

  // --- Jobs ---
  getJobDetailLink(jobId: string): Link {
    return detail(`Job: ${jobId}`, this.url(`queue/${jobId}`));
  }

  getJobsDashboardLink(): Link {
    return dashboard('Jobs in the project', this.url('queue'));
  }

  getJobLinks(jobId: string): Link[] {
    return [this.getJobDetailLink(jobId), this.getJobsDashboardLink()];
  }

  // --- Buckets ---
  getBucketDetailLink(bucketId: string, bucketName: string): Link {
    return detail(`Bucket: ${bucketName}`, this.url(`storage/${bucketId}`));
  }

  getBucketDashboardLink(): Link {
    return dashboard('Buckets in the project', this.url('storage'));
  }

  getBucketLinks(bucketId: string, bucketName: string): Link[] {
    return [this.getBucketDetailLink(bucketId, bucketName), this.getBucketDashboardLink()];
  }

  // --- Tables ---
  getTableDetailLink(bucketId: string, tableName: string): Link {
    return detail(`Table: ${tableName}`, this.url(`storage/${bucketId}/table/${tableName}`));
  }

  getTableDetailLinkFromTableId(tableId: string): Link {
    const segments = tableId.split('.');
    const tableName = segments[segments.length - 1]!;
    const bucketId = segments.slice(0, -1).join('.');
    return this.getTableDetailLink(bucketId, tableName);
  }

  getTableLinks(bucketId: string, bucketName: string, tableName: string): Link[] {
    return [
      this.getTableDetailLink(bucketId, tableName),
      this.getBucketDetailLink(bucketId, bucketName),
    ];
  }
}
