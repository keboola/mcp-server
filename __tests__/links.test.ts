import { describe, expect, it } from 'vitest';

import { ProjectLinksManager } from '@/links';

const BASE = 'https://connection.keboola.com';

const manager = (branchId?: string) =>
  new ProjectLinksManager({ baseUrl: BASE, projectId: '1234', branchId });

describe('ProjectLinksManager', () => {
  it('builds production URLs without a branch segment', () => {
    expect(manager().getProjectDetailLink()).toEqual({
      type: 'ui-detail',
      title: 'Project Dashboard',
      url: `${BASE}/admin/projects/1234/`,
    });
  });

  it('inserts the branch segment on a development branch', () => {
    expect(manager('567').getBucketDetailLink('in.c-main', 'main').url).toBe(
      `${BASE}/admin/projects/1234/branch/567/storage/in.c-main`,
    );
  });

  it.each([
    ['keboola.flow', 'flows-v2'],
    ['keboola.orchestrator', 'flows'],
  ] as const)('routes %s flows to /%s', (flowType, path) => {
    expect(manager().getFlowDetailLink('99', 'My Flow', flowType).url).toBe(
      `${BASE}/admin/projects/1234/${path}/99`,
    );
  });

  it('routes transformation components to the transformations path via getComponentConfigLink', () => {
    const link = manager().getComponentConfigLink('keboola.snowflake-transformation', 'cfg1', 'T');
    expect(link.url).toBe(
      `${BASE}/admin/projects/1234/transformations-v2/keboola.snowflake-transformation/cfg1`,
    );
  });

  it('routes data-app components to the data-apps path', () => {
    const link = manager().getComponentConfigLink('keboola.data-apps', 'cfg1', 'App');
    expect(link.url).toBe(`${BASE}/admin/projects/1234/data-apps/cfg1`);
  });

  it('splits a fully-qualified table id into bucket + table', () => {
    expect(manager().getTableDetailLinkFromTableId('in.c-main.users').url).toBe(
      `${BASE}/admin/projects/1234/storage/in.c-main/table/users`,
    );
  });

  it('getLinks picks the config link when component + configuration are given', () => {
    const links = manager().getLinks({
      componentId: 'keboola.ex-aws-s3',
      configurationId: 'c1',
      name: 'My cfg',
    });
    expect(links).toHaveLength(1);
    expect(links[0]!.url).toBe(`${BASE}/admin/projects/1234/components/keboola.ex-aws-s3/c1`);
  });
});
