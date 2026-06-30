import { describe, expect, it } from 'vitest';

import { callToolText, connectMcp } from '../helpers/mcp';
import { seedProject } from '../helpers/seed';
import { getTestProjectForTest } from '../testproject/fixture';

// Ported from integtests/tools/test_jobs.py. Each case leases a fresh project, seeds the
// standard fixtures (which include an ex-generic-v2 config), then runs a job against that
// config and polls get_jobs. Real jobs against the live stack take ~10-40s, so the suite is
// intentionally slow.

const RUN_TIMEOUT = 120_000;

/**
 * Polls get_jobs (listing mode, filtered by component + config) until the just-started job
 * id shows up, mirroring the Python `_wait_for_job_in_list` retry helper (the queue is
 * eventually-consistent right after a job is created).
 */
const waitForJobInList = async (
  client: Parameters<typeof callToolText>[0],
  jobId: string,
  componentId: string,
  configId: string,
  maxRetries = 15,
  delayMs = 1000,
): Promise<string> => {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    const text = await callToolText(client, 'get_jobs', {
      component_id: componentId,
      config_id: configId,
      limit: 10,
      sort_by: 'startTime',
      sort_order: 'desc',
    });
    if (text.includes(jobId)) return text;
    if (attempt < maxRetries - 1) await new Promise((r) => setTimeout(r, delayMs));
  }
  throw new Error(`Job ${jobId} not found in job list after ${maxRetries} attempts`);
};

/** Extracts the started job id from the run_job TOON output (top-level `id: <digits>`). */
const extractJobId = (runJobText: string): string => {
  // TOON quotes numeric-looking string scalars, so the id may be `id: "123"` or `id: 123`.
  const match = runJobText.match(/\bid:\s*"?(\d+)"?/);
  expect(match, `run_job output should contain a job id. Got: ${runJobText}`).not.toBeNull();
  return match![1]!;
};

describe('jobs tools (integration)', () => {
  it(
    'run_job starts a job and get_jobs lists it under the component/config filter',
    async () => {
      const project = await getTestProjectForTest();
      const seeded = await seedProject(project);
      const config = seeded.configs.find((c) => c.componentId === 'ex-generic-v2')!;
      const session = await connectMcp(project.config);
      try {
        const runText = await callToolText(session.client, 'run_job', {
          component_id: config.componentId,
          configuration_id: config.configurationId,
        });
        expect(runText).toContain(config.componentId);
        expect(runText).toContain(config.configurationId);
        const jobId = extractJobId(runText);

        const listText = await waitForJobInList(
          session.client,
          jobId,
          config.componentId,
          config.configurationId,
        );
        // Every listed job under this filter must belong to the same component + config.
        expect(listText).toContain(config.componentId);
        expect(listText).toContain(config.configurationId);
      } finally {
        await session.close();
      }
    },
    RUN_TIMEOUT,
  );

  it(
    'run_job then get_jobs(job_ids) returns the job detail with status, url and links',
    async () => {
      const project = await getTestProjectForTest();
      const seeded = await seedProject(project);
      const config = seeded.configs.find((c) => c.componentId === 'ex-generic-v2')!;
      const session = await connectMcp(project.config);
      try {
        const runText = await callToolText(session.client, 'run_job', {
          component_id: config.componentId,
          configuration_id: config.configurationId,
        });
        const jobId = extractJobId(runText);
        // The started-job response carries the component/config + UI links.
        expect(runText).toContain(config.componentId);
        expect(runText).toContain(config.configurationId);
        expect(runText).toMatch(/queue\/\d+/);
        expect(runText).toContain(`/queue/${jobId}`);

        const detailText = await callToolText(session.client, 'get_jobs', { job_ids: [jobId] });
        expect(detailText).toContain(jobId);
        expect(detailText).toContain(config.componentId);
        expect(detailText).toContain(config.configurationId);
        // Detail includes a status, a url and the ui-detail / ui-dashboard links.
        expect(detailText).toMatch(/status/i);
        expect(detailText).toContain(`/queue/${jobId}`);
        expect(detailText).toContain('ui-detail');
        expect(detailText).toContain('ui-dashboard');
      } finally {
        await session.close();
      }
    },
    RUN_TIMEOUT,
  );

  it(
    'get_jobs(job_ids, include_logs) returns the job detail and a logs section',
    async () => {
      const project = await getTestProjectForTest();
      const seeded = await seedProject(project);
      const config = seeded.configs.find((c) => c.componentId === 'ex-generic-v2')!;
      const session = await connectMcp(project.config);
      try {
        const runText = await callToolText(session.client, 'run_job', {
          component_id: config.componentId,
          configuration_id: config.configurationId,
        });
        const jobId = extractJobId(runText);

        const detailText = await callToolText(session.client, 'get_jobs', {
          job_ids: [jobId],
          include_logs: true,
        });
        expect(detailText).toContain(jobId);
        expect(detailText).toMatch(/logs/i);
      } finally {
        await session.close();
      }
    },
    RUN_TIMEOUT,
  );

  it(
    'run_job works against a freshly created config',
    async () => {
      const project = await getTestProjectForTest();
      const session = await connectMcp(project.config);
      try {
        const componentId = 'ex-generic-v2';
        const createText = await callToolText(session.client, 'create_config', {
          name: 'Test Config for Job Run',
          description: 'Test configuration created for job run test',
          component_id: componentId,
          parameters: { api: { baseUrl: 'https://wttr.in' } },
          storage: {},
        });
        const cfgMatch = createText.match(/configuration_id:\s*"?([^\s"]+)"?/);
        expect(cfgMatch, `create_config should return a configuration id. Got: ${createText}`).not.toBeNull();
        const configurationId = cfgMatch![1]!;

        const runText = await callToolText(session.client, 'run_job', {
          component_id: componentId,
          configuration_id: configurationId,
        });
        const jobId = extractJobId(runText);
        expect(runText).toContain(componentId);
        expect(runText).toContain(configurationId);

        const detailText = await callToolText(session.client, 'get_jobs', { job_ids: [jobId] });
        expect(detailText).toContain(jobId);
        expect(detailText).toContain(componentId);
        expect(detailText).toContain(configurationId);
      } finally {
        await session.close();
      }
    },
    RUN_TIMEOUT,
  );
});
