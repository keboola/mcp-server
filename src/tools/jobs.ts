import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients, createLinksManager } from '@/clients/keboola';
import type { Config } from '@/config';
import type { Link } from '@/links';
import { logger } from '@/logger';
import { registerTool } from '@/mcp/tool';

// Ported from tools/jobs.py.

const JOB_STATUS = [
  'waiting',
  'processing',
  'success',
  'error',
  'created',
  'warning',
  'terminating',
  'cancelled',
  'terminated',
] as const;

const SORT_BY = ['startTime', 'endTime', 'createdTime', 'durationSeconds', 'id'] as const;
const SORT_ORDER = ['asc', 'desc'] as const;
const LOG_EVENT_TYPES = ['info', 'warn', 'error', 'success'] as const;

type RawJob = Record<string, unknown>;

/** result/config_data must be an object: empty list / null become {} (port of validate_dict_fields). */
const asDict = (value: unknown): Record<string, unknown> => {
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  if (Array.isArray(value) && value.length > 0) {
    throw new Error(
      `Field "result" or "config_data" cannot be a list, expecting dictionary, got: ${JSON.stringify(value)}.`,
    );
  }
  return {};
};

const toJobListItem = (raw: RawJob) => ({
  id: String(raw.id ?? ''),
  status: raw.status,
  componentId: raw.component ?? raw.componentId ?? null,
  configId: raw.config ?? raw.configId ?? null,
  isFinished: raw.isFinished ?? false,
  createdTime: raw.createdTime ?? null,
  startTime: raw.startTime ?? null,
  endTime: raw.endTime ?? null,
  durationSeconds: raw.durationSeconds ?? null,
});

const toJobDetail = (raw: RawJob, links: Link[]) => ({
  ...toJobListItem(raw),
  url: raw.url ?? '',
  configData: raw.configData != null ? asDict(raw.configData) : null,
  configRow: raw.configRow ?? null,
  runId: raw.runId ?? null,
  result: raw.result != null ? asDict(raw.result) : null,
  links,
  logs: null as { message: unknown; type: unknown; created: unknown }[] | null,
});

export const registerJobTools = (server: McpServer, config: Config): void => {
  registerTool(server, {
    name: 'get_jobs',
    title: 'Get jobs',
    description: 'Retrieves job execution information from the Keboola project.',
    annotations: { readOnlyHint: true },
    inputSchema: {
      job_ids: z
        .array(z.string())
        .default([])
        .describe('IDs of jobs to retrieve full details for; empty lists jobs as summaries.'),
      status: z
        .enum(JOB_STATUS)
        .optional()
        .describe('Filter listed jobs by status (ignored if job_ids given).'),
      component_id: z
        .string()
        .optional()
        .describe('Filter listed jobs by component id (ignored if job_ids given).'),
      config_id: z
        .string()
        .optional()
        .describe('Filter listed jobs by configuration id (ignored if job_ids given).'),
      limit: z
        .number()
        .int()
        .min(1)
        .max(500)
        .default(100)
        .describe('Number of jobs to list (max 500).'),
      offset: z.number().int().min(0).default(0).describe('Offset of jobs to list.'),
      sort_by: z.enum(SORT_BY).default('startTime').describe('Field to sort listed jobs by.'),
      sort_order: z.enum(SORT_ORDER).default('desc').describe('Sort order for listed jobs.'),
      include_logs: z
        .boolean()
        .default(false)
        .describe('Include execution logs (only when job_ids given).'),
      log_tail_lines: z
        .number()
        .int()
        .min(1)
        .max(500)
        .default(50)
        .describe('Max log events per job (most recent).'),
      log_event_types: z
        .array(z.enum(LOG_EVENT_TYPES))
        .optional()
        .describe('Filter log events by type (only when include_logs=true).'),
    },
    handler: async (args) => {
      const clients = createKeboolaClients(config);
      const linksManager = await createLinksManager(config, clients);

      // MODE 1: full details for specific job ids.
      if (args.job_ids.length > 0) {
        const jobs = await Promise.all(
          args.job_ids.map(async (jobId) => {
            const raw = (await clients.queue.getJob(jobId)) as RawJob;
            return toJobDetail(raw, linksManager.getJobLinks(jobId));
          }),
        );

        if (args.include_logs) {
          await Promise.all(
            jobs.map(async (job) => {
              if (!job.id) return;
              const raw = (await clients.storage.events.getEvents({
                runId: job.id,
                limit: args.log_tail_lines,
                offset: 0,
                forceUuid: 'true',
              } as never)) as RawJob[];
              const typeSet = args.log_event_types ? new Set<string>(args.log_event_types) : null;
              const events = (Array.isArray(raw) ? raw : [])
                .filter((event) => !typeSet || typeSet.has(event.type as string))
                .reverse();
              job.logs = events.map((event) => ({
                message: event.message,
                type: event.type,
                created: event.created,
              }));
            }),
          );
        }

        logger.info(`Retrieved full details for ${jobs.length} jobs.`);
        return { jobs };
      }

      // MODE 2: list summaries with optional filtering. Queue uses the raw branch id
      // (omitted on production) — not the storage `default` alias.
      const query: Record<string, unknown> = {
        branchId: config.branchId,
        componentId: args.component_id,
        configId: args.config_id,
        status: args.status ? [args.status] : undefined,
        limit: args.limit,
        offset: args.offset,
        sortBy: args.sort_by,
        sortOrder: args.sort_order,
      };
      for (const key of Object.keys(query)) {
        if (query[key] === undefined) delete query[key];
      }

      const raw = (await clients.queue.searchJobs(query as never)) as unknown;
      const items = Array.isArray(raw) ? raw : ((raw as { jobs?: RawJob[] }).jobs ?? []);
      logger.info(`Found ${items.length} jobs.`);
      return {
        jobs: (items as RawJob[]).map(toJobListItem),
        links: [linksManager.getJobsDashboardLink()],
      };
    },
  });

  registerTool(server, {
    name: 'run_job',
    title: 'Run job',
    description: 'Starts a new job for a given component or transformation.',
    annotations: { destructiveHint: true },
    inputSchema: {
      component_id: z
        .string()
        .describe('The ID of the component or transformation to start a job for.'),
      configuration_id: z.string().describe('The ID of the configuration to start a job for.'),
      configuration_row_ids: z
        .array(z.string())
        .optional()
        .describe('Optional configuration row IDs to run; if omitted, all rows are executed.'),
    },
    handler: async (args) => {
      const clients = createKeboolaClients(config);

      const payload: Record<string, unknown> = {
        component: args.component_id,
        config: args.configuration_id,
        mode: 'run',
      };
      if (config.branchId) payload.branchId = config.branchId;
      if (args.configuration_row_ids?.length) payload.configRowIds = args.configuration_row_ids;

      const raw = await clients.rawQueue.post<RawJob>('jobs', { body: payload });
      const linksManager = await createLinksManager(config, clients);
      const job = toJobDetail(raw, linksManager.getJobLinks(String(raw.id ?? '')));
      logger.info(
        `Started a new job with id: ${job.id} for component ${args.component_id} and configuration ${args.configuration_id}.`,
      );
      return job;
    },
  });
};
