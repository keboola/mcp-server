import type { KeboolaClients } from '@/clients/keboola';
import { createRawClient, type RawClient } from '@/clients/raw';
import { deriveServiceUrls } from '@/clients/urls';
import type { Config } from '@/config';
import { logger } from '@/logger';
import type { ScheduleRequest } from './model';
import {
  configurationCreate,
  configurationDelete,
  configurationDetail,
  configurationUpdate,
  setCfgCreationMetadata,
  setCfgUpdateMetadata,
} from './utils';

// Ported from tools/flow/{scheduler,scheduler_model}.py and clients/scheduler.py.
// The scheduler service has no @keboola/api-client subpath, so it is built locally as a
// raw client (see createSchedulerClient below). Scheduler *configurations* are still
// stored as component configs via the typed Storage client (configurationCreate/etc.).

// =============================================================================
// SCHEDULER CLIENT (local raw client; no api-client subpath exists)
// =============================================================================

export type ScheduleApiResponse = {
  id: string;
  configurationId?: string;
  configuration_id?: string;
  schedule: { cronTab?: string; cron_tab?: string; timezone: string; state: string };
  target?: Record<string, unknown>;
  executions?: {
    jobId?: string;
    job_id?: string;
    executionTime?: string;
    execution_time?: string;
  }[];
};

export type SchedulerClient = {
  activateSchedule: (scheduleConfigId: string) => Promise<ScheduleApiResponse>;
  listSchedulesByConfigId: (
    componentId: string,
    configurationId: string,
  ) => Promise<ScheduleApiResponse[]>;
  deleteSchedule: (scheduleConfigId: string) => Promise<void>;
};

/** Builds a Scheduler API client against `deriveServiceUrls(...).scheduler`. */
export const createSchedulerClient = (config: Config): SchedulerClient => {
  const urls = deriveServiceUrls(config.storageApiUrl ?? '');
  const raw: RawClient = createRawClient({
    baseUrl: urls.scheduler,
    token: config.bearerToken ? `Bearer ${config.bearerToken}` : config.storageToken,
  });
  return {
    activateSchedule: (scheduleConfigId) =>
      raw.post<ScheduleApiResponse>('schedules', { body: { configurationId: scheduleConfigId } }),
    listSchedulesByConfigId: (componentId, configurationId) =>
      raw.get<ScheduleApiResponse[]>('schedules', {
        params: { componentId, configurationId },
      }),
    deleteSchedule: async (scheduleConfigId) => {
      await raw.delete(`configurations/${scheduleConfigId}`);
    },
  };
};

// =============================================================================
// SCHEDULER MODELS + LOGIC (port of scheduler.py + scheduler_model.py)
// =============================================================================

export const toScheduleDetail = (api: ScheduleApiResponse) => ({
  scheduleId: api.configurationId ?? api.configuration_id ?? '',
  timezone: api.schedule.timezone,
  state: api.schedule.state,
  cronTab: api.schedule.cronTab ?? api.schedule.cron_tab ?? '',
  target_executions: (api.executions ?? []).map((exec) => ({
    jobId: exec.jobId ?? exec.job_id ?? null,
    executionTime: exec.executionTime ?? exec.execution_time ?? null,
  })),
});

const SCHEDULER_COMPONENT_ID = 'keboola.scheduler';

const CRON_TAB_INSTRUCTIONS = `
Cron Tab Expression should be in the format: \`* * * * *\`.
Field order:
1. Minute (0-59)
2. Hour (0-23)
3. Day of month (1-31, or L for last day of month)
4. Month (1-12)
5. Day of week (0-6, where 0 = Sunday)

Examples:
1. schedule daily at 1:00 PM and 1:00 AM would be \`0 1,13 * * *\`
2. schedule weekly on Monday at 9:00 AM would be \`0 9 * * 1\`
3. schedule monthly on the 1st and 20th day of the month at 10:00 AM would be \`0 10 1,20 * *\`
4. schedule yearly on the 1st of january and august at 11:00 AM would be \`0 11 1 1,8 *\`
5. schedule hourly every 15 minutes would be \`0,15,30,45 * * * *\`
6. schedule monthly on the last day of the month at 10:00 AM would be \`0 10 L * *\`
`;

/** Port of scheduler.validate_cron_tab. */
export const validateCronTab = (cronTab: string | null | undefined): void => {
  if (cronTab === null || cronTab === undefined) return;
  try {
    const parts = cronTab.trim().split(/\s+/);
    if (parts.length !== 5) {
      throw new Error(
        `Cron expression must have exactly 5 parts got: ${cronTab} which has ${parts.length} parts.`,
      );
    }
    const toIntList = (field: string, allowL = false): { parts: number[]; hasL: boolean } => {
      if (field === '*') return { parts: [], hasL: false };
      let hasL = false;
      const nums: number[] = [];
      for (let x of field.split(',')) {
        x = x.trim();
        if (allowL && x.toUpperCase() === 'L') {
          hasL = true;
        } else if (/^-?\d+$/.test(x)) {
          nums.push(Number(x));
        } else {
          throw new Error(`Cron expression must have only digits got: ${field} in "${cronTab}".`);
        }
      }
      if (allowL && hasL && nums.length > 0) {
        throw new Error('Day of month must use either `L` or numeric values, not both.');
      }
      return { parts: nums, hasL };
    };

    const { parts: minutes } = toIntList(parts[0]!.trim());
    const { parts: hours } = toIntList(parts[1]!.trim());
    const { parts: days, hasL: hasLastDay } = toIntList(parts[2]!.trim(), true);
    const { parts: months } = toIntList(parts[3]!.trim());
    const { parts: weekdays } = toIntList(parts[4]!.trim());

    if (minutes.some((x) => x < 0 || x > 59)) {
      throw new Error(`Minutes of hour \`M _ _ _ _\` must be between 0 and 59, got: ${parts[0]}`);
    }
    if (hours.some((x) => x < 0 || x > 23)) {
      throw new Error(`Hours of day \`_ H _ _ _\` must be between 0 and 23, got: ${parts[1]}`);
    }
    if (days.some((x) => x < 1 || x > 31)) {
      throw new Error(`Days of month \`_ _ D _ _\`must be between 1 and 31, got: ${parts[2]}`);
    }
    if (months.some((x) => x < 1 || x > 12)) {
      throw new Error(`Months of year \`_ _ _ M _\` must be between 1 and 12, got: ${parts[3]}`);
    }
    if (weekdays.some((x) => x < 0 || x > 6)) {
      throw new Error(
        `Days of week \`_ _ _ _ W\` must be between 0=Sunday and 6=Saturday, got: ${parts[4]}`,
      );
    }
    if (months.length > 0 && days.length === 0 && !hasLastDay) {
      throw new Error(
        'Months of year must be specified with days of month. Example: `35 12 31 1,3 *`',
      );
    }
    if ((days.length > 0 || hasLastDay) && hours.length === 0) {
      throw new Error('Days of month must be specified with hours of day. Example: `55 12 31 * *`');
    }
    if (hours.length > 0 && minutes.length === 0) {
      throw new Error(
        'Hours of day must be specified with minutes of hour. Example: `55 12 * * *`',
      );
    }
    if (weekdays.length > 0 && hours.length === 0) {
      throw new Error('Days of week must be specified with hours of day. Example: `55 12 * * 0`');
    }
    if (weekdays.length > 0 && (days.length > 0 || months.length > 0 || hasLastDay)) {
      throw new Error('Days of week must not be specified with days of month nor months of year.');
    }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    throw new Error(`Invalid cron tab expression: ${msg}.\n${CRON_TAB_INSTRUCTIONS}`);
  }
};

type SimplifiedSchedule = {
  scheduleId: string | null;
  cronTab: string;
  timezone: string;
  state: string;
};

export const listSchedulesForConfig = async (
  scheduler: SchedulerClient,
  componentId: string,
  configurationId: string,
): Promise<ReturnType<typeof toScheduleDetail>[]> => {
  const apiSchedules = await scheduler.listSchedulesByConfigId(componentId, configurationId);
  return apiSchedules.map(toScheduleDetail);
};

/** Compute original/updated/new schedulers (port of scheduler._update_schedulers_internal). */
const updateSchedulersInternal = async (
  scheduler: SchedulerClient,
  configurationId: string,
  componentId: string,
  schedules: ScheduleRequest[],
): Promise<{
  original: Map<string, SimplifiedSchedule>;
  updated: Map<string, SimplifiedSchedule | null>;
  added: SimplifiedSchedule[];
}> => {
  const current = await listSchedulesForConfig(scheduler, componentId, configurationId);
  const original = new Map<string, SimplifiedSchedule>();
  for (const s of current) {
    original.set(s.scheduleId, {
      scheduleId: s.scheduleId,
      cronTab: s.cronTab,
      timezone: s.timezone,
      state: s.state,
    });
  }
  const added: SimplifiedSchedule[] = [];
  const updated = new Map<string, SimplifiedSchedule | null>();

  for (const request of schedules) {
    if (request.action === 'add') {
      if (request.cron_tab == null) {
        throw new Error('cron_tab is required to add a schedule.');
      }
      validateCronTab(request.cron_tab);
      added.push({
        scheduleId: request.schedule_id ?? null,
        cronTab: request.cron_tab,
        timezone: request.timezone ?? 'UTC',
        state: request.state ?? 'enabled',
      });
    } else if (request.action === 'update') {
      const id = request.schedule_id ?? '';
      const existing = original.get(id);
      if (!existing) {
        throw new Error(
          `Schedule (ID: ${request.schedule_id}) cannot be updated because it was not found in the existing schedulers.`,
        );
      }
      if (request.cron_tab != null) validateCronTab(request.cron_tab);
      updated.set(id, {
        scheduleId: existing.scheduleId,
        cronTab: request.cron_tab ?? existing.cronTab,
        timezone: request.timezone ?? existing.timezone,
        state: request.state ?? existing.state,
      });
    } else if (request.action === 'remove') {
      const id = request.schedule_id ?? '';
      if (!original.has(id)) {
        throw new Error(
          `Schedule (ID: ${request.schedule_id}) cannot be removed because it was not found in the existing schedulers.`,
        );
      }
      updated.set(id, null);
    } else {
      throw new Error(`Invalid action for schedulers: ${(request as { action: string }).action}.`);
    }
  }
  return { original, updated, added };
};

const createSchedule = async (
  clients: KeboolaClients,
  scheduler: SchedulerClient,
  targetComponentId: string,
  targetConfigurationId: string,
  cronTab: string,
  timezone: string,
  state: string,
): Promise<ReturnType<typeof toScheduleDetail>> => {
  const scheduleName = `Schedule for ${targetConfigurationId}`;
  const schedulerConfig = {
    schedule: { cronTab, timezone, state },
    target: { componentId: targetComponentId, configurationId: targetConfigurationId, mode: 'run' },
  };
  const storageResponse = await configurationCreate(
    clients,
    SCHEDULER_COMPONENT_ID,
    scheduleName,
    `Automated schedule for ${targetConfigurationId}`,
    schedulerConfig,
  );
  const scheduleConfigId = String(storageResponse.id ?? '');
  logger.info(`Created schedule configuration in Storage API: ${scheduleConfigId}`);
  const scheduleResponse = await scheduler.activateSchedule(scheduleConfigId);
  logger.info(`Activated schedule in Scheduler API: ${scheduleResponse.id}`);
  await setCfgCreationMetadata(clients, SCHEDULER_COMPONENT_ID, scheduleConfigId);
  return toScheduleDetail(scheduleResponse);
};

const updateSchedule = async (
  clients: KeboolaClients,
  scheduler: SchedulerClient,
  scheduleConfigId: string,
  cronTab: string | null,
  timezone: string | null,
  state: string | null,
): Promise<void> => {
  const currentConfig = await configurationDetail(
    clients,
    SCHEDULER_COMPONENT_ID,
    scheduleConfigId,
  );
  const schedulerConfig = (currentConfig.configuration as Record<string, unknown>) ?? {};
  const schedule = (schedulerConfig.schedule as Record<string, unknown>) ?? {};
  if (cronTab !== null) schedule.cronTab = cronTab;
  if (timezone !== null) schedule.timezone = timezone;
  if (state !== null) schedule.state = state;
  schedulerConfig.schedule = schedule;

  const updated = await configurationUpdate(
    clients,
    SCHEDULER_COMPONENT_ID,
    scheduleConfigId,
    schedulerConfig,
    'Schedule Updated',
  );
  logger.info(`Updated schedule configuration in Storage API: ${scheduleConfigId}`);
  await scheduler.activateSchedule(scheduleConfigId);
  await setCfgUpdateMetadata(
    clients,
    SCHEDULER_COMPONENT_ID,
    scheduleConfigId,
    Number(updated.version ?? 0),
  );
};

const removeSchedule = async (
  clients: KeboolaClients,
  scheduler: SchedulerClient,
  scheduleConfigId: string,
): Promise<void> => {
  await scheduler.deleteSchedule(scheduleConfigId);
  await configurationDelete(clients, SCHEDULER_COMPONENT_ID, scheduleConfigId);
};

/** Port of scheduler.process_schedule_request. */
export const processScheduleRequest = async (
  clients: KeboolaClients,
  scheduler: SchedulerClient,
  targetComponentId: string,
  targetConfigurationId: string,
  requests: ScheduleRequest[],
): Promise<string[]> => {
  const { updated, added } = await updateSchedulersInternal(
    scheduler,
    targetConfigurationId,
    targetComponentId,
    requests,
  );
  const responses: string[] = [];
  try {
    for (const [scheduleId, schedule] of updated) {
      if (schedule === null) {
        await removeSchedule(clients, scheduler, scheduleId);
        responses.push(`Removed schedule: ${scheduleId}`);
      } else {
        await updateSchedule(
          clients,
          scheduler,
          scheduleId,
          schedule.cronTab,
          schedule.timezone,
          schedule.state,
        );
        responses.push(`Updated schedule: ${scheduleId}`);
      }
    }
    for (const newScheduler of added) {
      const response = await createSchedule(
        clients,
        scheduler,
        targetComponentId,
        targetConfigurationId,
        newScheduler.cronTab,
        newScheduler.timezone,
        newScheduler.state,
      );
      responses.push(`Created schedule: ${response.scheduleId}`);
    }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    throw new Error(`Error processing schedule requests: ${msg}`);
  }
  return responses;
};
