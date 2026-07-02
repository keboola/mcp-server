import { z } from 'zod';

import type { Link } from '@/links';

// Models ported from tools/data_apps.py + clients/data_science.py.
// snake_case output field names are preserved verbatim for 1:1 tool-output parity.

/** Authentication type accepted by the modify_* tools. */
export const AUTHENTICATION_TYPES = ['no-auth', 'basic-auth', 'default'] as const;
export type AuthenticationType = (typeof AUTHENTICATION_TYPES)[number];

// --- data-science API response models (port of clients/data_science.py) ---

/**
 * Raw data-science `apps/{id}` response. Validation aliases from the Python models
 * (camelCase + snake_case) are normalized via `parseDataAppResponse`.
 */
export type DataAppResponse = {
  id: string;
  project_id: string;
  component_id: string;
  branch_id: string | null;
  config_id: string;
  config_version: string;
  type: string;
  state: string;
  desired_state?: string;
  last_request_timestamp?: string | null;
  last_start_timestamp?: string | null;
  url?: string | null;
  auto_suspend_after_seconds?: number | null;
  size?: string | null;
};

const str = (value: unknown): string => (value == null ? '' : String(value));
const strOrNull = (value: unknown): string | null => (value == null ? null : String(value));

/** Picks the first present alias (camelCase preferred), mirroring AliasChoices. */
const pick = (raw: Record<string, unknown>, ...keys: string[]): unknown => {
  for (const key of keys) {
    if (raw[key] !== undefined && raw[key] !== null) return raw[key];
  }
  return undefined;
};

export const parseDataAppResponse = (raw: Record<string, unknown>): DataAppResponse => ({
  id: str(pick(raw, 'id', 'data_app_id')),
  project_id: str(pick(raw, 'projectId', 'project_id')),
  component_id: str(pick(raw, 'componentId', 'component_id')),
  branch_id: strOrNull(pick(raw, 'branchId', 'branch_id')),
  config_id: str(pick(raw, 'configId', 'config_id')),
  config_version: str(pick(raw, 'configVersion', 'config_version')),
  type: str(raw.type),
  state: str(raw.state),
  desired_state: raw.desiredState != null ? String(raw.desiredState) : undefined,
  last_request_timestamp: strOrNull(pick(raw, 'lastRequestTimestamp', 'last_request_timestamp')),
  last_start_timestamp: strOrNull(pick(raw, 'lastStartTimestamp', 'last_start_timestamp')),
  url: strOrNull(raw.url),
  auto_suspend_after_seconds:
    (pick(raw, 'autoSuspendAfterSeconds', 'auto_suspend_after_seconds') as number | undefined) ??
    null,
  size: strOrNull(raw.size),
});

export type CreatedGitCredentialResponse = {
  id: string;
  type: string;
  name: string;
  permissions: string;
  owner_admin_id: string | null;
  created_at: string | null;
  secret: string | null;
};

export const parseCredentialResponse = (
  raw: Record<string, unknown>,
): CreatedGitCredentialResponse => ({
  id: str(raw.id),
  type: str(raw.type),
  name: raw.name != null ? String(raw.name) : '',
  permissions: str(raw.permissions),
  owner_admin_id: strOrNull(pick(raw, 'ownerAdminId', 'owner_admin_id')),
  created_at: strOrNull(pick(raw, 'createdAt', 'created_at')),
  secret: raw.secret != null ? String(raw.secret) : null,
});

export type AppGitRepoResponse = {
  ssh_url: string | null;
  https_url: string | null;
  is_managed_git_repo: boolean;
};

export const parseAppGitRepoResponse = (raw: Record<string, unknown>): AppGitRepoResponse => ({
  ssh_url: strOrNull(pick(raw, 'sshUrl', 'ssh_url')),
  https_url: strOrNull(pick(raw, 'httpsUrl', 'https_url')),
  is_managed_git_repo: Boolean(pick(raw, 'isManagedGitRepo', 'is_managed_git_repo') ?? false),
});

export type AppRunResponse = {
  id: string;
  app_id: string | null;
  state: string;
  created_at: string | null;
  started_at: string | null;
  stopped_at: string | null;
  startup_logs: string | null;
  failure_reason: { reason: string | null; message: string | null } | null;
  mode: string | null;
};

export const parseAppRunResponse = (raw: Record<string, unknown>): AppRunResponse => {
  const failure = pick(raw, 'failureReason', 'failure_reason') as
    | Record<string, unknown>
    | undefined;
  return {
    id: str(raw.id),
    app_id: strOrNull(pick(raw, 'appId', 'app_id')),
    state: str(raw.state),
    created_at: strOrNull(pick(raw, 'createdAt', 'created_at')),
    started_at: strOrNull(pick(raw, 'startedAt', 'started_at')),
    stopped_at: strOrNull(pick(raw, 'stoppedAt', 'stopped_at')),
    startup_logs: strOrNull(pick(raw, 'startupLogs', 'startup_logs')),
    failure_reason: failure
      ? {
          reason: failure.reason != null ? String(failure.reason) : null,
          message: failure.message != null ? String(failure.message) : null,
        }
      : null,
    mode: raw.mode != null ? String(raw.mode) : null,
  };
};

// --- tool output models (port of the Pydantic BaseModels in data_apps.py) ---

export type AppRunInfo = {
  state: string;
  created_at: string | null;
  stopped_at: string | null;
  failure_reason: string | null;
  failure_message: string | null;
  startup_logs: string[];
};

export type DataAppSummary = {
  component_id: string;
  configuration_id: string;
  data_app_id: string;
  project_id: string;
  branch_id: string;
  config_version: string;
  state: string;
  type: string;
  deployment_url: string | null;
  auto_suspend_after_seconds: number | null;
  repo_url: string | null;
};

export type DeploymentInfo = {
  version: string;
  state: string;
  url: string | null;
  last_request_timestamp: string | null;
  last_start_timestamp: string | null;
  logs: string[];
  last_run: AppRunInfo | null;
};

export type DataApp = {
  name: string;
  description: string | null;
  component_id: string;
  configuration_id: string;
  data_app_id: string;
  project_id: string;
  branch_id: string;
  config_version: string;
  state: string;
  type: string;
  deployment_url: string | null;
  auto_suspend_after_seconds: number | null;
  repo_url: string | null;
  configuration: Record<string, unknown>;
  folder: string;
  deployment_info: DeploymentInfo | null;
  drafts: DataAppSummary[];
  drafts_unavailable: number;
  links: Link[];
};

// Zod schemas for the modify_* / deploy / delete tool inputs that need enums/literals.
export const modeSchema = z.enum(['dev', 'production']);
export const actionSchema = z.enum(['deploy', 'stop']);
export const authenticationTypeSchema = z.enum(AUTHENTICATION_TYPES);
