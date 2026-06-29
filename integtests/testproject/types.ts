import { z } from 'zod';

// Port of go-utils pkg/testproject Definition. The projects.json array uses this exact
// shape (same as keboola/go-monorepo build/ci/projects.json).

export const BACKENDS = ['snowflake', 'bigquery'] as const;
export type Backend = (typeof BACKENDS)[number];

export const STAGING_STORAGES = ['abs', 'gcs', 's3'] as const;
export type StagingStorage = (typeof STAGING_STORAGES)[number];

export const projectDefinitionSchema = z.object({
  host: z.string().min(1),
  project: z.number().int().positive(),
  token: z.string().min(1),
  backend: z.enum(BACKENDS),
  stagingStorage: z.enum(STAGING_STORAGES),
  legacyTransformation: z.boolean().default(false),
  isGuest: z.boolean().default(false),
});

export type ProjectDefinition = z.infer<typeof projectDefinitionSchema>;

/** Unique redis/fs lock key for a project: host + numeric id (port of go `host-projectID`). */
export const lockKey = (def: ProjectDefinition): string => `${def.host}-${def.project}`;

/** Storage API base URL derived from the project's bare `host`. */
export const storageApiUrl = (def: ProjectDefinition): string => `https://${def.host}`;

/** A project leased for the duration of one test; `release` frees the lease. */
export type LockedProject = {
  definition: ProjectDefinition;
  storageApiUrl: string;
  storageApiToken: string;
  backend: Backend;
  release: () => Promise<void>;
};

/** Optional selector when a test requires a specific backend. */
export type AcquireOptions = {
  backend?: Backend;
};

export const isCompatible = (def: ProjectDefinition, opts: AcquireOptions): boolean =>
  !opts.backend || def.backend === opts.backend;
