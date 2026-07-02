import { readFileSync } from 'node:fs';
import { isAbsolute } from 'node:path';

import { type ProjectDefinition, projectDefinitionSchema } from './types';

// Port of go-utils getProjects / GetProjectsFrom: load + validate the projects.json pool
// once per process from TEST_KBC_PROJECTS_FILE (absolute path).

export const TEST_KBC_PROJECTS_FILE = 'TEST_KBC_PROJECTS_FILE';

/** Parses + validates a projects.json document (the array form). */
export const parseProjects = (json: string): ProjectDefinition[] => {
  const raw: unknown = JSON.parse(json);
  if (!Array.isArray(raw) || raw.length === 0) {
    throw new Error(
      'projects.json must be a non-empty array of {host, project, token, backend, stagingStorage}.',
    );
  }
  return raw.map((entry, i) => {
    const result = projectDefinitionSchema.safeParse(entry);
    if (!result.success) {
      throw new Error(`projects.json[${i}] is invalid: ${result.error.issues.map((x) => x.message).join('; ')}`);
    }
    return result.data;
  });
};

let cached: ProjectDefinition[] | undefined;

/** Loads the pool once per process. `path` overrides TEST_KBC_PROJECTS_FILE (must be absolute). */
export const loadProjects = (path?: string): ProjectDefinition[] => {
  if (cached) return cached;
  const file = path ?? process.env[TEST_KBC_PROJECTS_FILE];
  if (!file) {
    throw new Error(`Set ${TEST_KBC_PROJECTS_FILE} to the absolute path of the projects.json pool file.`);
  }
  if (!isAbsolute(file)) {
    throw new Error(`${TEST_KBC_PROJECTS_FILE} must be an absolute path, got: ${file}`);
  }
  cached = parseProjects(readFileSync(file, 'utf-8'));
  return cached;
};

/** Test-only: clears the process-singleton cache. */
export const _resetProjectsCache = (): void => {
  cached = undefined;
};
