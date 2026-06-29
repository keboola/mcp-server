import { createRawClient } from '@/clients/raw';
import { WorkspaceManager } from '@/workspace';
import { type ProjectDefinition, storageApiUrl } from './types';

// Reset a leased project to a clean state before a test runs — port of the Python
// integtests _purge_project + _guard_dedicated_test_project. Uses the raw Storage API
// (rooted at /v2/storage) so it has no dependency on the per-request server config.

// The integ fixtures only ever create stage-prefixed `*.c-test*` buckets. A project holding
// any other bucket is almost certainly NOT a dedicated test project — refuse to wipe it.
const TEST_BUCKET_PREFIXES = ['in.c-test', 'out.c-test', 'sys.c-test'];

type Bucket = { id: string };
type Component = { id: string; configurations?: { id: string }[] };
type Workspace = { id: string | number; creatorToken?: { description?: string } };
type Meta = { id: string | number; key?: string };

const STATIC_WORKSPACE_CREATORS = new Set(['Background Indexing Token']);

export const cleanProject = async (def: ProjectDefinition): Promise<void> => {
  const raw = createRawClient({ baseUrl: `${storageApiUrl(def)}/v2/storage`, token: def.token });

  // Guard: refuse to reset a project that holds non-test buckets.
  const buckets = await raw.get<Bucket[]>('buckets');
  const foreign = buckets.filter((b) => !TEST_BUCKET_PREFIXES.some((p) => b.id.startsWith(p)));
  if (foreign.length > 0) {
    throw new Error(
      `Refusing to reset project ${def.project}: found non-test buckets ${foreign
        .map((b) => b.id)
        .join(', ')}. The projects.json pool may point at a non-dedicated project.`,
    );
  }

  for (const bucket of buckets) {
    await raw.delete(`buckets/${bucket.id}`, { params: { force: 'true' } });
  }

  const components = await raw.get<Component[]>('components', { params: { include: 'configuration' } });
  for (const component of components) {
    for (const config of component.configurations ?? []) {
      // First delete moves to trash; second removes it.
      await raw.delete(`components/${component.id}/configs/${config.id}`);
      await raw.delete(`components/${component.id}/configs/${config.id}`);
    }
  }

  const workspaces = await raw.get<Workspace[]>('branch/default/workspaces');
  for (const ws of workspaces) {
    if (STATIC_WORKSPACE_CREATORS.has(ws.creatorToken?.description ?? '')) continue;
    await raw.delete(`workspaces/${ws.id}`).catch(() => {
      /* a workspace backed by a deleted sandbox config may already be gone */
    });
  }

  const metadata = await raw.get<Meta[]>('branch/default/metadata');
  for (const meta of metadata) {
    if (meta.key === WorkspaceManager.MCP_META_KEY) {
      await raw.delete(`branch/default/metadata/${meta.id}`);
    }
  }
};
