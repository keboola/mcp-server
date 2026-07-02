import { describe, expect, it } from 'vitest';

import { migrationSql } from '../scripts/docsIndex';

import {
  createEmbedderFromEnv,
  DEFAULT_LOCAL_MODEL,
  LocalEmbedder,
  StubEmbedder,
} from '@/clients/docsSearch';
import { parseEnv } from '@/env';

const env = (m: Record<string, string>) => parseEnv(m);

describe('createEmbedderFromEnv', () => {
  it('selects the stub embedder with a default 3072 dim', () => {
    const e = createEmbedderFromEnv(env({ DOCS_EMBEDDER_MODEL: 'stub' }));
    expect(e).toBeInstanceOf(StubEmbedder);
    expect(e?.dim).toBe(3072);
  });

  it('honors a custom dim for the stub embedder', () => {
    const e = createEmbedderFromEnv(
      env({ DOCS_EMBEDDER_MODEL: 'stub', DOCS_EMBEDDER_DIM: '1024' }),
    );
    expect(e?.dim).toBe(1024);
  });

  it('selects the local embedder with a default HF model + 384 dim', () => {
    const e = createEmbedderFromEnv(env({ DOCS_EMBEDDER_MODEL: 'local' }));
    expect(e).toBeInstanceOf(LocalEmbedder);
    expect(e?.model).toBe(DEFAULT_LOCAL_MODEL);
    expect(e?.dim).toBe(384);
  });

  it('honors a custom local HF model + dim (e.g. a 1024 model)', () => {
    const e = createEmbedderFromEnv(
      env({
        DOCS_EMBEDDER_MODEL: 'local',
        DOCS_EMBEDDER_LOCAL_MODEL: 'Xenova/bge-large-en-v1.5',
        DOCS_EMBEDDER_DIM: '1024',
      }),
    );
    expect(e).toBeInstanceOf(LocalEmbedder);
    expect(e?.model).toBe('Xenova/bge-large-en-v1.5');
    expect(e?.dim).toBe(1024);
  });

  it('selects a remote embedder when endpoint + key + model are set', () => {
    const e = createEmbedderFromEnv(
      env({
        DOCS_EMBEDDER_MODEL: 'text-embedding-3-large',
        DOCS_EMBEDDER_ENDPOINT: 'https://embed.example/v1/embeddings',
        DOCS_EMBEDDER_API_KEY: 'secret',
      }),
    );
    expect(e).not.toBeInstanceOf(StubEmbedder);
    expect(e).not.toBeInstanceOf(LocalEmbedder);
    expect(e?.model).toBe('text-embedding-3-large');
    expect(e?.dim).toBe(3072);
  });

  it('returns null when unconfigured or a remote model lacks endpoint/key', () => {
    expect(createEmbedderFromEnv(env({}))).toBeNull();
    expect(
      createEmbedderFromEnv(env({ DOCS_EMBEDDER_MODEL: 'text-embedding-3-large' })),
    ).toBeNull();
  });
});

describe('migrationSql', () => {
  it('parametrizes the halfvec column dimension', () => {
    expect(migrationSql(1024)).toContain('embedding   halfvec(1024) not null');
    expect(migrationSql(384)).toContain('halfvec(384)');
    expect(migrationSql(3072)).toContain('halfvec(3072)');
  });
});
