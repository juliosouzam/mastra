import { createHash } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleStore } from '../..';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle skill blobs', () => {
  let store: OracleStore;
  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-blobs-integration',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      skipDefaultIndexes: true,
    });
    await store.init();
  });
  afterAll(async () => {
    await store?.close();
  });

  it('stores immutable file content by hash and preserves the first write', async () => {
    const blobs = await store.getStore('blobs');
    expect(blobs).toBeDefined();
    const content = '# Pesquisa\nUse fontes confiáveis. 🌎';
    const entry = {
      hash: createHash('sha256').update(content).digest('hex'),
      content,
      size: Buffer.byteLength(content),
      mimeType: 'text/markdown',
      createdAt: new Date('2025-01-01T00:00:00.123Z'),
    };
    try {
      await blobs!.put(entry);
      await blobs!.put({ ...entry, content: 'duplicate input', createdAt: new Date() });
      expect(await blobs!.get(entry.hash)).toEqual(entry);
      expect(await blobs!.has(entry.hash)).toBe(true);
    } finally {
      await blobs!.delete(entry.hash);
    }
  });

  it.each([
    { name: 'empty files', content: '' },
    { name: 'large files', content: 'Conteúdo 🌎\n'.repeat(12_000) },
    { name: 'embedded null characters', content: 'first\u0000second' },
  ])('preserves $name', async ({ content }) => {
    const blobs = (await store.getStore('blobs'))!;
    const entry = {
      hash: createHash('sha256').update(content).digest('hex'),
      content,
      size: Buffer.byteLength(content),
      createdAt: new Date(),
    };
    try {
      await blobs.put(entry);
      expect(await blobs.get(entry.hash)).toEqual(entry);
      expect(await blobs.delete(entry.hash)).toBe(true);
      expect(await blobs.delete(entry.hash)).toBe(false);
      expect(await blobs.get(entry.hash)).toBeNull();
      expect(await blobs.has(entry.hash)).toBe(false);
    } finally {
      await blobs.delete(entry.hash);
    }
  });

  it('deduplicates batches and returns only existing hashes across read batches', async () => {
    const blobs = (await store.getStore('blobs'))!;
    const seed = Date.now();
    const entries = Array.from({ length: 501 }, (_, index) => {
      const content = `file-${seed}-${index}`;
      return {
        hash: createHash('sha256').update(content).digest('hex'),
        content,
        size: Buffer.byteLength(content),
        createdAt: new Date(),
      };
    });
    try {
      await blobs.putMany([]);
      await blobs.putMany([...entries, entries[0]!]);
      const result = await blobs.getMany([...entries.map(entry => entry.hash), 'missing-hash', entries[0]!.hash]);
      expect(result.size).toBe(entries.length);
      for (const entry of entries) expect(result.get(entry.hash)).toEqual(entry);
      expect(await blobs.getMany([])).toEqual(new Map());
    } finally {
      for (const entry of entries) await blobs.delete(entry.hash);
    }
  });

  it('rolls back the whole batch if a later blob cannot be written', async () => {
    const blobs = (await store.getStore('blobs'))!;
    const content = `transaction-${Date.now()}`;
    const entry = {
      hash: createHash('sha256').update(content).digest('hex'),
      content,
      size: content.length,
      createdAt: new Date(),
    };
    try {
      await expect(blobs.putMany([entry, { ...entry, hash: 'x'.repeat(513) }])).rejects.toThrow();
      expect(await blobs.has(entry.hash)).toBe(false);
    } finally {
      await blobs.delete(entry.hash);
    }
  });
});
