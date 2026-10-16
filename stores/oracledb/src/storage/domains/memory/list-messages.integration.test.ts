import { randomUUID } from 'node:crypto';
import type { StorageThreadType } from '@mastra/core/memory';
import type { MemoryStorage } from '@mastra/core/storage';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import { OracleStore } from '../..';
import { createSampleMessageV2 } from '../../../../../_test-utils/src/domains/memory/data';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')(
  'Oracle message pagination without totals',
  () => {
    let store: OracleStore;
    let memory: MemoryStorage;
    let thread: StorageThreadType;

    beforeAll(async () => {
      store = new OracleStore({
        id: 'oracle-message-pagination-integration',
        user: process.env.ORACLE_DATABASE_USER,
        password: process.env.ORACLE_DATABASE_PASSWORD,
        connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
        skipDefaultIndexes: true,
      });
      await store.init();
      memory = (await store.getStore('memory'))!;
      thread = await memory.saveThread({
        thread: { id: randomUUID(), resourceId: randomUUID(), createdAt: new Date(), updatedAt: new Date() },
      });
      await memory.saveMessages({
        messages: Array.from({ length: 3 }, (_, index) =>
          createSampleMessageV2({ threadId: thread.id, thread, createdAt: new Date(Date.now() + index) }),
        ),
      });
    });

    afterAll(async () => {
      if (thread) await memory.deleteThread({ threadId: thread.id });
      await store?.close();
    });

    it.each([
      { scope: 'thread', page: 0, perPage: 2, count: 2, hasMore: true },
      { scope: 'thread', page: 1, perPage: 2, count: 1, hasMore: false },
      { scope: 'resource', page: 0, perPage: 2, count: 2, hasMore: true },
      { scope: 'resource', page: 1, perPage: 2, count: 1, hasMore: false },
      { scope: 'thread', page: 0, perPage: false, count: 3, hasMore: false },
      { scope: 'resource', page: 0, perPage: false, count: 3, hasMore: false },
    ] as const)(
      'returns $scope page $page with perPage $perPage without a count query',
      async ({ scope, page: pageNumber, perPage, count, hasMore }) => {
        const pool = await store.getPool();
        const connection = await pool.getConnection();
        const borrow = vi.spyOn(pool, 'getConnection').mockResolvedValue(connection);
        const execute = vi.spyOn(connection, 'execute');
        try {
          const options = { perPage, page: pageNumber, includeTotal: false };
          const page =
            scope === 'thread'
              ? await memory.listMessages({ threadId: thread.id, ...options })
              : await memory.listMessagesByResourceId({ resourceId: thread.resourceId, ...options });

          expect(page.messages).toHaveLength(count);
          expect(page.hasMore).toBe(hasMore);
          expect(execute.mock.calls.some(call => /\bCOUNT\s*\(/i.test(String(call[0])))).toBe(false);
        } finally {
          borrow.mockRestore();
          execute.mockRestore();
          await connection.close().catch(() => undefined);
        }
      },
    );
  },
);
