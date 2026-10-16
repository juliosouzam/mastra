import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { OracleVector } from '.';
import type { OracleVectorFilter } from '.';

describe.runIf(process.env.RUN_ORACLE_VECTOR_INTEGRATION === 'true')('Oracle metadata filter compatibility', () => {
  const indexName = `regex_${randomUUID().replaceAll('-', '')}`;
  let vector: OracleVector;

  beforeAll(async () => {
    vector = new OracleVector({
      id: 'oracle-regex-filters',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
    });
    await vector.createIndex({ indexName, dimension: 3 });
    await vector.upsert({
      indexName,
      ids: ['match', 'extra', 'missing', 'empty', 'null'],
      vectors: [
        [1, 0, 0],
        [1, 0, 0],
        [1, 0, 0],
        [1, 0, 0],
        [1, 0, 0],
      ],
      metadata: [
        { body: 'ALPHA\nbeta', tags: ['x', 'y'], nums: [1, 2], flags: [true, false], nullable: null },
        { body: 'alpha beta', tags: ['y'], nums: [2, 3], flags: [false], nullable: 'value' },
        { body: 'gamma', tags: ['z'], nums: ['1'], flags: ['true'] },
        { tags: [], nums: [], flags: [] },
        { tags: null, flags: null },
      ],
    });
  });

  afterAll(async () => {
    await vector?.deleteIndex({ indexName });
    await vector?.disconnect();
  });

  const cases: Array<{ name: string; filter: OracleVectorFilter; expected: string[] }> = [
    { name: 'RegExp flags', filter: { body: /^alpha.*beta$/is }, expected: ['extra', 'match'] },
    {
      name: '$options flags',
      filter: { body: { $regex: '^alpha.*beta$', $options: 'is' } },
      expected: ['extra', 'match'],
    },
    { name: 'multiline', filter: { body: { $regex: '^beta$', $options: 'm' } }, expected: ['match'] },
    {
      name: 'extended patterns',
      filter: { body: { $regex: '^ a l p h a . b e t a $', $options: 'ix' } },
      expected: ['extra'],
    },
    { name: 'negated regex', filter: { body: { $not: { $regex: '^alpha', $options: 'i' } } }, expected: ['missing'] },
    { name: 'array exclusion', filter: { tags: { $nin: ['x'] } }, expected: ['empty', 'extra', 'missing'] },
    { name: 'numeric array exclusion', filter: { nums: { $nin: [1] } }, expected: ['empty', 'extra'] },
    { name: 'boolean array exclusion', filter: { flags: { $nin: [true] } }, expected: ['empty', 'extra'] },
    { name: 'null distinct from arrays', filter: { tags: { $eq: null } }, expected: ['null'] },
    { name: 'nonnull arrays', filter: { tags: { $ne: null } }, expected: ['empty', 'extra', 'match', 'missing'] },
    {
      name: 'missing or null values',
      filter: { nullable: { $eq: null } },
      expected: ['empty', 'match', 'missing', 'null'],
    },
  ];
  it.each(cases)('matches $name', async ({ filter, expected }) => {
    expect((await vector.query({ indexName, filter })).map(row => row.id).sort()).toEqual(expected);
  });

  it('rejects regex options without a regex', async () => {
    await expect(vector.query({ indexName, filter: { body: { $options: 'i' } } })).rejects.toThrow(
      /\$options.*\$regex/,
    );
  });
});
