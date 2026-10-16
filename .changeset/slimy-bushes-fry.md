---
'@mastra/code-sdk': minor
---

Added support for injecting `OracleStore` into the Code SDK. Supply an `OracleVector` to keep recall data in Oracle too:

```ts
import { createMastraCode } from '@mastra/code-sdk';
import { OracleStore, OracleVector, OraclePoolManager } from '@mastra/oracledb';

const poolManager = new OraclePoolManager({
  user: process.env.ORACLE_DATABASE_USER,
  password: process.env.ORACLE_DATABASE_PASSWORD,
  connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
});

await createMastraCode({
  storage: new OracleStore({ id: 'code-storage', poolManager }),
  vector: new OracleVector({ id: 'code-vectors', poolManager }),
});
```
