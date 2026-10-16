---
'@mastra/factory': minor
---

Added `OracleFactoryStorage` backend recognition. Factory applications can now pass Oracle storage to their existing configuration:

```ts
import { MastraFactory } from '@mastra/factory';
import { OracleFactoryStorage } from '@mastra/oracledb';

const factory = new MastraFactory({
  ...factoryConfig,
  storage: new OracleFactoryStorage({ store: oracleStore }),
});
```
