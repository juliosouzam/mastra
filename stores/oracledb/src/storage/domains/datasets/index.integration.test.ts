import { afterAll, beforeAll, describe } from 'vitest';

import { OracleStore } from '../..';
import { createDatasetFidelityTests } from '../../../../../_test-utils/src/domains/datasets/fidelity';

describe.runIf(process.env.RUN_ORACLE_STORAGE_INTEGRATION === 'true')('Oracle dataset JSON fidelity', () => {
  let store: OracleStore;
  beforeAll(async () => {
    store = new OracleStore({
      id: 'oracle-dataset-fidelity',
      user: process.env.ORACLE_DATABASE_USER,
      password: process.env.ORACLE_DATABASE_PASSWORD,
      connectString: process.env.ORACLE_DATABASE_CONNECT_STRING,
      skipDefaultIndexes: true,
    });
    await store.init();
  });
  afterAll(async () => {
    await store?.stores.datasets?.dangerouslyClearAll();
    await store?.close();
  });
  createDatasetFidelityTests(() => store.stores.datasets!);
});
