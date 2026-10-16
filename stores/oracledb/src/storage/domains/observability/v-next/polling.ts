import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';
import { coreFeatures } from '@mastra/core/features';
import type { DbClient } from './client';

export function deltaPollingFeatureEnabled(): boolean {
  return coreFeatures.has('observability-delta-polling');
}
export function assertDeltaPollingEnabled(): void {
  if (!deltaPollingFeatureEnabled())
    throw new MastraError({
      id: 'OBSERVABILITY_DELTA_POLLING_NOT_SUPPORTED',
      domain: ErrorDomain.MASTRA_OBSERVABILITY,
      category: ErrorCategory.SYSTEM,
      text: 'This storage provider does not support observability delta polling',
    });
}
function integer(value: string): string {
  if (!/^\d+$/.test(value) || BigInt(value) > 18446744073709551615n)
    throw new MastraError({
      id: 'OBSERVABILITY_INVALID_DELTA_CURSOR',
      domain: ErrorDomain.MASTRA_OBSERVABILITY,
      category: ErrorCategory.USER,
      text: 'Invalid observability delta cursor',
    });
  return value;
}
export function encodeDeltaCursor(xactId: unknown, cursorId: unknown = 0): string {
  return `${integer(String(xactId ?? 0))}:${integer(String(cursorId ?? 0))}`;
}
export function decodeDeltaCursor(cursor: string): { xactId: string; cursorId: string } {
  const [xactId, cursorId, extra] = cursor.split(':');
  if (extra !== undefined || xactId === undefined || cursorId === undefined) integer('invalid');
  return { xactId: integer(xactId!), cursorId: integer(cursorId!) };
}
export async function readSafeXactHorizon(client: Pick<DbClient, 'one' | 'snapshotHorizon'>): Promise<string> {
  if (client.snapshotHorizon !== undefined) return client.snapshotHorizon;
  // Requires EXECUTE ON SYS.DBMS_FLASHBACK. A sequence-only cursor could miss a late commit.
  const row = await client.one<{ xactId: string }>(
    'SELECT TO_CHAR(DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER) AS "xactId" FROM dual',
  );
  return integer(row.xactId);
}
