import * as coreStorage from '@mastra/core/storage';

// Namespace access keeps adapters loadable with core versions predating previews.
const generator = coreStorage.buildStoredPreview as
  | ((preview: string | null | undefined, value: unknown, serialized?: string | null) => string)
  | undefined;
export const canGeneratePreviews = typeof generator === 'function';
const legacyGenerator = coreStorage.buildInputPreview as ((value: unknown) => string | undefined) | undefined;
const canBuildPreview = canGeneratePreviews || typeof legacyGenerator === 'function';

/** Respect the preview fields exposed by the caller's Core response schema. */
export function previewFieldsForSchema(
  schema: { shape: object },
  previews: { inputPreview: string | null; outputPreview: string | null; errorPreview: string | null },
) {
  return {
    inputPreview: previews.inputPreview,
    ...('outputPreview' in schema.shape ? { outputPreview: previews.outputPreview } : {}),
    ...('errorPreview' in schema.shape ? { errorPreview: previews.errorPreview } : {}),
  };
}

/** SQL detection only; preview formatting stays in TypeScript. Empty strings are intentional. */
export function missingPreviewSql(column: string): string {
  return canBuildPreview ? `${column} IS NULL` : '1 = 0';
}

export function needsPreview(value: unknown): boolean {
  return canBuildPreview && value == null;
}

export function storedPreview(
  preview: string | null | undefined,
  value: unknown,
  serialized?: string | null,
): string | null {
  return canGeneratePreviews
    ? generator!(preview, value, serialized)
    : preview?.trim() || legacyGenerator?.(serialized ?? value) || null;
}

export function readPreview(preview: unknown, payload?: unknown, jsonEncoded = false): string | null {
  const supplied = preview as string | null | undefined;
  // A stored empty string is the writer's explicit "nothing to preview" sentinel.
  if (supplied != null) return supplied.trim() || null;
  let value = payload;
  if (needsPreview(preview) && jsonEncoded && typeof payload === 'string') {
    try {
      value = JSON.parse(payload);
    } catch {
      value = null;
    }
  }
  return storedPreview(supplied, value, jsonEncoded && typeof payload === 'string' ? payload : undefined) || null;
}

/** Core owns response fields and truncation rules for its installed version. */
export function buildSpanQueryResponse(...args: Parameters<typeof coreStorage.buildSpanQueryResponse>) {
  const [plan, selected, payloads, metrics = []] = args;
  // Older Core schemas reject the context fields added to current span responses.
  const compatibleSelected = coreStorage.SPAN_QUERY_FIELD_REGISTRY
    ? selected
    : (selected.map(row => ({
        organizationId: row.organizationId,
        resourceId: row.resourceId,
        traceId: row.traceId,
        spanId: row.spanId,
        parentSpanId: row.parentSpanId,
        name: row.name,
        spanType: row.spanType,
        status: row.status,
        startedAt: row.startedAt,
        endedAt: row.endedAt,
        durationMs: row.durationMs,
        entityType: row.entityType,
        entityId: row.entityId,
        entityName: row.entityName,
        model: row.model,
        provider: row.provider,
      })) as typeof selected);
  return coreStorage.buildSpanQueryResponse(plan, compatibleSelected, payloads, metrics);
}
