import { ErrorCategory, MastraError } from '@mastra/core/error';
import {
  calculatePagination,
  SkillsStorage,
  normalizePerPage,
  TABLE_SKILLS,
  TABLE_SKILL_VERSIONS,
  TABLE_FAVORITES,
  TABLE_SCHEMAS,
} from '@mastra/core/storage';
import type {
  StorageCreateSkillInput,
  StorageListSkillsInput,
  StorageListSkillsOutput,
  StorageSkillType,
  StorageUpdateSkillInput,
} from '@mastra/core/storage';
import type {
  CreateSkillVersionInput,
  ListSkillVersionsInput,
  ListSkillVersionsOutput,
  SkillVersion,
} from '@mastra/core/storage/domains/skills';
import { skillSnapshotFieldValuesEqual } from '@mastra/core/storage/domains/skills';

import oracledb from 'oracledb';

import { clobBind, jsonBindText, nullableClobBind } from '../../../shared/connection';
import { indexNameForTable, qualifyName } from '../../../vector/identifiers';
import { OracleDB, createOracleIndex, filterIndexesForTables, parseOracleJson } from '../../db';
import type { OracleCreateIndexOptions, OracleTxClient } from '../../db';
import {
  createOracleStorageError,
  buildOracleJsonContains,
  parseOptionalStringArray,
  toDate,
} from '../../domain-utils';
import type { OracleDomainConfig } from '../../types';

// Skills use the same registry/version pattern as agents: parent rows handle discovery while version rows snapshot skill content and file manifests.
const STORE_NAME = 'ORACLEDB';
const SNAPSHOT_FIELDS = [
  'name',
  'description',
  'instructions',
  'license',
  'compatibility',
  'source',
  'references',
  'scripts',
  'assets',
  'files',
  'metadata',
  'tree',
] as const;

const SKILL_ACTIVE_VERSION_ID = '"activeVersionId"';
const SKILL_AUTHOR_ID = '"authorId"';
const SKILL_CREATED_AT = '"createdAt"';
const SKILL_UPDATED_AT = '"updatedAt"';
const VERSION_SKILL_ID = '"skillId"';
const VERSION_VERSION_NUMBER = '"versionNumber"';
const VERSION_CHANGED_FIELDS = '"changedFields"';
const VERSION_CHANGE_MESSAGE = '"changeMessage"';
const VERSION_CREATED_AT = '"createdAt"';

type SkillRow = {
  id: string;
  status: StorageSkillType['status'];
  activeVersionId?: string | null;
  authorId?: string | null;
  visibility?: StorageSkillType['visibility'] | null;
  favoriteCount?: number | null;
  createdAt: Date | string;
  updatedAt: Date | string;
};

type SkillVersionRow = {
  id: string;
  skillId: string;
  versionNumber: number | string;
  name: string;
  description: string | null;
  instructions: string | null;
  license?: string | null;
  compatibility?: unknown;
  source?: unknown;
  references?: unknown;
  scripts?: unknown;
  assets?: unknown;
  files?: unknown;
  metadata?: unknown;
  tree?: unknown;
  changedFields?: unknown;
  changeMessage?: string | null;
  changeMessageIsNull: number;
  createdAt: Date | string;
};

type VersionWriteClient = Pick<OracleTxClient, 'none'>;

export class SkillsOracle extends SkillsStorage {
  // Skills follow the registry pattern: parent lifecycle row plus versioned skill definition snapshots.
  static readonly MANAGED_TABLES = [TABLE_SKILLS, TABLE_SKILL_VERSIONS] as const;

  private readonly db: OracleDB;
  private readonly schemaName?: string;
  private readonly skipDefaultIndexes?: boolean;
  private readonly indexes: OracleCreateIndexOptions[];

  constructor(config: OracleDomainConfig) {
    super();
    this.db = new OracleDB({
      ...config,
      jsonColumns: [
        'compatibility',
        'source',
        'references',
        'scripts',
        'assets',
        'files',
        'metadata',
        'tree',
        'changedFields',
      ],
    });
    this.schemaName = config.schemaName;
    this.skipDefaultIndexes = config.skipDefaultIndexes;
    this.indexes = filterIndexesForTables(config.indexes, SkillsOracle.MANAGED_TABLES);
  }

  async init(): Promise<void> {
    await this.db.createTable({
      tableName: TABLE_SKILLS,
      schema: TABLE_SCHEMAS[TABLE_SKILLS],
    });
    await this.db.createTable({
      tableName: TABLE_SKILL_VERSIONS,
      schema: TABLE_SCHEMAS[TABLE_SKILL_VERSIONS],
    });
    await this.createIndexes();
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.db.tx(async client => {
      await client.none(`DELETE FROM ${this.table(TABLE_SKILL_VERSIONS)}`);
      await client.none(`DELETE FROM ${this.table(TABLE_SKILLS)}`);
    });
  }

  getDefaultIndexDefinitions(): OracleCreateIndexOptions[] {
    return getDefaultSkillIndexDefinitions(this.indexName.bind(this));
  }

  async getById(id: string): Promise<StorageSkillType | null> {
    try {
      const row = await this.db.read.oneOrNone<SkillRow>(
        `${this.skillSelect()} FROM ${this.table(TABLE_SKILLS)} WHERE id = :id`,
        { id },
      );
      return row ? this.parseSkillRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_SKILL_BY_ID', 'FAILED', { skillId: id }, error);
    }
  }

  async create({ skill }: { skill: StorageCreateSkillInput }): Promise<StorageSkillType> {
    try {
      const now = new Date();
      const visibility = skill.visibility ?? (skill.authorId ? 'private' : undefined);
      await this.db.tx(async client => {
        await client.none(
          `INSERT INTO ${this.table(TABLE_SKILLS)} (id, status, "activeVersionId", "authorId", visibility, "favoriteCount", "createdAt", "updatedAt")
           VALUES (:id, 'draft', NULL, :authorId, :visibility, 0, :createdAt, :createdAt)`,
          {
            id: skill.id,
            authorId: skill.authorId ?? null,
            visibility: visibility ?? null,
            createdAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: now },
          },
        );
        const { id: _id, authorId: _author, visibility: _visibility, ...snapshot } = skill;
        await this.insertVersion(
          client,
          {
            id: crypto.randomUUID(),
            skillId: skill.id,
            versionNumber: 1,
            ...snapshot,
            changedFields: [...SNAPSHOT_FIELDS],
            changeMessage: 'Initial version',
          },
          now,
        );
      });
      return {
        id: skill.id,
        status: 'draft',
        activeVersionId: undefined,
        authorId: skill.authorId,
        visibility,
        favoriteCount: 0,
        createdAt: now,
        updatedAt: now,
      };
    } catch (error) {
      throw this.storageError('CREATE_SKILL', 'FAILED', { skillId: skill.id }, error);
    }
  }

  async update(input: StorageUpdateSkillInput): Promise<StorageSkillType> {
    const { id, authorId, visibility, activeVersionId, status, ...rawConfigFields } = input;
    try {
      return await this.db.tx(async client => {
        // Serialize automatic version allocation on the parent, as with other Oracle state updates.
        const row = await client.oneOrNone<SkillRow>(
          `${this.skillSelect()} FROM ${this.table(TABLE_SKILLS)} WHERE id = :id FOR UPDATE`,
          { id },
        );
        if (!row) throw new Error(`Skill with id ${id} not found`);
        const configFields = Object.fromEntries(
          Object.entries(rawConfigFields).filter(([, value]) => value !== undefined),
        );
        if (SNAPSHOT_FIELDS.some(field => field in configFields)) {
          const latestRow = await client.oneOrNone<SkillVersionRow>(
            `${this.versionSelect()} FROM ${this.table(TABLE_SKILL_VERSIONS)} WHERE ${VERSION_SKILL_ID} = :id ORDER BY ${VERSION_VERSION_NUMBER} DESC FETCH FIRST 1 ROW ONLY`,
            { id },
          );
          if (!latestRow) throw new Error(`No versions found for skill ${id}`);
          const latest = this.parseVersionRow(latestRow);
          const {
            id: _id,
            skillId: _skillId,
            versionNumber: _number,
            changedFields: _changedFields,
            changeMessage: _message,
            createdAt: _at,
            ...latestConfig
          } = latest;
          const changedFields = SNAPSHOT_FIELDS.filter(
            field => field in configFields && !skillSnapshotFieldValuesEqual(configFields[field], latestConfig[field]),
          );
          if (changedFields.length) {
            await this.insertVersion(client, {
              id: crypto.randomUUID(),
              skillId: id,
              versionNumber: latest.versionNumber + 1,
              ...latestConfig,
              ...configFields,
              changedFields,
              changeMessage: `Updated ${changedFields.join(', ')}`,
            });
          }
        }
        const setClauses = [`${SKILL_UPDATED_AT} = :updatedAt`];
        const binds: Record<string, unknown> = {
          id,
          updatedAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: new Date() },
        };
        if (authorId !== undefined) {
          setClauses.push(`${SKILL_AUTHOR_ID} = :authorId`);
          binds.authorId = authorId;
        }
        if (activeVersionId !== undefined) {
          setClauses.push(`${SKILL_ACTIVE_VERSION_ID} = :activeVersionId`);
          binds.activeVersionId = activeVersionId;
          if (status === undefined) {
            setClauses.push('status = :status');
            binds.status = 'published';
          }
        }
        if (status !== undefined) {
          setClauses.push('status = :status');
          binds.status = status;
        }
        if (visibility !== undefined) {
          setClauses.push('visibility = :visibility');
          binds.visibility = visibility;
        }
        await client.none(`UPDATE ${this.table(TABLE_SKILLS)} SET ${setClauses.join(', ')} WHERE id = :id`, binds);
        return this.parseSkillRow(
          await client.one<SkillRow>(`${this.skillSelect()} FROM ${this.table(TABLE_SKILLS)} WHERE id = :id`, { id }),
        );
      });
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('UPDATE_SKILL', 'FAILED', { skillId: id }, error);
    }
  }

  async delete(id: string): Promise<void> {
    try {
      await this.db.tx(async client => {
        await client.none(`DELETE FROM ${this.table(TABLE_SKILL_VERSIONS)} WHERE ${VERSION_SKILL_ID} = :id`, {
          id,
        });
        await client.none(`DELETE FROM ${this.table(TABLE_SKILLS)} WHERE id = :id`, { id });
      });
    } catch (error) {
      throw this.storageError('DELETE_SKILL', 'FAILED', { skillId: id }, error);
    }
  }

  async list(args?: StorageListSkillsInput): Promise<StorageListSkillsOutput> {
    const {
      page = 0,
      perPage: perPageInput,
      orderBy,
      authorId,
      visibility,
      status,
      metadata,
      entityIds,
      pinFavoritedFor,
      favoritedOnly,
    } = args ?? {};
    this.validatePagination(page, perPageInput, 100);
    const perPage = normalizePerPage(perPageInput, 100);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);
    const empty = { skills: [], total: 0, page, perPage: perPageForResponse, hasMore: false };
    if (entityIds?.length === 0 || (favoritedOnly && !pinFavoritedFor)) return empty;
    const { field, direction } = this.parseOrderBy(orderBy);
    const conditions: string[] = [];
    const binds: Record<string, unknown> = {};
    if (authorId !== undefined) {
      conditions.push('s."authorId" = :authorId');
      binds.authorId = authorId;
    }
    if (visibility !== undefined) {
      conditions.push('s.visibility = :visibility');
      binds.visibility = visibility;
    }
    if (status !== undefined) {
      conditions.push('s.status = :status');
      binds.status = status;
    }
    if (entityIds?.length) {
      const groups: string[] = [];
      for (let start = 0; start < entityIds.length; start += 500) {
        const names = entityIds.slice(start, start + 500).map((id, offset) => {
          const key = `entity${start + offset}`;
          binds[key] = id;
          return `:${key}`;
        });
        groups.push(`s.id IN (${names.join(', ')})`);
      }
      conditions.push(`(${groups.join(' OR ')})`);
    }
    if (metadata && Object.keys(metadata).length) {
      const filter = buildOracleJsonContains('metadata', metadata);
      const latestId = `(SELECT v.id FROM ${this.table(TABLE_SKILL_VERSIONS)} v WHERE v."skillId" = s.id ORDER BY v."versionNumber" DESC FETCH FIRST 1 ROW ONLY)`;
      const versionId = status === 'draft' ? latestId : `COALESCE(s."activeVersionId", ${latestId})`;
      conditions.push(
        `EXISTS (SELECT 1 FROM ${this.table(TABLE_SKILL_VERSIONS)} WHERE id = ${versionId} AND ${filter.sql})`,
      );
      Object.assign(binds, filter.binds);
    }
    const join = pinFavoritedFor
      ? `LEFT JOIN ${this.table(TABLE_FAVORITES)} f ON f."entityId" = s.id AND f."entityType" = 'skill' AND f."userId" = :favoriteUser`
      : '';
    if (pinFavoritedFor) binds.favoriteUser = pinFavoritedFor;
    if (favoritedOnly) conditions.push('f."userId" IS NOT NULL');
    const from = `FROM ${this.table(TABLE_SKILLS)} s ${join}${conditions.length ? ` WHERE ${conditions.join(' AND ')}` : ''}`;
    try {
      const count = await this.db.read.one<{ count: number }>(`SELECT COUNT(*) AS "count" ${from}`, binds);
      const total = Number(count.count);
      if (!total) return empty;
      if (perPage === 0) return { ...empty, total, hasMore: true };
      const rows = await this.db.read.manyOrNone<SkillRow>(
        `${this.skillSelect('s')} ${from}
        ORDER BY ${pinFavoritedFor ? 'CASE WHEN f."userId" IS NULL THEN 1 ELSE 0 END, ' : ''}s."${field}" ${direction}, s.id
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
        { ...binds, offset, limit: perPageInput === false ? total : perPage },
      );
      return {
        skills: rows.map(row => this.parseSkillRow(row)),
        total,
        page,
        perPage: perPageForResponse,
        hasMore: perPageInput === false ? false : offset + perPage < total,
      };
    } catch (error) {
      throw this.storageError('LIST_SKILLS', 'FAILED', {}, error);
    }
  }

  async createVersion(input: CreateSkillVersionInput): Promise<SkillVersion> {
    try {
      const existingById = await this.db.oneOrNone<{ id: string }>(
        `SELECT id AS "id" FROM ${this.table(TABLE_SKILL_VERSIONS)} WHERE id = :id`,
        { id: input.id },
      );
      if (existingById) {
        throw new Error(`Version with id ${input.id} already exists`);
      }

      const existingByNumber = await this.db.oneOrNone<{ id: string }>(
        `SELECT id AS "id" FROM ${this.table(
          TABLE_SKILL_VERSIONS,
        )} WHERE ${VERSION_SKILL_ID} = :skillId AND ${VERSION_VERSION_NUMBER} = :versionNumber`,
        { skillId: input.skillId, versionNumber: input.versionNumber },
      );
      if (existingByNumber) {
        throw new Error(`Version number ${input.versionNumber} already exists for Skill ${input.skillId}`);
      }

      const createdAt = new Date();
      await this.insertVersion(this.db, input, createdAt);

      return {
        ...input,
        createdAt,
      };
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('CREATE_SKILL_VERSION', 'FAILED', { versionId: input.id, skillId: input.skillId }, error);
    }
  }

  async getVersion(id: string): Promise<SkillVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<SkillVersionRow>(
        `${this.versionSelect()} FROM ${this.table(TABLE_SKILL_VERSIONS)} WHERE id = :id`,
        { id },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_SKILL_VERSION', 'FAILED', { versionId: id }, error);
    }
  }

  async getVersionByNumber(skillId: string, versionNumber: number): Promise<SkillVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<SkillVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_SKILL_VERSIONS,
        )} WHERE ${VERSION_SKILL_ID} = :skillId AND ${VERSION_VERSION_NUMBER} = :versionNumber`,
        { skillId, versionNumber },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_SKILL_VERSION_BY_NUMBER', 'FAILED', { skillId, versionNumber }, error);
    }
  }

  async getLatestVersion(skillId: string): Promise<SkillVersion | null> {
    try {
      const row = await this.db.read.oneOrNone<SkillVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_SKILL_VERSIONS,
        )} WHERE ${VERSION_SKILL_ID} = :skillId ORDER BY ${VERSION_VERSION_NUMBER} DESC FETCH FIRST 1 ROWS ONLY`,
        { skillId },
      );
      return row ? this.parseVersionRow(row) : null;
    } catch (error) {
      throw this.storageError('GET_LATEST_SKILL_VERSION', 'FAILED', { skillId }, error);
    }
  }

  async listVersions(input: ListSkillVersionsInput): Promise<ListSkillVersionsOutput> {
    const { skillId, page = 0, perPage: perPageInput, orderBy } = input;

    try {
      this.validatePagination(page, perPageInput, 20);
    } catch (error) {
      throw this.storageError('LIST_SKILL_VERSIONS', 'INVALID_INPUT', { page, skillId }, error, ErrorCategory.USER);
    }

    const perPage = normalizePerPage(perPageInput, 20);
    const { offset, perPage: perPageForResponse } = calculatePagination(page, perPageInput, perPage);

    try {
      const { field, direction } = this.parseVersionOrderBy(orderBy);
      const countRow = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(TABLE_SKILL_VERSIONS)} WHERE ${VERSION_SKILL_ID} = :skillId`,
        { skillId },
      );
      const total = Number(countRow?.count ?? 0);

      if (total === 0) {
        return {
          versions: [],
          total: 0,
          page,
          perPage: perPageForResponse,
          hasMore: false,
        };
      }

      if (perPage === 0) {
        return {
          versions: [],
          total,
          page,
          perPage: perPageForResponse,
          hasMore: total > 0,
        };
      }

      const limit = perPageInput === false ? total : perPage;
      // `id ASC` breaks ties between versions sharing the same order-by value so
      // pages stay stable instead of duplicating or dropping rows across pages.
      const rows = await this.db.read.manyOrNone<SkillVersionRow>(
        `${this.versionSelect()} FROM ${this.table(
          TABLE_SKILL_VERSIONS,
        )} WHERE ${VERSION_SKILL_ID} = :skillId ORDER BY ${this.versionOrderColumn(
          field,
        )} ${direction}, id ASC OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY`,
        { skillId, offset, limit },
      );

      return {
        versions: rows.map(row => this.parseVersionRow(row)),
        total,
        page,
        perPage: perPageForResponse,
        hasMore: perPageInput === false ? false : offset + perPage < total,
      };
    } catch (error) {
      if (error instanceof MastraError) throw error;
      throw this.storageError('LIST_SKILL_VERSIONS', 'FAILED', { skillId }, error);
    }
  }

  async deleteVersion(id: string): Promise<void> {
    try {
      await this.db.none(`DELETE FROM ${this.table(TABLE_SKILL_VERSIONS)} WHERE id = :id`, { id });
    } catch (error) {
      throw this.storageError('DELETE_SKILL_VERSION', 'FAILED', { versionId: id }, error);
    }
  }

  async deleteVersionsByParentId(entityId: string): Promise<void> {
    try {
      await this.db.none(`DELETE FROM ${this.table(TABLE_SKILL_VERSIONS)} WHERE ${VERSION_SKILL_ID} = :entityId`, {
        entityId,
      });
    } catch (error) {
      throw this.storageError('DELETE_SKILL_VERSIONS_BY_SKILL_ID', 'FAILED', { skillId: entityId }, error);
    }
  }

  async countVersions(skillId: string): Promise<number> {
    try {
      const row = await this.db.read.oneOrNone<{ count: number | string }>(
        `SELECT COUNT(*) AS "count" FROM ${this.table(TABLE_SKILL_VERSIONS)} WHERE ${VERSION_SKILL_ID} = :skillId`,
        { skillId },
      );
      return Number(row?.count ?? 0);
    } catch (error) {
      throw this.storageError('COUNT_SKILL_VERSIONS', 'FAILED', { skillId }, error);
    }
  }

  private async createIndexes(): Promise<void> {
    await this.db.withConnection(async connection => {
      for (const index of this.getDefaultIndexDefinitions()) {
        // Version-number uniqueness is an invariant, not an optional performance index.
        if (this.skipDefaultIndexes && !index.unique) continue;
        try {
          await createOracleIndex(connection, index, this.schemaName);
        } catch (error) {
          if (index.unique) throw error;
          this.logger?.warn?.(`Failed to create Oracle default index ${index.name}:`, error);
        }
      }

      for (const index of this.indexes) {
        await createOracleIndex(connection, index, this.schemaName);
      }
    });
  }

  private async insertVersion(
    client: VersionWriteClient,
    input: CreateSkillVersionInput,
    createdAt = new Date(),
  ): Promise<void> {
    await client.none(
      `INSERT INTO ${this.table(TABLE_SKILL_VERSIONS)} (id, "skillId", "versionNumber", name, description, instructions, license,
        compatibility, source, "references", scripts, assets, files, metadata, tree, "changedFields", "changeMessage", "createdAt")
       VALUES (:id, :skillId, :versionNumber, :name, NVL(:description, EMPTY_CLOB()), NVL(:instructions, EMPTY_CLOB()), :license,
        JSON(:compatibility), JSON(:sourceConfig), JSON(:refPaths), JSON(:scripts), JSON(:assets), JSON(:files), JSON(:metadata), JSON(:tree),
        JSON(:changedFields), CASE WHEN :emptyChangeMessage = 1 THEN EMPTY_CLOB() ELSE :changeMessage END, :createdAt)`,
      {
        id: input.id,
        skillId: input.skillId,
        versionNumber: input.versionNumber,
        name: input.name,
        description: clobBind(input.description),
        instructions: clobBind(input.instructions),
        license: input.license ?? null,
        compatibility: nullableClobBind(jsonBindText(input.compatibility)),
        sourceConfig: nullableClobBind(jsonBindText(input.source)),
        refPaths: nullableClobBind(jsonBindText(input.references)),
        scripts: nullableClobBind(jsonBindText(input.scripts)),
        assets: nullableClobBind(jsonBindText(input.assets)),
        files: nullableClobBind(jsonBindText(input.files)),
        metadata: nullableClobBind(jsonBindText(input.metadata)),
        tree: nullableClobBind(jsonBindText(input.tree)),
        changedFields: nullableClobBind(jsonBindText(input.changedFields)),
        changeMessage: input.changeMessage === undefined ? null : clobBind(input.changeMessage),
        emptyChangeMessage: input.changeMessage === '' ? 1 : 0,
        createdAt: { type: oracledb.DB_TYPE_TIMESTAMP_TZ, val: createdAt },
      },
    );
  }

  private skillSelect(alias?: string): string {
    const prefix = alias ? `${alias}.` : '';
    return `SELECT ${prefix}id AS "id", ${prefix}status AS "status", ${prefix}"activeVersionId", ${prefix}"authorId",
      ${prefix}visibility AS "visibility", ${prefix}"favoriteCount", ${prefix}"createdAt", ${prefix}"updatedAt"`;
  }

  private versionSelect(): string {
    return `SELECT id AS "id", "skillId", "versionNumber", name AS "name", description AS "description", instructions AS "instructions", license AS "license",
      JSON_SERIALIZE(compatibility RETURNING CLOB) AS "compatibility", JSON_SERIALIZE(source RETURNING CLOB) AS "source", JSON_SERIALIZE("references" RETURNING CLOB) AS "references", JSON_SERIALIZE(scripts RETURNING CLOB) AS "scripts", JSON_SERIALIZE(assets RETURNING CLOB) AS "assets", JSON_SERIALIZE(files RETURNING CLOB) AS "files", JSON_SERIALIZE(metadata RETURNING CLOB) AS "metadata", JSON_SERIALIZE(tree RETURNING CLOB) AS "tree",
      JSON_SERIALIZE("changedFields" RETURNING CLOB) AS "changedFields", "changeMessage", CASE WHEN "changeMessage" IS NULL THEN 1 ELSE 0 END AS "changeMessageIsNull", "createdAt"`;
  }

  private parseSkillRow(row: SkillRow): StorageSkillType {
    return {
      id: String(row.id),
      status: row.status,
      activeVersionId: optionalString(row.activeVersionId),
      authorId: optionalString(row.authorId),
      visibility: row.visibility ?? undefined,
      favoriteCount: Number(row.favoriteCount ?? 0),
      createdAt: toDate(row.createdAt),
      updatedAt: toDate(row.updatedAt),
    };
  }

  private parseVersionRow(row: SkillVersionRow): SkillVersion {
    return {
      id: String(row.id),
      skillId: String(row.skillId),
      versionNumber: Number(row.versionNumber),
      name: String(row.name),
      description: row.description ?? '',
      instructions: row.instructions ?? '',
      license: optionalString(row.license),
      // Decode the serialized projection once, preserving strings such as "false".
      compatibility: parseOracleJson<SkillVersion['compatibility']>(row.compatibility),
      source: parseOracleJson<SkillVersion['source']>(row.source),
      references: parseOracleJson<string[]>(row.references),
      scripts: parseOracleJson<string[]>(row.scripts),
      assets: parseOracleJson<string[]>(row.assets),
      files: parseOracleJson<SkillVersion['files']>(row.files),
      metadata: parseOracleJson<SkillVersion['metadata']>(row.metadata),
      tree: parseOracleJson<SkillVersion['tree']>(row.tree),
      changedFields: parseOptionalStringArray(row.changedFields),
      changeMessage: Number(row.changeMessageIsNull) === 1 ? undefined : String(row.changeMessage ?? ''),
      createdAt: toDate(row.createdAt),
    };
  }

  private skillOrderColumn(field: string): string {
    return field === 'updatedAt' ? SKILL_UPDATED_AT : SKILL_CREATED_AT;
  }

  private versionOrderColumn(field: string): string {
    return field === 'createdAt' ? VERSION_CREATED_AT : VERSION_VERSION_NUMBER;
  }

  private validatePagination(page: number, perPageInput: number | false | undefined, defaultPerPage: number): void {
    if (page < 0) {
      throw new Error('page must be >= 0');
    }

    const perPage = normalizePerPage(perPageInput, defaultPerPage);
    const maxOffset = Number.MAX_SAFE_INTEGER / 2;
    if (perPageInput !== false && page * perPage > maxOffset) {
      throw new Error('page value too large');
    }
  }

  private table(tableName: string): string {
    return qualifyName(tableName, this.schemaName);
  }

  private indexName(indexName: string): string {
    return indexNameForTable(indexName, 'IDX');
  }

  private storageError(
    operation: string,
    reason: string,
    details: Record<string, string | number | boolean | undefined>,
    cause: unknown,
    category: ErrorCategory = ErrorCategory.THIRD_PARTY,
  ): MastraError {
    return createOracleStorageError({ storeName: STORE_NAME, operation, reason, details, cause, category });
  }
}

export function getDefaultSkillIndexDefinitions(indexName: (name: string) => string): OracleCreateIndexOptions[] {
  return [
    {
      name: indexName('MASTRA_SKILL_VERSIONS_SKILL_VERSION'),
      table: TABLE_SKILL_VERSIONS,
      columns: ['skillId', 'versionNumber'],
      unique: true,
    },
  ];
}

function optionalString(value: unknown): string | undefined {
  if (value === null || value === undefined) return undefined;
  return String(value);
}
