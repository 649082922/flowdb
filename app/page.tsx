"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

type DbType = "oracle" | "mysql" | "postgresql" | "tdsql";
type MigrationContent = "structure_and_data" | "structure_only" | "data_only";
type SyncMode =
  | "full_only"
  | "full_then_incremental"
  | "full_and_incremental"
  | "incremental_only";
type CdcNoKeyPolicy = "reject" | "all_columns";
type IdentifierCasePolicy = "auto" | "preserve" | "lower" | "upper";
type CaseCapabilities = {
  lower_case_table_names: number | null;
  table_name_case_sensitive: boolean | null;
};
type Config = {
  type: DbType;
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  schema_name: string;
};
type TableInfo = {
  schema_name: string | null;
  name: string;
  columns: number;
  primary_keys: string[];
  object_type: "table" | "partitioned_table" | "view" | "sequence";
};
type MigrationLink = {
  id: string;
  name: string;
  source: Omit<Config, "password"> & { has_password: boolean };
  target: Omit<Config, "password"> & { has_password: boolean };
  created_at: string;
  updated_at: string;
};

function restoredConfig(
  saved: MigrationLink["source"] | MigrationLink["target"],
): Config {
  const { has_password, ...config } = saved;
  void has_password;
  return { ...config, password: "" };
}
type Job = {
  id: string;
  name: string;
  link_id?: string | null;
  link_name?: string | null;
  source_type: DbType;
  target_type: DbType;
  status: string;
  progress: number;
  rows_copied: number;
  bytes_copied: number;
  current_table: string | null;
  tables_total: number;
  tables_completed: number;
  migration_content: MigrationContent;
  batch_size: number;
  table_concurrency: number;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  error: string | null;
  fail_policy?: "stop_on_error" | "continue_on_error";
  identifier_case_policy?: IdentifierCasePolicy;
  identifier_case_resolved?: Exclude<IdentifierCasePolicy, "auto">;
  target_lower_case_table_names?: number | null;
  sync_mode?: SyncMode;
  sync_phase?: string;
  start_scn?: number | null;
  checkpoint_scn?: number | null;
  source_current_scn?: number | null;
  cdc_lag?: number;
  cdc_events?: number;
  cdc_transactions?: number;
  cdc_inserts?: number;
  cdc_updates?: number;
  cdc_deletes?: number;
  cdc_started_at?: string | null;
  cdc_last_event_at?: string | null;
  table_results?: {
    table: string;
    target_table?: string | null;
    object_type?: string;
    status: "success" | "failed" | "cancelled" | "skipped";
    rows: number;
    bytes: number;
    error: string | null;
    notes?: string | string[] | null;
    started_at: string | null;
    finished_at: string | null;
    elapsed_ms: number;
  }[];
  phase_progress?: {
    phase: "sequence" | "table" | "partitioned_table" | "view";
    label: string;
    total: number;
    completed: number;
    failed: number;
    cancelled: number;
    running: number;
    pending: number;
    progress: number;
    current_objects: string[];
  }[];
};
type JobPhase = NonNullable<Job["phase_progress"]>[number];

const jobPhaseDefinitions: {
  phase: JobPhase["phase"];
  label: string;
  shortLabel: string;
}[] = [
  { phase: "sequence", label: "序列进度", shortLabel: "序" },
  { phase: "table", label: "普通表进度", shortLabel: "表" },
  { phase: "partitioned_table", label: "分区表进度", shortLabel: "区" },
  { phase: "view", label: "视图进度", shortLabel: "视" },
];

function normalizedJobPhases(job: Job): JobPhase[] {
  const livePhases = new Map(
    (job.phase_progress || []).map((phase) => [phase.phase, phase]),
  );

  return jobPhaseDefinitions.map((definition) => {
    const livePhase = livePhases.get(definition.phase);
    if (livePhase) return livePhase;

    const results = (job.table_results || []).filter((result) => {
      const resultType = result.object_type || "table";
      return resultType === definition.phase;
    });
    const completed = results.filter(
      (result) => result.status === "success",
    ).length;
    const failed = results.filter(
      (result) => result.status === "failed",
    ).length;
    const cancelled = results.filter(
      (result) => result.status === "cancelled" || result.status === "skipped",
    ).length;
    const finished = completed + failed + cancelled;

    return {
      phase: definition.phase,
      label: definition.label.replace("进度", "迁移"),
      total: results.length,
      completed,
      failed,
      cancelled,
      running: 0,
      pending: Math.max(results.length - finished, 0),
      progress: results.length ? (finished / results.length) * 100 : 0,
      current_objects: [],
    };
  });
}
type JobLogEntry = {
  seq: number;
  ts: string;
  level: string;
  message: string;
};
type JobEditTemplate = {
  id: string;
  name: string;
  link_id: string | null;
  link_name: string | null;
  tables: string[];
  object_types: Record<
    string,
    "table" | "partitioned_table" | "view" | "sequence"
  >;
  sequences: string[];
  migrate_sequences: boolean;
  batch_size: number;
  table_concurrency: number;
  existing_table: "fail" | "append" | "truncate" | "drop_and_create";
  create_tables: boolean;
  migration_content: MigrationContent;
  fail_policy: "stop_on_error" | "continue_on_error";
  user_mappings?: { source: string; target: string }[];
  source_type?: string;
  identifier_case_policy?: IdentifierCasePolicy;
  sync_mode?: SyncMode;
  start_scn?: number | null;
  cdc_poll_seconds?: number;
  cdc_key_overrides?: Record<string, string[]>;
  cdc_no_key_policy?: CdcNoKeyPolicy;
  cdc_allow_source_ddl?: boolean;
};
type UserMapping = { source: string; target: string };
type CanonicalValue = unknown[];
type ValidationDifference = {
  row_index: number;
  primary_key: Record<string, CanonicalValue>;
  columns: { column: string; source: CanonicalValue; target: CanonicalValue }[];
};
type ValidationTable = {
  table: string;
  target_table: string;
  name_case_preserved: boolean;
  source_rows: number;
  target_rows: number;
  row_count_equal: boolean;
  hash_mode: string;
  rows_hashed: number;
  source_sha256: string | null;
  target_sha256: string | null;
  hash_equal: boolean;
  difference_types: { type: string; message: string }[];
  difference_rows: number;
  column_difference_counts: Record<string, number>;
  difference_samples: ValidationDifference[];
  passed: boolean;
};
type ValidationResult = {
  passed: boolean;
  tables: ValidationTable[];
  max_hash_rows: number;
  concurrency?: number;
  duration_ms?: number;
};
type ValidationRecord = ValidationResult & {
  id: string;
  job_id: string;
  job_name: string;
  created_at: string;
  table_count: number;
  consistent_count: number;
  inconsistent_count: number;
};
type ValidationHistoryItem = Pick<
  ValidationRecord,
  | "id"
  | "job_id"
  | "job_name"
  | "created_at"
  | "passed"
  | "table_count"
  | "consistent_count"
  | "inconsistent_count"
>;
type ValidationHistoryPage = {
  items: ValidationHistoryItem[];
  total: number;
  page: number;
  page_size: number;
  pages: number;
};
type JobsPage = {
  items: Job[];
  total: number;
  page: number;
  page_size: number;
  pages: number;
};
type AssessmentRisk = {
  level: "blocking" | "warning";
  code: string;
  message: string;
};
type AssessmentTable = {
  table: string;
  rows: number;
  columns: number;
  primary_keys: string[];
  estimated_bytes: number;
  target_exists: boolean;
  target_name: string | null;
  planned_target_name?: string;
  blocking_count: number;
  warning_count: number;
  risks: AssessmentRisk[];
  column_mappings: {
    column: string;
    source_type: string;
    target_type: string;
    nullable: boolean;
    identity: boolean;
    degraded?: boolean | null;
    degradation?: string | null;
  }[];
};
type Assessment = {
  ready: boolean;
  score: number;
  summary: {
    tables: number;
    rows: number;
    estimated_bytes: number;
    blocking: number;
    warnings: number;
    batch_size: number;
    table_concurrency: number;
  };
  tables: AssessmentTable[];
};
type DeepEnv = {
  dialect: DbType;
  version?: string | null;
  host?: string | null;
  database?: string | null;
  port?: number | null;
  startup_time?: string | null;
  status?: string | null;
  run_mode?: string | null;
  created?: string | null;
  log_mode?: string | null;
  open_mode?: string | null;
  platform_name?: string | null;
  version_comment?: string | null;
  uptime_seconds?: number | null;
  host_resources?: { cpu_cores?: number; memory_bytes?: number } | null;
  charset?: string | null;
  collation?: string | null;
  parameters: Record<string, string | null>;
  notes?: { section: string; message: string }[];
};
type DeepCounts = {
  tables?: number | null;
  views?: number | null;
  sequences?: number | null;
  synonyms?: number | null;
  dblinks?: number | null;
  procedures?: number | null;
  functions?: number | null;
  packages?: number | null;
  triggers?: number | null;
  materialized_views?: number | null;
  indexes?: number | null;
  constraints?: number | null;
  partitioned_tables?: number | null;
  scheduler_jobs?: number | null;
  events?: number | null;
};
type DeepObjects = {
  scope?: string | null;
  current_user?: string | null;
  counts: DeepCounts;
  details?: DeepObjectDetails | null;
};
type DeepDetailList = {
  items: Record<string, unknown>[];
  truncated: boolean;
  total?: number | null;
};
type DeepObjectDetails = {
  scope?: string | null;
  sequences?: DeepDetailList | null;
  synonyms?: DeepDetailList | null;
  dblinks?: DeepDetailList | null;
  procedures?: DeepDetailList | null;
  triggers?: DeepDetailList | null;
};
type DeepColumnMapping = {
  column: string;
  source_type: string;
  target_type: string;
  degraded?: boolean | null;
  degradation?: string | null;
};
type DeepTopTable = {
  table: string;
  size_bytes?: number | null;
  rows_estimate?: number | null;
  column_count?: number | null;
  has_pk?: boolean | null;
  partitioned?: boolean | null;
  column_mappings?: DeepColumnMapping[] | null;
};
type DeepLobTable = {
  table: string;
  columns: { column: string; type: string }[];
};
type DeepData = {
  scope?: string | null;
  total_bytes?: number | null;
  total_rows_estimate?: number | null;
  top_tables?: DeepTopTable[] | null;
  lob_tables?: DeepLobTable[] | null;
  empty_table_count?: number | null;
  no_pk_tables?: string[] | null;
};
type DeepQualityChecks = {
  null_rate: { column: string; null_rate: number }[];
  duplicates: {
    exists: boolean;
    key?: string | null;
    columns?: string[] | null;
  } | null;
  overlong: { column: string; source_length: number; target_type: string }[];
  encoding: { column: string; issue: string }[];
  unique_conflict: { risk: string; message: string } | null;
};
type DeepQualityTable = {
  table: string;
  rows?: number | null;
  rows_estimate?: number | null;
  risks: DeepRisk[];
  checks: DeepQualityChecks;
};
type DeepDataQuality = {
  scope?: string | null;
  checked_count: number;
  tables: DeepQualityTable[];
};
type DeepFkDependency = {
  child_table: string;
  parent_table: string;
  constraint_name?: string | null;
};
type DeepFk = {
  scope?: string | null;
  count?: number | null;
  dependencies?: DeepFkDependency[] | null;
};
type DeepComparisonItem = {
  key: string;
  label: string;
  category: string;
  source?: string | null;
  target?: string | null;
  risk: "info" | "warning" | "blocking" | null;
};
type DeepRisk = {
  level: "blocking" | "warning" | "info";
  category: string;
  message: string;
};
type DeepSide = {
  env: DeepEnv;
  objects: DeepObjects;
  data: DeepData;
  foreign_keys: DeepFk;
  connect_error?: string | null;
};
type DeepTimeTable = {
  table: string;
  rows_estimate?: number | null;
  size_bytes?: number | null;
  transfer_seconds: number;
  copy_seconds: number;
  total_seconds: number;
};
type DeepTimeEstimate = {
  per_table: DeepTimeTable[];
  summary: {
    total_bytes?: number | null;
    total_rows?: number | null;
    bandwidth_mbps: number;
    optimistic_seconds?: number | null;
    pessimistic_seconds?: number | null;
    optimistic?: string | null;
    pessimistic?: string | null;
  };
  assumptions: string[];
};
type DeepConclusionAction = {
  priority: "P0" | "P1" | "P2";
  owner: string;
  task: string;
};
type DeepConclusionDimension = {
  name: string;
  level: "ok" | "warning" | "blocking";
  summary: string;
  action_items: DeepConclusionAction[];
};
type DeepConclusion = {
  overall: { ready: boolean; score: number; statement: string };
  dimensions: DeepConclusionDimension[];
};
type DeepSecurity = {
  accounts?: {
    total?: number | null;
    items: Record<string, unknown>[];
    truncated?: boolean;
  } | null;
  roles?: {
    total?: number | null;
    items: Record<string, unknown>[];
    truncated?: boolean;
  } | null;
  system_privileges?: {
    total?: number | null;
    items: Record<string, unknown>[];
    truncated?: boolean;
  } | null;
  object_privileges?: {
    total?: number | null;
    items: Record<string, unknown>[];
    truncated?: boolean;
  } | null;
  sensitive_accounts?: string[] | null;
  security_settings?: Record<string, unknown> | null;
  settings?: { items: Record<string, unknown>[]; truncated?: boolean } | null;
  notes?: string[] | null;
};
type DeepPerformance = {
  level: string;
  source_cpu_cores?: number | null;
  source_memory_bytes?: number | null;
  max_table_bytes?: number | null;
  recommended_table_concurrency?: number | null;
  advice?: string[];
  low_peak_advice?: string | null;
  rationale?: string | null;
};
type DeepExportResult = {
  file_name: string;
  file_path: string;
  download_url: string;
};
type DeepAssessment = {
  generated_at: string;
  source: DeepSide;
  target: DeepSide;
  parameter_comparison: DeepComparisonItem[];
  object_stats: { source: DeepObjects; target: DeepObjects };
  data_analysis: { source: DeepData; target: DeepData };
  foreign_keys: { source: DeepFk; target: DeepFk };
  data_quality: DeepDataQuality;
  time_estimate: DeepTimeEstimate;
  conclusion: DeepConclusion;
  security: DeepSecurity;
  performance: DeepPerformance;
  risks: DeepRisk[];
  summary: { blocking: number; warnings: number; info: number };
  score: number;
  ready: boolean;
  suggestions: string[];
  notes: { side: "source" | "target"; section: string; message: string }[];
  partition_analysis?: {
    partitioned_total?: number;
    interval_tables?: string[];
    by_type?: Record<string, number>;
    downgrades?: string[];
  };
};
type ApiErrorDetail = {
  message?: string;
  code?: string;
  cause?: string;
  hint?: string;
};

const dbMeta: Record<
  DbType,
  { label: string; mark: string; port: number; sample: string; tone: string }
> = {
  oracle: {
    label: "Oracle",
    mark: "O",
    port: 1521,
    sample: "ORCL",
    tone: "oracle",
  },
  mysql: {
    label: "MySQL",
    mark: "M",
    port: 3306,
    sample: "app_db",
    tone: "mysql",
  },
  postgresql: {
    label: "PostgreSQL",
    mark: "P",
    port: 5432,
    sample: "postgres",
    tone: "pg",
  },
  tdsql: {
    label: "TDSQL",
    mark: "T",
    port: 3306,
    sample: "tdsqlpcloud",
    tone: "mysql",
  },
};

const emptyConfig = (type: DbType): Config => ({
  type,
  host: "",
  port: dbMeta[type].port,
  database: "",
  username: "",
  password: "",
  schema_name: "",
});
const contentLabels: Record<MigrationContent, string> = {
  structure_and_data: "表结构＋数据",
  structure_only: "仅表结构",
  data_only: "仅数据",
};
function parseCdcKeyOverrides(value: string): Record<string, string[]> {
  const result: Record<string, string[]> = {};
  for (const rawLine of value.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) continue;
    const separator = line.indexOf("=");
    if (separator <= 0 || separator === line.length - 1) {
      throw new Error(`业务唯一键格式错误：${line}；请使用 表名=字段1,字段2`);
    }
    const table = line.slice(0, separator).trim();
    const columns = line
      .slice(separator + 1)
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
    if (!columns.length || new Set(columns.map((item) => item.toUpperCase())).size !== columns.length) {
      throw new Error(`业务唯一键字段为空或重复：${line}`);
    }
    result[table] = columns;
  }
  return result;
}
function formatCdcKeyOverrides(value?: Record<string, string[]>): string {
  return Object.entries(value || {})
    .map(([table, columns]) => `${table}=${columns.join(",")}`)
    .join("\n");
}
const objectCountLabels: Record<string, string> = {
  tables: "表",
  views: "视图",
  sequences: "序列",
  synonyms: "同义词",
  dblinks: "DBLINK",
  procedures: "存储过程",
  functions: "函数",
  packages: "包",
  triggers: "触发器",
  materialized_views: "物化视图",
  indexes: "索引",
  constraints: "约束",
  partitioned_tables: "分区表",
  scheduler_jobs: "定时任务",
  events: "事件",
};
function objectCountLabel(key: string) {
  return objectCountLabels[key] ?? key;
}
const detailValue = (value: unknown): string => {
  if (value === null || value === undefined) return "—";
  const text = String(value);
  return text.length > 60 ? `${text.slice(0, 60)}…` : text;
};
function DeepDetailBlock({ detail }: { detail?: DeepDetailList | null }) {
  if (!detail) {
    return (
      <p className="deep-unavailable">不可用（查询失败或当前方言不支持）</p>
    );
  }
  if (!detail.items.length) {
    return <p className="deep-empty">无记录</p>;
  }
  const keys = Object.keys(detail.items[0]);
  const columns = `repeat(${keys.length}, minmax(80px, 1fr))`;
  return (
    <div className="deep-table">
      <div
        className="deep-detail-row deep-table-head"
        style={{ gridTemplateColumns: columns }}
      >
        {keys.map((key) => (
          <b key={key}>{key}</b>
        ))}
      </div>
      {detail.items.map((item, index) => (
        <div
          className="deep-detail-row"
          key={`${index}-${detailValue(item[keys[0]])}`}
          style={{ gridTemplateColumns: columns }}
        >
          {keys.map((key) => (
            <span key={key}>{detailValue(item[key])}</span>
          ))}
        </div>
      ))}
      {detail.truncated && (
        <p className="deep-note">
          已截断，仅显示前 {detail.items.length} 条（总计约{" "}
          {detail.total ?? "未知"} 条）
        </p>
      )}
    </div>
  );
}
function DeepQualityBlock({ quality }: { quality?: DeepDataQuality | null }) {
  if (!quality) {
    return <p className="deep-unavailable">数据质量预检不可用</p>;
  }
  if (!quality.tables.length) {
    return (
      <p className="deep-empty">
        未执行预检（无可检查表或源端信息不可用，详见降级说明）
      </p>
    );
  }
  return (
    <div className="deep-quality-list">
      {quality.tables.map((table) => (
        <div className="deep-quality-card" key={table.table}>
          <b>{table.table}</b>
          <span>
            {table.rows != null
              ? `${formatRows(table.rows)}`
              : table.rows_estimate != null
                ? `约 ${formatRows(table.rows_estimate)}`
                : "行数未知"}
          </span>
          <div className="deep-quality-checks">
            {table.checks.null_rate.map((item) => (
              <span key={`null-${item.column}`} className="deep-tag warn">
                空值率 {Math.round(item.null_rate * 100)}% · {item.column}
              </span>
            ))}
            {table.checks.duplicates?.exists ? (
              <span className="deep-tag warn">
                重复记录 · {table.checks.duplicates.key ?? "唯一键"}
              </span>
            ) : (
              <span className="deep-tag ok">无重复</span>
            )}
            {table.checks.overlong.map((item) => (
              <span key={`over-${item.column}`} className="deep-tag warn">
                超长 · {item.column} ({item.source_length})
              </span>
            ))}
            {table.checks.encoding.map((item) => (
              <span key={`enc-${item.column}`} className="deep-tag warn">
                编码异常 · {item.column}
              </span>
            ))}
            {table.checks.unique_conflict && (
              <span className="deep-tag warn">目标冲突预判</span>
            )}
            {table.risks.length === 0 && (
              <span className="deep-empty">未发现风险</span>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}
function DeepColumnMappings({ table }: { table: DeepTopTable }) {
  if (!table.column_mappings || !table.column_mappings.length) return null;
  return (
    <details className="deep-mappings">
      <summary>列类型映射预览（{table.column_mappings.length} 列）</summary>
      <div className="deep-table">
        <div className="deep-table-row deep-table-head">
          <b>列</b>
          <b>源类型</b>
          <b>目标类型</b>
          <b>降级</b>
        </div>
        {table.column_mappings.map((column) => (
          <div className="deep-table-row" key={column.column}>
            <span>{column.column}</span>
            <span>{column.source_type}</span>
            <span>{column.target_type}</span>
            <span>{column.degraded ? (column.degradation ?? "是") : "否"}</span>
          </div>
        ))}
      </div>
    </details>
  );
}

function DatabaseBadge({
  type,
  small = false,
}: {
  type: DbType;
  small?: boolean;
}) {
  const meta = dbMeta[type];
  return (
    <span className={`db-badge ${meta.tone} ${small ? "small" : ""}`}>
      {meta.mark}
    </span>
  );
}

function formatRows(value: number) {
  return value >= 100000000
    ? `${(value / 100000000).toFixed(2)} 亿`
    : value >= 10000
      ? `${(value / 10000).toFixed(1)} 万`
      : value.toLocaleString("zh-CN");
}
function formatBytes(value: number) {
  return value >= 1073741824
    ? `${(value / 1073741824).toFixed(2)} GB`
    : value >= 1048576
      ? `${(value / 1048576).toFixed(1)} MB`
      : `${(value / 1024).toFixed(1)} KB`;
}
function formatDuration(seconds?: number | null): string {
  if (seconds == null) return "—";
  if (seconds < 60) return `${Math.round(seconds)} 秒`;
  if (seconds < 3600) return `${(seconds / 60).toFixed(1)} 分钟`;
  if (seconds < 86400) return `${(seconds / 3600).toFixed(1)} 小时`;
  return `${(seconds / 86400).toFixed(1)} 天`;
}
function buildFkGraph(
  dependencies?:
    | {
        child_table: string;
        parent_table: string;
        constraint_name?: string | null;
      }[]
    | null,
): string[] {
  if (!dependencies || !dependencies.length) return [];
  const parentCount = new Map<string, number>();
  dependencies.forEach((dependency) => {
    parentCount.set(
      dependency.parent_table,
      (parentCount.get(dependency.parent_table) ?? 0) + 1,
    );
  });
  const childCount = new Map<string, number>();
  dependencies.forEach((dependency) => {
    childCount.set(
      dependency.child_table,
      (childCount.get(dependency.child_table) ?? 0) + 1,
    );
  });
  const isRoot = (name: string) => !childCount.has(name);
  const lines: string[] = [];
  const visited = new Set<string>();
  const walk = (name: string, depth: number) => {
    const children = dependencies.filter(
      (dependency) => dependency.parent_table === name,
    );
    lines.push(
      `${"  ".repeat(depth)}${name}${parentCount.get(name) ? `（${parentCount.get(name)} 个子表）` : ""}`,
    );
    visited.add(name);
    children.forEach((child) => {
      if (!visited.has(child.child_table)) {
        walk(child.child_table, depth + 1);
      }
    });
  };
  const roots = [...parentCount.keys()].filter((name) => isRoot(name));
  if (roots.length === 0 && parentCount.size > 0) {
    const first = dependencies[0].parent_table;
    walk(first, 0);
  } else {
    roots.forEach((root) => walk(root, 0));
  }
  return lines;
}
function displayCanonical(value: CanonicalValue) {
  const kind = String(value?.[0] ?? "");
  if (kind === "null") return "NULL";
  if (kind === "missing") return "（不存在）";
  if (kind === "bytes") return `二进制 ${value[1]} 字节，SHA-256 ${value[2]}`;
  return String(value?.[1] ?? value);
}

export function MigrationApp({
  initialPage = "workspace",
}: {
  initialPage?:
    | "workspace"
    | "datasources"
    | "links"
    | "tasks"
    | "validation"
    | "nodes"
    | "settings";
}) {
  const [source, setSource] = useState<Config>(() => emptyConfig("oracle"));
  const [target, setTarget] = useState<Config>(() => emptyConfig("postgresql"));
  const [connectionState, setConnectionState] = useState({
    source: "idle",
    target: "idle",
  });
  const [connectionErrors, setConnectionErrors] = useState({
    source: "",
    target: "",
  });
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [objectTab, setObjectTab] = useState<
    "table" | "partitioned_table" | "view" | "sequence"
  >("table");
  const [objectSearch, setObjectSearch] = useState("");
  const [selected, setSelected] = useState<string[]>([]);
  const [selectedSequences, setSelectedSequences] = useState<string[]>([]);
  const [migrateSequences, setMigrateSequences] = useState(true);
  const [userMappings, setUserMappings] = useState<UserMapping[]>([]);
  const [mappingDraft, setMappingDraft] = useState<UserMapping[]>([]);
  const [mappingModalOpen, setMappingModalOpen] = useState(false);
  const [mappingViewOpen, setMappingViewOpen] = useState(false);
  const [mappingError, setMappingError] = useState("");
  const [editUserMappings, setEditUserMappings] = useState<UserMapping[]>([]);
  const [editMappingDraft, setEditMappingDraft] = useState<UserMapping[]>([]);
  const [editMappingModalOpen, setEditMappingModalOpen] = useState(false);
  const [editMappingViewOpen, setEditMappingViewOpen] = useState(false);
  const [editMappingError, setEditMappingError] = useState("");
  const [ownerFilters, setOwnerFilters] = useState<string[]>([]);
  const [ownerList, setOwnerList] = useState<string[]>([]);
  const [showOwnerPicker, setShowOwnerPicker] = useState(false);
  const [loadingOwners, setLoadingOwners] = useState(false);
  const [showOwnerWarning, setShowOwnerWarning] = useState(false);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loadingTables, setLoadingTables] = useState(false);
  const [creating, setCreating] = useState(false);
  const [createdJobId, setCreatedJobId] = useState("");
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [apiBase, setApiBase] = useState("");
  const [apiToken, setApiToken] = useState("");
  const [taskName, setTaskName] = useState("生产库全量迁移");
  const [existingTable, setExistingTable] = useState("fail");
  const [failPolicy, setFailPolicy] = useState<
    "stop_on_error" | "continue_on_error"
  >("stop_on_error");
  const [identifierCasePolicy, setIdentifierCasePolicy] =
    useState<IdentifierCasePolicy>("auto");
  const [targetCaseCapabilities, setTargetCaseCapabilities] =
    useState<CaseCapabilities | null>(null);
  const [migrationContent, setMigrationContent] =
    useState<MigrationContent>("structure_and_data");
  const [syncMode, setSyncMode] = useState<SyncMode>("full_only");
  const [cdcBusinessKeysText, setCdcBusinessKeysText] = useState("");
  const [cdcNoKeyPolicy, setCdcNoKeyPolicy] =
    useState<CdcNoKeyPolicy>("reject");
  const [cdcAllowSourceDdl, setCdcAllowSourceDdl] = useState(false);
  const [cdcCapability, setCdcCapability] = useState<{
    ready: boolean;
    log_mode: string;
    supplemental_log_data_min: string;
    supplemental_log_data_pk: string;
    current_scn: number;
  } | null>(null);
  const [batchSize, setBatchSize] = useState(2000);
  const [tableConcurrency, setTableConcurrency] = useState(1);
  const [jobRefreshSeconds, setJobRefreshSeconds] = useState(3);
  const [lastNodeCheck, setLastNodeCheck] = useState("尚未检测");
  const [testingNode, setTestingNode] = useState(false);
  const [nodeTestResult, setNodeTestResult] = useState<{
    status: "success" | "error";
    message: string;
  } | null>(null);
  const [assessment, setAssessment] = useState<Assessment | null>(null);
  const [assessing, setAssessing] = useState(false);
  const [assessmentError, setAssessmentError] = useState("");
  const assessmentController = useRef<AbortController | null>(null);
  const [showAssessment, setShowAssessment] = useState(false);
  const [deepAssessment, setDeepAssessment] = useState<DeepAssessment | null>(
    null,
  );
  const [deepAssessing, setDeepAssessing] = useState(false);
  const [deepAssessmentError, setDeepAssessmentError] = useState("");
  const deepController = useRef<AbortController | null>(null);
  const [showDeepAssessment, setShowDeepAssessment] = useState(false);
  const [deepBandwidthMbps, setDeepBandwidthMbps] = useState(50);
  const [deepExporting, setDeepExporting] = useState(false);
  const [deepExport, setDeepExport] = useState<DeepExportResult | null>(null);
  const [jobPage, setJobPage] = useState(1);
  const [jobPageSize, setJobPageSize] = useState(10);
  const [jobTotal, setJobTotal] = useState(0);
  const [jobPages, setJobPages] = useState(1);
  const [openJobMenu, setOpenJobMenu] = useState("");
  const [selectedJob, setSelectedJob] = useState<Job | null>(null);
  const [cancelConfirmJob, setCancelConfirmJob] = useState<Job | null>(null);
  const [cancellingJobId, setCancellingJobId] = useState("");
  const [finishConfirmJob, setFinishConfirmJob] = useState<Job | null>(null);
  const [finishingJobId, setFinishingJobId] = useState("");
  const [resumeConfirmJob, setResumeConfirmJob] = useState<Job | null>(null);
  const [resumingJobId, setResumingJobId] = useState("");
  const [tableStatusFilter, setTableStatusFilter] = useState("all");
  const [jobLogs, setJobLogs] = useState<JobLogEntry[]>([]);
  const jobLogSeqRef = useRef(0);
  const jobLogViewRef = useRef<HTMLPreElement | null>(null);
  const [jobLogAutoScroll, setJobLogAutoScroll] = useState(true);
  const [editTemplate, setEditTemplate] = useState<JobEditTemplate | null>(
    null,
  );
  const [editName, setEditName] = useState("");
  const [editSelected, setEditSelected] = useState<string[]>([]);
  const [editSequences, setEditSequences] = useState<string[]>([]);
  const [editSequenceSearch, setEditSequenceSearch] = useState("");
  const [editTableSearch, setEditTableSearch] = useState("");
  const [editViewSearch, setEditViewSearch] = useState("");
  const [editMigrateSequences, setEditMigrateSequences] = useState(true);
  const [editBatchSize, setEditBatchSize] = useState(2000);
  const [editConcurrency, setEditConcurrency] = useState(1);
  const [editExistingTable, setEditExistingTable] = useState("fail");
  const [editContent, setEditContent] =
    useState<MigrationContent>("structure_and_data");
  const [editFailPolicy, setEditFailPolicy] = useState<
    "stop_on_error" | "continue_on_error"
  >("stop_on_error");
  const [editIdentifierCasePolicy, setEditIdentifierCasePolicy] =
    useState<IdentifierCasePolicy>("auto");
  const [editCdcBusinessKeysText, setEditCdcBusinessKeysText] = useState("");
  const [editCdcNoKeyPolicy, setEditCdcNoKeyPolicy] =
    useState<CdcNoKeyPolicy>("reject");
  const [editCdcAllowSourceDdl, setEditCdcAllowSourceDdl] = useState(false);
  const [editSaving, setEditSaving] = useState(false);
  const [activeNav] = useState(initialPage);
  const [links, setLinks] = useState<MigrationLink[]>([]);
  const [linkName, setLinkName] = useState("");
  const [activeLinkId, setActiveLinkId] = useState("");
  const [linkWizardStep, setLinkWizardStep] = useState<1 | 2 | 3>(1);
  const [editingLinkId, setEditingLinkId] = useState("");
  const [savingLink, setSavingLink] = useState(false);
  const [nodeOnline, setNodeOnline] = useState(false);
  const [validationJob, setValidationJob] = useState("");
  const [validating, setValidating] = useState(false);
  const [validationResult, setValidationResult] =
    useState<ValidationRecord | null>(null);
  const [validationTableFilter, setValidationTableFilter] = useState<
    "all" | "passed" | "failed"
  >("all");
  const [validationHistory, setValidationHistory] = useState<
    ValidationHistoryItem[]
  >([]);
  const [validationHistoryTotal, setValidationHistoryTotal] = useState(0);
  const [validationHistoryPage, setValidationHistoryPage] = useState(1);
  const [validationHistoryPages, setValidationHistoryPages] = useState(1);
  const [validationHistoryPageSize, setValidationHistoryPageSize] =
    useState(10);
  const [validationHistoryFilter, setValidationHistoryFilter] = useState<
    "all" | "passed" | "failed"
  >("all");
  const [loadingValidationRecord, setLoadingValidationRecord] = useState("");

  useEffect(() => {
    const timer = window.setTimeout(() => {
      setApiBase(window.localStorage.getItem("flowdb_api") || "");
      setApiToken(window.localStorage.getItem("flowdb_token") || "");
      setBatchSize(
        Number(window.localStorage.getItem("flowdb_default_batch")) || 2000,
      );
      setTableConcurrency(
        Number(window.localStorage.getItem("flowdb_default_concurrency")) || 1,
      );
      setJobRefreshSeconds(
        Number(window.localStorage.getItem("flowdb_refresh_seconds")) || 3,
      );
    }, 0);
    return () => window.clearTimeout(timer);
  }, []);
  const api = useCallback(
    async <T,>(path: string, options?: RequestInit): Promise<T> => {
      const response = await fetch(
        `${apiBase.trim().replace(/\/$/, "")}${path}`,
        {
          ...options,
          headers: {
            "Content-Type": "application/json",
            ...(apiToken.trim() ? { "X-FlowDB-Token": apiToken.trim() } : {}),
            ...(options?.headers || {}),
          },
        },
      );
      const body = (await response.json().catch(() => ({}))) as {
        detail?: string | ApiErrorDetail;
      };
      if (!response.ok) {
        const detail = body.detail;
        if (typeof detail === "object" && detail) {
          const parts = [
            detail.code,
            detail.message,
            detail.cause,
            detail.hint,
          ].filter(Boolean);
          throw new Error(
            parts.join(" · ") || `请求失败 (HTTP ${response.status})`,
          );
        }
        if (typeof detail === "string") throw new Error(detail);
        if (response.status === 404)
          throw new Error(
            "迁移节点接口不存在（HTTP 404）。请在系统设置中填写已部署的 FlowDB API 地址。",
          );
        throw new Error(`迁移节点请求失败（HTTP ${response.status}）`);
      }
      return body as T;
    },
    [apiBase, apiToken],
  );

  const refreshJobs = useCallback(async () => {
    try {
      const result = await api<JobsPage>(
        `/api/jobs/page?page=${jobPage}&page_size=${jobPageSize}`,
      );
      setJobs(result.items);
      setJobTotal(result.total);
      setJobPages(result.pages);
      if (result.page !== jobPage) setJobPage(result.page);
      setNodeOnline(true);
      setLastNodeCheck(new Date().toLocaleTimeString("zh-CN"));
    } catch {
      setNodeOnline(false);
      setLastNodeCheck(new Date().toLocaleTimeString("zh-CN"));
    }
  }, [api, jobPage, jobPageSize]);
  const testNodeConnection = useCallback(async () => {
    setTestingNode(true);
    setNodeTestResult(null);
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 10000);
    const startedAt = performance.now();
    try {
      await api<JobsPage>("/api/jobs/page?page=1&page_size=10", {
        signal: controller.signal,
      });
      const latency = Math.max(1, Math.round(performance.now() - startedAt));
      setNodeOnline(true);
      setLastNodeCheck(new Date().toLocaleTimeString("zh-CN"));
      setNodeTestResult({
        status: "success",
        message: `连接成功，节点与访问令牌均有效（${latency} ms）`,
      });
    } catch (reason) {
      const message =
        reason instanceof DOMException && reason.name === "AbortError"
          ? "连接超时（10 秒），请检查节点地址、端口和防火墙"
          : reason instanceof TypeError
            ? "无法访问节点，请检查 API 地址、HTTPS 证书和网络"
            : reason instanceof Error
              ? reason.message
              : "未知连接错误";
      setNodeOnline(false);
      setLastNodeCheck(new Date().toLocaleTimeString("zh-CN"));
      setNodeTestResult({ status: "error", message: `连接失败：${message}` });
    } finally {
      window.clearTimeout(timeout);
      setTestingNode(false);
    }
  }, [api]);
  const refreshLinks = useCallback(async () => {
    try {
      setLinks(await api<MigrationLink[]>("/api/links"));
    } catch {
      /* node status is handled by job refresh */
    }
  }, [api]);
  const refreshValidationHistory = useCallback(async () => {
    try {
      const result = await api<ValidationHistoryPage>(
        `/api/validations/page?page=${validationHistoryPage}&page_size=${validationHistoryPageSize}&status=${validationHistoryFilter}`,
      );
      setValidationHistory(result.items);
      setValidationHistoryTotal(result.total);
      setValidationHistoryPages(result.pages);
      if (result.page !== validationHistoryPage) {
        setValidationHistoryPage(result.page);
      }
    } catch {
      setValidationHistory([]);
      setValidationHistoryTotal(0);
      setValidationHistoryPages(1);
    }
  }, [
    api,
    validationHistoryFilter,
    validationHistoryPage,
    validationHistoryPageSize,
  ]);
  useEffect(() => {
    const initial = window.setTimeout(refreshJobs, 0);
    const timer = window.setInterval(
      refreshJobs,
      Math.max(1, jobRefreshSeconds) * 1000,
    );
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(timer);
    };
  }, [refreshJobs, jobRefreshSeconds]);
  useEffect(() => {
    const timer = window.setTimeout(refreshLinks, 0);
    return () => window.clearTimeout(timer);
  }, [refreshLinks]);
  useEffect(() => {
    if (activeNav !== "validation") return;
    const timer = window.setTimeout(refreshValidationHistory, 0);
    return () => window.clearTimeout(timer);
  }, [activeNav, refreshValidationHistory]);
  useEffect(() => {
    if (!selectedJob) return;
    let stopped = false;
    let timer: number | undefined;
    const poll = async () => {
      try {
        const result = await api<{ logs: JobLogEntry[]; after_seq: number }>(
          `/api/jobs/${selectedJob.id}/logs?after_seq=${jobLogSeqRef.current}`,
        );
        if (stopped) return;
        if (result.logs.length) {
          setJobLogs((current) => [...current, ...result.logs]);
          jobLogSeqRef.current = result.after_seq;
        }
        const latest = await api<Job>(`/api/jobs/${selectedJob.id}`);
        if (!stopped) setSelectedJob(latest);
      } catch {
        /* keep the previous logs when the node is temporarily unreachable */
      }
    };
    const initial = window.setTimeout(() => {
      setJobLogs([]);
      setJobLogAutoScroll(true);
      jobLogSeqRef.current = 0;
      void poll();
      timer = window.setInterval(poll, 2000);
    }, 0);
    return () => {
      stopped = true;
      window.clearTimeout(initial);
      if (timer !== undefined) window.clearInterval(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedJob?.id]);
  useEffect(() => {
    if (!jobLogAutoScroll || !jobLogViewRef.current) return;
    const frame = window.requestAnimationFrame(() => {
      const view = jobLogViewRef.current;
      if (view) view.scrollTop = view.scrollHeight;
    });
    return () => window.cancelAnimationFrame(frame);
  }, [jobLogs, jobLogAutoScroll]);
  useEffect(() => {
    if (activeNav !== "tasks" || !window.location.hash.startsWith("#job-"))
      return;
    const target = document.getElementById(window.location.hash.slice(1));
    target?.scrollIntoView({ block: "center", behavior: "smooth" });
  }, [activeNav, jobs]);
  useEffect(() => {
    if (activeNav !== "datasources" || !links.length) return;
    const editId = new URLSearchParams(window.location.search).get("edit");
    if (!editId || editingLinkId === editId) return;
    const link = links.find((item) => item.id === editId);
    if (!link) return;
    const applySavedLink = window.setTimeout(() => {
      setEditingLinkId(link.id);
      setActiveLinkId("");
      setLinkName(link.name);
      setSource(restoredConfig(link.source));
      setTarget(restoredConfig(link.target));
      setConnectionState({ source: "idle", target: "idle" });
    }, 0);
    return () => window.clearTimeout(applySavedLink);
  }, [activeNav, editingLinkId, links]);

  const updateConfig = (
    side: "source" | "target",
    key: keyof Config,
    value: string | number,
  ) => {
    const setter = side === "source" ? setSource : setTarget;
    setter((current) => ({ ...current, [key]: value }));
    setConnectionState((current) => ({ ...current, [side]: "idle" }));
    setConnectionErrors((current) => ({ ...current, [side]: "" }));
    if (side === "source") {
      setTables([]);
      setSelected([]);
    }
    setActiveLinkId("");
  };
  const chooseType = (side: "source" | "target", type: DbType) => {
    const current = side === "source" ? source : target;
    const next = {
      ...current,
      type,
      port: dbMeta[type].port,
      database: "",
      schema_name: "",
    };
    if (side === "source") setSource(next);
    else setTarget(next);
    setActiveLinkId("");
    setConnectionState((state) => ({ ...state, [side]: "idle" }));
    setConnectionErrors((state) => ({ ...state, [side]: "" }));
  };
  const cleanConfig = (config: Config) => ({
    ...config,
    schema_name: config.schema_name || null,
  });

  async function testConnection(side: "source" | "target") {
    const config = side === "source" ? source : target;
    setError("");
    setConnectionErrors((state) => ({ ...state, [side]: "" }));
    setConnectionState((state) => ({ ...state, [side]: "testing" }));
    if (!apiBase && window.location.hostname.endsWith("chatgpt.site")) {
      setConnectionState((state) => ({ ...state, [side]: "failed" }));
      setConnectionErrors((state) => ({
        ...state,
        [side]:
          "未配置迁移节点 API。当前是在线管理页面，不能直接访问 192.168.x.x 内网数据库；请点击右上角设置，填写部署在数据库可达网络中的 FlowDB API HTTPS 地址和访问令牌。",
      }));
      return;
    }
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 10000);
    try {
      const result = await api<{ latency_ms: number } & CaseCapabilities>(
        "/api/connections/test",
        {
          method: "POST",
          signal: controller.signal,
          body: JSON.stringify({ connection: cleanConfig(config) }),
        },
      );
      setConnectionState((state) => ({
        ...state,
        [side]: `ok:${result.latency_ms}`,
      }));
      if (side === "target") {
        setTargetCaseCapabilities({
          lower_case_table_names: result.lower_case_table_names,
          table_name_case_sensitive: result.table_name_case_sensitive,
        });
        if (
          result.lower_case_table_names === 1 &&
          identifierCasePolicy === "preserve"
        ) {
          setIdentifierCasePolicy("auto");
          setNotice("目标实例以小写存储表名，已切换为自动适配");
        }
      }
    } catch (reason) {
      const message =
        reason instanceof DOMException && reason.name === "AbortError"
          ? "连接迁移节点超时（10 秒）。请检查 API 地址、HTTPS、网络、防火墙以及迁移节点是否启动。"
          : reason instanceof TypeError
            ? "无法访问迁移节点。请检查 API 地址、HTTPS 证书、CORS 配置和节点网络。"
            : reason instanceof Error
              ? reason.message
              : "未知连接错误";
      setConnectionState((state) => ({ ...state, [side]: "failed" }));
      setConnectionErrors((state) => ({ ...state, [side]: message }));
    } finally {
      window.clearTimeout(timeout);
    }
  }

  async function loadTables(): Promise<boolean> {
    setLoadingTables(true);
    setError("");
    try {
      const result = activeLinkId
        ? await api<TableInfo[]>(
            `/api/links/${activeLinkId}/objects${
              ownerFilters.length
                ? `?${ownerFilters
                    .map((item) => `owners=${encodeURIComponent(item)}`)
                    .join("&")}`
                : ""
            }`,
          )
        : await api<TableInfo[]>("/api/connections/objects", {
            method: "POST",
            body: JSON.stringify({
              connection: cleanConfig(source),
              owners: ownerFilters.length ? ownerFilters : null,
            }),
          });
      setTables(result);
      setSelected(
        result
          .filter(
            (item) =>
              item.object_type === "table" ||
              item.object_type === "partitioned_table",
          )
          .map((item) => item.name),
      );
      setSelectedSequences(
        result
          .filter((item) => item.object_type === "sequence")
          .map((item) => item.name),
      );
      return true;
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "读取表和视图失败");
      return false;
    } finally {
      setLoadingTables(false);
    }
  }

  async function loadOwners() {
    if (!activeLinkId) {
      setError("请先保存或选择迁移链路后再筛选 owner");
      return;
    }
    setLoadingOwners(true);
    setError("");
    try {
      const owners = activeLinkId
        ? await api<string[]>(`/api/links/${activeLinkId}/owners`)
        : await api<string[]>("/api/connections/owners", {
            method: "POST",
            body: JSON.stringify({ connection: cleanConfig(source) }),
          });
      setOwnerList(owners);
      setShowOwnerPicker(true);
    } catch (reason) {
      setError(
        reason instanceof Error ? reason.message : "读取 owner 列表失败",
      );
    } finally {
      setLoadingOwners(false);
    }
  }

  function toggleOwner(owner: string) {
    setOwnerFilters((current) =>
      current.includes(owner)
        ? current.filter((item) => item !== owner)
        : [...current, owner],
    );
    setTables([]);
    setSelected([]);
    setSelectedSequences([]);
    setShowOwnerWarning(false);
  }

  function clearOwners() {
    setOwnerFilters([]);
    setTables([]);
    setSelected([]);
    setSelectedSequences([]);
    setShowOwnerPicker(false);
    setShowOwnerWarning(false);
  }

  const migrationPayload = () => ({
    name: taskName,
    source: activeLinkId ? null : cleanConfig(source),
    target: activeLinkId ? null : cleanConfig(target),
    link_id: activeLinkId || null,
    link_name: links.find((item) => item.id === activeLinkId)?.name || null,
    tables: selected,
    object_types: Object.fromEntries(
      tables
        .filter((item) => selected.includes(item.name))
        .map((item) => [item.name, item.object_type]),
    ),
    sequences: migrateSequences ? selectedSequences : [],
    migrate_sequences: migrateSequences,
    user_mappings: userMappings
      .filter((item) => item.source.trim() && item.target.trim())
      .map((item) => ({
        source: item.source.trim(),
        target: item.target.trim(),
      })),
    batch_size: Math.max(100, Math.min(20000, batchSize || 100)),
    table_concurrency: Math.max(1, Math.min(16, tableConcurrency || 1)),
    existing_table: existingTable,
    create_tables: migrationContent !== "data_only",
    migration_content: migrationContent,
    sync_mode: syncMode,
    cdc_poll_seconds: 3,
    cdc_key_overrides:
      syncMode === "full_only" ? {} : parseCdcKeyOverrides(cdcBusinessKeysText),
    cdc_no_key_policy: cdcNoKeyPolicy,
    cdc_allow_source_ddl: cdcAllowSourceDdl,
    fail_policy: failPolicy,
    identifier_case_policy: identifierCasePolicy,
  });

  async function startMigration() {
    if (!selected.length && !(migrateSequences && selectedSequences.length)) {
      setError("请至少选择一个迁移对象（表或序列）");
      return;
    }
    if (!activeLinkId) {
      setError("请先保存当前连接为唯一命名链路，或选择一个已保存链路");
      return;
    }
    assessmentController.current?.abort();
    assessmentController.current = null;
    setAssessing(false);
    setCreating(true);
    setCreatedJobId("");
    setError("");
    try {
      if (syncMode !== "full_only") {
        const capability = await api<{
          ready: boolean;
          log_mode: string;
          supplemental_log_data_min: string;
          supplemental_log_data_pk: string;
          current_scn: number;
        }>(`/api/links/${activeLinkId}/cdc-capabilities`);
        setCdcCapability(capability);
        if (!capability.ready) {
          throw new Error(
            `LogMiner 未就绪：ARCHIVELOG=${capability.log_mode}，最小补充日志=${capability.supplemental_log_data_min}`,
          );
        }
      }
      const job = await api<Job>("/api/jobs", {
        method: "POST",
        body: JSON.stringify(migrationPayload()),
      });
      setJobPage(1);
      setJobs((current) => [job, ...current].slice(0, jobPageSize));
      setJobTotal((value) => value + 1);
      setCreatedJobId(job.id);
      setNotice("迁移任务已生成，可立即前往迁移任务查看进度");
      setShowAssessment(false);
      window.setTimeout(() => setNotice(""), 3500);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "创建任务失败");
    } finally {
      setCreating(false);
    }
  }

  async function assessMigration() {
    if (!selected.length) {
      setError("请至少选择一张表后再评估");
      return;
    }
    if (!activeLinkId) {
      setError("请先保存或选择迁移链路后再评估");
      return;
    }
    assessmentController.current?.abort();
    const controller = new AbortController();
    assessmentController.current = controller;
    setAssessing(true);
    setError("");
    setAssessmentError("");
    setAssessment(null);
    setShowAssessment(true);
    try {
      const result = await api<Assessment>("/api/assessments", {
        method: "POST",
        signal: controller.signal,
        body: JSON.stringify(migrationPayload()),
      });
      setAssessment(result);
    } catch (reason) {
      if (!(reason instanceof DOMException && reason.name === "AbortError")) {
        setAssessmentError(
          reason instanceof Error ? reason.message : "迁移前评估失败",
        );
      }
    } finally {
      if (assessmentController.current === controller) {
        assessmentController.current = null;
        setAssessing(false);
      }
    }
  }

  function closeAssessmentModal() {
    assessmentController.current?.abort();
    assessmentController.current = null;
    setAssessing(false);
    setShowAssessment(false);
  }

  function chooseMigrationContent(value: MigrationContent) {
    setMigrationContent(value);
    if (
      value === "data_only" &&
      !["append", "truncate"].includes(existingTable)
    )
      setExistingTable("append");
  }

  async function runDeepAssessment() {
    if (!activeLinkId) {
      setError("请先保存或选择迁移链路后再深度评估");
      return;
    }
    if (!ownerFilters.length) {
      setShowOwnerWarning(true);
      return;
    }
    deepController.current?.abort();
    const controller = new AbortController();
    deepController.current = controller;
    setDeepAssessing(true);
    setError("");
    setDeepAssessmentError("");
    setDeepAssessment(null);
    setShowDeepAssessment(true);
    try {
      const result = await api<DeepAssessment>("/api/assessments/deep", {
        method: "POST",
        signal: controller.signal,
        body: JSON.stringify({
          link_id: activeLinkId,
          owners: ownerFilters,
          bandwidth_mbps: deepBandwidthMbps,
          batch_size: Math.max(100, Math.min(20000, batchSize || 2000)),
          table_concurrency: Math.max(1, Math.min(16, tableConcurrency || 1)),
        }),
      });
      setDeepAssessment(result);
      setDeepExport(null);
    } catch (reason) {
      if (!(reason instanceof DOMException && reason.name === "AbortError")) {
        setDeepAssessmentError(
          reason instanceof Error ? reason.message : "深度评估失败",
        );
      }
    } finally {
      if (deepController.current === controller) {
        deepController.current = null;
        setDeepAssessing(false);
      }
    }
  }

  function closeDeepAssessmentModal() {
    deepController.current?.abort();
    deepController.current = null;
    setDeepAssessing(false);
    setShowDeepAssessment(false);
  }

  async function exportDeepReport() {
    if (!deepAssessment) return;
    setDeepExporting(true);
    setError("");
    try {
      const result = await api<DeepExportResult>(
        "/api/assessments/deep/export",
        {
          method: "POST",
          body: JSON.stringify({
            link_id: activeLinkId,
            owners: ownerFilters.length ? ownerFilters : null,
          }),
        },
      );
      setDeepExport(result);
      const fullUrl = `${apiBase.trim().replace(/\/$/, "")}${result.download_url}`;
      const downloadResp = await fetch(fullUrl, {
        headers: apiToken.trim() ? { "X-FlowDB-Token": apiToken.trim() } : {},
      });
      if (!downloadResp.ok) {
        throw new Error(`报告下载失败（HTTP ${downloadResp.status}）`);
      }
      const downloadBlob = await downloadResp.blob();
      const objectUrl = URL.createObjectURL(downloadBlob);
      const anchor = document.createElement("a");
      anchor.href = objectUrl;
      anchor.download = result.file_name;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(objectUrl);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "报告导出失败");
    } finally {
      setDeepExporting(false);
    }
  }

  async function cancelJob(job: Job) {
    setOpenJobMenu("");
    setCancellingJobId(job.id);
    setError("");
    try {
      const updated = await api<Job>(`/api/jobs/${job.id}/cancel`, {
        method: "POST",
      });
      setJobs((items) =>
        items.map((item) => (item.id === job.id ? updated : item)),
      );
      setSelectedJob(updated);
      setCancelConfirmJob(null);
      setNotice("迁移任务已取消");
      window.setTimeout(() => setNotice(""), 3500);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "取消任务失败");
    } finally {
      setCancellingJobId("");
    }
  }

  async function finishSyncJob(job: Job) {
    setOpenJobMenu("");
    setFinishingJobId(job.id);
    setError("");
    try {
      const updated = await api<Job>(`/api/jobs/${job.id}/finish-sync`, {
        method: "POST",
      });
      setJobs((items) =>
        items.map((item) => (item.id === job.id ? updated : item)),
      );
      setSelectedJob(updated);
      setFinishConfirmJob(null);
      setNotice("实时同步已正常结束，任务已完成");
      window.setTimeout(() => setNotice(""), 3500);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "结束同步失败");
    } finally {
      setFinishingJobId("");
    }
  }

  async function startIncrementalJob(job: Job) {
    setError("");
    try {
      const created = await api<Job>(`/api/jobs/${job.id}/start-incremental`, {
        method: "POST",
      });
      setJobs((items) => [created, ...items]);
      setSelectedJob(created);
      setNotice(`已从保存的 SCN 创建增量任务：${created.name}`);
      window.setTimeout(() => setNotice(""), 4500);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "启动增量同步失败");
    }
  }

  async function resumeIncrementalJob(job: Job) {
    setOpenJobMenu("");
    setResumingJobId(job.id);
    setError("");
    try {
      const updated = await api<Job>(`/api/jobs/${job.id}/resume-incremental`, {
        method: "POST",
      });
      setJobs((items) =>
        items.map((item) => (item.id === job.id ? updated : item)),
      );
      setSelectedJob(updated);
      setResumeConfirmJob(null);
      setNotice(
        `已从检查点 SCN ${(updated.checkpoint_scn || 0).toLocaleString("zh-CN")} 继续增量同步`,
      );
      window.setTimeout(() => setNotice(""), 4500);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "继续增量同步失败");
    } finally {
      setResumingJobId("");
    }
  }

  async function retryJob(job: Job) {
    setOpenJobMenu("");
    setError("");
    try {
      const createdJob = await api<Job>(`/api/jobs/${job.id}/retry`, {
        method: "POST",
      });
      setJobs((items) => [createdJob, ...items]);
      setNotice("重试任务已创建");
      window.setTimeout(() => setNotice(""), 3500);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "重试任务失败");
    }
  }

  async function openEditJob(job: Job) {
    setOpenJobMenu("");
    setError("");
    try {
      const template = await api<JobEditTemplate>(`/api/jobs/${job.id}/edit`);
      setEditTemplate(template);
      setEditSequenceSearch("");
      setEditTableSearch("");
      setEditViewSearch("");
      setEditName(template.name.replace(/（重试）$/, ""));
      setEditSelected(template.tables);
      setEditSequences(template.sequences || []);
      setEditMigrateSequences(template.migrate_sequences !== false);
      setEditBatchSize(template.batch_size);
      setEditConcurrency(template.table_concurrency);
      setEditExistingTable(template.existing_table);
      setEditContent(template.migration_content);
      setEditFailPolicy(template.fail_policy);
      setEditIdentifierCasePolicy(template.identifier_case_policy || "auto");
      setEditCdcBusinessKeysText(
        formatCdcKeyOverrides(template.cdc_key_overrides),
      );
      setEditCdcNoKeyPolicy(template.cdc_no_key_policy || "reject");
      setEditCdcAllowSourceDdl(template.cdc_allow_source_ddl === true);
      setEditUserMappings(
        (template.user_mappings || []).map((item) => ({
          source: item.source,
          target: item.target,
        })),
      );
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "加载任务参数失败");
    }
  }

  async function submitEditedJob() {
    if (
      !editTemplate ||
      (!editSelected.length && (!editMigrateSequences || !editSequences.length))
    ) {
      setError("序列、表和视图均可单独迁移；请至少选择 1 个对象");
      return;
    }
    setError("");
    setEditSaving(true);
    try {
      const objectTypes = Object.fromEntries(
        editSelected.map((table) => [
          table,
          editTemplate.object_types[table] || "table",
        ]),
      );
      const createdJob = await api<Job>("/api/jobs", {
        method: "POST",
        body: JSON.stringify({
          name: editName.trim() || editTemplate.name,
          link_id: editTemplate.link_id,
          link_name: editTemplate.link_name,
          tables: editSelected,
          object_types: objectTypes,
          sequences: editMigrateSequences ? editSequences : [],
          migrate_sequences: editMigrateSequences,
          user_mappings: editUserMappings
            .filter((item) => item.source.trim() && item.target.trim())
            .map((item) => ({
              source: item.source.trim(),
              target: item.target.trim(),
            })),
          batch_size: Math.max(100, Math.min(20000, editBatchSize || 100)),
          table_concurrency: Math.max(1, Math.min(16, editConcurrency || 1)),
          existing_table: editExistingTable,
          create_tables: editContent !== "data_only",
          migration_content: editContent,
          sync_mode: editTemplate.sync_mode || "full_only",
          start_scn:
            editTemplate.sync_mode === "incremental_only"
              ? editTemplate.start_scn
              : null,
          cdc_poll_seconds: editTemplate.cdc_poll_seconds || 3,
          cdc_key_overrides: parseCdcKeyOverrides(editCdcBusinessKeysText),
          cdc_no_key_policy: editCdcNoKeyPolicy,
          cdc_allow_source_ddl: editCdcAllowSourceDdl,
          fail_policy: editFailPolicy,
          identifier_case_policy: editIdentifierCasePolicy,
        }),
      });
      setEditTemplate(null);
      setSelectedJob(null);
      setJobPage(1);
      setJobs((current) => [createdJob, ...current]);
      setNotice("已按修改后的参数创建新任务");
      window.setTimeout(() => setNotice(""), 3500);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "创建任务失败");
    } finally {
      setEditSaving(false);
    }
  }

  function toggleEditTable(name: string) {
    setEditSelected((current) =>
      current.includes(name)
        ? current.filter((item) => item !== name)
        : [...current, name],
    );
  }

  const compareObjectNames = (left: string, right: string) =>
    left.localeCompare(right, "en", { sensitivity: "base", numeric: true });
  const matchesObjectSearch = (name: string, query: string) =>
    name.toLocaleUpperCase().includes(query.trim().toLocaleUpperCase());

  const editSequencesSorted = editTemplate
    ? [...editTemplate.sequences].sort(compareObjectNames)
    : [];
  const editOrdinaryTables = editTemplate
    ? editTemplate.tables.filter(
        (name) => (editTemplate.object_types[name] || "table") === "table",
      ).sort(compareObjectNames)
    : [];
  const editPartitionedTables = editTemplate
    ? editTemplate.tables.filter(
        (name) => editTemplate.object_types[name] === "partitioned_table",
      ).sort(compareObjectNames)
    : [];
  const editViews = editTemplate
    ? editTemplate.tables.filter(
        (name) => editTemplate.object_types[name] === "view",
      ).sort(compareObjectNames)
    : [];
  const editAllTables = [...editOrdinaryTables, ...editPartitionedTables];
  const visibleEditSequences = editSequencesSorted.filter((name) =>
    matchesObjectSearch(name, editSequenceSearch),
  );
  const visibleEditOrdinaryTables = editOrdinaryTables.filter((name) =>
    matchesObjectSearch(name, editTableSearch),
  );
  const visibleEditPartitionedTables = editPartitionedTables.filter((name) =>
    matchesObjectSearch(name, editTableSearch),
  );
  const visibleEditViews = editViews.filter((name) =>
    matchesObjectSearch(name, editViewSearch),
  );

  function setEditObjectsSelected(names: string[], checked: boolean) {
    setEditSelected((current) => {
      const group = new Set(names);
      if (!checked) return current.filter((name) => !group.has(name));
      return Array.from(new Set([...current, ...names]));
    });
  }

  function navigateTo(name: string, legacyAnchor?: string) {
    void legacyAnchor;
    const routes: Record<string, string> = {
      workspace: "/",
      tasks: "/tasks",
      datasources: "/datasources",
      sources: "/datasources",
      links: "/links",
      validation: "/validation",
      nodes: "/nodes",
      settings: "/settings",
    };
    window.location.assign(routes[name] || "/");
  }

  async function saveLink() {
    if (!linkName.trim()) {
      setError("请输入链路名称");
      return;
    }
    setSavingLink(true);
    setError("");
    try {
      const payload = {
        name: linkName.trim(),
        source: cleanConfig(source),
        target: cleanConfig(target),
      };
      const saved = editingLinkId
        ? await api<MigrationLink>(`/api/links/${editingLinkId}`, {
            method: "PUT",
            body: JSON.stringify(payload),
          })
        : await api<MigrationLink>("/api/links", {
            method: "POST",
            body: JSON.stringify(payload),
          });
      setEditingLinkId("");
      setActiveLinkId(saved.id);
      setLinkName(saved.name);
      await refreshLinks();
      setNotice("连接链路已保存，可用于生产迁移");
      window.setTimeout(() => setNotice(""), 3500);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "保存链路失败");
    } finally {
      setSavingLink(false);
    }
  }

  async function selectLink(link: MigrationLink) {
    setActiveLinkId(link.id);
    setEditingLinkId("");
    setLinkName(link.name);
    setSource(restoredConfig(link.source));
    setTarget(restoredConfig(link.target));
    setConnectionState({ source: "ok:已保存", target: "ok:已保存" });
    setTables([]);
    setSelected([]);
    setSelectedSequences([]);
    setOwnerFilters([]);
    setOwnerList([]);
    setCreatedJobId("");
    setLinkWizardStep(1);
    setTargetCaseCapabilities(null);
    try {
      const result = await api<{
        target: CaseCapabilities;
      }>(`/api/links/${link.id}/test`, { method: "POST" });
      setTargetCaseCapabilities(result.target);
      if (
        result.target.lower_case_table_names === 1 &&
        identifierCasePolicy === "preserve"
      ) {
        setIdentifierCasePolicy("auto");
        setNotice("目标实例以小写存储表名，已切换为自动适配");
      }
    } catch {
      // Object loading and task creation will retry detection server-side.
    }
  }

  async function deleteLink(link: MigrationLink) {
    try {
      await api<void>(`/api/links/${link.id}`, { method: "DELETE" });
      if (activeLinkId === link.id) setActiveLinkId("");
      await refreshLinks();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "删除链路失败");
    }
  }

  async function validateMigration() {
    const jobId =
      validationJob ||
      jobs.find(
        (job) =>
          job.status === "completed" &&
          job.migration_content !== "structure_only",
      )?.id;
    if (!jobId) {
      setError("暂无已完成任务可校验");
      return;
    }
    setValidating(true);
    setError("");
    try {
      const result = await api<ValidationRecord>(
        `/api/jobs/${jobId}/validate`,
        { method: "POST" },
      );
      setValidationResult(result);
      setValidationTableFilter("all");
      setValidationHistoryPage(1);
      await refreshValidationHistory();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "迁移校验失败");
    } finally {
      setValidating(false);
    }
  }

  async function openValidationRecord(validationId: string) {
    setLoadingValidationRecord(validationId);
    setError("");
    try {
      setValidationTableFilter("all");
      setValidationResult(
        await api<ValidationRecord>(`/api/validations/${validationId}`),
      );
      document
        .getElementById("validation-report")
        ?.scrollIntoView({ behavior: "smooth", block: "start" });
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "读取校验记录失败");
    } finally {
      setLoadingValidationRecord("");
    }
  }

  const completed = useMemo(
    () => jobs.filter((job) => job.status === "completed").length,
    [jobs],
  );
  const running = useMemo(
    () =>
      jobs.filter((job) =>
        ["queued", "running", "catching_up", "syncing"].includes(job.status),
      ).length,
    [jobs],
  );
  const sortedObjects = (items: TableInfo[]) =>
    [...items].sort((left, right) => compareObjectNames(left.name, right.name));
  const visibleObjects = useMemo(() => {
    const items = objectTab === "table"
      ? tables.filter((item) => item.object_type === "table")
      : tables.filter((item) => item.object_type === objectTab);
    return sortedObjects(items).filter((item) =>
      matchesObjectSearch(item.name, objectSearch),
    );
  }, [tables, objectTab, objectSearch]);
  const sequenceObjects = useMemo(
    () => sortedObjects(tables.filter((item) => item.object_type === "sequence")),
    [tables],
  );
  const normalTables = useMemo(
    () => sortedObjects(tables.filter((item) => item.object_type === "table")),
    [tables],
  );
  const partitionedTables = useMemo(
    () => sortedObjects(tables.filter((item) => item.object_type === "partitioned_table")),
    [tables],
  );
  const selectedTables = useMemo(
    () =>
      tables.filter(
        (item) =>
          (item.object_type === "table" ||
            item.object_type === "partitioned_table") &&
          selected.includes(item.name),
      ),
    [tables, selected],
  );
  const selectedNormalTables = useMemo(
    () => normalTables.filter((item) => selected.includes(item.name)),
    [normalTables, selected],
  );
  const selectedPartitionedTables = useMemo(
    () => partitionedTables.filter((item) => selected.includes(item.name)),
    [partitionedTables, selected],
  );
  const viewObjects = useMemo(
    () => sortedObjects(tables.filter((item) => item.object_type === "view")),
    [tables],
  );
  const selectionPercent = (selectedCount: number, totalCount: number) =>
    totalCount ? Math.round((selectedCount / totalCount) * 100) : 0;
  const objectTypeLabel = (type: string) =>
    type === "view"
      ? "视图"
      : type === "sequence"
        ? "序列"
        : type === "partitioned_table"
          ? "分区表"
          : "普通表";
  const renderObjectItem = (table: {
    name: string;
    object_type: string;
    columns?: number;
    primary_keys?: string[];
  }) => {
    const isSequence = table.object_type === "sequence";
    const checked = isSequence
      ? selectedSequences.includes(table.name)
      : selected.includes(table.name);
    return (
      <label key={table.name} className={checked ? "picked" : ""}>
        <input
          type="checkbox"
          checked={checked}
          onChange={(event) => {
            if (isSequence) {
              setSelectedSequences((current) =>
                event.target.checked
                  ? Array.from(new Set([...current, table.name]))
                  : current.filter((name) => name !== table.name),
              );
            } else {
              setSelected((current) =>
                event.target.checked
                  ? Array.from(new Set([...current, table.name]))
                  : current.filter((name) => name !== table.name),
              );
            }
          }}
        />
        <span className="table-icon">
          {isSequence
            ? "⧗"
            : table.object_type === "view"
              ? "◫"
              : table.object_type === "partitioned_table"
                ? "▦"
                : "▦"}
        </span>
        <span>
          <b>{table.name}</b>
          <small>
            {isSequence
              ? "序列"
              : `${table.columns} 列 · ${
                  table.object_type === "view"
                    ? "视图（迁移为实体表）"
                    : (table.primary_keys?.length ?? 0) > 0
                      ? `主键 ${(table.primary_keys ?? []).join(", ")}`
                      : "无主键"
                }`}
          </small>
        </span>
        <span className={`object-type-badge type-${table.object_type}`}>
          {objectTypeLabel(table.object_type)}
        </span>
      </label>
    );
  };
  const selectedViews = useMemo(
    () =>
      tables.filter(
        (item) => item.object_type === "view" && selected.includes(item.name),
      ),
    [tables, selected],
  );

  function openObjectSelection() {
    if (!activeLinkId) {
      setError("请先从连接链路库选择一条链路");
      return;
    }
    setError("");
    setObjectTab("sequence");
    setObjectSearch("");
    setLinkWizardStep(2);
  }

  function openExecutionSettings() {
    if (!selected.length && !(migrateSequences && selectedSequences.length)) {
      setError("请至少选择一个要迁移的序列、普通表、分区表或视图");
      return;
    }
    setError("");
    setLinkWizardStep(3);
  }

  const selectedTargetType = activeLinkId
    ? links.find((item) => item.id === activeLinkId)?.target.type
    : target.type;
  const showIdentifierCaseSettings = ["mysql", "tdsql"].includes(
    selectedTargetType || "",
  );
  const resolvedIdentifierCasePolicy: Exclude<IdentifierCasePolicy, "auto"> =
    identifierCasePolicy === "auto"
      ? targetCaseCapabilities?.lower_case_table_names === 1
        ? "lower"
        : "preserve"
      : identifierCasePolicy;
  const identifierNameMappings = selected
    .map((name) => ({
      source: name,
      target:
        resolvedIdentifierCasePolicy === "lower"
          ? name.toLowerCase()
          : resolvedIdentifierCasePolicy === "upper"
            ? name.toUpperCase()
            : name,
    }))
    .filter((item) => item.source !== item.target);
  const preserveUnavailable =
    targetCaseCapabilities?.lower_case_table_names === 1;

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-mark">
            <i />
            <i />
            <i />
          </span>
          <span>FlowDB</span>
        </div>
        <nav aria-label="主导航">
          <button
            onClick={() => navigateTo("workspace")}
            className={`nav-item ${activeNav === "workspace" ? "active" : ""}`}
          >
            <span className="nav-icon">⌁</span>迁移工作台
          </button>
          <button
            onClick={() => navigateTo("datasources")}
            className={`nav-item ${activeNav === "datasources" ? "active" : ""}`}
          >
            <span className="nav-icon">▱</span>数据源
          </button>
          <button
            onClick={() => navigateTo("links")}
            className={`nav-item ${activeNav === "links" ? "active" : ""}`}
          >
            <span className="nav-icon">⌘</span>连接链路库
          </button>
          <button
            onClick={() => navigateTo("tasks")}
            className={`nav-item ${activeNav === "tasks" ? "active" : ""}`}
          >
            <span className="nav-icon">⇄</span>迁移任务
            <span className="nav-count">{running}</span>
          </button>
          <button
            onClick={() => navigateTo("validation")}
            className={`nav-item ${activeNav === "validation" ? "active" : ""}`}
          >
            <span className="nav-icon">⌕</span>迁移校验
          </button>
          <div className="nav-label">管理</div>
          <button
            onClick={() => navigateTo("nodes")}
            className={`nav-item ${activeNav === "nodes" ? "active" : ""}`}
          >
            <span className="nav-icon">♧</span>运行节点
          </button>
          <button
            className={`nav-item ${activeNav === "settings" ? "active" : ""}`}
            onClick={() => navigateTo("settings")}
          >
            <span className="nav-icon">⚙</span>系统设置
          </button>
        </nav>
        <div className="sidebar-foot">
          <button
            className="system-state node-link"
            onClick={() => navigateTo("nodes", "node-status")}
          >
            <span className={`pulse ${nodeOnline ? "" : "offline"}`} />
            <div>
              <b>迁移节点</b>
              <small>
                {nodeOnline ? "在线" : "未连接"} · {apiBase || "当前服务器"}
              </small>
            </div>
          </button>
          <div className="security-note">🔒 数据库密码使用 Fernet 加密存储</div>
        </div>
      </aside>

      <section className="workspace">
        <header className="topbar">
          <div>
            <h1>数据库迁移</h1>
            <p>Oracle、MySQL 与 PostgreSQL 全量迁移</p>
          </div>
          <div className="top-actions">
            <span className="node-chip">
              <i /> {running} 个任务运行中
            </span>
            <button
              className="icon-button"
              onClick={() => navigateTo("settings")}
              aria-label="系统设置"
            >
              ⚙
            </button>
            <button
              className="primary-button"
              onClick={() => navigateTo("links")}
            >
              <b>＋</b> 新建迁移
            </button>
          </div>
        </header>
        <div className={`content page-${activeNav}`}>
          <section className="hero-card">
            <div className="hero-copy">
              <span className="eyebrow">真实迁移引擎</span>
              <h2>从连接测试到批量写入，全程可观测</h2>
              <p>迁移任务在你的服务器内运行，源库密码不会发送到第三方服务。</p>
              <div className="feature-row">
                <span>✓ 流式读取</span>
                <span>✓ 批量写入</span>
                <span>✓ 类型转换</span>
                <span>✓ 任务持久化</span>
              </div>
            </div>
            <div className="hero-stats">
              <div>
                <b>{jobs.length}</b>
                <small>全部任务</small>
              </div>
              <div>
                <b>{running}</b>
                <small>正在运行</small>
              </div>
              <div>
                <b>{completed}</b>
                <small>迁移完成</small>
              </div>
            </div>
          </section>

          <section className="workbench-dashboard">
            <div className="section-title-row">
              <div>
                <h3>迁移工作台</h3>
                <p>连接链路与生产任务分开管理，按流程快速进入对应模块</p>
              </div>
            </div>
            <div className="workbench-actions">
              <button onClick={() => navigateTo("datasources")}>
                <i>＋</i>
                <span>
                  <b>数据源</b>
                  <small>配置并测试源端和目标端连接</small>
                </span>
                <em>→</em>
              </button>
              <button onClick={() => navigateTo("links")}>
                <i>⌘</i>
                <span>
                  <b>连接链路库</b>
                  <small>选择已保存链路并创建迁移</small>
                </span>
                <em>→</em>
              </button>
              <button onClick={() => navigateTo("tasks")}>
                <i>⇄</i>
                <span>
                  <b>迁移任务</b>
                  <small>查看进度、日志、重试与分页</small>
                </span>
                <em>→</em>
              </button>
              <button onClick={() => navigateTo("validation")}>
                <i>✓</i>
                <span>
                  <b>迁移校验</b>
                  <small>查看行数、哈希和字段差异</small>
                </span>
                <em>→</em>
              </button>
            </div>
            <div className="workbench-overview">
              <div>
                <span>已保存链路</span>
                <b>{links.length}</b>
                <small>名称全局唯一</small>
              </div>
              <div>
                <span>生产任务</span>
                <b>{jobTotal}</b>
                <small>{running} 个正在运行</small>
              </div>
              <div>
                <span>最近链路</span>
                {links.slice(0, 3).map((link) => (
                  <button key={link.id} onClick={() => navigateTo("links")}>
                    <b>{link.name}</b>
                    <small>
                      {dbMeta[link.source.type].label} →{" "}
                      {dbMeta[link.target.type].label}
                    </small>
                  </button>
                ))}
                {!links.length && <small>尚未保存连接链路</small>}
              </div>
            </div>
          </section>

          {error && (
            <div className="alert error-alert">
              <b>操作失败</b>
              <span>{error}</span>
              <button onClick={() => setError("")}>×</button>
            </div>
          )}

          <section
            className={`link-library ${
              activeNav === "links" && linkWizardStep !== 1
                ? "wizard-hidden"
                : ""
            }`}
          >
            <div className="section-title-row">
              <div>
                <h3>连接链路库</h3>
                <p>
                  源端和目标端组成可复用链路；链路名称不允许重复，和生产任务互不冲突
                </p>
              </div>
              <button
                className="secondary-button"
                onClick={() => navigateTo("datasources")}
              >
                ＋ 新建数据源链路
              </button>
            </div>
            <div className="link-grid">
              {links.map((link) => (
                <article
                  key={link.id}
                  className={activeLinkId === link.id ? "active" : ""}
                >
                  <div>
                    <b>{link.name}</b>
                    <small>
                      更新于 {new Date(link.updated_at).toLocaleString("zh-CN")}
                    </small>
                  </div>
                  <div className="link-route">
                    <DatabaseBadge type={link.source.type} small />
                    <span>
                      <b>
                        {link.source.host}:{link.source.port}
                      </b>
                      <small>
                        {link.source.database} / {link.source.username}
                      </small>
                    </span>
                    <em>→</em>
                    <DatabaseBadge type={link.target.type} small />
                    <span>
                      <b>
                        {link.target.host}:{link.target.port}
                      </b>
                      <small>
                        {link.target.database} / {link.target.username}
                      </small>
                    </span>
                  </div>
                  <footer>
                    <button onClick={() => selectLink(link)}>
                      {activeLinkId === link.id ? "✓ 已选择" : "选择此链路"}
                    </button>
                    <button
                      onClick={() =>
                        window.location.assign(`/datasources?edit=${link.id}`)
                      }
                    >
                      编辑
                    </button>
                    <button className="danger" onClick={() => deleteLink(link)}>
                      删除
                    </button>
                  </footer>
                </article>
              ))}
              {!links.length && (
                <div className="empty-links">
                  尚未创建连接链路。请先到“数据源”页面配置并保存。
                </div>
              )}
            </div>
            {activeNav === "links" && (
              <div className="link-library-actions">
                <span>
                  {activeLinkId
                    ? "已选择连接链路，下一步选择 owner 并读取迁移对象"
                    : "请选择一条连接链路后继续"}
                </span>
                <button
                  type="button"
                  className="continue-button"
                  disabled={!activeLinkId}
                  onClick={openObjectSelection}
                >
                  下一步：选择 owner 和迁移对象 →
                </button>
              </div>
            )}
          </section>

          <section
            className={`migration-builder ${
              activeNav === "links"
                ? `link-wizard wizard-step-${linkWizardStep}`
                : ""
            }`}
            id="sources"
          >
            <div className="wizard-sticky">
              <div className="section-head">
                <div>
                  <h3>
                    {activeNav === "links"
                      ? "从连接链路创建迁移任务"
                      : "配置数据源"}
                  </h3>
                  <p>
                    {activeNav === "links"
                      ? "按步骤选择连接链路、迁移对象并配置执行参数"
                      : "填写并测试源端和目标端，确认无误后在页面下方保存为连接链路"}
                  </p>
                </div>
                <div className="stepper">
                  <button
                    type="button"
                    className="step active"
                    onClick={() =>
                      activeNav === "links" && setLinkWizardStep(1)
                    }
                  >
                    <i>1</i>
                    {activeNav === "links" ? "选择链路" : "连接配置"}
                  </button>
                  <b />
                  <button
                    type="button"
                    className={`step ${activeNav === "links" ? (linkWizardStep >= 2 ? "active" : "") : tables.length ? "active" : ""}`}
                    onClick={() =>
                      activeNav === "links" &&
                      tables.length &&
                      setLinkWizardStep(2)
                    }
                  >
                    <i>2</i>对象选择
                  </button>
                  <b />
                  <button
                    type="button"
                    className={`step ${activeNav === "links" && linkWizardStep === 3 ? "active" : ""}`}
                    onClick={() =>
                      activeNav === "links" &&
                      linkWizardStep === 3 &&
                      setLinkWizardStep(3)
                    }
                  >
                    <i>3</i>配置执行
                  </button>
                </div>
              </div>
            </div>
            <div className="connection-grid real-grid">
              {(["source", "target"] as const).map((side) => {
                const config = side === "source" ? source : target;
                const state = connectionState[side];
                return (
                  <div className="connection-panel" key={side}>
                    <div className="panel-label">
                      <span
                        className={`label-number ${side === "target" ? "purple" : ""}`}
                      >
                        {side === "source" ? "1" : "2"}
                      </span>
                      <div>
                        <h4>{side === "source" ? "源数据库" : "目标数据库"}</h4>
                        <p>
                          {side === "source"
                            ? "读取结构与数据"
                            : "创建表并写入数据"}
                        </p>
                      </div>
                      <span className="required">必填</span>
                    </div>
                    <div className="db-options">
                      {(Object.keys(dbMeta) as DbType[]).map((type) => (
                        <button
                          type="button"
                          key={type}
                          onClick={() => chooseType(side, type)}
                          className={`db-option ${config.type === type ? "selected" : ""}`}
                        >
                          <DatabaseBadge type={type} />
                          <span>
                            <b>{dbMeta[type].label}</b>
                            <small>默认端口 {dbMeta[type].port}</small>
                          </span>
                          <i>{config.type === type ? "✓" : ""}</i>
                        </button>
                      ))}
                    </div>
                    <div className="form-row">
                      <label>
                        主机地址
                        <input
                          value={config.host}
                          onChange={(event) =>
                            updateConfig(side, "host", event.target.value)
                          }
                          placeholder="10.0.0.12"
                          autoComplete="off"
                        />
                      </label>
                      <label className="port">
                        端口
                        <input
                          type="number"
                          value={config.port}
                          onChange={(event) =>
                            updateConfig(
                              side,
                              "port",
                              Number(event.target.value),
                            )
                          }
                        />
                      </label>
                    </div>
                    <div className="form-row">
                      <label>
                        {config.type === "oracle" ? "服务名" : "数据库名"}
                        <input
                          value={config.database}
                          onChange={(event) =>
                            updateConfig(side, "database", event.target.value)
                          }
                          placeholder={dbMeta[config.type].sample}
                        />
                      </label>
                      <label>
                        Schema（可选）
                        <input
                          value={config.schema_name}
                          onChange={(event) =>
                            updateConfig(
                              side,
                              "schema_name",
                              event.target.value,
                            )
                          }
                          placeholder={
                            config.type === "postgresql"
                              ? "public"
                              : config.type === "oracle"
                                ? "默认用户名"
                                : "默认数据库"
                          }
                        />
                      </label>
                    </div>
                    <div className="form-row">
                      <label>
                        用户名
                        <input
                          value={config.username}
                          onChange={(event) =>
                            updateConfig(side, "username", event.target.value)
                          }
                          autoComplete="username"
                        />
                      </label>
                      <label>
                        密码
                        <input
                          type="password"
                          value={config.password}
                          onChange={(event) =>
                            updateConfig(side, "password", event.target.value)
                          }
                          autoComplete="new-password"
                        />
                      </label>
                    </div>
                    <div className="test-row">
                      <button
                        onClick={() => testConnection(side)}
                        disabled={state === "testing" || Boolean(activeLinkId)}
                        className={`test-button ${state.startsWith("ok") ? "ok" : state === "failed" ? "failed" : ""}`}
                      >
                        {state === "testing"
                          ? "正在连接…"
                          : state.startsWith("ok")
                            ? state.includes("已保存")
                              ? "✓ 已保存链路"
                              : `✓ 连接成功 · ${state.split(":")[1]} ms`
                            : state === "failed"
                              ? "× 连接失败，重试"
                              : "⌁ 测试真实连接"}
                      </button>
                      <span>
                        {activeLinkId
                          ? "使用已加密保存的凭据"
                          : "10 秒连接超时"}
                      </span>
                    </div>
                    {connectionErrors[side] && (
                      <div className="connection-error" role="alert">
                        <b>失败原因</b>
                        <span>{connectionErrors[side]}</span>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>

            <div className="link-editor">
              <label>
                链路名称
                <input
                  value={linkName}
                  onChange={(event) => setLinkName(event.target.value)}
                  placeholder="例如：Oracle生产库到TDSQL测试库"
                  maxLength={80}
                />
              </label>
              <div>
                {editingLinkId && <span>正在编辑已有连接链路</span>}
                <button
                  className="continue-button"
                  onClick={saveLink}
                  disabled={savingLink || !linkName.trim()}
                >
                  {savingLink
                    ? "正在保存…"
                    : editingLinkId
                      ? "更新连接链路"
                      : "保存连接链路"}
                </button>
              </div>
              <small>
                链路名称忽略大小写全局唯一；编辑时密码留空表示保留原密码。
              </small>
            </div>

            <div className="objects-panel">
              <div className="objects-head">
                <div>
                  <h4>分阶段选择迁移对象</h4>
                  <p>
                    先选择
                    owner（schema）并读取对象，再按序列、普通表、分区表、视图选择迁移范围
                  </p>
                </div>
                <div className="objects-head-actions">
                  <button
                    className="secondary-button"
                    onClick={loadOwners}
                    disabled={loadingOwners}
                    title={
                      ownerFilters.length
                        ? `当前筛选 owner：${ownerFilters.join("、")}`
                        : "点击选择要评估/迁移的 owner（schema，支持多选）"
                    }
                  >
                    {loadingOwners
                      ? "加载中…"
                      : ownerFilters.length
                        ? `1 已选 owner：${ownerFilters.join("、")}`
                        : "1 选择 owner"}
                  </button>
                  <button
                    className="secondary-button"
                    onClick={loadTables}
                    disabled={
                      loadingTables ||
                      (activeNav === "links" && !ownerFilters.length)
                    }
                    title={
                      activeNav === "links" && !ownerFilters.length
                        ? "请先选择 owner（schema）"
                        : "按当前 owner 范围读取迁移对象"
                    }
                  >
                    {loadingTables
                      ? "正在读取…"
                      : tables.length
                        ? "2 ↻ 重新读取"
                        : "2 读取迁移对象"}
                  </button>
                </div>
              </div>
              {tables.length ? (
                <>
                  <div className="object-inventory" aria-label="源库对象统计">
                    <div className="inventory-total">
                      <span>本次读取</span>
                      <b>{normalTables.length + partitionedTables.length}</b>
                      <small>张源表</small>
                    </div>
                    <div className="inventory-stat ordinary">
                      <span>普通表</span>
                      <b>{normalTables.length}</b>
                      <small>第 2 阶段迁移</small>
                    </div>
                    <div className="inventory-stat partitioned">
                      <span>分区表</span>
                      <b>{partitionedTables.length}</b>
                      <small>普通表完成后迁移</small>
                    </div>
                    <div className="inventory-stat auxiliary">
                      <span>依赖对象</span>
                      <b>{sequenceObjects.length + viewObjects.length}</b>
                      <small>
                        {sequenceObjects.length} 序列 · {viewObjects.length}{" "}
                        视图
                      </small>
                    </div>
                  </div>
                  <div className="migration-phases" aria-label="迁移执行顺序">
                    <button
                      className={`phase-sequence ${objectTab === "sequence" ? "active" : ""}`}
                      onClick={() => { setObjectTab("sequence"); setObjectSearch(""); }}
                    >
                      <i>1</i>
                      <span>
                        <b>先迁移序列</b>
                        <small>
                          已选 {migrateSequences ? selectedSequences.length : 0}{" "}
                          / {sequenceObjects.length} 个序列
                        </small>
                        <em>
                          <u
                            style={{
                              width: `${selectionPercent(
                                migrateSequences ? selectedSequences.length : 0,
                                sequenceObjects.length,
                              )}%`,
                            }}
                          />
                        </em>
                      </span>
                    </button>
                    <button
                      className={`phase-table ${objectTab === "table" ? "active" : ""}`}
                      onClick={() => { setObjectTab("table"); setObjectSearch(""); }}
                    >
                      <i>2</i>
                      <span>
                        <b>再迁移普通表</b>
                        <small>
                          已选 {selectedNormalTables.length} /{" "}
                          {normalTables.length} 张普通表
                        </small>
                        <em>
                          <u
                            style={{
                              width: `${selectionPercent(
                                selectedNormalTables.length,
                                normalTables.length,
                              )}%`,
                            }}
                          />
                        </em>
                      </span>
                    </button>
                    <button
                      className={`phase-partitioned ${objectTab === "partitioned_table" ? "active" : ""}`}
                      onClick={() => { setObjectTab("partitioned_table"); setObjectSearch(""); }}
                    >
                      <i>3</i>
                      <span>
                        <b>再迁移分区表</b>
                        <small>
                          已选 {selectedPartitionedTables.length} /{" "}
                          {partitionedTables.length} 张分区表
                        </small>
                        <em>
                          <u
                            style={{
                              width: `${selectionPercent(
                                selectedPartitionedTables.length,
                                partitionedTables.length,
                              )}%`,
                            }}
                          />
                        </em>
                      </span>
                    </button>
                    <button
                      className={`phase-view ${objectTab === "view" ? "active" : ""}`}
                      onClick={() => { setObjectTab("view"); setObjectSearch(""); }}
                    >
                      <i>4</i>
                      <span>
                        <b>最后迁移视图</b>
                        <small>
                          已选 {selectedViews.length} / {viewObjects.length}{" "}
                          个视图
                        </small>
                        <em>
                          <u
                            style={{
                              width: `${selectionPercent(
                                selectedViews.length,
                                viewObjects.length,
                              )}%`,
                            }}
                          />
                        </em>
                      </span>
                    </button>
                  </div>
                  <div className={`object-stage-banner stage-${objectTab}`}>
                    <span>
                      阶段{" "}
                      {objectTab === "sequence"
                        ? 1
                        : objectTab === "table"
                          ? 2
                          : objectTab === "partitioned_table"
                            ? 3
                            : 4}
                    </span>
                    <div>
                      <b>
                        {objectTab === "sequence"
                          ? "迁移序列"
                          : objectTab === "table"
                            ? "迁移普通表"
                            : objectTab === "partitioned_table"
                              ? "迁移分区表"
                              : "迁移视图"}
                      </b>
                      <small>
                        {objectTab === "sequence"
                          ? "先建立自增依赖，完成后自动进入普通表阶段"
                          : objectTab === "table"
                            ? `当前共 ${normalTables.length} 张普通表，不包含分区表`
                            : objectTab === "partitioned_table"
                              ? `普通表完成后再处理 ${partitionedTables.length} 张分区表`
                              : "所有表迁移完成后，最后创建视图快照"}
                      </small>
                    </div>
                  </div>
                  <div className="select-tools">
                    <label>
                      <input
                        type="checkbox"
                        checked={
                          visibleObjects.length > 0 &&
                          (objectTab === "sequence"
                            ? visibleObjects.every((item) =>
                                selectedSequences.includes(item.name),
                              )
                            : visibleObjects.every((item) =>
                                selected.includes(item.name),
                              ))
                        }
                        onChange={(event) => {
                          const names = visibleObjects.map((item) => item.name);
                          if (objectTab === "sequence") {
                            setSelectedSequences((current) =>
                              event.target.checked
                                ? Array.from(new Set([...current, ...names]))
                                : current.filter(
                                    (name) => !names.includes(name),
                                  ),
                            );
                          } else {
                            setSelected((current) =>
                              event.target.checked
                                ? Array.from(new Set([...current, ...names]))
                                : current.filter(
                                    (name) => !names.includes(name),
                                  ),
                            );
                          }
                        }}
                      />{" "}
                      全选当前 {visibleObjects.length} 个
                      {objectTab === "view"
                        ? "视图"
                        : objectTab === "sequence"
                          ? "序列"
                          : objectTab === "partitioned_table"
                            ? "分区表"
                            : "表"}
                    </label>
                    <span>
                      已选 <b>{selectedNormalTables.length}</b> /{" "}
                      {normalTables.length} 张普通表；
                      <b>{selectedPartitionedTables.length}</b> /{" "}
                      {partitionedTables.length} 张分区表；
                      <b>{selectedViews.length}</b> 个视图；
                      {migrateSequences ? (
                        <>
                          <b>{selectedSequences.length}</b> 个序列
                        </>
                      ) : (
                        "序列迁移已关闭"
                      )}
                    </span>
                    <label className="object-search-box">
                      <span aria-hidden="true">⌕</span>
                      <input
                        value={objectSearch}
                        onChange={(event) => setObjectSearch(event.target.value)}
                        placeholder={`搜索${objectTab === "sequence" ? "序列" : objectTab === "view" ? "视图" : objectTab === "partitioned_table" ? "分区表" : "普通表"}`}
                        aria-label="搜索迁移对象"
                      />
                    </label>
                  </div>
                  <div className="table-picker">
                    {objectTab === "table" ? (
                      <>
                        {visibleObjects.map((table) => renderObjectItem(table))}
                        {!visibleObjects.length && (
                          <div className="empty-object-tab">
                            源库中没有普通表
                          </div>
                        )}
                      </>
                    ) : (
                      <>
                        {visibleObjects.map((table) => renderObjectItem(table))}
                        {!visibleObjects.length && (
                          <div className="empty-object-tab">
                            源库中没有
                            {objectTab === "view"
                              ? "视图"
                              : objectTab === "sequence"
                                ? "序列"
                                : "分区表"}
                          </div>
                        )}
                      </>
                    )}
                  </div>
                </>
              ) : (
                <div className="empty-tables">
                  <span>▦</span>
                  <b>尚未读取迁移对象</b>
                  <small>
                    {ownerFilters.length
                      ? `已选择 owner：${ownerFilters.join("、")}，请点击“2 读取迁移对象”`
                      : "请先点击“1 选择 owner”，完成后再读取迁移对象"}
                  </small>
                </div>
              )}
            </div>

            {activeNav === "links" && linkWizardStep === 2 && (
              <div className="wizard-object-actions">
                <span>
                  已选 {selectedNormalTables.length} 张普通表、
                  {selectedPartitionedTables.length} 张分区表、
                  {selectedViews.length} 个视图
                  {migrateSequences
                    ? `、${selectedSequences.length} 个序列`
                    : ""}
                </span>
                <div>
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => setLinkWizardStep(1)}
                  >
                    ← 上一步
                  </button>
                  <button
                    type="button"
                    className="continue-button"
                    onClick={openExecutionSettings}
                  >
                    下一步：配置迁移任务 →
                  </button>
                </div>
              </div>
            )}

            {activeNav === "links" && linkWizardStep === 3 && (
              <div className="wizard-config-heading">
                <div>
                  <b>配置并执行迁移</b>
                  <small>
                    确认迁移策略、批次和并发参数后进行评估或启动任务
                  </small>
                </div>
                <span>
                  {selectedNormalTables.length +
                    selectedPartitionedTables.length}
                  张表 · {selectedViews.length} 个视图 ·{" "}
                  {selectedSequences.length}
                  个序列
                </span>
              </div>
            )}

            <div className="execution-settings">
              <label className="config-task-field">
                任务名称
                <input
                  value={taskName}
                  onChange={(event) => setTaskName(event.target.value)}
                />
              </label>
              {(activeLinkId
                ? (links.find((item) => item.id === activeLinkId)?.source
                    ?.type || "") === "oracle"
                : source.type === "oracle") && (
                <label className="user-mapping-field">
                  <span>
                    用户名映射
                    <span
                      className="user-mapping-help"
                      title="建表 / 写数时目标 schema 使用映射后的用户名；DDL 中的 `源用户.对象` 引用同步替换。"
                    >
                      ?
                    </span>
                  </span>
                  {userMappings.length > 0 ? (
                    <button
                      type="button"
                      className="user-mapping-trigger"
                      onClick={() => setMappingViewOpen(true)}
                    >
                      查看映射（{userMappings.length} 条）
                    </button>
                  ) : (
                    <button
                      type="button"
                      className="user-mapping-trigger"
                      onClick={() => {
                        setMappingDraft([{ source: "", target: "" }]);
                        setMappingError("");
                        setMappingModalOpen(true);
                      }}
                    >
                      配置用户名映射
                    </button>
                  )}
                </label>
              )}

              <label className="sync-mode-field">
                同步方式
                <select
                  value={syncMode}
                  onChange={(event) => {
                    const mode = event.target.value as SyncMode;
                    setSyncMode(mode);
                    setCdcCapability(null);
                    if (mode !== "full_only" && migrationContent === "structure_only") {
                      setMigrationContent("structure_and_data");
                    }
                  }}
                >
                  <option value="full_only">仅全量迁移</option>
                  <option value="full_then_incremental">
                    先全量，成功后手动启动增量
                  </option>
                  <option value="full_and_incremental">全量 + LogMiner 持续增量</option>
                </select>
                <small>
                  {syncMode === "full_only"
                    ? "迁移完成后任务结束"
                    : syncMode === "full_then_incremental"
                      ? "全量开始前保存 SCN；仅在全量成功后可手动创建独立增量任务"
                    : cdcCapability
                      ? `LogMiner 已检查，当前 SCN ${cdcCapability.current_scn.toLocaleString("zh-CN")}`
                      : "创建任务前自动检查归档模式、补充日志和 LogMiner 权限"}
                </small>
              </label>
              {syncMode !== "full_only" && (
                <section className="cdc-key-settings">
                  <div className="cdc-key-settings-head">
                    <div>
                      <b>无主键表的增量定位</b>
                      <small>
                        自动按“主键 → 非空唯一约束/索引 → 业务唯一键”选择；无法可靠定位时默认拒绝启动
                      </small>
                    </div>
                    <span>安全优先</span>
                  </div>
                  <div className="cdc-key-settings-grid">
                    <label>
                      业务唯一键（可选，每行一张表）
                      <textarea
                        value={cdcBusinessKeysText}
                        onChange={(event) =>
                          setCdcBusinessKeysText(event.target.value)
                        }
                        placeholder={"ORDERS=TENANT_ID,ORDER_NO\nCUSTOMER=CODE"}
                      />
                      <small>字段必须全非 NULL，且组合值不能重复</small>
                    </label>
                    <label>
                      最后兜底策略
                      <select
                        value={cdcNoKeyPolicy}
                        onChange={(event) =>
                          setCdcNoKeyPolicy(
                            event.target.value as CdcNoKeyPolicy,
                          )
                        }
                      >
                        <option value="reject">拒绝无可靠键的表（推荐）</option>
                        <option value="all_columns">
                          ALL COLUMNS 匹配（高风险）
                        </option>
                      </select>
                      <small>
                        ALL COLUMNS 不使用 LOB 定位；重复行会导致任务安全失败
                      </small>
                    </label>
                    <label className="cdc-source-ddl-toggle">
                      <input
                        type="checkbox"
                        checked={cdcAllowSourceDdl}
                        onChange={(event) =>
                          setCdcAllowSourceDdl(event.target.checked)
                        }
                      />
                      <span>
                        <b>允许自动补充源表日志组</b>
                        <small>
                          仅在现有补充日志不足时执行 Oracle ALTER TABLE；不开启则只检查、不修改源库
                        </small>
                      </span>
                    </label>
                  </div>
                  {cdcNoKeyPolicy === "all_columns" && (
                    <p className="cdc-risk-warning">
                      高风险模式：所有非 LOB 标量列共同定位旧行。表内必须无重复行；字段更新、字符空格和隐式类型转换均可能使定位失败。
                    </p>
                  )}
                </section>
              )}
              <div className="migration-content-field">
                <b>迁移内容</b>
                <div className="migration-content-control">
                  {(Object.keys(contentLabels) as MigrationContent[]).map(
                    (value) => (
                      <button
                        type="button"
                        key={value}
                        className={migrationContent === value ? "active" : ""}
                        disabled={
                          syncMode !== "full_only" && value === "structure_only"
                        }
                        onClick={() => chooseMigrationContent(value)}
                      >
                        {contentLabels[value]}
                      </button>
                    ),
                  )}
                </div>
              </div>
              <label className="existing-table-field">
                目标表已存在时
                <select
                  value={existingTable}
                  onChange={(event) => setExistingTable(event.target.value)}
                >
                  <option
                    value="fail"
                    disabled={migrationContent === "data_only"}
                  >
                    停止并报错（安全）
                  </option>
                  <option value="append">追加数据</option>
                  <option value="truncate">清空后重写</option>
                  <option
                    value="drop_and_create"
                    disabled={migrationContent === "data_only"}
                  >
                    删除并重建
                  </option>
                </select>
              </label>
              <label className="failure-policy-field">
                失败策略
                <select
                  value={failPolicy}
                  onChange={(event) =>
                    setFailPolicy(
                      event.target.value as
                        "stop_on_error" | "continue_on_error",
                    )
                  }
                >
                  <option value="stop_on_error">失败即停止（安全）</option>
                  <option value="continue_on_error">
                    失败继续（跳过失败表）
                  </option>
                </select>
              </label>
              {showIdentifierCaseSettings && (
                <section className="identifier-case-settings">
                  <div className="identifier-case-head">
                    <div>
                      <b>目标表与视图名称大小写</b>
                      <small>
                        只控制迁移对象命名，不修改 TDSQL
                        参数，也不影响字段名、数据排序规则和序列名
                      </small>
                    </div>
                    <span
                      className={
                        targetCaseCapabilities ? "detected" : "pending"
                      }
                    >
                      {targetCaseCapabilities
                        ? `已检测 lower_case_table_names=${targetCaseCapabilities.lower_case_table_names}`
                        : "启动任务时自动检测"}
                    </span>
                  </div>
                  <div className="identifier-case-options">
                    {(["auto", "preserve", "lower", "upper"] as const).map(
                      (policy) => (
                        <button
                          type="button"
                          key={policy}
                          className={
                            identifierCasePolicy === policy ? "active" : ""
                          }
                          disabled={
                            (policy === "preserve" || policy === "upper") &&
                            preserveUnavailable
                          }
                          onClick={() => setIdentifierCasePolicy(policy)}
                        >
                          <b>
                            {
                              {
                                auto: "自动适配（推荐）",
                                preserve: "保留源端大小写",
                                lower: "统一小写",
                                upper: "统一大写",
                              }[policy]
                            }
                          </b>
                          <small>
                            {
                              {
                                auto: "根据目标实例参数自动选择",
                                preserve: preserveUnavailable
                                  ? "目标以小写存储，当前不可用"
                                  : "目标名称与源端保持一致",
                                lower: "表和视图名称转换为小写",
                                upper: preserveUnavailable
                                  ? "目标以小写存储，当前不可用"
                                  : "表和视图名称转换为大写",
                              }[policy]
                            }
                          </small>
                        </button>
                      ),
                    )}
                  </div>
                  <div className="identifier-case-preview">
                    <span>
                      实际策略：<b>{resolvedIdentifierCasePolicy}</b>
                    </span>
                    {identifierNameMappings.length ? (
                      <span
                        title={identifierNameMappings
                          .map((item) => `${item.source} → ${item.target}`)
                          .join("；")}
                      >
                        将重命名 {identifierNameMappings.length} 个对象，例如：
                        {identifierNameMappings
                          .slice(0, 2)
                          .map((item) => `${item.source} → ${item.target}`)
                          .join("；")}
                      </span>
                    ) : (
                      <span>当前所选对象名称无需转换</span>
                    )}
                  </div>
                </section>
              )}
              <label>
                每批写入行数
                <input
                  type="number"
                  min="100"
                  max="20000"
                  step="100"
                  value={batchSize || ""}
                  onChange={(event) =>
                    setBatchSize(
                      event.target.value === ""
                        ? 0
                        : Number(event.target.value),
                    )
                  }
                  onBlur={() =>
                    setBatchSize(
                      Math.max(100, Math.min(20000, batchSize || 100)),
                    )
                  }
                />
              </label>
              <label>
                并发迁移表数
                <input
                  type="number"
                  min="1"
                  max="16"
                  value={tableConcurrency || ""}
                  onChange={(event) =>
                    setTableConcurrency(
                      event.target.value === ""
                        ? 0
                        : Number(event.target.value),
                    )
                  }
                  onBlur={() =>
                    setTableConcurrency(
                      Math.max(1, Math.min(16, tableConcurrency || 1)),
                    )
                  }
                />
              </label>
              <div className="execution-warning">
                ⚠{" "}
                {migrationContent === "structure_only"
                  ? "仅创建目标表，不读取或写入业务数据。"
                  : migrationContent === "data_only"
                    ? "目标表必须已存在，字段需与源表兼容。"
                    : syncMode === "full_and_incremental"
                      ? "将先记录 Oracle SCN 并完成全量，随后自动进入 LogMiner 增量追平和实时同步；停止任务前会持续运行。"
                      : syncMode === "full_then_incremental"
                        ? "将先保存 Oracle SCN 并执行全量；全量全部成功后，可从任务详情手动启动独立增量任务。"
                      : "迁移期间请勿修改源表结构。清空和删除策略会改变目标库数据。"}
              </div>
            </div>
            <div className="builder-actions">
              <label
                className="sequence-migrate-toggle"
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "6px",
                  fontSize: 13,
                  cursor: "pointer",
                  flexWrap: "wrap",
                }}
                title="目标为 TDSQL 时，将 Oracle 序列转换为 TDSQL 序列语法（create tdsql_sequence）并先于表迁移"
              >
                <input
                  type="checkbox"
                  checked={migrateSequences}
                  onChange={(event) =>
                    setMigrateSequences(event.target.checked)
                  }
                />
                迁移序列
                <small>
                  {migrateSequences
                    ? `将迁移 ${selectedSequences.length} 个序列`
                    : "不迁移序列"}
                </small>
              </label>
              <span>
                执行顺序：
                {migrateSequences && selectedSequences.length
                  ? `${selectedSequences.length} 个序列全部完成后，`
                  : ""}
                {selectedTables.length} 张表（含分区表）全部完成后，再迁移{" "}
                {selectedViews.length} 个视图；阶段内最多{" "}
                {Math.min(
                  tableConcurrency,
                  Math.max(
                    selectedTables.length,
                    selectedViews.length,
                    migrateSequences ? selectedSequences.length : 0,
                    1,
                  ),
                )}{" "}
                个对象并发
              </span>
              <div className="builder-button-group">
                {activeNav === "links" && linkWizardStep === 3 && (
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => setLinkWizardStep(2)}
                  >
                    ← 上一步
                  </button>
                )}
                <button
                  className="secondary-button assessment-button"
                  disabled={assessing || !selected.length || !activeLinkId}
                  onClick={assessMigration}
                >
                  {assessing ? "正在读取元数据与行数…" : "迁移前评估"}
                </button>
                <button
                  className="secondary-button assessment-button"
                  disabled={deepAssessing || !activeLinkId}
                  onClick={runDeepAssessment}
                  title="对源库做 DBA 级深度评估：实例/参数/对象/数据量/外键/耗时/安全/性能"
                >
                  {deepAssessing ? "正在深度评估…" : "深度评估"}
                </button>
                <label
                  className="deep-bandwidth-input"
                  title="用于耗时估算的网络带宽假设"
                >
                  带宽
                  <input
                    type="number"
                    min="1"
                    max="10000"
                    value={deepBandwidthMbps || ""}
                    disabled={deepAssessing}
                    onChange={(event) =>
                      setDeepBandwidthMbps(
                        event.target.value === ""
                          ? 0
                          : Number(event.target.value),
                      )
                    }
                    onBlur={() =>
                      setDeepBandwidthMbps(
                        Math.max(1, Math.min(10000, deepBandwidthMbps || 50)),
                      )
                    }
                  />
                  Mbps
                </label>
                <button
                  className="continue-button"
                  disabled={
                    creating ||
                    (!createdJobId &&
                      ((!selected.length &&
                        !(migrateSequences && selectedSequences.length)) ||
                        !activeLinkId))
                  }
                  onClick={() =>
                    createdJobId
                      ? window.location.assign(`/tasks#job-${createdJobId}`)
                      : startMigration()
                  }
                >
                  {creating
                    ? "正在启动迁移…"
                    : createdJobId
                      ? "查看迁移进度 →"
                      : "开始迁移"}
                </button>
              </div>
            </div>
          </section>

          <section className="recent-section" id="tasks">
            <div className="section-title-row">
              <div>
                <h3>生产迁移任务</h3>
                <p>
                  共 {jobTotal} 个任务；生产任务与连接链路独立管理，页面每 3
                  秒更新
                </p>
              </div>
              <button onClick={refreshJobs}>↻ 立即刷新</button>
            </div>
            <div className="task-table">
              <div className="table-row table-head">
                <span>任务名称</span>
                <span>迁移链路</span>
                <span>状态</span>
                <span>进度</span>
                <span>处理数据</span>
                <span>当前表</span>
                <span />
              </div>
              {jobs.length ? (
                jobs.map((job) => (
                  <div className="table-row" key={job.id} id={`job-${job.id}`}>
                    <span className="task-name">
                      <i>⇄</i>
                      <span>
                        <b>{job.name}</b>
                        <small>
                          {
                            contentLabels[
                              job.migration_content || "structure_and_data"
                            ]
                          }{" "}
                          · {new Date(job.created_at).toLocaleString("zh-CN")}
                          {job.link_name
                            ? ` · 链路 ${job.link_name}`
                            : " · 历史直连"}
                        </small>
                      </span>
                    </span>
                    <span className="task-route">
                      <DatabaseBadge type={job.source_type} small />
                      <em>→</em>
                      <DatabaseBadge type={job.target_type} small />
                    </span>
                    <span>
                      <i className={`task-status ${job.status}`}>
                        {job.status === "syncing"
                          ? "● 实时同步"
                          : job.status === "catching_up"
                            ? "◌ 追增量"
                            : job.status === "running"
                          ? "◌ 迁移中"
                          : job.status === "completed"
                            ? job.sync_mode === "incremental_only" &&
                              job.sync_phase === "stopped"
                              ? "✓ 同步已结束"
                              : "✓ 已完成"
                            : job.status === "failed"
                              ? "× 失败"
                              : job.status === "cancelled"
                                ? "— 已取消"
                                : "• 等待中"}
                      </i>
                    </span>
                    <span className="progress-cell">
                      <span
                        className={
                          ["queued", "running", "catching_up"].includes(job.status) &&
                          job.progress === 0
                            ? "indeterminate"
                            : ""
                        }
                      >
                        <i style={{ width: `${job.progress}%` }} />
                      </span>
                      <b>{Math.round(job.progress)}%</b>
                    </span>
                    <span>
                      <b className="speed">
                        累计 {formatRows(job.rows_copied + (job.cdc_events || 0))} 条
                      </b>
                      <small>
                        {job.sync_mode === "incremental_only"
                          ? `增量 ${formatRows(job.cdc_events || 0)}`
                          : `全量 ${formatRows(job.rows_copied)} · 增量 ${formatRows(job.cdc_events || 0)}`}
                      </small>
                    </span>
                    <span title={job.error || ""}>
                      <b className="speed">
                        {job.current_table ||
                          (job.status === "queued"
                            ? "等待迁移节点调度"
                            : `${job.tables_completed}/${job.tables_total} 个对象`)}
                      </b>
                      {job.error && (
                        <button
                          className="job-error"
                          onClick={() => setSelectedJob(job)}
                        >
                          {job.error.split("\n")[0]}
                        </button>
                      )}
                    </span>
                    <span className="job-actions">
                      <button
                        className="more"
                        onClick={() =>
                          setOpenJobMenu((current) =>
                            current === job.id ? "" : job.id,
                          )
                        }
                        aria-label={`任务 ${job.name} 更多操作`}
                      >
                        •••
                      </button>
                      {openJobMenu === job.id && (
                        <span className="job-menu">
                          <button
                            onClick={() => {
                              setSelectedJob(job);
                              setOpenJobMenu("");
                            }}
                          >
                            查看详情与日志
                          </button>
                          {["failed", "cancelled"].includes(job.status) && (
                            <button onClick={() => openEditJob(job)}>
                              编辑并重试
                            </button>
                          )}
                          {["failed", "cancelled"].includes(job.status) && (
                            <button onClick={() => retryJob(job)}>
                              重新执行
                            </button>
                          )}
                          {["catching_up", "syncing"].includes(job.status) && (
                            <button
                              onClick={() => {
                                setOpenJobMenu("");
                                setFinishConfirmJob(job);
                              }}
                            >
                              结束同步
                            </button>
                          )}
                          {job.sync_mode === "incremental_only" &&
                            ["completed", "cancelled"].includes(job.status) &&
                            job.sync_phase === "stopped" &&
                            !!job.checkpoint_scn && (
                              <button
                                onClick={() => {
                                  setOpenJobMenu("");
                                  setResumeConfirmJob(job);
                                }}
                              >
                                继续增量同步
                              </button>
                            )}
                          {["queued", "running"].includes(job.status) && (
                            <button
                              className="danger"
                              onClick={() => {
                                setOpenJobMenu("");
                                setCancelConfirmJob(job);
                              }}
                            >
                              取消任务
                            </button>
                          )}
                          {job.status === "completed" &&
                            job.migration_content !== "structure_only" && (
                              <button
                                onClick={() => {
                                  setValidationJob(job.id);
                                  setOpenJobMenu("");
                                  navigateTo("validation", "validation");
                                }}
                              >
                                执行迁移校验
                              </button>
                            )}
                        </span>
                      )}
                    </span>
                  </div>
                ))
              ) : (
                <div className="empty-jobs">
                  暂无迁移任务。完成连接配置并选择表后即可开始。
                </div>
              )}
            </div>
            <div className="pagination">
              <span>
                第 {jobPage} / {jobPages} 页
              </span>
              <label>
                每页
                <select
                  value={jobPageSize}
                  onChange={(event) => {
                    setJobPageSize(Number(event.target.value));
                    setJobPage(1);
                  }}
                >
                  <option value="5">5</option>
                  <option value="10">10</option>
                  <option value="20">20</option>
                  <option value="50">50</option>
                </select>
                条
              </label>
              <button disabled={jobPage <= 1} onClick={() => setJobPage(1)}>
                首页
              </button>
              <button
                disabled={jobPage <= 1}
                onClick={() => setJobPage((value) => value - 1)}
              >
                上一页
              </button>
              <button
                disabled={jobPage >= jobPages}
                onClick={() => setJobPage((value) => value + 1)}
              >
                下一页
              </button>
              <button
                disabled={jobPage >= jobPages}
                onClick={() => setJobPage(jobPages)}
              >
                末页
              </button>
            </div>
          </section>

          <section className="validation-section" id="validation">
            <div className="section-title-row">
              <div>
                <h3>迁移数据校验</h3>
                <p>
                  不一致时输出差异类型、字段、主键、源值与目标值；最多展示前 20
                  行明细
                </p>
              </div>
            </div>
            <div className="validation-controls">
              <label>
                选择当前页已完成任务
                <select
                  value={
                    validationJob ||
                    jobs.find(
                      (job) =>
                        job.status === "completed" &&
                        job.migration_content !== "structure_only",
                    )?.id ||
                    ""
                  }
                  onChange={(event) => {
                    setValidationJob(event.target.value);
                    setValidationResult(null);
                    setValidationTableFilter("all");
                  }}
                >
                  <option value="">请选择任务</option>
                  {jobs
                    .filter(
                      (job) =>
                        job.status === "completed" &&
                        job.migration_content !== "structure_only",
                    )
                    .map((job) => (
                      <option key={job.id} value={job.id}>
                        {job.name}
                      </option>
                    ))}
                </select>
              </label>
              <button
                className="continue-button"
                disabled={
                  validating ||
                  !jobs.some(
                    (job) =>
                      job.status === "completed" &&
                      job.migration_content !== "structure_only",
                  )
                }
                onClick={validateMigration}
              >
                {validating ? "正在逐行比对…" : "开始数据库级校验"}
              </button>
            </div>
            {validationResult ? (
              <div className="validation-results" id="validation-report">
                <div className="validation-report-meta">
                  <span>
                    校验 ID：
                    <b>#{validationResult.id.slice(0, 8).toUpperCase()}</b>
                  </span>
                  <span>任务：{validationResult.job_name}</span>
                  <span>
                    检查时间：
                    {new Date(validationResult.created_at).toLocaleString(
                      "zh-CN",
                    )}
                  </span>
                </div>
                <div
                  className={`validation-summary ${validationResult.passed ? "passed" : "failed"}`}
                >
                  <span>{validationResult.passed ? "✓" : "!"}</span>
                  <div>
                    <b>{validationResult.passed ? "校验通过" : "发现不一致"}</b>
                    <small>
                      {validationResult.tables.length} 张表已完成检查
                      {validationResult.duration_ms != null
                        ? ` · 耗时 ${(validationResult.duration_ms / 1000).toFixed(2)} 秒 · 并发 ${validationResult.concurrency || 1}`
                        : ""}
                    </small>
                  </div>
                  <label className="validation-table-filter">
                    <span>筛选结果</span>
                    <select
                      value={validationTableFilter}
                      onChange={(event) =>
                        setValidationTableFilter(
                          event.target.value as "all" | "passed" | "failed",
                        )
                      }
                    >
                      <option value="all">全部</option>
                      <option value="passed">一致</option>
                      <option value="failed">不一致</option>
                    </select>
                  </label>
                </div>
                {validationResult.tables
                  .filter(
                    (table) =>
                      validationTableFilter === "all" ||
                      (validationTableFilter === "passed" && table.passed) ||
                      (validationTableFilter === "failed" && !table.passed),
                  )
                  .map((table) => (
                  <div className="validation-table-result" key={table.table}>
                    <div className="validation-row">
                      <span>
                        <b>{table.table}</b>
                        <small>
                          {table.name_case_preserved
                            ? "表名一致"
                            : `目标名：${table.target_table}`}
                        </small>
                      </span>
                      <span>
                        <b>
                          {table.source_rows} / {table.target_rows}
                        </b>
                        <small>源端 / 目标端行数</small>
                      </span>
                      <span>
                        <b>
                          {table.hash_mode === "full"
                            ? `${table.rows_hashed} 行全字段`
                            : "仅行数"}
                        </b>
                        <small>
                          {table.difference_rows
                            ? `${table.difference_rows} 行有差异`
                            : "校验范围"}
                        </small>
                      </span>
                      <i className={table.passed ? "pass" : "fail"}>
                        {table.passed ? "✓ 一致" : "× 不一致"}
                      </i>
                    </div>
                    {!table.passed && (
                      <div className="difference-panel">
                        <div className="difference-reasons">
                          {table.difference_types.map((item) => (
                            <span key={`${item.type}-${item.message}`}>
                              ! {item.message}
                            </span>
                          ))}
                        </div>
                        {Object.keys(table.column_difference_counts || {})
                          .length > 0 && (
                          <div className="column-difference-counts">
                            <b>差异字段统计</b>
                            {Object.entries(table.column_difference_counts).map(
                              ([name, count]) => (
                                <span key={name}>
                                  {name}：{count} 行
                                </span>
                              ),
                            )}
                          </div>
                        )}
                        {table.difference_samples?.map((sample) => (
                          <details
                            key={sample.row_index}
                            className="difference-sample"
                          >
                            <summary>
                              第 {sample.row_index} 行
                              {Object.keys(sample.primary_key).length
                                ? ` · 主键 ${Object.entries(sample.primary_key)
                                    .map(
                                      ([key, value]) =>
                                        `${key}=${displayCanonical(value)}`,
                                    )
                                    .join(", ")}`
                                : ""}{" "}
                              · {sample.columns.length} 个字段不同
                            </summary>
                            <div>
                              {sample.columns.map((column) => (
                                <div
                                  className="difference-value"
                                  key={column.column}
                                >
                                  <b>{column.column}</b>
                                  <span>
                                    <i>源端</i>
                                    <code>
                                      {displayCanonical(column.source)}
                                    </code>
                                  </span>
                                  <span>
                                    <i>目标端</i>
                                    <code>
                                      {displayCanonical(column.target)}
                                    </code>
                                  </span>
                                </div>
                              ))}
                            </div>
                          </details>
                        ))}
                      </div>
                    )}
                  </div>
                  ))}
                {!validationResult.tables.some(
                  (table) =>
                    validationTableFilter === "all" ||
                    (validationTableFilter === "passed" && table.passed) ||
                    (validationTableFilter === "failed" && !table.passed),
                ) && (
                  <div className="validation-report-empty">
                    当前报告中没有
                    {validationTableFilter === "passed" ? "一致" : "不一致"}
                    的表。
                  </div>
                )}
              </div>
            ) : (
              <div className="validation-empty">
                选择一个已完成任务后，可重新连接源库和目标库进行独立校验。
              </div>
            )}
            <div className="validation-history">
              <div className="validation-history-head">
                <div>
                  <h4>校验历史记录</h4>
                  <p>
                    共 {validationHistoryTotal}{" "}
                    次检查；记录保留当时的逐表结果和差异明细
                  </p>
                </div>
                <div className="validation-filter" aria-label="校验结果筛选">
                  {(
                    [
                      ["all", "全部"],
                      ["passed", "一致"],
                      ["failed", "不一致"],
                    ] as const
                  ).map(([value, label]) => (
                    <button
                      key={value}
                      className={
                        validationHistoryFilter === value ? "active" : ""
                      }
                      onClick={() => {
                        setValidationHistoryFilter(value);
                        setValidationHistoryPage(1);
                      }}
                    >
                      {label}
                    </button>
                  ))}
                </div>
              </div>
              {validationHistory.length ? (
                <div className="validation-history-list">
                  {validationHistory.map((record) => (
                    <button
                      key={record.id}
                      className={`validation-history-row ${record.passed ? "passed" : "failed"} ${validationResult?.id === record.id ? "selected" : ""}`}
                      onClick={() => void openValidationRecord(record.id)}
                      disabled={loadingValidationRecord === record.id}
                    >
                      <span className="validation-record-id">
                        #{record.id.slice(0, 8).toUpperCase()}
                      </span>
                      <span>
                        <b>{record.job_name}</b>
                        <small>
                          {new Date(record.created_at).toLocaleString("zh-CN")}
                        </small>
                      </span>
                      <span className="validation-record-counts">
                        <b>{record.table_count} 张表</b>
                        <small>
                          {record.consistent_count} 一致 ·{" "}
                          {record.inconsistent_count} 不一致
                        </small>
                      </span>
                      <i>{record.passed ? "✓ 一致" : "× 不一致"}</i>
                      <em>
                        {loadingValidationRecord === record.id
                          ? "读取中…"
                          : "查看报告 →"}
                      </em>
                    </button>
                  ))}
                </div>
              ) : (
                <div className="validation-history-empty">
                  {validationHistoryFilter === "all"
                    ? "尚无校验历史，完成第一次校验后会自动保存在这里。"
                    : `没有${validationHistoryFilter === "passed" ? "一致" : "不一致"}的校验记录。`}
                </div>
              )}
              <div className="pagination validation-history-pagination">
                <label>
                  每页
                  <select
                    value={validationHistoryPageSize}
                    onChange={(event) => {
                      setValidationHistoryPageSize(Number(event.target.value));
                      setValidationHistoryPage(1);
                    }}
                  >
                    {[5, 10, 20].map((size) => (
                      <option value={size} key={size}>
                        {size} 条
                      </option>
                    ))}
                  </select>
                </label>
                <span>
                  第 {validationHistoryPage} / {validationHistoryPages} 页
                </span>
                <button
                  disabled={validationHistoryPage <= 1}
                  onClick={() => setValidationHistoryPage(1)}
                >
                  首页
                </button>
                <button
                  disabled={validationHistoryPage <= 1}
                  onClick={() =>
                    setValidationHistoryPage((page) => Math.max(1, page - 1))
                  }
                >
                  上一页
                </button>
                <button
                  disabled={validationHistoryPage >= validationHistoryPages}
                  onClick={() =>
                    setValidationHistoryPage((page) =>
                      Math.min(validationHistoryPages, page + 1),
                    )
                  }
                >
                  下一页
                </button>
                <button
                  disabled={validationHistoryPage >= validationHistoryPages}
                  onClick={() =>
                    setValidationHistoryPage(validationHistoryPages)
                  }
                >
                  末页
                </button>
              </div>
            </div>
          </section>

          <section className="node-management">
            <div className="management-head">
              <div>
                <span className="management-icon">♧</span>
                <div>
                  <h2>运行节点</h2>
                  <p>查看迁移执行节点的在线状态、任务负载和最近检测结果</p>
                </div>
              </div>
              <button className="secondary-button" onClick={refreshJobs}>
                ↻ 立即检测
              </button>
            </div>
            <div className="node-overview-grid">
              <div className={nodeOnline ? "healthy" : "unhealthy"}>
                <small>节点状态</small>
                <b>{nodeOnline ? "在线" : "离线"}</b>
                <span>
                  {nodeOnline ? "API 与任务调度正常" : "无法访问任务接口"}
                </span>
              </div>
              <div>
                <small>节点地址</small>
                <b>{apiBase || "当前服务器（同源）"}</b>
                <span>数据库连接和迁移都由此节点执行</span>
              </div>
              <div>
                <small>当前任务负载</small>
                <b>{running} 个运行中</b>
                <span>
                  任务总数 {jobTotal}，已完成 {completed}
                </span>
              </div>
              <div>
                <small>最近检测</small>
                <b>{lastNodeCheck}</b>
                <span>页面每 {jobRefreshSeconds} 秒自动刷新</span>
              </div>
            </div>
            <div className="node-scope-note">
              <b>运行节点负责什么？</b>
              <span>
                连接数据库、调度并发任务、批量写入、记录日志和迁移进度。
              </span>
              <button onClick={() => navigateTo("settings")}>
                配置节点连接 →
              </button>
            </div>
          </section>

          <section className="system-settings-page">
            <div className="management-head">
              <div>
                <span className="management-icon">⚙</span>
                <div>
                  <h2>系统设置</h2>
                  <p>配置管理页面连接迁移节点的方式，以及新任务的默认参数</p>
                </div>
              </div>
            </div>
            <div className="settings-groups">
              <div className="settings-group">
                <div>
                  <h3>迁移节点连接</h3>
                  <p>
                    同机部署时 API 地址留空；分开部署时填写后端 HTTPS 地址。
                  </p>
                </div>
                <label>
                  API 地址
                  <input
                    value={apiBase}
                    onChange={(event) => {
                      setApiBase(event.target.value);
                      setNodeTestResult(null);
                    }}
                    placeholder="例如 https://migration-api.example.com"
                  />
                </label>
                <label>
                  访问令牌
                  <input
                    type="password"
                    value={apiToken}
                    onChange={(event) => {
                      setApiToken(event.target.value);
                      setNodeTestResult(null);
                    }}
                    placeholder="FLOWDB_API_TOKEN"
                    autoComplete="off"
                  />
                </label>
              </div>
              <div className="settings-group">
                <div>
                  <h3>任务默认值</h3>
                  <p>创建新迁移任务时自动带入，可在具体任务中再次修改。</p>
                </div>
                <label>
                  默认每批写入行数
                  <input
                    type="number"
                    min="100"
                    max="20000"
                    value={batchSize || ""}
                    onChange={(event) =>
                      setBatchSize(
                        event.target.value === ""
                          ? 0
                          : Number(event.target.value),
                      )
                    }
                  />
                </label>
                <label>
                  默认并发迁移表数
                  <input
                    type="number"
                    min="1"
                    max="16"
                    value={tableConcurrency || ""}
                    onChange={(event) =>
                      setTableConcurrency(
                        event.target.value === ""
                          ? 0
                          : Number(event.target.value),
                      )
                    }
                  />
                </label>
                <label>
                  任务列表刷新频率
                  <select
                    value={jobRefreshSeconds}
                    onChange={(event) =>
                      setJobRefreshSeconds(Number(event.target.value))
                    }
                  >
                    <option value="1">每 1 秒</option>
                    <option value="3">每 3 秒</option>
                    <option value="5">每 5 秒</option>
                    <option value="10">每 10 秒</option>
                  </select>
                </label>
              </div>
            </div>
            <div className="settings-savebar">
              <div className="settings-feedback">
                <span>
                  设置仅保存在当前浏览器；数据库密码仍由服务器加密保存。
                </span>
                {nodeTestResult ? (
                  <strong className={nodeTestResult.status} role="status">
                    {nodeTestResult.status === "success" ? "✓ " : "× "}
                    {nodeTestResult.message}
                  </strong>
                ) : null}
              </div>
              <div>
                <button
                  className="secondary-button"
                  onClick={testNodeConnection}
                  disabled={testingNode}
                >
                  {testingNode ? "正在测试…" : "测试节点连接"}
                </button>
                <button
                  className="continue-button"
                  onClick={() => {
                    const cleanApiBase = apiBase.trim().replace(/\/$/, "");
                    const cleanToken = apiToken.trim();
                    const cleanBatch = Math.max(
                      100,
                      Math.min(20000, batchSize || 100),
                    );
                    const cleanConcurrency = Math.max(
                      1,
                      Math.min(16, tableConcurrency || 1),
                    );
                    setApiBase(cleanApiBase);
                    setApiToken(cleanToken);
                    setBatchSize(cleanBatch);
                    setTableConcurrency(cleanConcurrency);
                    window.localStorage.setItem("flowdb_api", cleanApiBase);
                    window.localStorage.setItem("flowdb_token", cleanToken);
                    window.localStorage.setItem(
                      "flowdb_default_batch",
                      String(cleanBatch),
                    );
                    window.localStorage.setItem(
                      "flowdb_default_concurrency",
                      String(cleanConcurrency),
                    );
                    window.localStorage.setItem(
                      "flowdb_refresh_seconds",
                      String(jobRefreshSeconds),
                    );
                    setNotice("系统设置已保存");
                    window.setTimeout(() => setNotice(""), 3500);
                    window.setTimeout(refreshJobs, 0);
                  }}
                >
                  保存系统设置
                </button>
              </div>
            </div>
          </section>

          <section className="node-section" id="node-status">
            <div className="node-status-icon">♧</div>
            <div>
              <h3>迁移运行节点</h3>
              <p>{apiBase || "当前服务器"}</p>
            </div>
            <span className={nodeOnline ? "online" : "offline"}>
              ●{" "}
              {nodeOnline
                ? "在线，任务调度正常"
                : "未连接，请检查节点地址和令牌"}
            </span>
            <button
              className="secondary-button"
              onClick={() => navigateTo("settings")}
            >
              系统设置
            </button>
          </section>
        </div>
      </section>

      {showAssessment && !assessment && (
        <div
          className="modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) closeAssessmentModal();
          }}
        >
          <div
            className="assessment-modal assessment-progress-modal"
            role="dialog"
            aria-modal="true"
            aria-label="迁移前评估进度"
          >
            <button className="modal-close" onClick={closeAssessmentModal}>×</button>
            <div className={`assessment-progress-state ${assessmentError ? "failed" : ""}`}>
              <span className={assessmentError ? "assessment-progress-error" : "assessment-spinner"}>
                {assessmentError ? "!" : ""}
              </span>
              <h3>{assessmentError ? "迁移前评估未完成" : "正在执行迁移前评估"}</h3>
              <p>
                {assessmentError || "正在并行读取对象字段、主键、行数与目标端兼容性，请稍候…"}
              </p>
              {assessmentError && (
                <button className="continue-button" onClick={assessMigration}>
                  重新评估
                </button>
              )}
            </div>
          </div>
        </div>
      )}
      {showAssessment && assessment && (
        <div
          className="modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) closeAssessmentModal();
          }}
        >
          <div
            className="assessment-modal"
            role="dialog"
            aria-modal="true"
            aria-label="迁移前评估报告"
          >
            <button
              className="modal-close"
              onClick={closeAssessmentModal}
            >
              ×
            </button>
            <div className="assessment-head">
              <span
                className={`assessment-score ${assessment.ready ? "ready" : "blocked"}`}
              >
                {assessment.score}
              </span>
              <div>
                <h3>迁移前评估报告</h3>
                <p>
                  {assessment.ready
                    ? "未发现阻断项，可以开始迁移"
                    : "发现阻断项，请处理后再迁移"}
                </p>
              </div>
            </div>
            <div className="assessment-summary">
              <div>
                <b>{assessment.summary.tables}</b>
                <small>表数量</small>
              </div>
              <div>
                <b>{formatRows(assessment.summary.rows)}</b>
                <small>预计行数</small>
              </div>
              <div>
                <b>{formatBytes(assessment.summary.estimated_bytes)}</b>
                <small>估算数据量</small>
              </div>
              <div>
                <b>{assessment.summary.table_concurrency}</b>
                <small>表并发</small>
              </div>
              <div className={assessment.summary.blocking ? "bad" : ""}>
                <b>{assessment.summary.blocking}</b>
                <small>阻断项</small>
              </div>
              <div className={assessment.summary.warnings ? "warn" : ""}>
                <b>{assessment.summary.warnings}</b>
                <small>警告</small>
              </div>
            </div>
            <div className="assessment-tables">
              {assessment.tables.map((table) => (
                <details key={table.table} open={table.blocking_count > 0}>
                  <summary>
                    <span>
                      <b>{table.table}</b>
                      <small>
                        {formatRows(table.rows)} 行 · {table.columns} 列 · 约{" "}
                        {formatBytes(table.estimated_bytes)}
                      </small>
                    </span>
                    <span
                      className={
                        table.blocking_count
                          ? "blocked"
                          : table.warning_count
                            ? "warning"
                            : "ready"
                      }
                    >
                      {table.blocking_count
                        ? `${table.blocking_count} 个阻断`
                        : table.warning_count
                          ? `${table.warning_count} 个警告`
                          : "✓ 兼容"}
                    </span>
                  </summary>
                  <div className="assessment-table-body">
                    <div className="assessment-facts">
                      <span>主键：{table.primary_keys.join(", ") || "无"}</span>
                      <span>
                        目标表：
                        {table.target_exists
                          ? `已存在${table.target_name ? `（${table.target_name}）` : ""}`
                          : "不存在，将创建"}
                      </span>
                    </div>
                    {table.risks.length > 0 && (
                      <div className="assessment-risks">
                        {table.risks.map((risk) => (
                          <div
                            className={risk.level}
                            key={`${risk.code}-${risk.message}`}
                          >
                            <b>
                              {risk.level === "blocking" ? "阻断" : "警告"} ·{" "}
                              {risk.code}
                            </b>
                            <span>{risk.message}</span>
                          </div>
                        ))}
                      </div>
                    )}
                    <div className="mapping-table">
                      <div>
                        <b>字段</b>
                        <b>源类型</b>
                        <b>目标类型</b>
                        <b>属性</b>
                      </div>
                      {table.column_mappings.map((column) => (
                        <div key={column.column}>
                          <span>{column.column}</span>
                          <code>{column.source_type}</code>
                          <code>
                            {column.target_type}
                            {column.degraded && (
                              <em
                                className="mapping-degraded"
                                title={column.degradation ?? ""}
                              >
                                ⚠
                              </em>
                            )}
                          </code>
                          <span>
                            {column.identity ? "自增 " : ""}
                            {column.nullable ? "可空" : "非空"}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                </details>
              ))}
            </div>
            <div className="modal-actions">
              <button
                className="secondary-button"
                onClick={closeAssessmentModal}
              >
                返回修改
              </button>
              <button
                className="continue-button"
                disabled={!assessment.ready || creating}
                onClick={startMigration}
              >
                {assessment.ready
                  ? creating
                    ? "正在创建…"
                    : "确认并开始迁移"
                  : "存在阻断项，不能迁移"}
              </button>
            </div>
          </div>
        </div>
      )}
      {showDeepAssessment && !deepAssessment && (
        <div
          className="modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) closeDeepAssessmentModal();
          }}
        >
          <div
            className="assessment-modal assessment-progress-modal"
            role="dialog"
            aria-modal="true"
            aria-label="DBA 深度评估进度"
          >
            <button className="modal-close" onClick={closeDeepAssessmentModal}>×</button>
            <div className={`assessment-progress-state ${deepAssessmentError ? "failed" : ""}`}>
              <span className={deepAssessmentError ? "assessment-progress-error" : "assessment-spinner"}>
                {deepAssessmentError ? "!" : ""}
              </span>
              <h3>{deepAssessmentError ? "深度评估未完成" : "正在执行 DBA 深度评估"}</h3>
              <p>
                {deepAssessmentError || "正在并行检查源端与目标端环境、对象规模、数据质量、性能和安全配置…"}
              </p>
              {deepAssessmentError && (
                <button className="continue-button" onClick={runDeepAssessment}>
                  重新评估
                </button>
              )}
            </div>
          </div>
        </div>
      )}
      {showDeepAssessment && deepAssessment && (
        <div
          className="modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget)
              closeDeepAssessmentModal();
          }}
        >
          <div
            className="assessment-modal deep-modal"
            role="dialog"
            aria-modal="true"
            aria-label="DBA 深度评估报告"
          >
            <button
              className="modal-close"
              onClick={closeDeepAssessmentModal}
            >
              ×
            </button>
            <div className="assessment-head">
              <span
                className={`assessment-score ${deepAssessment.ready ? "ready" : "blocked"}`}
              >
                {deepAssessment.score}
              </span>
              <div>
                <h3>DBA 深度评估报告</h3>
                <p>
                  {deepAssessment.ready
                    ? "未发现阻断项，可以开始迁移"
                    : "发现阻断项，请处理后再迁移"}
                  {" · "}
                  {new Date(deepAssessment.generated_at).toLocaleString(
                    "zh-CN",
                  )}
                </p>
              </div>
              <div className="deep-export-area">
                <button
                  className="secondary-button"
                  disabled={deepExporting}
                  onClick={exportDeepReport}
                >
                  {deepExporting ? "正在导出…" : "导出报告"}
                </button>
                {deepExport && (
                  <button
                    type="button"
                    className="deep-export-link"
                    onClick={() => {
                      if (!deepExport) return;
                      const fullUrl = `${apiBase.trim().replace(/\/$/, "")}${deepExport.download_url}`;
                      fetch(fullUrl, {
                        headers: apiToken.trim()
                          ? { "X-FlowDB-Token": apiToken.trim() }
                          : {},
                      })
                        .then((resp) => {
                          if (!resp.ok)
                            throw new Error(
                              `报告下载失败（HTTP ${resp.status}）`,
                            );
                          return resp.blob();
                        })
                        .then((blob) => {
                          const objectUrl = URL.createObjectURL(blob);
                          const anchor = document.createElement("a");
                          anchor.href = objectUrl;
                          anchor.download = deepExport.file_name;
                          document.body.appendChild(anchor);
                          anchor.click();
                          anchor.remove();
                          URL.revokeObjectURL(objectUrl);
                        })
                        .catch((reason) =>
                          setError(
                            reason instanceof Error
                              ? reason.message
                              : "报告下载失败",
                          ),
                        );
                    }}
                  >
                    {deepExport.file_name}（下载）
                  </button>
                )}
              </div>
            </div>
            <div className="assessment-summary deep-summary">
              <div className={deepAssessment.summary.blocking ? "bad" : ""}>
                <b>{deepAssessment.summary.blocking}</b>
                <small>阻断项</small>
              </div>
              <div className={deepAssessment.summary.warnings ? "warn" : ""}>
                <b>{deepAssessment.summary.warnings}</b>
                <small>警告</small>
              </div>
              <div>
                <b>{deepAssessment.summary.info}</b>
                <small>提示</small>
              </div>
              <div>
                <b>{deepAssessment.source.env.dialect}</b>
                <small>源端类型</small>
              </div>
              <div>
                <b>{deepAssessment.target.env.dialect}</b>
                <small>目标端类型</small>
              </div>
              <div>
                <b>{deepAssessment.notes.length}</b>
                <small>降级说明</small>
              </div>
            </div>

            {deepAssessment.conclusion && (
              <div className="deep-section">
                <h4>迁移结论与待办</h4>
                <div className="deep-conclusion-overall">
                  <span
                    className={
                      deepAssessment.conclusion.overall.ready ? "ok" : "bad"
                    }
                  >
                    {deepAssessment.conclusion.overall.ready
                      ? "就绪"
                      : "未就绪"}
                  </span>
                  <p>{deepAssessment.conclusion.overall.statement}</p>
                </div>
                <div className="deep-conclusion-dims">
                  {deepAssessment.conclusion.dimensions.map((dimension) => (
                    <div
                      key={dimension.name}
                      className={`deep-conclusion-dim ${dimension.level}`}
                    >
                      <b>
                        {dimension.name}
                        <span>
                          {dimension.level === "ok"
                            ? "正常"
                            : dimension.level === "warning"
                              ? "警告"
                              : "阻断"}
                        </span>
                      </b>
                      <p>{dimension.summary}</p>
                      {dimension.action_items.length > 0 && (
                        <ul>
                          {dimension.action_items.map((item, index) => (
                            <li key={`${dimension.name}-${index}`}>
                              <em>{item.priority}</em>
                              <span>{item.task}</span>
                              <small>{item.owner}</small>
                            </li>
                          ))}
                        </ul>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}

            {deepAssessment.risks.length > 0 && (
              <div className="deep-section">
                <h4>风险清单</h4>
                <div className="assessment-risks">
                  {deepAssessment.risks.map((risk) => (
                    <div
                      className={risk.level === "info" ? "warning" : risk.level}
                      key={`${risk.category}-${risk.message}`}
                    >
                      <b>
                        {risk.level === "blocking"
                          ? "阻断"
                          : risk.level === "warning"
                            ? "警告"
                            : "提示"}{" "}
                        · {risk.category}
                      </b>
                      <span>{risk.message}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div className="deep-section">
              <h4>关键参数对比（源 → 目标）</h4>
              <div className="deep-compare-table">
                <div className="deep-compare-row deep-compare-head">
                  <b>源端</b>
                  <b>参数</b>
                  <b>目标端</b>
                </div>
                {deepAssessment.parameter_comparison.map((item) => (
                  <div
                    className={`deep-compare-row ${item.risk ? item.risk : ""}`}
                    key={item.key}
                  >
                    <code>{item.source ?? "—"}</code>
                    <span>{item.label}</span>
                    <code>{item.target ?? "—"}</code>
                  </div>
                ))}
              </div>
            </div>

            <div className="deep-section">
              <h4>实例与环境</h4>
              <div className="deep-instance-grid">
                {[deepAssessment.source, deepAssessment.target].map((side) => (
                  <div
                    className="deep-instance-card"
                    key={`env-${side.env.dialect}`}
                  >
                    <b>
                      <DatabaseBadge type={side.env.dialect} small />{" "}
                      {side.env.dialect}
                    </b>
                    <dl>
                      <dt>版本</dt>
                      <dd>{side.env.version ?? "不可用"}</dd>
                      <dt>主机</dt>
                      <dd>{side.env.host ?? "不可用"}</dd>
                      <dt>数据库</dt>
                      <dd>{side.env.database ?? "不可用"}</dd>
                      <dt>运行模式</dt>
                      <dd>{side.env.run_mode ?? "不可用"}</dd>
                      <dt>启动时间</dt>
                      <dd>{side.env.startup_time ?? "不可用"}</dd>
                      <dt>日志模式</dt>
                      <dd>{side.env.log_mode ?? "不可用"}</dd>
                      <dt>字符集</dt>
                      <dd>{side.env.charset ?? "不可用"}</dd>
                      <dt>主机资源</dt>
                      <dd>
                        {side.env.host_resources
                          ? `${side.env.host_resources.cpu_cores ?? "?"} 核 / ${
                              side.env.host_resources.memory_bytes != null
                                ? formatBytes(
                                    side.env.host_resources.memory_bytes,
                                  )
                                : "未知"
                            }`
                          : "不可用"}
                      </dd>
                    </dl>
                    {side.connect_error && (
                      <p className="deep-unavailable">
                        连接失败：{side.connect_error}
                      </p>
                    )}
                  </div>
                ))}
              </div>
            </div>

            <div className="deep-section">
              <h4>对象规模统计</h4>
              <div className="deep-instance-grid">
                {[deepAssessment.source, deepAssessment.target].map((side) => (
                  <div
                    className="deep-instance-card"
                    key={`counts-${side.env.dialect}`}
                  >
                    <b>{side.env.dialect}</b>
                    <div className="deep-counts">
                      {Object.entries(side.objects.counts)
                        .filter(([, value]) => value != null)
                        .map(([key, value]) => (
                          <div key={key}>
                            <b>{value as number}</b>
                            <small>{objectCountLabel(key)}</small>
                          </div>
                        ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div className="deep-section">
              <h4>对象明细清单（源端）</h4>
              {deepAssessment.source.objects.details ? (
                <div className="deep-detail-tabs">
                  <details open>
                    <summary>
                      序列（
                      {deepAssessment.source.objects.details.sequences?.items
                        .length ?? 0}
                      ）
                    </summary>
                    <DeepDetailBlock
                      detail={deepAssessment.source.objects.details.sequences}
                    />
                  </details>
                  <details>
                    <summary>
                      同义词（
                      {deepAssessment.source.objects.details.synonyms?.items
                        .length ?? 0}
                      ）
                    </summary>
                    <DeepDetailBlock
                      detail={deepAssessment.source.objects.details.synonyms}
                    />
                  </details>
                  <details>
                    <summary>
                      DBLINK（
                      {deepAssessment.source.objects.details.dblinks?.items
                        .length ?? 0}
                      ）
                    </summary>
                    <DeepDetailBlock
                      detail={deepAssessment.source.objects.details.dblinks}
                    />
                  </details>
                  <details>
                    <summary>
                      存储过程 / 函数（
                      {deepAssessment.source.objects.details.procedures?.items
                        .length ?? 0}
                      ）
                    </summary>
                    <DeepDetailBlock
                      detail={deepAssessment.source.objects.details.procedures}
                    />
                  </details>
                  <details>
                    <summary>
                      触发器（
                      {deepAssessment.source.objects.details.triggers?.items
                        .length ?? 0}
                      ）
                    </summary>
                    <DeepDetailBlock
                      detail={deepAssessment.source.objects.details.triggers}
                    />
                  </details>
                </div>
              ) : (
                <p className="deep-unavailable">对象明细不可用</p>
              )}
            </div>

            <div className="deep-section">
              <h4>数据质量预检（源端，轻量采样）</h4>
              <DeepQualityBlock quality={deepAssessment.data_quality} />
            </div>

            <div className="deep-section">
              <h4>数据量分析（源端）</h4>
              <div className="deep-facts">
                <span>
                  总估算容量：
                  <b>
                    {deepAssessment.data_analysis.source.total_bytes != null
                      ? formatBytes(
                          deepAssessment.data_analysis.source.total_bytes,
                        )
                      : "不可用"}
                  </b>
                </span>
                <span>
                  总估算行数：
                  <b>
                    {deepAssessment.data_analysis.source.total_rows_estimate !=
                    null
                      ? formatRows(
                          deepAssessment.data_analysis.source
                            .total_rows_estimate,
                        )
                      : "不可用"}
                  </b>
                </span>
                <span>
                  空表数量：
                  <b>
                    {deepAssessment.data_analysis.source.empty_table_count ??
                      "不可用"}
                  </b>
                </span>
              </div>
              <h5>TOP 10 大表（按容量排序）</h5>
              {deepAssessment.data_analysis.source.top_tables ? (
                <div className="deep-table">
                  <div className="deep-table-row deep-table-head">
                    <b>表名</b>
                    <b>估算行数</b>
                    <b>容量</b>
                    <b>列数</b>
                    <b>主键</b>
                    <b>分区</b>
                  </div>
                  {deepAssessment.data_analysis.source.top_tables.map(
                    (table) => (
                      <div key={table.table} className="deep-top-table-cell">
                        <div className="deep-table-row">
                          <span>{table.table}</span>
                          <span>
                            {table.rows_estimate != null
                              ? formatRows(table.rows_estimate)
                              : "—"}
                          </span>
                          <span>
                            {table.size_bytes != null
                              ? formatBytes(table.size_bytes)
                              : "—"}
                          </span>
                          <span>{table.column_count ?? "—"}</span>
                          <span>{table.has_pk ? "有" : "无"}</span>
                          <span>{table.partitioned ? "是" : "否"}</span>
                        </div>
                        <DeepColumnMappings table={table} />
                      </div>
                    ),
                  )}
                </div>
              ) : (
                <p className="deep-unavailable">TOP 10 大表信息不可用</p>
              )}
              <h5>含 LOB / 大字段的表</h5>
              {deepAssessment.data_analysis.source.lob_tables ? (
                deepAssessment.data_analysis.source.lob_tables.length ? (
                  <div className="deep-lob-list">
                    {deepAssessment.data_analysis.source.lob_tables.map(
                      (lob) => (
                        <div key={lob.table}>
                          <b>{lob.table}</b>
                          <span>
                            {lob.columns
                              .map(
                                (column) => `${column.column} (${column.type})`,
                              )
                              .join(", ")}
                          </span>
                        </div>
                      ),
                    )}
                  </div>
                ) : (
                  <p className="deep-empty">未发现 LOB 字段</p>
                )
              ) : (
                <p className="deep-unavailable">LOB 表信息不可用</p>
              )}
              <h5>无主键表</h5>
              {deepAssessment.data_analysis.source.no_pk_tables ? (
                deepAssessment.data_analysis.source.no_pk_tables.length ? (
                  <div className="deep-tag-list">
                    {deepAssessment.data_analysis.source.no_pk_tables.map(
                      (name) => (
                        <span key={name}>{name}</span>
                      ),
                    )}
                  </div>
                ) : (
                  <p className="deep-empty">所有表均有主键</p>
                )
              ) : (
                <p className="deep-unavailable">无主键表信息不可用</p>
              )}
            </div>

            {deepAssessment.time_estimate && (
              <div className="deep-section">
                <h4>迁移耗时估算（估算值）</h4>
                <div className="deep-facts">
                  <span>
                    总容量：
                    <b>
                      {deepAssessment.time_estimate.summary.total_bytes != null
                        ? formatBytes(
                            deepAssessment.time_estimate.summary.total_bytes,
                          )
                        : "不可用"}
                    </b>
                  </span>
                  <span>
                    总行数：
                    <b>
                      {deepAssessment.time_estimate.summary.total_rows != null
                        ? formatRows(
                            deepAssessment.time_estimate.summary.total_rows,
                          )
                        : "不可用"}
                    </b>
                  </span>
                  <span>
                    带宽假设：
                    <b>
                      {deepAssessment.time_estimate.summary.bandwidth_mbps} Mbps
                    </b>
                  </span>
                  <span>
                    乐观：
                    <b>
                      {deepAssessment.time_estimate.summary.optimistic ?? "—"}
                    </b>
                  </span>
                  <span>
                    悲观：
                    <b>
                      {deepAssessment.time_estimate.summary.pessimistic ?? "—"}
                    </b>
                  </span>
                </div>
                <div className="deep-table">
                  <div className="deep-table-row deep-table-head deep-time-head">
                    <b>表</b>
                    <b>估算行数</b>
                    <b>容量</b>
                    <b>传输耗时</b>
                    <b>复制耗时</b>
                    <b>合计</b>
                  </div>
                  {deepAssessment.time_estimate.per_table.map((item) => (
                    <div
                      className="deep-table-row deep-time-row"
                      key={item.table}
                    >
                      <span>{item.table}</span>
                      <span>
                        {item.rows_estimate != null
                          ? formatRows(item.rows_estimate)
                          : "—"}
                      </span>
                      <span>
                        {item.size_bytes != null
                          ? formatBytes(item.size_bytes)
                          : "—"}
                      </span>
                      <span>{formatDuration(item.transfer_seconds)}</span>
                      <span>{formatDuration(item.copy_seconds)}</span>
                      <span>{formatDuration(item.total_seconds)}</span>
                    </div>
                  ))}
                </div>
                <ul className="deep-assumptions">
                  {deepAssessment.time_estimate.assumptions.map(
                    (assumption) => (
                      <li key={assumption}>{assumption}</li>
                    ),
                  )}
                </ul>
              </div>
            )}

            <div className="deep-section">
              <h4>外键依赖关系（源端）</h4>
              <div className="deep-facts">
                <span>
                  外键数量：
                  <b>{deepAssessment.foreign_keys.source.count ?? "不可用"}</b>
                </span>
              </div>
              {deepAssessment.foreign_keys.source.dependencies &&
              deepAssessment.foreign_keys.source.dependencies.length ? (
                <div className="deep-fk-wrap">
                  <div className="deep-fk-table">
                    <div className="deep-table-row deep-table-head deep-fk-head">
                      <b>父表</b>
                      <b>→</b>
                      <b>子表</b>
                      <b>约束</b>
                    </div>
                    {deepAssessment.foreign_keys.source.dependencies.map(
                      (dependency, index) => (
                        <div
                          className="deep-table-row deep-fk-row"
                          key={`${dependency.child_table}-${dependency.parent_table}-${index}`}
                        >
                          <span>{dependency.parent_table}</span>
                          <em>→</em>
                          <span>{dependency.child_table}</span>
                          <span>{dependency.constraint_name ?? "—"}</span>
                        </div>
                      ),
                    )}
                  </div>
                  <div className="deep-fk-graph">
                    {buildFkGraph(
                      deepAssessment.foreign_keys.source.dependencies,
                    ).map((line, index) => (
                      <pre key={index}>{line}</pre>
                    ))}
                  </div>
                </div>
              ) : (
                <p className="deep-empty">无外键依赖或信息不可用</p>
              )}
            </div>

            {deepAssessment.security && (
              <div className="deep-section">
                <h4>权限与安全清单（源端）</h4>
                <div className="deep-facts">
                  <span>
                    账号数：
                    <b>{deepAssessment.security.accounts?.total ?? "不可用"}</b>
                  </span>
                  <span>
                    角色数：
                    <b>{deepAssessment.security.roles?.total ?? "不可用"}</b>
                  </span>
                </div>
                {deepAssessment.security.accounts?.items.length ? (
                  <details className="deep-mappings">
                    <summary>
                      账号清单（{deepAssessment.security.accounts.items.length}{" "}
                      条）
                    </summary>
                    <DeepDetailBlock
                      detail={{
                        items: deepAssessment.security.accounts.items,
                        total: deepAssessment.security.accounts.total ?? null,
                        truncated: !!deepAssessment.security.accounts.truncated,
                      }}
                    />
                  </details>
                ) : (
                  <p className="deep-empty">账号信息不可用或为空</p>
                )}
                {deepAssessment.security.roles?.items.length ? (
                  <details className="deep-mappings">
                    <summary>
                      角色清单（{deepAssessment.security.roles.items.length}{" "}
                      条）
                    </summary>
                    <DeepDetailBlock
                      detail={{
                        items: deepAssessment.security.roles.items,
                        total: deepAssessment.security.roles.total ?? null,
                        truncated: !!deepAssessment.security.roles.truncated,
                      }}
                    />
                  </details>
                ) : (
                  <p className="deep-empty">角色信息不可用或为空</p>
                )}
                {deepAssessment.security.system_privileges?.items.length ? (
                  <details className="deep-mappings">
                    <summary>
                      系统权限概要（
                      {
                        deepAssessment.security.system_privileges.items.length
                      }{" "}
                      条）
                    </summary>
                    <DeepDetailBlock
                      detail={{
                        items: deepAssessment.security.system_privileges.items,
                        total:
                          deepAssessment.security.system_privileges.total ??
                          null,
                        truncated:
                          !!deepAssessment.security.system_privileges.truncated,
                      }}
                    />
                  </details>
                ) : (
                  <p className="deep-empty">系统权限信息不可用或为空</p>
                )}
                {deepAssessment.security.sensitive_accounts?.length ? (
                  <details className="deep-mappings">
                    <summary>
                      敏感账号（
                      {deepAssessment.security.sensitive_accounts.length} 个）
                    </summary>
                    <DeepDetailBlock
                      detail={{
                        items: deepAssessment.security.sensitive_accounts.map(
                          (name) => ({ username: name }),
                        ),
                        total: null,
                        truncated: false,
                      }}
                    />
                  </details>
                ) : (
                  <p className="deep-empty">敏感账号不可用或为空</p>
                )}
                {deepAssessment.security.security_settings &&
                Object.keys(deepAssessment.security.security_settings).length >
                  0 ? (
                  <details className="deep-mappings">
                    <summary>加密 / 审计设置</summary>
                    <DeepDetailBlock
                      detail={{
                        items: Object.entries(
                          deepAssessment.security.security_settings,
                        ).map(([key, value]) => ({ name: key, value })),
                        total: null,
                        truncated: false,
                      }}
                    />
                  </details>
                ) : (
                  <p className="deep-empty">加密 / 审计设置不可用或为空</p>
                )}
                {(deepAssessment.security.notes?.length ?? 0) > 0 && (
                  <ul className="deep-notes">
                    {(deepAssessment.security.notes ?? []).map(
                      (note, index) => (
                        <li key={`security-${index}`}>{note}</li>
                      ),
                    )}
                  </ul>
                )}
              </div>
            )}

            {deepAssessment.performance && (
              <div className="deep-section">
                <h4>性能压力评估（迁移期间）</h4>
                <div className="deep-facts">
                  <span>
                    源端负载影响：
                    <b>{deepAssessment.performance.level}</b>
                  </span>
                  <span>
                    建议表并发上限：
                    <b>
                      {deepAssessment.performance
                        .recommended_table_concurrency ?? "—"}
                    </b>
                  </span>
                </div>
                <ul className="deep-assumptions">
                  {deepAssessment.performance.low_peak_advice && (
                    <li>{deepAssessment.performance.low_peak_advice}</li>
                  )}
                  {deepAssessment.performance.rationale && (
                    <li>{deepAssessment.performance.rationale}</li>
                  )}
                </ul>
              </div>
            )}

            {deepAssessment.suggestions.length > 0 && (
              <div className="deep-section">
                <h4>迁移建议</h4>
                <ul className="deep-suggestions">
                  {deepAssessment.suggestions.map((suggestion) => (
                    <li key={suggestion}>{suggestion}</li>
                  ))}
                </ul>
              </div>
            )}

            {deepAssessment.notes.length > 0 && (
              <div className="deep-section">
                <h4>数据降级说明</h4>
                <ul className="deep-notes">
                  {deepAssessment.notes.map((note, index) => (
                    <li key={`${note.side}-${note.section}-${index}`}>
                      <b>{note.side}</b>
                      <code>{note.section}</code>
                      <span>{note.message}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {deepAssessment.partition_analysis && (
              <div className="deep-section">
                <h4>分区表分析</h4>
                <div className="deep-facts">
                  <span>
                    分区表总数：
                    <b>
                      {deepAssessment.partition_analysis.partitioned_total ?? 0}
                    </b>
                  </span>
                  {deepAssessment.partition_analysis.by_type &&
                    Object.entries(
                      deepAssessment.partition_analysis.by_type,
                    ).map(([type, count]) => (
                      <span key={type}>
                        {type}：<b>{count as number}</b>
                      </span>
                    ))}
                </div>
                {deepAssessment.partition_analysis.interval_tables?.length ? (
                  <>
                    <p className="deep-empty">
                      间隔分区表（
                      {
                        deepAssessment.partition_analysis.interval_tables.length
                      }{" "}
                      个）：
                      {deepAssessment.partition_analysis.interval_tables.join(
                        "、",
                      )}
                    </p>
                    <ul className="deep-notes">
                      {deepAssessment.partition_analysis.downgrades?.map(
                        (note, index) => (
                          <li key={`partition-${index}`}>
                            <span>{note}</span>
                          </li>
                        ),
                      )}
                    </ul>
                  </>
                ) : (
                  <p className="deep-empty">未检测到间隔分区表</p>
                )}
              </div>
            )}

            <div className="modal-actions">
              <button
                className="secondary-button"
                onClick={closeDeepAssessmentModal}
              >
                关闭
              </button>
            </div>
          </div>
        </div>
      )}
      {selectedJob && (
        <div
          className="modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) setSelectedJob(null);
          }}
        >
          <div
            className="job-detail-modal"
            role="dialog"
            aria-modal="true"
            aria-label="任务详情与日志"
          >
            <button
              className="modal-close"
              onClick={() => setSelectedJob(null)}
            >
              ×
            </button>
            <div className="job-detail-head">
              <span className="modal-icon">⇄</span>
              <div>
                <h3>{selectedJob.name}</h3>
                <p>
                  {
                    contentLabels[
                      selectedJob.migration_content || "structure_and_data"
                    ]
                  }{" "}
                  · {selectedJob.source_type.toUpperCase()} →{" "}
                  {selectedJob.target_type.toUpperCase()}
                </p>
              </div>
              <i className={`task-status ${selectedJob.status}`}>
                {selectedJob.status === "failed"
                  ? "× 失败"
                  : selectedJob.status === "completed"
                    ? selectedJob.sync_mode === "incremental_only" &&
                      selectedJob.sync_phase === "stopped"
                      ? "✓ 同步已结束"
                      : "✓ 已完成"
                    : selectedJob.status === "syncing"
                      ? "● 实时同步"
                      : selectedJob.status === "catching_up"
                        ? "◌ 追增量"
                    : selectedJob.status === "running"
                      ? "◌ 迁移中"
                      : selectedJob.status === "cancelled"
                        ? "— 已取消"
                        : "• 等待中"}
              </i>
            </div>
            <dl className="job-detail-grid">
              <div>
                <dt>任务 ID</dt>
                <dd>{selectedJob.id}</dd>
              </div>
              {selectedJob.sync_mode !== "incremental_only" && (
                <>
                  <div>
                    <dt>当前对象</dt>
                    <dd>{selectedJob.current_table || "—"}</dd>
                  </div>
                  <div>
                    <dt>总体对象进度</dt>
                    <dd>
                      {selectedJob.tables_completed}/{selectedJob.tables_total}
                    </dd>
                  </div>
                  <div>
                    <dt>全量迁移</dt>
                    <dd>
                      {formatRows(selectedJob.rows_copied)} 行 /{" "}
                      {formatBytes(selectedJob.bytes_copied)}
                    </dd>
                  </div>
                  <div>
                    <dt>批量大小 / 表并发</dt>
                    <dd>
                      {selectedJob.batch_size.toLocaleString("zh-CN")} 行 /{" "}
                      {selectedJob.table_concurrency} 张
                    </dd>
                  </div>
                </>
              )}
              <div>
                <dt>失败策略</dt>
                <dd>
                  {selectedJob.fail_policy === "continue_on_error"
                    ? "失败继续（跳过失败表）"
                    : "失败即停止（安全）"}
                </dd>
              </div>
              <div>
                <dt>目标对象命名策略</dt>
                <dd>
                  {selectedJob.identifier_case_policy || "auto"} →{" "}
                  {selectedJob.identifier_case_resolved || "preserve"}
                </dd>
              </div>
              <div>
                <dt>目标大小写参数</dt>
                <dd>
                  {selectedJob.target_lower_case_table_names == null
                    ? "非 MySQL/TDSQL"
                    : `lower_case_table_names=${selectedJob.target_lower_case_table_names}`}
                </dd>
              </div>
              <div>
                <dt>创建时间</dt>
                <dd>
                  {new Date(selectedJob.created_at).toLocaleString("zh-CN")}
                </dd>
              </div>
              <div>
                <dt>完成时间</dt>
                <dd>
                  {selectedJob.finished_at
                    ? new Date(selectedJob.finished_at).toLocaleString("zh-CN")
                    : "—"}
                </dd>
              </div>
              {selectedJob.sync_mode !== "full_only" && (
                <>
                  <div>
                    <dt>同步阶段</dt>
                    <dd>
                      {selectedJob.sync_phase === "realtime"
                        ? "实时同步"
                        : selectedJob.sync_phase === "catching_up"
                          ? "增量追平"
                          : selectedJob.sync_phase === "stopped"
                            ? "同步已正常结束（可继续）"
                            : selectedJob.sync_phase === "incremental"
                              ? "增量准备"
                          : "全量基线"}
                    </dd>
                  </div>
                  <div>
                    <dt>SCN 位点</dt>
                    <dd>
                      {selectedJob.checkpoint_scn?.toLocaleString("zh-CN") || "—"}
                      {selectedJob.source_current_scn
                        ? ` / ${selectedJob.source_current_scn.toLocaleString("zh-CN")}`
                        : ""}
                    </dd>
                  </div>
                  <div>
                    <dt>增量延迟</dt>
                    <dd>{(selectedJob.cdc_lag || 0).toLocaleString("zh-CN")} SCN</dd>
                  </div>
                  <div>
                    <dt>增量事务 / 事件</dt>
                    <dd>
                      {(selectedJob.cdc_transactions || 0).toLocaleString("zh-CN")} /{" "}
                      {(selectedJob.cdc_events || 0).toLocaleString("zh-CN")}
                    </dd>
                  </div>
                  <div>
                    <dt>增量新增 / 更新 / 删除</dt>
                    <dd>
                      {(selectedJob.cdc_inserts || 0).toLocaleString("zh-CN")} /{" "}
                      {(selectedJob.cdc_updates || 0).toLocaleString("zh-CN")} /{" "}
                      {(selectedJob.cdc_deletes || 0).toLocaleString("zh-CN")}
                    </dd>
                  </div>
                  <div>
                    <dt>累计处理</dt>
                    <dd>
                      {formatRows(selectedJob.rows_copied + (selectedJob.cdc_events || 0))} 条
                    </dd>
                  </div>
                  {["full_and_incremental", "full_then_incremental"].includes(
                    selectedJob.sync_mode || "",
                  ) && (
                    <div>
                      <dt>目标净行数（推算）</dt>
                      <dd>
                        {formatRows(
                          selectedJob.rows_copied +
                            (selectedJob.cdc_inserts || 0) -
                            (selectedJob.cdc_deletes || 0),
                        )} 行
                      </dd>
                    </div>
                  )}
                </>
              )}
            </dl>
            {selectedJob.sync_mode !== "incremental_only" && (
            <section className="job-phase-progress">
              <div className="job-phase-progress-head">
                <div>
                  <b>分阶段迁移进度</b>
                  <span>序列、普通表、分区表和视图分别统计</span>
                </div>
                <small>失败和取消也计入阶段处理进度</small>
              </div>
              <div className="job-phase-grid">
                {normalizedJobPhases(selectedJob).map((phase) => {
                  const definition = jobPhaseDefinitions.find(
                    (item) => item.phase === phase.phase,
                  )!;
                  const processed =
                    phase.completed + phase.failed + phase.cancelled;
                  return (
                    <article
                      className={`job-phase-card phase-${phase.phase}`}
                      key={phase.phase}
                    >
                      <header>
                        <span>{definition.shortLabel}</span>
                        <div>
                          <b>{definition.label}</b>
                          <small>
                            {phase.total
                              ? `${processed}/${phase.total} 个对象已处理`
                              : "本任务未选择此类对象"}
                          </small>
                        </div>
                        <strong>
                          {phase.total
                            ? `${Math.round(phase.progress)}%`
                            : "未选择"}
                        </strong>
                      </header>
                      <div className="job-phase-track" aria-hidden="true">
                        <i
                          style={{
                            width: `${phase.total ? Math.min(100, Math.max(0, phase.progress)) : 0}%`,
                          }}
                        />
                      </div>
                      <dl className="job-phase-stats">
                        <div>
                          <dt>成功</dt>
                          <dd>{phase.completed}</dd>
                        </div>
                        <div>
                          <dt>失败</dt>
                          <dd className={phase.failed ? "bad" : ""}>
                            {phase.failed}
                          </dd>
                        </div>
                        <div>
                          <dt>取消/跳过</dt>
                          <dd>{phase.cancelled}</dd>
                        </div>
                        <div>
                          <dt>运行中</dt>
                          <dd className={phase.running ? "active" : ""}>
                            {phase.running}
                          </dd>
                        </div>
                        <div>
                          <dt>待处理</dt>
                          <dd>{phase.pending}</dd>
                        </div>
                      </dl>
                      {phase.current_objects.length ? (
                        <p title={phase.current_objects.join("、")}>
                          当前：{phase.current_objects.join("、")}
                        </p>
                      ) : null}
                    </article>
                  );
                })}
              </div>
            </section>
            )}
            {selectedJob.table_results?.length ? (
              <section className="job-table-results">
                <div>
                  <b>对象级结果</b>
                  <span>每个序列、表和视图的成功 / 失败及原因</span>
                </div>
                <div className="table-results-filter">
                  {(
                    [
                      ["all", "全部"],
                      ["success", "成功"],
                      ["failed", "失败"],
                      ["other", "跳过/取消"],
                    ] as const
                  ).map(([key, label]) => (
                    <button
                      key={key}
                      className={tableStatusFilter === key ? "active" : ""}
                      onClick={() => setTableStatusFilter(key)}
                    >
                      {label}
                    </button>
                  ))}
                </div>
                {selectedJob.table_results.some(
                  (result) => result.notes && result.notes.length,
                ) ? (
                  <div className="result-notes-warn">
                    {selectedJob.table_results
                      .filter((result) => result.notes && result.notes.length)
                      .flatMap((result) =>
                        (Array.isArray(result.notes)
                          ? result.notes
                          : String(result.notes).split(/[;；]\s*/)
                        )
                          .filter(Boolean)
                          .map((note, index) => (
                            <div
                              className="result-note-row"
                              key={`${result.table}-${index}`}
                            >
                              <b>{result.table}</b>：{note}
                            </div>
                          )),
                      )}
                  </div>
                ) : null}
                <table className="table-results-table">
                  <thead>
                    <tr>
                      <th>对象</th>
                      <th>类型</th>
                      <th>状态</th>
                      <th>行数</th>
                      <th>耗时</th>
                      <th>失败原因</th>
                    </tr>
                  </thead>
                  <tbody>
                    {selectedJob.table_results
                      .filter((result) => {
                        if (tableStatusFilter === "success") {
                          return result.status === "success";
                        }
                        if (tableStatusFilter === "failed") {
                          return result.status === "failed";
                        }
                        if (tableStatusFilter === "other") {
                          return (
                            result.status !== "success" &&
                            result.status !== "failed"
                          );
                        }
                        return true;
                      })
                      .map((result) => (
                        <tr key={result.table}>
                          <td>
                            {result.target_table &&
                            result.target_table !== result.table
                              ? `${result.table} → ${result.target_table}`
                              : result.table}
                          </td>
                          <td>
                            <span
                              className={`object-type-badge type-${
                                result.object_type || "table"
                              }`}
                            >
                              {objectTypeLabel(result.object_type || "table")}
                            </span>
                          </td>
                          <td>
                            {result.status === "success"
                              ? "✓ 成功"
                              : result.status === "failed"
                                ? "× 失败"
                                : result.status === "cancelled"
                                  ? "— 已取消"
                                  : "· 已跳过"}
                          </td>
                          <td>{result.rows.toLocaleString("zh-CN")}</td>
                          <td>{result.elapsed_ms} ms</td>
                          <td className="table-result-error">
                            {result.error ? (
                              <details>
                                <summary>
                                  {result.error.split("\n", 1)[0]}
                                </summary>
                                <pre>{result.error}</pre>
                              </details>
                            ) : (
                              "—"
                            )}
                          </td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </section>
            ) : null}
            <section
              className={`job-log ${selectedJob.error ? "has-error" : ""}`}
            >
              <div>
                <b>{selectedJob.error ? "失败日志" : "实时运行日志"}</b>
                <span>
                  {selectedJob.error
                    ? "完整错误信息"
                    : jobLogs.length
                      ? `已加载 ${jobLogs.length} 条日志`
                      : "等待日志输出"}
                </span>
              </div>
              {!selectedJob.error && (
                <button
                  className="secondary-button"
                  onClick={() => setJobLogAutoScroll((current) => !current)}
                >
                  {jobLogAutoScroll ? "暂停自动滚动" : "跟随最新日志"}
                </button>
              )}
              {selectedJob.error && jobLogs.length === 0 ? (
                <pre className="job-log-error">{selectedJob.error}</pre>
              ) : (
                <pre className="job-log-live" ref={jobLogViewRef}>
                  {jobLogs.length
                    ? jobLogs
                        .map(
                          (entry) =>
                            `${new Date(entry.ts).toLocaleTimeString("zh-CN")} [${entry.level}] ${entry.message}`,
                        )
                        .join("\n")
                    : `任务状态：${selectedJob.status}\n当前进度：${Math.round(selectedJob.progress)}%\n批量大小：${selectedJob.batch_size} 行\n表并发：${selectedJob.table_concurrency}\n已完成对象：${selectedJob.tables_completed}/${selectedJob.tables_total}`}
                </pre>
              )}
            </section>
            <div className="modal-actions">
              {selectedJob.sync_mode === "full_then_incremental" &&
                selectedJob.status === "completed" &&
                selectedJob.sync_phase === "ready_for_incremental" &&
                selectedJob.start_scn && (
                  <button
                    className="continue-button"
                    onClick={() => void startIncrementalJob(selectedJob)}
                  >
                    启动增量同步
                  </button>
                )}
              {["failed", "cancelled"].includes(selectedJob.status) && (
                <button
                  className="continue-button"
                  onClick={() => {
                    openEditJob(selectedJob);
                  }}
                >
                  编辑并重试
                </button>
              )}
              {["failed", "cancelled"].includes(selectedJob.status) && (
                <button
                  className="secondary-button"
                  onClick={() => {
                    retryJob(selectedJob);
                    setSelectedJob(null);
                  }}
                >
                  重新执行
                </button>
              )}
              {["catching_up", "syncing"].includes(selectedJob.status) && (
                <button
                  className="continue-button"
                  onClick={() => setFinishConfirmJob(selectedJob)}
                >
                  结束同步
                </button>
              )}
              {["queued", "running"].includes(selectedJob.status) && (
                <button
                  className="danger-button"
                  onClick={() => setCancelConfirmJob(selectedJob)}
                >
                  取消任务
                </button>
              )}
              <button
                className="secondary-button"
                onClick={() => setSelectedJob(null)}
              >
                关闭
              </button>
            </div>
          </div>
        </div>
      )}
      {editTemplate && (
        <div
          className="modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) setEditTemplate(null);
          }}
        >
          <div
            className="modal-card job-edit-modal"
            role="dialog"
            aria-modal="true"
          >
            <h3>编辑并重试</h3>
            <p className="modal-sub">
              基于失败任务「{editTemplate.name}」的参数创建新任务，连接沿用
              {editTemplate.link_name || "原链路"}凭据。
            </p>
            <p className="job-edit-mode-tip">
              序列、表、视图均可单独迁移，也可组合迁移；只需至少选择 1 个对象。
            </p>
            <div className="job-edit-grid">
              <label>
                任务名称
                <input
                  value={editName}
                  onChange={(event) => setEditName(event.target.value)}
                  maxLength={120}
                />
              </label>
              <label>
                批量大小（行）
                <input
                  type="number"
                  min={100}
                  max={20000}
                  value={editBatchSize}
                  onChange={(event) =>
                    setEditBatchSize(Number(event.target.value))
                  }
                />
              </label>
              <label>
                表并发
                <input
                  type="number"
                  min={1}
                  max={16}
                  value={editConcurrency}
                  onChange={(event) =>
                    setEditConcurrency(Number(event.target.value))
                  }
                />
              </label>
              <label>
                目标表已存在
                <select
                  value={editExistingTable}
                  onChange={(event) => setEditExistingTable(event.target.value)}
                >
                  <option value="fail">失败（安全）</option>
                  <option value="drop_and_create">删除并重建</option>
                  <option value="truncate">清空后重写</option>
                  <option value="append">追加数据</option>
                </select>
              </label>
              <label>
                迁移内容
                <select
                  value={editContent}
                  onChange={(event) =>
                    setEditContent(event.target.value as MigrationContent)
                  }
                >
                  <option value="structure_and_data">结构 + 数据</option>
                  <option value="structure_only">仅结构</option>
                  <option value="data_only">仅数据</option>
                </select>
              </label>
              <label>
                失败策略
                <select
                  value={editFailPolicy}
                  onChange={(event) =>
                    setEditFailPolicy(
                      event.target.value as
                        "stop_on_error" | "continue_on_error",
                    )
                  }
                >
                  <option value="stop_on_error">失败即停止（安全）</option>
                  <option value="continue_on_error">
                    失败继续（跳过失败表）
                  </option>
                </select>
              </label>
              <label>
                目标表/视图名称
                <select
                  value={editIdentifierCasePolicy}
                  onChange={(event) =>
                    setEditIdentifierCasePolicy(
                      event.target.value as IdentifierCasePolicy,
                    )
                  }
                >
                  <option value="auto">自动适配（推荐）</option>
                  <option value="preserve">保留源端大小写</option>
                  <option value="lower">统一小写</option>
                  <option value="upper">统一大写</option>
                </select>
              </label>
              {editTemplate.source_type === "oracle" && (
                <label>
                  <span>
                    用户名映射
                    <span
                      className="user-mapping-help"
                      title="建表 / 写数时目标 schema 使用映射后的用户名；DDL 中的 `源用户.对象` 引用同步替换。"
                    >
                      ?
                    </span>
                  </span>
                  {editUserMappings.length > 0 ? (
                    <button
                      type="button"
                      className="user-mapping-trigger"
                      onClick={() => setEditMappingViewOpen(true)}
                    >
                      查看映射（{editUserMappings.length} 条）
                    </button>
                  ) : (
                    <button
                      type="button"
                      className="user-mapping-trigger"
                      onClick={() => {
                        setEditMappingDraft([{ source: "", target: "" }]);
                        setEditMappingError("");
                        setEditMappingModalOpen(true);
                      }}
                    >
                      配置用户名映射
                    </button>
                  )}
                </label>
              )}
            </div>
            {editTemplate.sync_mode !== "full_only" && (
              <section className="job-edit-cdc-settings">
                <div>
                  <b>无主键表增量定位</b>
                  <small>原任务策略可在重试前调整</small>
                </div>
                <label>
                  业务唯一键（每行 表名=字段1,字段2）
                  <textarea
                    value={editCdcBusinessKeysText}
                    onChange={(event) =>
                      setEditCdcBusinessKeysText(event.target.value)
                    }
                    placeholder="ORDERS=TENANT_ID,ORDER_NO"
                  />
                </label>
                <label>
                  无可靠键时
                  <select
                    value={editCdcNoKeyPolicy}
                    onChange={(event) =>
                      setEditCdcNoKeyPolicy(
                        event.target.value as CdcNoKeyPolicy,
                      )
                    }
                  >
                    <option value="reject">拒绝启动（推荐）</option>
                    <option value="all_columns">ALL COLUMNS（高风险）</option>
                  </select>
                </label>
                <label className="cdc-source-ddl-toggle">
                  <input
                    type="checkbox"
                    checked={editCdcAllowSourceDdl}
                    onChange={(event) =>
                      setEditCdcAllowSourceDdl(event.target.checked)
                    }
                  />
                  <span>允许在源表自动创建补充日志组</span>
                </label>
              </section>
            )}
            <div className="job-edit-object-stack">
              <section className="job-edit-object-module sequence-module">
                <header>
                  <div>
                    <span className="job-edit-step">1</span>
                    <div>
                      <b>序列</b>
                      <small>先于表执行，避免默认值引用的序列不存在</small>
                    </div>
                  </div>
                  <label className="job-edit-object-search">
                    <span aria-hidden="true">⌕</span>
                    <input value={editSequenceSearch} onChange={(event) => setEditSequenceSearch(event.target.value)} placeholder="搜索序列" aria-label="搜索序列" />
                  </label>
                  <div className="job-edit-module-actions">
                    <label className="job-edit-enable-toggle">
                      <input
                        type="checkbox"
                        checked={editMigrateSequences}
                        onChange={(event) =>
                          setEditMigrateSequences(event.target.checked)
                        }
                      />
                      启用迁移
                    </label>
                    <label className="job-edit-select-all">
                      <input
                        type="checkbox"
                        disabled={
                          !editMigrateSequences ||
                          !editTemplate.sequences.length
                        }
                        checked={
                          editMigrateSequences &&
                          editTemplate.sequences.length > 0 &&
                          editTemplate.sequences.every((name) =>
                            editSequences.includes(name),
                          )
                        }
                        onChange={(event) =>
                          setEditSequences(
                            event.target.checked
                              ? [...editTemplate.sequences]
                              : [],
                          )
                        }
                      />
                      {editTemplate.sequences.length > 0 &&
                      editTemplate.sequences.every((name) =>
                        editSequences.includes(name),
                      )
                        ? "取消全选"
                        : "全选"}
                    </label>
                  </div>
                </header>
                <div className="job-edit-module-count">
                  已选 {editMigrateSequences ? editSequences.length : 0} /{" "}
                  {editTemplate.sequences.length} 个序列
                </div>
                {editMigrateSequences && visibleEditSequences.length ? (
                  <div className="job-edit-object-list">
                    {visibleEditSequences.map((sequence) => (
                      <label key={sequence}>
                        <input
                          type="checkbox"
                          checked={editSequences.includes(sequence)}
                          onChange={() =>
                            setEditSequences((current) =>
                              current.includes(sequence)
                                ? current.filter((item) => item !== sequence)
                                : [...current, sequence],
                            )
                          }
                        />
                        <span>{sequence}</span>
                        <i className="object-kind sequence">序列</i>
                      </label>
                    ))}
                  </div>
                ) : editMigrateSequences && editTemplate.sequences.length ? (
                  <div className="job-edit-object-empty">没有匹配的序列</div>
                ) : (
                  <div className="job-edit-object-empty">
                    {editTemplate.sequences.length
                      ? "本次不迁移序列"
                      : "原任务没有序列"}
                  </div>
                )}
              </section>

              <section className="job-edit-object-module table-module">
                <header>
                  <div>
                    <span className="job-edit-step">2</span>
                    <div>
                      <b>表</b>
                      <small>普通表先执行，随后执行分区表</small>
                    </div>
                  </div>
                  <label className="job-edit-object-search">
                    <span aria-hidden="true">⌕</span>
                    <input value={editTableSearch} onChange={(event) => setEditTableSearch(event.target.value)} placeholder="搜索普通表或分区表" aria-label="搜索表" />
                  </label>
                  <label className="job-edit-select-all">
                    <input
                      type="checkbox"
                      disabled={!editAllTables.length}
                      checked={
                        editAllTables.length > 0 &&
                        editAllTables.every((name) =>
                          editSelected.includes(name),
                        )
                      }
                      onChange={(event) =>
                        setEditObjectsSelected(
                          editAllTables,
                          event.target.checked,
                        )
                      }
                    />
                    {editAllTables.length > 0 &&
                    editAllTables.every((name) => editSelected.includes(name))
                      ? "取消全选"
                      : "全选"}
                  </label>
                </header>
                <div className="job-edit-module-count">
                  已选{" "}
                  {
                    editAllTables.filter((name) => editSelected.includes(name))
                      .length
                  }{" "}
                  / {editAllTables.length} 张表
                </div>
                {editOrdinaryTables.length ? (
                  <div className="job-edit-object-group">
                    <div className="job-edit-group-title">
                      <b>普通表</b>
                      <div>
                        <span>{editOrdinaryTables.length} 张</span>
                        <label className="job-edit-group-select-all">
                          <input type="checkbox" checked={editOrdinaryTables.every((name) => editSelected.includes(name))} onChange={(event) => setEditObjectsSelected(editOrdinaryTables, event.target.checked)} />
                          {editOrdinaryTables.every((name) => editSelected.includes(name)) ? "取消全选" : "全选"}
                        </label>
                      </div>
                    </div>
                    <div className="job-edit-object-list">
                      {visibleEditOrdinaryTables.map((table) => (
                        <label key={table}>
                          <input
                            type="checkbox"
                            checked={editSelected.includes(table)}
                            onChange={() => toggleEditTable(table)}
                          />
                          <span>{table}</span>
                          <i className="object-kind ordinary">普通表</i>
                        </label>
                      ))}
                      {!visibleEditOrdinaryTables.length && <div className="job-edit-object-empty">没有匹配的普通表</div>}
                    </div>
                  </div>
                ) : null}
                {editPartitionedTables.length ? (
                  <div className="job-edit-object-group partition-group">
                    <div className="job-edit-group-title">
                      <b>分区表</b>
                      <div>
                        <span>{editPartitionedTables.length} 张</span>
                        <label className="job-edit-group-select-all">
                          <input type="checkbox" checked={editPartitionedTables.every((name) => editSelected.includes(name))} onChange={(event) => setEditObjectsSelected(editPartitionedTables, event.target.checked)} />
                          {editPartitionedTables.every((name) => editSelected.includes(name)) ? "取消全选" : "全选"}
                        </label>
                      </div>
                    </div>
                    <div className="job-edit-object-list">
                      {visibleEditPartitionedTables.map((table) => (
                        <label key={table}>
                          <input
                            type="checkbox"
                            checked={editSelected.includes(table)}
                            onChange={() => toggleEditTable(table)}
                          />
                          <span>{table}</span>
                          <i className="object-kind partitioned">分区表</i>
                        </label>
                      ))}
                      {!visibleEditPartitionedTables.length && <div className="job-edit-object-empty">没有匹配的分区表</div>}
                    </div>
                  </div>
                ) : null}
                {!editAllTables.length && (
                  <div className="job-edit-object-empty">原任务没有表</div>
                )}
              </section>

              <section className="job-edit-object-module view-module">
                <header>
                  <div>
                    <span className="job-edit-step">3</span>
                    <div>
                      <b>视图</b>
                      <small>表全部完成后最后创建视图</small>
                    </div>
                  </div>
                  <label className="job-edit-object-search">
                    <span aria-hidden="true">⌕</span>
                    <input value={editViewSearch} onChange={(event) => setEditViewSearch(event.target.value)} placeholder="搜索视图" aria-label="搜索视图" />
                  </label>
                  <label className="job-edit-select-all">
                    <input
                      type="checkbox"
                      disabled={!editViews.length}
                      checked={
                        editViews.length > 0 &&
                        editViews.every((name) => editSelected.includes(name))
                      }
                      onChange={(event) =>
                        setEditObjectsSelected(editViews, event.target.checked)
                      }
                    />
                    {editViews.length > 0 &&
                    editViews.every((name) => editSelected.includes(name))
                      ? "取消全选"
                      : "全选"}
                  </label>
                </header>
                <div className="job-edit-module-count">
                  已选{" "}
                  {
                    editViews.filter((name) => editSelected.includes(name))
                      .length
                  }{" "}
                  / {editViews.length} 个视图
                </div>
                {visibleEditViews.length ? (
                  <div className="job-edit-object-list">
                    {visibleEditViews.map((view) => (
                      <label key={view}>
                        <input
                          type="checkbox"
                          checked={editSelected.includes(view)}
                          onChange={() => toggleEditTable(view)}
                        />
                        <span>{view}</span>
                        <i className="object-kind view">视图</i>
                      </label>
                    ))}
                  </div>
                ) : editViews.length ? (
                  <div className="job-edit-object-empty">没有匹配的视图</div>
                ) : (
                  <div className="job-edit-object-empty">原任务没有视图</div>
                )}
              </section>
            </div>
            <div className="modal-actions">
              <button
                className="continue-button"
                onClick={submitEditedJob}
                disabled={editSaving}
              >
                {editSaving ? "创建中…" : "创建新任务并执行"}
              </button>
              <button
                className="secondary-button"
                onClick={() => setEditTemplate(null)}
              >
                取消
              </button>
            </div>
            {editMappingModalOpen && (
              <div
                className="modal-backdrop"
                role="presentation"
                onClick={(event) => {
                  if (event.target === event.currentTarget)
                    setEditMappingModalOpen(false);
                }}
              >
                <div
                  className="assessment-modal user-mapping-modal"
                  role="dialog"
                  aria-modal="true"
                >
                  <div className="modal-header">
                    <h3>配置用户名映射</h3>
                    <button
                      className="modal-close"
                      onClick={() => setEditMappingModalOpen(false)}
                      aria-label="关闭"
                    >
                      ×
                    </button>
                  </div>
                  <div className="modal-body">
                    <p className="owner-picker-tip">
                      建表 / 写数时目标 schema 使用映射后的用户名；DDL 中的
                      `源用户.对象` 引用同步替换。
                    </p>
                    <div className="user-mapping-list">
                      {editMappingDraft.map((mapping, index) => (
                        <div className="user-mapping-row" key={index}>
                          <input
                            value={mapping.source}
                            placeholder="源端用户（如 CLX）"
                            onChange={(event) =>
                              setEditMappingDraft((current) =>
                                current.map((item, itemIndex) =>
                                  itemIndex === index
                                    ? { ...item, source: event.target.value }
                                    : item,
                                ),
                              )
                            }
                          />
                          <span className="user-mapping-arrow">→</span>
                          <input
                            value={mapping.target}
                            placeholder="目标端用户名"
                            onChange={(event) =>
                              setEditMappingDraft((current) =>
                                current.map((item, itemIndex) =>
                                  itemIndex === index
                                    ? { ...item, target: event.target.value }
                                    : item,
                                ),
                              )
                            }
                          />
                          <button
                            type="button"
                            className="user-mapping-del"
                            title="删除该映射"
                            onClick={() =>
                              setEditMappingDraft((current) =>
                                current.filter(
                                  (_, itemIndex) => itemIndex !== index,
                                ),
                              )
                            }
                          >
                            ✕
                          </button>
                        </div>
                      ))}
                    </div>
                    <button
                      type="button"
                      className="user-mapping-add"
                      onClick={() =>
                        setEditMappingDraft((current) => [
                          ...current,
                          { source: "", target: "" },
                        ])
                      }
                    >
                      + 添加映射
                    </button>
                    {editMappingError && (
                      <p className="user-mapping-error">{editMappingError}</p>
                    )}
                  </div>
                  <div className="modal-actions">
                    <button
                      className="secondary-button"
                      onClick={() => setEditMappingModalOpen(false)}
                    >
                      取消
                    </button>
                    <button
                      className="continue-button"
                      onClick={() => {
                        const hasPartial = editMappingDraft.some(
                          (item) =>
                            (item.source.trim() && !item.target.trim()) ||
                            (!item.source.trim() && item.target.trim()),
                        );
                        if (hasPartial) {
                          setEditMappingError(
                            "请将源端用户与目标端用户名填写完整",
                          );
                          return;
                        }
                        const filled = editMappingDraft.filter(
                          (item) => item.source.trim() || item.target.trim(),
                        );
                        if (filled.length === 0) {
                          setEditUserMappings([]);
                          setEditMappingModalOpen(false);
                          setEditMappingError("");
                          return;
                        }
                        setEditUserMappings(
                          filled.map((item) => ({
                            source: item.source.trim(),
                            target: item.target.trim(),
                          })),
                        );
                        setEditMappingModalOpen(false);
                        setEditMappingError("");
                      }}
                    >
                      确定
                    </button>
                  </div>
                </div>
              </div>
            )}
            {editMappingViewOpen && (
              <div
                className="modal-backdrop"
                role="presentation"
                onClick={(event) => {
                  if (event.target === event.currentTarget)
                    setEditMappingViewOpen(false);
                }}
              >
                <div
                  className="assessment-modal user-mapping-modal"
                  role="dialog"
                  aria-modal="true"
                >
                  <div className="modal-header">
                    <h3>用户名映射配置</h3>
                    <button
                      className="modal-close"
                      onClick={() => setEditMappingViewOpen(false)}
                      aria-label="关闭"
                    >
                      ×
                    </button>
                  </div>
                  <div className="modal-body">
                    <div className="user-mapping-view-list">
                      {editUserMappings.map((mapping, index) => (
                        <div className="user-mapping-view-row" key={index}>
                          <b>{mapping.source}</b>
                          <span className="user-mapping-arrow">→</span>
                          <b>{mapping.target}</b>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div className="modal-actions">
                    <button
                      className="secondary-button"
                      onClick={() => {
                        setEditMappingViewOpen(false);
                        setEditMappingDraft(
                          editUserMappings.map((item) => ({ ...item })),
                        );
                        setEditMappingError("");
                        setEditMappingModalOpen(true);
                      }}
                    >
                      编辑
                    </button>
                    <button
                      className="continue-button"
                      onClick={() => setEditMappingViewOpen(false)}
                    >
                      关闭
                    </button>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
      {mappingModalOpen && (
        <div
          className="modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget)
              setMappingModalOpen(false);
          }}
        >
          <div
            className="assessment-modal user-mapping-modal"
            role="dialog"
            aria-modal="true"
          >
            <div className="modal-header">
              <h3>配置用户名映射</h3>
              <button
                className="modal-close"
                onClick={() => setMappingModalOpen(false)}
                aria-label="关闭"
              >
                ×
              </button>
            </div>
            <div className="modal-body">
              <p className="owner-picker-tip">
                建表 / 写数时目标 schema 使用映射后的用户名；DDL 中的
                `源用户.对象` 引用同步替换。
              </p>
              <div className="user-mapping-list">
                {mappingDraft.map((mapping, index) => (
                  <div className="user-mapping-row" key={index}>
                    <input
                      value={mapping.source}
                      placeholder="源端用户（如 CLX）"
                      onChange={(event) =>
                        setMappingDraft((current) =>
                          current.map((item, itemIndex) =>
                            itemIndex === index
                              ? { ...item, source: event.target.value }
                              : item,
                          ),
                        )
                      }
                    />
                    <span className="user-mapping-arrow">→</span>
                    <input
                      value={mapping.target}
                      placeholder="目标端用户名"
                      onChange={(event) =>
                        setMappingDraft((current) =>
                          current.map((item, itemIndex) =>
                            itemIndex === index
                              ? { ...item, target: event.target.value }
                              : item,
                          ),
                        )
                      }
                    />
                    <button
                      type="button"
                      className="user-mapping-del"
                      title="删除该映射"
                      onClick={() =>
                        setMappingDraft((current) =>
                          current.filter((_, itemIndex) => itemIndex !== index),
                        )
                      }
                    >
                      ✕
                    </button>
                  </div>
                ))}
              </div>
              <button
                type="button"
                className="user-mapping-add"
                onClick={() =>
                  setMappingDraft((current) => [
                    ...current,
                    { source: "", target: "" },
                  ])
                }
              >
                + 添加映射
              </button>
              {mappingError && (
                <p className="user-mapping-error">{mappingError}</p>
              )}
            </div>
            <div className="modal-actions">
              <button
                className="secondary-button"
                onClick={() => setMappingModalOpen(false)}
              >
                取消
              </button>
              <button
                className="continue-button"
                onClick={() => {
                  const hasPartial = mappingDraft.some(
                    (item) =>
                      (item.source.trim() && !item.target.trim()) ||
                      (!item.source.trim() && item.target.trim()),
                  );
                  if (hasPartial) {
                    setMappingError("请将源端用户与目标端用户名填写完整");
                    return;
                  }
                  const filled = mappingDraft.filter(
                    (item) => item.source.trim() || item.target.trim(),
                  );
                  if (filled.length === 0) {
                    setUserMappings([]);
                    setMappingModalOpen(false);
                    setMappingError("");
                    return;
                  }
                  setUserMappings(
                    filled.map((item) => ({
                      source: item.source.trim(),
                      target: item.target.trim(),
                    })),
                  );
                  setMappingModalOpen(false);
                  setMappingError("");
                }}
              >
                确定
              </button>
            </div>
          </div>
        </div>
      )}
      {mappingViewOpen && (
        <div
          className="modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) setMappingViewOpen(false);
          }}
        >
          <div
            className="assessment-modal user-mapping-modal"
            role="dialog"
            aria-modal="true"
          >
            <div className="modal-header">
              <h3>用户名映射配置</h3>
              <button
                className="modal-close"
                onClick={() => setMappingViewOpen(false)}
                aria-label="关闭"
              >
                ×
              </button>
            </div>
            <div className="modal-body">
              <div className="user-mapping-view-list">
                {userMappings.map((mapping, index) => (
                  <div className="user-mapping-view-row" key={index}>
                    <b>{mapping.source}</b>
                    <span className="user-mapping-arrow">→</span>
                    <b>{mapping.target}</b>
                  </div>
                ))}
              </div>
            </div>
            <div className="modal-actions">
              <button
                className="secondary-button"
                onClick={() => {
                  setMappingViewOpen(false);
                  setMappingDraft(userMappings.map((item) => ({ ...item })));
                  setMappingError("");
                  setMappingModalOpen(true);
                }}
              >
                编辑
              </button>
              <button
                className="continue-button"
                onClick={() => setMappingViewOpen(false)}
              >
                关闭
              </button>
            </div>
          </div>
        </div>
      )}
      {showOwnerPicker && (
        <div
          className="modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) setShowOwnerPicker(false);
          }}
        >
          <div className="assessment-modal" role="dialog" aria-modal="true">
            <div className="modal-header">
              <h3>筛选 owner</h3>
              <button
                className="modal-close"
                onClick={() => setShowOwnerPicker(false)}
                aria-label="关闭"
              >
                ×
              </button>
            </div>
            <div className="modal-body">
              <p className="owner-picker-tip">
                选择评估/迁移范围对应的
                owner（schema），支持多选；深度评估与读取对象均按所选范围收敛。
              </p>
              <div className="owner-picker-grid">
                {[...ownerList].sort(compareObjectNames).map((owner) => (
                  <button
                    key={owner}
                    className={`owner-picker-item ${
                      ownerFilters.includes(owner) ? "active" : ""
                    }`}
                    onClick={() => toggleOwner(owner)}
                  >
                    <b title={owner}>{owner}</b>
                    <small>
                      {ownerFilters.includes(owner)
                        ? "已选，点击取消"
                        : "点击选择"}
                    </small>
                  </button>
                ))}
              </div>
            </div>
            <div className="modal-actions">
              <button className="secondary-button" onClick={clearOwners}>
                清除选择
              </button>
              <button
                className="continue-button"
                onClick={() => setShowOwnerPicker(false)}
              >
                完成（已选 {ownerFilters.length} 个）
              </button>
            </div>
          </div>
        </div>
      )}
      {showOwnerWarning && (
        <div
          className="modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget)
              setShowOwnerWarning(false);
          }}
        >
          <div className="assessment-modal" role="dialog" aria-modal="true">
            <div className="modal-header">
              <h3>未选择 owner</h3>
              <button
                className="modal-close"
                onClick={() => setShowOwnerWarning(false)}
                aria-label="关闭"
              >
                ×
              </button>
            </div>
            <div className="modal-body">
              <p className="owner-picker-tip">
                当前未筛选
                owner，深度评估将扫描整个源库（含所有用户的表、视图、LOB
                等），可能导致数据量过大、评估缓慢甚至页面卡顿。
              </p>
              <p className="owner-picker-tip">
                建议先点击「筛选 owner」选择源端 schema（如
                CLX），再执行深度评估。
              </p>
            </div>
            <div className="modal-actions">
              <button
                className="continue-button"
                onClick={() => {
                  setShowOwnerWarning(false);
                  setShowOwnerPicker(true);
                }}
              >
                去筛选 owner
              </button>
              <button
                className="secondary-button"
                onClick={() => setShowOwnerWarning(false)}
              >
                取消
              </button>
            </div>
          </div>
        </div>
      )}
      {cancelConfirmJob && (
        <div
          className="modal-backdrop cancel-confirm-backdrop"
          role="presentation"
          onClick={(event) => {
            if (
              event.target === event.currentTarget &&
              cancellingJobId !== cancelConfirmJob.id
            ) {
              setCancelConfirmJob(null);
            }
          }}
        >
          <div
            className="cancel-confirm-modal"
            role="alertdialog"
            aria-modal="true"
            aria-labelledby="cancel-confirm-title"
            aria-describedby="cancel-confirm-description"
          >
            <div className="cancel-confirm-icon" aria-hidden="true">
              !
            </div>
            <h3 id="cancel-confirm-title">确认取消迁移任务？</h3>
            <p id="cancel-confirm-description">
              确认后将立即停止后续对象迁移，并把当前任务标记为“已取消”。已经写入目标库的数据不会自动回滚。
            </p>
            <div className="cancel-confirm-job">
              <span>任务名称</span>
              <b>{cancelConfirmJob.name}</b>
              <span>当前进度</span>
              <b>
                {cancelConfirmJob.tables_completed || 0}/
                {cancelConfirmJob.tables_total || 0} 个对象
              </b>
            </div>
            <div className="modal-actions">
              <button
                className="secondary-button"
                disabled={cancellingJobId === cancelConfirmJob.id}
                onClick={() => setCancelConfirmJob(null)}
              >
                返回
              </button>
              <button
                className="danger-button"
                disabled={cancellingJobId === cancelConfirmJob.id}
                onClick={() => void cancelJob(cancelConfirmJob)}
              >
                {cancellingJobId === cancelConfirmJob.id
                  ? "正在取消…"
                  : "确定取消"}
              </button>
            </div>
          </div>
        </div>
      )}
      {finishConfirmJob && (
        <div
          className="modal-backdrop cancel-confirm-backdrop"
          role="presentation"
          onClick={(event) => {
            if (
              event.target === event.currentTarget &&
              finishingJobId !== finishConfirmJob.id
            ) setFinishConfirmJob(null);
          }}
        >
          <div className="cancel-confirm-modal" role="alertdialog" aria-modal="true">
            <div className="cancel-confirm-icon" aria-hidden="true">✓</div>
            <h3>确认结束实时同步？</h3>
            <p>
              当前检查点之前的增量已经写入目标库。结束后不再监听新的 Oracle 变更，任务将正常标记为“已完成”，不是“已取消”。
            </p>
            <div className="cancel-confirm-job">
              <span>任务名称</span><b>{finishConfirmJob.name}</b>
              <span>增量阶段</span><b>{finishConfirmJob.sync_phase === "realtime" ? "实时同步" : "增量追平"}</b>
            </div>
            <div className="modal-actions">
              <button className="secondary-button" disabled={finishingJobId === finishConfirmJob.id} onClick={() => setFinishConfirmJob(null)}>继续同步</button>
              <button className="continue-button" disabled={finishingJobId === finishConfirmJob.id} onClick={() => void finishSyncJob(finishConfirmJob)}>
                {finishingJobId === finishConfirmJob.id ? "正在结束…" : "确认结束同步"}
              </button>
            </div>
          </div>
        </div>
      )}
      {resumeConfirmJob && (
        <div
          className="modal-backdrop cancel-confirm-backdrop"
          role="presentation"
          onClick={(event) => {
            if (
              event.target === event.currentTarget &&
              resumingJobId !== resumeConfirmJob.id
            ) setResumeConfirmJob(null);
          }}
        >
          <div className="cancel-confirm-modal" role="alertdialog" aria-modal="true">
            <div className="cancel-confirm-icon" aria-hidden="true">↻</div>
            <h3>确认继续增量同步？</h3>
            <p>
              系统会先检查该检查点对应的 Oracle redo/归档日志是否仍可读取；检查通过后，从最后保存的 SCN 继续追平并监听新变更。
            </p>
            <div className="cancel-confirm-job">
              <span>任务名称</span><b>{resumeConfirmJob.name}</b>
              <span>继续位点</span>
              <b>SCN {(resumeConfirmJob.checkpoint_scn || 0).toLocaleString("zh-CN")}</b>
            </div>
            <div className="modal-actions">
              <button
                className="secondary-button"
                disabled={resumingJobId === resumeConfirmJob.id}
                onClick={() => setResumeConfirmJob(null)}
              >
                返回
              </button>
              <button
                className="continue-button"
                disabled={resumingJobId === resumeConfirmJob.id}
                onClick={() => void resumeIncrementalJob(resumeConfirmJob)}
              >
                {resumingJobId === resumeConfirmJob.id ? "正在检查并继续…" : "确认继续同步"}
              </button>
            </div>
          </div>
        </div>
      )}
      {notice && (
        <div className="toast">
          <span>✓</span>
          {notice}
        </div>
      )}
    </main>
  );
}

export default function Home() {
  return <MigrationApp initialPage="workspace" />;
}
