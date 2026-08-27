from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

DatabaseType = Literal["oracle", "mysql", "postgresql", "tdsql"]
SyncMode = Literal["full_only", "full_then_incremental", "full_and_incremental", "incremental_only"]
CdcNoKeyPolicy = Literal["reject", "all_columns"]


class ConnectionConfig(BaseModel):
    type: DatabaseType
    host: str = Field(min_length=1, max_length=255)
    port: int = Field(ge=1, le=65535)
    database: str = Field(min_length=1, max_length=255)
    username: str = Field(min_length=1, max_length=255)
    password: str = Field(min_length=1, max_length=2048)
    schema_name: str | None = Field(default=None, max_length=255)

    @field_validator("host", "database", "username", "schema_name")
    @classmethod
    def reject_control_characters(cls, value: str | None) -> str | None:
        if value is not None and any(ord(char) < 32 for char in value):
            raise ValueError("字段包含无效控制字符")
        return value


class TestConnectionRequest(BaseModel):
    connection: ConnectionConfig


class TableListRequest(BaseModel):
    connection: ConnectionConfig
    owners: list[str] | None = Field(default=None, max_length=50, description="按 owner/schema 列表筛选对象，为空时使用连接默认 schema")


class TableInfo(BaseModel):
    schema_name: str | None
    name: str
    columns: int
    primary_keys: list[str]
    object_type: Literal["table", "view", "sequence", "partitioned_table"] = "table"


class EditableConnectionConfig(BaseModel):
    type: DatabaseType
    host: str = Field(min_length=1, max_length=255)
    port: int = Field(ge=1, le=65535)
    database: str = Field(min_length=1, max_length=255)
    username: str = Field(min_length=1, max_length=255)
    password: str = Field(default="", max_length=2048)
    schema_name: str | None = Field(default=None, max_length=255)


class MigrationLinkCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    source: ConnectionConfig
    target: ConnectionConfig


class MigrationLinkUpdate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    source: EditableConnectionConfig
    target: EditableConnectionConfig


class MigrationLinkSummary(BaseModel):
    id: str
    name: str
    source: dict[str, Any]
    target: dict[str, Any]
    created_at: str
    updated_at: str


class UserMapping(BaseModel):
    """用户名映射：源端用户（owner/schema）→ 目标端用户名。

    迁移建表/写数时，源 schema 下的对象在目标端归属映射后的用户名；
    DDL 中 `源用户.对象` 引用会替换为目标用户名。
    """
    source: str = Field(min_length=1, max_length=255, description="源端用户/owner（如 CLX）")
    target: str = Field(min_length=1, max_length=255, description="目标端用户名（如 mig_user）")

    @field_validator("source", "target")
    @classmethod
    def reject_invalid_user(cls, value: str) -> str:
        value = value.strip()
        if not value or any(ord(char) < 32 for char in value):
            raise ValueError("用户名无效")
        return value


class DeepAssessmentRequest(BaseModel):
    source: ConnectionConfig | None = None
    target: ConnectionConfig | None = None
    link_id: str | None = Field(default=None, max_length=64)
    owners: list[str] | None = Field(default=None, max_length=50, description="源端评估 owner/schema 列表，为空时保持默认（可能扫描全库）")
    bandwidth_mbps: float = Field(default=50.0, ge=1, le=10000, description="迁移耗时估算用网络带宽（Mbps），默认 50")
    batch_size: int = Field(default=2000, ge=100, le=20000, description="耗时估算用每批写入行数，默认 2000")
    table_concurrency: int = Field(default=1, ge=1, le=16, description="耗时估算用并发迁移表数，默认 1")


class CreateJobRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    source: ConnectionConfig | None = None
    target: ConnectionConfig | None = None
    link_id: str | None = Field(default=None, max_length=64)
    link_name: str | None = Field(default=None, max_length=120)
    tables: list[str] = Field(default_factory=list, max_length=500)
    object_types: dict[str, Literal["table", "view", "sequence", "partitioned_table"]] = Field(default_factory=dict)
    sequences: list[str] = Field(default_factory=list, max_length=200, description="需要迁移的源端序列名列表（目标为 TDSQL 时生效，序列在表之前创建）")
    batch_size: int = Field(default=2000, ge=100, le=20000)
    table_concurrency: int = Field(default=1, ge=1, le=16)
    existing_table: Literal["fail", "append", "truncate", "drop_and_create"] = "fail"
    create_tables: bool = True
    migration_content: Literal["structure_and_data", "structure_only", "data_only"] = "structure_and_data"
    fail_policy: Literal["stop_on_error", "continue_on_error"] = Field(default="stop_on_error", description="失败策略：stop_on_error 失败即停止；continue_on_error 失败继续迁移其余表")
    migrate_sequences: bool = Field(default=True, description="是否迁移源端序列（目标为 TDSQL 时生效，序列在表之前创建）")
    user_mappings: list[UserMapping] = Field(default_factory=list, max_length=50, description="用户名映射：源端用户→目标端用户名，建表/写数时目标 schema 使用映射后的用户名")
    identifier_case_policy: Literal["auto", "preserve", "lower", "upper"] = Field(
        default="auto", description="目标表/视图命名策略；不会修改目标数据库 lower_case_table_names"
    )
    sync_mode: SyncMode = Field(
        default="full_only",
        description="full_only 仅全量；full_then_incremental 全量成功后保留 SCN；full_and_incremental 全量后持续同步；incremental_only 从指定 SCN 开始",
    )
    start_scn: int | None = Field(default=None, ge=1, description="仅增量模式的 Oracle 起始 SCN")
    cdc_poll_seconds: float = Field(default=3.0, ge=1, le=60)
    cdc_window_scn: int = Field(default=100000, ge=1000, le=1000000)
    cdc_key_overrides: dict[str, list[str]] = Field(
        default_factory=dict,
        max_length=500,
        description="无主键表的用户指定业务唯一键，格式为对象名到字段名列表",
    )
    cdc_no_key_policy: CdcNoKeyPolicy = Field(
        default="reject",
        description="没有可靠键时拒绝，或显式使用 ALL COLUMNS 风险模式",
    )
    cdc_allow_source_ddl: bool = Field(
        default=False,
        description="允许 FlowDB 为业务键或 ALL COLUMNS 模式创建表级补充日志",
    )

    @model_validator(mode="after")
    def validate_migration_content(self):
        if not self.link_id and (self.source is None or self.target is None):
            raise ValueError("必须提供源端和目标端连接，或选择已保存链路")
        active_sequences = self.sequences if self.migrate_sequences else []
        if not self.tables and not active_sequences:
            raise ValueError("序列、表和视图均可单独迁移；必须至少选择一个对象")
        if self.migration_content == "data_only" and self.existing_table not in {"append", "truncate"}:
            raise ValueError("仅迁移数据时，目标表策略必须是追加数据或清空后重写")
        if self.sync_mode != "full_only":
            if not self.tables:
                raise ValueError("增量同步必须至少选择一张可可靠定位的表")
            if self.migration_content == "structure_only":
                raise ValueError("增量同步不能选择仅迁移表结构")
            if self.sync_mode == "incremental_only" and self.start_scn is None:
                raise ValueError("仅增量模式必须填写有效的 Oracle 起始 SCN")
        for table in self.tables:
            self.object_types.setdefault(table, "table")
        selected_upper = {
            alias
            for table in self.tables
            for alias in {table.upper(), table.rsplit(".", 1)[-1].strip('"').upper()}
        }
        for table, columns in self.cdc_key_overrides.items():
            if table.upper() not in selected_upper:
                raise ValueError(f"业务键对象不在本次迁移范围：{table}")
            clean_columns = [column.strip() for column in columns if column.strip()]
            if not clean_columns or len(clean_columns) > 32:
                raise ValueError(f"业务键字段配置无效：{table}")
            if any(len(column) > 128 or any(ord(char) < 32 for char in column) for column in clean_columns):
                raise ValueError(f"业务键字段名称无效：{table}")
            self.cdc_key_overrides[table] = list(dict.fromkeys(clean_columns))
        return self

    @field_validator("tables")
    @classmethod
    def validate_tables(cls, tables: list[str]) -> list[str]:
        clean = []
        for name in tables:
            value = name.strip()
            if not value or len(value) > 255 or any(ord(char) < 32 for char in value):
                raise ValueError("表名无效")
            clean.append(value)
        return list(dict.fromkeys(clean))


class TableResult(BaseModel):
    table: str
    target_table: str | None = None
    object_type: str = "table"
    status: Literal["success", "failed", "cancelled", "skipped"]
    rows: int = 0
    bytes: int = 0
    error: str | None = None
    notes: str | None = Field(default=None, description="迁移提示（如 Oracle 间隔分区表降级说明，多条以分号分隔）")
    started_at: str | None = None
    finished_at: str | None = None
    elapsed_ms: int = 0


class PhaseProgress(BaseModel):
    phase: Literal["sequence", "table", "partitioned_table", "view"]
    label: str
    total: int = 0
    completed: int = 0
    failed: int = 0
    cancelled: int = 0
    running: int = 0
    pending: int = 0
    progress: float = 0
    current_objects: list[str] = Field(default_factory=list)


class JobSummary(BaseModel):
    id: str
    name: str
    source_type: DatabaseType
    target_type: DatabaseType
    status: str
    progress: float
    rows_copied: int
    bytes_copied: int
    current_table: str | None
    created_at: str
    started_at: str | None
    finished_at: str | None
    error: str | None
    tables_total: int
    tables_completed: int
    migration_content: str
    batch_size: int
    table_concurrency: int
    link_id: str | None = None
    link_name: str | None = None
    fail_policy: str = "stop_on_error"
    identifier_case_policy: str = "auto"
    identifier_case_resolved: str = "preserve"
    target_lower_case_table_names: int | None = None
    table_results: list[TableResult] = Field(default_factory=list)
    phase_progress: list[PhaseProgress] = Field(default_factory=list)
    sync_mode: SyncMode = "full_only"
    sync_phase: str = "full"
    start_scn: int | None = None
    checkpoint_scn: int | None = None
    source_current_scn: int | None = None
    cdc_lag: int = 0
    cdc_events: int = 0
    cdc_transactions: int = 0
    cdc_inserts: int = 0
    cdc_updates: int = 0
    cdc_deletes: int = 0
    cdc_started_at: str | None = None
    cdc_last_event_at: str | None = None


class JobLogEntry(BaseModel):
    seq: int
    ts: str
    level: str
    message: str


class JobLogResponse(BaseModel):
    job_id: str
    after_seq: int
    logs: list[JobLogEntry]


class JobEditTemplate(BaseModel):
    """任务编辑模板：返回创建任务所需的可编辑参数，密码字段一律脱敏隐藏，提交时沿用链路凭据。"""
    id: str
    name: str
    link_id: str | None = None
    link_name: str | None = None
    tables: list[str]
    object_types: dict[str, Literal["table", "view", "sequence", "partitioned_table"]]
    sequences: list[str] = Field(default_factory=list)
    batch_size: int
    table_concurrency: int
    existing_table: Literal["fail", "append", "truncate", "drop_and_create"]
    create_tables: bool
    migration_content: Literal["structure_and_data", "structure_only", "data_only"]
    fail_policy: Literal["stop_on_error", "continue_on_error"]
    migrate_sequences: bool = True
    user_mappings: list[UserMapping] = Field(default_factory=list)
    identifier_case_policy: Literal["auto", "preserve", "lower", "upper"] = "auto"
    sync_mode: SyncMode = "full_only"
    start_scn: int | None = None
    cdc_poll_seconds: float = 3.0
    cdc_key_overrides: dict[str, list[str]] = Field(default_factory=dict)
    cdc_no_key_policy: CdcNoKeyPolicy = "reject"
    cdc_allow_source_ddl: bool = False


class PaginatedJobs(BaseModel):
    items: list[JobSummary]
    total: int
    page: int
    page_size: int
    pages: int
