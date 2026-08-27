# FlowDB Oracle 增量同步（LogMiner）配置指南

> 由 `flowdb增量需要配置.docx` 整理，密钥示例已脱敏。当前实现按 Oracle 19c 及以上设计和测试。

## 一、Oracle 必须满足的条件

### 1. 开启归档模式

先检查：

```sql
SELECT name, log_mode, force_logging,
       supplemental_log_data_min,
       supplemental_log_data_pk,
       supplemental_log_data_ui,
       supplemental_log_data_all
FROM v$database;
```

必须满足：

- `LOG_MODE = ARCHIVELOG`
- `SUPPLEMENTAL_LOG_DATA_MIN = YES 或 IMPLICIT`

如果还是 NOARCHIVELOG，需要以 SYSDBA 执行：

```sql
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
```

建议同时开启强制日志，防止 NOLOGGING 操作产生无法同步的数据：

```sql
ALTER DATABASE FORCE LOGGING;
```

ASM 和普通文件系统都可以。FlowDB 不直接读取归档文件路径，而是通过 Oracle 的 `DBMS_LOGMNR` 读取，因此路径不用配置到 FlowDB。

RAC 环境注意：连接实例必须能访问所有相关线程的归档日志。使用共享 ASM 通常没有问题；如果各节点归档放在本机文件系统，则可能出现其他线程归档不可访问。

### 2. 开启最小补充日志

以 SYSDBA 执行：

```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
```

这是 LogMiner 正确重建事务和 DML 的基础要求。

如果迁移表基本都有主键或唯一键，也可以开启数据库级键值日志：

```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY, UNIQUE) COLUMNS;
```

这会增加一些 redo。生产环境如果只同步少量表，更建议使用表级补充日志，避免全库增加不必要的日志量。

## 二、迁移账号权限

### 方案 A：单机、非 CDB Oracle

可以直接让 FlowDB 源库账号同时承担全量和 LogMiner 读取。以 SYS 执行，假设账号为 `FLOWDB_SRC`：

```sql
GRANT CREATE SESSION TO FLOWDB_SRC;
GRANT SELECT ANY TRANSACTION TO FLOWDB_SRC;
GRANT LOGMINING TO FLOWDB_SRC;
GRANT SELECT_CATALOG_ROLE TO FLOWDB_SRC;
GRANT EXECUTE_CATALOG_ROLE TO FLOWDB_SRC;
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO FLOWDB_SRC;
```

业务数据读取可以选择：

```sql
GRANT SELECT ANY TABLE TO FLOWDB_SRC;
-- 或按最小权限逐表授权：
GRANT SELECT ON CLX.TABLE_A TO FLOWDB_SRC;
GRANT SELECT ON CLX.TABLE_B TO FLOWDB_SRC;
```

如果环境对目录角色管控严格，也可以改成直接授权 FlowDB 当前实际访问的视图：

```sql
GRANT SELECT ON SYS.V_$DATABASE TO FLOWDB_SRC;
GRANT SELECT ON SYS.V_$TRANSACTION TO FLOWDB_SRC;
GRANT SELECT ON SYS.V_$ARCHIVED_LOG TO FLOWDB_SRC;
GRANT SELECT ON SYS.V_$LOG TO FLOWDB_SRC;
GRANT SELECT ON SYS.V_$LOGFILE TO FLOWDB_SRC;
GRANT SELECT ON SYS.V_$LOGMNR_CONTENTS TO FLOWDB_SRC;
```

`SELECT_CATALOG_ROLE` 和 `EXECUTE_CATALOG_ROLE` 分别用于读取数据字典和执行字典包。

### 方案 B：CDB/PDB Oracle

建议使用两个连接：

- FlowDB 页面里的源库连接：连接业务 PDB，用于全量读取和表结构检查
- LogMiner 专用连接：连接 CDB$ROOT，读取整个 CDB 的 redo（传统 CDB LogMiner 的 `V$LOGMNR_CONTENTS` 主要在根容器使用）

在 CDB$ROOT 创建专用公共账号：

```sql
ALTER SESSION SET CONTAINER = CDB$ROOT;

CREATE USER C##FLOWDB_LOGMINER
IDENTIFIED BY "请替换为强密码"
CONTAINER = ALL;

GRANT CREATE SESSION TO C##FLOWDB_LOGMINER CONTAINER = ALL;
GRANT LOGMINING TO C##FLOWDB_LOGMINER CONTAINER = ALL;
GRANT SELECT ANY TRANSACTION TO C##FLOWDB_LOGMINER CONTAINER = ALL;
GRANT SELECT_CATALOG_ROLE TO C##FLOWDB_LOGMINER CONTAINER = ALL;
GRANT EXECUTE_CATALOG_ROLE TO C##FLOWDB_LOGMINER CONTAINER = ALL;
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO C##FLOWDB_LOGMINER;
```

然后在 FlowDB 服务器的 `/opt/flowdb/.env` 添加：

```ini
FLOWDB_LOGMINER_USERNAME=C##FLOWDB_LOGMINER
FLOWDB_LOGMINER_PASSWORD=请替换为实际密码
FLOWDB_LOGMINER_SERVICE=连接CDB_ROOT的服务名
```

配置示例（密钥已脱敏）：

```ini
FLOWDB_SECRET_KEY=<安装时自动生成>
FLOWDB_API_TOKEN=<安装时自动生成>
FLOWDB_LOGMINER_SERVICE=ORCL
FLOWDB_LOGMINER_USERNAME=C##FLOWDB_CDC
FLOWDB_LOGMINER_PASSWORD=<请替换为实际密码>
```

修改后重启：`systemctl restart flowdb-api`

如果不是 CDB，可以不配置这三个变量，FlowDB 会复用页面配置的 Oracle 源连接。

## 三、不同表需要怎样处理

### 有主键的表（最推荐）

确保：主键有效；主键值不为空；redo 中包含主键旧值。可以使用数据库级主键补充日志，或者让 FlowDB 创建对应的表级日志。

### 无主键但有非空唯一键

FlowDB 会选择可靠的非空唯一键定位行。建议对该键增加表级补充日志，例如：

```sql
ALTER TABLE CLX.ORDERS
ADD SUPPLEMENTAL LOG GROUP FDBC_ORDERS_UK (TENANT_ID, ORDER_NO) ALWAYS;
```

### 没有主键和唯一键，但有业务唯一键

在 FlowDB 页面配置（如 `ORDERS=TENANT_ID,ORDER_NO`、`CUSTOMER=CODE`）。业务键必须：能唯一定位一行；最好全部为 NOT NULL；同步期间不能随意产生重复值。

然后：勾选"允许在源表自动创建补充日志组"由 FlowDB 创建；或让 DBA 提前执行表级补充日志 SQL（安全性更高，不需要给业务账号 ALTER 权限）。

### 完全没有可靠键的表

只能选择 ALL COLUMNS 高风险模式，仅适合测试或明确能靠整行定位的表。限制：重复行可能无法安全更新或删除；LOB、BFILE、XMLTYPE、对象类型等不能作为可靠定位键；大字段显著增加 redo 和定位开销。生产环境更建议先增加主键或业务唯一键。

## 四、归档日志保留要求（增量最容易忽略的问题）

FlowDB 会记录全量开始 SCN 和当前增量检查点 SCN。**从记录 SCN 开始到增量追平之前，对应归档日志不能被 RMAN、清理脚本或存储策略删除。**

- 测试环境至少保留 2～3 天
- 生产环境根据：全量迁移耗时、最大可能中断时间、是否需要"继续增量同步"、每天归档产生量，设置足够的保留窗口

如果检查点对应的归档已不存在，FlowDB 会安全停止并提示：`增量起始 SCN 对应的 redo/归档日志已不存在，无法安全继续，请重新执行全量基线`。

## 五、建议的完整测试流程

1. 检查 ARCHIVELOG 和补充日志状态
2. 保存 Oracle → TDSQL/MySQL 链路
3. 在"同步方式"选择"全量 + LogMiner 持续增量"
4. 选择有主键、无主键、普通表和分区表
5. 启动任务，确认日志先出现：`LogMiner 前置检查通过`、`ARCHIVELOG=ARCHIVELOG`、`最小补充日志=YES`、`起始 SCN=...`
6. 全量迁移期间向 Oracle 执行并提交 INSERT / UPDATE / DELETE
7. 全量完成、进入实时监听后，再执行一轮 INSERT / UPDATE / DELETE
8. 检查目标端数据和 FlowDB 增量计数
9. 点击"结束同步"，任务应显示"同步已结束"（不是"已取消"）
10. 执行迁移校验，检查整表数据一致性
11. 再测试"继续增量同步"，确认对应 SCN 的归档仍存在且能够继续追平

## 六、测试期间的限制

测试过程中避免：

- TRUNCATE TABLE
- 修改表结构
- DROP / RENAME TABLE
- 使用 NOLOGGING 或无法产生完整 redo 的批量操作
- 在增量追平前删除归档日志

当前增量主要处理已提交的 INSERT / UPDATE / DELETE 及相关 LOB 写入，不应把 DDL 当成可靠的增量同步能力。

## 参考

- [Oracle LogMiner 官方说明](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)
- [Oracle CDB LogMiner 说明](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)
- [Oracle 权限说明](https://docs.oracle.com/en/database/oracle/oracle-database/19/dbseg/managing-system-privileges.html)
