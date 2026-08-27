# FlowDB 便携版归档

FlowDB：Oracle → TDSQL/MySQL 数据迁移同步工具（全量 + LogMiner 增量）。

本仓库归档 Linux 便携版安装包与配套文档，来源：小黄。

## 下载安装包

安装包超过 GitHub 单文件 100MB 限制，放在 [Releases](../../releases) 附件：

- `FlowDB-portable-linux-x86_64-20260827.tar`（112MB，Intel/AMD 64 位）

下载后按 `README-部署使用说明.md` 操作：解压 → `sha256sum -c checksums.sha256` → `./install.sh`，默认安装到 `/opt/flowdb`，Web 8080 / API 8000 起步自动找空闲端口。

## 文档

| 文件 | 内容 |
|---|---|
| [README-部署使用说明.md](README-部署使用说明.md) | 部署、启动停止、数据与升级保护、已验证环境 |
| [flowdb增量需要配置.docx](flowdb增量需要配置.docx) | Oracle LogMiner 增量同步的源库配置：归档模式、补充日志、迁移账号权限（非 CDB / CDB 两种方案）、无主键表处理、归档保留要求、完整测试流程 |

## 增量同步要点（摘自 docx）

- 源库须开启 `ARCHIVELOG` + 最小补充日志，建议 `FORCE LOGGING`
- CDB 环境需建 `C##` 公共账号连 CDB$ROOT，并在 `/opt/flowdb/.env` 配 `FLOWDB_LOGMINER_USERNAME / PASSWORD / SERVICE`
- 无主键表需配置业务唯一键 + 表级补充日志组
- 全量起始 SCN 到增量追平前，对应归档日志不能删除，否则只能重建全量基线
- 增量仅覆盖已提交的 INSERT / UPDATE / DELETE，不包含 DDL / TRUNCATE

## 技术栈

FastAPI + uvicorn + SQLAlchemy（Python 3.11）、Node.js 22 网关、SQLite 状态库；自带私有运行时，x86_64 兼容 glibc 2.17（CentOS 7）。

> 注：`README-部署使用说明.md` 中的 SHA-256 与文件名（20260824 / .tar.gz）对应旧版发布，实际包以 Releases 内 20260827 纯 `.tar` 为准，校验以包内 `checksums.sha256` 为准。
