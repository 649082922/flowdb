# FlowDB

自部署的数据库迁移同步平台：Oracle / MySQL / PostgreSQL 之间做**迁移评估 → 全量迁移 → LogMiner 增量同步 → 数据校验**，全程数据不经过第三方服务。

> 作者：**鸡牛特战队**（[jiniu-squad](https://github.com/jiniu-squad)），由 [小黄（lee43787320）](https://github.com/lee43787320) 主导开发。

## 核心能力

**迁移前评估**
- 对象发现：普通表 / 分区表 / 视图 / 序列，按 Schema 浏览、全选筛选
- 深度评估（兼容性 / 容量估算）并导出报告（`assessment_deep.py`，后端最大模块）

**全量迁移**
- 任意两端组合：Oracle ⇄ MySQL ⇄ PostgreSQL（MySQL 协议兼容库如 TDSQL 走 MySQL 通道）
- 常见数字 / 字符 / 日期时间 / 布尔 / 二进制 / 大文本类型转换，目标表自动创建
- 流式批量迁移（每批 2,000 行），支持表并发、失败策略（失败即停止 / 失败继续 / 跳过失败表）
- 源用户 → 目标用户名映射

**增量同步（CDC）**
- Oracle 端基于 LogMiner 读取 redo，支持"全量 + 持续增量"、结束同步、继续增量（断点按 SCN 追平）
- 有主键表自动用主键定位；无主键表可配置业务唯一键 + 自动创建表级补充日志组
- CDB 环境支持独立 LogMiner 专用连接（CDB$ROOT 公共账号），与业务连接分离
- 详细源库配置要求见 [flowdb增量同步配置指南](flowdb增量同步配置指南.md)

**数据校验**
- 迁移后行数 + 全字段规范化哈希对比，两侧采样差异明细，分页报告
- 默认 4 表并发、单批 5,000 行（`FLOWDB_VALIDATION_CONCURRENCY` / `FLOWDB_VALIDATION_MAX_ROWS` 可调）

**安全**
- 连接配置 Fernet 加密落库（`FLOWDB_SECRET_KEY`），API 令牌鉴权（`FLOWDB_API_TOKEN`）

## 架构

```
浏览器 ── Node.js 22 网关(8080, 代理 /api) ── FastAPI 后端(8000, Python 3.11)
                                                   ├── database.py   连接/类型映射/对象发现
                                                   ├── assessment*.py 评估与报告
                                                   ├── worker.py     任务执行器（全量）
                                                   ├── cdc.py        OracleLogMiner + IncrementalReplicator
                                                   ├── validation.py 行数/全字段哈希校验
                                                   └── store.py      SQLite 状态库
前端：Next.js (vinext) 单页应用 —— 数据源 / 链路 / 节点 / 任务 / 校验 / 系统设置 六个页面
另含 Cloudflare Worker + Drizzle ORM 模块（worker/）
```

后端约 8,000 行 Python（含单元测试与 LogMiner 手工端到端测试），前端主组件 `app/page.tsx` 约 7,000 行 TSX。

## 快速开始

### 方式一：便携版（推荐，不需要联网装依赖）

从 [Releases](../../releases) 下载 `FlowDB-portable-linux-x86_64-20260827.tar`（112MB，自带 Python / Node 运行时与离线依赖，glibc 2.17+，兼容 CentOS 7）：

```bash
tar -xf FlowDB-portable-linux-x86_64-20260827.tar
cd FlowDB-portable-linux-x86_64-20260827
sha256sum -c checksums.sha256
./install.sh          # 默认 /opt/flowdb，Web 8080 / API 8000 起步自动找空闲端口
```

详见 [README-部署使用说明.md](README-部署使用说明.md)。

### 方式二：源码 Docker 部署

```bash
cp .env.example .env   # 填入 FLOWDB_SECRET_KEY / FLOWDB_API_TOKEN
docker compose up -d --build
```

详见 [DEPLOYMENT.md](DEPLOYMENT.md)（注：其"当前不支持增量 CDC"的表述对应早期版本，增量同步已实现）。

## 目录结构

```
app/          前端源码（TSX，六页面共用 app/page.tsx 的 MigrationApp）
backend/      FastAPI 后端（app/ 业务模块 + tests/ 单元与手工端到端测试）
worker/       Cloudflare Worker + Drizzle ORM
db/ deploy/   数据库 schema、部署配置（含 nginx.conf）
public/       静态资源
```

## 已知限制（增量同步）

- 仅覆盖已提交的 INSERT / UPDATE / DELETE；DDL、TRUNCATE、NOLOGGING 操作不在同步范围
- 全量起始 SCN 到增量追平前，对应归档日志不可删除，否则只能重建全量基线
- LOB / BFILE / XMLTYPE 不能作为无主键表的定位键
- 源库需开启 ARCHIVELOG + 补充日志，完整要求见[配置指南](flowdb增量同步配置指南.md)

## 来源说明

本仓库源码展开自便携版安装包 `payload/flowdb-release.tar.gz`（已剔除 `backend/.venv` 与构建产物 `dist/`）。`README.vinext-starter.md` 为前端脚手架模板自带的 README，非项目文档。
