# FlowDB 自部署与使用

FlowDB 的网页、迁移 API 和任务执行器都运行在你的服务器上。迁移数据不经过第三方服务。

## 1. 服务器要求

- Linux x86_64 或 ARM64
- Docker Engine 24+ 与 Docker Compose v2
- 建议至少 4 核 CPU、8 GB 内存、20 GB 可用磁盘
- 服务器网络必须能访问源库和目标库端口
- 对外使用时，应由现有反向代理提供 HTTPS，并限制可信 IP 或 VPN

Oracle 使用 python-oracledb Thin 模式，不需要安装 Oracle Instant Client。Thin 模式支持常用 Oracle 版本；如果数据库仅允许旧式认证或需要 Thick 特性，需要在后端镜像中额外安装 Instant Client。

## 2. 启动

复制环境变量模板并填写两个密钥：

```bash
cp .env.example .env
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
openssl rand -hex 32
```

将第一条输出写入 `FLOWDB_SECRET_KEY`，第二条输出写入 `FLOWDB_API_TOKEN`，然后启动：

```bash
docker compose up -d --build
```

访问 `http://服务器IP:8080`，打开左侧“系统设置”，把 `.env` 中的 `FLOWDB_API_TOKEN` 填入访问令牌。API 地址留空。

## 3. 数据库账号权限

源库账号仅需读取权限：

- Oracle：目标 Schema 中表的 `SELECT` 权限，以及可读取元数据
- MySQL：目标数据库的 `SELECT` 权限
- PostgreSQL：Schema 的 `USAGE` 和表的 `SELECT` 权限

目标库账号至少需要 `CREATE`、`INSERT` 权限。若使用“清空后重写”或“删除并重建”，还需要 `TRUNCATE` 或 `DROP` 权限。

## 4. 当前支持范围

- Oracle、MySQL、PostgreSQL 任意两端组合
- 连接测试、Schema 表发现、主键识别
- 常见数字、字符、日期时间、布尔、二进制和大文本类型转换
- 目标表自动创建
- 每批 2,000 行的流式全量迁移
- 任务进度、行数、数据量、错误信息和取消标记
- 任务与加密后的连接配置持久化

当前版本不包含增量 CDC、外键/索引/触发器/视图/序列/存储过程迁移，也不做迁移后逐行校验。大表迁移前建议先用测试库演练，并保留目标库备份。

## 5. 运维

查看状态与日志：

```bash
docker compose ps
docker compose logs -f api
```

升级版本时不要删除 `flowdb_state` 数据卷。`FLOWDB_SECRET_KEY` 必须长期保留；更换后旧任务中的加密连接配置将无法解密。

