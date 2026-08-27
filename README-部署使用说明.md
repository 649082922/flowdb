# FlowDB Linux 便携版部署使用说明

## 1. 选择安装包

- Intel/AMD 64 位 Linux：`FlowDB-portable-linux-x86_64-20260824.tar.gz`
- ARM64/aarch64 Linux：`FlowDB-portable-linux-aarch64-20260824.tar.gz`

安装包为纯 `tar.gz`，不安装 RPM，不调用 yum/dnf/apt，不依赖 Nginx，也不会替换系统 Python 或 Node.js。

默认由 `root` 安装并运行 API、网页网关和 systemd 服务。安装器不会创建 `flowdb` 用户。

## 2. 校验安装包

```bash
sha256sum -c FlowDB-portable-linux-x86_64-20260824.tar.gz.sha256
```

ARM64 服务器把文件名替换为 aarch64 包名。

已发布包 SHA-256：

- x86_64：`7ada491c3a2049291e215428cc910e18287d949f7550682a474f702d9257e804`
- aarch64：`ce67a9a3ea9012285a0449c64cf425110f2157561273d9355ea63043d09a145c`

## 3. 解压并安装

```bash
tar -xzf FlowDB-portable-linux-x86_64-20260824.tar.gz
cd FlowDB-portable-linux-x86_64-20260824
sha256sum -c checksums.sha256
./install.sh
```

默认安装目录为 `/opt/flowdb`，Web 起始端口为 8080，API 起始端口为 8000。

如需安装到其他目录：

```bash
FLOWDB_HOME=/home/flowdb ./install.sh
```

端口被占用时，安装器会从起始端口向上递增寻找空闲端口，并把最终 Web/API 端口同步写入：

```text
<安装目录>/runtime-selection.env
```

因此不要仅凭 8080 判断实际地址。安装结束时会显示最终访问地址，也可以执行 `status.sh` 查看。

## 4. Python 和 Node.js 选择逻辑

- 系统 CPython 为 3.11.x，且能够创建 venv、安装离线 wheel、通过核心依赖导入探测时，才复用系统 Python。
- 系统 Python 不符合条件时，使用包内私有 Python 3.11，不修改 `/usr/bin/python`。
- 系统 Node.js 为 22.13～22.x 时复用；否则使用包内私有 Node.js。
- x86_64 私有运行时支持 glibc 2.17+，用于兼容 CentOS 7；ARM64 包要求 glibc 2.28+。

运行时选择结果和原因保存在 `<安装目录>/runtime-selection.env`。

## 5. 启动、停止和状态检查

默认安装目录：

```bash
/opt/flowdb/bin/start.sh
/opt/flowdb/bin/stop.sh
/opt/flowdb/bin/status.sh
/opt/flowdb/bin/show-token.sh
```

若使用了自定义 `FLOWDB_HOME`，将 `/opt/flowdb` 替换成实际安装目录。

## 6. 数据与升级保护

- 状态数据库：`<安装目录>/data/flowdb.sqlite3`
- 加密密钥和访问令牌：`<安装目录>/.env`
- 程序版本：`<安装目录>/releases`
- 当前版本链接：`<安装目录>/current`

重复安装会保留数据和 `.env`。若已有状态库但 `.env` 丢失，安装器会停止，避免生成新密钥导致旧密码无法解密。

## 7. 已完成的实机验证

- Rocky Linux 8.10 x86_64：复用系统 Python 3.11 和 Node 22，因原 8080/8000 被占用，自动使用 8081/8001；root 进程、systemd、页面、静态资源和 API 均通过。
- CentOS 7 x86_64 / glibc 2.17：使用包内私有 Python 和 Node；root 进程、systemd、页面、静态资源和 API 均通过。因测试机根分区已满，实际安装在 `/home/flowdb`。
- ARM64 包已完成架构、文件、脚本语法及内外层哈希检查；因目前没有 ARM64 测试机，尚未完成 ARM64 实机运行验证。
