# Databend 跨版本升级兼容性测试

自动化验证 Databend 从旧版本滚动升级到新版本的兼容性，覆盖：

- Meta 集群滚动升级（零停机）
- 新旧版本 Query 节点共存，共享同一 Meta 集群和 S3 存储
- 跨版本数据读写兼容性
- 权限/认证系统跨版本兼容性

支持两种部署模式：

- **local**（默认）：所有进程跑在同一台机器上
- **distributed**：Meta 和 Query 分布在多台 EC2 上，通过 SSH 编排

## 版本矩阵

| 组件 | 旧版本 | 中间版本 | 目标版本 |
|------|--------|----------|----------|
| Meta | v1.2.307 | v1.2.768 | v1.2.879 |
| Query | v1.2.636-rc8.6 | - | v1.2.887-nightly |

升级路径：`Meta v1.2.307 → v1.2.768 → v1.2.879`（滚动升级，每次一个节点）

## 目录结构

```
├── .env.example            # 环境变量模板
├── scripts/
│   ├── run.py              # 主入口
│   ├── remote.py           # Host 抽象层（LocalHost / RemoteHost）
│   ├── run.sh              # 薄封装（兼容旧 CI）
│   └── tests/              # YAML 测试套件
│       ├── test_basic_crud.yaml
│       ├── test_ddl.yaml
│       └── ...
└── docker-compose.yaml     # 分布式模式本地 Docker 测试环境
```

## 环境要求

- Linux x86_64
- MySQL 客户端（`mysql` 命令）
- Python 3 + PyYAML（`pip install pyyaml`）
- AWS S3 存储（或兼容的对象存储）
- 网络可访问 `https://repo.databend.com`

## 快速开始

1. 复制并填写环境变量：

```bash
cp .env.example .env
# 编辑 .env，填入 S3_BUCKET、S3_ACCESS_KEY_ID、S3_SECRET_ACCESS_KEY
```

2. 加载环境变量并执行：

```bash
export $(grep -v '^#' .env | xargs)
python3 scripts/run.py
```

## 命令行用法

```bash
# 完整执行（下载 + 升级 + 测试）
python3 scripts/run.py

# 跳过下载（已有二进制文件）
python3 scripts/run.py --skip-download

# 清理数据后重新执行（保留已下载的二进制文件）
python3 scripts/run.py --cleanup --skip-download

# 跳过新 GrantObject 类型权限测试
python3 scripts/run.py --skip-experimental-grants

# 指定初始 meta 版本
python3 scripts/run.py --initial-meta 1.2.292

# 只执行特定阶段
python3 scripts/run.py --skip-download meta-setup meta-upgrade query-setup compat-test

# 分布式模式
python3 scripts/run.py --mode distributed \
  --meta-hosts 10.0.1.1,10.0.1.2,10.0.1.3 \
  --query-hosts 10.0.2.1,10.0.2.2,10.0.2.3
```

可用阶段：`download`, `meta-setup`, `meta-upgrade`, `query-setup`, `compat-test`, `all`（默认）

## 环境变量

所有参数均可通过环境变量覆盖，详见 `.env.example`。

必填项：

| 变量 | 说明 |
|------|------|
| `S3_BUCKET` | S3 存储桶名称 |
| `S3_ACCESS_KEY_ID` | AWS Access Key |
| `S3_SECRET_ACCESS_KEY` | AWS Secret Key |

可选项：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `S3_ROOT` | `upgrade_test_data` | S3 路径前缀 |
| `S3_ENDPOINT_URL` | （空） | 自定义 S3 端点（MinIO 等） |
| `INITIAL_META_VERSION` | `1.2.307` | 初始 Meta 版本 |
| `INTERMEDIATE_META_VERSION` | `1.2.768` | 中间 Meta 版本 |
| `TARGET_META_VERSION` | `1.2.879` | 目标 Meta 版本 |
| `OLD_QUERY_VERSION` | `1.2.636` | 旧 Query 版本 |
| `NEW_QUERY_VERSION` | `1.2.887` | 新 Query 版本 |
| `DOWNLOAD_BASE_URL` | `https://repo.databend.com/databend/yf` | 二进制下载地址 |
| `WORK_DIR` | `./work` | 工作目录 |
| `SKIP_EXPERIMENTAL_GRANT_TESTS` | `0` | 设为 `1` 跳过新 GrantObject 权限测试 |

分布式模式专用：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DEPLOY_MODE` | `local` | 部署模式：`local` 或 `distributed` |
| `META_HOSTS` | （空） | Meta 节点 IP 列表，逗号分隔 |
| `QUERY_HOSTS` | （空） | Query 节点 IP 列表，逗号分隔 |
| `SSH_USER` | 当前用户 | SSH 登录用户名 |
| `SSH_KEY` | （空） | SSH 私钥路径 |
| `REMOTE_WORK_DIR` | 同 `WORK_DIR` | 远程主机工作目录 |

## 执行流程

```
1. Download     → 下载所有版本的 meta 和 query 二进制文件
                   （distributed 模式额外 rsync 到所有远程主机）
2. Meta Setup   → 启动 3 节点 meta 集群（初始版本）
3. Meta Upgrade → 启动 OLD query，验证连接正常并写入测试数据
                   滚动升级：v1.2.307 → v1.2.768 → v1.2.879
                   每次升级一个节点，升级后验证 query 仍可正常服务
4. Query Setup  → 启动 NEW query，与已在线的 OLD 共存
                   验证双方连接正常，双方均可读取升级前数据
5. Compat Test  → 依次执行 11 个测试套件
6. Cleanup      → 停止所有进程
```

## 分布式模式

分布式模式下，ctl（当前机器）通过 SSH 在远程 EC2 上启动进程：

```
                    ctl (本机)
                   ┌─────────┐
                   │ run.py  │  ← 发起部署、执行测试、生成报告
                   │ mysql   │  ← 连接远程 query 执行 SQL
                   └────┬────┘
                        │ SSH + mysql
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
    ┌──────────┐  ┌──────────┐  ┌──────────┐
    │ meta-1   │  │ meta-2   │  │ meta-3   │   ← 3 节点 raft 集群
    │ :9191    │  │ :9191    │  │ :9191    │
    └──────────┘  └──────────┘  └──────────┘
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
    ┌──────────┐  ┌──────────┐  ┌──────────┐
    │ query-1  │  │ query-2  │  │ query-3  │
    │ OLD:3307 │  │ OLD:3307 │  │ OLD:3307 │   ← cluster_old
    │ NEW:3308 │  │ NEW:3308 │  │ NEW:3308 │   ← cluster_new
    └──────────┘  └──────────┘  └──────────┘
```

distributed 模式下所有节点使用相同端口（每个节点在不同主机上，无需 offset）。

### Docker 本地测试

可用 `docker-compose.yaml` 在本地模拟分布式环境（6 个容器模拟 6 台 EC2）：

```bash
# 生成 SSH 密钥（首次）
mkdir -p .docker/ssh && ssh-keygen -t ed25519 -f .docker/ssh/id_ed25519 -N "" -q

# 启动容器
docker compose up -d

# 运行测试（需先配置 .env 中的 S3 凭证）
docker compose exec ctl bash -c 'export $(grep -v "^#" /workspace/.env | xargs) && python3 /workspace/scripts/run.py --mode distributed --meta-hosts meta1,meta2,meta3 --query-hosts query1,query2,query3 --skip-download'

# 重跑前清理远程节点数据（Docker 模式下需手动清理）
docker compose exec ctl bash -c '
  for h in meta1 meta2 meta3 query1 query2 query3; do
    ssh -o StrictHostKeyChecking=no $h "pkill -9 databend 2>/dev/null; rm -rf /tmp/databend-upgrade-test/work/{data,logs,conf}"
  done
'

# 销毁容器
docker compose down
```

## 测试套件

| 套件 | 文件 | 说明 |
|------|------|------|
| Basic CRUD | `test_basic_crud.yaml` | INSERT/SELECT/UPDATE/DELETE/MERGE INTO 跨版本操作 |
| DDL | `test_ddl.yaml` | CREATE/ALTER/RENAME TABLE、VIEW、CLUSTER KEY、TRUNCATE |
| Data Types | `test_data_types.yaml` | BOOLEAN、INT、FLOAT、DECIMAL、VARCHAR、DATE、TIMESTAMP、JSON、ARRAY、TUPLE、MAP、NULL、Decimal256 |
| Cross-Version R/W | `test_cross_version_rw.yaml` | 批量写入、COPY INTO、混合聚合、跨版本 JOIN |
| Complex Queries | `test_complex_queries.yaml` | 子查询、CTE、窗口函数、UNION、GROUP BY、CASE、EXISTS |
| Permissions Safe | `test_permissions_safe.yaml` | CREATE USER、GRANT、ROLE、ALTER USER、REVOKE 跨版本 |
| Permissions New Objects | `test_permissions_new_grant_objects.yaml` | 6 种新 GrantObject 类型兼容性 |
| Snapshot Statistics | `test_snapshot_statistics.yaml` | TableSnapshotStatistics v3/v4 跨版本兼容性 |
| Replace Into | `test_replace_into.yaml` | REPLACE INTO 跨版本兼容性 |
| Time Travel | `test_time_travel.yaml` | Time Travel（AT TIMESTAMP）跨版本兼容性 |
| Flashback | `test_flashback.yaml` | ALTER TABLE FLASHBACK 跨版本兼容性 |

测试用例采用 YAML 声明式定义，支持的断言类型：`ok`、`eq`、`fail`、`known_fail`、`contains`、`fail_contains`。
