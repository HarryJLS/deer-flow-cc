# DeerFlow × OceanBase 对接部署指南

> 目标：让一名运维/对接同学在拿到一个 OceanBase 集群之后，按这份文档把 DeerFlow 智能体应用从零跑起来。
>
> 适用版本：DeerFlow `main` 分支（含 `feat(persistence): add OceanBase backend` 提交），OceanBase ≥ 4.2.1。
>
> 与 [`OCEANBASE.md`](./OCEANBASE.md) 的差异：后者偏重设计/原理，本文档偏重**逐步操作**与**最少配置项清单**。

---

## 0. TL;DR — 三分钟版

1. OceanBase 集群升级到 ≥ 4.2.1（必须，否则 JSON / `ON DUPLICATE KEY UPDATE ... AS new` 不可用）。
2. 创建一个空库 `deerflow`，charset = `utf8mb4`，collation = `utf8mb4_unicode_ci`。
3. 创建一个用户，授予 `CREATE / SELECT / INSERT / UPDATE / DELETE / INDEX / ALTER` 七个权限。
4. 在项目根 `.env` 中写入 `OCEANBASE_URL=mysql://user:pass@host:2881/deerflow`。
5. 在项目根 `config.yaml` 中把 `database.backend` 改为 `oceanbase`，URL 填 `$OCEANBASE_URL`。
6. `cd backend && uv sync --all-packages --extra oceanbase`。
7. 项目根 `make dev`。看到 Gateway 健康检查 200 即对接完成；表会自动建出来。

剩下的所有内容都是对这 7 步的展开 + 排错。

---

## 1. 架构概览

DeerFlow 持久化分两层，OceanBase 同时承接：

| 层 | 谁在写 | 表 |
|---|---|---|
| 应用 ORM（async SQLAlchemy） | Gateway / `app.*` 路由 | `users`、`threads_meta`、`runs`、`run_events`、`feedback`（**5 张**） |
| LangGraph Checkpointer（`AsyncOceanBaseSaver`） | 智能体运行时 | `checkpoints`、`writes`（**2 张**） |

两层**共用同一个连接串**（`database.oceanbase_url`），但用的是**两个不同的连接池**：

- 应用 ORM 用 `create_async_engine()` + asyncmy，由 `init_engine_from_config()` 初始化；
- Checkpointer 由 `AsyncOceanBaseSaver.from_conn_string()` 在 FastAPI lifespan 内构建独立的 `asyncmy.Pool`。

这意味着：**OceanBase 端的连接数 ≈ `database.pool_size × 2 + 少量瞬时连接`**，估容时记得乘 2。

> ⚠️ OceanBase Saver 是 **async-only**。同步 graph 编译路径（CLI、`LangGraph Studio`）一旦走 `oceanbase` 后端会抛 `NotImplementedError`。生产部署走 Gateway 的 FastAPI lifespan，本就是 async 的，**没有影响**。

---

## 2. 前置条件

### 2.1 OceanBase 集群

| 项 | 要求 | 原因 |
|---|---|---|
| 版本 | **≥ 4.2.1** | JSON、CTE、窗口函数、`ON DUPLICATE KEY UPDATE ... AS new` 全部需要 |
| 模式 | MySQL 兼容模式 | DeerFlow 通过 MySQL wire 协议接入；Oracle 模式不支持 |
| `max_allowed_packet` | ≥ **16 MB** | `checkpoints.checkpoint` / `writes.value` 列是 `LONGBLOB`，单条最大可超 4 MB |
| `lower_case_table_names` | 建议 `1` | 与 DDL 中的全小写表名一致，避免大小写歧义 |
| 字符集 | 数据库默认 `utf8mb4` | 否则 emoji / 4 字节 unicode 会被截断（且应用层 `STRICT_TRANS_TABLES` 会直接报错） |
| 网络 | OBProxy 端口可达（默认 `2881`） | 客户端走 OBProxy，不直连 OBServer |
| 时间戳 | 不要求容器时区，但建议 UTC | DeerFlow 每次建立会话都会 `SET SESSION time_zone='+00:00'` |

### 2.2 应用侧依赖

- Python 3.12+；
- `uv`（项目已锁定）；
- 网络能访问 OceanBase / OBProxy；
- 已经能跑通 `sqlite` backend（推荐先用 sqlite 跑一次，确认应用层无问题再切 OceanBase）。

---

## 3. OceanBase 侧准备

### 3.1 建库

可以不手动建——DeerFlow 启动时 `_auto_create_mysql_db()` 检测到错误码 `1049 Unknown database` 会**自动**执行：

```sql
CREATE DATABASE IF NOT EXISTS `deerflow`
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

但这要求连接用户有 `CREATE` 权限。**生产环境推荐手动建好**，应用账号只授普通 DML/DDL 即可：

```sql
CREATE DATABASE IF NOT EXISTS deerflow
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

### 3.2 建用户与授权

```sql
-- 注意：OceanBase 多租户场景下用户名格式是 user@tenant#cluster
-- 在连接串里需要把 @ 转义为 %40 —— 见 §4.1
CREATE USER 'deerflow'@'%' IDENTIFIED BY '<strong-password>';

GRANT
  SELECT, INSERT, UPDATE, DELETE,   -- 业务读写
  CREATE, ALTER, INDEX, REFERENCES, -- 第一次启动建表 / 后续 Alembic 迁移
  CREATE TEMPORARY TABLES           -- JSON 大查询可能用到
ON deerflow.* TO 'deerflow'@'%';

FLUSH PRIVILEGES;
```

> 如果你**不希望应用账号有 DDL 权限**（推荐的安全姿势），那就先用 root 跑一次 `make dev` 让表全部建出来，然后回收 `CREATE/ALTER/INDEX`，之后只留 `SELECT/INSERT/UPDATE/DELETE`。

### 3.3 关键参数核查

在 OBServer 上确认：

```sql
-- 1. 字符集
SHOW VARIABLES LIKE 'character_set_database';      -- utf8mb4
SHOW VARIABLES LIKE 'collation_database';          -- utf8mb4_unicode_ci

-- 2. 包大小
SHOW VARIABLES LIKE 'max_allowed_packet';          -- ≥ 16777216 (16MB)

-- 3. SQL 模式（DeerFlow 会在会话里再强制一次，但租户级别也应包含 STRICT_TRANS_TABLES）
SHOW VARIABLES LIKE 'sql_mode';

-- 4. OBProxy 空闲超时（默认 7200s，DeerFlow 默认在 3600s 回收，安全）
SHOW PROXYCONFIG LIKE 'client_max_connections';
```

---

## 4. DeerFlow 配置

### 4.1 `.env` —— 写连接串

在 **项目根目录** 的 `.env` 中：

```bash
# 普通租户 / 单租户
OCEANBASE_URL="mysql://deerflow:secret@ob-proxy.example.com:2881/deerflow"

# 多租户：用户名形如 deerflow@biz_tenant#prod_cluster
# 必须把第一个 @ URL-encode 为 %40，否则 SQLAlchemy 会把 tenant 当作 host
OCEANBASE_URL="mysql://deerflow%40biz_tenant%23prod_cluster:secret@ob-proxy.example.com:2881/deerflow"

# 如果想让 make dev 自动加上 oceanbase 这个 uv extra（脚本会自动检测，多数时候不需要手动设）
UV_EXTRAS=oceanbase
```

> `mysql://` 会被代码自动改写成 `mysql+asyncmy://`，**不要**手写驱动后缀，否则会破坏 URL 解析。

### 4.2 `config.yaml` —— 切后端

在 **项目根目录** 复制一份 `config.example.yaml` 为 `config.yaml`（如还没有），定位到 `database:` 段，改成：

```yaml
database:
  backend: oceanbase
  oceanbase_url: $OCEANBASE_URL
  oceanbase_charset: utf8mb4         # 默认值，不写也行
  oceanbase_pool_recycle: 3600       # 默认值，OBProxy 默认 7200s 切空闲，半小时内回收最安全
  pool_size: 10                      # 默认 5；OceanBase 是分布式 DB，10 起步比较舒服
  echo_sql: false                    # 调试时可改 true
```

如果你同时显式配置了 `checkpointer:` 段（一般不需要），把它也设成同一个后端，**别让两层各跑各的**：

```yaml
# 可选：显式声明 checkpointer。不写则跟随 database.backend
checkpointer:
  type: oceanbase
  connection_string: $OCEANBASE_URL
```

### 4.3 全部 OceanBase 相关配置项一览

| 配置项 | 位置 | 默认值 | 说明 |
|---|---|---|---|
| `database.backend` | `config.yaml` | `memory` | 必须改为 `oceanbase` 才会启用本后端 |
| `database.oceanbase_url` | `config.yaml`，建议引用 `$OCEANBASE_URL` | `""` | 连接串，**必填** |
| `database.oceanbase_charset` | `config.yaml` | `utf8mb4` | 连接级 charset，保持默认 |
| `database.oceanbase_pool_recycle` | `config.yaml` | `3600` | 秒；必须小于 OBProxy `client_max_idle_time` |
| `database.pool_size` | `config.yaml` | `5` | 单进程连接池；建议 10 |
| `database.echo_sql` | `config.yaml` | `false` | 仅调试用 |
| `OCEANBASE_URL` | `.env` | — | 推荐使用，便于隔离机密 |
| `UV_EXTRAS` | `.env`（可选） | 自动检测 | `make dev` 会读 `config.yaml` 自动加 `oceanbase` extra |
| `OCEANBASE_TEST_URL` | shell（仅测试） | — | 跑集成测试 `pytest -m oceanbase` 时指定测试库 |

---

## 5. 表结构

> DeerFlow 启动时会通过 SQLAlchemy 的 `Base.metadata.create_all()` **自动建表**，**对接同学通常不需要手动建任何表**。
>
> 下表用于：① 你想审计应用要在你的库里建哪些东西；② 你想提前手动建好以回收 DDL 权限。

### 5.1 表清单（7 张）

| # | 表名 | 来源 | 用途 |
|---|---|---|---|
| 1 | `users` | App ORM | 用户账户、OAuth 绑定、JWT 版本 |
| 2 | `threads_meta` | App ORM | 会话元数据（标题、状态、所属用户） |
| 3 | `runs` | App ORM | 单次 Run 的状态机 + token 计费 |
| 4 | `run_events` | App ORM | 一次 Run 的完整事件流（消息、追踪、生命周期） |
| 5 | `feedback` | App ORM | 用户对 Run / 单条消息的 👍/👎 + 评论 |
| 6 | `checkpoints` | LangGraph | 智能体状态快照（一次 Run 多个） |
| 7 | `writes` | LangGraph | 多分支 / pending writes 暂存 |

### 5.2 DDL 总览（参考用，启动会自动跑）

下面是各表在 OceanBase 上的最终样貌（应用 ORM 的部分由 SQLAlchemy 编译生成；checkpointer 两张表的 DDL 是手写在 `_oceanbase_schema.sql` 里的）：

```sql
-- ============ App ORM 5 张 ============

CREATE TABLE users (
  id              VARCHAR(36)  NOT NULL,
  email           VARCHAR(320) NOT NULL,
  password_hash   VARCHAR(128),
  system_role     VARCHAR(16)  NOT NULL DEFAULT 'user',
  created_at      DATETIME     NOT NULL,
  oauth_provider  VARCHAR(32),
  oauth_id        VARCHAR(128),
  needs_setup     TINYINT(1)   NOT NULL DEFAULT 0,
  token_version   INT          NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  UNIQUE KEY ix_users_email (email),
  UNIQUE KEY idx_users_oauth_identity (oauth_provider, oauth_id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE threads_meta (
  thread_id     VARCHAR(64)  NOT NULL,
  assistant_id  VARCHAR(128),
  user_id       VARCHAR(64),
  display_name  VARCHAR(256),
  status        VARCHAR(20)  NOT NULL DEFAULT 'idle',
  metadata_json JSON,
  created_at    DATETIME     NOT NULL,
  updated_at    DATETIME     NOT NULL,
  PRIMARY KEY (thread_id),
  KEY ix_threads_meta_assistant_id (assistant_id),
  KEY ix_threads_meta_user_id (user_id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE runs (
  run_id              VARCHAR(64)  NOT NULL,
  thread_id           VARCHAR(64)  NOT NULL,
  assistant_id        VARCHAR(128),
  user_id             VARCHAR(64),
  status              VARCHAR(20)  NOT NULL DEFAULT 'pending',
  model_name          VARCHAR(128),
  multitask_strategy  VARCHAR(20)  NOT NULL DEFAULT 'reject',
  metadata_json       JSON,
  kwargs_json         JSON,
  error               TEXT,
  message_count       INT          NOT NULL DEFAULT 0,
  first_human_message TEXT,
  last_ai_message     TEXT,
  total_input_tokens  INT          NOT NULL DEFAULT 0,
  total_output_tokens INT          NOT NULL DEFAULT 0,
  total_tokens        INT          NOT NULL DEFAULT 0,
  llm_call_count      INT          NOT NULL DEFAULT 0,
  lead_agent_tokens   INT          NOT NULL DEFAULT 0,
  subagent_tokens     INT          NOT NULL DEFAULT 0,
  middleware_tokens   INT          NOT NULL DEFAULT 0,
  follow_up_to_run_id VARCHAR(64),
  created_at          DATETIME     NOT NULL,
  updated_at          DATETIME     NOT NULL,
  PRIMARY KEY (run_id),
  KEY ix_runs_thread_id (thread_id),
  KEY ix_runs_user_id (user_id),
  KEY ix_runs_thread_status (thread_id, status)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE run_events (
  id              BIGINT       NOT NULL AUTO_INCREMENT,
  thread_id       VARCHAR(64)  NOT NULL,
  run_id          VARCHAR(64)  NOT NULL,
  user_id         VARCHAR(64),
  event_type      VARCHAR(32)  NOT NULL,
  category        VARCHAR(16)  NOT NULL,
  content         TEXT,
  event_metadata  JSON,
  seq             INT          NOT NULL,
  created_at      DATETIME     NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_events_thread_seq (thread_id, seq),
  KEY ix_run_events_user_id (user_id),
  KEY ix_events_thread_cat_seq (thread_id, category, seq),
  KEY ix_events_run (thread_id, run_id, seq)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE feedback (
  feedback_id  VARCHAR(64)  NOT NULL,
  run_id       VARCHAR(64)  NOT NULL,
  thread_id    VARCHAR(64)  NOT NULL,
  user_id      VARCHAR(64),
  message_id   VARCHAR(64),
  rating       INT          NOT NULL,
  comment      TEXT,
  created_at   DATETIME     NOT NULL,
  PRIMARY KEY (feedback_id),
  UNIQUE KEY uq_feedback_thread_run_user (thread_id, run_id, user_id),
  KEY ix_feedback_run_id (run_id),
  KEY ix_feedback_thread_id (thread_id),
  KEY ix_feedback_user_id (user_id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============ LangGraph Checkpointer 2 张 ============

CREATE TABLE checkpoints (
  thread_id            VARCHAR(128) NOT NULL,
  checkpoint_ns        VARCHAR(128) NOT NULL DEFAULT '',
  checkpoint_id        VARCHAR(128) NOT NULL,
  parent_checkpoint_id VARCHAR(128),
  type                 VARCHAR(64),
  checkpoint           LONGBLOB,
  metadata             LONGBLOB,
  PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE writes (
  thread_id     VARCHAR(128) NOT NULL,
  checkpoint_ns VARCHAR(128) NOT NULL DEFAULT '',
  checkpoint_id VARCHAR(128) NOT NULL,
  task_id       VARCHAR(128) NOT NULL,
  idx           INT          NOT NULL,
  channel       VARCHAR(255) NOT NULL,
  type          VARCHAR(64),
  value         LONGBLOB,
  PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

> 上面 7 张表 DDL 的源头：5 张 ORM 表来自 `backend/packages/harness/deerflow/persistence/**/model.py`；2 张 checkpoint 表来自 `backend/packages/harness/deerflow/runtime/checkpointer/_oceanbase_schema.sql`。这两张表是在第一次 `AsyncOceanBaseSaver.setup()` 时通过执行该 SQL 文件建出来的（`IF NOT EXISTS`，幂等）。

### 5.3 容量与索引建议

- `run_events` 是写入热点：每条用户/AI/工具消息都会落一行。建议**单独**关注它的分区策略。如果 OceanBase 集群支持表分区，可以按 `thread_id` 哈希分区。
- `checkpoints.checkpoint` / `writes.value` 是 `LONGBLOB`，**单行可超过 1 MB**。请确认 `max_allowed_packet ≥ 16 MB`。
- `feedback` 行很稀疏，长尾增长，不需要分区。
- `users` 通常是小表（< 10k 行级），按 PK + email 走即可。

---

## 6. 启动

### 6.1 安装依赖

```bash
cd backend
uv sync --all-packages --extra oceanbase
```

> 后续 `make dev` 会通过 `scripts/detect_uv_extras.py` 读取 `config.yaml` 中 `database.backend == oceanbase`，自动追加 `--extra oceanbase`，所以这一步**只在首次需要**手动跑。

### 6.2 启动全部服务

```bash
# 项目根目录
make dev
```

启动流程会做四件事：

1. 解析 `config.yaml`，确认 `database.backend = oceanbase`。
2. `init_engine_from_config()` 建 ORM 连接池，必要时自动建库（错误码 1049 触发 `_auto_create_mysql_db`）。
3. SQLAlchemy `create_all()` 把 5 张 ORM 表建出来（已存在则跳过）。
4. FastAPI lifespan 拉起 `AsyncOceanBaseSaver`，执行 `_oceanbase_schema.sql`，建出 `checkpoints` / `writes` 两张表。

### 6.3 仅启动后端

```bash
cd backend && make gateway
```

---

## 7. 验证清单

按下面 5 步逐一确认，每一步都过了再去交付：

```bash
# 1. 健康检查
curl -fsS http://localhost:8001/health
# → 200 OK

# 2. 表全部建出来（7 张）
mysql -h <ob-proxy> -P 2881 -u deerflow -p deerflow -e "SHOW TABLES;"
# → users / threads_meta / runs / run_events / feedback / checkpoints / writes

# 3. 字符集真的是 utf8mb4
mysql -h <ob-proxy> -P 2881 -u deerflow -p deerflow -e \
  "SELECT TABLE_NAME, TABLE_COLLATION FROM information_schema.tables
   WHERE TABLE_SCHEMA='deerflow';"
# → 所有行 TABLE_COLLATION 都应该是 utf8mb4_unicode_ci

# 4. 跑一次完整对话（前端 http://localhost:2026 ，或直接调 /api/runs/wait）
#    然后看库里有数据：
mysql ... -e "SELECT COUNT(*) FROM runs;"
mysql ... -e "SELECT COUNT(*) FROM run_events;"
mysql ... -e "SELECT COUNT(*) FROM checkpoints;"
# → 三个计数都应 > 0

# 5. （可选）跑集成测试
docker run -d --name ob -p 2881:2881 oceanbase/oceanbase-ce:4.2.1
cd backend && OCEANBASE_TEST_URL='mysql://root@127.0.0.1:2881/deerflow_test' \
  PYTHONPATH=. uv run pytest -m oceanbase -v
```

---

## 8. 故障排查

| 现象 | 原因 | 处理 |
|---|---|---|
| `ImportError: asyncmy is not installed` | 没装 oceanbase extra | `uv sync --all-packages --extra oceanbase`；或在 `.env` 加 `UV_EXTRAS=oceanbase` |
| `Unknown database 'deerflow' (1049)` 且没有自动创建 | 应用账号没有 `CREATE` 权限 | 临时给 `CREATE`，或手动 `CREATE DATABASE deerflow CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;` |
| `Access denied for user 'deerflow@biz_tenant#prod'@'%'` | 多租户连接串没转义 | 把 `@` → `%40`，`#` → `%23` |
| 时间戳少 8 小时 / 多 8 小时 | 没跑到 `SET SESSION time_zone='+00:00'` | 确认 driver 是 `asyncmy`（不是 `aiomysql`）；查 `init_engine()` 是否走到 `event.listens_for(_engine.sync_engine, "connect")` 这段 |
| `Data too long for column 'X'` | `STRICT_TRANS_TABLES` 在拦截 | 这是**故意**的——业务数据被异常截断时不再静默成功。查上层应用为何要写超长数据 |
| 长时间空闲后下一条查询失败 | OBProxy 把空闲连接切了 | 降低 `database.oceanbase_pool_recycle`（默认 3600s），并确认 `pool_pre_ping=True` 已生效 |
| `Packet for query is too large` | `max_allowed_packet` 太小 | 集群侧调到 ≥ 16MB（checkpoint blob 可能很大） |
| 启动报 `OceanBase checkpointer is async-only` | 走了同步 graph 编译路径（如 LangGraph CLI） | 切到 async 入口（FastAPI Gateway / `asyncio.run`），生产 `make dev` 不会触发 |
| `LockWaitTimeoutExceeded` | `runs.status` 更新冲突 | 检查是否同一个 run_id 被并发 update；正常情况下 `RunManager` 会保证单写者 |

详细排错（含原因）见 [`OCEANBASE.md`](./OCEANBASE.md#troubleshooting)。

---

## 9. 运维事项

### 9.1 监控

最少观察这三类指标：

| 指标 | 来源 | 告警阈值（建议） |
|---|---|---|
| SQLAlchemy 池水位（active / overflow） | `_engine.pool.status()`，或 Prometheus exporter | active > `pool_size` 持续 5 分钟 |
| `run_events` 行增量 | OceanBase 本身的 row-stats | 增量突降 → 应用 Run 量异常 |
| 慢查询 | OceanBase `gv$sql_audit` | 单条 > 1s 应排查（一般是 JSON path 没走索引） |

### 9.2 备份

5 张 ORM 表 + 2 张 checkpoint 表都是单库内的普通 InnoDB-on-OceanBase 表。直接走集群标准物理备份/逻辑备份策略即可，**没有任何需要单独处理的特殊文件**（智能体本地缓存如 `.deer-flow/users/`、`skills/custom/` 在文件系统，不在 OceanBase 内）。

### 9.3 升级 DeerFlow 时

应用层 schema 用 Alembic 管理。OceanBase 端表结构变化时，请优先走：

```bash
cd backend && uv run alembic upgrade head
```

`alembic/env.py` 已经做了 dialect 区分：SQLite 才走 batch 模式，OceanBase 走原生 `ALTER TABLE`。

---

## 10. 附录

### 10.1 与其它 backend 的差异表

| 维度 | sqlite | postgres | oceanbase |
|---|---|---|---|
| 适用规模 | 单节点开发 | 中小生产 | 大规模生产 / 已有 OB 集群 |
| Driver | `aiosqlite`（内置） | `asyncpg` | `asyncmy`（`--extra oceanbase`） |
| Checkpointer | `AsyncSqliteSaver`（官方） | `AsyncPostgresSaver`（官方） | `AsyncOceanBaseSaver`（自研，async-only） |
| JSON 查询 | `json_extract` | `->>` + `::cast` | `JSON_EXTRACT` + `JSON_UNQUOTE` + `JSON_TYPE` |
| Alembic 模式 | batch | 原生 ALTER | 原生 ALTER |
| 时区 | TZ-aware DATETIME | TZ-aware TIMESTAMP | naive DATETIME + session `time_zone='+00:00'` |
| 自动建库 | N/A | 1049-like 触发 | 1049 触发 `_auto_create_mysql_db` |

### 10.2 相关代码位置

| 模块 | 路径 |
|---|---|
| 数据库配置 | `backend/packages/harness/deerflow/config/database_config.py` |
| Checkpointer 配置 | `backend/packages/harness/deerflow/config/checkpointer_config.py` |
| ORM 引擎初始化 | `backend/packages/harness/deerflow/persistence/engine.py` |
| OceanBase Saver | `backend/packages/harness/deerflow/runtime/checkpointer/oceanbase_saver.py` |
| Checkpoint DDL | `backend/packages/harness/deerflow/runtime/checkpointer/_oceanbase_schema.sql` |
| JSON 方言适配 | `backend/packages/harness/deerflow/persistence/json_compat.py` |
| Alembic 迁移环境 | `backend/packages/harness/deerflow/persistence/migrations/env.py` |
| `uv extras` 自动检测 | `scripts/detect_uv_extras.py` |
| 设计原理文档 | `backend/docs/OCEANBASE.md` |

### 10.3 一键回退到 sqlite

如果要临时把 OceanBase 摘掉跑本地：

```yaml
# config.yaml
database:
  backend: sqlite
  sqlite_dir: .deer-flow/data
```

`make dev` 即可。原 OceanBase 数据保留在 OceanBase 端，回切时数据仍在。

---

**问题反馈**：在仓库 issues 提 `oceanbase` 标签，附上 §7 验证清单的执行结果。
