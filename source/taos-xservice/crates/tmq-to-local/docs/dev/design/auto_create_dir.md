# 备份目录自动创建（auto_create_dir）

## 背景

`taosx` 的备份任务通过 DSN（如 `local:/data/taosx/backup/<bucket>/<task>`）指定备份文件落地目录。
历史实现要求该目录必须**预先存在**，否则在 `parse_backup_dir` → `parse_dir_in_dsn`
的 `PathBuf::canonicalize()` 阶段直接报错：

```
invalid path: /data/taosx/backup/td777bca8bc67d/saige_location/<uuid>
```

云服务（飞书 defect 6979290978）在封装 `taosx` 创建备份任务时，仅生成一个目录路径，
并未在文件系统上实际创建该目录，因此命中上述报错。

为兼容此类调用方，`taosx` 后端新增一个**显式开关**，允许在解析备份目录时自动 `mkdir -p`
用户指定的目录。开关默认 **关闭**，对历史用户无任何行为变更。

## 设计目标

- 提供一个明确、可观测的开关，避免隐式 `mkdir` 在拼写错误等场景掩盖问题。
- 仅作用于 `parse_backup_dir`，不放宽其它路径解析（如 `move.to` / `move.from` /
  `error_restore_dir`）的严格校验。
- 配置入口与现有 `[log]` / `data_dir` 等字段保持同构：TOML / 环境变量 / CLI 三选一，
  优先级由 [`twelf`](https://docs.rs/twelf) 的 `Layer` 顺序决定（CLI > Env > TOML）。
- 默认 `false`，老用户/老配置零影响。

## 配置项

| 维度     | 写法                                                                      |
| -------- | ------------------------------------------------------------------------- |
| TOML     | `[backup]` 段下 `autoCreateDir = true`                                    |
| 环境变量 | `TAOSX_BACKUP_AUTO_CREATE_DIR=true`（接受 `1/true/yes/on`，大小写不敏感） |
| 命令行   | `--backup.autoCreateDir`（不带值时等价于 `=true`）                        |
| 默认     | `false`                                                                   |

`examples/taosx.toml` 中已附带注释示例。

## 数据流

```
CLI / Env / TOML
        │
        ▼
struct BackupOpts { auto_create_dir: Option<bool> }   // src/main.rs
        │  Args::init_with_arg_matches → set_env_backup_auto_create_dir(bool)
        ▼
环境变量 TAOSX_BACKUP_AUTO_CREATE_DIR                 // 进程内全局
        │  get_backup_auto_create_dir()
        ▼
parse_backup_dir(dsn, task_id)                        // taosx-core/src/utils/mod.rs
```

与 `data_dir` / `logs_home` / `plugins_home` / `log_keep_days` 等已有配置一致，**配置真实
来源是进程环境变量**，setter 只在初始化阶段写一次，runtime 各模块通过 getter 读取，
从而避免线程间共享可变状态。

## 关键代码

### 1. helpers — `taosx-core/src/plugins/runners/mod.rs`

```rust
pub const ENV_TAOSX_BACKUP_AUTO_CREATE_DIR: &str = "TAOSX_BACKUP_AUTO_CREATE_DIR";

pub fn set_env_backup_auto_create_dir(enabled: bool) { /* set_var */ }

#[inline]
pub fn get_backup_auto_create_dir() -> bool { /* parse env */ }
```

通过 `taosx-core/src/plugins/mod.rs` 的 `pub use runners::{...}` 重新导出，外部以
`taosx_core::get_backup_auto_create_dir()` 的形式调用。

### 2. CLI / TOML 接入 — `src/main.rs`

```rust
#[derive(Parser, Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct BackupOpts {
    #[clap(
        id = "backup.autoCreateDir",
        long = "backup.autoCreateDir",
        env = "TAOSX_BACKUP_AUTO_CREATE_DIR",
        global = true,
        num_args = 0..=1,
        default_missing_value = "true",
    )]
    #[serde(rename = "autoCreateDir", alias = "auto_create_dir")]
    auto_create_dir: Option<bool>,
}
```

`Global` 中 `#[clap(flatten)] backup: Option<BackupOpts>`，与 `LogOpts` 同构。
`Args::init_with_arg_matches` 内合并 TOML / CLI 后调用 `set_env_backup_auto_create_dir(...)`。

### 3. 行为变更

仅当**满足全部以下条件**时执行 `create_dir_all`：

1. `Dsn.path` 字段非空（即 DSN 路径段非空，例如 `local:/data/taosx/backup/foo` 中的
   `/data/taosx/backup/foo`；区别于 DSN 查询参数 `?key=value`，也区别于走默认路径
   `$TAOSX_DATA_DIR/backup` 的"DSN 仅形如 `local:` 没有路径"分支）
2. 该目录尚不存在
3. `get_backup_auto_create_dir()` 返回 `true`

满足时：

- 调用 `std::fs::create_dir_all(dir)`；
- 失败时报 `backup dir not exists and failed to create: <path>`，与历史的
  `backup dir not exists: <path>` 区分；
- 创建成功后 `tracing::info!("auto-created user-specified backup dir: ...")`，便于运维审计。

后续仍走原有 `parse_dir_in_dsn → canonicalize` 流程，保证返回的是绝对、规范化的路径。

## 行为对照表

| 用户指定目录          | 开关            | 结果                                                           |
| --------------------- | --------------- | -------------------------------------------------------------- |
| 已存在                | 任意            | 返回 canonicalized 路径（拼接 `task_id`）                      |
| 不存在                | `false`（默认） | 返回 `Err("invalid path: ...")`，行为同历史版本                |
| 不存在                | `true`          | `mkdir -p`，再返回 canonicalized 路径，并打 info 日志          |
| 不存在 + 创建失败     | `true`          | 返回 `Err("backup dir not exists and failed to create: ...")`  |
| 未指定（DSN 无 path） | 任意            | 走默认 `$TAOSX_DATA_DIR/backup`，由 taosx 自行创建（历史行为） |

## 安全性与运维考量

- **默认关闭**：避免拼写错误把数据落到非预期路径。
- **作用域受限**：仅 `parse_backup_dir`，不影响 `move.to`/`move.from`/`error_restore_dir`
  等仍要求路径预先存在的场景。
- **审计可追踪**：每次自动创建均产生 `tracing::info!` 日志（含完整路径）。
- **失败诊断清晰**：错误信息明确区分"未启用开关"与"启用但创建失败"两类原因，便于排障。
- **权限延后暴露**：开启后，磁盘只读 / 配额耗尽 / SELinux 拒绝等问题会推迟到首次备份时
  才报出，部署方需自行确保目标目录所在分区可写。

## 测试

`taosx-core/src/utils/mod.rs` 内的 `test_parse_backup_dir_auto_create` 覆盖：

1. 用户指定不存在目录 + 开关关闭 → 报错且目录未被创建。
2. 同一目录 + 开关开启 → 自动创建并返回 canonicalized 路径。

测试使用 `tempfile::TempDir` 以避免污染真实目录；并用 `defer` 在测试结束时恢复
`TAOSX_BACKUP_AUTO_CREATE_DIR` 环境变量，避免影响其它并发测试。

## 相关文件

| 文件                                            | 改动点                                         |
| ----------------------------------------------- | ---------------------------------------------- |
| `taosx-core/src/plugins/runners/mod.rs`         | 新增 `ENV_*` / `set_env_*` / `get_*` 三件套    |
| `taosx-core/src/plugins/mod.rs`                 | 重新导出新符号                                 |
| `taosx-core/src/utils/mod.rs::parse_backup_dir` | 实际行为变更 + 单元测试                        |
| `src/main.rs`                                   | `BackupOpts` 结构、`Global` flatten、env 注入  |
| `examples/taosx.toml`                           | `[backup]` 段示例                              |

> CHANGELOG 由 `clog.sh` / `cliff.toml` 在发版时基于 Conventional Commits 自动生成，
> 无需手工维护。本特性在提交时使用 `feat(serve): ...` scope 即可被收录。

## 关联

- 飞书 defect: <https://project.feishu.cn/taosdata_td/defect/detail/6979290978>
