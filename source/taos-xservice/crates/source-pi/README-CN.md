# source-pi

PI Data In 连接器的 Rust 端实现。负责启动 C# 连接器子进程 (`taosx-pi.exe` / `taosx-pi-backfill.exe`)，通过 IPC 接收 PI 数据并写入 TDengine。

> **平台限制**：PI 连接器仅在 Windows 上运行。非 Windows 平台编译通过但运行时返回 "PI connector support only windows platform" 错误。

## 目录结构

```
crates/source-pi/
├── src/
│   └── lib.rs                  # 主入口：连接器生命周期管理
├── docs/
│   ├── design/
│   │   └── pi-csv.md           # PI CSV 配置文件格式规范
│   └── specs/
│       ├── unable-change-name.md           # Bug 报告：CSV 自定义超级表名失败
│       └── unable-change-name-solution.md  # 上述 Bug 的解决方案
├── scripts/                    # PI Server 运维脚本（PowerShell）
│   ├── fix_pi_time.ps1         # 修改 Windows 系统时间以绕过 PI Server 许可证过期限制
│   └── cleanup_pi.ps1          # 清空 PI Server 的所有 PI Points 和 AF Database (Meters)
└── Cargo.toml
```

## 核心逻辑分布

本 crate 的代码量较少（`src/lib.rs` ~700 行），核心逻辑分散在以下位置：

| 模块                   | 路径                                                 | 职责                                                                                        |
| ---------------------- | ---------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| **连接器启动与 IPC**   | `crates/source-pi/src/lib.rs`                        | 启动子进程、IPC 数据接收、日志转发、生命周期管理                                            |
| **CSV 解析与数据模型** | `taosx-core/src/plugins/runners/pi/transform/mod.rs` | CSV 配置文件解析、`PIPointModelConfig` / `PIElementModelConfig`、SuperTable schema 定义     |
| **连接器配置生成**     | `taosx-core/src/plugins/runners/pi/config.rs`        | `PiConfig` 构造、DSN 解析、从 CSV 提取 PointList/TemplateList 传给 C# 连接器                |
| **数据写入映射**       | `taosx-core/src/plugins/sink/lush.rs`                | `LushModelConfig`：将 C# 连接器上报的 IPC 数据（`using`/`name`）映射到 TDengine 超级表/子表 |

## 架构概览

```
CSV config ──→ taosx (Rust)
                 │
                 ├── 解析 CSV → PIPointModelConfig / PIElementModelConfig
                 ├── 生成 PiConfig (TOML) → 传给 C# 连接器
                 ├── 构建 LushModelConfig（super_table_name_mapping）
                 │
                 └── 启动子进程 taosx-pi.exe
                       │
                       ├── 连接 PI Server（PI SDK / AF SDK）
                       ├── 订阅数据变更
                       └── 通过 IPC 上报数据 ──→ taosx 写入 TDengine
```

**关键约束**：C# 连接器只接收 PointList / ElementIDList / TemplateList，不感知 SuperTable 名、SubTable 模式或列名。所有名称映射在 taosx Rust 端完成。

## 数据模型

支持两种数据模型，由 DSN 参数 `model` 控制：

- **单列模型** (`single-column`, 默认)：按 PI 点位数据类型分组，每种类型一个超级表。IPC `using` 字段为类型标识（如 `ts_float32`），`name` 为 PI 点位名。
- **多列模型** (`multi-column`)：按 AF Template 分组，每个 Template 一个超级表。IPC `using` 字段为 Template 名，`name` 为 AF Element 路径。

## 公开 API

| 函数                              | 用途                                                |
| --------------------------------- | --------------------------------------------------- |
| `pi_to_taos()`                    | 主入口：启动 PI/PIBackfill 连接器，执行数据采集任务 |
| `query_data_source()`             | 查询 PI 数据源元数据（点位列表等）                  |
| `is_pi_valid()`                   | 验证 PI 连接是否可用                                |
| `is_pi_backfill_valid()`          | 验证 PI Backfill 连接是否可用                       |
| `parse_query_datasource_params()` | 从 DSN 解析查询参数（模式、过滤条件）               |

## 运维脚本

### `scripts/fix_pi_time.ps1`

用于 Azure piserver 虚拟机。当 PI Server 许可证过期时，通过修改 Windows 系统时间绕过限制：

1. 禁用 Hyper-V 时间同步和 Windows Time 服务（防止时间被自动校正）
2. 将系统时间设置到许可证有效期内（硬编码为 `2025-04-18`）
3. 重启所有 PI 服务和 IIS

```powershell
# 在 piserver 上以管理员权限运行
powershell -ExecutionPolicy Bypass -File fix_pi_time.ps1
```

### `scripts/cleanup_pi.ps1`

清空 piserver 上的所有测试数据，用于重置测试环境：

1. 通过 AF SDK 连接 PI Server，循环删除所有 PI Points
2. 删除名为 `Meters` 的 AF Database
3. 输出验证结果确认清理完成

```powershell
# 在 piserver 上运行（需要 PI AF SDK）
powershell -ExecutionPolicy Bypass -File cleanup_pi.ps1
```

> 两个脚本均需在 piserver（Azure VM）上以管理员权限运行。
