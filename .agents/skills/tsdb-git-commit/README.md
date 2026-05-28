# taosx-git-commit

taosx 项目的 Git Commit Message 规范 Skill，基于 Conventional Commits 标准，支持飞书任务链接关联自动抽取为规范的[ID](URL)形式，scope 限制在 tasox 要求范围。

## 触发场景

- 带 taosx 关键字的生成 git commit 要求
- 带飞书链接的 taosx git commit 要求

## 最终产生格式

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Type 类型

| 类型               | 说明                             |
| ------------------ | -------------------------------- |
| `feat`             | 新功能                           |
| `enh`              | 已有功能优化                     |
| `fix`              | 修复 bug                         |
| `docs`             | 文档变更                         |
| `ci`               | CI 相关变更                      |
| `refactor` / `ref` | 代码重构                         |
| `perf`             | 性能优化                         |
| `test`             | 测试相关                         |
| `release`          | 发布相关                         |
| `chore`            | 其他杂项变更（构建、辅助工具等） |

### Scope 范围（可选）

**核心模块：** `serve` | `api` | `grpc` | `agent` | `explorer` | `core` | `utils` | `ipc` | `packaging` | `xnoded`

**数据源：** `kafka` | `mongodb` | `mysql` | `postgresql` | `oracle` | `influxdb` | `opentsdb` | `pi` | `legacy` | `mssql` | `pulsar` | `opcua` | `opcda` | `mqtt` | `orc` | `pspace` | `sparkplugb` | `csv` | `historian`

### Description 规则

- 必须以动词开头，使用现在时（如 `add` / `fix` / `update` / `remove`，不用 `added` / `fixed`）
- 首字母小写（专有名词除外）
- 末尾不加句号
- 标题行控制在 72 个字符以内

### Footer 飞书链接

- 提供了任务链接时，必须在 footer 用 `Closes` 关联，ID 取链接末尾数字：
  ```
  Closes [6857598725](https://project.feishu.cn/taosdata_td/sub_task1/detail/6857598725)
  ```
- 多个链接使用列表格式：
  ```
  Closes
  - [6857598725](https://project.feishu.cn/taosdata_td/sub_task1/detail/6857598725)
  - [9999999999](https://project.feishu.cn/taosdata_td/sub_task1/detail/9999999999)
  ```
- 未提供任何链接时，省略 footer，不得自行补充或虚构链接。

### 示例

```
feat(serve): add new gRPC service

Including new endpoints for user management and data retrieval.

Closes [6857598725](https://project.feishu.cn/taosdata_td/sub_task1/detail/6857598725)
```

```
fix(grpc): fix memory leak in gRPC service

Closes [6857598725](https://project.feishu.cn/taosdata_td/sub_task1/detail/6857598725)
```

```
docs: update API reference
```

