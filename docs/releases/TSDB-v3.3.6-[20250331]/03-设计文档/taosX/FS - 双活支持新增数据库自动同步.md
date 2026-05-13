# FS - 双活支持新增数据库自动同步

## 1. 背景

双活命令行执行时仅对当前已存在的数据库进行同步。启动同步后，对于新增的数据库，希望能够自动添加同步。

## 2. 变更历史

注：版本变更规则，初始版本为 0.1，中间若经过几次较大修改要增加版本号为 0.2， 0.3，最后定稿时的版本号为 1.0，以下为示例

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/02/25 | 0.1 | @霍琳贺 | FS |

## 3. 定义

- **双活**：本文档中指通过 taosx replica 命令创建的 Active-Active 模型的数据同步。

## 4. 行为说明

### 4.1 `taosx replica` 新增参数

新增参数：`--no-new-databases` ，表示命令不对新增数据库生效（默认对新增数据库生效）。

### 4.2 `taosx replica start`

`start` 命令在指定数据库时，行为与之前一致。
`start` 命令在不指定数据库时，在 taosx 服务端将监听数据源和目标的数据库变更，当源和目标均存在同名新增数据库时，自动创建新增数据库的双活同步任务。
新增参数 `--new-databases-checking-interval`：指定新增数据库检查间隔，单位：秒。默认 1800s 即 30 分钟。
示例：
- `taosx replica start -f localhost:6030 -t target:6030`：在对当前已存在的数据库创建双活同步任务后，创建对源 `localhost:6030` 和目标 `target:6030` 的双活监听任务。
- `taosx replica start -f ``localhost:6030`` -t target:6030 --no-new-databases`：行为与之前一致，仅对当前已存在的数据库创建双活同步任务。

### 4.3 `taosx replia stop`

`stop` 命令在指定数据库时，行为与之前一致。
`stop` 命令在不指定数据库时，将同时停止新增数据库监听任务，`--no-new-databases`可禁用此行为。

### 4.4 `taosx replia restart`

`restart` 命令行为与之前一致。`--no-new-databases` 对此不生效。

### 4.5 `taosx replia remove`

`remove` 命令在指定数据库时，行为与之前一致。
`remove` 命令在不指定数据库时，将同时移除新增数据库监听任务，`--no-new-databases`可禁用此行为。

### 4.6 `taosx replia update <REPLICA>`

新增 update 命令，通过参数`--new-databases-checking-inerval` 更新数据库检查间隔。

## 5. 性能

无。

## 6. 兼容性

无。

## 7. 运维

无。

## 8. 使用场景

- 此功能用于 `taosx replica start` 启动一次双活，之后创建的数据库自动创建双活同步任务。

## 9. 约束和限制

无。

## 10. 常见错误和排查

无。

## 11. 可观测性

无。

## 12. 安装和卸载

无。

## 13. 文档

无。

## 14. 参考文档

无。

## 15. 附录

`taosX` 新增一组 API：

| 方法 | 说明 | 输入参数 | 输出 |
| --- | --- | --- | --- |
| `POST /replicas` | 创建或更新双活监听任务 | ```json { "source": "", "sink": "", "options": {} } ``` |
| `POST /replica/{id}` | 更新双活任务 | ```json { "action": "start|stop|update", "options": { "new_databases_checking_interval": 300 } } ``` |
| `DELETE /replica/{id}` | 删除双活任务 | - |
