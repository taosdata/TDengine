# Lite 动态资源删除接口

## 删除范围

`taosanodectl resource delete moment` 删除 `model/tsfm/moment`、该模型安装状态及下载任务文件，保留共享 TSFM venv 和其他模型。

`taosanodectl resource delete tsfm_runtime` 删除 `model_venvs/tsfm` 和对应下载任务文件，保留所有模型权重、TDGPT 主 venv 和 Python runtime。

删除直接移除文件，没有回收站；权限错误或中断可能留下部分文件。删除前需要停止目标模型；删除共享 runtime 前需要停止所有模型。不会自动停止服务。

## HTTP 调用

向 `POST /api/v1/foundation-models/delete` 发送 JSON：

```json
{"model_name": "moment"}
```

删除共享 venv 使用 `POST /api/v1/foundation-models/runtime/delete`，请求体为 `{}`。
HTTP 202 返回 `task_id`，仅表示任务受理。轮询
`GET /api/v1/foundation-models/<task_id>/status`，判断 `data.status`：

| 值 | 含义 |
| --- | --- |
| `queued` | 等待执行 |
| `running` | 正在执行 |
| `completed` | 删除成功或目标已经不存在 |
| `failed` | 删除失败，查看 `data.error` |

外层 `status: success` 只表示查询成功。不能根据 `progress: 100` 判断删除成功。后台命令超时为 300 秒。

缺少或类型无效的模型名返回 400，模型不在数据库目录中返回 404。API 会先根据模型状态拒绝运行中的模型或使用中的共享 runtime；后台持锁的 CLI 还会再次检查实际进程，避免状态滞后造成误删。CLI 对不存在的目标返回 `not_found`、退出码 0；API 将其作为成功任务处理。

## 并发和状态

CLI 的模型启动、停止、资源下载导入和删除共用本机生命周期文件锁。操作在执行期间串行化，冲突命令失败并提示 `another TDgpt resource operation is running`；API 中体现为异步任务失败。重复请求不会订阅同一个任务，调用方应等待已有任务完成再重试。

操作系统在进程退出时释放锁。不要删除锁文件来解除正在执行的操作；旧进程必须结束后再切换到新版脚本。

API 删除成功后更新模型数据库的下载、运行标志。CLI 删除不直接写 API 数据库：通过资源清单刷新，或查询单模型信息时同步实际安装状态。历史 API 任务保留用于查询，但不代表当前模型已安装。

删除共享 runtime 不修改模型权重的已安装状态，界面需要区分模型与 runtime。重新执行 `resource apply moment` 可恢复所需 runtime 和模型。

## 页面约定

- 模型行的删除不连带删除共享 runtime。
- 未下载、下载中、导入中及删除中禁用删除；运行中的模型先停止。
- 确认后显示“删除中”，任务完成后刷新资源清单；失败显示原因。
- 删除成功保留模型行，显示“未下载”，清空运行和加载时间展示。
- 共享 runtime 删除使用独立高级入口，提示会影响所有依赖模型。
