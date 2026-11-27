# TDgpt TSFM 资源发布

`upload_tsfm_release.py` 根据 TSFM release manifest 批量调用 `cmd.tdengine.net`
的 `POST /upload-any` 接口。

在上传服务已经具备对应源文件后执行：

```bash
python3 upload_tsfm_release.py \
  --release-manifest ../tdengine-tdgpt-tsfm-release-1.0.0.json
```

默认目标为：

```text
container: tdengine-tdgpt-enterprise
filename:  tsfm/1.0.0/<resource-file>
```

先用 `--dry-run` 核对实际请求列表。上传接口是异步任务：脚本会先一次性提交全部
runtime、模型和 release manifest，再逐个轮询任务至 `success`。对网络和服务端 `5xx`
错误，脚本默认重试三次；`4xx` 直接失败，便于尽快发现上传服务缺少源文件或参数错误。

## 正式发布职责

本目录上传脚本仅保留历史 schema 1 行为。schema 2 资源 staging、共享模型校验、upload-any、轮询及发布门禁统一由 [platform-group !526](https://git.tdengine.net/rd-test/platform-group/-/merge_requests/526) 的 `versionRelease/publish_pkg/publish_tdgpt_tsfm.py` 维护，操作方法见该仓库的 `versionRelease/docs/tdgpt-tsfm-release.md`。

latest 指针由发布端在资源及固定清单验收后显式发布，格式仍为 `{"schema_version":1,"release":"1.0.1"}`，与资源清单的 schema 2 分开。latest 发布能力尚须在 platform-group 接入并验收；本目录不提供 `--publish-latest`，客户端不会自行创建或更新远端 latest。入口尚未发布或不可用时，检查更新应返回查询失败，不得误报“已是最新”。
