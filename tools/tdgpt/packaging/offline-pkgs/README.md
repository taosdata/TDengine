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
