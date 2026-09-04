# TDgpt Lite 动态资源构建

本目录保存 TDgpt Lite 动态资源的可重复构建脚本，是模型资源归档布局和发布清单
的唯一来源。不要只在发布机器上保留一次性脚本。

## 资源协议

| 资源 | 归档格式 | 归档根目录 | 安装目标 |
| --- | --- | --- | --- |
| Linux 共享 TSFM runtime | `tar.gz` | `model_venvs/tsfm/` | `model_venvs/tsfm/` |
| Windows 共享 TSFM runtime | `zip` | `model_venvs/tsfm/` | `model_venvs/tsfm/` |
| 官方 TSFM 模型 | `zip` | `model/<name>/` | `model/tsfm/<name>/` |

共享 runtime 包必须包含 venv 根目录、`requirements.lock` 和 `manifest.json`，不
得包含 `.tdgpt-install-complete.json`；完成标志由导入器在校验成功后写入本机。
官方模型 ZIP 保持原始模型文件，不包含 TDgpt manifest 或 ready 标志。

## 拆分全量模型包

全量模型包仅作为输入，发布前必须拆分为单模型 ZIP：

```bash
python3 split_model_resource.py \
  --source /pkgs/TDengine/deps/tdengine-tdgpt-model-resource-1.0.0.zip \
  --output-dir /pkgs/TDengine/tsfm/1.0.0 \
  --version 1.0.0
```

脚本从 `tsfm_model_catalog.json` 读取模型身份和命名规则，生成 Hugging Face 风格
的小写文件名，例如 `tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip`。同时生成
`tdengine-tdgpt-tsfm-models-manifest.json`，记录模型 ID、版本、备注、归档大小和
SHA256。保持 ZIP 内容不变时，重命名不会改变 digest。

## 构建共享 TSFM runtime

先解压对应平台的 Python runtime，再在原生平台执行：

```bash
python3 build_tsfm_runtime.py \
  --runtime-dir /data/chr/python-runtime \
  --platform linux \
  --arch x64 \
  --version 1.0.0 \
  --work-dir /data/chr/tsfm-runtime-build \
  --output-dir /pkgs/TDengine/tsfm/1.0.0
```

ARM64 节点使用 `--arch arm64`，Windows x64 使用 `--platform windows --arch x64`。
脚本默认读取 `../requirements_tsfm.txt`，可通过 `--requirements` 指定依赖文件。
构建工作目录必须为空，验证归档后即可删除。

输出文件为：

```text
tdengine-tdgpt-tsfm-runtime-1.0.0-linux-x64.tar.gz
tdengine-tdgpt-tsfm-runtime-1.0.0-linux-arm64.tar.gz
tdengine-tdgpt-tsfm-runtime-1.0.0-windows-x64.zip
```

ARM64 构建会修复 `jaxlib/libjax_common.so` 对 `librt.so.1` 的错误依赖，要求
`patchelf >= 0.14.0`。`patchelf 0.9` 不满足要求；发布前必须在目标 ARM64 系统
验证 JAX 导入和 Moirai 启动。
