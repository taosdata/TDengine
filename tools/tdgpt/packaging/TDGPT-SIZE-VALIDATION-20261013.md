# TDgpt 缓存清理验证（2026-10-13）

## 范围

!2431 保留用户模型编译、离线安装修复能力。保留 gcc/libc-dev、Cython、pip/setuptools/wheel、静态库及 Torch 头文件。
新增离线清理仅删除 __pycache__、类型提示 .pyi 和 share/man；保留 test/tests、文档、许可证和语言包。Docker 既有 tests 清理不扩展。
依赖验证使用 Python -B，验证后再次清理缓存，避免检查生成的缓存进入归档。Docker 最后一次清理覆盖主 Python 和 taosanode venv。

## Lite Docker

本地已发布 arm64 镜像 tdengine/tdgpt-lite:latest，ID d3f51a751bd6，版本 3.4.2.13。
以全新只读容器、禁用网络、跳过服务入口、Python -B 检查：677 个缓存文件，10,879,067 字节；缓存单独 gzip 归档 4,484,029 字节。这不是发布镜像压缩层的实际减少量。
构建历史已有主 Python 的缓存清理；后续安装和 venv 会留下缓存。

Linux x64 2.104 上，基于先前通过 E2E 的 tdgpt-e2e-base:a66002b2c820，在一次性派生镜像执行本次 Dockerfile 的验证和最终清理命令：主 Python、venv 缓存目录为 0；保留 3 个静态库。工具链导入和 gcc 检查通过。
派生镜像只验证清理行为，不用于证明镜像体积收益；生产构建在原安装层中执行清理。

## 发布包副本测量

从 release.tdengine.net 下载已发布 Linux x64 包。在 2.104 的独立目录中，用相同 tarfile/gzip level=6 参数重新归档，唯一变量为是否排除 __pycache__。未修改发布资源。

| 包 | 原发布文件字节 | 对照重打包 MiB | 去缓存后 MiB | 减少 MiB |
|---|---:|---:|---:|---:|
| Lite venv 1.0.1 | 234088682 | 222.93 | 174.00 | 48.93 |
| TSFM runtime 1.0.0 | 578639183 | 556.41 | 459.00 | 97.41 |

Lite 有 7,419 个缓存文件，143,753,904 字节；TSFM 有 14,799 个，288,807,617 字节。两包中缓存 .pyc 均找到对应源码。
TSFM 发布包 SHA256：f08c32f166680b944b7aa5d30af9511a09e19352110d053b058dfb31e31e67d1。
Lite 发布包 SHA256：e2765e3a6723b0f593bd66d80b4cfd611292ee1c504910f7b184e736605a9ded。

原发布包的压缩参数不同，因此收益比较使用同参数重打包对照，不直接减原文件大小。

## 运行验证及限制

清理后的归档解压后以只读挂载方式在 Python 3.12 容器中加载 site-packages，禁用网络并使用 -B：

- Lite：NumPy、Pandas、SciPy 计算，Statsmodels 指数平滑预测、sklearn IsolationForest，以及 Flask、Cython、setuptools、wheel 导入通过。
- TSFM：MoiraiMoEForecast、TimesFM、BaseChronosPipeline、MOMENTPipeline、transformers 导入，Torch/JAX 实际计算通过。
- Python/Shell 清理回归真实生成缓存，清理后仍能从源码导入；静态库编译、链接并运行成功，文档许可证和测试目录保留。

本次离线实测只删除缓存，未测 .pyi/man 的独立体积收益；源码回归覆盖其清理行为。未重建完整离线发版包，未跑服务生命周期、模型权重推理、Windows 或 ARM 离线运行测试。缓存可能在部署后重新生成，首次启动有源码编译开销。

实机副本、报告和日志保留于 root@192.168.2.104:/data/chr/tdgpt-pycache-audit-20261013/。
