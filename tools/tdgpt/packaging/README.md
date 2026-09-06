# TDgpt Lite 打包与动态资源文档

本目录只维护 TDgpt Lite 当前版本的构建、动态资源设计、发布和验收说明。
历史 Full 安装包、独立模型 venv 和全量模型包不属于本目录的维护范围。

## 文档关系

| 文档 | 用途 | 主要读者 |
| --- | --- | --- |
| [TDGPT-LITE-BUILD-CN.md](TDGPT-LITE-BUILD-CN.md) | 三平台安装包、runtime 和主 venv 构建 | 构建与发布人员 |
| [TDGPT-LITE-DESIGN-CN.md](TDGPT-LITE-DESIGN-CN.md) | 安装契约、资源目录、下载、导入、升级和回滚 | 开发与架构人员 |
| [TDGPT-LITE-VALIDATION-CN.md](TDGPT-LITE-VALIDATION-CN.md) | 三平台动态资源和模型启动验收 | 测试与发布人员 |
| [offline-pkgs/README.md](offline-pkgs/README.md) | TSFM 资源上传流程 | 资源发布人员 |

修改代码前查看设计文档；构建制品时查看构建文档；发布前按验收文档执行完整验证。

## 当前交付范围

- Linux x64 Lite 安装包和 Docker 镜像
- Linux ARM64 Lite 安装包和 Docker 镜像
- Windows x64 Lite 安装包
- 三平台 TDgpt 主 venv
- Linux Python runtime
- 三平台共享 TSFM runtime、官方模型包和 release manifest
- `taosanodectl resource` 动态资源生命周期

## 主要入口

| 文件 | 作用 |
| --- | --- |
| `build_python_runtime_linux.sh` | 构建 Linux Python runtime |
| `build_linux_offline_assets.sh` | 构建 Linux Lite 主 venv |
| `build_linux_offline_release.sh` | 组合构建 Linux Lite 发布物和 SHA256 清单 |
| `build_windows_lite_venv.py` | 构建 Windows x64 Lite 主 venv ZIP |
| `win_release.py` | 构建 Windows Lite 安装包 |
| `../offline-pkgs/build_tsfm_runtime.py` | 构建共享 TSFM runtime |
| `../offline-pkgs/split_model_resource.py` | 拆分全量模型 ZIP |
| `offline-pkgs/upload_tsfm_release.py` | 批量上传 TSFM 动态资源 |

## 发布原则

1. 三个平台使用同一资源协议和 release manifest。
2. 安装包、主 venv、Python runtime、TSFM runtime 和模型包独立发布。
3. 外部资源导入前必须校验大小和 SHA256。
4. 各架构必须在对应原生节点构建并验收。
5. 发布前必须验证 runtime 复用、重复导入、模型启动、端口 readiness 和 URL 兜底。

具体命令和参数以构建、设计及验收文档为准，避免在入口文档中重复维护。
