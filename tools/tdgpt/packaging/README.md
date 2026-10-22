# TDgpt 发版与生命周期文件

本目录保留 Full 与 Lite 安装包构建入口。Lite 动态资源的详细契约由下列专门文档维护。
安装包类型与 Docker 镜像类型是两个不同维度：云服务的 Full 镜像仍使用 Lite
代码安装包，venv 由 Full 基础镜像提供，模型权重由镜像构建时保留。

## 发版、安装、卸载与启动链路

所有路径相对于 `tools/tdgpt`；Docker 文件位于 `../../packaging/docker/`。

| 阶段 | Linux Full / Lite 安装包 | Windows Full / Lite 安装包 |
| --- | --- | --- |
| 代码打包 | `script/release.sh`；`-l` 生成 Lite，未指定时保留 Full 模型打包能力 | `packaging/win_release.py`；`--lite` 选择 `tdgpt-lite.iss`，否则选择 `tdgpt.iss` |
| 离线依赖 | `build_python_runtime_linux.sh`、`build_linux_offline_assets.sh`；`build_linux_offline_release.sh` 组合发版 | Full 使用 `build_offline_assets.py`，Lite 使用 `build_windows_lite_venv.py` |
| 安装 | 包根目录 `install.sh`（源码 `script/install.sh`） | 生成的 `install.bat` 调用 `script/install.py`，Inno Setup 驱动 |
| 卸载 | 包内 `bin/uninstall.sh`，安装后提供 `rmtaosanode` | 生成的 `uninstall.bat` 调用 `script/uninstall.py`，Inno Setup 驱动 |
| 主服务 | `cfg/taosanoded.service` 调用 `taosanode_service.py start --foreground` | `taosanode-service.xml` 与 WinSW；生成 start/stop/status 批处理入口 |
| 模型 | `start-model.sh` / `stop-model.sh` 调用 `taosanode_service.py` | 生成 `start-model.bat` / `stop-model.bat` / `status-model.bat` |
| 统一命令 | `taosanodectl` 启动器调用 `script/taosanodectl.py` | 生成 `taosanodectl.bat` 调用同一 Python 入口 |

`taosanode_service.py` 使用 `resource_manager.py` 和 `resource_manifest.py` 管理动态资源，
这四个 Python 文件均需进入安装包。主服务使用 `taosanodectl service start/stop/status`，
模型使用 `start-model <name|all>` / `stop-model <name|all>` 或 `taosanodectl model`。
原 `start-tdtsfm` / `stop-tdtsfm` 对应 `start-model tdtsfm` / `stop-model tdtsfm`；
原 `start-time-moe` / `stop-time-moe` 对应 `start-model timemoe` / `stop-model timemoe`。
旧的 `start-model-from-remote` 不再交付；动态资源准备使用 `taosanodectl resource`。
升级安装清理旧命令软链接，卸载同时清理当前与旧模型命令。

### Docker 交付

`DockerfileTDgpt` 的 `tdgpt` / `tdgpt_full` 使用 `tdgpt-env-*` 基础镜像已有的
venv，设置 `INSTALL_VENV=False`，安装 Lite 代码包后由 `entrypoint.sh` 启动模型和 API。
`tdgpt_full` 继续携带 TimeMoE、MOMENT、Moirai 模型，`tdgpt` 携带 TDtsfm。
`tdgpt_lite` 使用 `tdgpt-lite-env-*` 和 `entrypoint-lite.sh`，只启动 API。
因此不得因 Lite 安装包不内置模型而删除 `lib/tsfm/service/`、
模型依赖构建文件、Full 基础镜像或 Full 模型复制逻辑。

### 文件清理边界

Linux release 明确列出交付脚本，不再按 `st*.sh` 自动打包。
删除 `start-with-allmd.sh`、`start.sh`、`stop.sh`、四个单模型包装脚本和
`start_model_from_remote.sh`；不存在的 `ini_utils.sh` 复制逻辑已移除。
`build_offline_assets.py` 保留 Full 资源归档能力，删除没有调用入口的 Python runtime
下载函数及 uv 查找逻辑，工具缓存文档只保留实际使用的 WinSW。
历史 SQL `resource/` 目录仅在存在时复制，干净源码不要求该目录。
模型示例、依赖清单、Full / Lite Inno 模板及独立资源构建工具仍有用途，继续保留。

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
