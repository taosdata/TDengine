# TDgpt Lite 动态资源手工验收

本文用于验收 Linux x64、Linux arm64 和 Windows x64 的 Lite 动态资源导入。
测试目标是确认：共享 TSFM runtime 只下载/导入一次，后续模型复用该
runtime；导入失败不会破坏已安装资源；模型服务能够正常启动。

## 1. 资源命名

建议共享模型运行环境统一称为 **TSFM runtime**，不要命名为通用 `venv`：

```text
tdengine-tdgpt-tsfm-runtime-1.0.0-linux-x64.tar.gz
tdengine-tdgpt-tsfm-runtime-1.0.0-linux-arm64.tar.gz
tdengine-tdgpt-tsfm-runtime-1.0.0-windows-x64.zip
```

理由：`venv` 描述实现形式，`runtime` 表达它是多个 TSFM 模型共享的运行时；
磁盘内仍使用 `model_venvs/tsfm/`，因为该目录是实际 Python venv 根目录。

## 2. 验收范围

每次发布的 Lite 安装包必须在三个目标平台各完成一次全新安装验收。全新安装仅验证安装器、主 venv、控制命令和空动态资源目录；不应下载模型或 TSFM runtime。

资源管理器、发布清单或资源包变更后，无需再次重装 Lite。保留已安装的 Lite，清空本次验收的动态资源目录后执行第 4 节在线验收即可。已有动态资源不能直接删除；应使用独立测试安装目录或先备份以下目录：

```text
model/tsfm/
model_venvs/tsfm/
data/resource/
```

每个平台至少验收以下场景：

| 场景 | 预期结果 |
| --- | --- |
| 全新 Lite 安装 | 不下载模型和 TSFM runtime；控制命令可用 |
| 首次在线 `apply moirai` | 下载一次 TSFM runtime 和 Moirai 模型 |
| 第二次在线 `apply moment` | runtime 为 `unchanged`，仅下载 Moment 模型 |
| 重复在线 `apply moirai` | runtime、模型均为 `unchanged`，不下载、不解压 |
| `verify all` 与模型启动 | 校验通过；Moirai 监听 `6064`，Moment 监听 `6066` |
| 损坏包或错误 SHA | 命令非零退出，已有资源仍可使用 |
| 配置内网、内网不可用 | 自动尝试外网备用地址；日志记录实际来源 |
| 内网与外网均不可用 | 命令非零退出，并输出两个来源的失败原因 |

## 3. 前置条件

- 已安装对应平台的 TDgpt Lite；
- 已准备 Python 3.12.12 runtime；
- 已发布 `1.0.0` 资源清单和至少两个模型包，例如 `moirai`、`moment`；
- 测试前停止正在运行的目标模型。

Linux 使用 `/usr/bin/taosanodectl`，Windows 使用
`<安装目录>\bin\taosanodectl.bat`。以下示例使用内部发布站点；外网验收时将
`https://release.tdengine.net` 替换为 `https://downloads.taosdata.com`。

资源导入器优先使用显式传入的 `--python-runtime-dir`，否则依次从 Lite 主
venv 的 `pyvenv.cfg`、当前 Python 的基础 runtime 和平台默认路径中自动发现
Python runtime。源码模式或非标准部署仍可显式传入该参数覆盖自动发现结果。

## 4. 在线导入与复用

首次导入 Moirai。首次执行下载当前平台的 TSFM runtime 和 Moirai 模型：

```bash
time /usr/bin/taosanodectl resource apply moirai \
  --release 1.0.0 \
  --resource-base-url https://release.tdengine.net
```

Windows PowerShell：

```powershell
Measure-Command {
  & C:\TDengine\taosanode\bin\taosanodectl.bat resource apply moirai `
    --release 1.0.0 `
    --resource-base-url https://release.tdengine.net
}
```

确认：

```text
model_venvs/tsfm/manifest.json
model_venvs/tsfm/requirements.lock
model_venvs/tsfm/.tdgpt-install-complete.json
model/tsfm/moirai/
```

并执行 `taosanodectl resource check moirai`、`taosanodectl resource list`
和 `taosanodectl resource verify all`。确认 runtime 的平台、架构和 Python
版本正确；Moirai 输出包含 `model_id`、`model_version`、`digest`、`size`、
`deployed_at`、`install_status`、`service_status`、`port` 和 `note`。

导入第二个模型 Moment。预期 runtime 返回 `unchanged`，仅下载并导入 Moment 模型：

```bash
time /usr/bin/taosanodectl resource apply moment \
  --release 1.0.0 \
  --resource-base-url https://release.tdengine.net
```

重复执行 Moirai 导入，预期 runtime 和模型均返回 `unchanged`：

```bash
time /usr/bin/taosanodectl resource apply moirai \
  --release 1.0.0 \
  --resource-base-url https://release.tdengine.net
```

## 5. 模型启动与端口

```bash
/usr/bin/taosanodectl resource verify all
/usr/bin/taosanodectl model start moirai
/usr/bin/taosanodectl model start moment
/usr/bin/taosanodectl model status all
```

确认 Moirai readiness 和端口 `6064`，Moment readiness 和端口 `6066`。Linux
可使用 `ss -ltn` 检查端口；Windows 使用
`Get-NetTCPConnection -LocalPort 6064,6066`。

## 6. 离线导入、幂等与失败测试

离线导入适用于隔离网络环境。将 runtime 包与模型 ZIP 作为多个 `--package` 参数传入；导入器仍会校验归档 SHA-256、使用同盘 staging，并原子切换目录。

```bash
/usr/bin/taosanodectl resource import \
  --package <tsfm-runtime.tar.gz> \
  --package <model.zip>
```

重复导入同一个完整模型包，预期返回 `status=unchanged`，不产生新的 staging
或 backup。使用错误 SHA-256 或截断 ZIP，预期命令非零退出，目标目录保持不变，
已有模型仍可启动。

```bash
/usr/bin/taosanodectl resource import --package <已导入模型包>
```

### 6.1 主机安装离线导入与页面启停

将离线包放入 TDgpt 安装目录之外的临时目录。离线介质文件清单见设计文档第
7.6 节；每台机器只选择一个与自身平台和架构一致的 TSFM runtime，可在同一命令
中同时导入多个模型 ZIP。Linux 使用 `/usr/bin/taosanodectl`；Windows 使用实际
安装目录下的 `taosanodectl.bat`。以下 Linux x64 命令同时导入 Moment 和 Moirai：

```bash
sudo /usr/bin/taosanodectl resource import \
  --package /opt/tdgpt-offline/tdengine-tdgpt-tsfm-runtime-1.0.0-linux-x64.tar.gz \
  --package /opt/tdgpt-offline/tdengine-tdgpt-model-moment-1-base.zip \
  --package /opt/tdgpt-offline/tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip
sudo /usr/bin/taosanodectl resource verify all
sudo /usr/bin/taosanodectl resource list --format json
```

```powershell
& C:\TDengine\taosanode\bin\taosanodectl.bat resource import `
  --package C:\tdgpt-offline\tdengine-tdgpt-tsfm-runtime-1.0.0-windows-x64.zip `
  --package C:\tdgpt-offline\tdengine-tdgpt-model-moment-1-base.zip `
  --package C:\tdgpt-offline\tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip
& C:\TDengine\taosanode\bin\taosanodectl.bat resource verify all
```

页面或 HTTP 接口先查询 `GET /api/v1/foundation-models/moment`，确认
`install_status=installed` 且 `service_status=stopped`，再调用 start、stop
接口，并通过返回的 `task_id` 查询任务状态。执行 Lite 升级后重复查询、验证和
启停，确认已导入资源未被覆盖。离线环境不验证 `download` 或 `download-start`。

### 6.2 Docker 离线导入与页面启停

先按以下方式挂载 `model`、`model_venvs`、`resource`，并额外将离线资源包以
只读方式挂载至 `/offline-pkgs`。前三项为 Docker 动态资源持久化的必需挂载；
不要挂载整个 `/var/lib/taos/taosanode`，也不要由页面直接写入资源目录。

```yaml
volumes:
  - ./tdgpt-data/model:/var/lib/taos/taosanode/model
  - ./tdgpt-data/model_venvs:/var/lib/taos/taosanode/model_venvs
  - ./tdgpt-data/resource:/var/lib/taos/taosanode/resource
  - <host>/offline-pkgs:/offline-pkgs:ro
```

```bash
docker compose up -d tdengine-tdgpt
docker exec tdengine-tdgpt taosanodectl resource import \
  --package /offline-pkgs/tdengine-tdgpt-tsfm-runtime-1.0.0-linux-x64.tar.gz \
  --package /offline-pkgs/tdengine-tdgpt-model-moment-1-base.zip
docker exec tdengine-tdgpt taosanodectl resource verify moment
docker exec tdengine-tdgpt taosanodectl resource list --format json
```

导入完成后，以页面或 HTTP 接口先查询
`GET /api/v1/foundation-models/moment`，确认 `install_status=installed` 且
`service_status=stopped`，再调用以下接口验证启停：

```bash
curl -sS -X POST http://127.0.0.1:6035/api/v1/foundation-models/start \
  -H 'Content-Type: application/json' \
  -d '{"model_name":"moment"}'
# 使用响应中的 task_id 查询任务状态；完成后 service_status 应为 running。
curl -sS -X POST http://127.0.0.1:6035/api/v1/foundation-models/stop \
  -H 'Content-Type: application/json' \
  -d '{"model_name":"moment"}'
```

重新创建容器后，再执行 `resource list`、`resource verify moment` 和上述启停
验证，确认动态资源挂载持久化。隔离网络中不验证页面的 `download` 或
`download-start`，它们需要访问资源发布站点；当前页面也不负责上传离线包或触发
`resource import`。

显式升级必须使用 `--replace`。升级共享 TSFM runtime 前，先停止所有使用该 runtime 的模型：

```bash
/usr/bin/taosanodectl model stop all
/usr/bin/taosanodectl resource import --replace --package <新模型或新runtime包>
```

## 7. 速度记录与日志

分别记录 runtime 首次导入、首个模型导入、第二个模型导入和重复导入的耗时。
runtime 首次导入需要完整解压和校验，通常最慢；第二个模型只解压模型文件；
重复导入只做哈希和完整性检查，不应再次解压。Windows 与 Linux 都是在目标盘
staging 后整体切换，不应出现逐文件复制造成的额外第二次拷贝。

测试结果至少记录：平台、CPU 架构、安装包版本、资源清单 URL、包大小、耗时、退出码、目标目录大小、端口和服务日志路径。

## 8. URL 兜底验收

默认地址是 `https://downloads.taosdata.com`。主机安装可在
`taosanode.config.py` 中设置 `resource_base_url`；Docker 使用 Compose 的
`TDGPT_RESOURCE_BASE_URL`。请求显式传入 `--resource-base-url` 时优先使用该
参数。

在外网资源正式发布后，在每个平台执行以下验收：

1. 不配置 URL，运行 `resource check --release 1.0.0`，确认直接访问外网；
2. 配置一个不可达或返回 `404` 的内网地址，运行相同命令，确认日志出现
   fallback 记录且随后从外网成功获得清单；
3. 使用两个不可达地址，确认命令非零退出，且错误中包含主地址和备用地址；
4. 使用内网正式地址，确认优先命中内网，不访问外网。

Docker 修改 `TDGPT_RESOURCE_BASE_URL` 后必须执行：

```bash
docker compose up -d --force-recreate tdengine-tdgpt
```

不要使用 `docker compose restart` 验证地址切换，因为它不会更新已创建容器的环境变量。

## 9. 当前验收记录（v3.4.2.8.0828）

<!-- markdownlint-disable MD013 -->

| 部署目标 | `moment` | `moirai` | runtime 复用 | 端口 | `/list` |
| --- | --- | --- | --- | --- | --- |
| Linux x64 安装包 | 已导入并启动 | 已导入并启动 | 已验证 | `6066`、`6064` 就绪 | 六个 TSFM 函数均已注册 |
| Linux arm64 安装包 | 已导入并启动 | 已导入并启动 | 已验证 | `6066`、`6064` 就绪 | 六个 TSFM 函数均已注册 |
| Windows x64 安装包 | 已导入并启动 | 已导入并启动 | 已验证 | `6066`、`6064` 就绪 | 六个 TSFM 函数均已注册 |
| Linux x64 Docker | 已导入并启动 | 已导入并启动 | 已验证 | `6066`、`6064` 就绪 | 六个 TSFM 函数均已注册 |
| Linux arm64 Docker | 已导入并启动 | 已导入并启动 | 已验证 | `6066`、`6064` 就绪 | 六个 TSFM 函数均已注册 |

<!-- markdownlint-enable MD013 -->

六个 TSFM 函数为 `chronos`、`moirai`、`moment`、`tdtsfm_1`、
`timemoe-fc` 和 `timesfm`。本轮逐个启动验收范围是 `moment` 与
`moirai`；其余四个模型仅验证函数注册，不代表已逐个完成模型启动验收。

Windows 验收使用 `tdengine-tdgpt-lite-venv-1.0.1-windows-x64.zip`；该包包含
`requests==2.32.3`。使用旧主 venv 会导致算法模块无法导入，`/list` 不会显示
对应函数，因此不得用于本版本验收。

公网 `tsfm/1.0.0` 发布清单、三个 TSFM runtime 和六个模型包均已发布。
已使用 HTTP Range 请求逐个验证资源可访问；后续正式发版仍须按第 8 节在
目标环境验证内网失败后自动切换公网的完整下载流程。

### 9.1 最终验收产物

<!-- markdownlint-disable MD013 -->

| 产物 | SHA256 |
| --- | --- |
| `tdengine-tdgpt-enterprise-lite-3.4.2.8.0828-linux-x64.tar.gz` | `582bd7755bf2db2bd9e61145c9af02e396e3479c927e4cacee8a9b9a55c87646` |
| `tdengine-tdgpt-enterprise-lite-3.4.2.8.0828-linux-arm64.tar.gz` | `184abc13d355b54913dd043bd0cf79f21ce51cbb7ce8bea1dfe987b18c1f4263` |
| `tdengine-tdgpt-enterprise-lite-3.4.2.8.0828-windows-x64.exe` | `cedf3ee918fab4e9abf6934a70f5da36964cbd085b71389c9ee3932278fdf8fd` |
| `tdengine-tdgpt-lite-venv-1.0.1-linux-x64.tar.gz` | `e2765e3a6723b0f593bd66d80b4cfd611292ee1c504910f7b184e736605a9ded` |
| `tdengine-tdgpt-lite-venv-1.0.1-linux-arm64.tar.gz` | `5316252a26b1e0f14f2c375a84d3887b9069746a33dd09179b23a2676a187a78` |
| `tdengine-tdgpt-lite-venv-1.0.1-windows-x64.zip` | `26d65111b1152b8a14d7a63fe08e97f9f7071e5ed4e6f0e5ce93fde51c5b4191` |
| `tdengine-tdgpt-enterprise-lite-docker-3.4.2.8.0828-linux-x64.tar.gz` | `4d76d239e144136df8b36e9f94e13cdec22fa052ac0a24668c0f1e19c838e96e` |
| `tdengine-tdgpt-enterprise-lite-docker-3.4.2.8.0828-linux-arm64.tar.gz` | `1c24edb56ce685861950c65f92bfc1cdfcab199606c20a9c8f0129c2829e4658` |

<!-- markdownlint-enable MD013 -->

验收结论和产物 SHA256 保留在本文中；原始验收日志不作为发布产物保存。


### 模型版本展示

`taosanodectl resource list` 与 `taosanodectl resource check` 的表格分别显示
`MODEL_VERSION`（如 `moirai-moe-1.0-r-small`）和 `RESOURCE_VERSION`（如 `1.0.0`）。
`list` 读取本地安装记录，`check` 读取指定发布清单；后者展示的是发布提供的版本。
旧安装缺少模型版本记录时显示 `unknown`，共享 runtime 的模型版本显示 `-`。
JSON 输出保留原有 `model_version` 和 `version` 字段。
