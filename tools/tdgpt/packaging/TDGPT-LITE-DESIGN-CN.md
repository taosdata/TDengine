<!-- markdownlint-disable MD013 -->

# TDgpt Lite 安装与动态资源设计

## 1. 目标与边界

TDgpt Lite 安装包只交付 TDgpt 服务和主运行环境，不内置、不下载 TSFM 模型或模型虚拟环境。模型及其运行环境作为独立资源，在安装完成后按需下载或离线导入。

本设计同时适用于安装包和 Docker：升级 TDgpt Lite 不覆盖动态资源；资源不完整或不兼容时不启动相应模型，但不影响 TDgpt 主服务。

本文描述动态资源的目标设计。TDgpt Lite 主 venv 已按现有路径交付；官方 TSFM runtime 和模型已经支持本地离线导入、发布清单驱动的在线下载、SHA-256 校验、幂等检查、原子切换及启动前校验。统一 `taosanodectl` 资源入口已落地；用户模型 worker 仍按后续阶段实现。

## 2. Lite 安装包契约

### 2.1 安装输入

TDgpt Lite 基础安装只需要以下资源：

- TDgpt Lite 安装包；
- 对应平台和架构的 Python runtime；
- 对应平台和架构的 Lite 主 venv 离线包。

Windows 安装器通过 `/PYTHON_RUNTIME_DIR=<path>` 接收已解压的 Python runtime，通过 `/OFFLINE=<path>` 接收 Lite 主 venv ZIP。Linux 安装器使用等价的 runtime 和主 venv 参数。Python runtime 只负责执行安装、导入和修复主 venv；TDgpt 服务安装完成后使用主 venv 中的 Python 启动。

### 2.2 安装行为

Lite 安装流程必须显式使用 `--model-source none`，只安装 TDgpt 程序、配置、控制脚本和主 venv，并注册对应平台的服务。安装过程中不得扫描、下载或导入模型资源、TSFM runtime 或用户模型 venv。

当前主 venv 路径如下：

| 平台 | TDgpt Lite 主 venv | 主服务 Python |
| --- | --- | --- |
| Windows | `C:\TDengine\taosanode\venvs\venv` | `C:\TDengine\taosanode\venvs\venv\Scripts\python.exe` |
| Linux | `/var/lib/taos/taosanode/venv` | `/var/lib/taos/taosanode/venv/bin/python3` |

主 venv 不属于动态资源，由 Lite 安装器管理。Windows 路径是现有离线包、WinSW 服务和升级流程的一部分；Linux 路径与现有安装布局保持一致。动态加载功能不迁移或重打主 venv。

### 2.3 升级与卸载边界

Lite 升级可以替换程序文件、配置模板和主 venv，但不得删除或覆盖 `model/` 与 `model_venvs/`。卸载默认保留模型和模型 venv；只有用户显式选择删除数据和动态资源时才清理这些目录。

## 3. 动态资源目录

Linux 的动态资源根目录为 `/var/lib/taos/taosanode`，Windows 为 `C:\TDengine\taosanode`。动态资源在两个平台使用相同的相对路径：

```text
.
├── model/
│   ├── dynamic/                           # tdmodel 动态导入的既有模型
│   ├── tsfm/
│   │   └── <model_name>/                  # 官方 TSFM 模型资源
│   └── user/
│       └── <model_name>/                  # 用户模型资源
└── model_venvs/
    ├── tsfm/                              # 官方 TSFM 共用 venv
    └── user/
        └── <model_name>/                  # 用户模型独立完整 venv
```

完整历史包中的 `model/<model_name>` 与各模型独立 venv 仅作兼容读取，不能作为新增动态资源的目标目录。

## 4. 资源所有权

| 资源 | 所有者 | 升级 Lite 时 | 运行环境 |
| --- | --- | --- | --- |
| 平台现有主 venv | TDgpt Lite 安装器 | 可按版本升级 | Lite 主服务 |
| `model/dynamic/` | tdmodel / 用户 | 保留 | 由既有动态模型机制决定 |
| `model/tsfm/` | 官方模型资源包 | 保留 | `model_venvs/tsfm` |
| `model/user/` | 用户 | 保留 | 对应的用户独立 venv |
| `model_venvs/tsfm/` | 官方 runtime 资源包 | 保留 | 仅供官方 TSFM 模型共享 |
| `model_venvs/user/` | 用户资源导入器 | 保留 | 每个用户模型独立使用 |

官方 TSFM venv 可以被用户作为基础环境参考，但不建议直接在其中执行 `pip install`。用户额外依赖应写入其自己的 venv，避免一个用户的依赖变更破坏官方模型或其他用户模型。

## 5. 官方 TSFM 动态加载协议

每个官方 TSFM 模型使用同一个 `model_venvs/tsfm`。该目录本身就是 Python venv 根目录，不再嵌套一层 `venv/`。服务仅在运行时和模型资源都完整、且相互兼容时启动模型：

```text
model_venvs/tsfm/
├── bin/ 或 Scripts/
├── lib/ 或 Lib/
├── pyvenv.cfg
├── requirements.lock
├── manifest.json
└── .tdgpt-install-complete.json

model/tsfm/<model_name>/
└── 模型自身携带的原始文件和目录
```

共享 TSFM venv 包由构建流程生成，除完整 Python venv 外，还携带 `requirements.lock` 和 `manifest.json`。资源导入器在本机完成解压、校验和目录切换后，最后创建 `.tdgpt-install-complete.json`；发布包中不得预置该文件。

这三个文件的职责如下：

- `requirements.lock`：记录共享 venv 实际安装的 Python 包及版本，用于复现构建结果和排查依赖问题；
- `manifest.json`：记录该 venv 适用的 Python 版本、操作系统、CPU 架构和依赖锁文件 hash；
- `.tdgpt-install-complete.json`：本机导入成功状态，记录归档 SHA-256、runtime ID、安装时间和实际 Python 路径。缺少该文件表示 venv 尚未完成安装或上次更新中断，服务不得使用该 venv。

`manifest.json` 使用以下结构：

```json
{
  "schema_version": 1,
  "kind": "tsfm-runtime",
  "runtime_id": "tsfm-runtime-1.0.0",
  "python_version": "3.12.12",
  "platform": "linux",
  "arch": "x64",
  "requirements_sha256": "<requirements.lock 的 SHA-256>"
}
```

`platform` 和 `arch` 分别使用 `windows`/`x64`、`linux`/`x64` 或 `linux`/`arm64`。服务启动时校验以下字段：

- `schema_version` 和资源类型；
- `runtime_id`；
- Python 版本、操作系统和 CPU 架构；
- `requirements_sha256`，即共享 venv 依赖锁文件的 hash。

官方单模型资源包统一使用 ZIP，Linux 和 Windows 共用同一份。ZIP 内保留模型原有内容，不添加 `manifest.json`、`requirements.lock` 或完成状态文件。模型包版本和归档 SHA-256 由产品下载清单管理；本地导入时通过 staging 解压并原子替换目标目录。

共享 venv 校验失败、缺少 `.tdgpt-install-complete.json`、模型目录不存在或模型必要文件缺失时，服务记录原因并跳过该模型，不下载资源，也不阻塞 TDgpt 主服务。

### 5.1 共享 venv 的复用

官方 TSFM 模型共同使用 `model_venvs/tsfm`，因此 venv 只需成功导入一次。例如首次导入 `moirai` 时：

1. 若本地共享 venv 不存在或校验失败，导入器导入对应平台的 TSFM venv 包；
2. 校验通过后，将 venv 原子切换到 `model_venvs/tsfm`，并写入 `.tdgpt-install-complete.json`；
3. 再导入 `model/tsfm/moirai` 并启动 `moirai` worker。

之后导入 `moment`、`chronos` 等官方模型时，导入器先校验现有 `model_venvs/tsfm`，不重复下载或解压 venv；只导入模型 ZIP。校验内容包括：

- `.tdgpt-install-complete.json` 存在且类型正确；
- `manifest.json` 的 schema、平台、架构和 Python 版本匹配当前主机；
- `requirements.lock` 的 SHA-256 与 `manifest.json.requirements_sha256` 一致；
- venv 的 Python 可执行文件可以运行，并报告 manifest 中声明的版本。

模型 ZIP 的 SHA-256 只用于校验模型归档和幂等状态，不用于重新下载共享 venv。只有共享 venv 缺失、损坏、平台/架构/Python 版本不匹配，或显式升级到新的 `runtime_id` 时，才需要重新导入新的 venv 包；升级时应先停止所有使用该共享 venv 的模型 worker。

当前官方模型 ZIP 保持原始模型文件，不包含 `manifest.json` 或 `requirements.lock`；模型与共享 venv 的兼容关系由产品发布清单和统一 TSFM requirements 维护。若未来需要让第三方模型绑定专用 runtime，应使用用户模型独立 venv，不应修改官方共享 `model_venvs/tsfm`。

## 6. 用户模型约定

用户模型不能与官方共享 TSFM venv 混装。一个可交付用户模型由以下内容组成：

```text
model/user/<model_name>/
└── 模型自身携带的原始文件和目录

model_venvs/user/<model_name>/
├── bin/ 或 Scripts/
├── pyvenv.cfg
├── requirements.lock
├── manifest.json
└── .tdgpt-install-complete.json
```

`<model_name>` 是用户为模型指定的唯一名称，目标目录只保留当前激活版本。用户提供 `requirements.txt` 后，资源导入器在 staging 目录创建或更新该模型的独立 venv，生成锁文件与 manifest，原子切换到目标目录，最后写入 `.tdgpt-install-complete.json`。模型目录保持用户模型自身的原始内容。

用户的 `requirements.txt` 只在资源导入或更新时处理，服务启动时不得自动执行 `pip install`。导入器应根据它创建独立 venv，并生成确定的 `requirements.lock`；依赖安装失败时继续使用旧资源。

用户模型的动态下载、requirements 安装器和 worker 隔离属于后续功能；在其实现前，Lite 不应宣称已支持自动加载 `model/user` 中的任意模型。

## 7. 资源导入、升级与回滚

### 7.1 升级触发

资源管理器可以检查远端产品发布清单并提示存在新版本，但不得自动切换资源。只有以下操作触发安装或升级：

- 用户在产品界面中显式安装或升级模型；
- 产品发布流程显式调用 `taosanodectl resource apply`；
- 管理员通过 `taosanodectl resource import` 导入一个本地资源包。

TDgpt Lite 安装或升级、taosanode 启动、机器或容器重启以及模型 worker 启动均不得自动下载、解压或升级动态资源。

### 7.2 发布清单与升级判定

产品发布清单记录共享 TSFM runtime 和每个模型的真实模型标识、版本、文件名、归档大小与 SHA-256。模型通过 `required_runtime` 声明产品验证时使用的共享 runtime 版本：

```json
{
  "schema_version": 1,
  "release": "1.0.0",
  "runtimes": [
    {
      "kind": "runtime",
      "name": "tsfm",
      "version": "1.0.0",
      "platform": "linux",
      "arch": "x64",
      "file": "tdengine-tdgpt-tsfm-runtime-1.0.0-linux-x64.tar.gz",
      "size": 578639183,
      "sha256": "<archive sha256>"
    }
  ],
  "models": [
    {
      "kind": "model",
      "name": "moirai",
      "model_id": "Salesforce/moirai-moe-1.0-R-small",
      "model_version": "moirai-moe-1.0-r-small",
      "version": "1.2.0",
      "file": "tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip",
      "size": 433629526,
      "sha256": "<archive sha256>",
      "required_runtime": "1.0.0",
      "note": "Salesforce Moirai-MoE 1.0 R small model"
    }
  ]
}
```

资源管理器按以下规则判定操作：

| 本地状态 | 处理方式 |
| --- | --- |
| 没有对应资源 | 首次安装 |
| 版本和 SHA-256 均相同，且必要文件完整 | 直接返回成功，不下载、不解压、不重启 worker |
| 版本不同或 SHA-256 不同 | 显式升级时下载并切换 |
| 版本相同但 SHA-256 不同 | 视为同版本资源被重新发布，必须明确提示后再升级 |
| `required_runtime` 不匹配 | 先升级共享 TSFM venv，再启动模型 |

模型的安装状态保存在 `resource/state/models/<model_name>.json`，记录 `model_id`、`model_version`、资源版本、发布文件名、归档大小、SHA-256、目标路径、部署时间和备注；状态文件只在模型目录切换成功后原子写入。共享 venv 使用自身的 `.tdgpt-install-complete.json` 记录等价状态。

### 7.3 下载与缓存

下载缓存位于 `resource/cache`。资源先下载到以归档 SHA-256 命名的 `.part` 文件，支持 HTTP Range 断点续传；下载完成并通过 SHA-256 校验后，再原子改名为正式缓存文件。缓存中已有相同 SHA-256 的完整归档时不得重复下载。

归档成功导入并完成目标目录校验后，资源管理器会删除对应的 `.archive` 文件，仅保留已安装的模型、共享 venv、状态元数据和未完成的 `.part` 文件。这样既保留断点续传能力，也避免长期同时占用压缩包和解压目录的磁盘空间。若导入失败，已完成下载的归档暂不删除，便于排查或重试；下次成功导入后再清理。

本地离线导入跳过下载步骤，但仍须计算并校验归档 SHA-256。已安装状态与目标 SHA-256 相同、且必要文件检查通过时，不创建 staging，不重复解压。

### 7.4 原子切换

官方 TSFM venv 包只写入 `model_venvs/tsfm`；每个官方模型包只写入 `model/tsfm/<model_name>`。用户资源只写入自己的 `model/user/<model_name>` 与 `model_venvs/user/<model_name>`。

所有资源在目标目录所在文件系统的 staging 目录中完成解压和校验，禁止从其他磁盘逐文件复制到目标目录。首次安装时，将 staging 整体改名为目标目录；升级时先将目标目录改名为 backup，再将 staging 改名为目标目录。同一文件系统内的目录改名不会复制模型文件，不应成为安装速度瓶颈。

单个模型资源升级只停止目标模型 worker；其他模型可以继续运行。共享 TSFM venv 升级会影响所有使用它的模型，必须停止全部 worker。同一次产品发布包含共享 venv 和多个模型时，资源管理器先将整组归档全部下载并校验到本地，再进入切换阶段。

### 7.5 启动验证与回滚

资源导入成功只表示文件已安装；模型升级成功还要求对应 worker 完成模型权重加载并通过 readiness 检查。模型进程存在但 readiness 未通过，不能判定为升级成功。

首次安装的模型启动失败时保留已导入文件并将状态标记为未就绪，便于查看日志和重试。升级已有模型时，新模型 readiness 通过后再删除旧 backup；启动失败则停止新 worker、恢复 backup，并重新启动旧模型。共享 TSFM venv 升级后逐个恢复原先运行的模型，启动失败的模型保持停止并明确报告。

删除旧 backup 可以在切换成功后异步执行，不阻塞资源激活结果。首次安装和重复应用相同 SHA-256 不产生 backup。

资源命令启动时必须先恢复上次中断的事务：目标目录缺失而 backup 完整时恢复 backup；目标目录完整时清理遗留 staging；目标和 backup 同时存在时，根据完成状态与完整性校验选择目标或回滚。资源导入使用全局互斥锁，同一时间只允许一个资源事务运行。

Lite 升级不得删除 `model/dynamic`、`model/tsfm`、`model/user`、`model_venvs`、资源状态或下载缓存。

## 8. Docker 部署

Docker 镜像只包含 Lite 服务与主运行环境。宿主机应分别挂载动态资源目录，而不是覆盖整个 TDgpt 数据根目录：

```text
<host>/model        -> /var/lib/taos/taosanode/model
<host>/model_venvs  -> /var/lib/taos/taosanode/model_venvs
<host>/resource     -> /var/lib/taos/taosanode/resource
```

`resource` 挂载用于持久化发布缓存、任务状态和模型状态；只挂载 `model`、`model_venvs`、`resource`，不要覆盖整个 TDgpt 数据根目录。这样容器升级不会覆盖模型和模型 venv，也不会因整个数据根目录被挂载而遮蔽镜像或安装器创建的主 venv。容器启动时按第 5 节的校验规则发现可用 TSFM 资源；没有资源时只运行 Lite 主服务。

资源下载地址通过 `TDGPT_RESOURCE_BASE_URL` 注入容器，例如：

```yaml
environment:
  TDGPT_RESOURCE_BASE_URL: "${TDGPT_RESOURCE_BASE_URL:-https://downloads.taosdata.com}"
```

Docker 环境变量在容器创建时确定。修改 Compose 中的 URL 后，必须执行
`docker compose up -d --force-recreate tdengine-tdgpt` 重新创建 TDgpt 容器；
`docker compose restart` 不会更新既有容器的环境变量。挂载的 `model`、
`model_venvs` 与 `resource` 数据不受重新创建容器影响。

## 8.1 资源下载地址与外网兜底

资源地址按照以下优先级确定：

```text
API 请求的 resource_base_url
  > TDGPT_RESOURCE_BASE_URL
  > taosanode.config.py 的 resource_base_url
  > https://downloads.taosdata.com
```

主机安装模式通过 `taosanode.config.py` 保存默认地址；Docker 模式通过
`TDGPT_RESOURCE_BASE_URL` 提供默认地址。`taosanalytics/conf.py` 和
`taosanode_service.py` 是读取并合并这些配置的代码，不是额外配置文件。

对于由配置或环境变量提供的 HTTP(S) 基础地址，资源管理器先访问主地址；获取
发布清单或下载资源时遇到连接失败、超时、HTTP `404` 或 HTTP `5xx`，会尝试
公共备用地址 `https://downloads.taosdata.com`。日志必须记录主地址失败和实际
使用的备用地址。显式指定完整发布清单 URL 或本地路径时不启用备用地址，避免
调用方要求严格来源时被静默替换。

如果主地址与备用地址均失败，命令返回非零，并同时保留主地址错误。备用地址的
清单和资源必须先正式发布，才能作为可用性兜底；未发布资源返回 `404` 属于预期
发布状态，不应被误判为代码故障。

## 9. 发布物与兼容绑定

每个平台和架构分别发布：

- TDgpt Lite 安装包；
- Python runtime；
- Lite 主 venv 离线包；
- 官方 TSFM venv 资源包，Linux 使用 `tar.gz`，Windows 使用 ZIP；
- 每个官方 TSFM 模型资源包，所有平台统一使用 ZIP。

共享 TSFM venv 包的 manifest 必须绑定 `platform`、`arch`、`python_version`、`runtime_id` 和 `requirements_sha256`。不同平台、架构或 runtime 的 venv 包不得混用。下载目录或产品资源清单必须提供每个 venv 包和模型 ZIP 的归档 SHA-256；`requirements_sha256` 只校验依赖锁文件，不能替代资源包完整性校验。

首版发布的六个模型包使用真实模型名称，不提供旧文件名兼容别名：

| 服务名 | 模型标识 | 发布文件名 |
| --- | --- | --- |
| `chronos` | `amazon/chronos-bolt-base` | `tdengine-tdgpt-model-chronos-bolt-base.zip` |
| `moirai` | `Salesforce/moirai-moe-1.0-R-small` | `tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip` |
| `moment` | `AutonLab/MOMENT-1-base` | `tdengine-tdgpt-model-moment-1-base.zip` |
| `tdtsfm` | `tdengine/tdtsfm-v1.0` | `tdengine-tdgpt-model-tdtsfm-v1-0.zip` |
| `timemoe` | `Maple728/TimeMoE-200M` | `tdengine-tdgpt-model-timemoe-200m.zip` |
| `timesfm` | `google/timesfm-2.5-200m-pytorch` | `tdengine-tdgpt-model-timesfm-2-5-200m-pytorch.zip` |

文件名只用于发布和下载，安装目录仍使用稳定的服务名。若 ZIP 内容不变，仅修改发布文件名不会改变 SHA-256；本地已安装状态与发布清单 digest 相同时，`resource apply` 直接返回 `unchanged`，不会重复下载或导入。

## 10. 服务、资源与模型生命周期命令

### 10.1 唯一控制入口

后续以 `taosanodectl` 作为跨平台唯一的稳定接口。它调用 `taosanode_service.py`，屏蔽 Linux 的 systemd/进程模式、Windows 服务模式和 Docker 前台模式的差异；业务脚本、安装器和自动化平台不得直接调用 `start.sh`、`stop.sh` 或 Python 脚本。

```text
taosanodectl service start
taosanodectl service stop
taosanodectl service status
taosanodectl service wait-ready [--timeout <seconds>]

taosanodectl resource import --package <path> [--package <path> ...]
taosanodectl resource check [<model_name|all>]
taosanodectl resource apply <model_name|all> [--release <path-or-url>]
taosanodectl resource list
taosanodectl resource verify [<model_name|all>]

taosanodectl model start <model_name|all> [--timeout <seconds>]
taosanodectl model stop <model_name|all>
taosanodectl model status [<model_name|all>]
```

模型启动的默认 readiness 期限由 `taosanode.config.py` 中的
`timeouts["model_startup"]` 控制，默认值为 180 秒。该值是模型进程创建后到端口就绪的
总期限，不是单次 TCP 连接超时；单次 TCP 探测仍为 1 秒。模型进程若在期限内提前退出，
启动立即失败并输出模型日志路径及日志尾部。`model start --timeout` 仅覆盖本次命令，
不修改配置文件。

`resource check` 只比较发布清单和本地状态，不下载或修改资源，并输出模型标识、模型版本、digest、归档大小、状态和备注；它用于展示可下载模型列表。`resource apply` 按发布清单同步下载并显式安装或升级资源。`resource import` 导入本地包。`resource list` 不访问远端，输出本机已安装模型的上述元数据、部署时间、安装状态、服务状态、PID 和端口，并合并当前下载/导入任务的状态和进度。资源不存在或校验失败时返回非零，并保留当前可用版本。

对外展示时，`resource check` 的资源状态使用 `missing`、`installed`、`outdated`、`downloading` 和 `failed`；`resource list` 的服务状态使用 `running`、`stopped`，任务进行中可显示 `downloading`、`verifying` 或 `importing`。`check` 表示“发布清单中有什么以及本机是否满足”，`list` 表示“本机现在有什么以及正在做什么”，二者都不会隐式下载资源。

资源命令仍采用同步 CLI 和全局文件锁：同一时间只执行一个导入事务；第二个并发请求直接返回 `another TDgpt resource operation is running`。调用方可以在后台执行 `resource apply`，同时轮询 `resource list`。任务状态保存在 `resource/state/tasks/<sha256>.json`，包含 `downloading`、`verifying`、`importing`、`completed` 或 `failed`，以及 `bytes_done`、`bytes_total` 和错误信息；这是内部实现，不要求调用方直接读取。模型导入完成后，再通过 `model start` 启动并通过 `model status` 检查运行状态。

`taosanodectl` 是唯一对外资源入口。`taosanode_service.py` 是内部实现，不作为安装器、发布平台或用户自动化的调用接口。

资源导入会校验归档路径和可选 SHA-256，在目标目录所在文件系统创建 staging，校验通过后整体切换。重复导入已存在且完整的资源不会重复解压；`--replace` 用于显式升级，并要求先停止全部模型 worker，避免运行中的进程持有旧文件。

`all` 必须显式传入；尤其 `model stop` 不允许省略目标后默认停止全部模型。命令成功返回 `0`，执行失败返回非零；`status` 可供自动化读取，并在目标未运行时返回非零。

### 10.2 平台入口

| 平台 | 控制命令 |
| --- | --- |
| Linux 安装包 | `/usr/bin/taosanodectl` |
| Windows 安装包 | `C:\TDengine\taosanode\bin\taosanodectl.bat` |
| Docker | 容器内 `taosanodectl`；主服务由容器 entrypoint 前台运行 |

Linux 包可额外提供以下兼容别名，全部只转发到 `taosanodectl`：

```text
/usr/bin/start-taosanode  -> taosanodectl service start
/usr/bin/stop-taosanode   -> taosanodectl service stop
/usr/bin/status-taosanode -> taosanodectl service status
/usr/bin/start-model      -> taosanodectl model start
/usr/bin/stop-model       -> taosanodectl model stop
/usr/bin/status-model     -> taosanodectl model status
```

Windows 提供同名 `.bat` 包装命令。现有 `start.sh`、`stop.sh`、`start-model.sh`、`stop-model.sh` 仅是包内兼容实现，不再作为对外文档或自动化接口。

### 10.3 职责边界

- `service` 只管理 taosanode 主服务，不会隐式启动或停止模型 worker；
- `model` 只管理模型 worker，启动前校验共享 venv 的 manifest 和 `.tdgpt-install-complete.json`，并检查模型目录及必要文件；
- `resource` 只管理 `model/tsfm`、`model/user` 和 `model_venvs`，不得修改 Lite 主 venv；
- 主服务停止或升级前，安装器可以显式执行 `model stop all`，但普通人工 `service stop` 不隐式改变模型状态；
- 失败原因写入 `taosanode-service.log`，调用方只根据退出码判断成功或失败。

资源命令默认以表格输出，便于运维人员直接查看；UI、Apex 和自动化脚本应显式使用 `--format json` 获取稳定的机器可读结果：

```text
taosanodectl resource list --format json
taosanodectl resource check --format json
```

`--format table` 和 `--format json` 只改变输出格式，不改变命令语义。表格中的 `status` 是综合状态，`service_status` 表示模型服务进程状态，`install_status` 表示资源文件状态；任务进行中还会返回 `task_status`、`bytes_done` 和 `bytes_total`。

## 11. 实施阶段

### 第一阶段：官方 TSFM 动态资源（当前进度）

- 将动态模型 venv 路径统一为 `model_venvs`；
- 已实现官方 TSFM venv 与模型资源的离线导入、SHA-256 校验、幂等检查、原子切换和启动前校验；
- 已提供 `taosanodectl resource check/apply/import/list/verify` 统一入口；
- 已将六个官方模型的真实模型标识、版本、发布文件名、归档大小和备注写入发布清单；
- 当前资源操作采用同步 CLI 和全局文件锁，不重复下载相同 digest；
- 已在 Linux x64、Linux arm64 和 Windows x64 完成在线首次导入、第二模型复用 runtime、重复导入、完整性校验、Moirai/Moment 启动与端口验收；

ARM64 的共享 TSFM runtime 发布前必须使用 `patchelf >= 0.14.0` 修复
`jaxlib/libjax_common.so` 的 `librt.so.1` 依赖，将其改为
`libc.so.6`。这是 Ubuntu 18.04/glibc 2.27 上 Moirai 使用的
`clock_gettime@GLIBC_2.17` 兼容修复；系统默认的 `patchelf 0.9` 不得用于
构建。发布验收必须直接导入 `jax/jaxlib`，并实际启动 Moirai 检查 6064
端口。

- `resource apply` 支持发布清单、平台/架构筛选、SHA-256 缓存、HTTP Range 断点续传和显式替换。
- platform-group 已支持 Lite 三平台安装包构建、双架构 Docker 构建和发布；动态资源下载到模型 readiness 的验收当前保留为手工发布检查，后续再固化为流水线自动化。
- 用户模型独立 venv/worker 仍属于后续范围，不阻塞本期六个官方 TSFM 模型。

### 第二阶段：用户模型

- 定义用户模型 worker 接口；
- 根据用户 `requirements.txt` 创建独立 venv；
- 支持用户模型资源的导入、更新、回滚和隔离运行；
- 在产品下载器中接入动态资源下载与状态展示。

本文会在动态资源验收自动化和用户模型 worker 实现后继续补充最终错误码与扩展测试矩阵。
