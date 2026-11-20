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

### 2.1.1 打包工具与产品 Python 环境

打包机用于执行 `build_tdgpt.py` 等发版脚本的 Python，与产品运行环境分开管理，不会因为用于构建而被复制进安装包或替换离线 venv。本轮打包工具已使用 Python 3.10 验证通过；无需为构建统一改成 Python 3.12。执行前确认解释器及构建依赖可用，构建失败后不得将旧输出误认为本次产物。

产品安装和验收必须使用既有的对应平台、架构资源：

| 环境 | 用途 |
| --- | --- |
| Python runtime | 为安装器导入、创建或修复主 venv 提供解释器，新构建目标为 Python 3.12.16；已发布资源以实际清单为准 |
| Lite 主 venv | 运行 TDgpt 主服务，使用已交付的 Lite 离线 venv |
| TSFM runtime / 模型 venv | 运行动态模型，使用匹配平台与架构的共享 TSFM runtime |

模型权重使用独立模型资源包。产品验收应记录实际运行的解释器路径及版本，不得以打包工具 Python 或系统 Python 代替主 venv、TSFM runtime 执行导入和推理检查。

### 2.1.2 产品与资源版本分别管理

以下为 Python 3.12.16 升级批次的候选资源及产品示例版本；后续发版以实际发布清单、资源 manifest 和 SHA-256 为准，不根据产品版本推导资源版本。

| 对象 | 本轮版本 | 升级方式 |
| --- | --- | --- |
| TDgpt Enterprise Lite 程序 | 以本轮产品发布登记为准 | 使用对应平台的产品安装包，或替换对应架构的 Lite 镜像 |
| 安装用 Python runtime | Python `3.12.16` | 由依赖部署流程提供，不能用打包工具或系统 Python 代替 |
| Lite 主 venv | `1.0.4` | 安装/升级产品时显式传入对应平台的离线包；Apex 使用清单中的依赖版本和 SHA-256 |
| 动态资源发布清单 | 随包推荐 `1.0.1`，schema 2 | CLI 默认读取本地 `share/tsfm/tsfm-release.json`；`--release <版本、URL 或本地清单>` 显式选择 |
| 共享 TSFM runtime | 本轮候选 `1.0.1`，默认清单为 `1.0.1` | 通过资源命令单独导入或替换，不随主 venv 升级 |
| 模型权重 | 各模型独立版本 | 按发布清单中的 `model_version`、资源版本与 SHA-256 识别 |

`1.0.4` 是 Lite 主 venv 资源版本，不是 Python 或 TSFM 版本。CLI 不传 `--release` 时读取随包清单，不访问远端清单；HTTP 既有下载接口及下载事件默认 release 为 `1.0.1`，runtime 状态接口默认读取随包清单。显式指定 `--release 1.0.0` 仍可选择旧清单，检查最新版须显式查询 `latest`。随包推荐版本与本机已安装版本分别展示，程序更新不自动替换已安装 TSFM。正式发布产品前必须先上传并核验 `1.0.1` 清单及引用资源；资源版本不能由产品或 Lite 主 venv 版本推导。安装、模型导入与推理验收分别使用第 2.1.1 节指定的解释器。

Linux x64 与 ARM64 的 Lite 主环境和共享 TSFM runtime 统一要求 glibc ≥ 2.28；glibc 2.27 / Ubuntu 18.04 不再属于本轮及后续版本的支持范围。历史低版本系统测试通过不构成后续兼容承诺。部署前须确认目标系统版本，并在真实 glibc 2.28 环境验收资源和服务。

Python patch 版本从 3.12.12 升级到 3.12.16 时，必须重新构建并发布 Linux/Windows Lite 主 venv，以及共享 TSFM runtime；不能在现有 venv 上直接替换解释器或执行增量升级。模型权重和用户算法目录不需要重建，但升级前必须停止使用共享 TSFM runtime 的模型 worker，完成新 runtime 校验后再恢复服务。Full venv 若继续发布，也必须使用 3.12.16 runtime 重建。

本轮三平台主 venv 资源为：

```text
tdengine-tdgpt-lite-venv-1.0.4-linux-x64.tar.xz
tdengine-tdgpt-lite-venv-1.0.4-linux-arm64.tar.xz
tdengine-tdgpt-lite-venv-1.0.4-windows-x64.zip
```

资源位于 `tdengine-tdgpt-enterprise/deps/`；构建与验收时应核对清单，复用已发布资源不等于从 requirements 重建环境。资源构建方法见 [TDGPT-LITE-BUILD-CN.md](TDGPT-LITE-BUILD-CN.md)。

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

Windows 动态资源运行数据统一保存在 `data/resource`。从已发布版本升级时，安装器保留其中的模型状态、任务状态和缓存，并将已有配置中的 `resource_dir` 修正为该目录。早期版本错误写入安装目录 `resource` 的 `foundation_models.db` 属于可重建的运行时索引，升级时删除并在 `data/resource` 中重新生成。安装目录 `resource` 已废弃，新安装不再创建，从旧版本升级时删除整个目录。

## 3. 动态资源目录

Linux 的动态资源根目录为 `/var/lib/taos/taosanode`，Windows 的模型和运行时根目录为 `C:\TDengine\taosanode`。模型目录结构如下：

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

资源状态、任务、缓存、运行时数据库和并发锁统一位于资源目录：

| 平台 | 资源目录 |
| --- | --- |
| Linux / Docker | `/var/lib/taos/taosanode/resource` |
| Windows | `C:\TDengine\taosanode\data\resource` |

`foundation_models.db`、`state/models/`、`state/tasks/`、`cache/` 以及资源操作锁必须使用同一个资源目录，不能分别写入安装目录和 `data` 目录。

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
  "runtime_id": "tsfm-runtime-1.0.1",
  "python_version": "3.12.16",
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

### 6.1 用户 Python 算法持久化

本节的配置、加载器和安装迁移随本 MR 实现，覆盖 Linux、Windows 安装包及 Docker 的 Full / Lite 交付。旧版从程序目录 `lib/taosanalytics/algo/custom/ad/`、`fc/` 导入用户算法，新版改为下述持久化目录。构建和升级验收状态以 MR 验证记录为准，代码实现不代表所有平台均已完成发布验收。

配置项 `custom_algo_dir`，指定用户 Python 算法根目录。它与第 6 节的用户模型独立 venv/worker 是两个入口：本期 Python 算法继续使用主服务解释器，遵守现有算法基类、类名和算法名注册规则，不自动安装依赖，也不提供进程隔离。

| 平台 | 默认 `custom_algo_dir` | 配置与保留规则 |
| --- | --- | --- |
| Linux / Docker | `/var/lib/taos/taosanode/custom` | 主机使用已有配置文件；容器使用配置文件或下述环境变量 |
| Linux 自定义安装目录 | `<安装目录>/data/custom` | 由实际 `data_dir` 派生默认值 |
| Windows | `<安装目录>/data/custom` | 跟随实际安装目录，不固定盘符，不需要 junction |

目录结构固定为：

```text
custom/
├── ad/                          # 用户异常检测算法 Python 文件
├── fc/                          # 用户预测算法 Python 文件
└── batch/                       # 用户批量包络线算法 Python 文件
```

首次安装自动创建默认目录，无需用户手工配置。默认统一从 `data_dir` 派生为 `<data_dir>/custom`，配置模板不单独固定算法路径；仅修改 `data_dir` 时加载器自动跟随新位置。更换数据目录须同步迁移已有数据，不会自动搬移已部署文件。管理员需要更换存储位置时，可将 `custom_algo_dir` 改为其他持久化目录。Docker 支持可选的 `TDGPT_CUSTOM_ALGO_DIR` 环境变量，非空值优先于配置文件；容器内路径必须与挂载目标一致。该变量仅覆盖运行时目录，安装器迁移不继承它；需要改变主机迁移位置时，须将路径写入现有配置的 `custom_algo_dir` 或 `data_dir`，确保安装后服务使用相同位置。配置路径必须是绝对目录，拒绝与程序 `lib/`、主 venv、`model/` 或 `model_venvs/` 及资源状态目录重叠，避免升级清理与资源操作相互覆盖。旧配置未包含该项时，由加载器按实际数据目录兼容；旧配置连 `data_dir` 也没有时由安装器补齐数据根目录，不写死 `custom_algo_dir`，不能要求用户覆盖整个旧配置文件。Windows 还须拒绝程序 `bin/`、 `requirements/`、`python/`、`resource/` 和日志目录，避免升级或卸载删除用户算法。

加载器从 `ad/*.py`、`fc/*.py`、`batch/*.py` 发现算法，使用独立包命名空间导入，支持同目录辅助模块及相对导入，不将用户目录插到全局 `sys.path` 前端。辅助包允许被导入，但不递归发现包内算法。内置算法先注册，用户算法后注册；与内置或其他用户算法重名时记录算法名、冲突文件并跳过冲突项，不覆盖已有注册。单个模块导入或实例化失败须记录完整文件路径和原因，其余算法继续加载；显式配置的目录不可读或配置无效时启动失败，默认空目录则正常启动。文件变更后显式重启主服务生效，本期不做热加载，不能把 `tdmodel` 的动态同步行为当作 Python 文件热更新。

算法类名按现有加载规则以 `_` 开头，并在该文件中定义；`name` 是对外注册名。迁移只复制文件，不改写 Python 导入语句。引用历史 `lib.taosanalytics.algo.custom` 路径的用户代码需改用相对导入后验证。

`batch/*.py` 承载批量包络线算法，服务 `/api/v1/analysis/batch` 与兼容路径 `/tool/batch`。包络线是每请求构造、返回数组的无状态计算，与 `ad/`、`fc/` 的服务对象形态不同，因此使用独立协议：模块内定义以 `_` 开头的类，暴露 `name` 与 `build(ts, values, windows, params)`，返回等长、有限的 `center`、`lower`、`upper` 三条曲线。该协议的插件不继承服务基类，也不注册进 `ServiceRegistry.services`：`/list` 的成员契约要求 `type` 属性与 `get_desc()` 等访问器，且 `get_typed_services()` 在 `try` 之外读取 `val.type`，混入不符合契约的对象会使 `/list` 返回 500。包络线插件注册在独立的 `batch_services` 中，由 `GET /api/v1/analysis/batch/algos` 列出，该接口只返回名字、版本与参数声明，不返回文件路径；内置 `golden` 流水线保持默认算法且不被同名插件覆盖。

`batch/` 的参数契约可由同名 sidecar `<module>.json` 或类属性 `param_spec` 声明，支持 `type`（number/integer/boolean/string/array）、`minimum`、`maximum`、`enum`、`default`、`required`。声明存在时，请求 `config` 中的未知键返回错误而非被丢弃；未声明时请求参数原样透传，与 sidecar 引入前的行为一致。`version` 与 `entry_sha256` 随结果回带，未声明版本时为 `null`。包络线算法与 `ad/`、`fc/` 一样在本期不做热加载，修改后重启主服务生效。

无需公共初始化代码时，不需要创建 `custom/__init__.py`。存在该文件时，加载器按正常 Python 包规则执行一次；根包初始化失败时记录原因并跳过该目录的用户算法，内置算法不受影响。例如将通用函数放在根包，在预测算法中相对导入：

```python
# <data_dir>/custom/__init__.py
def adjust(value):
    return value + 10
```

```python
# <data_dir>/custom/fc/my_ets.py
from .. import adjust
from lib.taosanalytics.algo.fc.ets import _EtsService

class _MyETS(_EtsService):
    name = "my_ets"
    _builtins = False

    def get_desc(self):
        return f"Custom ETS example: {adjust(32)}"
```

该示例复用内置 ETS 的预测实现，服务重启后以 `my_ets` 注册，描述中包含 `42`。独立算法也可省略根包文件及 `adjust` 导入。

手工启动服务需要外部配置时，可使用以下命令（Windows 示例）：

```powershell
& "D:\TDengine\taosanode\venvs\venv\Scripts\python.exe" `
  "D:\TDengine\taosanode\bin\taosanode_service.py" start --foreground `
  --config "D:\TDgpt settings\custom.py"
```

`custom.py` 应包含完整服务配置，可复制已有配置后修改 `bind` 和 `custom_algo_dir`。控制器与应用均使用该文件，无需再设置 `TDGPT_CONF`；显式路径优先于继承的环境变量。 `--config` 仅对本次命令生效，不会修改已注册的 Windows 服务启动参数。

`custom/` 保存 Python 算法代码；`model/user/` 保存用户模型资源； `model_venvs/user/` 保存未来用户模型独立环境；`model_venvs/tsfm/` 仅供官方 TSFM。内置算法仍随程序交付，不复制到 `custom/`。主 venv 被替换时，算法代码保留，但用户额外安装到旧主 venv 的依赖不会自动保留；升级前须记录依赖并在测试环境验证它们与新主 venv 的兼容性。不能通过修改 TSFM venv 解决主服务算法的缺失依赖。

安装与迁移顺序如下：

1. 在维护窗口读取旧配置并确定最终目标目录，在清理程序文件前检查历史算法，通过只读预检查一次列出全部目标冲突；预检查不复制文件或修改配置。 Linux `script/install.sh::install_main_path` 当前会删除旧 `lib/`，迁移必须先于此步骤；Linux 安装器先确认已有模型 worker 与主服务停止，再迁移并替换程序。停止失败则终止安装，保留旧算法目录与程序，不忽略失败继续升级。 Windows 发行包不再交付旧 custom 目录占位文件，保留旧算法供安装脚本在替换主 venv 前迁移。Docker 用户须在删除旧容器前，从旧容器导出未挂载的历史目录，不能指望新镜像访问旧容器文件。
2. 首次安装创建 `custom/ad`、`custom/fc`；升级将历史目录的代码、辅助包和资源复制到目标目录，排除 `__pycache__` 和字节码。目标不存在则复制，内容相同则跳过，同名内容不同则停止升级并列出冲突，保留源目录与备份，不静默覆盖用户文件。
3. 校验新目录及备份中的文件内容与源文件一致后，将旧目录原子改名并清理，不再作为后续升级的迁移来源；已存在且内容相同的文件也须备份后退役。在备份目录中保留迁移记录，包括源路径、目标路径、备份路径、文件摘要和退役状态。输出迁移数量、备份与记录路径到安装日志。复制或校验失败时保留旧源，可重试；退役后即使清理中断，后续升级也不会覆盖新目录的修改或恢复已删除的算法。新版不再同时扫描旧目录，避免同一算法重复注册。复制优先用硬链接发布完整临时文件；文件系统不支持硬链接时采用独占创建，不覆盖已有文件。备用复制发生异常时清理本次创建的半成品并保留旧源，以便重试。进程被强制终止时可能留下半成品；再次安装会报告内容冲突，须核对备份后处理。
4. 升级保留 `custom/` 以及原有模型、venv 和资源状态；默认卸载也保留用户算法。外部自定义路径绝不由卸载器递归删除。清空数据应由管理员备份后单独执行。 Linux 非默认路径不得放进后续会被删除的安装目录；Windows 两套 Inno 模板须将默认目录标为卸载保留。

Docker 在第 8 节三个动态资源挂载之外增加独立算法挂载：

```yaml
volumes:
  - ./tdgpt-data/custom:/var/lib/taos/taosanode/custom
```

宿主机预先创建 `custom/ad`、`custom/fc`，赋予服务账号读取和目录遍历权限，只允许可信管理员修改这些可执行代码。可使用只读算法挂载；模型和资源目录继续按各自用途读写。无需软链接，也不挂载整个 `/var/lib/taos/taosanode`，避免遮蔽镜像里的主 venv。Full 镜像已有内置模型目录，增加挂载时需先导出其中需要保留的资源，不能直接以空目录遮蔽预置模型。

实现覆盖 `lib/conf.py`、配置模板、`lib/service_registry.py`、Linux 安装/卸载、Windows `install.py` / `uninstall.py` 与 Full / Lite Inno 模板，以及 Docker 部署示例。发布前在 Linux x64、Linux ARM64、Windows x64 和双架构镜像验证新安装、旧目录迁移、迁移冲突、中断重试、重复升级、默认卸载保留、外部路径、相对导入、重名拒绝、坏模块隔离、重启生效及算法实际调用。只验证目录存在不算通过。

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
      "name": "tsfm_runtime",
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

模型的安装状态保存在 `<resource_dir>/state/models/<model_name>.json`，记录 `model_id`、`model_version`、资源版本、发布文件名、归档大小、SHA-256、目标路径、部署时间和备注；状态文件只在模型目录切换成功后原子写入。共享 venv 使用自身的 `.tdgpt-install-complete.json` 记录等价状态。

#### 共享模型下载路径（schema 2）

新发布清单使用 `schema_version: 2`，将模型权重与 TSFM runtime 的发布版本目录分开。模型归档存放在 `tdengine-tdgpt-enterprise/tsfm/models/`，runtime 和清单仍存放在 `tdengine-tdgpt-enterprise/tsfm/<release>/`。这里是下载站点的制品布局，本机安装后的 `model/tsfm/<model_name>/` 和 `model_venvs/tsfm/` 不变。

```text
tdengine-tdgpt-enterprise/tsfm/
├── models/
│   └── tdengine-tdgpt-model-moment-1-base.zip
└── <release>/
    ├── tdengine-tdgpt-tsfm-release-<release>.json
    └── tdengine-tdgpt-tsfm-runtime-<version>-<platform>-<arch>.<suffix>
```

schema 2 的模型条目保留 `file`、模型版本、大小和 SHA-256，并增加 `path`。例如：

```json
{
  "kind": "model",
  "name": "moment",
  "version": "1.0.0",
  "file": "tdengine-tdgpt-model-moment-1-base.zip",
  "path": "models/tdengine-tdgpt-model-moment-1-base.zip",
  "size": 421135114,
  "sha256": "63f7a78b608948e513813fa68589314d26e125d786f0c695814ce596fc18cb7d",
  "required_runtime": "1.0.0"
}
```

`path` 只允许 `models/<file>`，相对于清单目录的上一级 TSFM 目录解析。HTTP 下载保持清单所在站点及其 URL 前缀；内网站点失败后，备用站点也使用同一个共享模型路径。runtime 不设置 `path`，继续从清单所在目录获取。离线使用 `resource apply --release <本地清单>` 时，应保持上述目录结构；直接 `resource import --package <归档>` 不依赖下载路径。

schema 1 仍按 `file` 相对于清单目录下载，已发布的 `1.0.0`、`1.0.1` 清单与旧地址不得因目录整理被删除或覆盖。仅支持 schema 1 的旧客户端会拒绝 schema 2 清单，因此必须先交付支持新协议的 TDgpt 程序，再为它发布 schema 2 清单；不能仅修改 NAS 清单就认为现有安装包已支持共享模型。首次获取和显式升级使用同一套路径解析，产品安装或升级仍不会自动下载 TSFM 资源。

TSDB 仓库负责构建制品、资源协议、下载与安装；正式发布的清单转换、NAS staging、Azure 上传、任务轮询、CDN 刷新和发布门禁由 platform-group 统一编排，与 Lite 主 venv 发布保持独立开关。两个仓库通过带版本的制品清单对接，不在客户端代码中嵌入发布环境或上传接口。仓库中原有 `packaging/offline-pkgs/upload_tsfm_release.py` 仅保留历史 schema 1 行为，不用于发布 schema 2 清单。

发布端须将真实模型 ZIP 放到 NAS 的 `/pkgs/release-apex/tdengine-tdgpt-enterprise/tsfm/models/`，runtime 与新清单放到该目录旁的 `<release>/`。Azure Blob 必须保存文件的实际字节，NAS 软链不能代替远端对象。只有全部资源上传并校验完成，才可发布引用它们的新清单；NAS 源站可访问不表示 Azure/CDN 目的站点已可用。

共享模型文件名是不可变的制品标识：同名文件不得替换成不同 SHA-256 的内容，权重变化必须使用新文件名并更新新清单。发布端应校验已有与新上传对象的真实大小和 SHA-256，并串行发布共享模型，避免并发覆盖。runtime 版本变化、权重不变时继续复用同一个模型文件，不重新打模型 ZIP，也不将模型版本改成 runtime 版本。具体发布命令、重试和门禁配置由 platform-group 发布文档维护。

#### 7.2.1 在线与离线发布清单发现（规划）

默认 `resource check` 从随包 `share/tsfm/tsfm-release.json` 读取发布清单，与本地安装状态比较，不联网；显式指定远端版本或 URL 时才请求远端清单。页面打开先显示本地信息，检查更新使用显式 `release=latest`。

新增统一配置 `resource_release`，允许发布通道或固定版本：

```python
# taosanode.config.py
resource_release = "stable"
```

```yaml
environment:
  TDGPT_RESOURCE_RELEASE: "${TDGPT_RESOURCE_RELEASE:-stable}"
```

在需要选择远端通道或固定版本的显式查询中，读取优先级为：API 显式 `release`、`TDGPT_RESOURCE_RELEASE`、`taosanode.config.py` 的 `resource_release`、默认 `stable`。固定为 `1.0.0` 时不得自动切换到新版本，便于验收和回滚；普通 `resource check` 不传版本时仍以随包清单为准，不读取该远端通道。

发布站点使用不可变版本清单和一个稳定通道指针：

```text
tsfm/stable.json
tsfm/1.0.0/tdengine-tdgpt-tsfm-release-1.0.0.json
tsfm/1.0.1/tdengine-tdgpt-tsfm-release-1.0.1.json
```

`stable.json` 只记录当前稳定版本。发布时先上传全部资源包，再上传并验证不可变的版本清单，最后原子更新 `stable.json`。正式发布后不得覆盖同版本清单；资源内容或 SHA-256 变化必须发布新版本。

在线 `check` 按以下顺序处理：

1. 解析配置的通道或固定版本；
2. 依次尝试配置的主地址和公共备用地址；
3. 校验成功后，将通道指针和版本清单原子保存到 `resource/releases/<version>/`；
4. 两个远端地址均失败时，读取同版本的本地缓存；
5. 将清单与本地目录、状态文件和必要文件校验结果比较后返回，不自动下载或升级。

首次离线部署必须携带版本清单、当前平台 runtime 和所需模型包。导入命令应支持显式 `--release-manifest`，校验并保存清单后再导入多个包。以后离线升级只需携带新版本清单和发生变化的资源包。既无网络又无本地清单时，`check` 明确失败；仅凭已解压目录无法得知完整的可部署模型目录。

`check` 成功响应应包含 `configured_release`、`resolved_release`、 `manifest_source`（`remote` 或 `local_cache`）和 `repository_available`。页面始终正常展示资源目录；`repository_available=false` 时禁用在线下载，但已安装模型仍可启动、停止和删除。

资源状态与服务状态必须分开。资源状态规划如下：

| 状态 | 定义 | 页面含义 |
| --- | --- | --- |
| `installed` | 目录完整，状态文件 digest 与清单一致 | 已安装 |
| `missing` | 清单中存在，本机目录不存在 | 未安装 |
| `outdated` | 目录完整且已纳管，但状态文件版本或 digest 与清单不同 | 有更新，旧版本仍可运行 |
| `unmanaged` | 目录存在，但没有有效安装状态，来源无法确认 | 未纳管，需明确替换导入 |
| `invalid` | 已纳管资源缺少必要文件或完整性校验失败 | 资源损坏，禁止启动 |
| `unavailable` | 清单中存在，但没有当前平台匹配的资源 | 当前平台不可用 |

`downloading`、`verifying`、`importing` 和 `failed` 属于任务状态，不应覆盖资源状态；`running` 和 `stopped` 属于服务状态。`outdated` 是正常业务结果，不返回 HTTP 错误，也不触发自动升级。手工保留目录但缺少状态文件应显示 `unmanaged`，手工删除已纳管目录中的必要文件应显示 `invalid`，两者都不能误报为 `outdated`。

以上通道指针、清单缓存、`--release-manifest` 和细分状态为后续实现范围；当前版本仍支持通过 `--release <本地-json路径>` 手工执行离线 `check`。

### 7.2.2 当前可执行的升级操作

升级 Lite 程序或主 venv 时，使用产品安装器并显式提供所需主 venv 包； `resource apply/import` 不管理主 venv。已有模型和共享 TSFM runtime 保留，不会因为产品或主 venv 版本变化自动下载新资源。

升级共享 TSFM runtime 前，先记录正在运行的模型并停止全部模型 worker。使用 `--replace` 显式替换，再校验资源并逐个恢复原先运行的模型。例如 Linux：

```bash
sudo /usr/bin/taosanodectl model status all
sudo /usr/bin/taosanodectl model stop all
sudo /usr/bin/taosanodectl resource import \
  --package "/opt/tdgpt-offline/<新版本的当前平台-runtime-归档>" \
  --expected-sha256 "<发布清单中的归档SHA256>" --replace
sudo /usr/bin/taosanodectl resource verify all
sudo /usr/bin/taosanodectl model start "<原先运行的模型名>"
sudo /usr/bin/taosanodectl model status "<原先运行的模型名>"
```

Windows 使用相同参数，通过第 10.2 节的 `.bat` 入口执行。只升级一个模型时，停止该模型后导入其 ZIP 并指定 `--replace`，共享 runtime 无需同时替换。在线升级也必须显式选择已发布清单；先 `resource check`，再对目标执行 `resource apply <目标> --release <清单> --replace`。共享 runtime 的目标名为 `tsfm_runtime`；不要使用 `all` 代替单个目标，以免安装清单中的全部模型。

即使 Python 依赖版本不变，仅清理文件或改变归档压缩格式也会改变归档 SHA-256。发布新的资源包时必须更新文件名、大小、SHA-256 和对应资源版本，不覆盖已发布的不可变清单。旧清单仍指向旧 gzip 文件时，不能只把下载 URL 后缀改成 `.tar.xz`；须先发布并核验实际 XZ 文件及新清单，再显式切换消费方。

目录校验通过不等于模型可用；还需确认主服务就绪，并完成相关模型的权重加载及预测/补值请求。以下原子切换、整组升级和启动失败自动回滚描述包含目标设计，当前操作不能依赖自动恢复全部模型；应保留旧清单和资源包，失败时停止相关模型，使用旧包显式替换并重新验收。

### 7.2.3 下一版产品与已有动态资源的升级顺序（发布方案）

本轮 Python 3.12.16 升级包括 Lite 主 venv `1.0.4` 和共享 TSFM runtime `1.0.1` 的构建与验收；既有安装的 TSFM 不随产品安装自动升级，须在维护窗口显式替换。模型权重内容未变时继续复用。实际验收状态以本轮 `1.0.1` 归档、包内元数据和 SHA-256 对应的记录为准，不将候选测试等同于正式发布。

产品版本以实际发布登记为准，主 venv、TSFM runtime 和资源 release 独立编号，发布清单必须绑定本轮 runtime `1.0.1` 的真实文件名与摘要，具体候选版本、依赖差异和验收要求见 [构建文档第 7 节](TDGPT-LITE-BUILD-CN.md#7-下一版发布准备与版本决策待执行)。本方案不表示这些制品已经正式发布；本轮代码默认 release 已改为 `1.0.1`，发布产品前须先核验该清单及全部资源可下载，已有资源仍须显式升级。

已动态加载的模型不必因产品版本变化而重新下载权重，但旧 TSFM venv 不会自动获得新版 pyarrow。需要修复 TSFM 依赖问题的安装必须显式升级共享 runtime。升级流程如下：

1. 记录产品、主 venv、Python runtime、TSFM runtime 的版本、包摘要及正在运行的模型；备份配置、用户算法、模型、模型 venv 与资源状态，并保留旧安装介质和旧资源清单。不要仅依赖安装过程中短暂存在的 backup 目录。
2. 在维护窗口停止全部模型 worker 与主服务，确认无遗留进程。先迁移旧算法，再安装新产品并显式提供新版主 venv；Docker 换用已重建依赖的镜像并复用四类持久化挂载。Python runtime 未变化且已验证兼容时复用；不要随意移动旧 TSFM venv 引用的基础 Python 路径。
3. 先启动主服务，验证基本算法、动态模型管理及用户算法。如果暂时保留旧 TSFM runtime，必须完成新程序配旧 runtime 的验收，并明确依赖漏洞尚未因此修复。
4. 确认全部模型仍停止后，按第 7.2.2 节显式升级 `tsfm_runtime`，再逐个检查已有模型。新清单中权重 digest 未变时，针对已安装模型执行 `apply` 应为 `unchanged`，只刷新发布元数据；无需执行 `apply all`，也无需重新训练 `model/dynamic`。
5. 校验资源后只恢复原先运行的模型，逐个检查 readiness，并执行实际预测/补值。主 venv 依赖变化也可能影响 `model/dynamic` 的序列化模型，须进行加载和推理回归，不能将“保留文件”当作“保证兼容”。全部通过后才结束维护窗口。

在线操作使用同一份已发布清单的 URL 或本地文件绝对路径，避免依赖 CLI 默认版本。以下占位符须替换为发布登记中的实际值，不能直接作为发布脚本运行：

```bash
sudo /usr/bin/taosanodectl resource check tsfm_runtime --release "<新发布清单URL或绝对路径>"
sudo /usr/bin/taosanodectl resource apply tsfm_runtime --release "<新发布清单URL或绝对路径>" --replace
sudo /usr/bin/taosanodectl resource check "<已安装模型名>" --release "<同一新发布清单>"
sudo /usr/bin/taosanodectl resource apply "<已安装模型名>" --release "<同一新发布清单>" --replace
sudo /usr/bin/taosanodectl resource verify all
```

Windows 使用 `taosanodectl.bat`，参数相同；Docker 在新容器内执行同一入口。离线升级按第 7.2.2 节导入带正确 SHA-256 的新 runtime 包；权重未变时不需再导入模型 ZIP。若清单对模型的运行时要求发生变化，在线需刷新模型发布元数据，离线需随介质提供相同清单并以本地清单执行模型 `apply`，复用已安装的相同 digest。

现有 `resource_manager.py` 的单模型 `apply` 只选择该模型，不隐式更新 runtime； `runtime_readiness` 检查 runtime 的完整性，但当前没有强制校验清单中的 `required_runtime`。新发布不能只把该字段改成新版本便宣称已完成兼容保护：实施时须补充运行时版本要求的校验、已有模型状态更新和对应回归。共享 runtime 升级必须验证全部已安装官方模型；同一目标目录只有一个激活 runtime，无法让部分模型继续使用旧共享环境。版本要求采用明确的兼容规则，避免新 runtime 错误拒绝经验证兼容的旧模型权重。

任何模型验证失败时停止受影响 worker，以旧清单/归档显式替换旧 TSFM runtime，必要时同时还原程序、主 venv、配置和资源状态，再恢复原运行集合。当前不能依赖整组自动回滚；回滚旧 runtime 也意味着回到旧依赖，须记录修复尚未完成。

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

### 7.6 主机安装的离线导入与页面管理

主机安装模式与 Docker 使用同一资源协议，但没有离线包卷挂载。管理员将与当前操作系统和架构匹配的 TSFM runtime 包及模型 ZIP 放在本机任意可读的临时目录，再通过 `taosanodectl resource import` 导入。导入器只从该路径读取归档，资源实际安装到 TDgpt 数据目录；导入成功后可删除临时归档。

以下保留历史 `1.0.0` 发布清单对应的资源示例，Linux runtime 仍为历史 gzip 文件；不要将其误认为本轮 Lite 主 venv `1.0.4`。新构建的 Linux runtime 使用 XZ，导入时以所选发布清单的实际文件名与 SHA-256 为准。内网目录为 `https://release.tdengine.net/tdengine-tdgpt-enterprise/tsfm/1.0.0/`。以下文件用于旧版介质与显式回滚示例；本轮新介质须按 `1.0.1` 清单取得对应平台的 XZ/ZIP 及 SHA-256；发布清单用于核对版本和 SHA-256，不作为 `resource import --package` 的输入。

| 类型 | 文件 | 使用方式 |
| --- | --- | --- |
| 发布清单 | [tdengine-tdgpt-tsfm-release-1.0.0.json](https://release.tdengine.net/tdengine-tdgpt-enterprise/tsfm/1.0.0/tdengine-tdgpt-tsfm-release-1.0.0.json) | 核对离线介质版本、大小和 SHA-256 |
| Linux x64 runtime | [tdengine-tdgpt-tsfm-runtime-1.0.0-linux-x64.tar.gz](https://release.tdengine.net/tdengine-tdgpt-enterprise/tsfm/1.0.0/tdengine-tdgpt-tsfm-runtime-1.0.0-linux-x64.tar.gz) | 仅 Linux x64 导入一次 |
| Linux arm64 runtime | [tdengine-tdgpt-tsfm-runtime-1.0.0-linux-arm64.tar.gz](https://release.tdengine.net/tdengine-tdgpt-enterprise/tsfm/1.0.0/tdengine-tdgpt-tsfm-runtime-1.0.0-linux-arm64.tar.gz) | 仅 Linux arm64 导入一次 |
| Windows x64 runtime | [tdengine-tdgpt-tsfm-runtime-1.0.0-windows-x64.zip](https://release.tdengine.net/tdengine-tdgpt-enterprise/tsfm/1.0.0/tdengine-tdgpt-tsfm-runtime-1.0.0-windows-x64.zip) | 仅 Windows x64 导入一次 |
| Chronos 模型 | [tdengine-tdgpt-model-chronos-bolt-base.zip](https://release.tdengine.net/tdengine-tdgpt-enterprise/tsfm/1.0.0/tdengine-tdgpt-model-chronos-bolt-base.zip) | 按需导入 |
| Moirai 模型 | [tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip](https://release.tdengine.net/tdengine-tdgpt-enterprise/tsfm/1.0.0/tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip) | 按需导入 |
| Moment 模型 | [tdengine-tdgpt-model-moment-1-base.zip](https://release.tdengine.net/tdengine-tdgpt-enterprise/tsfm/1.0.0/tdengine-tdgpt-model-moment-1-base.zip) | 按需导入 |
| TDTSFM 模型 | [tdengine-tdgpt-model-tdtsfm-v1-0.zip](https://release.tdengine.net/tdengine-tdgpt-enterprise/tsfm/1.0.0/tdengine-tdgpt-model-tdtsfm-v1-0.zip) | 按需导入 |
| Time-MoE 模型 | [tdengine-tdgpt-model-timemoe-200m.zip](https://release.tdengine.net/tdengine-tdgpt-enterprise/tsfm/1.0.0/tdengine-tdgpt-model-timemoe-200m.zip) | 按需导入 |
| TimesFM 模型 | [tdengine-tdgpt-model-timesfm-2-5-200m-pytorch.zip](https://release.tdengine.net/tdengine-tdgpt-enterprise/tsfm/1.0.0/tdengine-tdgpt-model-timesfm-2-5-200m-pytorch.zip) | 按需导入 |

一次命令可导入一个 runtime 和多个模型包。相同 runtime 的重复导入会返回 `unchanged`，无需为每个模型重复导入 runtime。Linux x64 导入 Moment、Moirai 与 Time-MoE 的示例：

```bash
sudo /usr/bin/taosanodectl resource import \
  --package /opt/tdgpt-offline/tdengine-tdgpt-tsfm-runtime-1.0.0-linux-x64.tar.gz \
  --package /opt/tdgpt-offline/tdengine-tdgpt-model-moment-1-base.zip \
  --package /opt/tdgpt-offline/tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip \
  --package /opt/tdgpt-offline/tdengine-tdgpt-model-timemoe-200m.zip
sudo /usr/bin/taosanodectl resource verify all
sudo /usr/bin/taosanodectl resource list --format json
```

Linux arm64 仅将上述 runtime 文件替换为 `tdengine-tdgpt-tsfm-runtime-1.0.0-linux-arm64.tar.gz`；模型 ZIP 不区分架构。 Windows PowerShell 同样可在一次命令中传入多个模型包：

```powershell
& C:\TDengine\taosanode\bin\taosanodectl.bat resource import `
  --package C:\tdgpt-offline\tdengine-tdgpt-tsfm-runtime-1.0.0-windows-x64.zip `
  --package C:\tdgpt-offline\tdengine-tdgpt-model-moment-1-base.zip `
  --package C:\tdgpt-offline\tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip
& C:\TDengine\taosanode\bin\taosanodectl.bat resource verify all
& C:\TDengine\taosanode\bin\taosanodectl.bat resource list --format json
```

导入完成后，IDMP 页面与 Docker 场景一样，调用模型信息、启动、停止、删除和任务状态接口管理本机模型。页面在展示启停按钮前应先查询对应模型信息，以同步本地 `install_status` 和 `service_status`。离线环境不使用页面的 `download` 或 `download-start`；当前 HTTP API 不接收离线包上传，也不接收任意本地文件路径。

主机升级 Lite 时安装器保留动态资源目录，因此无需重新导入已安装资源。升级共享 TSFM runtime 时必须先停止全部使用它的模型；升级单个模型时只停止该模型。离线包本身不应放在 TDgpt 安装目录或动态资源目录中，以免安装、卸载或运维清理误删它们。

## 8. Docker 部署

Lite Docker 镜像只包含 Lite 服务与主运行环境；Full 镜像仍保留预置模型及其运行环境，不能按 Lite 的“无模型”初始状态验收。Lite Docker 部署必须分别挂载以下四个持久化目录，而不是覆盖整个 TDgpt 数据根目录：

```yaml
volumes:
  - ./tdgpt-data/custom:/var/lib/taos/taosanode/custom
  - ./tdgpt-data/model:/var/lib/taos/taosanode/model
  - ./tdgpt-data/model_venvs:/var/lib/taos/taosanode/model_venvs
  - ./tdgpt-data/resource:/var/lib/taos/taosanode/resource
```

`resource` 挂载用于持久化发布缓存、任务状态和模型状态；分别挂载 `custom`、`model`、`model_venvs`、`resource`，不要覆盖整个 TDgpt 数据根目录。这样容器升级不会覆盖模型和模型 venv，也不会因整个数据根目录被挂载而遮蔽镜像或安装器创建的主 venv。容器启动时按第 5 节的校验规则发现可用 TSFM 资源；没有资源时只运行 Lite 主服务。

资源下载地址通过 `TDGPT_RESOURCE_BASE_URL` 注入容器，例如：

```yaml
environment:
  TDGPT_RESOURCE_BASE_URL: "${TDGPT_RESOURCE_BASE_URL:-https://downloads.taosdata.com}"
```

Docker 环境变量在容器创建时确定。修改 Compose 中的 URL 后，必须执行 `docker compose up -d --force-recreate tdengine-tdgpt` 重新创建 TDgpt 容器； `docker compose restart` 不会更新既有容器的环境变量。挂载的 `model`、 `model_venvs` 与 `resource` 数据不受重新创建容器影响。

### 8.1 Docker 离线导入与页面管理

隔离网络中不使用 `resource apply`，而是由运维将与容器架构匹配的 TSFM runtime 包和模型包提供给容器，再执行本地导入。以上三个动态资源挂载是必需项；离线包可以额外作为只读卷挂载，也可以通过 `docker cp` 复制到容器；不要让浏览器或页面直接写入容器资源目录。

```yaml
volumes:
  - ./tdgpt-data/custom:/var/lib/taos/taosanode/custom
  - ./tdgpt-data/model:/var/lib/taos/taosanode/model
  - ./tdgpt-data/model_venvs:/var/lib/taos/taosanode/model_venvs
  - ./tdgpt-data/resource:/var/lib/taos/taosanode/resource
  - <host>/offline-pkgs:/offline-pkgs:ro
```

以 Linux x64 的 Moment 为例：

```bash
docker exec tdengine-tdgpt taosanodectl resource import \
  --package /offline-pkgs/tdengine-tdgpt-tsfm-runtime-1.0.0-linux-x64.tar.gz \
  --package /offline-pkgs/tdengine-tdgpt-model-moment-1-base.zip
docker exec tdengine-tdgpt taosanodectl resource verify moment
docker exec tdengine-tdgpt taosanodectl resource list --format json
```

离线 `resource import` 与在线 `resource apply` 使用相同的本地资源目录、完整性校验、状态文件和模型启动路径。因此导入成功后，后台页面仍可调用现有模型管理接口查询状态、启动、停止和删除模型：

```text
GET  /api/v1/foundation-models/<model_name>
POST /api/v1/foundation-models/start
POST /api/v1/foundation-models/stop
POST /api/v1/foundation-models/delete
GET  /api/v1/foundation-models/<task_id>/status
```

页面在离线导入完成后应先查询对应模型信息再展示启停操作，以同步 `install_status`、`service_status` 与异步任务状态。模型、TSFM runtime 及 `resource` 目录按本节挂载持久化时，重建容器后模型仍可继续由页面启停。

页面的 `download` 与 `download-start` 接口仍是在线下载流程，隔离网络下不可用。当前 HTTP API 不提供离线包上传或按容器路径执行导入的能力；若后续需要页面“一键离线导入”，应由宿主机控制面提供受控的包选择与导入任务，不能由浏览器任意传入容器文件路径。

### 8.2 资源下载地址与外网兜底

资源地址按照以下优先级确定：

```text
API 请求的 resource_base_url
  > TDGPT_RESOURCE_BASE_URL
  > taosanode.config.py 的 resource_base_url
  > https://downloads.taosdata.com
```

主机安装模式通过 `taosanode.config.py` 保存默认地址；Docker 模式通过 `TDGPT_RESOURCE_BASE_URL` 提供默认地址。`taosanalytics/conf.py` 和 `taosanode_service.py` 是读取并合并这些配置的代码，不是额外配置文件。

对于由配置或环境变量提供的 HTTP(S) 基础地址，资源管理器先访问主地址；获取发布清单或下载资源时遇到连接失败、超时、HTTP `404` 或 HTTP `5xx`，会尝试公共备用地址 `https://downloads.taosdata.com`。日志必须记录主地址失败和实际使用的备用地址。显式指定完整发布清单 URL 或本地路径时不启用备用地址，避免调用方要求严格来源时被静默替换。

如果主地址与备用地址均失败，命令返回非零，并同时保留主地址错误。备用地址的清单和资源必须先正式发布，才能作为可用性兜底；未发布资源返回 `404` 属于预期发布状态，不应被误判为代码故障。

## 9. 发布物与兼容绑定

每个平台和架构分别发布：

- TDgpt Lite 安装包：Linux 新包使用 `tar.xz`，Windows 使用 EXE；
- Python runtime：Linux 新构建包使用 `tar.xz`，Windows 使用 ZIP；已发布依赖清单仍可引用历史 gzip；
- Lite 主 venv 离线包：Linux `tar.xz`，Windows ZIP；
- 官方 TSFM venv 资源包：Linux 新构建包使用 `tar.xz`，Windows ZIP；历史 `1.0.0` 清单引用 Linux gzip，本轮默认 `1.0.1` 清单须引用实际 XZ 资源；
- 每个官方 TSFM 模型资源包，所有平台统一使用 ZIP。

Linux 产品安装包直接包含程序目录，不再生成内层 `package.tar.gz` 或 `package.tar.xz`。Lite 镜像的离线 `docker save` 归档使用 XZ，文件名为 `tdengine-tdgpt-enterprise-lite-docker-<产品版本>-linux-<x64|arm64>.tar.xz`；它与产品安装包是不同产物。XZ 是离线归档格式，不改变 registry 的镜像层格式。 Linux 解压目标需提供 `xz`，Python 资源导入解释器需支持 `lzma`；历史 gzip 导入继续兼容。格式变化须同步构建、NAS 文件名、Apex 清单、下载 URL、SHA-256 和验收入口。

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

对外以 `taosanodectl` 作为跨平台统一的稳定接口。它调用 `taosanode_service.py`，屏蔽 Linux 的 systemd/进程模式、Windows 服务模式和 Docker 前台模式的差异。业务脚本和自动化平台应使用该接口；安装器及兼容包装脚本内部仍可调用 `taosanode_service.py`。

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

模型启动的默认 readiness 期限由 `taosanode.config.py` 中的 `timeouts["model_startup"]` 控制，默认值为 300 秒。该值是模型进程创建后到端口就绪的总期限，不是单次 TCP 连接超时；单次 TCP 探测仍为 1 秒。模型进程若在期限内提前退出，启动立即失败并输出模型日志路径及日志尾部。`model start --timeout` 仅覆盖本次命令，不修改配置文件。

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

Linux 安装器当前只创建以下两个模型命令兼容链接；脚本内部调用 `taosanode_service.py` 的 `model-start`、`model-stop`：

```text
/usr/bin/start-model -> <安装目录>/bin/start-model.sh
/usr/bin/stop-model  -> <安装目录>/bin/stop-model.sh
```

Linux 当前不创建 `start-taosanode`、`stop-taosanode`、`status-taosanode` 或 `status-model` 别名；主服务使用 `taosanodectl service`，模型状态使用 `taosanodectl model status`。`start-model.sh`、`stop-model.sh` 保留在包内； `start.sh`、`stop.sh` 以及 `start-tdtsfm.sh`、`stop-tdtsfm.sh`、 `start-time-moe.sh`、`stop-time-moe.sh` 四个单模型包装脚本已移除。

Windows 打包工具在安装目录的 `bin` 下生成 `start-taosanode.bat`、 `stop-taosanode.bat`、`status-taosanode.bat`、`start-model.bat`、 `stop-model.bat` 和 `status-model.bat`，内部直接调用 `taosanode_service.py`。这些兼容包装脚本与 `taosanodectl.bat` 的参数及交互行为并不完全相同：例如 Windows 的模型启停包装脚本省略目标时默认使用 `all`，部分脚本会等待按键。自动化应使用 `taosanodectl.bat service ...` 或 `taosanodectl.bat model ...`，并显式指定模型启停目标。

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

### 10.4 共享 TSFM runtime 升级与 IDMP 调用

共享 TSFM runtime 的升级使用单独的异步维护任务，页面不需要自行逐个停止、替换、再启动模型。此入口只更新 `model_venvs/tsfm/`，不更新基础 Python、Lite 主 venv、模型权重和用户算法；基础 Python 的升级仍由安装器或外部维护工具协调。升级前必须已安装一个完整有效的 runtime；首次安装或已损坏环境的修复继续使用原有 runtime download/import 入口。

#### 10.4.1 查询最新发布版本

页面默认调用 `GET /api/v1/foundation-models/runtime`，读取本地安装状态、升级任务和随包 `share/tsfm/tsfm-release.json`，不联网；按当前平台和架构选择推荐 runtime，返回 `data.target.release`、`data.target.sha256` 和制品信息。Linux、Windows 与 Docker 发布均携带推荐元数据，不包含 TSFM 环境或模型权重；当前源清单为 `packaging/tdengine-tdgpt-tsfm-release-1.0.1.json`，使用 schema 2，引用 runtime `1.0.1` 和原有模型权重。runtime 位于 `tsfm/1.0.1/`，模型位于 `tsfm/models/`，清单 release、runtime 版本和模型版本分别管理。不能把浮动 latest 当作随包清单。元数据随程序更新，不覆盖持久化安装状态或缓存。用户点击检查更新时调用 `?release=latest`：从已配置的 `resource_base_url` 读取 `tdengine-tdgpt-enterprise/tsfm/latest/tdengine-tdgpt-tsfm-release-latest.json`（例如 `{"schema_version":1,"release":"1.0.1"}`），再读取固定版本清单。latest 指针的 schema 1 与资源清单的 schema 2 分开；固定清单读取同时兼容 schema 1 和 schema 2。显式 `?release=1.0.1` 可查询指定远端版本。latest 由正式发布端维护，不扫描目录、不根据文件名猜测，也不随主程序版本自动递增。

`share/tsfm/` 位于 TDgpt 安装目录内，存放程序自带的只读推荐元数据，三平台使用相同的相对布局；它不属于用户配置或 datadir 下的持久化资源。升级 TDgpt 程序会更新该清单，但保留已安装的 TSFM runtime、模型权重与资源状态，不自动下载或替换环境。页面应分别展示本机实际版本、随包推荐版本和显式检查更新后得到的远端目标。旧环境必须有有效的 runtime 清单和 `.tdgpt-install-complete.json` 安装记录才可进入托管升级；手工复制、记录缺失或损坏的环境会返回 `installed_error`，需先核实备份并通过原有资源安装/导入流程处理。

升级随包推荐版本时直接使用本地清单；升级其他版本时读取指定版本的远端清单。目标版本和 SHA-256 固定后，先检查本地缓存中的完整归档，匹配大小和摘要就直接使用，否则从配置的远端地址下载并校验，后续替换和恢复流程相同。本地离线包仍可通过现有 `resource import` 使用；CLI 托管升级也可显式传入本地发布清单路径。当前 HTTP 升级入口使用具体 release 和摘要，不接收浏览器提交的服务器文件路径。

正式发布统一由 platform-group !526 编排，客户端不维护上传实现。发布端应在制品验收完成且所有制品与固定版本清单上传成功后，才显式更新 latest 并刷新对应 CDN；latest 发布能力仍须在 platform-group 接入并验收，不能把 schema 2 资源上传完成等同于 latest 已可用。发布时不得把旧版本重新提升为 latest，也不能将尚未完成平台验收的制品加入该入口。普通重传旧包不更新 latest；程序安装和升级不会发布资源或修改远端 latest。

查询失败、latest 尚未发布或没有当前平台的制品时，保留 `data.installed` 和 `data.task`，同时返回 `data.target=null` 和 `data.target_error`；页面显示“最新版本查询失败”及原因，不得将其显示为“已是最新”。`upgrade_available` 表示当前有效环境与查询目标的归档摘要不同；显式查询旧版本时也可能为 true，因此不能仅据此宣称目标一定比当前更新。页面展示 latest 返回的实际版本，用户确认后提交这一个具体 release 和摘要；升级请求不接受浮动的 `latest`，避免查询后发布渠道变化导致安装目标漂移。查询不下载制品，也不停止模型。

#### 10.4.2 页面调用和升级编排

| 步骤 | 接口 | 页面处理 |
| --- | --- | --- |
| 查询当前状态与随包推荐版本 | `GET /api/v1/foundation-models/runtime` | 本地读取；显示 `data.installed`、`data.target`、`data.target_error` 和 `data.task` |
| 检查更新 | `GET /api/v1/foundation-models/runtime?release=latest` | 访问远端；保存目标的具体 `release`、`sha256`，网络失败不改变本地安装状态 |
| 查询指定版本（可选） | `GET /api/v1/foundation-models/runtime?release=<release>` | 展示明确指定的发布版本；普通页面不需要使用此入口 |
| 发起升级 | `POST /api/v1/foundation-models/runtime/upgrade` | 请求体为 `{"release":"1.0.1","expected_sha256":"<目标 sha256>"}`；HTTP 202 仅表示已受理，保存返回的 `task_id`；重复请求已完成的相同 digest 可返回 HTTP 200，复用原任务和回滚快照 |
| 查询进度 | `GET /api/v1/foundation-models/runtime/upgrades/<task_id>` | 轮询 `data.status`、`data.phase`、`data.progress`、`data.error` 和 `data.rollback_error` |
| 显式恢复 | `POST /api/v1/foundation-models/runtime/upgrades/<task_id>/recover` | 对中断任务恢复旧环境，或对最近一次已完成升级执行回滚；继续轮询同一任务 |

通过 IDMP 的既有代理调用时，在以上地址前加 `/tdgpt`；代理鉴权和权限控制沿用现有模型管理接口。新升级任务使用上述独立轮询接口，不复用下载任务的 SSE `runtime_import` 完成事件，避免环境导入结束、模型尚未重新启动时被页面误判成功。页面关闭只停止查询，不取消后端升级。

任务状态使用 `queued`、`running`、`completed`、`failed` 和 `interrupted`；阶段使用 `queued`、`preparing`、`stopping`、`installing`、`verifying`、`resuming`、`completed`、`unchanged`、`rolling_back`、`rolled_back`、`manual_intervention` 和 `aborted`。只有 `status=completed` 表示升级成功，`phase=unchanged` 表示已安装目标 digest，无须停止模型。`status=failed, phase=rolled_back, rollback_status=completed` 表示升级未成功、旧环境已恢复，应提示升级失败但恢复完成；不能显示升级成功。`status=interrupted, phase=manual_intervention` 表示需要显式恢复，维护门禁继续阻止模型和 runtime 的写操作。停止模型之前中断的任务可以通过 recover 解除门禁，不停止现有模型。

服务端先下载并验证目标归档 SHA-256，在 runtime 同级临时目录解压、重定位并校验内部 manifest、依赖锁和解释器完整版本；目标不兼容时直接失败，不停止现有模型。随后保留并验证旧环境快照，记录升级前正在运行的模型，停止这些 worker，将已准备好的新环境切换到正式路径，再校验安装完整性并重新启动原先运行的模型，确认进程和端口就绪。原先停止的模型保持停止，不在用户升级时运行完整依赖或推理测试。快照位于 `model_venvs/.tsfm-rollback-<task_id>/`，与环境使用同一文件系统；Docker 必须持久化整个 `model_venvs` 父目录和 `resource` 状态目录。运维应为目标归档、新环境及旧快照预留磁盘空间；未完成的准备目录和残缺快照在失败或显式中断恢复时清理，已验证的快照保留用于恢复，不应当作普通下载缓存清理。

整个替换和恢复流程由独立管理进程持有现有生命周期锁，直接调用模型管理方法，不在持锁期间嵌套启动同样需要该锁的 CLI。任务记录保存在 `resource/state/upgrades/`；API 进程重启后仍可查询。若管理进程退出且任务未写终态，查询会将其标记为需要人工恢复，而不是自动重新执行安装。查询只有取得生命周期锁后才会改写中断状态；新排队及重新排队的恢复任务保留 30 秒启动窗口，过期且未执行的任务标记为 `interrupted/manual_intervention`，通过显式 recover 清理并解除门禁。正常下载和安装全程持锁，不受此启动窗口限制；worker 等锁超时只退出，不在锁外改写任务，由后续状态查询处理。恢复只接受最新升级任务，避免用较早快照覆盖后续已经安装的新版本。升级期间普通模型和 runtime 写操作返回冲突，CLI 也受同一维护门禁保护。

已完成任务的显式回滚还要求当前环境摘要仍与该任务目标一致；已自动回滚的任务再恢复时，当前环境必须仍匹配原环境摘要。通过原有 import/apply 等入口独立安装其他环境后，旧任务恢复会拒绝，不能覆盖后装版本。中断任务允许正式环境缺失或不完整，以支持半切换恢复。回滚时若正式路径已经完整匹配旧环境摘要，直接复用旧环境并恢复原运行模型，不再复制整份快照；只有环境确实需要恢复时才复制。历史 `interrupted` 下载记录不作为永久升级阻塞项，正在执行的任务和实际资源操作仍受状态检查与生命周期锁保护。恢复请求若直接完成准备阶段清理，返回 HTTP 200 及终态；只有真正排队的恢复任务返回 HTTP 202。

#### 10.4.3 Python 配套升级、配置兼容与验收边界

Python runtime 的补丁升级需要配套处理。比如 !3585 将构建目标从 Python `3.12.12` 调整为 `3.12.16`，必须重新构建与解释器精确匹配的 Lite 主 venv 和 TSFM runtime，并分别完成三平台验收；不能只替换基础 Python 或在旧 venv 中增量安装依赖。模型权重、用户算法和未受影响的配置保留，但发布验收仍须确认它们可被新环境加载。TSFM 依赖升级与基础 Python 升级应分别登记 release、manifest、依赖锁和验收记录。

仅替换 TSFM runtime 时，主服务可以保持运行；同时替换基础 Python 和主 venv 时，必须由独立于待替换 Python 的安装器或外部维护工具停止主服务及 worker，协调替换和回滚。现有校验要求 Python 完整版本与 manifest 一致，保留旧 TSFM 包不足以支持跨 Python 版本回滚，必须保留匹配的基础 Python、主 venv、主程序及资源状态；本节的 runtime API 不负责这类升级。

升级保留用户现有 `taosanode.config.py`，不能用新版默认配置覆盖端口、模型目录等本地设置。旧配置中的六个内置模型入口可能使用 `tdtsfm-server.py`、`timemoe-server.py`、`moirai-server.py`、`chronos-server.py`、`timesfm-server.py` 或 `moment-server.py`。启动器仅在这些旧入口不存在时回退到对应的 `*_server.py`；已有入口优先，自定义脚本和显式路径不做通用改写，兼容目标不存在时仍明确失败，不改写配置文件本身。

底层 runtime download/apply/import 仍保留，单次操作的临时备份会在激活成功后清除，不能替代升级编排保留的旧环境快照。`resource verify` 校验完成标记、manifest、平台/架构、依赖锁摘要和解释器版本，不执行完整依赖导入、`pip check` 或模型推理。Linux runtime 使用 `tar.xz`（历史 `tar.gz` 按清单兼容），Windows runtime 使用 ZIP，与 Windows 产品 EXE 分开管理。

完整依赖兼容性、各受支持模型的推理、Linux x64/arm64 与 Windows x64 安装，以及 Docker 卷持久化行为，属于正式发布前的测试验收；还应覆盖相同摘要重复请求、并发启停、错误摘要、Python 版本不匹配、停止超时、升级和回滚中断、服务重启及旧版本恢复。PyArrow/glibc 等依赖兼容性是独立发布门禁，升级接口通过回归不代表新制品已通过发布验收，也不代表基础 Python 已支持在线升级。

## 11. 实施阶段

### 第一阶段：官方 TSFM 动态资源（当前进度）

- 将动态模型 venv 路径统一为 `model_venvs`；
- 已实现官方 TSFM venv 与模型资源的离线导入、SHA-256 校验、幂等检查、原子切换和启动前校验；
- 已提供 `taosanodectl resource check/apply/import/list/verify` 统一入口；
- 已将六个官方模型的真实模型标识、版本、发布文件名、归档大小和备注写入发布清单；
- 当前资源操作采用同步 CLI 和全局文件锁，不重复下载相同 digest；
- 已在 Linux x64、Linux arm64 和 Windows x64 完成在线首次导入、第二模型复用 runtime、重复导入、完整性校验、Moirai/Moment 启动与端口验收；

ARM64 的共享 TSFM runtime 发布前必须使用 `patchelf >= 0.14.0` 修复 `jaxlib/libjax_common.so` 的 `librt.so.1` 依赖，将其改为 `libc.so.6`。这是 Moirai 使用的 `clock_gettime@GLIBC_2.17` 符号绑定兼容修复（也曾在 Ubuntu 18.04/glibc 2.27 上验证，不表示继续支持该系统）；系统默认的 `patchelf 0.9` 不得用于构建。发布验收必须直接导入 `jax/jaxlib`，并实际启动 Moirai 检查 6064 端口。

- `resource apply` 支持发布清单、平台/架构筛选、SHA-256 缓存、HTTP Range 断点续传和显式替换。
- platform-group 已支持 Lite 三平台安装包构建、双架构 Docker 构建和发布；动态资源下载到模型 readiness 的验收当前保留为手工发布检查，后续再固化为流水线自动化。
- 用户模型独立 venv/worker 仍属于后续范围，不阻塞本期六个官方 TSFM 模型。

### 第二阶段：用户模型

- 定义用户模型 worker 接口；
- 根据用户 `requirements.txt` 创建独立 venv；
- 支持用户模型资源的导入、更新、回滚和隔离运行；
- 在产品下载器中接入动态资源下载与状态展示。

本文会在动态资源验收自动化和用户模型 worker 实现后继续补充最终错误码与扩展测试矩阵。
