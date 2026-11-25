---
name: tdgpt-custom-algorithm
description: TDgpt 用户自定义算法部署：支持异常检测（ad）、预测（fc）和批量包络线（batch），并可为不符合 TDgpt 类体系的用户模型生成 Adapter。检查 TDgpt 主 venv 依赖、按原生/Docker/SSH 部署到 custom 对应目录、重启服务，并用 curl 验证。触发关键词：TDgpt 自定义算法, 用户算法, custom algorithm, custom anomaly detection, custom forecast, batch envelope, Adapter, 部署自定义算法, TDgpt 算法依赖缺失, custom_algo_dir
metadata:
  short-description: Deploy any supported user-defined algorithm to TDgpt
  author: TDgpt Team
  version: 1.0.0
  owner_team: ai
---

# TDgpt 用户自定义算法部署

把用户自己的 Python 算法注册到 TDgpt。脚本使用 TDgpt 主 venv 运行，按
`custom_algo_dir` 下的类型目录加载，修改后必须重启主服务。

> 本 skill 位于 TDgpt 仓库的 `skills/tdgpt-custom-algorithm/`。下文命令中的
> `<skill-dir>` 指本 SKILL.md 所在目录；agent 应按读取到的 SKILL.md 路径解析。

## 支持的用户算法类型

| 类型 | 目录 | 注册协议 | 发现接口 | 调用接口 |
| --- | --- | --- | --- | --- |
| `ad` 异常检测 | `custom/ad/` | 继承 `AbstractAnomalyDetectionService`，`execute()` 返回标记列表 | `GET /list` | `POST /anomaly-detect` |
| `fc` 预测 | `custom/fc/` | 继承 `AbstractForecastService`，`execute()` 返回结果字典 | `GET /list` | `POST /forecast` |
| `batch` 批量包络线 | `custom/batch/` | 类名 `_X`，实现 `build(ts, values, windows, params)` | `GET /api/v1/analysis/batch/algos` | `POST /api/v1/analysis/batch` |

当前版本中 `ServiceRegistry` 只扫描 `ad/`、`fc/` 作为自定义服务，`batch/` 由独立
发现器加载。插补、相关性、回归、分类、聚类、序列诊断等类别没有用户扩展目录，
不在本 skill 范围内；动态 pkl 模型（`/deploy`）和独立 venv 的用户模型也走各自
入口，不使用本 skill。

## 两种接入方式

先判断用户代码是否已经符合 TDgpt 的类体系：

| 用户代码形态 | 做法 |
| --- | --- |
| 已继承 `AbstractAnomalyDetectionService` / `AbstractForecastService`，或已实现 batch `build()` | 直接用 `--check` 校验后部署 |
| 普通函数、普通类（`predict` / `forecast` / `fit`）、CLI 脚本、第三方模型对象 | **生成 Adapter**：新建入口文件，继承 TDgpt 基类并在内部调用用户代码 |
| 多文件包、模型权重或配置文件 | Adapter + `--asset` 一起部署 |

用户代码不符合类体系是常态，不要要求用户改写模型；由本 skill 生成 Adapter。

## 适用与不适用

适用：

- 用户有一个单变量或单序列算法脚本，希望注册为 `ad`、`fc` 或 `batch` 算法；
- 脚本依赖的第三方包可能不在 TDgpt 主 venv 中，需要先检测再决定是否安装；
- 执行端为 Linux/macOS；目标 TDgpt 是 Linux 原生安装、Docker 容器或可通过
  SSH 到达的 Linux 主机。

不适用：

- 多列联合算法。异常检测和预测框架只把第一列/第一条时间序列传给算法；
- 需要热加载、进程隔离或独立 venv 的用户模型；
- 修改 TDgpt 内置算法（内置算法随程序交付，不能放进 `custom/`）；
- Windows 原生部署。部署脚本依赖 POSIX `sh`、`chmod`、`sha256sum`、`base64`；
  Windows 执行端只支持 `--deploy-mode remote` 管理 Linux 目标，本地和 Docker
  模式会直接被拒绝。

## 必须先向用户收集的信息

| 输入 | 必填 | 说明 |
| --- | --- | --- |
| TDgpt 地址 | 是 | 例如 `http://192.168.1.20:6035`，默认端口 6035 |
| 算法名称 | 是 | 对外算法名，必须与脚本类里的 `name` 完全一致，建议 `^[a-z][a-z0-9_]*$` |
| 算法类型 | 否 | `ad` / `fc` / `batch`；默认从脚本自动识别 |
| 算法脚本路径 | 是 | 用户提供的 `.py` 文件；没有提供时，从 `templates/` 选择模板 |
| 部署形态 | 否 | `local` / `docker` / `remote`；默认自动探测 |
| 容器名 | Docker 时必填 | 自动探测到多个 TDgpt 容器时必须指定 |
| SSH 目标 | remote 时必填 | 例如 `root@192.168.1.20` |
| 主 venv Python | 否 | 默认依次探测 `/var/lib/taos/taosanode/venv/bin/python3` 等路径 |
| `custom_algo_dir` | 否 | 默认从 TDgpt 配置的 `data_dir` 派生 |
| 配置文件 | 否 | 默认 `TDGPT_CONF`、`/etc/taos/taosanode.config.py` 或安装目录 `cfg/taosanode.config.py` |

用户只给了地址和算法名时，先问脚本路径和算法类型；没有脚本时按情况复制模板：

| 类型 | 已符合类体系 | 需要 Adapter |
| --- | --- | --- |
| `ad` | `templates/anomaly_ad_template.py` | `templates/adapter_ad_template.py` |
| `fc` | `templates/forecast_template.py` | `templates/adapter_fc_template.py` |
| `batch` | `templates/batch_env_template.py` | `templates/adapter_batch_template.py` |

用户直接提供模型代码时，优先使用 Adapter 模板，而不是要求用户改写模型。

## 算法契约

所有类型共同的加载规则（`lib/service_registry.py`）：

- 文件放在 `<custom_algo_dir>/<类型>/` 下，扩展名 `.py`，文件名必须是合法 Python
  标识符，不能是 `__init__.py`；
- 算法类名以 `_` 开头，且必须定义在该模块内，不能从其他模块 import 后重导出；
- 类必须能无参实例化（`algo_cls()` 或 `plugin.cls()`）；
- 类属性 `name` 就是对外的算法名，必须等于用户给的算法名。

### `ad` 异常检测

- 继承 `lib.base.AbstractAnomalyDetectionService`；
- `set_input_list()` 后从 `self.list` 读取单列浮点数据，时间戳在 `self.ts_list`；
- `execute()` 返回与输入等长的整数列表，正常点用 `valid_code`（默认 `1`），
  异常点用其他值（建议 `-1`）；
- `option` 里的参数值都是**字符串**，`set_params()` 里必须显式转换
  `int()` / `float()` 并校验范围。

### `fc` 预测

- 继承 `lib.base.AbstractForecastService`（或 `AbstractStatsForecastService`）；
- `set_params()` 必须先调用 `super().set_params(params)`，由基类解析
  `start_ts`、`time_step`、`rows`、`period`、`conf`、`return_conf`；
- 输入数据在 `self.list`，时间戳在 `self.ts_list`；
- `execute()` 返回字典：

  ```python
  {
      "mse": float,
      "model_info": str,
      "res": [timestamps, forecast]            # return_conf=0
      # 或 [timestamps, forecast, lower, upper]  # return_conf=1
  }
  ```

- `timestamps`、`forecast`、`lower`、`upper` 长度都等于 `self.rows`；
- `self.rows` 取值范围 1..1024，服务端通过 `forecast_rows` 传入。

### `batch` 批量包络线

- **不要继承服务基类**；类名以 `_` 开头，属性 `name` 为对外名称；
- 实现 `build(ts, values, windows, params)`，返回 `(center, lower, upper)`；
- 三个返回值等长、可转 `float`、不含 `NaN`/`Inf`；
- 可选 sidecar `<模块名>.json` 声明 `version` 和 `params` 参数校验；
- 每个请求新建实例，`build()` 必须无状态、可重入；
- 参数来自请求的 `config` 对象；没有 sidecar 时不校验，原样传入。

## Adapter：封装不符合 TDgpt 类体系的用户模型

### 何时生成 Adapter

出现以下任一情况就生成 Adapter，不要修改用户模型本身：

- `--check` 报 `no module-level class subclassing ...`；
- 用户代码是普通函数、普通类（`predict` / `forecast` / `fit`）、CLI 脚本或第三方
  模型对象；
- 用户代码有多个文件、模型权重或配置文件。

### 目录布局

```text
work/
├── my_algo.py            # 入口：定义 _MyAlgo(Abstract...Service)
└── my_algo_impl/         # 用户代码，保持原样
    ├── __init__.py
    ├── user_model.py
    └── ...
```

- 入口文件名使用算法名，例如 `my_algo.py`；
- Adapter 类必须定义在入口文件内，加载器要求 `algo_cls.__module__` 与模块名一致；
- 用户代码放在 `<算法名>_impl/` 子包里，入口通过相对导入调用：
  `from .my_algo_impl.user_model import load_model`；
- 入口和 asset 内的辅助模块必须用**相对导入**；`import my_algo_impl` 这类绝对
  导入在服务加载时会失败，因为加载器把入口作为隔离子模块导入，
  `custom_algo_dir` 不在 `sys.path`。预检会直接拒绝这种写法；
- 不要把辅助代码放成 `ad/`、`fc/` 顶层的散装 `.py` 文件：加载器会把这些文件
  也当作算法导入，类名以 `_` 开头且带 `name` 时可能被误注册。

### 部署 Adapter

```bash
python3 "<skill-dir>/scripts/deploy_custom_algorithm.py" \
  --tdgpt-url "http://192.168.1.20:6035" \
  --model-name "my_algo" \
  --algorithm-type ad \
  --script ./work/my_algo.py \
  --asset ./work/my_algo_impl \
  --check
```

`--asset` 可重复，支持文件和目录；目录会递归复制，自动跳过 `__pycache__`、
`.pyc`、`.pyo`。依赖检查会同时扫描入口和所有 asset 中的 Python 文件。

### Adapter 必须做的事

| 事项 | 要求 |
| --- | --- |
| 导入方式 | 辅助模块使用相对导入；绝对导入会在加载器的隔离包中失败 |
| 输入转换 | 把 `self.list`（浮点列表）转成用户模型需要的数组、DataFrame 或张量 |
| 参数转换 | `ad`/`fc` 的 `option` 是字符串，显式 `int()`/`float()`；`batch` 的 `config` 是对象 |
| 输出转换 | 转成 TDgpt 协议：`ad` 标记列表、`fc` 结果字典、`batch` 三条等长曲线 |
| 模型加载 | 首次 `execute()`/`build()` 时懒加载，使用 `threading.Lock` 防并发重复加载 |
| 异常处理 | 抛出带上下文的 `ValueError`，服务会把它放进响应 `msg` |
| 无状态 | 不要在模块顶层读取文件、起线程、连接数据库或训练模型 |

### 各类型的转换点

- `ad`：用户模型返回布尔、0-1 或分数时，明确阈值并转成 `1`（正常）/ `-1`
  （异常），长度必须等于输入；
- `fc`：用户模型返回的预测点数必须等于 `self.rows`；`return_conf=1` 时补齐
  `lower`/`upper`，没有置信区间就退化为 `forecast` 本身；
- `batch`：用户函数返回的 `center`/`lower`/`upper` 必须等长且不含 `NaN`/`Inf`。

### 模型文件

模型权重、配置、词表等小文件可以用 `--asset` 一起部署；大文件建议放在目标机
固定目录，Adapter 里用绝对路径或环境变量读取，不要放进 `custom/` 参与升级迁移。

## 部署流程

所有命令在仓库根目录执行。脚本只依赖 Python 标准库，但执行端和目标端需要
POSIX shell/coreutils；Windows 原生目标不支持，Windows 执行端只能通过
`--deploy-mode remote` 管理 Linux 目标。

### 步骤 1：预检（不写文件、不重启）

```bash
python3 "<skill-dir>/scripts/deploy_custom_algorithm.py" \
  --tdgpt-url "http://192.168.1.20:6035" \
  --model-name "my_algo" \
  --script /path/to/my_algo.py \
  --algorithm-type auto \
  --asset /path/to/my_algo_impl \
  --check
```

`--asset` 只在 Adapter 或多文件模型时需要，可重复。

`--check` 会：

1. 请求 `GET /status` 确认地址可达；
2. 解析目标运行环境：主 venv Python、配置文件、`custom_algo_dir`；
3. 识别算法类型（或校验显式指定的类型），静态校验类名、基类/协议、`name`、
   无参构造、文件名；
4. 检查 Docker 部署位置是否为持久化挂载；
5. 校验 `--asset` 路径和名称冲突；
6. 用**目标主 venv** 检查入口和所有 asset 中 import 的第三方包是否可导入；只把
   TDgpt 安装根目录加入 `sys.path`（供 `import lib`），不加入 `custom_dir` 或
   本地工作区路径；探测会剥掉执行端 cwd，因此结果不依赖运行目录。加载器不会把
   用户算法目录放进 `sys.path`；
7. 绝对导入的本地辅助模块只有在**目标 venv 中不可导入**时才拒绝，提示改用相对
   导入 + `--asset`；已安装包和标准库不会被误判。

退出码：`0` 预检通过；`2` 缺少依赖；`3` 预检失败（地址、路径、契约或挂载错误）。

### 步骤 2：处理缺失依赖（必须由用户确认）

脚本检测到缺失依赖时不会自动安装，会打印缺失模块、对应 pip 包和安装命令，
然后以退出码 `2` 结束。此时：

1. 把缺失包和安装命令原样展示给用户；
2. 明确说明依赖必须装进 **TDgpt 主 venv**，不能装进 TSFM 模型 venv；
3. 等用户确认后再二选一：
   - 用户手动执行打印出的命令；或
   - 让本脚本代为安装（**仅在用户明确同意后**）：

```bash
python3 "<skill-dir>/scripts/deploy_custom_algorithm.py" \
  --tdgpt-url "http://192.168.1.20:6035" \
  --model-name "my_algo" \
  --script /path/to/my_algo.py \
  --check --install-missing --yes
```

不要跳过用户确认直接加 `--install-missing`。安装完成后重新执行 `--check`。

### 步骤 3：部署文件（不重启）

```bash
python3 "<skill-dir>/scripts/deploy_custom_algorithm.py" \
  --tdgpt-url "http://192.168.1.20:6035" \
  --model-name "my_algo" \
  --script /path/to/my_algo.py \
  --algorithm-type auto \
  --asset /path/to/my_algo_impl \
  --restart none
```

脚本会：

- 按类型创建 `<custom_algo_dir>/ad|fc|batch/`，目录权限 `0750`；
- 目标文件存在且内容不同时，先备份到 `<custom_algo_dir>/.deploy-backups/`；
- 通过临时文件 + `mv` 原子替换，文件权限 `0640`；
- 拒绝覆盖符号链接；
- 回读 SHA-256 校验；
- 递归部署 `--asset` 文件和目录，覆盖前先备份，单个文件 `0640`；
- 打印类型、目标路径、asset 路径和重启命令。

### 步骤 4：重启服务

| 部署形态 | 重启方式 |
| --- | --- |
| 原生 systemd | `sudo systemctl restart taosanoded`（部分版本是 `taosanode`） |
| 原生手工 | `taosanodectl service stop` 后 `taosanodectl service start` |
| Docker / 镜像 | `docker restart <容器名>`，**必须先让用户确认** |
| SSH 远程 | 在目标机执行上述原生命令 |

也可以由脚本执行重启，但执行前必须得到用户确认：

```bash
# 原生
... --restart systemd

# Docker / 镜像
... --restart docker --container <容器名>
```

Docker 部署必须在部署完成后重启容器，否则发现接口看不到新算法。

### 步骤 5：验证

```bash
python3 "<skill-dir>/scripts/deploy_custom_algorithm.py" \
  --tdgpt-url "http://192.168.1.20:6035" \
  --model-name "my_algo" \
  --script /path/to/my_algo.py \
  --algorithm-type auto \
  --verify-only
```

验证按类型执行：

**`ad`** — `GET /list` 中出现 `type=anomaly-detection` 且 `builtins=false`，然后：

```bash
curl -s -X POST http://192.168.1.20:6035/anomaly-detect \
  -H 'Content-Type: application/json' \
  -d '{
    "schema": [["ts", "TIMESTAMP", 8], ["val", "DOUBLE", 8]],
    "data": [[1577808000000, 1577808001000, "..."],
             [10.0, 10.1, "...", 40.0, 10.2, "..."]],
    "rows": 60,
    "algo": "my_algo",
    "wncheck": 0
  }'
```

**`fc`** — `GET /list` 中出现 `type=forecast` 且 `builtins=false`，然后：

```bash
curl -s -X POST http://192.168.1.20:6035/forecast \
  -H 'Content-Type: application/json' \
  -d '{
    "schema": [["ts", "TIMESTAMP", 8], ["val", "DOUBLE", 8]],
    "data": [[1577808000000, 1577808001000, "..."],
             [10.0, 10.1, "..."]],
    "rows": 80,
    "algo": "my_algo",
    "forecast_rows": 5,
    "start": 1577808087000,
    "every": 1000,
    "conf": 0.95,
    "return_conf": 1,
    "prec": "ms",
    "wncheck": 0
  }'
```

**`batch`** — `GET /api/v1/analysis/batch/algos` 中出现该名称且 `builtin=false`，然后：

```bash
curl -s -X POST http://192.168.1.20:6035/api/v1/analysis/batch \
  -H 'Content-Type: application/json' \
  -d '{
    "algo": "my_algo",
    "ts": [1577808000000, 1577808001000, "..."],
    "data": [10.0, 10.1, "..."],
    "window": [[1577808000000, 1577808059000]],
    "prec": "ms",
    "protocol": 1.0,
    "config": {}
  }'
```

成功判定：

- HTTP 200；
- 发现接口包含该算法名，且不是内置算法；
- 调用接口没有 `msg`，且 `rows != -1`；
- `ad` 的 `rows` 是异常窗口数，`fc` 的 `rows` 是预测点数，`batch` 的 `rows`
  是输出曲线长度；
- `ad` 返回 `rows == 0` 表示调用成功但没有异常窗口，需要换 `--option` 或验证数据。

可用参数：

- `--option "window=20,threshold=4"`：`ad` / `fc` 的 `option` 字符串；
- `--config-json '{"factor":3}'`：`batch` 的 `config` 对象；
- `--values "1,2,..."`：替换默认验证数据；
- `--sample-file data.json`：自定义验证负载或纯数值列表；
- `--forecast-rows 10`：`fc` 验证时的预测点数。

### 步骤 6：输出部署报告

报告至少包含：

- TDgpt 地址、部署形态、容器名或 SSH 目标；
- 算法类型、算法名称；
- 主 venv Python、配置文件、`custom_algo_dir`；
- 部署文件实际路径（Docker bind mount 时是宿主机路径）、大小、SHA-256、
  是否有备份；
- 部署的 asset 文件和目录清单；
- 缺失依赖的处理结果；
- 是否重启以及重启命令；
- 发现接口与调用接口的验证结论和原始 `rows` / `msg`；
- 回滚命令。

## Docker 必须写进持久化挂载

镜像 / 容器部署不能把算法只写进容器可写层。容器重建（`docker rm` 后重新
`docker run`，或 compose 重新部署）会用镜像内容覆盖可写层，算法随之丢失。

部署脚本会先执行 `docker inspect` 检查 `<custom_algo_dir>` 对应的挂载：

| 挂载类型 | 部署位置 | 说明 |
| --- | --- | --- |
| bind mount | **宿主机挂载源目录** | 脚本解析 `Source` + 相对路径，直接写宿主机目录；容器重建后仍保留 |
| named volume | 容器内路径 | 通过 `docker exec` 写入；数据保存在 volume，重建容器时只要挂载同一个 volume 就不丢 |
| 没有挂载 | 拒绝部署 | 脚本以退出码 `3` 结束，并给出 `docker run -v <宿主目录>:<容器目录>` 示例 |

推荐做法是 bind mount，例如：

```bash
docker run -d --name tdgpt \
  -v /data/tdgpt-custom:/var/lib/taos/taosanode/custom \
  -p 6035:6035 <tdgpt-image>
```

此时 `custom_algo_dir` 保持容器内路径 `/var/lib/taos/taosanode/custom`，脚本会实际
写入宿主机的 `/data/tdgpt-custom/ad|fc|batch/`。不要只用 `docker cp` 把文件复制
进容器，容器重建后同样会丢失。

`--allow-ephemeral` 只用于一次性验证：它允许在没有任何挂载时把算法写进容器
可写层，并明确提示容器重建后会丢失。

## 回滚

```bash
# 删除算法文件和 Adapter 辅助目录（保留备份）
rm -f /var/lib/taos/taosanode/custom/<类型>/my_algo.py
rm -rf /var/lib/taos/taosanode/custom/<类型>/my_algo_impl

# 或恢复备份
cp -p /var/lib/taos/taosanode/custom/.deploy-backups/my_algo.<时间戳>.py \
      /var/lib/taos/taosanode/custom/<类型>/my_algo.py

# 重启后生效
sudo systemctl restart taosanoded
# Docker: docker restart <容器名>
```

## 排查

| 现象 | 原因与处理 |
| --- | --- |
| `--check` 报 `main venv python not found` | 用 `--venv-python` 指定主 venv 解释器 |
| `--check` 报算法类型无法识别 | 显式传 `--algorithm-type ad|fc|batch`；用户代码不符合类体系时生成 Adapter |
| `--check` 报 `no module-level class subclassing ...` | 需要 Adapter，不要修改用户模型；用 `templates/adapter_*_template.py` |
| `--check` 报 `absolute import of a bundled helper` | 该名字在目标 venv 中不可导入，且来自本地目录/asset；改为 `from .my_algo_impl...` 并用 `--asset` 部署。若是已安装包，先确认主 venv 中可导入 |
| Adapter 运行时 `ModuleNotFoundError` | 辅助模块没随 `--asset` 部署，或入口用了绝对导入 |
| 辅助模块里的类被注册成算法 | 把辅助代码放进 `<算法名>_impl/` 子包，不要放在 `ad/`、`fc/` 顶层 |
| 每次请求都重新加载模型 | 在 `_load_model()` 中缓存实例，并用 `threading.Lock` 防止并发重复加载 |
| `custom_dir` 在 `lib/` 或程序目录下 | 服务会拒绝加载；改用 `<data_dir>/custom` 或显式 `--custom-dir` 指向持久目录 |
| 发现接口中没有新算法 | 未重启；类名不以 `_` 开头；类不是模块内定义；`name` 不匹配；模块 import 失败 |
| 服务日志 `failed to import module <name>` | 依赖缺失或脚本语法错误；在主 venv 中执行脚本 import 复现 |
| `{"rows": -1, "msg": "..."}` | 算法执行异常；`msg` 通常包含参数或数据问题 |
| `specified model not found` | 算法未注册，先确认发现接口，再查日志 |
| `fc` 报 `params are missing, start_ts, time_step, rows` | 请求缺少 `forecast_rows` / `start` / `every`，或自定义 `set_params()` 没调 `super()` |
| `fc` 报 `result length should greater than or equal to 2` | `execute()` 返回的 `res` 至少要有时间戳和预测两条列表 |
| `batch` 报 `unknown batch algorithm` | 文件不在 `batch/` 顶层、类名不以 `_` 开头、`name` 缺失或 `build()` 不可调用 |
| 依赖已装仍报缺失 | 装到了 TSFM venv 或其他 Python；必须装进主 venv |
| 容器重建后算法消失 | 算法只写进了可写层。给 `custom_algo_dir` 加 bind mount，重新部署 |
| 修改脚本后行为不变 | 服务不会热加载；必须重启全部 worker |
| Windows 上运行报 `sh: not found` 或缺少 `chmod`/`sha256sum`/`base64` | 脚本是 POSIX 实现；在 Linux/macOS 上执行，或用 `--deploy-mode remote` 从 Windows 管理 Linux 目标 |

关键日志位置：Linux 默认 `/var/log/taos/taosanode/taosanode.app.log`；容器内同路径。
可检索 `load algorithm:`、`failed to import module`、`failed to register service`。

## 约束

- 只使用主 venv；不要修改 TSFM venv、`model/`、`model_venvs/`、`resource/`。
- 不要创建软链接；安装迁移遇到链接会中止。
- 不要改 `custom/__init__.py`，除非确实需要跨算法共享函数；它抛异常会让整个
  `custom/` 加载失败。
- 不要在算法文件里写顶层副作用（读文件、起线程、连接数据库）；模块会被直接
  import。
- 不要把算法放进 TDgpt 程序 `lib/` 目录，升级会删除。
- 不要用 `systemctl restart taosanode` 替代容器重启；镜像部署必须重启容器。
- `ad`/`fc` 的参数来自 `option` 字符串，必须显式类型转换；`batch` 的参数来自
  `config` 对象。
- 用户代码不符合 TDgpt 类体系时，生成 Adapter 包装，不要改写用户模型；
  Adapter 类必须定义在入口文件内，辅助代码放在 `<算法名>_impl/` 子包并通过
  `--asset` 部署。
- 部署脚本不自动安装依赖，除非用户明确同意并传入 `--install-missing --yes`。
