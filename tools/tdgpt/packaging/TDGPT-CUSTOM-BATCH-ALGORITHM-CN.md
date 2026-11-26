# 开发用户自定义黄金包络线算法

本文说明如何为 TDgpt 的批量包络线接口编写一个用户算法，包括放置位置、插件协议、参数声明、调用方式和常见错误。

## 1. 适用场景

批量包络线接口接收一条时序曲线和若干时间窗口，为每个窗口计算一条中心线与上下界，再把结果汇总成黄金包络线。内置实现是固定流水线：按窗口切分、Hampel 滤波、导数检查、归一化、Savitzky-Golay 平滑、按分位数或均值方差聚合。

当内置流水线的某个环节不满足业务需要时，可以编写自定义算法整体替换这条流水线。适合的场景：

- 聚合方式不同，例如按业务阈值而不是分位数确定上下界；
- 平滑策略不同，例如改用中值滤波或样条；
- 需要业务参数，例如最小批次数、允许的离群比例、衰减系数。

不适合的场景：需要在同一进程内热更新算法代码。算法文件修改后需重启主服务才生效，详见第 9 节。

## 2. 放置位置

用户算法放在 `custom_algo_dir` 下的 `batch/` 目录：

| 平台 | 默认路径 |
| --- | --- |
| Linux / Docker | `/var/lib/taos/taosanode/custom/batch/` |
| Windows | `<安装目录>/data/custom/batch/` |

`custom_algo_dir` 由配置派生，默认是 `<data_dir>/custom`。该目录由安装器创建，随升级保留，不需要手工创建；显式修改配置时须使用绝对路径，且不能与程序 `lib/`、主 venv、`model/`、`model_venvs/`、`resource/` 等受管目录重叠。

目录约定：

- 每个算法是一个顶层 `.py` 文件，文件名即模块名；
- 可选同名 sidecar `<模块名>.json` 声明版本与参数；
- 辅助代码放在子目录里作为包，通过相对导入使用，不会被当作算法扫描。

```text
<custom_algo_dir>/batch/
├── percentile_env.py        # 算法入口
├── percentile_env.json      # 可选，参数与版本声明
└── percentile_helpers/      # 可选，辅助包
    └── __init__.py
```

## 3. 最小示例

`/var/lib/taos/taosanode/custom/batch/my_env.py`：

```python
import numpy as np


class _MyEnv:
    name = "my_env"
    description = "按固定偏移构造上下界"

    def build(self, ts, values, windows, params):
        center = np.asarray(values, dtype=float)
        offset = params.get("offset", 1.0)
        return center, center - offset, center + offset
```

重启服务后即可调用，此时没有参数声明，请求中的 `config` 会原样传给 `build()`。

## 4. 插件协议

加载器在 `batch/` 下扫描一层 `.py` 文件，导入后找出**类名以 `_` 开头**且**定义在该模块内**的类，逐个尝试注册。

必需成员：

| 成员 | 要求 |
| --- | --- |
| 类名 | 以 `_` 开头，例如 `_MyEnv`；不以 `_` 开头的类不会被注册 |
| `name` | 非空字符串，对外算法名，请求里通过 `algo` 指定 |
| `build(ts, values, windows, params)` | 可调用对象，返回 `(center, lower, upper)` |

可选成员：`description`（字符串，sidecar 中的 `description` 优先）、`param_spec`（参数声明，见第 6 节）。

`build()` 的返回契约：

- 三个返回值各自是一维数值序列，可转换为 `float`；
- 三者等长；
- 不含 `NaN` 与 `Inf`。

长度不必与输入等长，内置流水线也会丢弃点数不足的窗口。返回值的 `lower` 长度决定响应中的 `rows`。

实例化语义：**每个请求新建一次插件实例**（内部执行 `plugin.cls()`），因此 `build()` 应当无状态、可重入。服务可能以多线程 worker 运行，同一进程内会并发处理多个请求，不应在实例属性或模块级变量中保存请求相关状态。

## 5. 输入数据

调用方保证以下不变量，算法可以直接依赖：

| 输入 | 保证 |
| --- | --- |
| `ts` | `numpy` 数组，时间戳数值，严格升序，与 `values` 等长 |
| `values` | `numpy` 数组，浮点，不含 `NaN`/`Inf` |
| 点数 | 10 到 40000 |
| `windows` | 由 `(start, end)` 组成的时间戳区间列表，最多 1000 项 |

超出上述范围时请求在进入插件之前即被拒绝。`windows` 原样传入，不做切分；内置实现通过 `split_by_windows()` 自行筛选区间内的点，自定义算法若需要同样的切分，可自行按 `(ts >= start) & (ts <= end)` 处理。

## 6. 参数声明

没有声明时，请求中的 `config` 原样作为 `params` 传给 `build()`，不做任何校验。声明之后，`config` 按声明校验、补默认值，再作为 `params` 传入，未知键会被拒绝。

声明可以写在 sidecar `<模块名>.json`，或类属性 `param_spec`，两者都是 `{参数名: 描述}` 的对象，sidecar 优先。

```json
{
  "version": "1.0.0",
  "description": "按分位数构造上下界",
  "params": {
    "lower_pct": {"type": "number", "minimum": 0, "maximum": 100, "default": 10},
    "upper_pct": {"type": "number", "minimum": 0, "maximum": 100, "default": 90},
    "method": {"type": "string", "enum": ["linear", "nearest"], "default": "linear"},
    "min_batches": {"type": "integer", "minimum": 1, "required": true}
  }
}
```

支持的描述键：

| 键 | 说明 |
| --- | --- |
| `type` | `number`、`integer`、`boolean`、`string`、`array` |
| `minimum` / `maximum` | 数值上下界，仅对 `number`、`integer` 生效；必须是数字，且 `minimum` 不得大于 `maximum` |
| `enum` | 允许取值的列表 |
| `default` | 缺省值，必须是可 JSON 序列化的值，且满足该参数自身的声明；未提供且未标 `required` 时该参数不出现在 `params` 中 |
| `required` | 必填；缺省且无默认值时请求被拒绝 |
| `description` | 参数说明，出现在发现接口的返回中 |

`version` 用于结果追踪；`entry_sha256` 若填写，会与算法文件的实际摘要比对，不一致时记录告警并采用实测值。声明本身写错（例如未知键、不支持的 `type`、`enum` 不是列表、`minimum`/`maximum` 写成字符串或上下界颠倒、`default` 不满足自身声明）时记录错误并**关闭校验**，算法本身仍会加载。

注意 `config` 字段在两种算法下含义不同：

- `algo` 为内置 `golden` 时，`config` 是流水线配置，形如 `{"hampel": {...}, "golden": {...}}`；
- `algo` 为用户算法时，`config` 是扁平参数表，形如 `{"lower_pct": 5}`。

## 7. 多文件算法

辅助代码放在子目录中作为包，入口文件通过相对导入使用：

```text
/var/lib/taos/taosanode/custom/batch/
├── quantile_env.py
├── quantile_env.json
└── quantile_impl/
    ├── __init__.py
    └── core.py
```

```python
# quantile_env.py
from .quantile_impl.core import compute_band


class _QuantileEnv:
    name = "quantile_env"

    def build(self, ts, values, windows, params):
        return compute_band(values, params)
```

两条约束：

- 入口必须是 `batch/` 下的顶层 `.py` 文件，扫描不会进入子目录，因此包内定义的算法类不会被注册；
- 入口文件名不能与包目录同名。Python 解析模块名时优先匹配包目录，同名时 `.py` 文件不可达。

根目录下的 `custom/__init__.py` 会被执行一次，可在其中放置跨类别共用的函数。该文件抛异常时，整个 `custom/` 下的用户算法都不会加载，内置算法不受影响。

## 8. 调用与验证

算法列表（只返回名字、版本与参数声明，不返回文件路径）：

```bash
curl -s http://127.0.0.1:6035/api/v1/analysis/batch/algos | python3 -m json.tool
```

返回中 `golden` 的 `builtin` 为 `true`，其配置以 `config` 字段给出；用户算法的参数声明在 `params` 字段。

调用自定义算法：

```bash
curl -s -X POST http://127.0.0.1:6035/api/v1/analysis/batch \
  -H 'Content-Type: application/json' \
  -d '{
    "algo": "quantile_env",
    "ts":   [1,2,3,4,5,6,7,8,9,10,11,12],
    "data": [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,1.1,1.2],
    "window": [[1, 12]],
    "config": {"lower_pct": 5, "upper_pct": 95, "method": "linear", "min_batches": 1}
  }' | python3 -m json.tool
```

成功响应：

```json
{
  "rows": 12,
  "center": [0.1, 0.2, "..."],
  "lower": [0.05, 0.15, "..."],
  "upper": [0.15, 0.25, "..."],
  "algo": "quantile_env",
  "version": "1.0.0",
  "entry_sha256": "3f5c..."
}
```

`algo`、`version`、`entry_sha256` 用于标识产出这条包络线的代码版本。未在 sidecar 中声明 `version` 时该字段为 `null`。

`algo` 省略时使用内置 `golden`，与改造前的行为一致。

## 9. 生效方式

算法文件、sidecar、辅助包的任何修改都需要**重启主服务**后生效。本期不做热加载：服务以预加载方式启动多个 worker，单个进程内重新导入模块只会让该进程切换到新代码，其余 worker 仍是旧代码，会出现同一批请求命中不同版本的结果。

重启方式按部署形态选择：systemd 部署执行 `systemctl restart taosanoded`；手工方式执行 `taosanodectl service stop` 后 `taosanodectl service start`；Windows 通过服务管理器或控制器的 start/stop 完成。

## 10. 错误与排查

接口沿用既有约定：请求被拒绝或算法执行失败时返回 `{"msg": "...", "rows": -1}`，HTTP 状态码仍为 200，调用方以 `rows == -1` 判定失败。`msg` 中会带上算法名、参数名或失败原因。

| 现象 | 原因 |
| --- | --- |
| `unknown batch algorithm 'x', available: golden, ...` | 算法未注册：文件名不是 `batch/` 下的顶层 `.py`、类名未以 `_` 开头、`name` 缺失或 `build()` 不可调用 |
| 算法未出现在发现接口 | 同上；另需检查服务日志中该模块的导入错误 |
| `unknown parameter(s) for algorithm 'x'` | 请求 `config` 中有 sidecar 未声明的键 |
| `parameter 'x' of algorithm 'y' must be ...` | 类型、枚举或范围校验失败 |
| `missing required parameter 'x'` | 必填参数未提供且无默认值 |
| `center, lower and upper must have the same length` | 三个返回值长度不一致 |
| `'center' contains NaN or infinite values` | 返回了 `NaN` 或 `Inf` |
| `batch algorithm 'x' failed: ...` | `build()` 抛出异常，详情见日志中的算法名与版本 |
| 与内置或其他用户算法重名 | 该算法被跳过；`golden` 为内置保留名，同名插件不会生效 |

服务日志中可检索的关键字：`load batch algorithm:`（成功）、`failed to import batch algorithm module`（导入失败）、`conflicts with an existing algorithm`（重名）、`ignoring sidecar`（声明文件有问题）。

## 11. 约束与注意事项

- **不要继承服务基类**。`ad/`、`fc/` 插件使用的服务基类带有 `type` 属性，那是 `ServiceRegistry` 的成员契约；批量包络线插件注册在独立表中，不参与 `/list` 输出。误继承会被记录一条告警，插件仍可用，但容易引起误解。
- **依赖装在主 venv**。用户算法使用主服务解释器，第三方库需提前装入主 venv，服务启动时不会执行 `pip install`。官方模型的 TSFM venv 与主 venv 相互独立。
- **目录权限**。`batch/` 下是会被导入并执行的代码，属主应为服务的运行用户，目录 `0750`、文件 `0640` 即可。`custom/`、`custom/ad`、`custom/fc`、`custom/classification`、`custom/regression`、`custom/batch` 中任一目录为 world-writable 时，服务会在启动日志中告警；Windows 上不检查该权限（Python 为目录合成的 `st_mode` 恒为 `0777`，与真实 ACL 无关）。
- **不要使用符号链接**。安装迁移遇到链接文件会中止并提示手工处理，升级路径上容易留下未迁移的算法；请使用实体文件。
- **Docker 部署需挂载**。容器内路径必须与挂载目标一致，否则容器重建后算法丢失；也可用 `TDGPT_CUSTOM_ALGO_DIR` 覆盖运行时目录，但该变量不影响安装器迁移。
