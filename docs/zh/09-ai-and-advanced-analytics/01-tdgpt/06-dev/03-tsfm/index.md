---
title: 部署时序基础模型
sidebar_label: 部署时序基础模型
---

研究机构与企业开源的时序基础模型，降低了时序数据分析的复杂度，在统计分析、机器学习与深度学习模型之外提供了另一类可选能力。本章介绍如何部署并使用开源时序基础模型（Time Series Foundation Model，TSFM）。

自 `v3.3.6.4` 起，TDgpt 已陆续支持六种时序基础模型：涛思时序基础模型（TDtsfm v1.0）、Time-MoE、Chronos、Moirai、TimesFM、Moment。
安装包中内置了 TDtsfm 和 Time-MoE 两个时序模型；若使用其他模型，需要在本地部署对应服务。第三方模型服务脚本位于 `<tdgpt 根目录>/lib/taosanalytics/tsfmservice/`。下表列出该目录下各适配文件及能力（TDtsfm 由安装包内置服务提供，不在此目录以独立 `*-server.py` 形式列出）。未勾选项可能是模型本身不支持，也可能是尚未适配。

<table>
<tr><th rowspan="2">模型</th> <th rowspan="2">文件</th> <th colspan="3">模型说明</th><th colspan="5">功能说明</th></tr>
<tr><th>名称</th><th>参数 (亿)</th><th>大小 (MiB)</th><th>单变量预测</th><th>协变量预测</th><th>多变量预测</th><th>异常检测</th><th>补值</th></tr>
<tr><th rowspan="2">timemoe</th><th rowspan="2">timemoe-server.py</th><th>Maple728/TimeMoE-50M</th><th>0.50</th><th align="right">227</th><th rowspan="2">✔</th><th rowspan="2">✘</th><th rowspan="2">✘</th><th rowspan="2">✘</th><th rowspan="2">✘</th></tr>
<tr><th>Maple728/TimeMoE-200M</th><th>4.53</th><th align="right">906</th></tr>
<tr><th rowspan="2">moirai</th><th rowspan="2">moirai-server.py</th><th>Salesforce/moirai-moe-1.0-R-small</th><th>1.17</th><th align="right">469</th><th rowspan="2">✔</th><th rowspan="2">✔</th><th rowspan="2">✘</th><th rowspan="2">✘</th><th rowspan="2">✘</th></tr>
<tr><th>Salesforce/moirai-moe-1.0-R-base</th><th>9.35</th><th align="right">3,740</th></tr>
<tr><th rowspan="4">chronos</th><th rowspan="4">chronos-server.py</th><th>amazon/chronos-bolt-tiny</th><th>0.09</th><th align="right">35</th><th rowspan="4">✔</th><th rowspan="4">✘</th><th rowspan="4">✘</th><th rowspan="4">✘</th><th rowspan="4">✘</th></tr>
<tr><th>amazon/chronos-bolt-mini</th><th>0.21</th><th align="right">85</th></tr>
<tr><th>amazon/chronos-bolt-small</th><th>0.48</th><th align="right">191</th></tr>
<tr><th>amazon/chronos-bolt-base</th><th>2.05</th><th align="right">821</th></tr>
<tr><th>timesfm</th><th>timesfm-server.py</th><th>google/timesfm-2.0-500m-pytorch</th><th>4.99</th><th align="right">2,000</th><th>✔</th><th>✘</th><th>✘</th><th>✘</th><th>✘</th></tr>
<tr><th rowspan="3">moment</th><th rowspan="3">moment-server.py</th><th>AutonLab/MOMENT-1-small</th><th>0.38</th><th align="right">152</th><th rowspan="3">✘</th><th rowspan="3">✘</th><th rowspan="3">✘</th><th rowspan="3">✘</th><th rowspan="3">✔</th></tr>
<tr><th>AutonLab/MOMENT-1-base</th><th>1.13</th><th align="right">454</th></tr>
<tr><th>AutonLab/MOMENT-1-large</th><th>3.46</th><th align="right">1,039</th></tr>
</table>

本章将以支持 time-moe 模型为例，说明如何将一个独立部署的 MaaS 服务整合到 TDgpt 中，并通过 SQL 语句调用其时序数据分析能力。

下面介绍如何本地部署 [Time-MoE](https://github.com/Time-MoE/Time-MoE) 时序基础模型并与 TDgpt 适配后，提供时序数据预测服务。

## 准备环境

为了使用时间序列基础模型，需要在本地部署环境支持其运行。首先需要准备一个虚拟的 Python 环境，使用 `pip` 安装必要的依赖包：

```bash
pip install torch==2.3.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install flask==3.0.3
pip install transformers==4.40.0
pip install accelerate
```

> 脚本中安装了 CPU 驱动版本的 PyTorch，如果服务部署在具有 GPU 的服务器上，可以在虚拟环境中安装支持 GPU 加速的 PyTorch。例如：

```bash
pip install torch==2.3.1 -f https://download.pytorch.org/whl/torch_stable.html
```

你可以使用 TDgpt 的虚拟环境，也可以新创建一个虚拟环境，使用该虚拟环境之前，确保正确安装了上述依赖包。

## 设置时序基础模型服务地址

TDgpt 安装根目录下的 `./lib/taosanalytics/tsfmservice/timemoe-server.py`（早期版本曾使用 `time-moe.py`）负责 Time-MoE 模型的部署和服务，
修改文件设置合适的服务 URL。

```python
@app.route('/ds_predict', methods=['POST'])
def time_moe():
    #...
```

修改 `host` 参数为需要开启的 URL 服务地址，或者使用默认值亦可。

```Python
    app.run(
            host='0.0.0.0',
            port=6062,
            threaded=True,  
            debug=False     
        )
```

其中的 port 修改为希望开启的端口，使用默认值亦可。完成之后重启服务即可。

# 启动 Python 脚本

⚠️ NOTE：如下启动服务的方式只针对 `v3.3.8.0` 之前的版本有效；如果你使用的是 `v3.3.8.0` 及之后的版本，请参考 [动态下载时序模型](#动态下载时序模型)

```bash
nohup python timemoe-server.py > service_output.out 2>&1 &
```

第一次启动脚本会从 huggingface 自动加载 [0.5 亿参数模型](https://huggingface.co/Maple728/TimeMoE-50M) (`Maple728/TimeMoE-50M`),
如果你需要部署参数规模更大的版本（`'Maple728/TimeMoE-200M'`）请将 `timemoe-server.py` 文件中 `_model_list[0],`  
修改为 `_model_list[1],` 即可。

如果加载失败，请尝试执行如下命令切换为国内镜像下载模型。

```bash
export HF_ENDPOINT=https://hf-mirror.com
```

然后再次尝试启动服务。

检查 `service_output.out` 文件，有如下输出，则说明加载成功

```text
Running on all addresses (0.0.0.0)
Running on http://127.0.0.1:6062
```

# 检查服务状态

使用 Shell 命令可以验证服务是否正常

```bash
curl 127.0.0.1:6062/ds_predict
```

如果看到如下返回信息表明服务正常，自此部署 Time-MoE 完成。

```html
<!doctype html>
<html lang=en>
<title>405 Method Not Allowed</title>
<h1>Method Not Allowed</h1>
<p>The method is not allowed for the requested URL.</p>
```

# 添加模型适配代码

你可参考 [timemoe.py](https://github.com/taosdata/TDengine/blob/main/tools/tdgpt/taosanalytics/algo/fc/timemoe.py)
文件进行 MaaS 服务的适配。我们适配 Time-MoE 提供预测服务。

```python
# 所有的时序基础模型服务类均是从 TsfmBaseService 继承而来

class _TimeMOEService(TsfmBaseService):
    # 模型名称，用户可根据需求定义，该名称也是后续调用该模型的 key
    name = 'timemoe-fc'

    # 说明信息
    desc = ("Time-MoE: Billion-Scale Time Series Foundation Models with Mixture of Experts; "
            "Ref. to https://github.com/Time-MoE/Time-MoE")

    def __init__(self):
        super().__init__()

        # 如果配置文件中没有设置服务 URL 地址，这里使用默认地址
        if  self.service_host is None:
            self.service_host = 'http://127.0.0.1:6062/ds_predict'

    def execute(self):
        # 检查是否支持历史协变量分析，如果不支持，触发异常。time-moe 不支持历史协变量分析，因此触发异常
        if len(self.past_dynamic_real):
            raise ValueError("covariate forecast is not supported yet")

        # 调用父类的 execute 方法
        super().execute()
```

将代码添加到 `/usr/local/taos/taosanode/lib/taosanalytics/algo/fc` 目录下。你可以在该路径下找到 `timemoe.py` 的文件，该文件即为系统内置的支持 `Time-MoE` 的适配文件。

TDgpt 已经内置 Time-MoE 模型的支持，能够使用 Time-MoE 的能力进行时序数据预测分析，执行 `show anodes full`，可以看到 Time-MoE 的预测服务 `timemoe-fc`。

## 设置模型服务地址

修改 `/etc/taos/taosanode.config.py` 中模型服务地址（早期版本曾使用 `taosanode.ini` 的 `[tsfm-service]` 段）：

```python
timemoe_fc = 'http://127.0.0.1:6062/ds_predict'
```

添加服务的地址。此时的 `key` 是模型的名称，此时即为 `timemoe-fc`，`value` 是 Time-MoE 本地服务的地址：`http://127.0.0.1:6062/ds_predict`。

然后重启 taosnode 服务，并更新服务端算法缓存列表 `update all anodes`，之后即可通过 SQL 语句调用 Time-MoE 的时间序列数据预测服务。

## SQL 调用基础模型预测能力

```sql
SELECT FORECAST(val, 'algo=timemoe-fc') 
FROM foo;
```

## 部署其他时序基础模型

模型在本地部署服务以后，在 TDgpt 中注册的逻辑相似。只需要修改类名称和模型服务名称 (Key)、设置正确的服务地址即可。如果你想尝试
Chronos、TimesFM 等时序基础服务，适配文件已经默认提供，`v3.3.6.4` 及之后版本的用户只需要在本地启动相应的服务即可。
部署及启动方式如下：

### 启动 moirai 服务

为避免依赖库冲突，建议准备干净的 python 虚拟环境，在虚拟环境中安装依赖库。

```bash
pip install torch==2.3.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install uni2ts
pip install flask
```

在 moirai-server.py 文件中配置服务地址（配置服务地址方式见上），设置加载的模型（如果需要）。

```python
_model_list = [
    'Salesforce/moirai-moe-1.0-R-small',  # small model with 117M parameters
    'Salesforce/moirai-moe-1.0-R-base',   # base model with 205M parameters
]

pretrained_model = MoiraiMoEModule.from_pretrained(
    _model_list[0]   # 默认加载 small 模型，改成 1 即加载 base 模型
).to(device)
```

执行命令启动服务，首次启动会自动下载模型文件，如果下载速度太慢，可使用国内镜像（设置置方式见上）。

```bash
nohup python moirai-server.py > service_output.out 2>&1 &
```

检查服务状态的方式同上。

### 启动 chronos 服务

在干净的 python 虚拟环境中安装依赖库。

```bash
pip install torch==2.3.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install chronos-forecasting
pip install flask
```

在 chronos-server.py 文件中设置服务地址，设置加载模型。你也可以使用默认值。

```python

def main():
    app.run(
        host='0.0.0.0',
        port=6063,
        threaded=True,
        debug=False
    )
```

```python
_model_list = [
    'amazon/chronos-bolt-tiny',  # 9M parameters,   based on t5-efficient-tiny
    'amazon/chronos-bolt-mini',  # 21M parameters,  based on t5-efficient-mini
    'amazon/chronos-bolt-small', # 48M parameters,  based on t5-efficient-small
    'amazon/chronos-bolt-base',  # 205M parameters, based on t5-efficient-base
]

model = BaseChronosPipeline.from_pretrained(
    _model_list[0],   #  默认加载 tiny 模型，修改数值就可以调整加载启动的模型
    device_map=device,
    torch_dtype=torch.bfloat16,
)
```

在 shell 中执行命令，启动服务。

```bash
nohup python chronos-server.py > service_output.out 2>&1 &
```

### 启动 timesfm 服务

在干净的 python 虚拟环境中安装依赖库。

```bash
pip install torch==2.3.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install timesfm
pip install jax
pip install flask==3.0.3
```

调整 timesfm-server.py 文件中设置服务地址（如果需要）。然后执行下述命令启动服务。

```bash
nohup python timesfm-server.py > service_output.out 2>&1 &
```

### 启动 moment 服务

在干净的 python 虚拟环境中安装依赖库。

```bash
pip install torch==2.3.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install transformers==4.33.3
pip install numpy==1.25.2
pip install matplotlib
pip install pandas==1.5
pip install scikit-learn
pip install flask==3.0.3
pip install momentfm
```

调整 moment-server.py 文件中设置服务地址（如果需要）。然后执行下述命令启动服务。

```bash
nohup python moment-server.py > service_output.out 2>&1 &
```

## 时序模型服务启动和停止脚本

为便于启停时序基础模型服务，自 `v3.4.0.0` 起提供统一的启动脚本 `start-model.sh` 与停止脚本 `stop-model.sh`，用于一键启动或停止指定或全部时序基础模型服务。

### 启动脚本

`start-model.sh` 用于启动指定或全部时序基础模型服务。当前脚本本身只保留 Linux 入口命令，内部会统一委托 `taosanode_service.py` 读取 `taosanode.config.py`，再根据模型名称选择对应的 Python 虚拟环境并启动模型服务。入口脚本固定使用安装目录下主 `venv` 的 Python，不再回退到系统 `PATH`。

使用 `root` 安装完成后，可在 `<tdgpt根目录>/bin/` 目录下找到该脚本；安装过程通常会创建软链接 `/usr/bin/start-model`，便于全局调用。

默认日志输出到 `/var/log/taos/taosanode/` 目录下的 `taosanode_service_<model_name>.log` 文件中。

**用法说明**：

```bash
用法：/usr/bin/start-model [-c 配置文件] [模型名|all]

支持的模型名：tdtsfm, timesfm, timemoe, moirai, chronos, moment
```

**选项说明**：

```bash
  -c 配置文件    指定配置文件（默认：`<install_dir>/cfg/taosanode.config.py`，找不到时回退 `/etc/taos/taosanode.config.py`）
  -h, --help     显示本帮助信息
```

入口脚本不接受模型名以外的额外位置参数；模型规模或本地路径等高级参数请参阅下文「动态下载时序模型」。

**使用示例说明**：

1. 在后台启动全部的模型服务：`/usr/bin/start-model all`
2. 单独启动某个模型服务，例如：`/usr/bin/start-model timesfm`
3. 支持通过 `-c` 参数指定自定义配置文件，未指定时默认优先使用 `<install_dir>/cfg/taosanode.config.py`，找不到时回退 `/etc/taos/taosanode.config.py`，例如：`/usr/bin/start-model -c /path/to/custom_taosanode.config.py`

### 停止脚本

`stop-model.sh` 用于一键停止指定或全部时序基础模型服务。当前脚本同样保留为 Linux 入口命令，内部统一委托 `taosanode_service.py` 执行停止逻辑，使用方式与启动脚本一致，便于批量运维。

**使用示例说明**：

1. 停止 timesfm 服务，`/usr/bin/stop-model timesfm`
2. 一键停止全部模型服务：`/usr/bin/stop-model all`

## 动态下载时序模型

自 `v3.3.8.0` 起，你可以在启动模型时指定不同规模的模型。直接运行 `[xxx]-server.py` 且不指定参数时，通常按 `model_index=0` 加载列表中的较小模型；通过 `start-model` 或配置文件启动时，以 `taosanode.config.py` 中各模型的 `default_model` 为准（例如 Time-MoE 默认为 `Maple728/TimeMoE-200M`，Chronos 默认为 `amazon/chronos-bolt-base`，MOMENT 默认为 `AutonLab/MOMENT-1-base`）。
此外，如果你手动在本地下载了模型文件，可以通过指定本地模型文件路径的方式，运行已经下载完成的模型。

```bash
# 运行在本地目录 /var/lib/taos/taosanode/model/chronos 的 chronos-bolt-tiny 模型文件，如果指定目录不存在，则自动下载模型文件到 /var/lib/taos/taosanode/model/chronos 目录下。第三个参数表示下载模型文件时是否打开镜像，推荐国内用户打开该选项
python chronos-server.py /var/lib/taos/taosanode/model/chronos/ amazon/chronos-bolt-tiny True

```

## 时序基础模型 transformers 版本要求

| 模型名称        | transformers 版本 |
|-------------|----------------------|
| time-moe、moirai、tdtsfm      | 4.40 |
| chronos    | 4.55                                 |
| moment   | 4.33                            |
| timesfm        | N/A                               |
