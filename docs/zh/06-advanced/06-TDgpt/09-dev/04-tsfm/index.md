---
title: "部署时序基础模型"
sidebar_label: "部署时序基础模型"
---

由众多研究机构及企业开源时序基础模型极大地简化了时序数据分析的复杂程度，在数据分析算法、机器学习和深度学习模型之外，
提供了一个时间序列数据高级分析的新选择。本章介绍部署并使用开源时序基础模型（Time Series Foundation Model, TSFM）。

TDgpt 在 3.3.6.4 版本原生支持五种类型的时序基础模型：涛思时序基础模型 (TDtsfm v1.0) , time-moe，chronos, moirai, timesfm。
在官方的安装包中，内置了 TDtsfm 和 time-moe 两个时序模型，如果使用其他的模型，需要您在本地部署服务。部署其他时序基础模型服务的文件，位于
`< tdgpt 根目录>/lib/taosanalytics/tsfmservice/` 下，该目录下包含四个文件，分别用于本地部署启动对应的时序基础模型。

| 文件名               | 说明                 |
|-------------------|--------------------|
| timemoe-server.py | 部署启动 time-moe 时序基础模型 |
| chronos-server.py | 部署启动 chronos 时序基础模型 |
| timesfm-server.py | 部署启动 timesfm 时序基础模型 |
| moirai-server.py  | 部署启动 moirai 时序基础模型 |

本章将以支持 time-moe 模型为例，说明如何将一个独立部署的 MaaS 服务整合到 TDgpt 中，并通过 SQL 语句调用其时序数据分析能力。

本章介绍如何本地部署 [Time-MoE](https://github.com/Time-MoE/Time-MoE) 时序基础模型并与 TDgpt 适配后，提供时序数据预测服务。

## 准备环境

为了使用时间序列基础模型，需要在本地部署环境支持其运行。首先需要准备一个虚拟的 Python 环境，使用 `pip` 安装必要的依赖包：

```shell
pip install torch==2.3.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install flask==3.0.3
pip install transformers==4.40.0
pip install accelerate
```

> 脚本中安装了 CPU 驱动版本的 PyTorch，如果您服务是部署在具有 GPU 的服务器上，可以在虚拟环境中安装支持 GPU 加速的 PyTorch。例如：

```shell
pip install torch==2.3.1 -f https://download.pytorch.org/whl/torch_stable.html
```

您可以使用 TDgpt 的虚拟环境，也可以新创建一个虚拟环境，使用该虚拟环境之前，确保正确安装了上述依赖包。

## 设置时序基础模型服务地址

TDgpt 安装根目录下的 `./lib/taosanalytics/time-moe.py` 文件 (3.3.6.4 以后版本使用 ./lib/taosanalytics/tsfmservice/timemoe-service.py) 负责 Time-MoE 模型的部署和服务，
修改文件设置合适的服务 URL。

```python
@app.route('/ds_predict', methods=['POST'])
def time_moe():
    ...
```

修改 `ds_predict` 为需要开启的 URL 服务地址，或者使用默认值亦可。

```Python
    app.run(
            host='0.0.0.0',
            port=5001,
            threaded=True,  
            debug=False     
        )
```

其中的 port 修改为希望开启的端口，包括使用默认值亦可。完成之后重启服务。

# 启动 Python 脚本

```shell
nohup python timemoe-server.py > service_output.out 2>&1 &
```

第一次启动脚本会从 huggingface 自动加载 [0.5 亿参数模型](https://huggingface.co/Maple728/TimeMoE-50M) (`Maple728/TimeMoE-50M`),
如果您需要部署参数规模更大参数规模的版本（`'Maple728/TimeMoE-200M'`）请将 `timemoe-server.py` 文件中 `_model_list[0],`  
修改为 `_model_list[1],` 即可。

如果加载失败，请尝试执行如下命令切换为国内镜像下载模型。

```shell
export HF_ENDPOINT=https://hf-mirror.com
```

然后再次尝试启动服务。

检查 `service_output.out` 文件，有如下输出，则说明加载成功

```text
Running on all addresses (0.0.0.0)
Running on http://127.0.0.1:5001
```

# 检查服务状态

使用 Shell 命令可以验证服务是否正常

```shell
curl 127.0.0.1:5001/ds_predict
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

您可参考 [timemoe.py](https://github.com/taosdata/TDengine/blob/main/tools/tdgpt/taosanalytics/algo/fc/timemoe.py)
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

        # 如果  taosanode.ini 配置文件中没有设置服务 URL 地址，这里使用默认地址
        if  self.service_host is None:
            self.service_host = 'http://127.0.0.1:5001/timemoe'

    def execute(self):
        # 检查是否支持历史协变量分析，如果不支持，触发异常。time-moe 不支持历史协变量分析，因此触发异常
        if len(self.past_dynamic_real):
            raise ValueError("covariate forecast is not supported yet")

        # 调用父类的 execute 方法
        super().execute()
```

将代码添加到 `/usr/local/taos/taosanode/lib/taosanalytics/algo/fc` 目录下。您可以在该路径下找到 `timemoe.py` 的文件，该文件即为系统内置的支持 `Time-MoE` 的适配文件。

TDgpt 已经内置 Time-MoE 模型的支持，能够使用 Time-MoE 的能力进行时序数据预测分析，执行 `show anodes full`，可以看到 Time-MoE 的预测服务 `timemoe-fc`。

## 设置模型服务地址

修改 `/etc/taos/taosanode.ini` 配置文件中[tsfm-service]部分：

```ini
[tsfm-service]
timemoe-fc = http://127.0.0.1:5001/ds_predict
```

添加服务的地址。此时的 `key` 是模型的名称，此时即为 `timemoe-fc`，`value` 是 Time-MoE 本地服务的地址：<http://127.0.0.1:5001/ds_predict。>

然后重启 taosnode 服务，并更新服务端算法缓存列表 `update all anodes`，之后即可通过 SQL 语句调用 Time-MoE 的时间序列数据预测服务。

## SQL 调用基础模型预测能力

```sql
SELECT FORECAST(val, 'algo=timemoe-fc') 
FROM foo;
```

## 部署其他时序基础模型

模型在本地部署服务以后，在 TDgpt 中注册的逻辑相似。只需要修改类名称和模型服务名称 (Key)、设置正确的服务地址即可。如果您想尝试
chronos, timesfm, chronos 时序基础服务，适配文件已经默认提供，3.3.6.4 及之后版本的用户只需要在本地启动相应的服务即可。
部署及及启动方式如下：

### 启动 moirai 服务

为避免依赖库冲突，建议准备干净的 python 虚拟环境，在虚拟环境中安装依赖库。

```shell
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

```shell
nohup python moirai-server.py > service_output.out 2>&1 &
```

检查服务状态的方式同上。

### 启动 chronos 服务

在干净的 python 虚拟环境中安装依赖库。

```shell
pip install torch==2.3.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install chronos-forecasting
pip install flask
```

在 chronos-server.py 文件中设置服务地址，设置加载模型。您也可以使用默认值。

```python

def main():
    app.run(
        host='0.0.0.0',
        port=5002,
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

```shell
nohup python chronos-server.py > service_output.out 2>&1 &
```

### 启动 timesfm 服务

在干净的 python 虚拟环境中安装依赖库。

```shell
pip install torch==2.3.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install timesfm
pip install jax
pip install flask
```

调整 timesfm-server.py 文件中设置服务地址（如果需要）。然后执行下述命令启动服务。

```shell
nohup python timesfm-server.py > service_output.out 2>&1 &
```
