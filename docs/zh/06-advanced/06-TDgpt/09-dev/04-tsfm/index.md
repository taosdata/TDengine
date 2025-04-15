---
title: "部署时序基础模型"
sidebar_label: "部署时序基础模型"
---

由众多研究机构及企业开源时序基础模型极大地简化了时序数据分析的复杂程度，在数据分析算法、机器学习和深度学习模型之外，
提供了一个时间序列数据高级分析的新选择。本章介绍部署并使用开源时序基础模型（Time Series Foundation Model, TSFM）。

TDgpt 已经内置原生支持了两个时序基础模型涛思时序基础模型 (TDtsfm v1.0) 和 Time-MoE。但是越来越多的开源或商业时序模型需要
用户按需将其整合到 TDgpt 中，本章将以支持 Time-MoE 模型为例，说明如何将一个独立部署的 MaaS 服务整合到 TDgpt 中，
并通过 SQL 语句调用其时序数据分析能力。

本章介绍如何本地部署 [Time-MoE](https://github.com/Time-MoE/Time-MoE) 时序基础模型并与 TDgpt 适配后，提供时序数据预测服务。

## 准备环境

为了使用时间序列基础模型，需要在本地部署环境支持其运行。首先需要准备 Python 环境，使用 `pip` 安装必要的依赖包：

```shell
pip install torch==2.4.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install flask==3.0.3
pip install transformers==4.40.0
pip install accelerate
```
您可以使用 TDgpt 的虚拟环境，也可以新创建一个虚拟环境，使用该虚拟环境之前，确保正确安装了上述依赖包。


## 设置本地时序基础模型服务地址

TDgpt 安装根目录下的 `./lib/taosanalytics/time-moe.py` 文件负责 Time-MoE 模型的部署和服务，修改文件设置合适的服务 URL。

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

# 启动部署 Python 脚本

```shell
nohup python time-moe.py > service_output.out 2>&1 &
```

第一次启动脚本会从 huggingface 自动加载 [2 亿参数模型](https://huggingface.co/Maple728/TimeMoE-200M)。该模型是 Time-MoE 200M 参数版本，如果您需要部署参数规模更小的版本请将 `time-moe.py` 文件中 `'Maple728/TimeMoE-200M'` 修改为 `Maple728/TimeMoE-50M`，此时将加载 [0.5 亿参数模型](https://huggingface.co/Maple728/TimeMoE-50M)。

如果加载失败，请尝试执行如下命令切换为国内镜像下载模型。

```shell
export HF_ENDPOINT=https://hf-mirror.com
```

再执行脚本：
```shell
nohup python time-moe.py > service_output.out 2>&1 &
```

检查 `service_output.out` 文件，有如下输出，则说明加载成功
```text
Running on all addresses (0.0.0.0)
Running on http://127.0.0.1:5001
```

# 验证服务是否正常

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
class _TimeMOEService(AbstractForecastService):
    # 模型名称，用户可根据需求定义，该名称也是后续调用该模型的 key
    name = 'timemoe-fc'

    # 说明信息
    desc = ("Time-MoE: Billion-Scale Time Series Foundation Models with Mixture of Experts; "
            "Ref to https://github.com/Time-MoE/Time-MoE")

    def __init__(self):
        super().__init__()

        self.table_name = None

        # 读取 taosanode.ini 配置文件中的该模型对应的服务的地址信息，如果未读到，使用默认地址，用户可根据需求确定
        service_host = conf.get_tsfm_service("timemoe-fc")
        if  service_host is not None:
            self.service_host = service_host
        else:
            self.service_host = 'http://127.0.0.1:5001/timemoe'

        self.headers = {'Content-Type': 'application/json'}


    def execute(self):
        """分析主程序方法"""
        if self.list is None or len(self.list) < self.period:
            raise ValueError("number of input data is less than the periods")

        if self.rows <= 0:
            raise ValueError("fc rows is not specified yet")

        # let's request the gpt service
        data = {"input": self.list, 'next_len': self.rows}
        try:
            # 请求时序基础模型服务
            response = requests.post(self.service_host, data=json.dumps(data), headers=self.headers)
        except Exception as e:
            app_logger.log_inst.error(f"failed to connect the service: {self.service_host} ", str(e))
            raise e

         # 检查返回值
        if response.status_code == 404:
            app_logger.log_inst.error(f"failed to connect the service: {self.service_host} ")
            raise ValueError("invalid host url")
        elif response.status_code != 200:
            app_logger.log_inst.error(f"failed to request the service: {self.service_host}, reason: {response.text}")
            raise ValueError(f"failed to request the service, {response.text}")

        pred_y = response.json()['output']

        res =  {
            "res": [pred_y]
        }

        # 按照约定要求，组装预测分析结果
        insert_ts_list(res["res"], self.start_ts, self.time_step, self.rows)
        return res
```

将代码添加到 `/usr/local/taos/taosanode/lib/taosanalytics/algo/fc` 路径下。您可以在该路径下找到 `timemoe.py` 的文件，该文件即为系统内置的支持 `Time-MoE` 的适配文件。

TDgpt 默认已经内置了 Time-MoE 模型的支持，能够使用 Time-MoE 的能力进行时序数据预测分析，执行 `show anodes full`，可以看到 Time-MoE 的预测服务 `timemoe-fc`。

## 设置模型服务地址

修改 `/etc/taos/taosanode.ini` 配置文件中[tsfm-service]部分：

```ini
[tsfm-service]
timemoe-fc = http://127.0.0.1:5001/ds_predict
```

添加服务的地址。此时的 `key` 是模型的名称，此时即为 `timemoe-fc`，`value` 是 Time-MoE 本地服务的地址：http://127.0.0.1:5001/ds_predict。

然后重启 taosnode 服务，并更新服务端算法缓存列表 `update all anodes`，之后即可通过 SQL 语句调用 Time-MoE 的时间序列数据预测服务。

## SQL 调用基础模型预测能力
```sql
SELECT FORECAST(i32, 'algo=timemoe-fc') 
FROM foo;
```

## 添加其他开源时序基础模型
模型在本地部署服务以后，在 TDgpt 中注册的逻辑相似。只需要修改类名称和模型服务名称 (Key)、设置正确的服务地址即可。


## 参考文献

- Time-MoE: Billion-Scale Time Series Foundation Models with Mixture of Experts. [[paper](https://arxiv.org/abs/2409.16040)] [[GitHub Repo](https://github.com/Time-MoE/Time-MoE)]
