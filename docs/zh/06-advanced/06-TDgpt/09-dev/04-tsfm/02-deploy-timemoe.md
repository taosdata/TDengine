---
title: "部署 Time-MoE 模型"
sidebar_label: "部署 Time-MoE 模型"
---

本章介绍如何本地部署 [Time-MoE] (https://github.com/Time-MoE/Time-MoE) 时序基础模型并与 TDgpt 适配完成后，提供时序数据预测服务。

# 准备环境

为了使用时间序列基础模型，需要在本地部署环境支持其运行。首先需要准备 Python 环境。使用 PiPy 安装必要的依赖包：

```shell
pip install torch==2.4.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install flask==3.0.3
pip install transformers==4.40.0
pip install accelerate
```
您可以使用安装 TDgpt 过程中自动创建的虚拟环境，也可以创建一个独立虚拟环境，使用该虚拟环境之前，确保安装了上述的依赖包。

# 设置服务端口和地址

TDgpt 安装根目录下的 `./lib/taosanalytics/time-moe.py` 文件负责 Time-MoE 模型的部署和服务，修改文件设置合适的服务 URL。

```Python
@app.route('/ds_predict', methods=['POST'])
def time_moe():
...
```
修改 `ds_predict` 为需要开启的 URL 服务地址，或者使用默认值即可。

```Python
    app.run(
            host='0.0.0.0',
            port=5001,
            threaded=True,  
            debug=False     
        )
```
其中的 port 修改为希望开启的端口，重启脚本即可。

# 启动部署 Python 脚本

```shell
nohup python time-moe.py > service_output.out 2>&1 &
```

第一次启动脚本会从 huggingface 自动加载[2亿参数模型](https://huggingface.co/Maple728/TimeMoE-200M)。该模型是 Time-MoE 200M参数版本，如果您需要部署参数规模更小的版本请将 `time-moe.py` 文件中 `'Maple728/TimeMoE-200M'` 修改为 `Maple728/TimeMoE-50M`，此时将加载 [0.5亿参数模型](https://huggingface.co/Maple728/TimeMoE-50M)。

如果加载失败，请尝试执行如下命令切换为国内镜像下载模型。

```shell
export HF_ENDPOINT=https://hf-mirror.com
```

再执行脚本：
```shell
nohup python time-moe.py > service_output.out 2>&1 &
```

检查 `service_output.out` 文件，有如下输出，则说明加载成功
```shell
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

# 参考文献

- Time-MoE: Billion-Scale Time Series Foundation Models with Mixture of Experts. [[paper](https://arxiv.org/abs/2409.16040)] [[GitHub Repo](https://github.com/Time-MoE/Time-MoE)]
