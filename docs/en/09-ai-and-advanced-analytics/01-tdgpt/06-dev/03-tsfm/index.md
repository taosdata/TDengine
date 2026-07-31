---
title: Deploy a Time-Series Foundation Model
sidebar_label: Deploy a Time-Series Foundation Model
description: Deploy and connect time-series foundation models to TDgpt
---

A number of research institutions and enterprises have released open-source time-series foundation models (TSFMs), greatly simplifying time-series data analysis. Beyond traditional data analysis algorithms, machine learning, and deep learning models, TSFMs offer a new and powerful option for advanced time-series analytics.

Since `v3.3.6.4`, TDgpt has added support for six time-series foundation models: TDtsfm v1.0, Time-MoE, Chronos, Moirai, TimesFM, and Moment.

### Deployment Details

Installation packages include TDtsfm and Time-MoE. Other models require a local service. Third-party model service scripts are located in `<TDgpt_root_directory>/lib/taosanalytics/tsfmservice/`. TDtsfm is provided by the installation package and does not appear there as a separate `*-server.py` file.

TDgpt has been adapted to interface with specific features of these models. If a certain function is unavailable, it may be due to a limitation of the model itself or because TDgpt has not yet been adapted to support that specific feature for that model.

<table>
<tr><th rowspan="2">Models</th> <th rowspan="2">Files</th> <th colspan="3">Note</th><th colspan="5">Functions Description</th></tr>
<tr><th>Name</th><th>Parameters (100M)</th><th>Model Size(MiB)</th><th>Forecast</th><th>Covariate Forecast</th><th>Multivariate Forecast</th><th>Anomaly Detection</th><th>Imputation</th></tr>
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

This document describes how to integrate an independent TSFM service into TDengine, using [Time-MoE](https://github.com/Time-MoE/Time-MoE) as an example, and how to use the model in SQL statements for time-series forecasting.

## Prepare Your Environment

Before using TSFMs, prepare your environment as follows. Install a Python environment and use PiPy to install dependencies:

```shell
pip install torch==2.3.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install flask==3.0.3
pip install transformers==4.40.0
pip install accelerate
```

You can use the virtual Python environment installed by TDgpt or a separate environment.

## Configure TSFM Service Path & Port

The `lib/taosanalytics/tsfmservice/timemoe-server.py` file in the TDgpt root directory deploys and serves Time-MoE. Earlier versions used `time-moe.py`.

```python
@app.route('/ds_predict', methods=['POST'])
def time_moe():
...
```

Change `ds_predict` to the URL that you want to use in your environment.

```python
    app.run(
            host='0.0.0.0',
            port=6062,
            threaded=True,  
            debug=False     
        )
```

In this section, you can update the port if desired. After you have set your URL and port number, restart the service.

## Run the Python Script (Versions Before v3.3.8.0)

> The following method applies only to versions before `v3.3.8.0`. For later versions, see [Dynamic Model Download](#dynamic-model-download).

```shell
nohup python timemoe-server.py > service_output.out 2>&1 &
```

The script initially downloads [TimeMoE-50M](https://huggingface.co/Maple728/TimeMoE-50M) from Hugging Face. To use `Maple728/TimeMoE-200M`, select index `1` in `_model_list`. Users in China can set `HF_ENDPOINT=https://hf-mirror.com` if the download fails.

Check the `service-output.out` file to confirm that the model has been loaded:

```shell
Running on all addresses (0.0.0.0)
Running on http://127.0.0.1:6062
```

## Verify the Service

Verify that the service is running normally:

```shell
curl 127.0.0.1:6062/ds_predict
```

The following indicates that Time-MoE has been deployed:

```html
<!doctype html>
<html lang=en>
<title>405 Method Not Allowed</title>
<h1>Method Not Allowed</h1>
<p>The method is not allowed for the requested URL.</p>
```

## Load the Model into TDgpt

You can modify the  [timemoe.py](https://github.com/taosdata/TDengine/blob/main/tools/tdgpt/taosanalytics/algo/fc/timemoe.py) file and use it in TDgpt. In this example, Time-MoE is adapted to provide forecasting.

```python
class _TimeMOEService(TsfmBaseService):
    # model name, user-defined, used as model key
    name = 'timemoe-fc'

    # description
    desc = ("Time-MoE: Billion-Scale Time Series Foundation Models with Mixture of Experts; "
            "Ref to https://github.com/Time-MoE/Time-MoE")

    def __init__(self):
        super().__init__()

        # Use the default address if the service URL is not specified in taosanode.config.py.
        if  self.service_host is None:
            self.service_host = 'http://127.0.0.1:6062/ds_predict'

    def execute(self):
        # Verify support for past covariate analysis; raise an exception if unsupported. (Note: time-moe lacks this support and will trigger the exception.)
        if len(self.past_dynamic_real):
            raise ValueError("covariate forecast is not supported yet")

        super().execute()
```

Add your code to `/usr/local/taos/taosanode/lib/taosanalytics/algo/fc/`. Actually, you can find a `timemoe.py` file that we have already prepared already.

TDgpt has built-in support for Time-MoE. You can run `SHOW ANODES FULL` and see that forecasting based on Time-MoE is listed as `timemoe-fc`.

## Configure TSFM Service Path

Set the model service URL in `/etc/taos/taosanode.config.py`. Earlier versions used the `[tsfm-service]` section of `taosanode.ini`.

```python
timemoe_fc = 'http://127.0.0.1:6062/ds_predict'
```

Add the path for your deployment. The key is the name of the model defined in your Python code, and the value is the URL of Time-MoE on your local machine.

Then restart the taosanode service and run `UPDATE ALL ANODES`. You can now use Time-MoE forecasting in your SQL statements.

## Use a TSFM in SQL

```sql
SELECT FORECAST(i32, 'algo=timemoe-fc') 
FROM foo;
```

## Deploying Other Time-Series Foundation Models

The logic for registering models in TDgpt after deploying them locally is similar across all types. You only need to modify the class name and model-service key and set the correct service address. Adaptation files for **Chronos**, **TimesFM**, and **Moirai** are provided by default; users of `v3.3.6.4` and later only need to start the corresponding services locally.

The deployment and startup methods are as follows:

### Starting the Moirai Service

To avoid dependency conflicts, it is recommended to prepare a clean Python virtual environment and install the libraries there.

```bash
pip install torch==2.3.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install uni2ts
pip install flask

```

Configure the service address in the `moirai-server.py` file (see above for the method) and set the model to be loaded (if necessary).

```python
_model_list = [
    'Salesforce/moirai-moe-1.0-R-small',  # small model with 117M parameters
    'Salesforce/moirai-moe-1.0-R-base',   # base model with 205M parameters
]

pretrained_model = MoiraiMoEModule.from_pretrained(
    _model_list[0]   # Loads the 'small' model by default; change to 1 to load 'base'
).to(device)

```

Execute the command to start the service. The model files will be downloaded automatically during the first startup. If the download speed is too slow, you can use a domestic mirror (see above for setup).

```bash
nohup python moirai-server.py > service_output.out 2>&1 &

```

Follow the same steps as above to check the service status.

### Starting the Chronos Service

Install dependencies in a clean Python virtual environment:

```bash
pip install torch==2.3.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install chronos-forecasting
pip install flask

```

Set the service address and model in `chronos-server.py`. You can also use the default values.

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
    _model_list[0],   # Loads the 'tiny' model by default; modify the index to change the model
    device_map=device,
    torch_dtype=torch.bfloat16,
)

```

Execute the following in the shell to start the service:

```bash
nohup python chronos-server.py > service_output.out 2>&1 &

```

### Starting the TimesFM Service

Install dependencies in a clean Python virtual environment:

```bash
pip install torch==2.3.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install timesfm
pip install jax
pip install flask==3.0.3

```

Adjust the service address in `timesfm-server.py` if necessary, then execute the command below:

```bash
nohup python timesfm-server.py > service_output.out 2>&1 &

```

### Starting the Moment Service

Install dependencies in a clean Python virtual environment:

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

Adjust the service address in `moment-server.py` if necessary, then execute the command below:

```bash
nohup python moment-server.py > service_output.out 2>&1 &

```

---

## Service Management Scripts (Start and Stop)

Since `v3.4.0.0`, TDgpt provides the unified `start-model.sh` and `stop-model.sh` scripts.

### Start Script

The Linux entry script delegates to `taosanode_service.py`, reads `taosanode.config.py`, and starts each model with its configured virtual environment. It always uses the primary virtual environment in the installation directory and does not fall back to Python on `PATH`.

After a `root` installation, the script is located in `<tdgpt_root>/bin/`. A symbolic link is automatically created at `/usr/bin/start-model` for global access.

Logs are output to `/var/log/taos/taosanode/taosanode_service_<model_name>.log` by default.

**Usage**:

```bash
Usage: /usr/bin/start-model [-c config_file] [model_name|all]

Supported models: tdtsfm, timesfm, timemoe, moirai, chronos, moment

```

**Options**:

* `-c config_file`: Specifies the configuration file. The default is `<install_dir>/cfg/taosanode.config.py`, falling back to `/etc/taos/taosanode.config.py`.
* `-h, --help`: Displays help information.

**Examples**:

1. Start all services in the background: `/usr/bin/start-model all`
2. Start a specific service (e.g., TimesFM): `/usr/bin/start-model timesfm`
3. Specify a custom config file: `/usr/bin/start-model -c /path/to/custom_taosanode.config.py`

### Stop Script

`stop-model.sh` is used to terminate specified or all model services. It automatically identifies and kills the relevant Python processes.

**Examples**:

1. Stop the TimesFM service: `/usr/bin/stop-model timesfm`
2. Stop all model services: `/usr/bin/stop-model all`

---

## Dynamic Model Download

Since `v3.3.8.0`, you can specify different model scales during startup. Running a `[xxx]-server.py` file directly without parameters generally loads `model_index=0`. When using `start-model` or the configuration file, `default_model` in `taosanode.config.py` applies. For example, Time-MoE defaults to `Maple728/TimeMoE-200M`, Chronos to `amazon/chronos-bolt-base`, and MOMENT to `AutonLab/MOMENT-1-base`.

Additionally, if you have manually downloaded model files, you can run them by specifying the local path.

```bash
# Run the chronos-bolt-tiny model located at /var/lib/taos/taosanode/model/chronos. 
# If the directory doesn't exist, it will download automatically to that path.
# The third parameter (True) enables the mirror site for faster downloads (recommended for users in China).
python chronos-server.py /var/lib/taos/taosanode/model/chronos/ amazon/chronos-bolt-tiny True

```

## Transformers Version Requirements

| Model Name | Transformers Version |
| --- | --- |
| time-moe, moirai, tdtsfm | 4.40 |
| chronos | 4.55 |
| moment | 4.33 |
| timesfm | N/A |

### References

1. Time-MoE: Billion-Scale Time Series Foundation Models with Mixture of Experts  
   [Paper](https://arxiv.org/abs/2409.16040) | [GitHub Repo](https://github.com/Time-MoE/Time-MoE)
