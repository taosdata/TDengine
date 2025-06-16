---
title: Deploy a Time-Series Foundation Model
sidebar_label: Deploy a Time-Series Foundation Model
---

A number of research institutions and enterprises have released open-source time-series foundation models (TSFMs), greatly simplifying time-series data analysis. Beyond traditional data analysis algorithms, machine learning, and deep learning models,
TSFMs offer a new and powerful option for advanced time-series analytics. This chapter introduces how to deploy and use open-source TSFMs.

TDgpt includes two TSFMs, TDtsfm and Time-MoE, but you can add more open-source or proprietary TSFMs to TDgpt as needed.
This document describes how to integrate an independent TSFM service into TDengine, using [Time-MoE](https://github.com/Time-MoE/Time-MoE) as an example,
and how to use the model in SQL statements for time-series forecasting.

## Prepare Your Environment

Before using TSFMs, prepare your environment as follows. Install a Python environment and use PiPy to install dependencies:

```shell
pip install torch==2.4.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
pip install flask==3.0.3
pip install transformers==4.40.0
pip install accelerate
```

You can use the virtual Python environment installed by TDgpt or a separate environment.

## Configure TSFM Path

The `lib/taosanalytics/time-moe.py` file in the TDgpt root directory deploys and serves the Time-MoE model. Modify this file to set an appropriate URL.

```python
@app.route('/ds_predict', methods=['POST'])
def time_moe():
...
```

Change `ds_predict` to the URL that you want to use in your environment.

```Python
    app.run(
            host='0.0.0.0',
            port=5001,
            threaded=True,  
            debug=False     
        )
```

In this section, you can update the port if desired. After you have set your URL and port number, restart the service.

## Run the Python Script

```shell
nohup python time-moe.py > service_output.out 2>&1 &
```

The script automatically downloads [Time-MoE-200M](https://huggingface.co/Maple728/TimeMoE-200M) from Hugging Face the first time it is run. You can modify `time-moe.py` to use TimeMoE-50M if you prefer a smaller version.

Check the `service-output.out` file to confirm that the model has been loaded:

```shell
Running on all addresses (0.0.0.0)
Running on http://127.0.0.1:5001
```

## Verify the Service

Verify that the service is running normally:

```shell
curl 127.0.0.1:5001/ds_predict
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

You can modify the [timemoe.py] file and use it in TDgpt. In this example, Time-MoE is adapted to provide forecasting.

```python
class _TimeMOEService(AbstractForecastService):
    # model name, user-defined, used as model key
    name = 'timemoe-fc'

    # description
    desc = ("Time-MoE: Billion-Scale Time Series Foundation Models with Mixture of Experts; "
            "Ref to https://github.com/Time-MoE/Time-MoE")

    def __init__(self):
        super().__init__()

        self.table_name = None

        # find service address in taosanode.ini or use default if not found
        service_host = conf.get_tsfm_service("timemoe-fc")
        if  service_host is not None:
            self.service_host = service_host
        else:
            self.service_host = 'http://127.0.0.1:5001/timemoe'

        self.headers = {'Content-Type': 'application/json'}


    def execute(self):
        """analytics methods"""
        if self.list is None or len(self.list) < self.period:
            raise ValueError("number of input data is less than the periods")

        if self.rows <= 0:
            raise ValueError("fc rows is not specified yet")

        # let's request the gpt service
        data = {"input": self.list, 'next_len': self.rows}
        try:
            # request tsfm service
            response = requests.post(self.service_host, data=json.dumps(data), headers=self.headers)
        except Exception as e:
            app_logger.log_inst.error(f"failed to connect the service: {self.service_host} ", str(e))
            raise e

         # check returned value
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

        # package forecasting results according to specifications
        insert_ts_list(res["res"], self.start_ts, self.time_step, self.rows)
        return res
```

Add your code to `/usr/local/taos/taosanode/lib/taosanalytics/algo/fc/` where the `timemoe.py` file is located.

TDgpt has built-in support for Time-MoE. You can run `SHOW ANODES FULL` and see that forecasting based on Time-MoE is listed as `timemoe-fc`.

## Configure TSFM Service Path

Modify the `[tsfm-service]` section of `/etc/taos/taosanode.ini`:

```ini
[tsfm-service]
timemoe-fc = http://127.0.0.1:5001/ds_predict
```

Add the path for your deployment. The key is the name of the model defined in your Python code, and the value is the URL of Time-MoE on your local machine.

Then restart the taosanode service and run `UPDATE ALL ANODES`. You can now use Time-MoE forecasting in your SQL statements.

## Use a TSFM in SQL

```sql
SELECT FORECAST(i32, 'algo=timemoe-fc') 
FROM foo;
```

## Add Other TSFMs

You can add more open-source or proprietary TSFMs to TDgpt by following the process described in this document. Ensure that the class and service names have been configured appropriately and that the service URL is reachable.

### References

- Time-MoE: Billion-Scale Time Series Foundation Models with Mixture of Experts. [[paper](https://arxiv.org/abs/2409.16040)] [[GitHub Repo](https://github.com/Time-MoE/Time-MoE)]
