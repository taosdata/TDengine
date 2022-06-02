---
sidebar_label: Python
title: Connect with Python Connector
---

## Install Connector

First, you need to install the `taospy` module. Run the command below in your terminal.

```
pip3 install taospy
```

You'll need to have Python3 installed.

## Config

Run this command in your terminal to save connect parameters as environment variables:

```bash
export TDENGINE_CLOUD_HOST=<host>
export TDENGINE_CLOUD_PORT=<port>
export TDENGINE_CLOUD_TOKEN=<token>
export TDENGINE_USER_NAME=<username>
export TDENGINE_PASSWORD=<password>
```


<!-- exclude -->
:::note
You should replace above placeholders as real values. To get these values, please log in TDengine Cloud and switch to "Connector" section.

:::
<!-- exclude-end -->

## Connect

```python
import taosrest
import os

host = os.environ["TDENGINE_CLOUD_HOST"]
port = os.environ["TDENGINE_CLOUD_PORT"]
token = os.environ["TDENGINE_CLOUD_TOKEN"]
user = os.environ["TDENGINE_USER_NAME"]
password = os.environ["TDENGINE_PASSWORD"]

conn = taosrest.connect(host=host,
                        port=port,
                        username=user,
                        password=password,
                        token=token)
```
