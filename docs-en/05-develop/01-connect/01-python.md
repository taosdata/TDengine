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

Run this command in your terminal to save your url and token as variables:

```bash
export TDENGINE_CLOUD_URL=<url>
export TDENGINE_CLOUD_TOKEN=<token>
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

url = os.environ["TDENGINE_CLOUD_URL"]
token = os.environ["TDENGINE_CLOUD_TOKEN"]

conn = taosrest.connect(url=url, token=token)
```

The client connection is then established. 