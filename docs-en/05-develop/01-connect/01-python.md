---
sidebar_label: Python
title: Connect with Python Connector
---

## Install Connector

First, you need to install the `taospy` module version >= `2.3.3`. Run the command below in your terminal.

```
pip3 install taospy>=2.3.3
```

You'll need to have Python3 installed.

## Config

Run this command in your terminal to save your URL and token as variables:

```bash
export TDENGINE_CLOUD_URL=<URL>
export TDENGINE_CLOUD_TOKEN=<token>
```

<!-- exclude -->
:::note
You should replace above placeholders as real values. To obtain these values, please log in TDengine Cloud and switch to "Connector" section.

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

The client connection is then established. For how to write data and query data, please refer to [basic usage](../../connector/python#basic-usage).
