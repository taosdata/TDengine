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

Run this command in your terminal to save TDengine cloud token as variables:

```bash
export TDENGINE_TOKEN=<token>
```

<!-- exclude -->
:::note
To obtain cloud token, please log in TDengine Cloud and switch to "Connector" section.

:::
<!-- exclude-end -->

## Connect

Copy code bellow to your editor and run it with `python3` command.

```python
import taosrest
import os

token = os.environ["TDENGINE_TOKEN"]
url = "https://cloud.tdengine.com"

conn = taosrest.connect(url=url, token=token)
```

The client connection is then established. For how to write data and query data, please refer to [sample-program](https://docs.tdengine.com/cloud/connector/python/#sample-program).
