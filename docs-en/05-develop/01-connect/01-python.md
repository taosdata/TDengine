---
sidebar_label: Python
title: Connect with Python Connector
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Install Connector

First, you need to install the `taospy` module version >= `2.3.3`. Run the command below in your terminal.

<Tabs>
<TabItem value="pip" label="pip">

```
pip3 install taospy>=2.3.3
```

</TabItem>
<TabItem value="conda" label="conda">

```
conda install taospy=2.3.3
```

</TabItem>
</Tabs>


You'll need to have Python3 installed.

## Config

Run this command in your terminal to save TDengine cloud token as variables:

```bash
export TDENGINE_CLOUD_TOKEN=<token>
```

You can also set environment variable in IDE. For example, you can set environmental variables in Pycharm's run configurations menu.


<!-- exclude -->
:::note
To obtain your personal cloud token, please log in [TDengine Cloud](https://cloud.tdengine.com).

:::
<!-- exclude-end -->

## Connect

Copy code bellow to your editor and run it with `python3` command.

```python
import taosrest
import os

token = os.environ["TDENGINE_CLOUD_TOKEN"]
url = "https://cloud.tdengine.com"

conn = taosrest.connect(url=url, token=token)
```

The client connection is then established. For how to write data and query data, please refer to [sample-program](https://docs.tdengine.com/cloud/connector/python/#sample-program).
