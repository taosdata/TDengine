---
sidebar_label: Python
title: Connect with Python
description: This document describes how to connect to TDengine Cloud using the Python client library.
---

<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->
## Install Client Library

### Preparation

You must first install Python3 and Pip3.

* Install Python. The newer versions of the taospy package require Python 3.6.2+. Earlier versions of the taospy package require Python 3.7+. The taos-ws-py package requires Python 3.7+. If Python is not yet installed on your system, you can refer to the [Python Beginners Guide](https://wiki.python.org/moin/BeginnersGuide/Download) for installation.
* Install Pip3. In most cases, the Python installation package comes with the pip tool. If it's not included, please refer to the [pip documentation](https://pypi.org/project/pip/) for installation.

### Install with Pip

If you have installed an older version of the Python connector, please uninstall it in advance.

<Tabs defaultValue="rest" groupID="package">
<TabItem value="rest" label="REST">

```bash
pip3 uninstall taos taospy
```

</TabItem>
<TabItem value="websocket" label="WebSocket">

```bash
pip3 uninstall taos taos-ws-py
```

</TabItem>
</Tabs>

To install the latest or a specific version of `taospy` or `taos-ws-py`, execute the following command in the terminal.

<Tabs defaultValue="rest" groupID="package">
<TabItem value="rest" label="REST">

```bash
# install latest version
pip3 install taospy

# install specific version
pip3 install taospy==2.6.2

# install from github
pip3 install git+https://github.com/taosdata/taos-connector-python.git
```

</TabItem>
<TabItem value="websocket" label="WebSocket">

```bash
pip3 install taos-ws-py
```

</TabItem>
</Tabs>

### Verify

<Tabs defaultValue="rest" groupID="package">
<TabItem value="rest" label="REST">
For REST connections, simply verify that the `taosrest` module can be successfully imported. You can enter the following in the Python interactive Shell:

```python
import taosrest
```

</TabItem>
<TabItem value="websocket" label="WebSocket">
For WebSocket connections, simply verify that the `taosws` module can be successfully imported. You can enter the following in the Python interactive Shell:

```python
import taosws
```

</TabItem>
</Tabs>

## Config

Run this command in your terminal to save TDengine cloud token and URL as variables:

<Tabs defaultValue="bash">
<TabItem value="bash" label="Bash">

```bash
export TDENGINE_CLOUD_URL="<url>"
export TDENGINE_CLOUD_TOKEN="<token>"
```

</TabItem>
<TabItem value="cmd" label="CMD">

```shell
set TDENGINE_CLOUD_URL=<url>
set TDENGINE_CLOUD_TOKEN=<token>
```

</TabItem>
<TabItem value="powershell" label="Powershell">

```powershell
$env:TDENGINE_CLOUD_URL='<url>'
$env:TDENGINE_CLOUD_TOKEN='<token>'
```

</TabItem>
</Tabs>

Alternatively, you can also set environment variables in your IDE's run configurations.

<!-- exclude -->
:::note IMPORTANT
Replace  &lt;token&gt; and &lt;url&gt; with cloud token and URL.

To obtain the value of cloud token and URL, please login [TDengine Cloud](https://cloud.tdengine.com) and click "Programming" on the left menu, then select "Python".

Please ensure to distinguish between the URLs for REST connections and WebSocket connections.
:::
<!-- exclude-end -->

## Connect

Copy code bellow to your editor, then run it.
<Tabs defaultValue="rest">
<TabItem value="rest" label="REST">

```python
{{#include docs/examples/python/develop_tutorial.py:connect}}
```

</TabItem>
<TabItem value="websocket" label="WebSocket">

```python
{{#include docs/examples/python/develop_tutorial_ws.py:connect}}
```

</TabItem>
</Tabs>

For how to write data and query data, please refer to [Insert](https://docs.tdengine.com/cloud/programming/insert/) and [Query](https://docs.tdengine.com/cloud/programming/query/).

For more details about how to write or query data via REST API, please check [REST API](https://docs.tdengine.com/cloud/programming/connect/rest-api/).
