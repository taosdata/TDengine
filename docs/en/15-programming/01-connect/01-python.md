---
sidebar_label: Python
title: Connect with Python Connector
description: Connect to TDengine cloud service using Python connector
---

<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->
## Install Connector

First, you need to install the `taospy` module version >= `2.6.2`. Run the command below in your terminal.

<Tabs defaultValue="pip" groupID="package">
<TabItem value="pip" label="pip">

```
pip3 install -U taospy
```
You'll need to have Python3 installed.

</TabItem>
<TabItem value="conda" label="conda">

```
conda install -c conda-forge taospy
```

</TabItem>
</Tabs>

## Config

Run this command in your terminal to save TDengine cloud token and URL as variables:

<Tabs defaultValue="bash">
<TabItem value="bash" label="Bash">

```bash
export TDENGINE_CLOUD_TOKEN="<token>"
export TDENGINE_CLOUD_URL="<url>"
```

</TabItem>
<TabItem value="cmd" label="CMD">

```bash
set TDENGINE_CLOUD_TOKEN="<token>"
set TDENGINE_CLOUD_URL="<url>"
```

</TabItem>
<TabItem value="powershell" label="Powershell">

```powershell
$env:TDENGINE_CLOUD_TOKEN="<token>"
$env:TDENGINE_CLOUD_URL="<url>"
```

</TabItem>
</Tabs>


Alternatively, you can also set environment variables in your IDE's run configurations.


<!-- exclude -->
:::note
Replace  <token\> and <url\> with cloud token and URL.
To obtain the value of cloud token and URL, please log in [TDengine Cloud](https://cloud.tdengine.com) and click "Connector" and then select "Python".

:::
<!-- exclude-end -->

## Connect

Copy code bellow to your editor and run it. If you are using jupyter, assuming you have followed the guide about Jupyter in previous sections, you can copy the code into Jupyter editor in your browser.

```python
{{#include docs/examples/python/develop_tutorial.py:connect}}
```

For how to write data and query data, please refer to <https://docs.tdengine.com/cloud/data-in/insert-data/> and <https://docs.tdengine.com/cloud/data-out/query-data/>.

For more details about how to write or query data via REST API, please check [REST API](https://docs.tdengine.com/cloud/programming/connector/rest-api/).

## Jupyter

**Step 1: Install**

For the users who are familiar with Jupyter to program in Python, both TDengine Python connector and Jupyter need to be ready in your environment. If you have not done yet, please use the commands below to install them.

<Tabs defaultValue="pip" groupID="package">
<TabItem value="pip" label="pip">

```bash
pip install jupyterlab
pip3 install -U taospy
```

You'll need to have Python3 installed.

</TabItem>
<TabItem value="conda" label="conda">

```
conda install -c conda-forge jupyterlab
conda install -c conda-forge taospy
```

</TabItem>
</Tabs>

**Step 2: Configure**

In order for Jupyter to connect to TDengine cloud service, before launching Jupypter, the environment setting must be performed. We use Linux bash as example.

```bash
export TDENGINE_CLOUD_TOKEN="<token>"
export TDENGINE_CLOUD_URL="<url>"
jupyter lab
```

**Step 3: Connect**

Once jupyter lab is launched, Jupyter lab service is automatically connected and shown in your browser. You can create a new notebook and copy the sample code below and run it.

```python
{{#include docs/examples/python/develop_tutorial.py:connect}}
```
