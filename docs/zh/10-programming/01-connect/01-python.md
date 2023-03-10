---
sidebar_label: Python
title: 使用 Python 连接器建立连接
description: 使用 Python 连接器建立和 TDengine Cloud 的连接
---

<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->
## 安装连接器

首先，您需要安装版本大于 `2.6.2` 的 `taospy` 模块，然后在终端里面执行下面的命令。

<Tabs defaultValue="pip" groupID="package">
<TabItem value="pip" label="pip">

```bash
pip3 install -U taospy[ws]
```

您必须首先安装 Python3 。

</TabItem>
<TabItem value="conda" label="conda">

```bash
conda install -c conda-forge taospy taospyws
```

</TabItem>
</Tabs>

## 配置

在您的终端里面执行下面的命令来保存 TDengine Cloud 的 URL 和令牌到系统的环境变量里面：

<Tabs defaultValue="bash">
<TabItem value="bash" label="Bash">

```bash
export TDENGINE_CLOUD_TOKEN="<token>"
export TDENGINE_CLOUD_URL="<url>"
```

</TabItem>
<TabItem value="cmd" label="CMD">

```shell
set TDENGINE_CLOUD_TOKEN=<token>
set TDENGINE_CLOUD_URL=<url>
```

</TabItem>
<TabItem value="powershell" label="Powershell">

```powershell
$env:TDENGINE_CLOUD_TOKEN='<token>'
$env:TDENGINE_CLOUD_URL='<url>'
```

</TabItem>
</Tabs>

另外您也可以在您的 IDE 的运行配置里面设置这些环境变量。

<!-- exclude -->
:::note
替换 <token\> 和 <url\> 为 TDengine Cloud 的令牌和 URL 。
获取 TDengine Cloud 的令牌和 URL，可以登录[TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”编程“菜单，然后选择”Python“。
:::
<!-- exclude-end -->

## 建立连接

复制下面的代码到您的编辑器，然后执行这段代码。如果您正在使用 Jupyter 并且按照它的指南搭建好环境，您可以负责下面代码到您浏览器的 Jupyter 编辑器。

```python
{{#include docs/examples/python/develop_tutorial.py:connect}}
```

对于如何写入数据和查询输入，请参考<https://docs.taosdata.com/cloud/data-in/insert-data/>和 <https://docs.taosdata.com/cloud/data-out/query-data/>。

想知道更多通过 REST 接口写入数据的详情，请参考[REST 接口](https://docs.taosdata.com/cloud/programming/connector/rest-api/).

## Jupyter

### 步骤一：安装模块

对于熟悉使用 Jupyter 来进行 Python 编程的用户，在您的环境中必须准备好 TDengine 的 Python 连接器和 Jupyter。如果您还没有这样做，请使用下面的命令来安装他们。

<Tabs defaultValue="pip" groupID="package">
<TabItem value="pip" label="pip">

```bash
pip install jupyterlab
pip3 install -U taospy[ws]
```

您接下来需要安装 Python3 。

</TabItem>
<TabItem value="conda" label="conda">

```
conda install -c conda-forge jupyterlab
conda install -c conda-forge taospy
```

</TabItem>
</Tabs>

### 步骤二：配置

在使用 Jupyter 和 TDengine Cloud 连接连接之前，需要在环境变量设置按照下面内容设置，然后再启动 Jupyter。我们使用 Linux 脚本作为例子。

```bash
export TDENGINE_CLOUD_TOKEN="<token>"
export TDENGINE_CLOUD_URL="<url>"
jupyter lab
```

### 步骤三：建立连接

一旦 Jupter lab 启动成功，Jupyter lab 服务就会自动和 TDengine Cloud 连接并且显示在浏览器里面。您可以创建一个新的 notebook 页面，然后复制下面的样例代码到这个页面中并运行。

```python
{{#include docs/examples/python/develop_tutorial.py:connect}}
```
