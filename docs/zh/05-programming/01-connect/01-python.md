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

### 安装前准备

您必须先安装 Python3 和 Pip3。

* 安装 Python。新版本 taospy 包要求 Python 3.6.2+。早期版本 taospy 包要求 Python 3.7+。taos-ws-py 包要求 Python 3.7+。如果系统上还没有 Python 可参考 [Python BeginnersGuide](https://wiki.python.org/moin/BeginnersGuide/Download) 安装。
* 安装 Pip3。大部分情况下 Python 的安装包都自带了 pip 工具， 如果没有请参考 [pip documentation](https://pypi.org/project/pip/) 安装。

### 用 Pip 安装

如果以前安装过旧版本的 Python 连接器, 请提前卸载。

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

安装最新或指定版本 `taospy` 或 `taos-ws-py`, 在终端里面执行下面的命令。

<Tabs defaultValue="rest" groupID="package">
<TabItem value="rest" label="REST">

```bash
# 安装最新版本
pip3 install taospy

# 安装指定版本
pip3 install taospy==2.6.2

# 从 GitHub 安装
pip3 install git+https://github.com/taosdata/taos-connector-python.git
```

</TabItem>
<TabItem value="websocket" label="WebSocket">

```bash
pip3 install taos-ws-py
```

</TabItem>
</Tabs>

### 安装验证

<Tabs defaultValue="rest" groupID="package">
<TabItem value="rest" label="REST">

对于 REST 连接，只需验证是否能成功导入 `taosrest` 模块。可在 Python 交互式 Shell 中输入：

```python
import taosrest
```

</TabItem>
<TabItem value="websocket" label="WebSocket">

对于 WebSocket 连接，只需验证是否能成功导入 `taosws` 模块。可在 Python 交互式 Shell 中输入：

```python
import taosws
```

</TabItem>
</Tabs>

## 配置

在您的终端里面执行下面的命令来保存 TDengine Cloud 的 URL 和令牌到系统的环境变量里面：

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

另外您也可以在您的 IDE 的运行配置里面设置这些环境变量。

<!-- exclude -->
:::note IMPORTANT
替换 \<token> 和 \<url> 为您的 TDengine Cloud 实例的令牌和 URL 。

获取 TDengine Cloud 的令牌和 URL，可以登录[TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”编程“菜单，然后选择”Python“。

请注意区分 REST 连接和  WebSocket 连接的URL。
:::
<!-- exclude-end -->

## 建立连接  {#connect}

复制下面的代码到您的编辑器，然后执行这段代码。

<Tabs defaultValue="rest" groupID="package">
<TabItem value="rest" label="REST">

```python
{{#include docs/examples/python/develop_tutorial.py:connect}}
```

`connect()` 函数的所有参数都是可选的关键字参数。下面是连接参数的具体说明：

* `url`： TDengine Cloud 的URL。
* `token`: TDengine Cloud 的令牌.
* `timeout`: HTTP 请求超时时间。单位为秒。默认为 `socket._GLOBAL_DEFAULT_TIMEOUT`。 一般无需配置。

</TabItem>
<TabItem value="websocket" label="WebSocket">

```python
{{#include docs/examples/python/develop_tutorial_ws.py:connect}}
```

</TabItem>
</Tabs>

关于如何写入数据和查询数据，请参考[写入数据](https://docs.taosdata.com/cloud/programming/insert)和[查询数据](https://docs.taosdata.com/cloud/programming/query)。

更多关于 REST 接口的详情，请参考 [REST 接口](https://docs.taosdata.com/cloud/programming/client-libraries/rest-api/)。
