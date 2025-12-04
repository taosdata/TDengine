---
sidebar_label: ODBC
title: TDengine ODBC
---


## 简介

TDengine ODBC  是为 TDengine 实现的 ODBC 驱动程序，支持 Windows 系统的应用（如 [PowerBI](https://powerbi.microsoft.com/zh-cn/) 等）通过 ODBC 标准接口访问本地、远程和云服务的 TDengine 数据库。最小支持的 TDengine 版本是 3.2.1.0。

TDengine ODBC 提供了两种连接方式，原生连接和 WebSocket 连接。但是您必须使用 WebSocket 连接访问 TDengine Cloud 的实例。

注意：TDengine ODBC 只支持 64 位系统，调用 TDengine ODBC 必须通过 64 位的 ODBC 驱动管理器进行。因此调用 ODBC 的程序不能使用 32 位版本。

## 安装

1. 仅支持 Windows 平台。Windows 上需要安装过 VC 运行时库，可在此下载安装 [VC 运行时库](https://learn.microsoft.com/zh-cn/cpp/windows/latest-supported-vc-redist?view=msvc-170)。如果已经安装 VS 开发工具可忽略。
2. 下载和安装 TDengine Windows 客户端安装包，请参考下面的注意内容。

:::note 非常重要
请登录 [TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”编程“菜单，然后选择”ODBC“，在“安装 ODBC 连接器”步骤下载选中的 TDengine Cloud 实例对应的客户端。
:::

## 配置 ODBC 数据源

1. Windows 操作系统的【开始】菜单搜索打开【ODBC 数据源 (64 位)】管理工具（注意不要选择 ODBC 数据源 (32 位)）。
2. 选中【用户 DSN】标签页，通过【添加 (D)】按钮进入“创建数据源”界面。
3. 选择想要添加的数据源，然后选择【TDengine】，点击完成，进入 TDengine ODBC 数据源配置页面，填写如下必要信息：
    - 【DSN】：数据源名称，必填，比如“MyTDengine”
    - 【接类型】：选中【Websocket】
    - 【URL】：获取实际的 URL，请参考下面的注意内容
    - 【数据库】：可选，填写需要连接的数据库，比如“test”
4. 点击【测试连接】按钮测试连接情况，如果成功，会提示“成功连接到该 URL”。

:::note 非常重要
获取真实的 `URL` 的值，请登录 [TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”编程“菜单，然后选择”ODBC“，在“配置 ODBC 数据源”部分复制当前 TDengine Cloud 实例的 URL 值。
:::

## 样例

您可以通过 Power BI 来使用 TDengine ODBC 驱动直接访问 TDengine Cloud 服务里面的一个实例。更多细节请参考 [Power BI](../../../tools/powerbi)。
