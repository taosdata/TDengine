---
sidebar_label: ODBC
title: TDengine ODBC
---

## Introduction

The TDengine ODBC driver is a driver specifically designed for TDengine based on the ODBC standard. It can be used by ODBC based applications,like [PowerBI](https://powerbi.microsoft.com), on Windows, to access a local or remote TDengine cluster or an instance in the TDengine Cloud service. The TDengine instance version must be above 3.2.1.0.

TDengine ODBC provides two kinds of connections, native connection and WebSocket connection. But you must use WebSocket to access an instance in the TDengine Cloud service.

Note: TDengine ODBC driver can only be run on 64-bit systems, and can only be invoked by 64-bit applications.

## Install

1. The TDengine ODBC driver only supports the Windows platform. To run on Windows, the Microsoft Visual C++ Runtime library is required. If the Microsoft Visual C++ Runtime Library is missing on your platform, you can download and install it from [VC Runtime Library](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist?view=msvc-170).

2. Install TDengine Windows client installation package as the following. The client package includes both the TDengine ODBC driver and some other necessary libraries that will be used in either native connection or WebSocket connection.

:::note IMPORTANT
Please login [TDengine Cloud](https://cloud.tdengine.com) and select "ODBC" card of the "Programming" page. In the opened page, please download the selected TDengine Cloud instance's TDengine Windows client in the "Install ODBC Connector" part.
:::

## Configure Data Source

1. Click the "Start" Menu, and Search for "ODBC", and choose "ODBC Data Source (64-bit)" (Note: Don't choose 32-bit).

2. Select "User DSN" tab, and click "Add" to enter the page for "Create Data Source"

3. Choose the data source to be added, here we choose "TDengine" and click "Finish", and enter the configuration page for "TDengine ODBC Data Source", fill in required fields as the following:
    - \[DSN\]: Data Source Name, required field, such as "MyTDengine"
    - \[Connection Type\]: required field, we choose "WebSocket"
    - \[URL\]: To obtain the URL, please login [TDengine Cloud](https://cloud.tdengine.com) and click "Tools", select "PowerBI" and then copy the related value of URL
    - \[Database\]: optional field, the default database to access, such as "test"
4. Click "Test Connection" to test whether the connection to the data source is successful; if successful, it will prompt "Successfully connected to the URL".

:::note IMPORTANT
Please log in [TDengine Cloud](https://cloud.tdengine.com) and select "ODBC" card of the "Programming" page. In the opened page, please copy the value in the "URL" field of the "Configure ODBC DataSource" part.
:::

## Example

As an example, you can use PowerBI, which invokes TDengine ODBC driver, to access an instance in the TDengine Cloud service, please refer to [Power BI](../../../tools/powerbi) for more details.
