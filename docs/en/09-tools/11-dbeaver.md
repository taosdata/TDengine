---
sidebar_label: DBeaver
title: DBeaver
description: This document describes how to use DBeaver to access your data stored in TDengine Cloud.
---

[DBeaver](https://dbeaver.io/) is a popular cross-platform database management tool that facilitates data management for developers, database administrators, data analysts, and other users. Starting from version 23.1.1, DBeaver natively supports TDengine and can be used to manage TDengine Cloud as well as TDengine clusters deployed on-premises.

## Prerequisites

To use DBeaver to manage TDengine, you need to prepare the following:

- Install DBeaver. DBeaver supports mainstream operating systems including Windows, macOS, and Linux. Please make sure you download and install the correct version (23.1.1+) and platform package. Please refer to the [official DBeaver documentation](https://github.com/dbeaver/dbeaver/wiki/Installation) for detailed installation steps.
- If you use TDengine Cloud, please [register](https://cloud.tdengine.com/) for an account.

## Use DBeaver to access TDengine Cloud

1. Log in to the TDengine Cloud service, select **Programming** > **Java** in the management console, and then copy the string value of `TDENGINE_JDBC_URL` displayed in the **Config** section.

 ![Copy JDBC URL from TDengine Cloud](./dbeaver/tdengine-cloud-jdbc-dsn-en.webp)

Or copy TDENGINE_JDBC_URL value directly after log in.

```
<jdbcURL>
```

2. Start the DBeaver application, click the button or menu item to choose **New Database Connection**, and then select **TDengine Cloud** in the **Timeseries** category.

 ![Connect TDengine Cloud with DBeaver](./dbeaver/dbeaver-connect-tdengine-cloud-en.webp)

3. Configure the TDengine Cloud connection by filling in the JDBC URL value. Click **Test Connection**. If you do not have the TDengine Java client library installed on the local machine, DBeaver will prompt you to download and install it. If the connection is successful, it will be displayed as shown in the following figure. If the connection fails, please check whether the TDengine Cloud service is running properly and whether the JDBC URL is correct.

 ![Configure the TDengine Cloud connection](./dbeaver/dbeaver-connect-tdengine-cloud-test-en.webp)

4. Use DBeaver to select databases and tables and browse your data stored in TDengine Cloud.

 ![Browse TDengine Cloud data with DBeaver](./dbeaver/dbeaver-browse-data-cloud-en.webp)

5. You can also manipulate TDengine Cloud data by executing SQL commands.

 ![Use SQL commands to manipulate TDengine Cloud data in DBeaver](./dbeaver/dbeaver-sql-execution-cloud-en.webp)
