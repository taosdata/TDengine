---
sidebar_label: DBeaver
title: DBeaver
description: You can use DBeaver to access your data stored in TDengine and TDengine Cloud.
---

[DBeaver](https://https://dbeaver.io/) is a popular cross-platform database management tool that facilitates data management for developers, database administrators, data analysts, and other users. Starting from version 23.1.1, DBeaver natively supports TDengine and can be used to manage TDengine Cloud as well as TDengine clusters deployed on-premises.

## Prerequisites

To use DBeaver to manage TDengine, you need to prepare the following:

- Install DBeaver. DBeaver supports mainstream operating systems including Windows, macOS, and Linux. Please make sure you download and install the correct version (23.1.1+) and platform package. Please refer to the [official DBeaver documentation](https://github.com/dbeaver/dbeaver/wiki/Installation) for detailed installation steps.
- If you use an on-premises TDengine cluster, please make sure that TDengine and taosAdapter are deployed and running properly. For detailed information, please refer to the taosAdapter User Manual.
- If you use TDengine Cloud, please [register](https://cloud.tdengine.com/) for an account.

## Usage

### Use DBeaver to access on-premises TDengine cluster

1. Start the DBeaver application, click the button or menu item to choose **New Database Connection**, and then select **TDengine** in the **Timeseries** category.

![Connect TDengine with DBeaver](./dbeaver/dbeaver-connect-tdengine-en.webp)

2. Configure the TDengine connection by filling in the host address, port number, username, and password. If TDengine is deployed on the local machine, you are only required to fill in the username and password. The default username is root and the default password is taosdata. Click **Test Connection** to check whether the connection is workable. If you do not have the TDengine Java connector installed on the local machine, DBeaver will prompt you to download and install it.

![Configure the TDengine connection](./dbeaver/dbeaver-config-tdengine-en.webp))

3. If the connection is successful, it will be displayed as shown in the following figure. If the connection fails, please check whether the TDengine service and taosAdapter are running correctly and whether the host address, port number, username, and password are correct.

![Connection successful](./dbeaver/dbeaver-connect-tdengine-test-en.webp)

4. Use DBeaver to select databases and tables and browse your data stored in TDengine.

![Browse TDengine data with DBeaver](./dbeaver/dbeaver-browse-data-en.webp)

5. You can also manipulate TDengine data by executing SQL commands.

![Use SQL commands to manipulate TDengine data in DBeaver](./dbeaver/dbeaver-sql-execution-en.webp)

### Use DBeaver to access TDengine Cloud

1. Log in to the TDengine Cloud service, select **Programming** > **Java** in the management console, and then copy the string value of `TDENGINE_JDBC_URL` displayed in the **Config** section.

![Copy JDBC URL from TDengine Cloud](./dbeaver/tdengine-cloud-jdbc-dsn-en.webp)

2. Start the DBeaver application, click the button or menu item to choose **New Database Connection**, and then select **TDengine Cloud** in the **Timeseries** category.

![Connect TDengine Cloud with DBeaver](./dbeaver/dbeaver-connect-tdengine-cloud-en.webp)

3. Configure the TDengine Cloud connection by filling in the JDBC URL value. Click **Test Connection**. If you do not have the TDengine Java connector installed on the local machine, DBeaver will prompt you to download and install it. If the connection is successful, it will be displayed as shown in the following figure. If the connection fails, please check whether the TDengine Cloud service is running properly and whether the JDBC URL is correct.

![Configure the TDengine Cloud connection](./dbeaver/dbeaver-connect-tdengine-cloud-test-en.webp)

4. Use DBeaver to select databases and tables and browse your data stored in TDengine Cloud.

![Browse TDengine Cloud data with DBeaver](./dbeaver/dbeaver-browse-data-cloud-en.webp)

5. You can also manipulate TDengine Cloud data by executing SQL commands.

![Use SQL commands to manipulate TDengine Cloud data in DBeaver](./dbeaver/dbeaver-sql-execution-cloud-en.webp)
