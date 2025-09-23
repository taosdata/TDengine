---
title: DBeaver
slug: /third-party-tools/management/dbeaver
---

DBeaver is a popular cross-platform database management tool that facilitates developers, database administrators, and data analysts in managing data. DBeaver has embedded support for TDengine starting from version 23.1.1. It supports both standalone deployed TDengine clusters and TDengine Cloud.

## Prerequisites

Using DBeaver to manage TDengine requires the following preparations.

- Install DBeaver. DBeaver supports mainstream operating systems including Windows, macOS, and Linux. Please make sure to [download](https://dbeaver.io/download/) the correct platform and version (23.1.1+) of the installer. For detailed installation steps, refer to the [DBeaver official documentation](https://github.com/dbeaver/dbeaver/wiki/Installation).
- If using a standalone deployed TDengine cluster, ensure that TDengine is running normally, and that taosAdapter has been installed and is running properly. For specific details, refer to the [taosAdapter user manual](../../../tdengine-reference/components/taosadapter).

## Using DBeaver to Access Internally Deployed TDengine

1. Launch the DBeaver application, click the button or menu item to "New Database Connection", then select TDengine in the time-series category.

   ![](../../assets/dbeaver-01.webp)

2. Configure the TDengine connection by entering the host address, port number (6041), username, and password. For example, use TDengine server host address `www.taosdemo.com` and port number `6041`. It is recommended to select URL connection and add connection parameters `varcharAsString=true&conmode=1`. For parameter meanings, refer to the [JDBC documentation](../../../tdengine-reference/client-libraries/java/#properties). Click "Test Connection" to test whether the connection is available. If the TDengine Java connector is not installed on the local machine, DBeaver will prompt you to download and install it.

   ![](../../assets/dbeaver-02.webp)

3. A successful connection will be displayed as shown below. If the connection fails, check whether the TDengine service and taosAdapter are running correctly, and whether the host address, port number, username, and password are correct.

   ![](../../assets/dbeaver-03.webp)

4. Using DBeaver to select databases and tables allows you to browse data from the TDengine service.

   ![](../../assets/dbeaver-04.webp)

5. You can also operate on TDengine data by executing SQL commands.

   ![](../../assets/dbeaver-05.webp)
