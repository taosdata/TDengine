---
title: qStudio
slug: /third-party-tools/management/qstudio
---

qStudio is a free multi-platform SQL data analysis tool that allows easy browsing of tables, variables, functions, and configuration settings in databases. The latest version of qStudio has built-in support for TDengine.

## Prerequisites

Using qStudio to connect to TDengine requires the following preparations.

- Install qStudio. qStudio supports mainstream operating systems including Windows, macOS, and Linux. Please make sure to [download](https://www.timestored.com/qstudio/download/) the correct platform package.
- Install a TDengine instance, ensure that TDengine is running properly, and that taosAdapter is installed and running smoothly. For more details, please refer to [the taosAdapter user manual](../../../tdengine-reference/components/taosadapter).

## Using qStudio to Connect to TDengine

1. Launch the qStudio application, select "Server" and "Add Server..." from the menu items, then choose TDengine from the Server Type dropdown.

   ![](../../assets/qstudio-01.png)

2. Configure the TDengine connection by entering the host address, port number, username, and password. If TDengine is deployed on the local machine, you can just enter the username and password, with the default username being root and the default password being taosdata. Click "Test" to check if the connection is available. If the TDengine Java connector is not installed on the local machine, qStudio will prompt to download and install it.

   ![](../../assets/qstudio-02.png)

3. A successful connection will be displayed as shown below. If the connection fails, please check whether the TDengine service and taosAdapter are running correctly, and whether the host address, port number, username, and password are correct.

   ![](../../assets/qstudio-03.png)

4. Using qStudio to select databases and tables allows you to browse data from the TDengine service.

   ![](../../assets/qstudio-04.png)

5. You can also operate on TDengine data by executing SQL commands.

   ![](../../assets/qstudio-05.png)

6. qStudio supports features like charting based on data, please refer to [qStudio's help documentation](https://www.timestored.com/qstudio/help)

   ![](../../assets/qstudio-06.png)
