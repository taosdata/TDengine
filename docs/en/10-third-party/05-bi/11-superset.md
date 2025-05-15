---
sidebar_label: Superset
title: Integration With Superset
toc_max_heading_level: 4
---
Apache Superset is a modern enterprise level business intelligence (BI) web application primarily used for data exploration and visualization.
It is supported by the Apache Software Foundation and is an open source project with an active community and rich ecosystem.
Apache Superset provides an intuitive user interface that makes creating, sharing, and visualizing data simple, while supporting multiple data sources and rich visualization options.  

Through the Python connector of TDengine, Superset can support TDengine data sources and provide functions such as data presentation and analysis.  

## Prerequisites

Prepare the following environment:

- TDengine 3.2.3.0 and above version is installed and running normally (both Enterprise and Community versions are available).
- taosAdapter is running normally, refer to [taosAdapter](../../../tdengine-reference/components/taosadapter/).
- Apache Superset version 2.1.0 or above is already installed, refre to [Apache Superset](https://superset.apache.org/).
- Install Python connector driver, refer to [Python Client Library](../../../tdengine-reference/client-libraries/python).

:::tip
The TDengine Python connector has come with the Superset connection driver since version 2.7.18. It will be installed in the corresponding directory of Superset and provide data source services to Superset. Later, this driver was included in the Apache Superset 4.1.2 version. Therefore, there is no need to include it in the connector anymore. This driver was removed in TDengine Python v2.8.0. The matching table is as follows:
| TDengine Python Connector Version | Matching Apache Superset Version  |
|:--------------------------------- |:--------------------------------  |
| 2.7.18 ~ 2.7.23                   | 2.1.0 ~ 4.1.1                     |  
| 2.8.0 and above                   | 4.1.2 and above                   |

:::

## Configure Data Source

**Step 1**, enter the new database connection page, [Superset] -> [Setting] -> [Database Connections] -> [+DATABASE].

**Step 2**, select TDengine database connection, select the `TDengine` option from the drop-down list of [SUPPORTED DATABASES]. 

:::tip
If there is no TDengine option in the drop-down list, please confirm that the steps of installing, `Superset` is first and `Python Connector` is second.
:::

**Step 3**, write a name of connection in [DISPLAY NAME]. 

**Step 4**, The [SQLALCHEMY URL] field is a key connection information string, and it must be filled in correctly.

```bash
taosws://user:password@host:port
```

| Parameter  | <center>Parameter Description</center>                      |
|:---------- |:---------------------------------------------------------   |
|user        | Username for logging into TDengine database                 |   
|password    | Password for logging into TDengine database                 |
|host        | Name of the host where the TDengine database is located     |
|port        | The port that provides WebSocket services, default is 6041  |

Example: 

The TDengine database installed on this machine provides WebSocket service port 6041, using the default username and password, `SQLALCHEMY URL` is:

```bash
taosws://root:taosdata@localhost:6041  
```

**Step 5**, configure the connection string, click "TEST CONNECTION" to test if the connection can be successful. After passing the test, click the "CONNECT" button to complete the connection.
       
## Data Analysis

### Data preparation

There is no difference in the use of TDengine data source compared to other data sources. Here is a brief introduction to basic data queries: 

1. Click the [+] button in the upper right corner of the Superset interface, select [SQL query], and enter the query interface.  
2. Select the `TDengine` data source that has been created earlier from the dropdown list of [DATABASES] in the upper left corner.
3. Select the name of the database to be operated on from the drop-down list of [SCHEMA] (system libraries are not displayed).
4. [SEE TABLE SCHEMA] select the name of the super table or regular table to be operated on (sub tables are not displayed).  
5. Subsequently, the schema information of the selected table will be displayed in the following area.
6. In the `SQL` editor area, any `SQL` statement that conforms to `TDengine` syntax can be entered for execution.  

### Smart Meter Example

We chose two popular templates from the [Superset Chart] template to showcase their effects, using smart meter data as an example:

1. `Aggregate` Type, which displays the maximum voltage value collected per minute during the specified time period in Group 4.
   ![superset-demo1](./superset-demo1.webp)

2. `RAW RECORDS` Type, which displays the collected values of current and voltage during the specified time period in Group 4.  
    ![superset-demo2](./superset-demo2.webp)    