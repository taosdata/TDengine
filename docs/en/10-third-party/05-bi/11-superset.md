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
- The Python connector taospy version 2.7.18 or higher is installed, [Install reference link](https://pypi.org/project/taospy/).
- The Python connector (WebSocket) taos-ws-py version 0.3.9 or higher is installed, [Install reference link](https://pypi.org/project/taos-ws-py/).

## Configure Data Source

1. Enter the new database connection page, "Superset" -> "Setting" -> "Database Connections" -> "+DATABASE".

2. Select the last item "Other" from the drop-down list of "SUPPORTED DATABASES".

3. Enter a connection name in "DISPLAY NAME", you can enter any name.

4. Fill in the database connection string.  
   Fill in the "SQLAlchemy URI" in the following format, and make sure the information is correct.

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

   The TDengine database installed on this machine provides WebSocket service port 6041, using the default username and password, "SQLALCHEMY URI" is:

   ```bash
   taosws://root:taosdata@localhost:6041
   ```

5. Test the connection.  
   Click "TEST CONNECTION" to check if the connection is successful. After the test passes, click the "CONNECT" button to save the configuration.

## Data Analysis

### Data preparation

There is no difference in the use of TDengine data source compared to other data sources. Here is a brief introduction to basic data queries:

1. Click the "+" button in the upper right corner of the Superset interface, select "SQL query", and enter the query interface.  
2. Select the previously created "TDengine" data source from the "Database" dropdown list in the upper left corner.
3. Select the name of the database to be operated on from the drop-down list of "SCHEMA" (system libraries are not displayed).
4. "SEE TABLE SCHEMA" select the name of the super table or regular table to be operated on (sub tables are not displayed).  
5. Subsequently, the schema information of the selected table will be displayed in the following area.
6. In the "SQL" editor area, any SQL statement that conforms to TDengine syntax can be entered for execution.  

### Smart Meter Example

We chose two popular templates from the "Superset Chart" template to showcase their effects, using smart meter data as an example:

1. "Aggregate" Type, which displays the maximum voltage value collected per minute during the specified time period in Group 4.
   ![superset-demo1](./superset-demo1.webp)

2. "RAW RECORDS" Type, which displays the collected values of current and voltage during the specified time period in Group 4.  
    ![superset-demo2](./superset-demo2.webp)
