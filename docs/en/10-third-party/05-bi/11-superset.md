---
sidebar_label: Superset
title: Integration With Superset
toc_max_heading_level: 4
---
Apache Superset is a modern enterprise level business intelligence (BI) web application primarily used for data exploration and visualization.
It is supported by the Apache Software Foundation and is an open source project with an active community and rich ecosystem.
Apache Superset provides an intuitive user interface that makes creating, sharing, and visualizing data simple, while supporting multiple data sources and rich visualization options.  

Through the Python connector of TDengine, Superset can support TDengine data sources and provide functions such as data presentation and analysis  

## Prerequisites

Prepare the following environment:
- TDengine Cloud Server is running normally 
- taosAdapter is running normally, refer to [taosAdapter](https://docs.tdengine.com/tdengine-reference/components/taosadapter/)
- Apache Superset version 2.1.0 or above is already installed, refre to [Apache Superset](https://superset.apache.org/)


## Install TDengine Python Connector

The Python connector of TDengine comes with a connection driver that supports Superset in versions 2.1.18 and later, which will be automatically installed in the Superset directory and provide data source services.  
The connection uses the WebSocket protocol, so it is necessary to install the `taos-ws-py` component of TDengine separately. The complete installation script is as follows:  
```bash
pip3 install taospy
pip3 install taos-ws-py
```

## Configure TDengine Cloud Connection In Superset

**Step 1**, enter the new database connection page, "Superset" → "Setting" → "Database Connections" → "+DATABASE"   
**Step 2**, select TDengine database connection, select the "TDengine" option from the drop-down list of "SUPPORTED DATABASES". 
:::tip
If there is no TDengine option in the drop-down list, please confirm that the steps of installing, `Superset` is first and `Python Connector` is second.
:::
**Step 3**, write a name of connection in "DISPLAY NAME"  
**Step 4**, the "SQLALCHEMY URL" field is a key connection information string, copy and paste the following string  
```bash
taosws://gw.cloud.taosdata.com?token=0df909712bb345d6ba92253d3e6fb635d609c8ff
```
**Step 5**, click "TEST CONNECTION" to test if the connection can be successful. After passing the test, click the "CONNECT" button to complete the connection  
       

## Start

There is no difference in the use of TDengine data source compared to other data sources. Here is a brief introduction to basic data queries:  
1. Click the "+" button in the upper right corner of the Superset interface, select "SQL query", and enter the query interface  
2. Select the "TDengine" data source that has been created earlier from the dropdown list of "DATABASES" in the upper left corner
3. Select the name of the database to be operated on from the drop-down list of "SCHEMA" (system libraries are not displayed)  
4. "SEE TABLE SCHEMA" select the name of the super table or regular table to be operated on (sub tables are not displayed)  
5. Subsequently, the schema information of the selected table will be displayed in the following area  
6. In the SQL editor area, any SQL statement that conforms to TDengine syntax can be entered for execution  

## Example

We chose two popular templates from the Superset Chart template to showcase their effects, using smart meter data as an example:  

1. "Aggregate" Type, which displays the maximum voltage value collected per minute during the specified time period in Group 4  

  ![superset-demo1](./superset-demo1.jpeg)  

2. "RAW RECORDS" Type, which displays the collected values of current and voltage during the specified time period in Group 4  

  ![superset-demo2](./superset-demo2.jpeg)  