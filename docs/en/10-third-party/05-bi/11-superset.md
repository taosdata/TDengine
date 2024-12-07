---
sidebar_label: SuperSet
title: 与 SuperSet 的集成
toc_max_heading_level: 4
---
Apache Superset is a modern enterprise level business intelligence (BI) web application primarily used for data exploration and visualization.
It is supported by the Apache Software Foundation and is an open source project with an active community and rich ecosystem.
Apache Superset provides an intuitive user interface that makes creating, sharing, and visualizing data simple, while supporting multiple data sources and rich visualization options.

Through the Python connector of TDengine, Superset can support TDengine data sources and provide functions such as data presentation and analysis

## Install Apache Superset
Ensure that Apache SuperSet v2.1.0 or above is installed. If not, please visit [official website]（https://superset.apache.org/）to install


## Install TDengine
Both TDengine Enterprise Edition and Community Edition are supported, with version requirements of 3.0 or higher

## Install TDengine Python Connector
The Python connector of TDengine comes with a connection driver that supports SuperSet in versions 2.1.17 and later, which will be automatically installed in the SuperSet directory and provide data source services
The connection uses the WebSocket protocol, so it is necessary to install the taos ws py component of TDengine separately. The complete installation script is as follows:
```bash
pip3 install taospy
pip3 install taos-ws-py
```

## Configure TDengine Connection In SuperSet
Step 1, enter the new database connection page  
SuperSet -> Setting-> Database Connections -> +DATABASE   
Step 2, select TDengine database connection  
Select the "TDengine" option from the drop-down list of Supported DATABASES. If there is no TDengine option in the drop-down list, please confirm that the steps of installing SuperSet first and then installing the Python connector are correct  
Step 3, give the connection a name in the PLAY NAME and fill it in freely  
Step 4: The SQLALCHEMY URL * field is a key connection information string, and it must be filled in correctly  
```bash
    Connection string format： taosws://user:password@host:port
```
| Parameter  | Parameter Description |
|:-------:|:---------:|
|user: | Username for logging into TDengine database
|password: | Password for logging into TDengine database
|host: | Name of the host where the TDengine database is located
|port: | The port that provides WebSocket services, default is 6041
         

Example:
The TDengine database installed on this machine provides WebSocket service port 6041, using the default username and password, and the connection string is:  
```bash
taosws://root:taosdata@localhost:6041  
```
Step 5, configure the connection string, click "TEST CONNECTION" to test if the connection can be successful. After passing the test, click the CONNECT button to complete the connection
       

## Start
There is no difference in the use of TDengine data source compared to other data sources. Here is a brief introduction to basic data queries:  
1. Click the "+" button in the upper right corner of the SuperSet interface, select SQL query, and enter the query interface  
2. Select the TDengine data source that has been created earlier from the dropdown list in the upper left corner  
3. Select the name of the database to be operated on from the drop-down list of schema (system libraries are not displayed)  
4. SEE TABLE Schematic: Select the name of the super table or regular table to be operated on (sub tables are not displayed)  
5. Subsequently, the schema information of the selected table will be displayed in the following area  
6. In the SQL editor area, any SQL statement that conforms to TDengine syntax can be entered for execution  

## Example
We have selected two templates from the SuperSet Chart template that are more procedural to demonstrate their effectiveness, using smart meter data as an example:  

The first type is Aggregate, which displays the maximum voltage value collected per minute during the specified time period in Group 4  

![superset-demo1](./superset-demo1.jpeg)  

The second type is RAW RECORDS, which displays the collected values of current and voltage during the specified time period in Group 4  

![superset-demo2](./superset-demo2.jpeg)  