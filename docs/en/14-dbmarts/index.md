---
sidebar_label: DB Mart
title: DB Mart
description: This document describes DB Mart, a public repository of databases in TDengine Cloud.
---

TDengine Cloud provides a database mart for all customers, customers can view all the databases that have been published by TDengine Cloud. You can click on the switch button under the PUBLIC or PRIVATE type of each database, you can directly go to the "Explorer" page of the database to get access to that database. If the database type is PRIVATE, customers need to enter the corresponding access code to enter. Customers can only get read access to the database and perform query operations on the database.

Each customer can publish his own database to this database mart. Customers only need to go to the "Explorer" page, then select one database and click "Manage Database Privilege" button of the popped menu to open the "Database Access Control" page. In the page, click the "DB Mart Access" tab, and make a few simple configurations to publish any database to this database mart.

If you want to try out TDgpt's functionality, please click the toggle button next to the **Time Series Prediction Analysis Dataset** public database to enter the database browser for this database, and run the following SQL.

```SQL
  select _FROWTS, forecast(val, 'algo=tdtsfm_1,start=1324915200000,rows=300') from forecast.electricity_demand_sub;
```

For TDgpt details, please refer to [TDgpt Document](/advanced/tdgpt/).
