---
sidebar_label: Data Out
title: Get Data Out of TDengine
description: A number of ways getting data out of TDengine.
---

This chapter introduces how to get data out of TDengine cloud service. Besides normal query using SQL, users can use [data subscription](../../tmq) which is provided by the message queue component inside TDengine to access the data stored in TDengine. `taosdump`, which is a tool provided by TDengine, can be used to dump the data stored in TDengine cloud service into files. `taosX`, which is another tool provided by TDengine, can be used to sync up the data in one TDengine cloud service into another. Furthermore, 3rd party tools, like prometheus, can also be used to write data into TDengine.