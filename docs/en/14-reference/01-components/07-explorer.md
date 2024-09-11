---
title: Graphic User Interface
sidebar_label: Taos-Explorer
description: User guide about taosExplorer
---

taos-explorer is a web service which provides GUI based interactive database management tool. To ensure the best experience when accessing taosExplorer, please use Chrome version 79 or higher, Edge version 79 or higher.

## Install

taos-explorer is delivered in the TDengine server package since version 3.3.0.0. After installing TDengine server, you will get the `taos-explorer` service.

## Configure

The configuration file of `taos-explorer` service is `/etc/taos/explorer.toml` on Linux platform, the key items in the configuration are like below:

``` toml
port = 6060
cluster = "http://localhost:6041"
```

The description of these two parameters: 

- port：taos-explorer service port
- cluster：The end point of the TDengine cluster for the taos-explorer to manage. It supports only websocket connection, so this address is actually the end point of `taosAdapter` service in the TDengine cluster.

## Start & Stop

Before starting the service, please first make sure the configuration is correct, and the TDengine cluster (majorly including `taosd` and `taosAdapter` services) are already alive and working well.

### Linux

On Linux system you can use `systemctl` to manage the service as below:

- Start the service: `systemctl start taos-explorer`

- Stop the service: `systemctl stop taos-explorer`

- Restart the service: `systemctl restart taos-explorer`

- Check service status: `systemctl status taos-explorer`

## Register & Logon

### Register

After installing, configuring and starting, you can use your browser to access taos-explorer using address `http://ip:6060`. At this time, if you have not registered before, the registering page will first show up. You need to enter your valid enterprise email, receive the activation code, then input the code. Congratulations, you have registered successfully.

### Logon

After registering, you can use your user name and corresponding password in the database system to logon. The default username is `root`, but you can change it to use another one. After loggin into the system, you can view or manage databases, create super tables, create child tables, or view the data in the database. 

There are some functionalities only available to enterprise users, you can view and experience but can't really use them.
