---
title: Installation Guide
sidebar_label: Installation
---

## Linux Platform

On Linux, TDengine Enteprise can be installed with a single package named like  `TDengine-Enterprise-<version>-<OS>-<platform>.tar.gz` (for example `TDengine-server-3.1.0.3-Linux-x64.tar.gz`)

The TDengine-Enterprise package includes the following components: 
- `taosd`: the TDengine core
- `taosAdapter`: a service that provides RESTful and WebSocket interfaces to TDengine
- `taosKeeper`: a service that reports and records monitoring metrics
- `libtaos.so`: the native client SDK (C client library)
- `libtaosws.so`: the WebSocket client SDK (C client library)
- `taosX`: a zero-code platform for data ingestion, replication, backup, and restore
- `taosAgent`: an agent for ingesting data from certain sources into taosX
- `taosExplorer`: a graphical user interface for TDengine
- data source SDK: the SDK called by taosX or taosAgent to connect with each data source

The steps are as below:

1. Obtain the TDengine Server installation package.
2. In the directory where the package is located, use `tar` to decompress the package.
3. Run the `install.sh` script to install TDengine.

For example: Note: Replace <version\> with your version of TDengine.

```bash
tar -zxvf TDengine-server-<version>-Linux-x64.tar.gz
```

Run the `install.sh` script to install TDengine.

```bash
sudo ./install.sh
```

:::info
1. You will be prompted to enter some configuration information and the configuration will be automated for single machine installation by default. 
2. Run `./install.sh -e no` to disable interactive mode and automatic configuration. 
3. Run `./install.sh -h` to show all parameters with detailed explanations.
:::

## Windows Platform

On Windows, to use TDengine Enterprise, you need to install two packages: TDengine server and taosX. In this section we will only demonstrate how to install taosX package. The installation of TDengine server is similar to taosX. 

- Download and install the taosX installation package.
- To uninstall taosX, run the `uninstall_taosx.exe` file.
- To start or stop the `taosx` service, run the `sc start/stop taosx` command.
- To start or stop the `taosx-agent` service, run the `sc start/stop taosx-agent` command.
- To start or stop the taos-explorer service, run the `sc start/stop taosx-explorer` command.
By default, taosX is installed to the `C:\Program Files\taosX` directory.
~~~
├── bin
│   ├── taosx.exe
│   ├── taosx-srv.exe
│   ├── taosx-srv.xml
│   ├── taosx-agent.exe
│   ├── taosx-agent-srv.exe
│   ├── taosx-agent-srv.xml
│   ├── taos-explorer.exe
│   ├── taos-explorer-srv.exe
│   └── taos-explorer-srv.xml
├── plugins
│   ├── influxdb
│   │   └── taosx-inflxdb.jar
│   ├── mqtt
│   │   └── taosx-mqtt.exe
│   ├── opc
│   |    └── taosx-opc.exe
│   ├── pi
│   |   └── taosx-pi.exe
│   |   └── taosx-pi-backfill.exe
│   |   └── ...
└── config
│   ├── agent.toml
│   ├── explorer.toml
├── uninstall_taosx.exe
├── uninstall_taosx.dat
~~~
