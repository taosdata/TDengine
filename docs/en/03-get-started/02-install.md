---
title: Installation Guide
sidebar_label: Installation
---

## Introduction

There are two TDengine Enterprise installation packages: 
- `TDengine-server-<version>-<OS>-<platform>.tar.gz` (for example `TDengine-server-3.1.0.3-Linux-x64.tar.gz`)
- `taosX-<version>-<OS-<platform>.tar.gz` (for example `taosX-1.2.0-Linux-x64.tar.gz`)

The TDengine-server package includes the following components: 
- `taosd`: the TDengine core
- `taosAdapter`: a service that provides RESTful and WebSocket interfaces to TDengine
- `taosKeeper`: a service that reports and records monitoring metrics
- `libtaos.so`: the native client SDK (C client library)
- `libtaosws.so`: the WebSocket client SDK (C client library)

The taosX package includes the following components:
- `taosX`: a zero-code platform for data ingestion, replication, backup, and restore
- `taosAgent`: an agent for ingesting data from certain sources into taosX
- `taosExplorer`: a graphical user interface for TDengine
- data source SDK: the SDK called by taosX or taosAgent to connect with each data source

  To use the functionalities provided by TDengine Enterprise, you need to at least install the TDengine-server package. To use data transfer capability and visual management tool, you need to also install taosX package.

## Install TDengine Server

### Linux

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
Users will be prompted to enter some configuration information when `install.sh` is executing. Run `./install.sh -e no` to disable interactive mode. Run `./install.sh -h` to show all parameters with detailed explanations.
:::

## Install taosX

### Linux

Obtain the taosX installation package. This example uses `taosx-1.0.0-linux-x64.tar.gz` as an example.

``` bash
# Decompress the installation package to a directory.
tar -zxf taosx-1.0.0-linux-x64.tar.gz
cd taosx-1.0.0-linux-x64

# Install taosX
sudo ./install.sh

# Verify the installation
taosx -V 
# taosx 1.0.0-494d280c (built linux-x86_64 2023-06-21 11:06:00 +08:00)
taosx-agent -V 
# taosx-agent 1.0.0-494d280c (built linux-x86_64 2023-06-21 11:06:01 +08:00)

# Uninstall taosX
cd /usr/local/taosx
sudo ./uninstall.sh
```

**Frequently Asked Questions:**

1. What files are created during the installation process?
    * /usr/bin: taosx, taosx-agent, taos-explorer
    * /usr/local/taosx/plugins: influxdb, mqtt, opc
    * /etc/systemd/system:taosx.service, taosx-agent.service, taos-explorer.service
    * /usr/local/taosx: uninstall.sh 
    * /etc/taox: agent.toml, explorer.toml

2. Why does the `taosx -V` command return `"Command not found"`?
    * Ensure that all files have been copied to the appropriate directories.
    ``` bash
    ls /usr/bin | grep taosx
    ```

### Windows

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
