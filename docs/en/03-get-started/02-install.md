---
title: Installation Guide
sidebar_label: Installation
---

## Linux Platform

On Linux, TDengine Enteprise can be installed with a single package named like  `TDengine-enterprise-<version>-<OS>-<platform>.tar.gz` (for example `TDengine-enterprise-3.1.0.3-Linux-x64.tar.gz`)

The TDengine Enterprise package includes the following components: 
- `taosd`: the TDengine core
- `taosAdapter`: a service that provides RESTful and WebSocket interfaces to TDengine
- `taosKeeper`: a service that reports and records monitoring metrics
- `libtaos.so`: the native client SDK (C client library)
- `libtaosws.so`: the WebSocket client SDK (C client library)
- `taosX`: a zero-code platform for data ingestion, replication, backup, and restore
- `taosExplorer`: a graphical user interface for TDengine
- data source SDK: the SDK called by taosX to connect with each data source

Pre installation dependency check:

- Check if `JDK1.8` or higher is installed (checked through Shell command ```java -version```)

The steps are as below:

- Obtain the TDengine Server installation package.
- In the directory where the package is located, use `tar` to decompress the package.
- Run the `install.sh` script to install TDengine.
- The default installation localtio is /usr/local/taos
- start-all.sh can be used to start all services required by TDengine Enterprise on local machine
- Corresponding, stop-all.sh can be used to stop all services started by start-all.sh

For example: Note: Replace &lt;version&gt; with your version of TDengine.

```bash
tar -zxvf TDengine-enterprise-<version>-Linux-x64.tar.gz
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

On Linux, TDengine Enteprise can be installed with a single package named like  `TDengine-enterprise-<version>-<OS>-<platform>.tar.gz` (for example `TDengine-enterprise-3.1.0.3-Linux-x64.tar.gz`)

The TDengine Enterprise package includes the following components: 
- `taosd`: the TDengine core
- `taosAdapter`: a service that provides RESTful and WebSocket interfaces to TDengine
- `taosKeeper`: a service that reports and records monitoring metrics
- `libtaos.so`: the native client SDK (C client library)
- `libtaosws.so`: the WebSocket client SDK (C client library)
- `taosX`: a zero-code platform for data ingestion, replication, backup, and restore
- `taosExplorer`: a graphical user interface for TDengine
- data source SDK: the SDK called by taosX to connect with each data source

Pre installation dependency check:

- Check if `JDK1.8` or higher is installed (checked through CMD command ```java -version```)
- Check if the `Visual C++ runtime library` is installed (check if `Microsoft Visual C++ Redistributable` is present in `Control Panel - Programs and Features`)，if had not installed, you can download and install it from [VC Runtime Library](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist?view=msvc-170)

The steps are as below:

- Download and install the TDengine Enterprise installation package.
- To uninstall taosX, run the `uninstall_TDengine.exe` file.
- To start or stop the `taosx` service, run the `sc.exe start/stop taosd` command.
- To start or stop the `taosx` service, run the `sc.exe start/stop taosadapter` command.
- To start or stop the `taosx` service, run the `sc.exe start/stop taoskeeper` command.
- To start or stop the `taosx` service, run the `sc.exe start/stop taosx` command.
- To start or stop the taos-explorer service, run the `sc.exe start/stop taosx-explorer` command.