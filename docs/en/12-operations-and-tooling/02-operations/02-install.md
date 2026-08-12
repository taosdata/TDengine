---
sidebar_label: Download and Install
title: Download and Install
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import PkgList from "/src/components/PkgList";

The standard TDengine server package includes `taosd`, `taosAdapter`, `taosc`, the `taos` shell, taosdump, taosBenchmark, TDinsight installation scripts, and example code. A Lite package is also available when you only need the server and C/C++ client support.

TDengine provides Deb and RPM packages for Linux, a generic tar.gz package, an `apt-get` repository, a Windows client installer, and macOS packages.

The download lists in the installation steps below are for the **Community Edition (TDengine TSDB-OSS)**. For the Enterprise Edition, get the package for your platform and version from the [TDengine Product Download Center](https://www.taosdata.com/en/download-center?product=TDengine+TSDB-Enterprise).

## Requirements

The minimum Linux runtime requirements are:

1. Linux kernel 3.10.0-1160.83.1.el7.x86_64 or later.
2. GLIBC 2.17 or later on x64, or 2.27 or later on ARM.

Building from source additionally requires CMake 3.26.4 or later and GCC 9.3.1 or later.

## Install

:::note

- Starting with TDengine v3.0.6.0, taosTools is no longer distributed separately. Its tools are included in the TDengine server package.
- When installing the first node, leave the `Enter FQDN:` prompt empty. For later nodes, enter the FQDN of any available node in the existing cluster, or configure it before starting the new node.

:::

<Tabs>
<TabItem label="tar.gz" value="tarinst">

1. Download the generic Linux package:
   <PkgList productName="TDengine TSDB-OSS" platform="Linux-Generic"/>
2. Extract and install it:

   ```bash
   tar -zxvf tdengine-tsdb-oss-{{VERSION}}-linux-x64.tar.gz
   sudo ./install.sh
   ```

Use `./install.sh -e no` for a non-interactive installation, or `./install.sh -h` to list all options.

</TabItem>
<TabItem label="Deb" value="debinst">

1. Download the Ubuntu package:
   <PkgList productName="TDengine TSDB-OSS" platform="Linux-Ubuntu"/>
2. Install it:

   ```bash
   sudo dpkg -i tdengine-tsdb-oss-{{VERSION}}-linux-x64.deb
   ```

</TabItem>
<TabItem label="RPM" value="rpminst">

1. Download the Red Hat package:
   <PkgList productName="TDengine TSDB-OSS" platform="Linux-Red Hat"/>
2. Install it:

   ```bash
   sudo rpm -ivh tdengine-tsdb-oss-{{VERSION}}-linux-x64.rpm
   ```

</TabItem>
<TabItem label="apt-get" value="apt-get">

Configure the stable package repository and install TDengine:

```bash
wget -qO - http://repos.taosdata.com/tdengine.key | sudo apt-key add -
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-stable stable main" | sudo tee /etc/apt/sources.list.d/tdengine-stable.list
sudo apt-get update
apt-cache policy tdengine-tsdb
sudo apt-get install tdengine-tsdb
```

For beta packages, replace the repository line with:

```bash
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-beta beta main" | sudo tee /etc/apt/sources.list.d/tdengine-beta.list
```

This installation method supports Debian and Ubuntu only.

</TabItem>
<TabItem label="Windows" value="windows">

1. Download the Windows client installer:
   <PkgList productName="TDengine TSDB-OSS Client" platform="Windows"/>
2. Run the installer and follow the on-screen instructions. The default installation directory is `C:\\TDengine`. You can select an installation root directory. If the selected path does not end in `TDengine`, the installer creates a `TDengine` directory under it. For example, selecting `D:\\apps` installs TDengine in `D:\\apps\\TDengine`; selecting `D:\\apps\\TDengine` does not create a duplicate directory. An upgrade continues to use the existing installation directory and does not allow it to be changed.

Starting with TDengine v3.1.0.0, only the Windows client package is available for the Community Edition. The installer requires Microsoft Visual C++ Redistributable 2015-2022 x64 14.44 or later and can install its bundled runtime when necessary.

</TabItem>
<TabItem label="macOS" value="macos">

1. Download the macOS package:
   <PkgList productName="TDengine TSDB-OSS" platform="macOS"/>
2. Run the installer. If macOS blocks it, Control-click the package and select **Open**.

</TabItem>
</Tabs>

## Start Services

<Tabs>
<TabItem label="Linux" value="linux">

Start the installed services with `systemctl`:

```bash
sudo systemctl start taosd
sudo systemctl start taosadapter
sudo systemctl start taoskeeper
sudo systemctl start taos-explorer
```

You can also run `start-all.sh`. Use `systemctl stop`, `restart`, or `status` to manage an individual service. If `systemd` is unavailable, run `/usr/local/taos/bin/taosd` directly.

</TabItem>
<TabItem label="Windows" value="windows">

If you installed the Windows server package, the installer adds the actual installation directory to the `PATH` environment variable. After installation, open a new Command Prompt window as administrator, change to the actual installation directory, and run the following commands to start, inspect, or stop all services:

```cmd
start-all.bat
start-all.bat status
start-all.bat stop
```

To start services only and skip connectivity checks and Snode/Xnode initialization, run `start-all.bat -S`.

</TabItem>
<TabItem label="macOS" value="macos">

Use `launchctl` or `start-all.sh`:

```bash
sudo launchctl start com.tdengine.taosd
sudo launchctl start com.tdengine.taosadapter
sudo launchctl start com.tdengine.taoskeeper
sudo launchctl start com.tdengine.taos-explorer
```

</TabItem>
</Tabs>

## Customize taosd Startup with a systemd Drop-in

Use a drop-in file to preserve custom startup settings across upgrades. This example changes the startup retry accounting window to 60 seconds:

```bash
sudo mkdir -p /etc/systemd/system/taosd.service.d
sudo tee /etc/systemd/system/taosd.service.d/60-retry.conf >/dev/null <<'EOF'
[Service]
StartLimitInterval=60s
EOF
sudo systemctl daemon-reload
sudo systemctl restart taosd
```

The drop-in only needs to contain overridden or additional fields.

## Installation Layout

The default Linux installation creates the following paths:

| Path | Purpose |
| --- | --- |
| `/usr/local/taos/bin` | TDengine executables |
| `/usr/local/taos/driver` | Dynamic libraries |
| `/usr/local/taos/examples` | Connector examples |
| `/usr/local/taos/include` | Public C headers |
| `/etc/taos/taos.cfg` | Default configuration file |
| `/var/lib/taos` | Default data directory |
| `/var/log/taos` | Default log directory |

The executables include `taosd`, `taos`, `taosdump`, `taosBenchmark`, `taosadapter`, and maintenance scripts such as `remove.sh` and `set_core.sh`.

For IPv4 and IPv6 configuration, see [Network and FQDN Configuration](./08-network.md).
