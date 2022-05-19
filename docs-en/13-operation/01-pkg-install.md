---
title: Install & Uninstall
description: Install, Uninstall, Start, Stop and Upgrade
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

TDengine community version provides dev and rpm package for users to choose based on the system environment. deb supports Debian, Ubuntu and systems derived from them. rpm supports CentOS, RHEL, SUSE and systems derived from them. Furthermore, tar.gz package is provided for enterprise customers.

## Install

<Tabs>
<TabItem label="Install Deb" value="debinst">

1. Download deb package from official website, for example TDengine-server-2.4.0.7-Linux-x64.deb
2. In the directory where the package is located, execute below command

```bash
$ sudo dpkg -i TDengine-server-2.4.0.7-Linux-x64.deb
(Reading database ... 137504 files and directories currently installed.)
Preparing to unpack TDengine-server-2.4.0.7-Linux-x64.deb ...
TDengine is removed successfully!
Unpacking tdengine (2.4.0.7) over (2.4.0.7) ...
Setting up tdengine (2.4.0.7) ...
Start to install TDengine...

System hostname is: ubuntu-1804

Enter FQDN:port (like h1.taosdata.com:6030) of an existing TDengine cluster node to join
OR leave it blank to build one:

Enter your email address for priority support or enter empty to skip:
Created symlink /etc/systemd/system/multi-user.target.wants/taosd.service → /etc/systemd/system/taosd.service.

To configure TDengine : edit /etc/taos/taos.cfg
To start TDengine     : sudo systemctl start taosd
To access TDengine    : taos -h ubuntu-1804 to login into TDengine server


TDengine is installed successfully!
```

</TabItem>

<TabItem label="Install RPM" value="rpminst">

1. Download rpm package from official website, for example TDengine-server-2.4.0.7-Linux-x64.rpm；
2. In the directory where the package is located, execute below command

```
$ sudo rpm -ivh TDengine-server-2.4.0.7-Linux-x64.rpm
Preparing...                          ################################# [100%]
Updating / installing...
   1:tdengine-2.4.0.7-3               ################################# [100%]
Start to install TDengine...

System hostname is: centos7

Enter FQDN:port (like h1.taosdata.com:6030) of an existing TDengine cluster node to join
OR leave it blank to build one:

Enter your email address for priority support or enter empty to skip:

Created symlink from /etc/systemd/system/multi-user.target.wants/taosd.service to /etc/systemd/system/taosd.service.

To configure TDengine : edit /etc/taos/taos.cfg
To start TDengine     : sudo systemctl start taosd
To access TDengine    : taos -h centos7 to login into TDengine server


TDengine is installed successfully!
```

</TabItem>

<TabItem label="Install tar.gz" value="tarinst">

1. Download the tar.gz package, for example TDengine-server-2.4.0.7-Linux-x64.tar.gz；
   2、In the directory where the package is located, firstly decompress the file, then switch to the sub-directory generated in decompressing, i.e. "TDengine-enterprise-server-2.4.0.7/" in this example, and execute the `install.sh` script.

```bash
$ tar xvzf TDengine-enterprise-server-2.4.0.7-Linux-x64.tar.gz
TDengine-enterprise-server-2.4.0.7/
TDengine-enterprise-server-2.4.0.7/driver/
TDengine-enterprise-server-2.4.0.7/driver/vercomp.txt
TDengine-enterprise-server-2.4.0.7/driver/libtaos.so.2.4.0.7
TDengine-enterprise-server-2.4.0.7/install.sh
TDengine-enterprise-server-2.4.0.7/examples/
...

$ ll
total 43816
drwxrwxr-x  3 ubuntu ubuntu     4096 Feb 22 09:31 ./
drwxr-xr-x 20 ubuntu ubuntu     4096 Feb 22 09:30 ../
drwxrwxr-x  4 ubuntu ubuntu     4096 Feb 22 09:30 TDengine-enterprise-server-2.4.0.7/
-rw-rw-r--  1 ubuntu ubuntu 44852544 Feb 22 09:31 TDengine-enterprise-server-2.4.0.7-Linux-x64.tar.gz

$ cd TDengine-enterprise-server-2.4.0.7/

 $ ll
total 40784
drwxrwxr-x  4 ubuntu ubuntu     4096 Feb 22 09:30 ./
drwxrwxr-x  3 ubuntu ubuntu     4096 Feb 22 09:31 ../
drwxrwxr-x  2 ubuntu ubuntu     4096 Feb 22 09:30 driver/
drwxrwxr-x 10 ubuntu ubuntu     4096 Feb 22 09:30 examples/
-rwxrwxr-x  1 ubuntu ubuntu    33294 Feb 22 09:30 install.sh*
-rw-rw-r--  1 ubuntu ubuntu 41704288 Feb 22 09:30 taos.tar.gz

$ sudo ./install.sh

Start to update TDengine...
Created symlink /etc/systemd/system/multi-user.target.wants/taosd.service → /etc/systemd/system/taosd.service.
Nginx for TDengine is updated successfully!

To configure TDengine : edit /etc/taos/taos.cfg
To configure Taos Adapter (if has) : edit /etc/taos/taosadapter.toml
To start TDengine     : sudo systemctl start taosd
To access TDengine    : use taos -h ubuntu-1804 in shell OR from http://127.0.0.1:6060

TDengine is updated successfully!
Install taoskeeper as a standalone service
taoskeeper is installed, enable it by `systemctl enable taoskeeper`
```

:::info
Some configuration will be prompted for users to provide when install.sh is executing, the interactive mode can be disabled by executing `./install.sh -e no`. `./install -h` can show all parameters and detailed explanation.

:::

</TabItem>
</Tabs>

:::note
When installing on the first node in the cluster, when "Enter FQDN:" is prompted, nothing needs to be provided. When installing on following nodes, when "Enter FQDN:" is prompted, the end point of the first dnode in the cluster can be input if it has been already up; or just ignore it and configure later after installation is done.

:::

## Uninstall

<Tabs>
<TabItem label="Uninstall Deb" value="debuninst">

Deb package of TDengine can be uninstalled as below:

```bash
$ sudo dpkg -r tdengine
(Reading database ... 137504 files and directories currently installed.)
Removing tdengine (2.4.0.7) ...
TDengine is removed successfully!

```

</TabItem>

<TabItem label="Uninstall RPM" value="rpmuninst">

RPM package of TDengine can be uninstalled as below:

```
$ sudo rpm -e tdengine
TDengine is removed successfully!
```

</TabItem>

<TabItem label="Uninstall tar.gz" value="taruninst">

tar.gz package of TDengine can be uninstalled as below:

```
$ rmtaos
Nginx for TDengine is running, stopping it...
TDengine is removed successfully!

taosKeeper is removed successfully!
```

</TabItem>
</Tabs>

:::note

- It's strongly suggested not to use multiple kinds of installation packages on single host TDengine
- After deb package is installed, if the installation directory is removed manually so that uninstall or reinstall can't succeed, it can be resolved by cleaning up TDengine package information as below command and then reinstalling.

```bash
   $ sudo rm -f /var/lib/dpkg/info/tdengine*
```

- After rpm package is installed, if the installation directory is removed manually so that uninstall or reinstall can't succeed, it can be resolved by cleaning up TDengine package information as below command and then reinstalling.

```bash
   $ sudo rpm -e --noscripts tdengine
```

:::

## Installation Directory

TDengine is installed at /usr/local/taos if successful.

```bash
$ cd /usr/local/taos
$ ll
$ ll
total 28
drwxr-xr-x  7 root root 4096 Feb 22 09:34 ./
drwxr-xr-x 12 root root 4096 Feb 22 09:34 ../
drwxr-xr-x  2 root root 4096 Feb 22 09:34 bin/
drwxr-xr-x  2 root root 4096 Feb 22 09:34 cfg/
lrwxrwxrwx  1 root root   13 Feb 22 09:34 data -> /var/lib/taos/
drwxr-xr-x  2 root root 4096 Feb 22 09:34 driver/
drwxr-xr-x 10 root root 4096 Feb 22 09:34 examples/
drwxr-xr-x  2 root root 4096 Feb 22 09:34 include/
lrwxrwxrwx  1 root root   13 Feb 22 09:34 log -> /var/log/taos/
```

During the installation process:

- Configuration directory, data directory, and log directory are created automatically if they don't exist
- The default configuration file is located at /etc/taos/taos.cfg, which is a copy of /usr/local/taos/cfg/taos.cfg if not existing
- The default data directory is /var/lib/taos, which is a soft link to /usr/local/taos/data
- The default log directory is /var/log/taos, which is a soft link to /usr/local/taos/log
- The executables at /usr/local/taos/bin are linked to /usr/bin
- The DLL files at /usr/local/taos/driver are linked to /usr/lib
- The header files at /usr/local/taos/include are linked to /usr/include

:::note

- When TDengine is uninstalled, the configuration /etc/taos/taos.cfg, data directory /var/lib/taos, log directory /var/log/taos are kept. They can be deleted manually with caution because data can't be recovered once
- When reinstalling TDengine, if the default configuration file /etc/taos/taos.cfg exists, it will be kept and the configuration file in the installation package will be renamed to taos.cfg.orig and stored at /usr/loca/taos/cfg to be used as configuration sample. Otherwise the configuration file in the installation package will be installed to /etc/taos/taos.cfg and used.

## Start and Stop

Linux system services `systemd`, `systemctl` or `service` is used to start, stop and restart TDengine. The server process of TDengine is `taosd`, which is started automatically after the Linux system is started. System operator can use `systemd`, `systemctl` or `service` to start, stop or restart TDengine server.

For example, if using `systemctl` , the commands to start, stop, restart and check TDengine server are as below:

- Start server：`systemctl start taosd`

- Stop server：`systemctl stop taosd`

- Restart server：`systemctl restart taosd`

- Check server status：`systemctl status taosd`

From version 2.4.0.0, a new independent component named as `taosAdapter` has been included in TDengine. `taosAdapter` should be started and stopped using `systemctl`.

If the server process is OK, the output of `systemctl status` is like below:

```
Active: active (running)
```

Otherwise, the output is as below:

```
Active: inactive (dead)
```

## Upgrade

There are two aspects in upgrade operation: upgrade installation package and upgrade a running server.

Upgrading package should follow the steps mentioned previously to firstly uninstall old version then install new version.

Upgrading a running server is much more complex. Firstly please check the version number of old version and new version. The version number of TDengine consists of 4 sections, only the first 3 section match can the old version be upgraded to the new version. The steps of upgrading a running server are as below:

- Stop inserting data
- Make sure all data persisted into disk
- Stop the cluster of TDengine
- Uninstall old version and install new version
- Start the cluster of TDengine
- Make some simple queries to make sure no data loss
- Make some simple data insertion to make sure the cluster works well
- Restore business data

:::warning
TDengine doesn't guarantee any lower version is compatible with the data generated by a higher version, so it's never recommended to downgrade the version.

:::
