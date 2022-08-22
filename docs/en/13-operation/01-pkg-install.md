---
title: Install and Uninstall
description: Install, Uninstall, Start, Stop and Upgrade
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

This document gives more information about installing, uninstalling, and upgrading TDengine.

## Install

See [Quick Install](../../get-started/package) for preliminary steps.



## Installation Directory

TDengine is installed at /usr/local/taos if successful.

```
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

- Configuration directory, data directory, and log directory are created automatically if they don't exist
- The default configuration file is located at /etc/taos/taos.cfg, which is a copy of /usr/local/taos/cfg/taos.cfg
- The default data directory is /var/lib/taos, which is a soft link to /usr/local/taos/data
- The default log directory is /var/log/taos, which is a soft link to /usr/local/taos/log
- The executables at /usr/local/taos/bin are linked to /usr/bin
- The DLL files at /usr/local/taos/driver are linked to /usr/lib
- The header files at /usr/local/taos/include are linked to /usr/include

## Uninstall

<Tabs>
<TabItem label="apt-get uninstall" value="aptremove">

TBD

</TabItem>
<TabItem label="Uninstall Deb" value="debuninst">

Deb package of TDengine can be uninstalled as below:

```
$ sudo dpkg -r tdengine
(Reading database ... 120119 files and directories currently installed.)
Removing tdengine (3.0.0.10002) ...
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
TDengine is removed successfully!
```

</TabItem>
<TabItem label="Windows uninstall" value="windows">
Run C:\TDengine\unins000.exe to uninstall TDengine on a Windows system.
</TabItem>
</Tabs>

:::info

- We strongly recommend not to use multiple kinds of installation packages on a single host TDengine. The packages may affect each other and cause errors.

- After deb package is installed, if the installation directory is removed manually, uninstall or reinstall will not work. This issue can be resolved by using the command below which cleans up TDengine package information.

  ```
  $ sudo rm -f /var/lib/dpkg/info/tdengine*
  ```

You can then reinstall if needed.

- After rpm package is installed, if the installation directory is removed manually, uninstall or reinstall will not work. This issue can be resolved by using the command below which cleans up TDengine package information.

  ```
  $ sudo rpm -e --noscripts tdengine
  ```

You can then reinstall if needed.

:::

Uninstalling and Modifying Files

- When TDengine is uninstalled, the configuration /etc/taos/taos.cfg, data directory /var/lib/taos, log directory /var/log/taos are kept. They can be deleted manually with caution, because data can't be recovered. Please follow data integrity, security, backup or relevant SOPs before deleting any data.

- When reinstalling TDengine, if the default configuration file /etc/taos/taos.cfg exists, it will be kept and the configuration file in the installation package will be renamed to taos.cfg.orig and stored at /usr/local/taos/cfg to be used as configuration sample. Otherwise the configuration file in the installation package will be installed to /etc/taos/taos.cfg and used.

## Upgrade
There are two aspects in upgrade operation: upgrade installation package and upgrade a running server.

To upgrade a package, follow the steps mentioned previously to first uninstall the old version then install the new version.

Upgrading a running server is much more complex. First please check the version number of the old version and the new version. The version number of TDengine consists of 4 sections, only if the first 3 sections match can the old version be upgraded to the new version. The steps of upgrading a running server are as below:
- Stop inserting data
- Make sure all data is persisted to disk
- Stop the cluster of TDengine
- Uninstall old version and install new version
- Start the cluster of TDengine
- Execute simple queries, such as the ones executed prior to installing the new package, to make sure there is no data loss 
- Run some simple data insertion statements to make sure the cluster works well
- Restore business services

:::warning
TDengine doesn't guarantee any lower version is compatible with the data generated by a higher version, so it's never recommended to downgrade the version.

:::
