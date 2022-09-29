---
title: Install and Uninstall
description: Install, Uninstall, Start, Stop and Upgrade
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

This document gives more information about installing, uninstalling, and upgrading TDengine.

## Install

About details of installing TDenine, please refer to [Installation Guide](../../get-started/package/).

## Uninstall

<Tabs>
<TabItem label="Uninstall apt-get" value="aptremove">

Apt-get package of TDengine can be uninstalled as below:

```bash
$ sudo apt-get remove tdengine
Reading package lists... Done
Building dependency tree       
Reading state information... Done
The following packages will be REMOVED:
  tdengine
0 upgraded, 0 newly installed, 1 to remove and 18 not upgraded.
After this operation, 68.3 MB disk space will be freed.
Do you want to continue? [Y/n] y
(Reading database ... 135625 files and directories currently installed.)
Removing tdengine (3.0.0.0) ...
TDengine is removed successfully!

```

Apt-get package of taosTools can be uninstalled as below:

```
$ sudo apt remove taostools
Reading package lists... Done
Building dependency tree
Reading state information... Done
The following packages will be REMOVED:
  taostools
0 upgraded, 0 newly installed, 1 to remove and 0 not upgraded.
After this operation, 68.3 MB disk space will be freed.
Do you want to continue? [Y/n]
(Reading database ... 147973 files and directories currently installed.)
Removing taostools (2.1.2) ...
```

</TabItem>
<TabItem label="Uninstall Deb" value="debuninst">

Deb package of TDengine can be uninstalled as below:

```
$ sudo dpkg -r tdengine
(Reading database ... 137504 files and directories currently installed.)
Removing tdengine (3.0.0.0) ...
TDengine is removed successfully!

```

Deb package of taosTools can be uninstalled as below:

```
$ sudo dpkg -r taostools
(Reading database ... 147973 files and directories currently installed.)
Removing taostools (2.1.2) ...
```

</TabItem>

<TabItem label="Uninstall RPM" value="rpmuninst">

RPM package of TDengine can be uninstalled as below:

```
$ sudo rpm -e tdengine
TDengine is removed successfully!
```

RPM package of taosTools can be uninstalled as below:

```
sudo rpm -e taostools
taosToole is removed successfully!
```

</TabItem>

<TabItem label="Uninstall tar.gz" value="taruninst">

tar.gz package of TDengine can be uninstalled as below:

```
$ rmtaos
TDengine is removed successfully!
```

tar.gz package of taosTools can be uninstalled as below:

```
$ rmtaostools
Start to uninstall taos tools ...

taos tools is uninstalled successfully!
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
