---
title: File directory structure
description: "TDengine installation directory description"
---

After TDengine is installed, the following directories or files will be created in the system by default.

| directory/file | description |
| ------------------------- | -------------------------------------------------------------------- |
| /usr/local/taos/bin | The TDengine executable directory. The executable files are soft-linked to the /usr/bin directory. |
| /usr/local/taos/driver | The TDengine dynamic link library directory. It is soft-linked to the /usr/lib directory.                 |
| /usr/local/taos/examples | The TDengine various language application examples directory.                                      |
| /usr/local/taos/include | The header files for TDengine's external C interface.                             |
| /etc/taos/taos.cfg | TDengine default [configuration file] |
| /var/lib/taos | TDengine's default data file directory. The location can be changed via [configuration file].                |
| /var/log/taos | TDengine default log file directory. The location can be changed via [configure file].                |

## Executable files

All executable files of TDengine are in the _/usr/local/taos/bin_ directory by default. These include.

- _taosd_: TDengine server-side executable files
- _taos_: TDengine CLI executable
- _taosdump_: data import and export tool
- _taosBenchmark_: TDengine testing tool
- _remove.sh_: script to uninstall TDengine, please execute it carefully, link to the **rmtaos** command in the /usr/bin directory. Will remove the TDengine installation directory `/usr/local/taos`, but will keep `/etc/taos`, `/var/lib/taos`, `/var/log/taos`
- _taosadapter_: server-side executable that provides RESTful services and accepts writing requests from a variety of other softwares
- _tarbitrator_: provides arbitration for two-node cluster deployments
- _run_taosd_and_taosadapter.sh_: script to start both taosd and taosAdapter
- _TDinsight.sh_: script to download TDinsight and install it
- _set_core.sh_: script for setting up the system to generate core dump files for easy debugging
- _taosd-dump-cfg.gdb_: script to facilitate debugging of taosd's gdb execution.

:::note
taosdump after version 2.4.0.0 require taosTools as a standalone installation. A new version of taosBenchmark is include in taosTools too.
:::

:::tip
You can configure different data directories and log directories by modifying the system configuration file `taos.cfg`.
:::
