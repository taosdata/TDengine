---
sidebar_label: Linux
title: Install and Start TDengine on Linux
toc_max_heading_level: 4
---

This section provides a quick guide for installing and starting TDengine on a Linux system.

## Installation Steps

Visit the official TDengine release page: https://docs.taosdata.com/releases/tdengine/, and download the TDengine installation package: TDengine-server-3.3.0.0-Linux-x64.tar.gz. For other types of installation packages, please refer to the relevant documentation, as TDengine adheres to standard installation procedures for different package types.

1. Navigate to the directory where the installation package is located, and use the `tar` command to extract the package:
```bash
tar -zxvf TDengine-server-3.3.0.0-Linux-x64.tar.gz
```

2. After extracting the files, navigate to the corresponding subdirectory, `TDengine-server-3.3.0.0`, and run the `install.sh` script to install TDengine:
```bash
sudo ./install.sh
```

3. After installation, use the `systemctl` command to start the TDengine service process:
```bash
sudo systemctl start taosd
```

4. Check whether the service is running properly:
```bash
sudo systemctl status taosd
```

5. If the service is active, the `status` command will show the following information:
```bash
Active: active (running)
```

6. If the service is stopped, the `status` command will display the following information:
```bash
Active: inactive (dead)
```

If the TDengine service is running normally, you can use TDengine's command-line tool, `taos`, to access and experience TDengine.

The following `systemctl` commands can help you manage the TDengine service:
```bash
1. Start the service process: sudo systemctl start taosd
2. Stop the service process: sudo systemctl stop taosd
3. Restart the service process: sudo systemctl restart taosd
4. Check the service status: sudo systemctl status taosd
```

**Note**:
- When executing the `systemctl stop taosd` command, the TDengine service does not immediately terminate. It waits for the necessary data to be successfully written to disk, ensuring data integrity. This process may take some time if handling large amounts of data.
- If the operating system does not support `systemctl`, you can start the TDengine service manually by running the `/usr/local/taos/bin/taosd` command.

## Directory Structure

After installing TDengine, the following directories and files are created by default in the operating system:

| Directory/File             | Description                                                                      |
| -------------------------- | -------------------------------------------------------------------------------- |
| /usr/local/taos/bin         | Directory for TDengine executable files. The executable files are symlinked to the /usr/bin directory. |
| /usr/local/taos/driver      | Directory for TDengine dynamic libraries. They are symlinked to the /usr/lib directory. |
| /usr/local/taos/examples    | Directory containing application examples in various programming languages.      |
| /usr/local/taos/include     | Header files for TDengine's C-language interface.                                |
| /etc/taos/taos.cfg          | TDengine's default [configuration file].                                         |
| /var/lib/taos               | Default directory for TDengine data files. The location can be modified via the [configuration file]. |
| /var/log/taos               | Default directory for TDengine log files. The location can be modified via the [configuration file]. |

## Executable Programs

All TDengine executables are located in the `/usr/local/taos/bin` directory by default. These include:

- _taosd_: TDengine server executable
- _taos_: TDengine Shell executable
- _taosdump_: Data import/export tool
- _taosBenchmark_: TDengine performance testing tool
- _remove.sh_: Script for uninstalling TDengine. This is linked to the `/usr/bin` **rmtaos** command and will delete the TDengine installation directory `/usr/local/taos`, but it will preserve `/etc/taos`, `/var/lib/taos`, and `/var/log/taos`.
- _taosadapter_: Server executable that provides RESTful services and accepts write requests from various software.
- _TDinsight.sh_: Script for downloading and installing TDinsight.
- _set_core.sh_: Script for setting up the system to generate core dump files for debugging.
- _taosd-dump-cfg.gdb_: GDB execution script to facilitate debugging of `taosd`.
