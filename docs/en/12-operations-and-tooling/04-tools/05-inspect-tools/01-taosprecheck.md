---
sidebar_label: Pre-installation Check
title: Pre-installation Check Tool
toc_max_heading_level: 4
---

`taosprecheck` checks whether one host or every host in a cluster meets the environment requirements for installing TDengine.

## Usage

```text
usage: taosprecheck [-h] [--model {local,ssh}] [--config CONFIG]
                    [--backend] [--result RESULT] [--version]
                    [--log-level {debug,info}]
```

| Option | Description |
| --- | --- |
| `-m`, `--model` | Run locally or connect to all configured hosts over SSH; default: `local` |
| `-f`, `--config` | Full path to the configuration file; default: current directory |
| `-b`, `--backend` | Run in the background |
| `-r`, `--result` | Report output directory; default: current directory |
| `-l`, `--log-level` | Log level: `debug` or `info`; default: `info` |
| `-v`, `--version` | Print the tool version |

The configuration can define cluster hosts, timezone, required service states, core dump settings, kernel and user limits, and prerequisite packages. SSH passwords can be omitted when passwordless SSH is configured.

```ini
[test_env]
firstep=192.168.0.1||fqdn=tdengine1||username=root||password=123456||port=22
secondep=192.168.0.2||fqdn=tdengine2||username=root||password=123456||port=22

[timezone]
tz=Asia/Shanghai

[services]
firewall=inactive
selinux=inactive

[coredump]
kernel.core_pattern=/data/taos/core/core-%%e-%%p

[sys_vars:/etc/sysctl.conf]
fs.nr_open=2147483584
fs.file-max=2147483584
net.ipv4.ip_local_port_range=10000 65534
```

## Checks

The tool checks CPU, memory, disks and mounts, network and SSH connectivity, operating system settings, core dump configuration, hostname resolution, prerequisite packages, swap, KySec on Kylin Linux, configured system limits, and time synchronization between nodes.

## Output

The output directory contains:

- `precheck_report.md`: collected results.
- `precheck_advice.md`: recommendations based on failed or risky checks.

## Examples

```bash
# Check the current host
./taosprecheck

# Check all configured hosts over SSH
./taosprecheck -m ssh

# Use a specific configuration and debug logging
./taosprecheck -m ssh -f /path/to/precheck.cfg -l debug
```
