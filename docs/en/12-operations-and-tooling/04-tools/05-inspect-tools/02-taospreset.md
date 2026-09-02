---
sidebar_label: Pre-installation Configuration
title: Pre-installation Configuration Tool
toc_max_heading_level: 4
---

`taospreset` prepares one host or all hosts in a cluster for TDengine installation.

## Usage

```text
usage: taospreset [-h] [--model {local,ssh}] [--config CONFIG]
                  [--backend] [--disable-kysec] [--result RESULT]
                  [--version] [--log-level {debug,info}]
```

| Option | Description |
| --- | --- |
| `-m`, `--model` | Configure the local host or all configured hosts over SSH; default: `local` |
| `-f`, `--config` | Full path to the configuration file; default: current directory |
| `-b`, `--backend` | Run in the background |
| `-d`, `--disable-kysec` | Disable the KySec security framework on Kylin Linux |
| `-r`, `--result` | Report output directory; default: current directory |
| `-l`, `--log-level` | Log level: `debug` or `info`; default: `info` |
| `-v`, `--version` | Print the tool version |

The configuration format is shared with `taosprecheck`. It can define hosts, timezone, service state, core dump paths, and kernel and user limits.

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
```

## Configuration Scope

The tool can configure the system timezone, stop the firewall and SELinux services, apply system parameters, configure core dumps, replace a default `localhost` hostname with the configured FQDN, and add cluster host mappings to `/etc/hosts`.

## Output

`taospreset` writes `preset_report.md`, which records the settings changed by the tool.

## Examples

```bash
# Configure the current host
./taospreset

# Configure all hosts over SSH
./taospreset -m ssh

# Use a specific configuration
./taospreset -m ssh -f /path/to/preset.cfg

# Also disable KySec
./taospreset -m ssh -d
```
