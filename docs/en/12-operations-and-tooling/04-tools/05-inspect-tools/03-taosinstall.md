---
sidebar_label: Installation Tool
title: Installation Tool
toc_max_heading_level: 4
---

`taosinstall` automates single-node or cluster installation and upgrade of TDengine. It can install all configured nodes, resume on selected nodes, perform a stopped-service upgrade, or perform an experimental rolling upgrade.

:::warning

The upgrade functions are recommended for test environments only. Evaluate service-startup and availability risks before using them in production.

:::

## Install

```text
usage: taosinstall install [-h] [--model {local,ssh}] [--config CONFIG]
                           [--backend] [--check-md5] [--list LIST]
                           [--log-level {debug,info}] [--workers WORKERS]
                           [--set-hostname] [--set-ips] [--replicas {2,3}]
```

Important options include:

- `--model`: install locally or across configured hosts over SSH.
- `--config`: load a specific installation configuration.
- `--check-md5`: verify the installation package checksum.
- `--list`: limit the operation to a comma-separated host list.
- `--workers`: control concurrent file copies; default: 50.
- `--set-hostname` and `--set-ips`: apply configured FQDNs and `/etc/hosts` entries.
- `--replicas`: deploy with two or three replicas; default: three.

The workflow copies and installs the package, applies configuration files, starts `taosd`, creates dnodes and mnodes, configures component instance IDs, starts taosAdapter, taosKeeper, taosX, and taosExplorer, and creates the monitoring user.

## Upgrade

```text
usage: taosinstall upgrade [-h] [--model {local,ssh}] [--config CONFIG]
                           [--backend] [--check-md5] [--list LIST]
                           [--log-level {debug,info}] [--rolling-upgrade]
```

Without `--rolling-upgrade`, the tool stops the configured services on all nodes before upgrading them. Rolling upgrade processes non-mnode nodes first, then follower mnodes, and finally the leader mnode.

## Configuration

The configuration defines SSH access, the local package and checksum, database credentials, per-component configuration templates, the monitoring account, and the services to install.

```ini
[test_env]
firstep=192.168.0.1||fqdn=tdengine1||username=root||password=123456||port=22
secondep=192.168.0.2||fqdn=tdengine2||username=root||password=123456||port=22

[local_pack]
package=/path/to/tdengine-tsdb-enterprise-3.3.x.x-Linux-x64.tar.gz
md5=317f88bf13aa21706ae8c2d4f919d30f

[database]
username=root
password=taosdata
port=6030
rest_port=6041

[taos_cfg]
cfg_file=taos.cfg

[taosadapter_cfg]
cfg_file=taosadapter.toml

[taoskeeper_cfg]
cfg_file=taoskeeper.toml
```

## Examples

```bash
# Install on the current host
./taosinstall install -m local

# Install all configured hosts
./taosinstall install -m ssh -f /path/to/install.cfg

# Install selected hosts
./taosinstall install -m ssh -L server1,server2

# Stopped-service or rolling upgrade
./taosinstall upgrade -m ssh
./taosinstall upgrade -m ssh -r
```
