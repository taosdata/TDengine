---
sidebar_label: Log Files
title: Locating and Collecting Log Files
description: Where TDengine component log files are stored, how to confirm the effective log directory, and how to collect logs for technical support
toc_max_heading_level: 4
---

This page explains where each TDengine component writes its log files, how to confirm the effective log directory, and how to collect and package logs when asking for technical support. For how logs are written, rotated, and archived, see [Logging System](../../15-internals/09-log.md).

## Default Log Directory

The server, the client, and most components write to the same log directory by default:

| Operating system | Default log directory | Default configuration directory |
| --- | --- | --- |
| Linux | `/var/log/taos` | `/etc/taos` |
| macOS | `/var/log/taos` | `/etc/taos` |
| Windows | `C:\TDengine\log` | `C:\TDengine\cfg` |

If `logDir` (taosd and the client) or `log.path` (other components) has been changed in a configuration file, use the configured path. See [Confirm the Effective Log Directory](#confirm-the-effective-log-directory).

In a Docker deployment, logs are inside the container at `/var/log/taos`. Map that directory to the host with `-v`. See [Docker Deployment](./03-deployment/02-docker.md).

## Log Files by Component

The table uses the default Linux directory. On Windows, replace `/var/log/taos` with `C:\TDengine\log`. `<instance ID>` is the component `instanceId`. `<date>` uses the `YYYYMMDD` format.

| Component | Default directory | File name | Controlled by |
| --- | --- | --- | --- |
| taosd | `/var/log/taos` | `taosdlog.0`, `taosdlog.1` | `logDir` in `taos.cfg`. See [taosd Reference](../03-components/01-taosd.md#log-related) |
| Client | `/var/log/taos` | `taoslogX.Y` | `logDir` in the client `taos.cfg`. See [taosc Reference](../03-components/02-taosc.md) |
| Slow query log | `/var/log/taos` | `taosslowlog.<year-month-day>` | `logDir` in the client `taos.cfg` |
| UDF | `/var/log/taos` | `taosudfdlog.*`; `taospyudf.log` | `logDir` in `taos.cfg`. See [User-Defined Functions (UDF)](../../10-developer-guide/06-udf.md) |
| MQTT subscription | `/var/log/taos` | `taosmqttdlog.*` | `logDir` in `taos.cfg`. See [MQTT Data Subscription](../../06-data-subscription/03-mqtt.md) |
| Xnode daemon | `/var/log/taos` | `taosxnoded_<instance ID>_<date>.log` | `logDir` in `taos.cfg`. See [Data Ingestion (Xnode)](../../05-tdengine-sql/08-cluster-management/02-xnode.md) |
| taosAdapter | `/var/log/taos` | `taosadapter_<instance ID>_<date>.log` | `log.path` in `taosadapter.toml`. See [taosAdapter Reference](../03-components/03-taosadapter.md#log-configuration) |
| taosKeeper | `/var/log/taos` | `taoskeeper_<instance ID>_<date>.log` | `log.path` in `taoskeeper.toml`. See [taosKeeper Reference](../03-components/05-taoskeeper.md) |
| taosExplorer | `/var/log/taos` | `taosexplorer_<instance ID>_<date>.log` | `log.path` in `explorer.toml`. See [taosExplorer Reference](../03-components/04-explorer.md) |
| taosX | `/var/log/taos` | `taosx_<instance ID>_<date>.log` | `log.path` in `taosx.toml`. See [taosX Reference](../03-components/06-taosx.md) |
| taosX-Agent | `/var/log/taos` | `taosx_agent_<instance ID>_<date>.log` | `log.path` in `agent.toml`. See [Configuration Reference](../03-components/07-taosx-agent/01-configuration.md) |
| TDgpt (taosanode) | `/var/log/taos/taosanode` | `error.log`, `access.log`, `taosanode.app.log` | TDgpt configuration file. See [Anode Management](../../09-ai-and-advanced-analytics/01-tdgpt/03-management.md) |
| License center (CLS) | `/var/log/taoscls` | `taoscls.log` | `file` in the `[log]` section of the CLS configuration file. See [Deploy and Activate](../03-components/09-license-center/01-deploy-and-activate.md) |

After a file is rotated by day or by size, the older file keeps a date, a sequence number, or a `.gz` suffix. For troubleshooting, the files modified around the time of the problem are usually enough.

## Confirm the Effective Log Directory

- **taosd**: In the `taos` shell, run `SHOW DNODES;` to obtain `dnode_id`, then run `SHOW DNODE <dnode_id> VARIABLES LIKE 'logDir';`. You can also check the server configuration file: `grep -i '^logDir' /etc/taos/taos.cfg`.
- **Client**: In the `taos` shell, run `SHOW LOCAL VARIABLES LIKE 'logDir';`, or inspect `taos.cfg` on the client machine.
- **taosAdapter, taosKeeper, taosExplorer, taosX, and taosX-Agent**: Check `path` in the `[log]` section of the corresponding TOML file. Some components also accept `--log.path` or an environment variable, such as `TAOS_ADAPTER_LOG_PATH` or `TAOS_KEEPER_LOG_PATH`. Those override the configuration file.
- If `taosd` was started with `-o`, logs are written to the location given by that option. See [taosd Reference](../03-components/01-taosd.md#command-line-parameters).

For the full SQL syntax, see [SHOW Commands](../../05-tdengine-sql/09-system-info/03-show.md).

## When a Service Fails to Start

If a service fails before it opens its log file, the log directory may contain no related record. Check the system service log:

```bash
systemctl status taosd
journalctl -u taosd --since "1 hour ago"
```

For another component, replace `taosd` with the service name, such as `taosadapter`, `taoskeeper`, `taos-explorer`, `taosx`, or `taosx-agent`. On macOS, check `launchd.log`.

For the location of a core dump after a crash, see [Linux Debugging Tools](./13-analysis-and-debug/01-debug-linux.md#core-dump-files).

## Collect Logs for Technical Support

1. If the problem can be reproduced, raise the log level first.
   - Server: run `ALTER ALL DNODES 'debugFlag' '135';`, or change one node with `ALTER DNODE <dnode_id> 'debugFlag' '135';`. You can also add `debugFlag 135` at the end of `taos.cfg` and restart `taosd`.
   - Client: in the `taos` shell, run `ALTER LOCAL 'debugFlag' '135';`. This applies only to the current process.
2. Reproduce the problem and note the approximate time.
3. On each related node, package the log directory and the configuration directory:

   ```bash
   tar czf tdengine-$(hostname)-$(date +%Y%m%d%H%M).tar.gz /var/log/taos /etc/taos
   ```

   On Windows, compress `C:\TDengine\log` and `C:\TDengine\cfg`. If the log directory is large, include only the files modified around the time of the problem.
4. Review configuration files before sending them. Some files, such as `taoskeeper.toml`, contain a database user name and password. Remove those values or replace them with placeholders.
5. Include the TDengine version, operating system, deployment method, reproduction steps, and the time of the problem. Submit them as described in [FAQ and Feedback](../../16-faq/01-faq.md#issue-feedback).
6. After the problem is identified, set the log level back to `131`, for example `ALTER ALL DNODES 'debugFlag' '131';`. Level `135` produces a large volume of logs and reduces performance, so do not leave it enabled during normal operation.
