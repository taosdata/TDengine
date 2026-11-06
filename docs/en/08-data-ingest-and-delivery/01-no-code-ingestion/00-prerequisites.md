---
title: Data Ingestion Prerequisites
sidebar_label: Prerequisites
---

This page lists the general prerequisites to confirm before creating data ingestion tasks through taosExplorer, and applies to all data sources, including OPC DA, OPC UA, PI, and MQTT. For data-source-specific prerequisites (software dependencies, authentication and permissions, etc.), see the documentation of the corresponding data source.

## Environment Readiness

- The TDengine cluster is deployed and running normally.
- taosX is installed and accessible through taosExplorer.
- The target database is created, or you are ready to create it in taosExplorer.

## Determine the Network Topology

Choose direct connection mode or agent proxy mode based on the network relationship between taosX and the data source:

| Topology | When to Use | Network Requirements |
| --- | --- | --- |
| Direct mode | taosX can reach the data source directly | taosX → data source port connectivity |
| Agent proxy mode | taosX cannot reach the data source directly, e.g., the data source is in an isolated OT network, or taosX runs in the public cloud without access to the internal network | Deploy a taosX-Agent in the data source network; both the taosX ↔ taosX-Agent and taosX-Agent ↔ data source segments must be connected |

## Network and Firewall Requirements

| Source | Destination | Port | Description |
| --- | --- | --- | --- |
| taosX-Agent | taosX | taosX gRPC port, default `6055` | Required in agent proxy mode; when the Agent connects to multiple taosX instances, every taosX must be reachable |
| taosX-Agent | Data source | Per data source requirements, e.g., `5450`/`5457` for PI | Required in agent proxy mode; see each data source's documentation |
| taosX | Data source | Per data source requirements | Required in direct mode |

The ports above are defaults and can be changed in configuration files (the taosX gRPC listen address is configured via `serve.grpc`). For the complete port list and planning recommendations, see [Network Port Requirements](../../12-operations-and-tooling/02-operations/01-planning.md#network-port-requirements).

## Verify Port Connectivity

Before deployment, verify connectivity of the key ports. Run the following commands on the source host:

```bash
# Linux
nc -zv <host> <port>
```

```powershell
# Windows PowerShell
Test-NetConnection -ComputerName <host> -Port <port>
```

For example, on the host running taosX-Agent, check connectivity from the Agent to taosX:

```bash
nc -zv <taosX_host> 6055
```

```powershell
Test-NetConnection -ComputerName <taosX_host> -Port 6055
```

## Pre-Creation Checklist

- [ ] The TDengine cluster, taosX, and taosExplorer are all running normally
- [ ] Direct mode or agent proxy mode has been determined
- [ ] Direct mode: the taosX → data source port is allowed
- [ ] Agent proxy mode: taosX-Agent is installed and its status shows **Normal** in taosExplorer
- [ ] Agent proxy mode: the taosX-Agent → taosX port (default `6055`) is allowed
- [ ] Agent proxy mode: the taosX-Agent → data source port is allowed

## Related Documents

- [Install taosX-Agent](./01-install-agent.md): download, install, and bring the agent online
- [taosX-Agent Reference](../../12-operations-and-tooling/03-components/07-taosx-agent/index.md): component features, configuration, and troubleshooting
- [PI Data Ingestion Prerequisites](./04-pi/01-prerequisites.md): a full prerequisites example for a specific data source
