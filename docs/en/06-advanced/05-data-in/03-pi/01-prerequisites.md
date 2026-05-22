---
title: "PI Data Ingestion Prerequisites"
sidebar_label: "Prerequisites"
---

This page lists all prerequisites that need to be verified before ingesting data from a PI system into TDengine, including network connectivity, ports and protocols, authentication and permissions, and software dependencies. We recommend that PI administrators and network administrators complete this checklist together before creating PI data ingestion tasks.

## 1. PI System Connection Requirements

### 1.1 PI Data Archive

| Item | Description |
| ---- | ----------- |
| Server Address | Hostname or IP address of the PI Data Archive Server |
| Default Port | **5450** (PI Data Archive standard port) |
| Protocol | PI SDK / PI AF SDK proprietary protocol |

### 1.2 PI AF Server

When using AF mode (PI Data Archive + AF Server), the following is also required:

| Item | Description |
| ---- | ----------- |
| AF Server Address | Hostname of the PI AF Server |
| AF Database Name | Name of the AF Database to connect to |
| Default Port | **5457** (PI AF Server, via SQL Server) |
| Protocol | PI AF SDK, using SQL Server connection under the hood |

:::note
The ports listed above are PI system defaults. The PI connector (taosx-pi.exe) communicates with the PI system through PI AF SDK, and the ports are managed internally by the SDK — no manual port configuration is needed in the connector. However, the firewall must allow these ports; otherwise, the SDK connection will fail. If your PI system uses non-standard ports, please confirm the actual port numbers with your PI administrator.
:::

## 2. Network and Firewall Requirements

The host running taosX (or taosx-agent) must be able to access the following ports on the PI system:

| Source | Destination | Port | Protocol | Description |
| ------ | ----------- | ---- | -------- | ----------- |
| taosX / taosx-agent | PI Data Archive Server | 5450/TCP | PI SDK | Required, reads PI Point data |
| taosX / taosx-agent | PI AF Server | 5457/TCP | SQL Server (TDS) | Required when using AF mode |
| taosx-agent | taosX | taosX configured port | HTTPS/gRPC | Required when using Agent proxy mode |

If your network environment has firewalls or network isolation, ensure the above ports are allowed.

:::tip
**How to verify port connectivity:**
On the taosX / agent host, you can use the following commands for quick verification:

```powershell
# Windows PowerShell
Test-NetConnection -ComputerName <PI_SERVER_HOST> -Port 5450
Test-NetConnection -ComputerName <AF_SERVER_HOST> -Port 5457
```

```bash
# Linux (only for verifying taosX ↔ agent connectivity)
nc -zv <HOST> <PORT>
```

:::

## 3. Authentication and Service Account Requirements

The PI connector uses PI AF SDK to connect to the PI system, with authentication based on the **Windows service account** running the taosX (or taosx-agent) process.

### 3.1 PI Data Archive Permissions

The service account running the connector needs the following permissions on the PI Data Archive:

- Permission to read PI Point data (PI Identity or PI Mapping)
- Permission to read PI Point attributes

We recommend that the PI administrator creates a dedicated PI Mapping for the service account in the PI Data Archive.

### 3.2 PI AF Server Permissions

When using AF mode, the service account also needs:

- **Read permission** on the target AF database
- Read permission on AF Elements and their Attributes

### 3.3 Service Account Recommendations

| Recommendation | Description |
| -------------- | ----------- |
| Create a dedicated service account | Avoid using personal accounts or high-privilege administrator accounts |
| Principle of least privilege | Grant only read access to PI data; no write permission is needed |
| Domain account | If the PI system uses Windows domain authentication, the service account should be a domain account |
| Password policy | Recommend setting password to never expire, or coordinate with password rotation policies |

## 4. Software Dependencies

The PI connector depends on PI AF SDK (PI AF Client), which must be installed on the host running taosX or taosx-agent.

| Dependency | Minimum Version | Description |
| ---------- | --------------- | ----------- |
| Operating System | Windows Server 2016+ / Windows 10+ | PI AF SDK only supports Windows |
| PI AF SDK (PI AF Client) | 2018+ | Obtain the installer from OSIsoft / AVEVA |
| .NET Framework | 4.8+ | Runtime dependency for PI AF SDK |

:::warning
PI AF SDK **only supports Windows**. If taosX is deployed in a Linux environment, you must connect to the PI system through taosx-agent (deployed on a Windows host) as a proxy.
:::

## 5. Validation Checklist

Before creating a PI data ingestion task, please confirm each of the following items:

### Network and Connectivity

- [ ] PI Data Archive Server hostname/IP confirmed: `_______________`
- [ ] PI AF Server hostname confirmed (if using AF mode): `_______________`
- [ ] AF database name confirmed (if using AF mode): `_______________`
- [ ] PI Data Archive is accessible from taosX/agent host (port 5450)
- [ ] PI AF Server is accessible from taosX/agent host (port 5457, if using AF mode)
- [ ] If using Agent proxy mode, taosX ↔ agent network connectivity is established

### Authentication and Permissions

- [ ] Dedicated Windows service account has been created
- [ ] Service account has PI Mapping configured in PI Data Archive (read permission)
- [ ] Service account has read permission on AF database (if using AF mode)

### Software Environment

- [ ] taosX/agent host operating system is Windows
- [ ] PI AF SDK (PI AF Client) is installed
- [ ] .NET Framework 4.8+ is installed

### TDengine Side

- [ ] TDengine cluster is deployed and running normally
- [ ] Target database has been created (or ready to create in Explorer)
- [ ] taosX is installed and accessible through Explorer
