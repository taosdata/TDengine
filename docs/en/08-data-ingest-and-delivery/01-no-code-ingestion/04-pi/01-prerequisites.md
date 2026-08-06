---
title: "PI Data Ingestion Prerequisites"
sidebar_label: "Prerequisites"
---

This page lists all prerequisites that need to be verified before ingesting data from a PI system into TDengine, including network connectivity, ports and protocols, authentication and permissions, and software dependencies. We recommend that PI administrators and network administrators complete this checklist together before creating PI data ingestion tasks.

## PI System Connection Requirements

### PI Data Archive

| Item | Description |
| ---- | ----------- |
| Server Address | Hostname or IP address of the PI Data Archive Server |
| Default Port | **5450** (PI Data Archive standard port) |
| Protocol | PI SDK / PI AF SDK proprietary protocol |

### PI AF Server

When using AF mode (PI Data Archive + AF Server), the following is also required:

| Item | Description |
| ---- | ----------- |
| AF Server Address | Hostname of the PI AF Server |
| AF Database Name | Name of the AF Database to connect to |
| Default Port | **5457** (PI AF Server standard port) |
| Protocol | PI AF SDK proprietary protocol |

:::note
The ports listed above are PI system defaults. The PI connector (`taosx-pi.exe`) communicates with the PI system through PI AF SDK, and the ports are managed internally by the SDK — no manual port configuration is needed in the connector. However, the firewall must allow these ports; otherwise, the SDK connection will fail. If your PI system uses non-standard ports, please confirm the actual port numbers with your PI administrator.
:::

## Network and Firewall Requirements

The host running taosX (or taosX-Agent) must be able to access the following ports on the PI system:

| Source | Destination | Port | Protocol | Description |
| ------ | ----------- | ---- | -------- | ----------- |
| PI connector (taosX/taosX-Agent host) | PI Data Archive Server | 5450/TCP | PI AF SDK proprietary protocol | Required, reads PI Point data |
| PI connector (taosX/taosX-Agent host) | PI AF Server | 5457/TCP | PI AF SDK proprietary protocol | Required when using AF mode |
| taosX-Agent | taosX | taosX configured port | gRPC | Required when using taosX-Agent proxy mode |

If your network environment has firewalls or network isolation, ensure the above ports are allowed.

:::tip
**How to verify port connectivity**

On the taosX / taosX-Agent host, you can use the following commands for quick verification:

```powershell
# Windows PowerShell
Test-NetConnection -ComputerName <PI_SERVER_HOST> -Port 5450
Test-NetConnection -ComputerName <AF_SERVER_HOST> -Port 5457
```

```bash
# Linux (only for verifying taosX ↔ taosX-Agent connectivity)
nc -zv <HOST> <PORT>
```

:::

## Authentication and Service Account Requirements

The PI connector (`taosx-pi.exe`) runs as a subprocess of taosX or taosX-Agent and connects to the PI system through PI AF SDK. **Authentication is handled by the Windows operating system** (Kerberos or NTLM integrated authentication) — it does not rely on PI's internal account/password system.

### Authentication Mode

The Username/Password/Domain fields in the connection configuration (all optional) control the authentication mode:

| Field values | Authentication mode | Description |
| ------------ | ------------------- | ----------- |
| All blank (recommended) | **Windows Integrated Authentication** | The connector accesses PI using the Windows identity of the taosX-Agent service account (Kerberos or NTLM) |
| Username + Password (± Domain) filled in | **Explicit credentials** | Accesses PI using the specified Windows account, overriding the service account identity |

:::tip
**By default, you do not need to fill in Username, Password, or Domain.** Leaving them blank uses Windows integrated authentication, where the connector accesses PI using the taosX-Agent service account's Windows identity. This is the recommended approach for production environments. For a detailed explanation, see [Connection Configuration](./06-connection-config.md).
:::

taosX-Agent runs as the Windows **Local System** account by default, which in a domain environment corresponds to the machine account (e.g., `DOMAIN\machinename$`). In production environments, we recommend configuring taosX-Agent to run under a dedicated domain service account for easier permission management on the PI side.

### PI Data Archive Permissions

The service account running the connector needs the following permissions on the PI Data Archive:

- Permission to read PI Point data (via PI Identity or PI Mapping)
- Permission to read PI Point attributes

The PI administrator should use PI System Management Tools (SMT) to create a Mapping that maps the Windows account's SID to a **PI Identity** with read permissions (**SMT → Security → Mappings**).

### PI AF Server Permissions

When using AF mode, the service account also needs:

- **Read permission** on the target AF database
- Read permission on AF Elements and their Attributes

In PI System Explorer (PSE), add the Windows account to the AF database access control and grant Read and Read Data permissions.

### Service Account Recommendations

| Recommendation | Description |
| -------------- | ----------- |
| Create a dedicated domain service account | Avoid using Local System (machine account), personal accounts, or high-privilege administrator accounts |
| Principle of least privilege | Grant only read access to PI data; no write permission is needed |
| Domain account | When the PI system uses domain authentication, use a domain account to support Kerberos |
| Password policy | Recommend setting password to never expire, or coordinate with password rotation policies |

## Software Dependencies

The PI connector depends on PI AF SDK (PI AF Client), which must be installed on the host running taosX or taosX-Agent.

| Dependency | Minimum Version | Description |
| ---------- | --------------- | ----------- |
| Operating System | Windows Server 2016+ / Windows 10+ | PI AF SDK only supports Windows |
| PI AF SDK (PI AF Client) | 2018+ | Obtain the installer from OSIsoft / AVEVA |
| .NET Framework | 4.8+ | Runtime dependency for PI AF SDK |

:::warning
PI AF SDK **only supports Windows**. If taosX is deployed in a Linux environment, you must connect to the PI system through taosX-Agent (deployed on a Windows host) as a proxy.
:::

## Validation Checklist

Before creating a PI data ingestion task, please confirm each of the following items:

### Network and Connectivity

- [ ] PI Data Archive Server **hostname** confirmed (hostname recommended over IP; using IP may affect Kerberos authentication): `_______________`
- [ ] PI AF Server hostname confirmed (if using AF mode): `_______________`
- [ ] AF database name confirmed (if using AF mode): `_______________`
- [ ] PI Data Archive is accessible from taosX/taosX-Agent host (port 5450)
- [ ] PI AF Server is accessible from taosX/taosX-Agent host (port 5457, if using AF mode)
- [ ] On the taosX-Agent host, manually verified PI connectivity using PI System Management Tools (SMT) or PI System Explorer (PSE) — isolate environment issues before testing taosX
- [ ] If using taosX-Agent proxy mode, taosX-Agent service is running, taosX ↔ taosX-Agent network is connected, and taosX-Agent status shows online in taosExplorer

### Authentication and Permissions

- [ ] taosX-Agent's Windows running identity confirmed (default: Local System → domain machine account `DOMAIN\machinename$`; or a custom domain service account)
- [ ] That Windows identity has a **Mapping** configured in PI Data Archive pointing to the appropriate **PI Identity** (read permission)
- [ ] AF mode: that identity has Read / Read Data permissions in AF Server Security
- [ ] Connection configuration Username/Password/Domain are **left blank** (use Windows integrated authentication)

### Software Environment

- [ ] taosX/taosX-Agent host operating system is Windows
- [ ] PI AF SDK (PI AF Client) is installed
- [ ] .NET Framework 4.8+ is installed

### TDengine Side

- [ ] TDengine cluster is deployed and running normally
- [ ] Target database has been created (or ready to create in taosExplorer)
- [ ] taosX is installed and accessible through taosExplorer

### Task Configuration

- [ ] PI Data Archive Server name uses hostname format
- [ ] Clicked **Connectivity Check** button and connection test passed
