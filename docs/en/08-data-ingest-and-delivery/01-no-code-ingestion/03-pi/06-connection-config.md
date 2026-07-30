---
title: "PI Connection Configuration and Authentication"
sidebar_label: "Connection Configuration"
---

This page provides a detailed explanation of each field in the "Connection" section when creating PI or PI backfill tasks, with a focus on the Windows authentication mechanism, the taosx-agent access identity, and how to configure corresponding permissions on the PI side.

## 1. Connection Fields Reference

When creating a PI or PI backfill task in Explorer, the connection section contains these fields:

| Field | Required | Description |
| ----- | -------- | ----------- |
| PI Server Name | Yes | Hostname of the PI Data Archive Server. **Hostname is recommended over IP address** (see Section 2) |
| PI System (AF Server) Name | Required for AF mode | Hostname of the PI AF Server |
| AF Database Name | Required for AF mode | Name of the AF database to connect to |
| Username | No | Windows account username; **leave blank to use integrated authentication (recommended)** |
| Password | No | Windows account password; only needed when Username is provided |
| Domain | No | Windows domain name; only needed when Username is provided |

## 2. Why Use a Hostname Instead of an IP Address

The PI Data Archive Server address should use a **hostname** for these reasons:

- **Kerberos authentication depends on SPNs**: Kerberos tickets are bound to Service Principal Names (SPNs), which are registered based on hostnames. When connecting via IP address, Windows cannot match the SPN and automatically falls back to NTLM authentication.
- **NTLM may be restricted**: Some PI server security policies require Kerberos and restrict or disable NTLM.
- **Clearer diagnostics**: PI connection logs display hostnames, making troubleshooting easier.

## 3. Windows Authentication Mechanism

### 3.1 Authentication Is Handled by Windows, Not PI's Internal Accounts

The PI Connector (taosx-pi.exe) connects to the PI system through PI AF SDK. **Authentication is handled by the Windows operating system** using Kerberos or NTLM integrated authentication protocols. PI AF SDK is not involved in authentication logic.

This means:

- The connector **does not use PI's internal accounts** — the credentials configured here are **Windows accounts**, not PI accounts.
- Whether authentication succeeds depends on whether the Windows identity on the taosx-agent host is trusted by the PI system, and whether the corresponding Mapping is configured on the PI side (see Section 4).

### 3.2 Which Windows Identity the Connector Uses to Access PI

The PI Connector accesses PI using the **Windows identity of the taosx-agent service**. By default, taosx-agent runs as the Windows **Local System** account.

| taosx-agent running identity | Windows identity presented to PI |
| ----------------------------- | -------------------------------- |
| Local System (default) | Domain environment: machine account `DOMAIN\machinename$`; workgroup: local system account (Kerberos unavailable) |
| Domain service account (recommended) | That domain account, e.g., `DOMAIN\svc-taosx` |
| Local account | `machinename\accountname` |

You can view and modify the taosx-agent running identity in Windows Service Manager: **Services → taosx-agent → Properties → Log On**.

### 3.3 Integrated Authentication vs. Explicit Credentials

The Username/Password/Domain fields in the connection configuration control the authentication mode:

| Field values | Authentication mode | Description |
| ------------ | ------------------- | ----------- |
| **All blank (recommended)** | Windows Integrated Authentication | The connector accesses PI using the taosx-agent service account's Windows identity (Kerberos or NTLM) |
| Username + Password (± Domain) filled in | Explicit credentials | Accesses PI using the specified Windows account, overriding the service account identity |

:::tip
**By default, you do not need to fill in Username, Password, or Domain.**

In most enterprise domain environments, the taosx-agent service account already has a trust relationship with the PI system through Windows integrated authentication. You only need to configure a Mapping for its Windows identity on the PI side (see Section 4) for the connection to succeed.
:::

:::warning
When using explicit credentials (providing username and password), the password is stored in encrypted form in taosX's task configuration. Prefer integrated authentication (leave blank) in production environments; only use explicit credentials when the service account cannot access PI and a specific account is temporarily needed.
:::

## 4. PI-Side Permission Configuration

PI uses **two-layer access control**: the **authentication layer (Mapping)** determines "who you are," and the **authorization layer (ACL/Security)** determines "what you can do." Both layers are required.

### 4.1 Step 1: Identify the Connector's Windows Identity

In PI Data Archive or AF Server management tools (SMT or PSE), open the **Connections** tab and find the connection from the taosx-agent host. Record the Username shown.

If no connection has been established yet, you can infer it:

- taosx-agent defaults to running as Local System → in a domain, this is `DOMAIN\machinename$`
- Check the "Log On" tab of taosx-agent in Windows Service Manager for the actual running identity

### 4.2 PI Data Archive Permission Configuration

#### Layer 1: Authentication (Mapping)

In **PI System Management Tools (SMT)**: **Security → Mappings**

Create a Mapping that maps the Windows identity (Windows Identity / SID) to a **PI Identity** with read permissions.

#### Layer 2: Authorization (ACL)

In SMT → **Security → Identities**, configure the appropriate PI Point read permissions (Read, Read Data) for that PI Identity.

### 4.3 PI AF Server Permission Configuration (AF Mode)

In **PI System Explorer (PSE)**:

1. Open **Database Security** or **Server Security** for the target AF database
2. Add the Windows account as an AF Identity (or map to an existing AF Identity)
3. Grant **Read** and **Read Data** permissions on the target AF Database and its Elements/Attributes

### 4.4 Quick Permission Reference

| Requirement | Where to configure |
| ----------- | ------------------ |
| Allow connector to read all Points | SMT → Mappings (add new) + Identities (grant Read/Read Data) |
| Restrict to reading only specific Points | Use fine-grained point-level ACL or create a dedicated PI Identity |
| Read-only, disallow write-back to PI | Remove Write/Write Data permissions from the corresponding PI Identity |
| AF mode: read Elements/Attributes | PSE → Database Security → grant Read + Read Data |
| Completely block agent access | Delete the corresponding Mapping (causes authentication failure on connection) |

:::note
Principle: **Mapping controls "identity recognition," ACL controls "permissions."** To tighten permissions, modify the ACL — do not delete the Mapping (deleting the Mapping causes authentication failure, not a permission denial).
:::

## 5. Verify the Connection

After completing the authentication and permission configuration:

1. In Explorer's PI task creation page, fill in the connection information (**leave Username/Password/Domain blank**)
2. Click the **Connectivity Check** button
3. A successful check means the connector can access PI normally

## 6. Troubleshooting Connection Issues

### Connectivity Check Failure — Troubleshooting Steps

1. **Network connectivity**: On the taosx-agent host, run:

   ```powershell
   Test-NetConnection -ComputerName <PI_SERVER_HOST> -Port 5450
   Test-NetConnection -ComputerName <AF_SERVER_HOST> -Port 5457  # AF mode
   ```

2. **Manually verify with PI tools first**: On the agent host, open PI System Management Tools (SMT) or PI System Explorer (PSE) and connect to PI directly. If the manual connection also fails, the issue is environmental (network/permissions/SDK), not taosX configuration.

3. **Authentication issues**:
   - Confirm the taosx-agent running identity (Windows Service Manager → taosx-agent → Log On)
   - Confirm that identity has a Mapping configured in PI Data Archive
   - If using explicit credentials, verify the username, password, and domain are correct

4. **PI AF SDK not installed**: Confirm that PI AF SDK (PI AF Client 2018+) is installed on the taosx-agent host

:::note
The current version of the PI Connector may only return generic error messages on connection failure, without specifying the exact failure reason (e.g., the specific type of authentication failure). Set the connector log level to `debug` for more detailed information.
:::
