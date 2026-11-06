---
sidebar_label: Network Configuration
title: Network Configuration
toc_max_heading_level: 4
---

### ipv6

#### Overview

Starting from TDengine version 3.3.7, full support for IPv6 network environments is officially provided. This feature allows users to deploy and connect to TDengine in modern network infrastructures, eliminating the dependency on IPv4 and meeting the growing demand for IPv6 networks. This document details how to enable and use IPv6 functionality on both the TDengine server and client sides.

#### Supported Scope

- Supported versions: TDengine Server and Client >= 3.3.7.0
- Supported components:
  - taosd: TDengine database server
  - taos: TDengine command-line client (CLI)
  - Various connectors: such as JDBC, Go, Python, C#, Rust, etc. (must use versions that support IPv6)
- Network environments: Both pure IPv6 and dual-stack IPv4/IPv6 environments are supported.

#### Server-side (taosd) Configuration

To enable IPv6 support, you need to configure the TDengine server configuration file "taos.cfg".

1. Locate the configuration file: The default path is usually "/etc/taos/taos.cfg".
1. Modify configuration parameters: Find and modify the following key parameters:

   ```ini
   // Set the TDengine server to listen on the specified network interface's IPv6 address, or "::" to listen on all available IPv6 interfaces.
   Example:
   serverIPv6   ::
   firstEp  ipv6_address1:port
   secondEp ipv_address2:port
   fqdn     ipv6_address1
   enableIPv6 1
   ```

1. Restart the service: After modifying the configuration, restart the TDengine service to apply the changes.

   ```shell
   sudo systemctl restart taosd
   ```

:::note

- It is strongly recommended to use FQDN for configuring "firstEP" and "secondEP" instead of direct IP addresses. This allows automatic selection of IPv6 addresses via DNS resolution, providing better flexibility and compatibility.
- The default port 6030 is also applicable for IPv6 connections.

:::

#### Client Connection Methods

Clients can connect to TDengine servers with IPv6 support in the following ways:

1. Using FQDN (recommended): In the client's "taos.cfg" or connection string, use the server's domain name. As long as the domain's AAAA record points to the correct IPv6 address, the client will automatically connect via IPv6. Example:

   ```shell
   taos -h your_server_fqdn -P 6030
   ```

2. Using IPv6 address directly: Specify the server's IPv6 address directly when connecting. Note that when using an IPv6 address in the command line or connection string, you must enclose it in square brackets.

   ```shell
   taos -h [2001:db8::1] -P 6030
   ```

3. Verify the connection:
   After a successful connection, you can see in the TDengine logs that all connections are established via IPv6.

#### Notes and Troubleshooting

- Network infrastructure: Ensure that your servers, clients, and all intermediate routers/firewalls are properly configured for IPv6 and allow communication on port "6030" (default).
- DNS configuration: If using FQDN, make sure the DNS server has correctly configured AAAA records (pointing to IPv6 addresses) instead of A records (pointing to IPv4 addresses).
- Dual-stack environment priority: On hosts supporting both IPv4 and IPv6, explicitly set the FQDN to resolve to IPv6.
- Connector versions: Ensure all TDengine client connectors (such as JDBC, Go, Python, etc.) are version 3.3.7.0 or above for full IPv6 compatibility.

### TLS Configuration

#### Overview

Starting from TDengine version 3.3.8, The transport layer provides encrypted communication support.

#### Parameters

1. tlsCaPath: Path to the CA certificate. Must be configured on both client and server. Cannot be changed dynamically.
2. tlsSvrCertPath: Server certificate path. Server-side only. Cannot be changed dynamically.
3. tlsSvrKeyPath: Server private key path. Server-side only. Cannot be changed dynamically.
4. tlsCliCertPath: Client certificate path. Required on both client and server. The server needs this for inter-cluster (including single-node) communication. Cannot be changed dynamically.
5. tlsCliKeyPath: Client private key path. Required on both client and server. The server needs this for inter-cluster (including single-node) communication. Cannot be changed dynamically.
6. enableTLS: Client and server parameter. To enable TLS, all required certificate path parameters must be configured. See the 'Constraints' section for details. If configured incorrectly or incompletely, the service will not start.

#### View parameters

```sql
SHOW VARIABLES LIKE '%tls%';
```

Note: Use this to view TLS file paths on each node in the cluster.

#### Constraints

1. Server: To enable TLS, all five parameters above must be configured; if incomplete, startup fails. If none of the five are configured, the server starts in non-TLS mode.
2. Client: To enable TLS, parameters 1/4/5 must be configured; if incomplete, startup fails. If all three are absent, the client starts in non-TLS mode.
3. All cluster nodes must be started either in TLS mode or non-TLS mode; mixed modes prevent inter-node communication.
4. A non-TLS client cannot access a TLS-enabled cluster (connection failure).
5. A TLS-enabled client cannot access a non-TLS cluster (connection failure).
6. After TLS is enabled, all networked data is mutually authenticated. Even within a single taosd, internal RPC communications require mutual authentication.

#### Deployment example

1. Generate certificates

   ```shell
   openssl req -newkey rsa:2048 -nodes -keyout ca.key -x509 -days 365 -out ca.crt -subj "/CN=MyCA"

   # Generate server private key and CSR
   openssl genrsa -out server.key 2048
   openssl req -new -key server.key -out server.csr -subj "/CN=localhost" # CN is usually the server domain name or IP

   # Sign server certificate with CA
   openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365

   # Generate client private key and CSR
   openssl genrsa -out client.key 2048
   openssl req -new -key client.key -out client.csr -subj "/CN=Client"

   # Sign client certificate with CA
   openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 365
   ```

1. Client configuration (cfg)

   ```ini
   tlsCliKeyPath /path/client.key
   tlsCliCertPath /path/client.crt
   tlsCaPath /path/ca.crt
   enableTLS 1
   ```

1. Server configuration (cfg)

   ```ini
   tlsCliKeyPath /path/client.key
   tlsCliCertPath /path/client.crt
   tlsSvrKeyPath /path/server.key
   tlsSvrCertPath /path/server.crt
   tlsCaPath /path/ca.crt
   enableTLS 1
   ```

1. Start the server and connect with the client.

#### Performance

There is some impact: overall performance typically decreases by less than 5% (about 96%–99% of previous throughput).

#### Operations and upgrades

Dynamic upgrades are not supported.

### Changing the Address of a Node

A TDengine dnode uses `FQDN:port` as its address and persists it in configuration
files and the data directory. After the hostname, domain name, or `serverPort`
changes, taosd refuses to start with the new address because the local address no
longer matches the address recorded in the data directory.

To change the address, run the offline command described below on every node
while **all dnodes are stopped**. The command rewrites the old address in the
configuration files and the data directory to the new one. Then start the cluster
normally. It does not modify data.

Changing the address takes two steps: a precheck, then an apply.

:::note

Earlier versions supported using an `ep.json` file to map old addresses to new ones
at startup. That approach is no longer supported and the file is ignored. If
`<dataDir>/dnode/ep.json` exists, delete it and follow the steps in this
section.

:::

#### Applicable Scope

- Single-node deployments and multi-dnode clusters. Run the command on every
  node.
- The FQDN, the port, or both can be changed.
- Only healthy systems with complete data are supported. Persist all data and
  stop every node normally before starting.
- The command cannot be rolled back. Back up the configuration and data
  directories beforehand if needed.

#### Command Syntax

Precheck, which writes one result file per dnode:

```bash
taosd [-c <cfgDir>] --rewrite-endpoints-precheck <precheckDir> \
  --map '<oldFQDN:oldPort=newFQDN:newPort>' [--map '<oldFQDN:oldPort=newFQDN:newPort>' ...]
```

At least one `--map` is required, and every node in the cluster must use the same
mapping. Only the nodes whose address changes need to be listed; the address of
a node that is not listed is not rewritten.

Apply, which rewrites the local node from the collected result files:

```bash
taosd [-c <cfgDir>] --rewrite-endpoints-apply <precheckDir>
```

#### Procedure

1. Stop data ingestion, metadata operations, and subscription consumption, pause
   stream processing, and wait for the operations in flight, such as unfinished
   transactions, to finish.

2. Flush every database and the mnode:

   ```sql
   flush database <dbName>;
   flush mnode;
   ```

   After the flushes complete, stop every dnode normally:

   ```bash
   systemctl stop taosd
   ```

   Do not restart any node before the rewrite is complete.

3. If needed, configure resolution for the new FQDN. Add a record to DNS or
   `/etc/hosts` and confirm that it resolves to the right host:

   ```bash
   getent hosts <newFqdn>
   ```

4. On every node, run the precheck. `<precheckDir>` must be an existing
   directory, for example:

   ```bash
   taosd -c <cfgDir> --rewrite-endpoints-precheck <precheckDir> \
     --map 'localhost:6030=tdengine-1.example:6030' \
     --map 'localhost:7030=tdengine-2.example:6030'
   ```

   The precheck does not modify any file. On success it writes
   `precheck.dnode<id>.json` into the precheck directory and lists every location
   that will be changed, with its old and new value. If any check fails, `status`
   in the file is `failed`, the command exits with a non-zero code, and you must
   fix the reported problem and run the precheck again.

5. Collect the result files of all nodes into one directory, and make them
   readable on every node (copy the full set to each machine if needed).

6. On every node, apply:

   ```bash
   taosd -c <cfgDir> --rewrite-endpoints-apply <precheckDir>
   ```

   The command re-validates that the state of all nodes is consistent and that
   the local state still matches the precheck result before it changes
   anything. If validation fails, no file is modified. If the command fails,
   no file is modified either.

   While the apply runs, a `<original name>.before-rewrite-endpoints` backup
   appears in the directory of every file it rewrites. Whether the command
   succeeds or fails, these backups are deleted automatically when the command
   ends. If the command is interrupted or killed, however, these backups can be
   left behind; taosd then refuses to start. Please handle them as follows:

   - If every rewritten file still has its backup, the rewrite did not finish.
     Delete the rewritten files and rename the backups to `<original name>`,
     which rolls the state back to before the rewrite.
   - If some backups are already gone and the related files already hold the new
     address, the write phase finished and only the backup cleanup was
     interrupted. Delete the remaining backups to complete the rewrite.
   - If some backups are already gone and the related files still hold the old
     address, the apply was interrupted before the write phase started. Delete
     the remaining backups to roll the state back to before the rewrite.
   - **Note**: in a cluster, when only some of the dnodes have leftover backups
     and those dnodes are rolled back as described above, run the apply again so
     that the whole cluster ends up in a consistent state.

7. Start the cluster:

   ```bash
   systemctl start taosd
   ```

8. Check the result:

   - Node address: `endpoint` shown by commands such as `show dnodes` and
     `show mnodes` must be the new address and the status must be `ready`. For a
     few seconds after startup, `status` may be `offline` with `note` set to
     `status not received`, and then becomes `ready`.

   - Data: existing databases, tables, and rows are queryable and the row counts
     match the values recorded before the change. New writes are queryable as
     well.

   - Stream processing: restart streams, wait about 10 to 20 seconds, the stream
     status in `show <db>.streams` should be `Running` or `Idle`.

   - Data subscription: consumers can subscribe with the new address.

#### Addresses This Command Does Not Change

This command only changes the dnode addresses used by the TDengine TSDB cluster
itself. All other addresses must be changed manually according to the actual
deployment. These include, but are not limited to:

| Item | How to change it |
| --- | --- |
| monitor FQDN | `ALTER DNODE <id> 'monitorFqdn' '<new monitor fqdn>'` or `ALTER ALL DNODES 'monitorFqdn' '<new monitor fqdn>'` |
| Federated query external source | `ALTER EXTERNAL SOURCE` |
| ANode address | Drop the ANode and add it again |
| XNode address | Stop the related tasks, drop and re-add the XNode, update the XNode ID of the tasks, and restart them |
| Clients, connectors, TMQ consumers | Update their own connection settings when they do not share `taos.cfg` with taosd |
| taosAdapter | Update the `taos.cfg` that taosAdapter reads when it does not share it with taosd |
| taosKeeper, taosExplorer, and others | Check whether their own configuration uses the old address |

#### FAQ

##### Startup fails with `Unable to resolve FQDN` in the log

The new FQDN does not resolve. Check DNS or `/etc/hosts`, and confirm that `fqdn`,
`firstEp`, and `secondEp` in both `taos.cfg` and
`<dataDir>/dnode/config/local.json` use the new address.

##### Startup fails with `need to be reconfigured` in the log

The local address differs from the address recorded in
`<dataDir>/dnode/dnode.json`, which means the rewrite command was not run, or only
the configuration files were edited. The log also prints the correct command.
Run the precheck and the apply again as described above.

##### The precheck or apply reports `apply failed`

The output names the file, the field, the actual value, and the expected value.
Common causes are writes after the precheck, an incomplete set of result files,
mappings that differ between machines, and manual edits. Run the precheck again
after fixing the cause.

##### The log reports `file .../ep.json is ignored`

A legacy `ep.json` exists in the data directory. It no longer takes effect, so
delete it and change the address as described above.

##### taosd is still running while the address is changed

The command refuses to run when the instance is still running. Stop taosd
normally first.

##### Startup fails with `backup left behind` in the log

A `<original name>.before-rewrite-endpoints` file was left in the data directory,
the configuration directory, or a disk directory, and the log and the standard
output list every path. It means the previous rewrite did not finish normally,
for example because it was interrupted or killed. Handle the backups as step 6
describes, then start taosd again.
