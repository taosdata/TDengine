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

### Changing the FQDN of a Node

A TDengine dnode uses `FQDN:port` as its address and persists it in configuration files and the data directory. After the hostname changes, taosd fails to start because the local address no longer matches the address recorded in the data directory. This section describes how to manually make taosd start with the new FQDN and continue serving on a single-node deployment.

:::note

A migration tool for changing the FQDN is under development. Until it is released, follow the manual steps in this section.

:::

#### Applicable Scope

- Single-node deployments only, that is, the cluster contains exactly one dnode. Do not use this method on multi-dnode clusters.
- Only the FQDN is changed. The port stays the same.
- Configuration file encryption (`CFG_KEY`) is not enabled. When it is enabled, the JSON files involved cannot be edited manually.
- At most one snode, with no replica.
- Verified on TDengine `v3.4.x`, including data ingestion, queries, and stream processing.

#### How It Works

The local address of taosd is stored in the following locations:

| Location | Description |
| --- | --- |
| `taos.cfg` | The `fqdn`, `firstEp`, and `secondEp` parameters |
| `<dataDir>/dnode/config/local.json` | Persisted parameters. Loaded after `taos.cfg` and overrides parameters with the same name |
| `<dataDir>/dnode/dnode.json` | The local dnodeId and the dnode list |
| mnode and vnode metadata | dnode addresses, replica addresses, and so on |

At startup, taosd compares the local address with the address recorded in `dnode.json` and exits with an error if they differ. `<dataDir>/dnode/ep.json` declares the mapping from the old address to the new one. When this file exists, taosd replaces the old address with the new one in memory and skips the check. When taosd stops normally, it writes the new address back to `dnode.json` and the mnode metadata, and renames `ep.json` to `ep.json.bak`.

Changing only `taos.cfg` is not enough: the old FQDN in `local.json` overrides `taos.cfg`, and taosd exits because the old FQDN cannot be resolved before it reads `ep.json`. Therefore `taos.cfg`, `local.json`, and `ep.json` must all be modified.

#### Preparation

The following placeholders are used below. Replace them with actual values:

| Placeholder | Meaning | Default |
| --- | --- | --- |
| `<cfgDir>` | taosd configuration directory | `/etc/taos` |
| `<dataDir>` | taosd data directory, that is, `dataDir` in `taos.cfg` | `/var/lib/taos` |
| `<oldFqdn>` | Original FQDN | - |
| `<newFqdn>` | New FQDN | - |
| `<port>` | taosd port, that is, `serverPort` | `6030` |

1. Before changing the hostname, record the current state so that you can compare it afterward:

   ```bash
   taos -h <oldFqdn> -P <port> -s "show dnodes; show snodes; show databases;"
   ```

   If you use stream processing, also record the output of `show <db>.streams` for each database and the row counts of the stream result tables.

2. Stop taosd, then back up the configuration directory, the data directory, and `/etc/hosts`:

   ```bash
   systemctl stop taosd
   cp -a <cfgDir> <cfgDir>.bak
   cp -a <dataDir> <dataDir>.bak
   cp -a /etc/hosts /etc/hosts.bak
   ```

   taosd must be stopped normally. Do not use `kill -9`.

#### Procedure

Perform all of the following steps while taosd is stopped.

1. Configure resolution for the new FQDN. After changing the hostname, add the new FQDN to `/etc/hosts` or DNS and confirm that it resolves to the local host:

   ```bash
   getent hosts <newFqdn>
   ```

2. Edit `<cfgDir>/taos.cfg`. Replace the old FQDN with the new one in `fqdn`, `firstEp`, and `secondEp`, keeping the port unchanged:

   ```text
   fqdn      <newFqdn>
   firstEp   <newFqdn>:<port>
   ```

   If `secondEp` is configured and points to this host, change it as well.

3. Edit `<dataDir>/dnode/config/local.json`. View the address-related parameters in it:

   ```bash
   python3 - <<'EOF'
   import json
   c = json.load(open("<dataDir>/dnode/config/local.json"))["configs"]
   for k in ("fqdn", "firstEp", "secondEp", "serverPort"):
       print(k, c.get(k))
   EOF
   ```

   Replace the old FQDN with the new one in `fqdn`, `firstEp`, and `secondEp`, keeping the port unchanged. Run the command again afterward to confirm. Skip this step if the file does not exist.

4. Find `dnodeId` in `<dataDir>/dnode/dnode.json`. On a single-node deployment it is usually `1`. Create `<dataDir>/dnode/ep.json`:

   ```json
   {
     "dnodes": [
       {
         "id": 1,
         "fqdn": "<oldFqdn>",
         "port": 6030,
         "new_fqdn": "<newFqdn>",
         "new_port": 6030
       }
     ]
   }
   ```

   - Set `id` to `dnodeId`.
   - Set both `port` and `new_port` to the original port. The two values must be identical.
   - `fqdn` and `port` are required.

5. Start taosd:

   ```bash
   systemctl start taosd
   ```

   The taosd log (in `logDir`, `/var/log/taos/taosdlog.0` by default) should contain the following:

   ```text
   check global fqdn:<newFqdn> and port:<port>
   dnode:1, will update ep:<oldFqdn>:<port> to <newFqdn>:<port>
   dnode:1, endpoint changed
   The daemon initialized successfully
   ```

6. Connect with the new FQDN and wait for the dnode status to become `ready`:

   ```bash
   taos -h <newFqdn> -P <port> -s "show dnodes;"
   ```

   For a few seconds after startup, `status` may show `offline` with `note` set to `status not received`. It then changes to `ready`.

7. Stop taosd normally and start it again. taosd renames `ep.json` to `ep.json.bak`, and `dnode.json` now records the new FQDN. `ep.json` is no longer needed for later startups.

#### Verification

Connect with the new FQDN and compare against the state recorded before the change.

1. Node status: the endpoints in `show dnodes` and `show snodes` are `<newFqdn>:<port>`, and the status is `ready`.

2. Data: existing databases, tables, and data can be queried, and row counts match those before the change. Newly written data can be queried normally.

3. Stream processing: the streams in `show <db>.streams` are `Running` or `Idle`, and the row counts of the stream result tables increase after new data is written.

   After taosd restarts or the FQDN changes, streams first show `Undeployed` with `message` set to `Mnode may be unstable`, and return to `Running` or `Idle` after about 10 to 20 seconds. This is a normal redeployment and does not mean the streams have failed.

#### Updating Related Components

The steps above only modify the `taos.cfg` and data directory of taosd itself. Clients and other components that use the old FQDN in their own configuration still connect to the old address. Update them separately and restart the corresponding processes.

| Component | Configuration to check | Notes |
| --- | --- | --- |
| Client | `firstEp` and `secondEp` in `taos.cfg` on the client host; application connection strings; the `TAOS_FIRST_EP` environment variable | No change needed if it shares the same `taos.cfg` with taosd |
| taosAdapter | The `taos.cfg` referenced by `taosConfigDir` in `taosadapter.toml` | Only a restart is needed if it shares `taos.cfg` with taosd; update `http://<oldFqdn>:6041` in applications |
| taosKeeper | `host` under `[tdengine]` in `taoskeeper.toml`; `monitorFqdn` in the taosd `taos.cfg` | The default `host` is `127.0.0.1` and needs no change; `monitorFqdn` is the address of taosKeeper |
| taosExplorer | `cluster` and `cluster_native` in `explorer.toml` | The default `cluster` is `http://localhost:6041` and needs no change |

Details:

- **Client**: update the hostname in application connection strings, for example `jdbc:TAOS://<oldFqdn>:6030/` in JDBC, `taos.connect(host="<oldFqdn>")` in Python, and the DSN in Go.
- **taosAdapter**: taosAdapter reads `taos.cfg` through `taosConfigDir` (`/etc/taos` by default) to connect to taosd. If taosAdapter is deployed on another host, edit the `taos.cfg` on that host.
- **taosKeeper**: `host` under `[tdengine]` is the address taosKeeper uses to connect to taosAdapter. Change it only if it uses the old FQDN. If taosKeeper runs on the same host as taosd and `monitorFqdn` uses the old FQDN, change it to the new FQDN, then restart taosd and taosKeeper.
- **taosExplorer**: if `cluster` or `cluster_native` (such as `taos://<oldFqdn>:6030`) uses the old FQDN, change it and run `systemctl restart taos-explorer`. Update any old addresses saved in the browser.

#### Rollback

To roll back a failed change, stop taosd, restore the configuration directory, data directory, and `/etc/hosts` from the backups, and start taosd:

```bash
systemctl stop taosd
rm -rf <cfgDir> <dataDir>
cp -a <cfgDir>.bak <cfgDir>
cp -a <dataDir>.bak <dataDir>
cp -a /etc/hosts.bak /etc/hosts
systemctl start taosd
```

After restoring, make sure the old FQDN resolves to the local host.

#### FAQ

##### Startup fails with `Unable to resolve FQDN` in the log

The log looks like this:

```text
check global fqdn:<oldFqdn> and port:<port>
failed to apply cfg at ... since Unable to resolve FQDN
failed to start since apply config error
```

If `check global fqdn` still shows the old FQDN, `local.json` has not been modified and its old value overrides `taos.cfg`. Edit `local.json` as described in step 3 of the procedure.

##### Startup fails with `need to be reconfigured` in the log

The local address differs from the one recorded in `dnode.json`, and `ep.json` was not found. Check that `ep.json` is in the `<dataDir>/dnode/` directory, that it is valid JSON, and that `id` matches `dnodeId` in `dnode.json`.

##### Streams show `Undeployed` after the change

Streams need to be redeployed after taosd starts. Wait 10 to 20 seconds and run `show <db>.streams` again. The status returns to `Running` or `Idle`.

##### `ep.json.bak` appears in the data directory

When taosd stops normally, it renames `ep.json` to `ep.json.bak`, which means the new address has been written to `dnode.json`. You can delete this file after confirming the service is working.

##### Files are modified while taosd is running

Files modified while taosd is running may be overwritten when it stops. Always stop taosd before modifying the files.
