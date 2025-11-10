---
sidebar_label: network Configuration
title: network Configuration
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
2. Modify configuration parameters: Find and modify the following key parameters:
```
  // Set the TDengine server to listen on the specified network interface's IPv6 address, or "::" to listen on all available IPv6 interfaces.
  Example:
  serverIPv6   ::
  firstEp  ipv6_address1:port
  secondEp ipv_address2:port
  fqdn     ipv6_address1
  enableIPv6 1
```
3. Restart the service: After modifying the configuration, restart the TDengine service to apply the changes.
```
  sudo systemctl restart taosd
```
**Important Note**
- It is strongly recommended to use FQDN for configuring "firstEP" and "secondEP" instead of direct IP addresses. This allows automatic selection of IPv6 addresses via DNS resolution, providing better flexibility and compatibility.
- The default port 6030 is also applicable for IPv6 connections.

#### Client Connection Methods

Clients can connect to TDengine servers with IPv6 support in the following ways:

1. Using FQDN (recommended): In the client's "taos.cfg" or connection string, use the server's domain name. As long as the domain's AAAA record points to the correct IPv6 address, the client will automatically connect via IPv6. Example:
   ```
   taos -h your_server_fqdn -P 6030
   ```
2. Using IPv6 address directly: Specify the server's IPv6 address directly when connecting. Note that when using an IPv6 address in the command line or connection string, you must enclose it in square brackets.
   ```
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
```
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
```
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

2. Client configuration (cfg)
```
tlsCliKeyPath /path/client.key
tlsCliCertPath /path/client.crt
tlsCaPath /path/ca.crt
enableTLS 1
```

3. Server configuration (cfg)
```
tlsCliKeyPath /path/client.key
tlsCliCertPath /path/client.crt
tlsSvrKeyPath /path/server.key
tlsSvrCertPath /path/server.crt
tlsCaPath /path/ca.crt
enableTLS 1
```

4. Start the server and connect with the client.

#### Performance
There is some impact: overall performance typically decreases by less than 5% (about 96%–99% of previous throughput).

#### Operations and upgrades
Dynamic upgrades are not supported.\n