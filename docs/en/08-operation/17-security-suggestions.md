---
sidebar_label: Security Suggestions
title: Security Suggestions
toc_max_heading_level: 4
---

import Image from '@theme/IdealImage';
import imgEcosys from '../assets/tdengine-components-01.png';

## Background

The distributed and multi-component nature of TDengine makes its security configuration a concern in production systems. This document aims to explain the security issues of various TDengine components and different deployment methods, and provide deployment and configuration suggestions to support the security of user data.

## Components Involved in Security Configuration

TDengine includes multiple components:

- `taosd`: Core component.
- `taosc`: Client library.
- `taosAdapter`: REST API and WebSocket service.
- `taosKeeper`: Monitoring service component.
- `taosX`: Data pipeline and backup recovery component.
- `taosxAgent`: Auxiliary component for external data source access.
- `taosExplorer`: Web visualization management interface.

In addition to TDengine deployment and applications, there are also the following components:

- Applications that access and use the TDengine database through various connectors.
- External data sources: Other data sources that access TDengine, such as MQTT, OPC, Kafka, etc.

The relationship between the components is as follows:

<figure>
<Image img={imgEcosys} alt="TDengine ecosystem"/>
<figcaption>TDengine ecosystem</figcaption>
</figure>

## TDengine Security Settings

### `taosd`

The `taosd` cluster uses TCP connections based on its own protocol for data exchange, which has low risk, but the transmission process is not encrypted, so there is still some security risk.

Enabling compression may help with TCP data obfuscation.

- **compressMsgSize**: Whether to compress RPC messages. Integer, optional: -1: Do not compress any messages; 0: Compress all messages; N (N>0): Only compress messages larger than N bytes.

To ensure the traceability of database operations, it is recommended to enable the audit function.

- **audit**: Audit function switch, 0 is off, 1 is on. Default is on.
- **auditInterval**: Reporting interval, in milliseconds. Default is 5000.
- **auditCreateTable**: Whether to enable the audit function for creating sub-tables. 0 is off, 1 is on. Default is on.

To ensure the security of data files, database encryption can be enabled.

- **encryptAlgorithm**: Data encryption algorithm.
- **encryptScope**: Data encryption scope.

Enabling the whitelist can restrict access addresses and further enhance privacy.

- **enableWhiteList**: Whitelist function switch, 0 is off, 1 is on; default is off.

### `taosc`

Users and other components use the native client library (`taosc`) and its own protocol to connect to `taosd`, which has low data security risk, but the transmission process is still not encrypted, so there is some security risk.

### `taosAdapter`

`taosAdapter` uses the native client library (`taosc`) and its own protocol to connect to `taosd`, and also supports RPC message compression, so there is no data security issue.

Applications and other components connect to `taosAdapter` through various language connectors. By default, the connection is based on HTTP 1.1 and is not encrypted. To ensure the security of data transmission between `taosAdapter` and other components, SSL encrypted connections need to be configured. Modify the following configuration in the `/etc/taos/taosadapter.toml` configuration file:

```toml
[ssl]
enable = true
certFile = "/path/to/certificate-file"
keyFile = "/path/to/private-key"
```

Configure HTTPS/SSL access in the connector to complete encrypted access.

To further enhance security, the whitelist function can be enabled, and configured in `taosd`, which also applies to the `taosAdapter` component.

### `taosX`

`taosX` includes REST API and gRPC interfaces, where the gRPC interface is used for `taos-agent` connections.

- The REST API interface is based on HTTP 1.1 and is not encrypted, posing a security risk.
- The gRPC interface is based on HTTP 2 and is not encrypted, posing a security risk.

To ensure data security, it is recommended that the `taosX` API interface is limited to internal access only. Modify the following configuration in the `/etc/taos/taosx.toml` configuration file:

```toml
[serve]
listen = "127.0.0.1:6050"
grpc = "127.0.0.1:6055"
```

Starting from TDengine 3.3.6.0, `taosX` supports HTTPS connections. Add the following configuration in the `/etc/taos/taosx.toml` file:

```toml
[serve]
ssl_cert = "/path/to/server.pem"
ssl_key =  "/path/to/server.key"
ssl_ca =   "/path/to/ca.pem"
```

And modify the API address to HTTPS connection in Explorer:

```toml
# Local connection to taosX API
x_api = "https://127.0.01:6050"
# Public IP or domain address
grpc = "https://public.domain.name:6055"
```

### `taosExplorer`

Similar to the `taosAdapter` component, the `taosExplorer` component provides HTTP services for external access. Modify the following configuration in the `/etc/taos/explorer.toml` configuration file:

```toml
[ssl]
# SSL certificate file
certificate = "/path/to/ca.file"

# SSL certificate private key
certificate_key = "/path/to/key.file"
```

Then, use HTTPS to access Explorer, such as [https://192.168.12.34](https://192.168.12.34:6060).

### `taosxAgent`

After `taosX` enables HTTPS, the `Agent` component and `taosX` use HTTP 2 encrypted connections, using Arrow-Flight RPC for data exchange. The transmission content is in binary format, and only registered `Agent` connections are valid, ensuring data security.

It is recommended to always enable HTTPS connections for `Agent` services in insecure or public network environments.

### `taosKeeper`

`taosKeeper` uses WebSocket connections to communicate with `taosAdapter`, writing monitoring information reported by other components into TDengine.

The current version of `taosKeeper` has security risks:

- The monitoring address cannot be restricted to the local machine. By default, it monitors all addresses on port 6043, posing a risk of network attacks. This risk can be ignored when deploying with Docker or Kubernetes without exposing the `taosKeeper` port.
- The configuration file contains plaintext passwords, so the visibility of the configuration file needs to be reduced. In `/etc/taos/taoskeeper.toml`:

```toml
[tdengine]
host = "localhost"
port = 6041
username = "root"
password = "taosdata"
usessl = false
```

## Security Enhancements

We recommend using TDengine within a local area network.

If you must provide access outside the local area network, consider adding the following configurations:

### Load Balancing

Use load balancing to provide `taosAdapter` services externally.

Take Nginx as an example to configure multi-node load balancing:

```nginx
http {
    server {
        listen 6041;
        
        location / {
            proxy_pass http://websocket;
            # Headers for websocket compatible
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $connection_upgrade;
            # Forwarded headers
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header X-Forwarded-Host $host;
            proxy_set_header X-Forwarded-Port $server_port;
            proxy_set_header X-Forwarded-Server $hostname;
            proxy_set_header X-Real-IP $remote_addr;
        }
    }
 
    upstream websocket {
        server 192.168.11.61:6041;
        server 192.168.11.62:6041;
        server 192.168.11.63:6041;
   }
}
```

If the `taosAdapter` component is not configured with SSL secure connections, SSL needs to be configured to ensure secure access. SSL can be configured at a higher-level API Gateway or in Nginx; if you have stronger security requirements for the connections between components, you can configure SSL in all components. The Nginx configuration is as follows:

```nginx
http {
    server {
        listen 443 ssl;

        ssl_certificate /path/to/your/certificate.crt;
        ssl_certificate_key /path/to/your/private.key;
    }
}
```

### Security Gateway

In modern internet production systems, the use of security gateways is also very common. [traefik](https://traefik.io/) is a good open-source choice. We take traefik as an example to explain the security configuration in the API gateway.

Traefik provides various security configurations through middleware, including:

1. Authentication: Traefik provides multiple authentication methods such as BasicAuth, DigestAuth, custom authentication middleware, and OAuth 2.0.
2. IP Whitelist: Restrict the allowed client IPs.
3. Rate Limit: Control the number of requests sent to the service.
4. Custom Headers: Add configurations such as `allowedHosts` through custom headers to improve security.

A common middleware example is as follows:

```yaml
labels:
  - "traefik.enable=true"
  - "traefik.http.routers.tdengine.rule=Host(`api.tdengine.example.com`)"
  - "traefik.http.routers.tdengine.entrypoints=https"
  - "traefik.http.routers.tdengine.tls.certresolver=default"
  - "traefik.http.routers.tdengine.service=tdengine"
  - "traefik.http.services.tdengine.loadbalancer.server.port=6041"
  - "traefik.http.middlewares.redirect-to-https.redirectscheme.scheme=https"
  - "traefik.http.middlewares.check-header.headers.customrequestheaders.X-Secret-Header=SecretValue"
  - "traefik.http.middlewares.check-header.headers.customresponseheaders.X-Header-Check=true"
  - "traefik.http.middlewares.tdengine-ipwhitelist.ipwhitelist.sourcerange=127.0.0.1/32, 192.168.1.7"
  - "traefik.http.routers.tdengine.middlewares=redirect-to-https,check-header,tdengine-ipwhitelist"
```

The above example completes the following configurations:

- TLS authentication uses the `default` configuration, which can be configured in the configuration file or traefik startup parameters, as follows:

    ```yaml
    traefik:
    image: "traefik:v2.3.2"
    hostname: "traefik"
    networks:
    - traefik
    command:
    - "--log.level=INFO"
    - "--api.insecure=true"
    - "--providers.docker=true"
    - "--providers.docker.exposedbydefault=false"
    - "--providers.docker.swarmmode=true"
    - "--providers.docker.network=traefik"
    - "--providers.docker.watch=true"
    - "--entrypoints.http.address=:80"
    - "--entrypoints.https.address=:443"
    - "--certificatesresolvers.default.acme.dnschallenge=true"
    - "--certificatesresolvers.default.acme.dnschallenge.provider=alidns"
    - "--certificatesresolvers.default.acme.dnschallenge.resolvers=ns1.alidns.com"
    - "--certificatesresolvers.default.acme.email=linhehuo@gmail.com"
    - "--certificatesresolvers.default.acme.storage=/letsencrypt/acme.json"
    ```

The above startup parameters configure the `default` TSL certificate resolver and automatic acme authentication (automatic certificate application and renewal).

- Middleware `redirect-to-https`: Configure redirection from HTTP to HTTPS, forcing the use of secure connections.

    ```yaml
    - "traefik.http.middlewares.redirect-to-https.redirectscheme.scheme=https"
    ```

- Middleware `check-header`: Configure custom header checks. External access must add custom headers and match header values to prevent unauthorized access. This is a very simple and effective security mechanism when providing API access.
- Middleware `tdengine-ipwhitelist`: Configure IP whitelist. Only allow specified IPs to access, using CIDR routing rules for matching, and can set internal and external IP addresses.

## Summary

Data security is a key indicator of the TDengine product. These measures are designed to protect TDengine deployments from unauthorized access and data breaches while maintaining performance and functionality. However, the security configuration of TDengine itself is not the only guarantee in production. It is more important to develop solutions that better match customer needs in combination with the user's business system.
