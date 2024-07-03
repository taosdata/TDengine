---
title: Set Up taosExplorer
sidebar_label: taosExplorer
---

## Introduction

taosExplorer is a GUI for TDengine Enterprise. With taosExplorer, you can use TDengine features and manage your TDengine cluster and data sources in a convenient Web-based interface. taosExplorer is installed automatically when you install taosX. For more information, see [Installation Guide](../install/).

## Prerequisites

Before you start taosExplorer, ensure that your TDengine cluster is running, taosAdapter is running, and your cluster is connected to taosAdapter. If you want to use data backup, restore, or replication in taosExplorer, ensure that the taosX service and agent are running.

## Configuration

You must configure taosExplorer before running it. The configuration is as follows:

```TOML
# Explorer listen port
port = 6060

# Explorer listen address for IPv4
addr = "0.0.0.0"

# Explorer listen address for IPv4
#ipv6 = "::1"

# Explorer log level. Possible: error,warn,info,debug,trace
log_level = "info"

# taosAdapter address.
cluster = "http://localhost:6041"

# taosX gRPC address
x_api = "http://localhost:6050"

# GRPC endpoint for "Agent"s.
grpc = "http://localhost:6055"

# CORS configuration switch, it allows cross-origin access
cors = false

# Enable ssl: if the following two files exist, enable ssl protocol
[ssl]
# SSL certificate
#certificate = "/path/to/ca.file"
# SSL certificate key
#certificate_key = "/path/to/key.file"
```

Description:

- `port`: The port to which the Explorer service binds.
- `addr`: The IPv4 address to which the Explorer service binds, default is `0.0.0.0`. If you want to change it, configure an address other than `localhost` to provide external services.
- `ipv6`: The IPv6 address to which the Explorer service binds, by default, it does not bind an IPv6 address.
- `log_level`: Log level, optional values are "error", "warn", "info", "debug", "trace".
- `cluster`: The address of the taosAdapter in the TDengine cluster.
- `x_api`: The gRPC address of taosX.
- `grpc`: GRPC endpoint for "Agent"s.
- `cors`: CORS configuration switch, default is `false`. When set to `true`, it allows cross-origin access.
- `ssl.certificate`: SSL certificate(If both the certificate and certificate_key are set, enable HTTPS service, otherwise, do not enable).
- `ssl.certificate_key`: SSL certificate key.

## Start taosExplorer

To start taosExplorer, you can run the `taos-explorer` command or use `systemctl` to start the `taos-explorer` service.

```shell
systemctl start taos-explorer
```

Use `sc` command in Windows:

```shell
sc.exe start taos-explorer
```

## Troubleshooting

1. If you encounter the "Unable to access this website" error message when opening the Explorer site in a browser, log in to the machine where taosExplorer is located via the command line and check the service status with the command `systemctl status taos-explorer`. If the returned status is `inactive`, start the service with the command `systemctl start taos-explorer`.

2. If you need to obtain detailed logs for Explorer, you can use the command `journalctl -u taos-explorer`.

3. When using Nginx or other tools for forwarding, pay attention to CORS settings or use `cors = true` in the configuration file.

    Nginx configuration file with CORS settings example:

    ```conf
    http {
      server {
            listen 6060;
            location ~* {
                proxy_pass http://explorer;

                if ($request_method = 'OPTIONS') {
                    add_header 'Access-Control-Allow-Origin' '*';

                    add_header 'Access-Control-Allow-Credentials' 'true';
                    add_header 'Access-Control-Allow-Methods' 'GET, POST, OPTIONS';

                    add_header 'Access-Control-Allow-Headers' 'DNT,X-CustomHeader,Keep-Alive,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type';

                    add_header 'Access-Control-Max-Age' 86400;
                    add_header 'Content-Type' 'text/plain charset=UTF-8';
                    add_header 'Content-Length' 0;
                    return 204; break;
                }

                if ($request_method = 'POST') {
                    add_header 'Access-Control-Allow-Origin' '*';
                    add_header 'Access-Control-Allow-Credentials' 'true';
                    add_header 'Access-Control-Allow-Methods' 'GET, POST, OPTIONS';
                    add_header 'Access-Control-Allow-Headers' 'DNT,X-CustomHeader,Keep-Alive,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type';
                }
                if ($request_method = 'GET') {
                    add_header 'Access-Control-Allow-Origin' '*';
                    add_header 'Access-Control-Allow-Credentials' 'true';
                    add_header 'Access-Control-Allow-Methods' 'GET, POST, OPTIONS';
                    add_header 'Access-Control-Allow-Headers' 'DNT,X-CustomHeader,Keep-Alive,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type';
                }

                proxy_set_header Host      $host:$server_port;
                proxy_set_header X-Real-IP $remote_addr;

                #proxy_http_version 1.1;
                proxy_read_timeout 60s;
                proxy_next_upstream error  http_502 http_500  non_idempotent;
            }
        }
        upstream explorer{
            ip_hash;
            server 192.168.1.65:6060 ;
            server 192.168.1.68:6060 ;
        }
    }
    ```
