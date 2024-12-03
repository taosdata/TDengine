---
title: taosExplorer Reference
sidebar_label: taosExplorer
slug: /tdengine-reference/components/taosexplorer
---

taosExplorer is a web service that provides users with a visual management interface for TDengine instances. Although it is not open source, it is offered for free with the open-source installation package. This section primarily discusses its installation and deployment. Its features are based on an easy-to-use graphical interface that can be directly tried out. If needed, you can also refer to the advanced features and operations guide. To ensure the best experience when accessing taosExplorer, please use Chrome version 79 or above, or Edge version 79 or above.

## Installation

taosExplorer does not require separate installation. Starting from TDengine version 3.3.0.0, it is bundled with the TDengine installation package. After installation, you will see the `taos-explorer` service. If you compile the TDengine source code yourself following the steps in GitHub, the installation package will not include taosExplorer.

## Configuration

Before starting taosExplorer, please ensure that the contents of the configuration file are correct.

```TOML
# This is an automatically generated configuration file for Explorer in [TOML](https://toml.io/) format.
#
# Here is a full list of available options.

# Explorer server port to listen on.
# Default is 6060.
#
port = 6060

# IPv4 listen address.
# Default is 0.0.0.0
addr = "0.0.0.0"

# IPv6 listen address.

# ipv6 = "::1"

# explorer server instance id
# 
# The instanceId of each instance is unique on the host
# instanceId = 1

# Explorer server log level.
# Default is "info"
# 
# Deprecated: use log.level instead
log_level = "info"

# All data files are stored in this directory
# data_dir = "/var/lib/taos/explorer" # Default for Linux
# data_dir = "C:\\TDengine\\data\\explorer" # Default for Windows

# REST API endpoint to connect to the cluster.
# This configuration is also the target for data migration tasks.
# 
# Default is "http://localhost:6041" - the default endpoint for REST API.
#
cluster = "http://localhost:6041"

# native endpoint to connect to the cluster.
# Default is disabled. To enable it, set it to the native API URL like "taos://localhost:6030" and uncomment it.
# If you enable it, you will get more performance for data migration tasks.
#
# cluster_native = "taos://localhost:6030"

# API endpoint for data replication/backup/data sources. No default option.
#   Set it to API URL like "http://localhost:6050".
#
x_api = "http://localhost:6050"

# GRPC endpoint for "Agent"s.
#   Default is "http://localhost:6055" - the default endpoint for taosX grpc API.
#   You should set it to public IP or FQDN name like:
#   "http://192.168.111.111:6055" or "http://node1.company.domain:6055" and
#   ensure to add the port to the exception list of the firewall if it enabled.
grpc = "http://localhost:6055"

# CORS configuration switch, it allows cross-origin access
cors = true

# Enable ssl
# If the following two files exist, enable ssl protocol
#
[ssl]

# SSL certificate
#
# certificate = "/path/to/ca.file" # on linux/macOS
# certificate = "C:\\path\\to\\ca.file" # on windows

# SSL certificate key
#
# certificate_key = "/path/to/key.file" # on linux/macOS
# certificate_key = "C:\\path\\to\\key.file" # on windows

# log configuration
[log]
# All log files are stored in this directory
# 
# path = "/var/log/taos" # on linux/macOS
# path = "C:\\TDengine\\log" # on windows

# log filter level
#
# level = "info"

# Compress archived log files or not
# 
# compress = false

# The number of log files retained by the current explorer server instance in the `path` directory
# 
# rotationCount = 30

# Rotate when the log file reaches this size
# 
# rotationSize = "1GB"

# Log downgrade when the remaining disk space reaches this size, only logging `ERROR` level logs
# 
# reservedDiskSize = "1GB"

# The number of days log files are retained
#
# keepDays = 30
```

**Explanation:**

- `port`: The port that the taosExplorer service binds to.
- `addr`: The IPv4 address that the taosExplorer service binds to, default is `0.0.0.0`. If you need to modify it, configure it to an address other than `localhost` to provide external services.
- `ipv6`: The IPv6 address that the taosExplorer service binds to, default does not bind to IPv6.
- `instanceId`: The instance ID of the current explorer service. If multiple explorer instances are started on the same machine, ensure that their instance IDs are unique.
- `log_level`: The log level, optional values are "error", "warn", "info", "debug", "trace". This parameter is deprecated; please use `log.level` instead.
- `cluster`: The taosAdapter address for the TDengine cluster.
- `cluster_native`: The native connection address for the TDengine cluster, disabled by default.
- `x_api`: The gRPC address for taosX.
- `grpc`: The gRPC address for taosX agent to connect to taosX.
- `cors`: CORS configuration switch, default is `false`. When set to `true`, cross-origin access is allowed.
- `ssl.certificate`: SSL certificate (HTTPS service will be enabled if both certificate and certificate_key parameters are set).
- `ssl.certificate_key`: SSL certificate key.
- `log.path`: The directory for storing log files.
- `log.level`: The log level, optional values are "error", "warn", "info", "debug", "trace".
- `log.compress`: Whether to compress archived log files.
- `log.rotationCount`: The maximum number of log files to retain in the directory, exceeding this number will delete old files.
- `log.rotationSize`: The size of log files that triggers rotation (in bytes). When the log file exceeds this size, a new file will be generated, and new logs will be written to the new file.
- `log.reservedDiskSize`: The threshold for stopping log writing when the remaining disk space reaches this size (in bytes).
- `log.keepDays`: The number of days that log files are retained. Old log files exceeding this duration will be deleted.

## Starting and Stopping

Next, start taosExplorer. You can directly execute `taos-explorer` in the command line or use the systemctl command:

```bash
systemctl start taos-explorer  # Linux
sc.exe start taos-explorer # Windows
```

To stop it, use the following commands:

```shell
systemctl stop taos-explorer  # Linux
sc.exe stop taos-explorer # Windows
```

## Troubleshooting

1. If you encounter an error message saying "Cannot access this website" when opening the Explorer site in the browser, log in to the machine where taosExplorer is located via the command line and check the service status using the command `systemctl status taos-explorer`. If the returned status is `inactive`, start the service using the command `systemctl start taos-explorer`.
2. To obtain detailed logs for taosExplorer, use the command `journalctl -u taos-explorer`.
3. When using Nginx or other tools for forwarding, pay attention to CORS settings or use `cors = true` in the configuration file.

    Here is an example of Nginx configuration file CORS settings:

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

## Registration and Login

Once installed, open your browser and access the taos-explorer service by default at `http://ip:6060`. If you haven't registered yet, first go to the registration page. Enter your phone number to receive a verification code, and after entering the correct code, registration will be successful.

When logging in, use your database username and password. The default username for first-time use is `root`, and the password is `taosdata`. Once logged in successfully, you will enter the `Data Browser` page, where you can manage functionalities such as viewing databases, creating databases, and creating supertables/subtables.

Other feature pages, such as `Data Insertion - Data Source`, are exclusive to the enterprise version. You can click to view and have a simple experience, but they cannot be used practically.
