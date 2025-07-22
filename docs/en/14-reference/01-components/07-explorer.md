---
title: taosExplorer Reference
sidebar_label: taosExplorer
slug: /tdengine-reference/components/taosexplorer
---

taosExplorer is a web service that provides users with a visual management interaction tool for TDengine instances. Although it is not open source, it is provided for free with the open source version installation package. This section mainly discusses its installation and deployment. All its features are based on a simple and easy-to-use graphical interface, which you can try directly. If needed, you can also refer to the related content in the advanced features and operation and maintenance guide. To ensure the best experience when accessing taosExplorer, please use Chrome version 79 or above, or Edge version 79 or above.

## Installation

taosExplorer does not require separate installation. Starting from TDengine version 3.3.0.0, it is released together with the TDengine installation package. After installation, you can see the `taos-explorer` service. If you compile the TDengine source code according to the steps on GitHub, the installation package generated does not include taosExplorer.

## Configuration

Before starting taosExplorer, please make sure the content in the configuration file is correct.

```toml
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

Description:

- `port`: The port to which the taosExplorer service is bound.
- `addr`: The IPv4 address to which the taosExplorer service is bound, default is `0.0.0.0`. To modify, configure it to an address other than `localhost` to provide external service.
- `ipv6`: The IPv6 address to which the taosExplorer service is bound, by default no IPv6 address is bound.
- `instanceId`: The instance ID of the current explorer service. If multiple explorer instances are started on the same machine, it is necessary to ensure that the instance IDs of each instance are unique.
- `log_level`: Log level, options are "error", "warn", "info", "debug", "trace". This parameter is deprecated, please use `log.level` instead.
- `cluster`: The taosAdapter address of the TDengine cluster.
- `cluster_native`: The native connection address of the TDengine cluster, off by default.
- `x_api`: The gRPC address of taosX.
- `grpc`: The gRPC address for taosX proxy to establish connection with taosX.
- `cors`: CORS configuration switch, default is `false`. When set to `true`, cross-origin access is allowed.
- `ssl.certificate`: SSL certificate (if both certificate and certificate_key parameters are set, HTTPS service is enabled, otherwise it is not).
- `ssl.certificate_key`: SSL certificate key.
- `log.path`: The directory where log files are stored.
- `log.level`: Log level, options are "error", "warn", "info", "debug", "trace".
- `log.compress`: Whether to compress the log files after rolling.
- `log.rotationCount`: The maximum number of files to keep in the log file directory, older files exceeding this number are deleted.
- `log.rotationSize`: The file size that triggers log file rolling (in bytes), a new file is generated when the log file exceeds this size, and new logs are written to the new file.
- `log.reservedDiskSize`: The threshold of remaining disk space to stop writing logs (in bytes), logging stops when the disk space reaches this size.
- `log.keepDays`: The number of days to keep log files, older log files exceeding this number of days are deleted.

## Start and Stop

Then start taosExplorer, you can directly execute taos-explorer in the command line or use the systemctl command:

```shell
systemctl start taos-explorer  # Linux
sc.exe start taos-explorer # Windows
```

Correspondingly, use the following command to stop

```shell
systemctl stop taos-explorer  # Linux
sc.exe stop taos-explorer # Windows
```

## Troubleshooting

1. When encountering the error message "This site can't be reached" while opening the Explorer site through a browser, log in to the machine where taosExplorer is located via command line, and use the command `systemctl status taos-explorer` to check the status of the service. If the returned status is `inactive`, use the command `systemctl start taos-explorer` to start the service.
2. If detailed logs of taosExplorer are needed, use the command `journalctl -u taos-explorer`.
3. When using Nginx or other tools for forwarding, pay attention to setting CORS or use `cors = true` in the configuration file.

    Here is an example of a CORS setting in an Nginx configuration file:

    ```nginx
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

Once installed, open your browser and by default access `http://ip:6060` to visit the taos-explorer service. If you have not registered yet, first enter the registration screen. Enter your mobile number to get a verification code, and after entering the correct verification code, you can register successfully.

When logging in, please use the database username and password. For first-time use, the default username is `root` and the password is `taosdata`. After a successful login, you will enter the `Data Browser` page, where you can use management functions such as viewing databases, creating databases, and creating supertables/subtables.

Other feature pages, such as `Data Writing - Data Source` and others, are exclusive to the TSDB-Enterprise. You can click to view and have a simple experience, but they cannot be actually used.
