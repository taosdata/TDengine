---
title: taosExplorer 参考手册
sidebar_label: taosExplorer
toc_max_heading_level: 4
---

taosExplorer 是一个为用户提供 TDengine 实例的可视化管理交互工具的 web 服务，虽然它没有开源，但随开源版安装包免费提供。本节主要讲述其安装和部署。它的各项功能都是基于简单易上手的图形界面，可以直接尝试，如果有需要也可以参考高级功能和运维指南中的相关内容。为了确保访问 taosExplorer 的最佳体验，请使用 Chrome 79 及以上版本，或 Edge 79 及以上版本。

## 安装

taosExplorer 无需单独安装，从 TDengine 3.3.0.0 版本开始，它随着 TDengine 安装包一起发布，安装完成后，就可以看到 `taos-explorer` 服务。如果按照 GitHub 里步骤自己编译 TDengine 源代码生成的安装包不包含 taosExplorer。

## 配置

在启动 taosExplorer 之前，请确保配置文件中的内容正确。

```TOML
# This is a automacically generated configuration file for Explorer in [TOML](https://toml.io/) format.
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

说明：

- `port`：taosExplorer 服务绑定的端口。
- `addr`：taosExplorer 服务绑定的 IPv4 地址，默认为 `0.0.0.0`。如需修改，请配置为 `localhost` 之外的地址以对外提供服务。
- `ipv6`：taosExplorer 服务绑定的 IPv6 地址，默认不绑定 IPv6 地址。
- `instanceId`：当前 explorer 服务的实例 ID，如果同一台机器上启动了多个 explorer 实例，必须保证各个实例的实例 ID 互不相同。
- `log_level`：日志级别，可选值为 "error", "warn", "info", "debug", "trace"。此参数已弃用，请使用 `log.level` 代替。
- `cluster`：TDengine 集群的 taosAdapter 地址。
- `cluster_native`：TDengine 集群的原生连接地址，默认关闭。
- `x_api`：taosX 的 gRPC 地址。
- `grpc`：taosX 代理向 taosX 建立连接的 gRPC 地址。
- `cors`：CORS 配置开关，默认为 `false`。当为 `true` 时，允许跨域访问。
- `ssl.certificate`：SSL 证书（如果同时设置了 certificate 与 certificate_key 两个参数，则启用 HTTPS 服务，否则不启用）。
- `ssl.certificate_key`：SSL 证书密钥。
- `log.path`：日志文件存放的目录。
- `log.level`：日志级别，可选值为 "error", "warn", "info", "debug", "trace"。
- `log.compress`：日志文件滚动后的文件是否进行压缩。
- `log.rotationCount`：日志文件目录下最多保留的文件数，超出数量的旧文件被删除。
- `log.rotationSize`：触发日志文件滚动的文件大小（单位为字节），当日志文件超出此大小后会生成一个新文件，新的日志会写入新文件。
- `log.reservedDiskSize`：日志所在磁盘停止写入日志的阈值（单位为字节），当磁盘剩余空间达到此大小后停止写入日志。
- `log.keepDays`：日志文件保存的天数，超过此天数的旧日志文件会被删除。


## 启动停止

然后启动 taosExplorer，可以直接在命令行执行 taos-explorer 或者使用 systemctl 命令：

```bash
systemctl start taos-explorer  # Linux
sc.exe start taos-explorer # Windows
```

相应地，用如下命令停止
```shell
systemctl stop taos-explorer  # Linux
sc.exe stop taos-explorer # Windows
```

## 问题排查

1. 当通过浏览器打开 Explorer 站点遇到“无法访问此网站”的错误信息时，请通过命令行登录 taosExplorer 所在机器，并使用命令 `systemctl status taos-explorer` 检查服务的状态，如果返回的状态是 `inactive`，请使用命令`systemctl start taos-explorer` 启动服务。
2. 如果需要获取 taosExplorer 的详细日志，可通过命令 `journalctl -u taos-explorer`。
3. 当使用 Nginx 或其他工具进行转发时，注意进行 CORS 设置或在配置文件中使用 `cors = true`。

    这是一个 Nginx 配置文件 CORS 设置的例子：

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

## 注册登录

安装好，打开浏览器，默认访问`http://ip:6060`来访问 taos-explorer 服务。如果还没有注册过，则首先进入注册界面。输入手机号获取验证码，输入正确的验证码后，即可注册成功。

登录时，请使用数据库用户名和密码登录。首次使用，默认的用户名为 `root`，密码为 `taosdata`。登录成功后即可进入`数据浏览器`页面，您可以使用查看数据库、 创建数据库、创建超级表/子表等管理功能。

其他功能页面，如`数据写入-数据源`等页面，为企业版特有功能，您可以点击查看和简单体验，并不能实际使用。
