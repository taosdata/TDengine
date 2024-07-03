---
title: 部署 Explorer
sidebar_label: 部署 Explorer
---

## 简介

如果想通过图形化界面使用和管理 TDengine，或者通过图形化界面管理数据接入，则需要部署 taos explorer。在系统中安装了 taosX 安装包 之后，系统中就具备了 taos explorer，细节请参考 [安装与配置](../install)。

## 准备工作

在启动 Explorer 之前，请先确认 TDengine 集群已经正确设置并运行（即 taosd 服务），taosAdapter 也已经正确设置和运行并与 TDengine 集群保持连接状态。如果想要使用数据备份和恢复或者数据同步功能，请确保 taosX 服务和 Agent 服务也已经正确设置和运行。

## 配置

在启动 Explorer 之前，请确保配置文件中的内容正确。

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

说明：

- `port`：Explorer 服务绑定的端口。
- `addr`：Explorer 服务绑定的 IPv4 地址，默认为 `0.0.0.0`。如需修改，请配置为 `localhost` 之外的地址以对外提供服务。
- `ipv6`：Explorer 服务绑定的 IPv6 地址，默认不绑定 IPv6 地址。
- `log_level`：日志级别，可选值为 "error", "warn", "info", "debug", "trace"。
- `cluster`：TDengine 集群的 taosAdapter 地址。
- `x_api`：taosX 的 gRPC 地址。
- `grpc`: taosX 代理向 taosX 建立连接的 gRPC 地址.
- `cors`：CORS 配置开关，默认为 `false`。当为 `true` 时，允许跨域访问。
- `ssl.certificate`: SSL 证书（如果同时设置了 certificate 与 certificate_key 两个参数，则启用 HTTPS 服务，否则不启用）。
- `ssl.certificate_key`: SSL 证书密钥。

## 启动

然后启动 Explorer，可以直接在命令行执行 taos-explorer 或者使用 systemctl 命令：

```bash
systemctl start taos-explorer
```

Windows 下使用 `sc` 命令启动服务：

```shell
sc.exe start taos-explorer
```

## 问题排查

1. 当通过浏览器打开 Explorer 站点遇到“无法访问此网站”的错误信息时，请通过命令行登录 taosExplorer 所在机器，并使用命令 `systemctl status taos-explorer` 检查服务的状态，如果返回的状态是 `inactive`，请使用命令`systemctl start taos-explorer` 启动服务。
2. 如果需要获取 Explorer 的详细日志，可通过命令 `journalctl -u taos-explorer`。
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
