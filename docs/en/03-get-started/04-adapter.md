---
title: Deploy taosAdapter 
sidebar_label: taosAdapter
---

taosAdapter is a component of TDengine that provides RESTful and WebSocket interfaces.

## Installing taosAdapter

It is not necessary to install taosAdapter separately. When you install TDengine Server, taosAdapter is automatically installed. However, if desired, you can install taosAdapter and TDengine Server on different machines. For more information, see [Installation](../install/). For more information about taosAdapter, see [taosAdapter](../../reference/taosAdapter/).

## Single-Node Deployment

To install a single instance of taosAdapter, see [Install taosAdapter](https://docs.tdengine.com/reference/taosadapter/#install-taosadapter).

## Multi-Node Deployment

You can install multiple instances of taosAdapter to improve system throughput and prevent taosAdapter from becoming a bottleneck. Installing multiple instances also makes the system more robust and increases availability. If any instance of taosAdapter cannot provide services, requests entering the system are automatically routed to another taosAdapter instance. Load balancing is necessary for multi-node deployments of taosAdapter.

First, install each instance of taosAdapter as described in the single-node deployment section. Next, configure nginx as follows. Note that you must replace the sample endpoints with the actual endpoints for your environment. For details about nginx configuration, see the official nginx documentation.

```json     
user root;
worker_processes auto;
error_log /var/log/nginx_error.log;


events {
        use epoll;
        worker_connections 1024;
}

http {

    access_log off;

    map $http_upgrade $connection_upgrade {
        default upgrade;
        ''      close;
    }

    server {
        listen 6041;
        location ~* {
            proxy_pass http://dbserver;
            proxy_read_timeout 600s;
            proxy_send_timeout 600s;
            proxy_connect_timeout 600s;
            proxy_next_upstream error http_502 non_idempotent;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection $http_connection;
        }
    }
    server {
        listen 6043;
        location ~* {
            proxy_pass http://keeper;
            proxy_read_timeout 60s;
            proxy_next_upstream error  http_502 http_500  non_idempotent;
        }
    }

    server {
        listen 6060;
        location ~* {
            proxy_pass http://explorer;
            proxy_read_timeout 60s;
            proxy_next_upstream error  http_502 http_500  non_idempotent;
        }
    }
    upstream dbserver {
        least_conn;
        server 172.16.214.201:6041 max_fails=0;
        server 172.16.214.202:6041 max_fails=0;
        server 172.16.214.203:6041 max_fails=0;
    }
    upstream keeper {
        ip_hash;
        server 172.16.214.201:6043 ;
        server 172.16.214.202:6043 ;
        server 172.16.214.203:6043 ;
    }
    upstream explorer{
        ip_hash;
        server 172.16.214.201:6060 ;
        server 172.16.214.202:6060 ;
        server 172.16.214.203:6060 ;
    }
}
```
