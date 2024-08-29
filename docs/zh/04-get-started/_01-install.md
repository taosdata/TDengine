---
sidebar_label: Linux
title: 在 Linux 系统上安装和启动 TDengine
toc_max_heading_level: 4
---

本节简介在 Linux 系统上安装和启动 TDengine 的快速步骤。

## 安装步骤

访问 TDengine 的官方版本发布页面：https://docs.taosdata.com/releases/tdengine/ ，下载 TDengine 安装包：TDengine-server-3.3.0.0-Linux-x64.tar.gz 。其他类型安装包的安装方法请参考相关文档，TDengine 遵循各种安装包的标准。

1. 进入到安装包所在目录，使用 tar 解压安装包
```shell
tar -zxvf TDengine-server-3.3.0.0-Linux-x64.tar.gz
```

2. 解压文件后，进入相应子目录 TDengine-server-3.3.0.0，执行其中的 install.sh 安装脚本
```shell
sudo ./install.sh
```

3. 安装后，请使用 systemctl 命令来启动 TDengine 的服务进程。
```shell
sudo systemctl start taosd
```

4. 检查服务是否正常工作：
```shell
sudo systemctl status taosd
```

5. 如果服务进程处于活动状态，则 status 指令会显示如下的相关信息：
```shell
Active: active (running)
```

6. 如果后台服务进程处于停止状态，则 status 指令会显示如下的相关信息：
```shell
Active: inactive (dead)
```

如果 TDengine 服务正常工作，那么您可以通过 TDengine 的命令行程序 taos 来访问并体验 TDengine。

如下 systemctl 命令可以帮助你管理 TDengine 服务：
```shell
1. 启动服务进程：sudo systemctl start taosd
2. 停止服务进程：sudo systemctl stop taosd
3. 重启服务进程：sudo systemctl restart taosd
4. 查看服务状态：sudo systemctl status taosd
```

**注意**：
- 当执行 systemctl stop taosd 命令时，TDengine 服务并不会立即终止，而是会等待必要的数据成功落盘，确保数据的完整性。在处理大量数据的情况下，这一过程可能会花费较长的时间。
- 如果操作系统不支持 systemctl，可以通过手动运行 /usr/local/taos/bin/taosd 命令来启动 TDengine 服务。


## 目录结构

安装 TDengine 后，默认会在操作系统中生成下列目录或文件：

| 目录/文件                 | 说明                                                                 |
| ------------------------- | -------------------------------------------------------------------- |
| /usr/local/taos/bin       | TDengine 可执行文件目录。其中的执行文件都会软链接到/usr/bin 目录下。 |
| /usr/local/taos/driver    | TDengine 动态链接库目录。会软链接到/usr/lib 目录下。                 |
| /usr/local/taos/examples  | TDengine 各种语言应用示例目录。                                      |
| /usr/local/taos/include   | TDengine 对外提供的 C 语言接口的头文件。                             |
| /etc/taos/taos.cfg        | TDengine 默认[配置文件]                                              |
| /var/lib/taos             | TDengine 默认数据文件目录。可通过[配置文件]修改位置。                |
| /var/log/taos             | TDengine 默认日志文件目录。可通过[配置文件]修改位置。                |

## 可执行程序

TDengine 的所有可执行文件默认存放在 _/usr/local/taos/bin_ 目录下。其中包括：

- _taosd_：TDengine 服务端可执行文件
- _taos_：TDengine Shell 可执行文件
- _taosdump_：数据导入导出工具
- _taosBenchmark_：TDengine 测试工具
- _remove.sh_：卸载 TDengine 的脚本，请谨慎执行，链接到/usr/bin 目录下的**rmtaos**命令。会删除 TDengine 的安装目录/usr/local/taos，但会保留/etc/taos、/var/lib/taos、/var/log/taos
- _taosadapter_: 提供 RESTful 服务和接受其他多种软件写入请求的服务端可执行文件
- _TDinsight.sh_：用于下载 TDinsight 并安装的脚本
- _set_core.sh_：用于方便调试设置系统生成 core dump 文件的脚本
- _taosd-dump-cfg.gdb_：用于方便调试 taosd 的 gdb 执行脚本。

