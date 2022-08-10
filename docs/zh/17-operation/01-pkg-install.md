---
title: 安装和卸载
description: 安装、卸载、启动、停止和升级
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

本节将介绍一些关于安装和卸载更深层次的内容，以及升级的注意事项。

## 安装

关于安装，请参考 [使用安装包立即开始](../../get-started/package)



## 安装目录说明

TDengine 成功安装后，主安装目录是 /usr/local/taos，目录内容如下：

```
$ cd /usr/local/taos
$ ll
$ ll
total 28
drwxr-xr-x  7 root root 4096 Feb 22 09:34 ./
drwxr-xr-x 12 root root 4096 Feb 22 09:34 ../
drwxr-xr-x  2 root root 4096 Feb 22 09:34 bin/
drwxr-xr-x  2 root root 4096 Feb 22 09:34 cfg/
lrwxrwxrwx  1 root root   13 Feb 22 09:34 data -> /var/lib/taos/
drwxr-xr-x  2 root root 4096 Feb 22 09:34 driver/
drwxr-xr-x 10 root root 4096 Feb 22 09:34 examples/
drwxr-xr-x  2 root root 4096 Feb 22 09:34 include/
lrwxrwxrwx  1 root root   13 Feb 22 09:34 log -> /var/log/taos/
```

- 自动生成配置文件目录、数据库目录、日志目录。
- 配置文件缺省目录：/etc/taos/taos.cfg， 软链接到 /usr/local/taos/cfg/taos.cfg；
- 数据库缺省目录：/var/lib/taos， 软链接到 /usr/local/taos/data；
- 日志缺省目录：/var/log/taos， 软链接到 /usr/local/taos/log；
- /usr/local/taos/bin 目录下的可执行文件，会软链接到 /usr/bin 目录下；
- /usr/local/taos/driver 目录下的动态库文件，会软链接到 /usr/lib 目录下；
- /usr/local/taos/include 目录下的头文件，会软链接到到 /usr/include 目录下；

## 卸载

<Tabs>
<TabItem label="apt-get 卸载" value="aptremove">

内容 TBD

</TabItem>
<TabItem label="Deb 卸载" value="debuninst">

卸载命令如下:

```
$ sudo dpkg -r tdengine
(Reading database ... 120119 files and directories currently installed.)
Removing tdengine (3.0.0.10002) ...
TDengine is removed successfully!

```

</TabItem>

<TabItem label="RPM 卸载" value="rpmuninst">

卸载命令如下:

```
$ sudo rpm -e tdengine
TDengine is removed successfully!
```

</TabItem>

<TabItem label="tar.gz 卸载" value="taruninst">

卸载命令如下:

```
$ rmtaos
TDengine is removed successfully!
```

</TabItem>
</Tabs>

:::info

- TDengine 提供了多种安装包，但最好不要在一个系统上同时使用 tar.gz 安装包和 deb 或 rpm 安装包。否则会相互影响，导致在使用时出现问题。

- 对于 deb 包安装后，如果安装目录被手工误删了部分，出现卸载、或重新安装不能成功。此时，需要清除 TDengine 包的安装信息，执行如下命令：

  ```
  $ sudo rm -f /var/lib/dpkg/info/tdengine*
  ```

然后再重新进行安装就可以了。

- 对于 rpm 包安装后，如果安装目录被手工误删了部分，出现卸载、或重新安装不能成功。此时，需要清除 TDengine 包的安装信息，执行如下命令：

  ```
  $ sudo rpm -e --noscripts tdengine
  ```

然后再重新进行安装就可以了。

:::

## 卸载和更新文件说明

卸载安装包的时候，将保留配置文件、数据库文件和日志文件，即 /etc/taos/taos.cfg 、 /var/lib/taos 、 /var/log/taos 。如果用户确认后不需保留，可以手工删除，但一定要慎重，因为删除后，数据将永久丢失，不可以恢复！

如果是更新安装，当缺省配置文件（ /etc/taos/taos.cfg ）存在时，仍然使用已有的配置文件，安装包中携带的配置文件修改为 taos.cfg.orig 保存在 /usr/local/taos/cfg/ 目录，可以作为设置配置参数的参考样例；如果不存在配置文件，就使用安装包中自带的配置文件。

## 升级
升级分为两个层面：升级安装包 和 升级运行中的实例。

升级安装包请遵循前述安装和卸载的步骤先卸载旧版本再安装新版本。

升级运行中的实例则要复杂得多，首先请注意版本号，TDengine 的版本号目前分为四段，如 2.4.0.14 和 2.4.0.16，只有前三段版本号一致（即只有第四段版本号不同）才能把一个运行中的实例进行升级。升级步骤如下：
- 停止数据写入
- 确保所有数据落盘，即写入时序数据库
- 停止 TDengine 集群
- 卸载旧版本并安装新版本
- 重新启动 TDengine 集群
- 进行简单的查询操作确认旧数据没有丢失 
- 进行简单的写入操作确认 TDengine 集群可用
- 重新恢复业务数据的写入

:::warning
TDengine 不保证低版本能够兼容高版本的数据，所以任何时候都不推荐降级

:::
