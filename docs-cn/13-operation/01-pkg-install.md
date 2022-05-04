---
title: 安装和卸载
description: 安装、卸载、启动、停止和升级
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

TDengine 开源版本提供 deb 和 rpm 格式安装包，用户可以根据自己的运行环境选择合适的安装包。其中 deb 支持 Debian/Ubuntu 及衍生系统，rpm 支持 CentOS/RHEL/SUSE 及衍生系统。同时我们也为企业用户提供 tar.gz 格式安装包。

## 安装

<Tabs>
<TabItem label="Deb 安装" value="debinst">

1、从官网下载获得 deb 安装包，例如 TDengine-server-2.4.0.7-Linux-x64.deb；
2、进入到 TDengine-server-2.4.0.7-Linux-x64.deb 安装包所在目录，执行如下的安装命令：

```
$ sudo dpkg -i TDengine-server-2.4.0.7-Linux-x64.deb
(Reading database ... 137504 files and directories currently installed.)
Preparing to unpack TDengine-server-2.4.0.7-Linux-x64.deb ...
TDengine is removed successfully!
Unpacking tdengine (2.4.0.7) over (2.4.0.7) ...
Setting up tdengine (2.4.0.7) ...
Start to install TDengine...

System hostname is: ubuntu-1804

Enter FQDN:port (like h1.taosdata.com:6030) of an existing TDengine cluster node to join
OR leave it blank to build one:

Enter your email address for priority support or enter empty to skip:
Created symlink /etc/systemd/system/multi-user.target.wants/taosd.service → /etc/systemd/system/taosd.service.

To configure TDengine : edit /etc/taos/taos.cfg
To start TDengine     : sudo systemctl start taosd
To access TDengine    : taos -h ubuntu-1804 to login into TDengine server


TDengine is installed successfully!
```

</TabItem>

<TabItem label="RPM 安装" value="rpminst">

1、从官网下载获得 rpm 安装包，例如 TDengine-server-2.4.0.7-Linux-x64.rpm；
2、进入到 TDengine-server-2.4.0.7-Linux-x64.rpm 安装包所在目录，执行如下的安装命令：

```
$ sudo rpm -ivh TDengine-server-2.4.0.7-Linux-x64.rpm
Preparing...                          ################################# [100%]
Updating / installing...
   1:tdengine-2.4.0.7-3               ################################# [100%]
Start to install TDengine...

System hostname is: centos7

Enter FQDN:port (like h1.taosdata.com:6030) of an existing TDengine cluster node to join
OR leave it blank to build one:

Enter your email address for priority support or enter empty to skip:

Created symlink from /etc/systemd/system/multi-user.target.wants/taosd.service to /etc/systemd/system/taosd.service.

To configure TDengine : edit /etc/taos/taos.cfg
To start TDengine     : sudo systemctl start taosd
To access TDengine    : taos -h centos7 to login into TDengine server


TDengine is installed successfully!
```

</TabItem>

<TabItem label="tar.gz 安装" value="tarinst">

1、从官网下载获得 tar.gz 安装包，例如 TDengine-server-2.4.0.7-Linux-x64.tar.gz；
2、进入到 TDengine-server-2.4.0.7-Linux-x64.tar.gz 安装包所在目录，先解压文件后，进入子目录，执行其中的 install.sh 安装脚本：

```
$ tar xvzf TDengine-enterprise-server-2.4.0.7-Linux-x64.tar.gz
TDengine-enterprise-server-2.4.0.7/
TDengine-enterprise-server-2.4.0.7/driver/
TDengine-enterprise-server-2.4.0.7/driver/vercomp.txt
TDengine-enterprise-server-2.4.0.7/driver/libtaos.so.2.4.0.7
TDengine-enterprise-server-2.4.0.7/install.sh
TDengine-enterprise-server-2.4.0.7/examples/
...

$ ll
total 43816
drwxrwxr-x  3 ubuntu ubuntu     4096 Feb 22 09:31 ./
drwxr-xr-x 20 ubuntu ubuntu     4096 Feb 22 09:30 ../
drwxrwxr-x  4 ubuntu ubuntu     4096 Feb 22 09:30 TDengine-enterprise-server-2.4.0.7/
-rw-rw-r--  1 ubuntu ubuntu 44852544 Feb 22 09:31 TDengine-enterprise-server-2.4.0.7-Linux-x64.tar.gz

$ cd TDengine-enterprise-server-2.4.0.7/

 $ ll
total 40784
drwxrwxr-x  4 ubuntu ubuntu     4096 Feb 22 09:30 ./
drwxrwxr-x  3 ubuntu ubuntu     4096 Feb 22 09:31 ../
drwxrwxr-x  2 ubuntu ubuntu     4096 Feb 22 09:30 driver/
drwxrwxr-x 10 ubuntu ubuntu     4096 Feb 22 09:30 examples/
-rwxrwxr-x  1 ubuntu ubuntu    33294 Feb 22 09:30 install.sh*
-rw-rw-r--  1 ubuntu ubuntu 41704288 Feb 22 09:30 taos.tar.gz

$ sudo ./install.sh

Start to update TDengine...
Created symlink /etc/systemd/system/multi-user.target.wants/taosd.service → /etc/systemd/system/taosd.service.
Nginx for TDengine is updated successfully!

To configure TDengine : edit /etc/taos/taos.cfg
To configure Taos Adapter (if has) : edit /etc/taos/taosadapter.toml
To start TDengine     : sudo systemctl start taosd
To access TDengine    : use taos -h ubuntu-1804 in shell OR from http://127.0.0.1:6060

TDengine is updated successfully!
Install taoskeeper as a standalone service
taoskeeper is installed, enable it by `systemctl enable taoskeeper`
```

:::info
install.sh 安装脚本在执行过程中，会通过命令行交互界面询问一些配置信息。如果希望采取无交互安装方式，那么可以用 -e no 参数来执行 install.sh 脚本。运行 `./install.sh -h` 指令可以查看所有参数的详细说明信息。

:::

</TabItem>
</Tabs>

:::note
当安装第一个节点时，出现 Enter FQDN：提示的时候，不需要输入任何内容。只有当安装第二个或以后更多的节点时，才需要输入已有集群中任何一个可用节点的 FQDN，支持该新节点加入集群。当然也可以不输入，而是在新节点启动前，配置到新节点的配置文件中。

:::

## 卸载

<Tabs>
<TabItem label="Deb 卸载" value="debuninst">

卸载命令如下:

```
$ sudo dpkg -r tdengine
(Reading database ... 137504 files and directories currently installed.)
Removing tdengine (2.4.0.7) ...
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
Nginx for TDengine is running, stopping it...
TDengine is removed successfully!

taosKeeper is removed successfully!
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

## 卸载和更新文件说明

卸载安装包的时候，将保留配置文件、数据库文件和日志文件，即 /etc/taos/taos.cfg 、 /var/lib/taos 、 /var/log/taos 。如果用户确认后不需保留，可以手工删除，但一定要慎重，因为删除后，数据将永久丢失，不可以恢复！

如果是更新安装，当缺省配置文件（ /etc/taos/taos.cfg ）存在时，仍然使用已有的配置文件，安装包中携带的配置文件修改为 taos.cfg.orig 保存在 /usr/local/taos/cfg/ 目录，可以作为设置配置参数的参考样例；如果不存在配置文件，就使用安装包中自带的配置文件。

## 启动和停止

TDengine 使用 Linux 系统的 systemd/systemctl/service 来管理系统的启动和、停止、重启操作。TDengine 的服务进程是 taosd，默认情况下 TDengine 在系统启动后将自动启动。DBA 可以通过 systemd/systemctl/service 手动操作停止、启动、重新启动服务。

以 systemctl 为例，命令如下：

- 启动服务进程：`systemctl start taosd`

- 停止服务进程：`systemctl stop taosd`

- 重启服务进程：`systemctl restart taosd`

- 查看服务状态：`systemctl status taosd`

注意：TDengine 在 2.4 版本之后包含一个独立组件 taosAdapter 需要使用 systemctl 命令管理 taosAdapter 服务的启动和停止。

如果服务进程处于活动状态，则 status 指令会显示如下的相关信息：

   ```
   Active: active (running)
   ```

如果后台服务进程处于停止状态，则 status 指令会显示如下的相关信息：

   ```
   Active: inactive (dead)
   ```

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