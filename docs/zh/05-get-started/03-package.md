---
sidebar_label: 安装包
title: 使用安装包安装和卸载
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

:::info
如果您希望对 TDengine 贡献代码或对内部实现感兴趣，请参考我们的 [TDengine GitHub 主页](https://github.com/taosdata/TDengine) 下载源码构建和安装.

:::

TDengine 开源版本提供 deb 和 rpm 格式安装包，用户可以根据自己的运行环境选择合适的安装包。其中 deb 支持 Debian/Ubuntu 及衍生系统，rpm 支持 CentOS/RHEL/SUSE 及衍生系统。同时我们也为企业用户提供 tar.gz 格式安装包。

## 安装

<Tabs>
<TabItem value="apt-get" label="apt-get">
可以使用 apt-get 工具从官方仓库安装。

**安装包仓库**

```
wget -qO - http://repos.taosdata.com/tdengine.key | sudo apt-key add -
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-stable stable main" | sudo tee /etc/apt/sources.list.d/tdengine-stable.list
```

如果安装 Beta 版需要安装包仓库

```
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-beta beta main" | sudo tee /etc/apt/sources.list.d/tdengine-beta.list
```

**使用 apt-get 命令安装**

```
sudo apt-get update
apt-cache policy tdengine
sudo apt-get install tdengine
```

:::tip
apt-get 方式只适用于 Debian 或 Ubuntu 系统
::::
</TabItem>
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
<TabItem label="apt-get 卸载" value="aptremove">

内容 TBD

</TabItem>
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