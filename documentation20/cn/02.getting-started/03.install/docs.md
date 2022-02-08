
# TDengine 安装包的安装和卸载

TDengine 开源版本提供 deb 和 rpm 格式安装包，用户可以根据自己的运行环境选择合适的安装包。其中 deb 支持 Debian/Ubuntu 等系统，rpm 支持 CentOS/RHEL/SUSE 等系统。同时我们也为企业用户提供 tar.gz 格式安装包。

## deb 包的安装和卸载

### 安装 deb

1、从官网下载获得deb安装包，比如TDengine-server-2.0.0.0-Linux-x64.deb；
2、进入到TDengine-server-2.0.0.0-Linux-x64.deb安装包所在目录，执行如下的安装命令：

```
plum@ubuntu:~/git/taosv16$ sudo dpkg -i TDengine-server-2.0.0.0-Linux-x64.deb

Selecting previously unselected package tdengine.
(Reading database ... 233181 files and directories currently installed.)
Preparing to unpack TDengine-server-2.0.0.0-Linux-x64.deb ...
Failed to stop taosd.service: Unit taosd.service not loaded.
Stop taosd service success!
Unpacking tdengine (2.0.0.0) ...
Setting up tdengine (2.0.0.0) ...
Start to install TDEngine...
Synchronizing state of taosd.service with SysV init with /lib/systemd/systemd-sysv-install...
Executing /lib/systemd/systemd-sysv-install enable taosd
insserv: warning: current start runlevel(s) (empty) of script `taosd' overrides LSB defaults (2 3 4 5).
insserv: warning: current stop runlevel(s) (0 1 2 3 4 5 6) of script `taosd' overrides LSB defaults (0 1 6).
Enter FQDN:port (like h1.taosdata.com:6030) of an existing TDengine cluster node to join OR leave it blank to build one :
To configure TDengine : edit /etc/taos/taos.cfg
To start TDengine     : sudo systemctl start taosd
To access TDengine    : use taos in shell
TDengine is installed successfully!
```

注：当安装第一个节点时，出现 Enter FQDN：提示的时候，不需要输入任何内容。只有当安装第二个或以后更多的节点时，才需要输入已有集群中任何一个可用节点的 FQDN，支持该新节点加入集群。当然也可以不输入，而是在新节点启动前，配置到新节点的配置文件中。

后续两种安装包也是同样的操作。

### 卸载 deb

卸载命令如下:

```
    plum@ubuntu:~/git/tdengine/debs$ sudo dpkg -r tdengine
    (Reading database ... 233482 files and directories currently installed.)
    Removing tdengine (2.0.0.0) ...
    TDEngine is removed successfully!
```

## rpm包的安装和卸载

### 安装 rpm

1、从官网下载获得rpm安装包，比如TDengine-server-2.0.0.0-Linux-x64.rpm；
2、进入到TDengine-server-2.0.0.0-Linux-x64.rpm安装包所在目录，执行如下的安装命令：

```
    [root@bogon x86_64]# rpm -iv TDengine-server-2.0.0.0-Linux-x64.rpm
    Preparing packages...
    TDengine-2.0.0.0-3.x86_64
    Start to install TDEngine...
    Created symlink from /etc/systemd/system/multi-user.target.wants/taosd.service to /etc/systemd/system/taosd.service.
    Enter FQDN:port (like h1.taosdata.com:6030) of an existing TDengine cluster node to join OR leave it blank to build one :
    To configure TDengine : edit /etc/taos/taos.cfg
    To start TDengine     : sudo systemctl start taosd
    To access TDengine    : use taos in shell
    TDengine is installed successfully!
```

### 卸载 rpm

卸载命令如下:

```
    [root@bogon x86_64]# rpm -e tdengine
    TDEngine is removed successfully!
```

## tar.gz 格式安装包的安装和卸载

### 安装 tar.gz 安装包

1、从官网下载获得tar.gz安装包，比如TDengine-server-2.0.0.0-Linux-x64.tar.gz；
2、进入到TDengine-server-2.0.0.0-Linux-x64.tar.gz安装包所在目录，先解压文件后，进入子目录，执行其中的install.sh安装脚本：

```
    plum@ubuntu:~/git/tdengine/release$ sudo tar -xzvf TDengine-server-2.0.0.0-Linux-x64.tar.gz
    plum@ubuntu:~/git/tdengine/release$ ll
    total 3796
    drwxr-xr-x  3 root root    4096 Aug  9 14:20 ./
    drwxrwxr-x 11 plum plum    4096 Aug  8 11:03 ../
    drwxr-xr-x  5 root root    4096 Aug  8 11:03 TDengine-server/
    -rw-r--r--  1 root root 3871844 Aug  8 11:03 TDengine-server-2.0.0.0-Linux-x64.tar.gz
    plum@ubuntu:~/git/tdengine/release$ cd TDengine-server/
    plum@ubuntu:~/git/tdengine/release/TDengine-server$ ll
    total 2640
    drwxr-xr-x 5 root root    4096 Aug  8 11:03 ./
    drwxr-xr-x 3 root root    4096 Aug  9 14:20 ../
    drwxr-xr-x 5 root root    4096 Aug  8 11:03 connector/
    drwxr-xr-x 2 root root    4096 Aug  8 11:03 driver/
    drwxr-xr-x 8 root root    4096 Aug  8 11:03 examples/
    -rwxr-xr-x 1 root root   13095 Aug  8 11:03 install.sh*
    -rw-r--r-- 1 root root 2651954 Aug  8 11:03 taos.tar.gz
    plum@ubuntu:~/git/tdengine/release/TDengine-server$ sudo ./install.sh
    This is ubuntu system
    verType=server interactiveFqdn=yes
    Start to install TDengine...
    Synchronizing state of taosd.service with SysV init with /lib/systemd/systemd-sysv-install...
    Executing /lib/systemd/systemd-sysv-install enable taosd
    insserv: warning: current start runlevel(s) (empty) of script `taosd' overrides LSB defaults (2 3 4 5).
    insserv: warning: current stop runlevel(s) (0 1 2 3 4 5 6) of script `taosd' overrides LSB defaults (0 1 6).
    Enter FQDN:port (like h1.taosdata.com:6030) of an existing TDengine cluster node to join OR leave it blank to build one :hostname.taosdata.com:7030
    To configure TDengine : edit /etc/taos/taos.cfg
    To start TDengine     : sudo systemctl start taosd
    To access TDengine    : use taos in shell
    Please run: taos -h hostname.taosdata.com:7030  to login into cluster, then execute : create dnode 'newDnodeFQDN:port'; in TAOS shell to add this new node into the clsuter
    TDengine is installed successfully!
```

说明：install.sh 安装脚本在执行过程中，会通过命令行交互界面询问一些配置信息。如果希望采取无交互安装方式，那么可以用 -e no 参数来执行 install.sh 脚本。运行 ./install.sh -h 指令可以查看所有参数的详细说明信息。

### tar.gz 安装后的卸载

卸载命令如下:

```
    plum@ubuntu:~/git/tdengine/release/TDengine-server$ rmtaos
    TDEngine is removed successfully!
```

## 安装目录说明

TDengine成功安装后，主安装目录是/usr/local/taos，目录内容如下：

```
    plum@ubuntu:/usr/local/taos$ cd /usr/local/taos
    plum@ubuntu:/usr/local/taos$ ll
    total 36
    drwxr-xr-x  9 root root 4096 7月  30 19:20 ./
    drwxr-xr-x 13 root root 4096 7月  30 19:20 ../
    drwxr-xr-x  2 root root 4096 7月  30 19:20 bin/
    drwxr-xr-x  2 root root 4096 7月  30 19:20 cfg/
    lrwxrwxrwx  1 root root   13 7月  30 19:20 data -> /var/lib/taos/
    drwxr-xr-x  2 root root 4096 7月  30 19:20 driver/
    drwxr-xr-x  8 root root 4096 7月  30 19:20 examples/
    drwxr-xr-x  2 root root 4096 7月  30 19:20 include/
    drwxr-xr-x  2 root root 4096 7月  30 19:20 init.d/
    lrwxrwxrwx  1 root root   13 7月  30 19:20 log -> /var/log/taos/
```

- 自动生成配置文件目录、数据库目录、日志目录。
- 配置文件缺省目录：/etc/taos/taos.cfg， 软链接到/usr/local/taos/cfg/taos.cfg；
- 数据库缺省目录：/var/lib/taos， 软链接到/usr/local/taos/data；
- 日志缺省目录：/var/log/taos， 软链接到/usr/local/taos/log；
- /usr/local/taos/bin目录下的可执行文件，会软链接到/usr/bin目录下；
- /usr/local/taos/driver目录下的动态库文件，会软链接到/usr/lib目录下；
- /usr/local/taos/include目录下的头文件，会软链接到到/usr/include目录下；

## 卸载和更新文件说明

卸载安装包的时候，将保留配置文件、数据库文件和日志文件，即 /etc/taos/taos.cfg 、 /var/lib/taos 、 /var/log/taos 。如果用户确认后不需保留，可以手工删除，但一定要慎重，因为删除后，数据将永久丢失，不可以恢复！

如果是更新安装，当缺省配置文件（ /etc/taos/taos.cfg ）存在时，仍然使用已有的配置文件，安装包中携带的配置文件修改为taos.cfg.org保存在 /usr/local/taos/cfg/ 目录，可以作为设置配置参数的参考样例；如果不存在配置文件，就使用安装包中自带的配置文件。

## 注意事项

- TDengine提供了多种安装包，但最好不要在一个系统上同时使用 tar.gz 安装包和 deb 或 rpm 安装包。否则会相互影响，导致在使用时出现问题。

- 对于deb包安装后，如果安装目录被手工误删了部分，出现卸载、或重新安装不能成功。此时，需要清除 tdengine 包的安装信息，执行如下命令：

```
    plum@ubuntu:~/git/tdengine/$ sudo rm -f /var/lib/dpkg/info/tdengine*
```

然后再重新进行安装就可以了。

- 对于rpm包安装后，如果安装目录被手工误删了部分，出现卸载、或重新安装不能成功。此时，需要清除tdengine包的安装信息，执行如下命令：

```
    [root@bogon x86_64]# rpm -e --noscripts tdengine
```

然后再重新进行安装就可以了。
