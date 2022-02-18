
# How to install/uninstall TDengine with installtion package

TDengine open source version provides `deb` and `rpm` format installation packages. Our users can choose the appropriate installation package according to their own running environment. The `deb` supports Debian/Ubuntu etc. and the `rpm` supports CentOS/RHEL/SUSE etc. We also provide `tar.gz` format installers for enterprise users.

## Install and uninstall deb package

### Install deb package

- Download and obtain the deb installation package from the official website, such as TDengine-server-2.0.0.0-Linux-x64.deb.
- Go to the directory where the TDengine-server-2.0.0.0-Linux-x64.deb installation package is located and execute the following installation command.

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

Note: When the Enter FQDN: prompt appears when the first node is installed, nothing needs to be entered. Only when installing the second or later more nodes is it necessary to enter the FQDN of any of the available nodes in the existing cluster to support that new node joining the cluster. It is of course possible to not enter it, but to configure it into the new node's configuration file before the new node starts
in the configuration file of the new node before it starts.

The same operation is performed for the other installation packages format.

### Uninstall deb

Uninstall command is below:

```
    plum@ubuntu:~/git/tdengine/debs$ sudo dpkg -r tdengine
    (Reading database ... 233482 files and directories currently installed.)
    Removing tdengine (2.0.0.0) ...
    TDEngine is removed successfully!
```

## Install and unstall rpm package

### Install rpm

- Download and obtain the rpm installation package from the official website, such as TDengine-server-2.0.0.0-Linux-x64.rpm.
- Go to the directory where the TDengine-server-2.0.0.0-Linux-x64.rpm installation package is located and execute the following installation command.

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

### Uninstall rpm

Uninstall command is following:

```
    [root@bogon x86_64]# rpm -e tdengine
    TDEngine is removed successfully!
```

## Install and uninstall tar.gz

### Install tar.gz

- Download and obtain the tar.gz installation package from the official website, such as `TDengine-server-2.0.0.0-Linux-x64.tar.gz`.
- Go to the directory where the `TDengine-server-2.0.0.0-Linux-x64.tar.gz` installation package is located, unzip the file first, then enter the subdirectory and execute the install.sh installation script in it as follows

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

Note: The install.sh install script asks for some configuration information through an interactive command line interface during execution. If you prefer a non-interactive installation, you can execute the install.sh script with the -e no parameter. Run . /install.sh -h command to see detailed information about all parameters.

### Uninstall TDengine after tar.gz package installed

Uninstall command is following:

```
    plum@ubuntu:~/git/tdengine/release/TDengine-server$ rmtaos
    TDEngine is removed successfully!
```

## Installation directory description

After TDengine is successfully installed, the main installation directory is /usr/local/taos, and the directory contents are as follows:

```
    plum@ubuntu:/usr/local/taos$ cd /usr/local/taos
    plum@ubuntu:/usr/local/taos$ ll
    total 36
    drwxr-xr-x  9 root root 4096 7  30 19:20 ./
    drwxr-xr-x 13 root root 4096 7  30 19:20 ../
    drwxr-xr-x  2 root root 4096 7  30 19:20 bin/
    drwxr-xr-x  2 root root 4096 7  30 19:20 cfg/
    lrwxrwxrwx  1 root root   13 7  30 19:20 data -> /var/lib/taos/
    drwxr-xr-x  2 root root 4096 7  30 19:20 driver/
    drwxr-xr-x  8 root root 4096 7  30 19:20 examples/
    drwxr-xr-x  2 root root 4096 7  30 19:20 include/
    drwxr-xr-x  2 root root 4096 7  30 19:20 init.d/
    lrwxrwxrwx  1 root root   13 7  30 19:20 log -> /var/log/taos/
```

- Automatically generates the configuration file directory, database directory, and log directory.
- Configuration file default directory: /etc/taos/taos.cfg, softlinked to /usr/local/taos/cfg/taos.cfg.
- Database default directory: /var/lib/taos, softlinked to /usr/local/taos/data.
- Log default directory: /var/log/taos, softlinked to /usr/local/taos/log.
- executables in the /usr/local/taos/bin directory, which are soft-linked to the /usr/bin directory.
- Dynamic library files in the /usr/local/taos/driver directory, which are soft-linked to the /usr/lib directory.
- header files in the /usr/local/taos/include directory, which are soft-linked to the /usr/include directory.

## Uninstall and update file instructions

When uninstalling the installation package, the configuration files, database files and log files will be kept, i.e. /etc/taos/taos.cfg, /var/lib/taos, /var/log/taos. If users confirm that they do not need to keep them, they can delete them manually, but must be careful, because after deletion, the data will be permanently lost and cannot be recovered!

If the installation is updated, when the default configuration file (/etc/taos/taos.cfg) exists, the existing configuration file is still used. The configuration file carried in the installation package is modified to taos.cfg.org and saved in the /usr/local/taos/cfg/ directory, which can be used as a reference sample for setting configuration parameters; if the configuration file does not exist, the Use the configuration file that comes with the installation package
file that comes with the installation package.

## Caution

- TDengine provides several installers, but it is best not to use both the tar.gz installer and the deb or rpm installer on one system. Otherwise, they may affect each other and cause problems when using them.

- For deb package installation, if the installation directory is manually deleted by mistake, the uninstallation, or reinstallation cannot be successful. In this case, you need to clear the installation information of the tdengine package by executing the following command:

```
    plum@ubuntu:~/git/tdengine/$ sudo rm -f /var/lib/dpkg/info/tdengine*
```

Then just reinstall it.

- For the rpm package after installation, if the installation directory is manually deleted by mistake part of the uninstallation, or reinstallation can not be successful. In this case, you need to clear the installation information of the tdengine package by executing the following command:

```
    [root@bogon x86_64]# rpm -e --noscripts tdengine
```

Then just reinstall it.
