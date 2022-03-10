
# How to install/uninstall TDengine with installation package

TDengine open source version provides `deb` and `rpm` format installation packages. Our users can choose the appropriate installation package according to their own running environment. The `deb` supports Debian/Ubuntu etc. and the `rpm` supports CentOS/RHEL/SUSE etc. We also provide `tar.gz` format installers for enterprise users.

## Install and uninstall deb package

### Install deb package

- Download and obtain the deb installation package from the official website, such as TDengine-server-2.0.0.0-Linux-x64.deb.
- Go to the directory where the TDengine-server-2.0.0.0-Linux-x64.deb installation package is located and execute the following installation command.

```
$ sudo dpkg -i TDengine-server-2.4.0.7-Linux-x64.deb
(Reading database ... 137504 files and directories currently installed.)
Preparing to unpack TDengine-server-2.4.0.7-Linux-x64.deb ...
TDengine is removed successfully!
Unpacking tdengine (2.4.0.7) over (2.4.0.7) ...
Setting up tdengine (2.4.0.7) ...
Start to install TDengine...

System hostname is: shuduo-1804

Enter FQDN:port (like h1.taosdata.com:6030) of an existing TDengine cluster node to join
OR leave it blank to build one:

Enter your email address for priority support or enter empty to skip:
Created symlink /etc/systemd/system/multi-user.target.wants/taosd.service → /etc/systemd/system/taosd.service.

To configure TDengine : edit /etc/taos/taos.cfg
To start TDengine     : sudo systemctl start taosd
To access TDengine    : taos -h shuduo-1804 to login into TDengine server


TDengine is installed successfully!
```

Note: When the Enter FQDN: prompt appears when the first node is installed, nothing needs to be entered. Only when installing the second or later more nodes is it necessary to enter the FQDN of any of the available nodes in the existing cluster to support that new node joining the cluster. It is of course possible to not enter it, but to configure it into the new node's configuration file before the new node starts
in the configuration file of the new node before it starts.

The same operation is performed for the other installation packages format.

### Uninstall deb

Uninstall command is below:

```
$ sudo dpkg -r tdengine
(Reading database ... 137504 files and directories currently installed.)
Removing tdengine (2.4.0.7) ...
TDengine is removed successfully!
```

## Install and uninstall rpm package

### Install rpm

- Download and obtain the rpm installation package from the official website, such as TDengine-server-2.0.0.0-Linux-x64.rpm.
- Go to the directory where the TDengine-server-2.0.0.0-Linux-x64.rpm installation package is located and execute the following installation command.

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

### Uninstall rpm

Uninstall command is following:

```
$ sudo rpm -e tdengine
TDengine is removed successfully!
```

## Install and uninstall tar.gz

### Install tar.gz

- Download and obtain the tar.gz installation package from the official website, such as `TDengine-server-2.0.0.0-Linux-x64.tar.gz`.
- Go to the directory where the `TDengine-server-2.0.0.0-Linux-x64.tar.gz` installation package is located, unzip the file first, then enter the subdirectory and execute the install.sh installation script in it as follows

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
To access TDengine    : use taos -h shuduo-1804 in shell OR from http://127.0.0.1:6060

TDengine is updated successfully!
Install taoskeeper as a standalone service
taoskeeper is installed, enable it by `systemctl enable taoskeeper`
```

Note: The install.sh install script asks for some configuration information through an interactive command line interface during execution. If you prefer a non-interactive installation, you can execute the install.sh script with the -e no parameter. Run . /install.sh -h command to see detailed information about all parameters.

### Uninstall TDengine after tar.gz package installed

Uninstall command is following:

```
$ rmtaos
Nginx for TDengine is running, stopping it...
TDengine is removed successfully!

taosKeeper is removed successfully!
```

## Installation directory description

After TDengine is successfully installed, the main installation directory is /usr/local/taos, and the directory contents are as follows:

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

- Automatically generates the configuration file directory, database directory, and log directory.
- Configuration file default directory: /etc/taos/taos.cfg, soft-linked to /usr/local/taos/cfg/taos.cfg.
- Database default directory: /var/lib/taos, soft-linked to /usr/local/taos/data.
- Log default directory: /var/log/taos, soft-linked to /usr/local/taos/log.
- executables in the /usr/local/taos/bin directory, which are soft-linked to the /usr/bin directory.
- Dynamic library files in the /usr/local/taos/driver directory, which are soft-linked to the /usr/lib directory.
- header files in the /usr/local/taos/include directory, which are soft-linked to the /usr/include directory.

## Uninstall and update file instructions

When uninstalling the installation package, the configuration files, database files and log files will be kept, i.e. /etc/taos/taos.cfg, /var/lib/taos, /var/log/taos. If users confirm that they do not need to keep them, they can delete them manually, but must be careful, because after deletion, the data will be permanently lost and cannot be recovered!

If the installation is updated, when the default configuration file (/etc/taos/taos.cfg) exists, the existing configuration file is still used. The configuration file carried in the installation package is modified to taos.cfg.orig and saved in the /usr/local/taos/cfg/ directory, which can be used as a reference sample for setting configuration parameters; if the configuration file does not exist, the use the configuration file that comes with the installation package file that comes with the installation package.

## Caution

- TDengine provides several installers, but it is best not to use both the tar.gz installer and the deb or rpm installer on one system. Otherwise, they may affect each other and cause problems when using them.

- For deb package installation, if the installation directory is manually deleted by mistake, the uninstallation, or reinstallation cannot be successful. In this case, you need to clear the installation information of the TDengine package by executing the following command:

```
$ sudo rm -f /var/lib/dpkg/info/tdengine*
```

Then just reinstall it.

- For the rpm package after installation, if the installation directory is manually deleted by mistake part of the uninstallation, or reinstallation can not be successful. In this case, you need to clear the installation information of the TDengine package by executing the following command:

```
$ sudo rpm -e --noscripts tdengine
```

Then just reinstall it.
