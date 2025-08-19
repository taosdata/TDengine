# Table of Contents

1. [Introduction](#1-introduction)
1. [Documentation](#2-documentation)
1. [Prerequisites](#3-prerequisites)
1. [Building](#4-building)
1. [Packaging](#5-packaging)
1. [Installation](#6-installing)
1. [Running](#7-running)
1. [Testing](#8-testing)
1. [Releasing](#9-releasing)
1. [CI/CD](#10-cicd)
1. [Contributing](#11-contributing)

# 1. Introduction

TDgpt: an analytic platform for TDengine

# 2. Documentation

For user manual, system design and architecture, please refer to [TDengine Documentation](https://docs.tdengine.com/next) ([TDengine 文档](https://docs.taosdata.com/next)).

# 3. Prerequisites

List the software and tools required to work on the project.

- python 3.10.12+ (for test)

Step-by-step instructions to set up the prerequisites software.

## 3.1 Install Python3.10
Make sure Python3.10 or above is available before installing anode in your system.

In case of Ubuntu, use the following instructions to install Python 3.10.

```
sudo apt-get install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.10
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 2
sudo update-alternatives --config python3
sudo apt install python3.10-venv
sudo apt install python3.10-dev
```

Install the Pip3.10

```bash
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10
```

Add the ``~/.local/bin`` into ``~/.bashrc`` or ``~/.bash_profile``

```bash
export PATH=$PATH:~/.local/bin
```

# 4. Building
There is no need to build the taosanode, since it is implemented in Python, which is an interpreted language.


# 5. Packaging
In the base directory, you can use the following command to package to build an tarball.

```bash
cd script && ./release.sh -e community -v 3.3.7.0
```

After the packaging is completed, you will find the tarball in the `release` directory.

```bash
ls -lht release

-rw-rw-r-- 1 root root 74K Feb 21 17:04 tdengine-tdgpt-oss-3.3.7.0-linux-x64.tar.gz
```

# 6. Installing

## 6.1 Install taosanode

Please use the following command to install taosanode in your system.

```bash
./install.sh
```

During the installation, Python virtual environment will be established in `/var/lib/taos/taosanode/venv` by default, as well as the required libraries.
The taosanode will be installed as an system service, but will not automatic started when installed. You need to start the service mannually, by using the following command

```bash
systemctl start taosanoded
```

## 6.2 Configure the Service
taosanode provides the RESTFul service powered by `uWSGI`. You can config the options to tune the 
performance by changing the default configuration file `taosanode.ini` located in `/etc/taos`, which is also the configuration directory for `taosd` service.

```ini
# taosanode service ip:port
http = 127.0.0.1:6090
```

# 7. Running
## 7.1 Start/Stop Service
`systemctl start/stop/restart taosanoded.service` will start/stop/restart the service of taosanode.


## 7.2 Uninstall
The command `rmtaosanode` will remove the installed taosanode from your system. Note that the python environment won't removed by this script, you need to remove it mannually.

# 8. Testing

We use Github Actions to run the test suite. Please refer to the workflow definition yaml file in [.github/workflows](../../.github/workflows/) for details.

# 9 Releasing

For the complete list of taosanode Releases, please see Releases.

# 10 CI/CD

We use Github Actions for CI/CD workflow configuration. Please refer to the workflow definition yaml file in [.github/workflows](../../.github/workflows/) for details.

# 11 Contributing

Guidelines for contributing to the project:

- Fork the repository
- Create a feature branch
- Submit a pull request

