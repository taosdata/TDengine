# taosX - Zero-code Ingestion Pipeline for TDengine

<!-- omit in toc -->
## Table of Contents
<!-- omit in toc -->

- [1. Introduction](#1-introduction)
- [2. Prerequisites](#2-prerequisites)
- [3. Build](#3-build)
- [4. Packaging](#4-packaging)
- [5. Installation](#5-installation)
- [6. Running](#6-running)
- [7. Testing](#7-testing)
- [8. Releasing](#8-releasing)
- [9. CI/CD](#9-cicd)
- [10. Coverage](#10-coverage)
- [11. Contributing](#11-contributing)
- [12. Documentation](#12-documentation)

## 1. Introduction

taosX is an easy-to-use, feature-rich TDengine data pipeline tool. It's a bridge between a data source and data sink. It supports offline data import/export and real-time data replication from or to a TDengine instance. It's built for performance, reliability, productivity, observability and ergonomics.

Features:

- Databases or tables replication based on subscription.
- Databases or tables data migration from one version to another.
- Incremental backup/restore for database or (S)tables.
- Export or import offline data files, currently support CSV and Parquet.
- Active-Active TDengine deployment and management.
- Web-based UI for database management.
- External data source ingestion pipeline, including:
  - Relational databases: MySQL, Oracle, PostgreSQL, Microsoft SQL Server.
  - Time-series databases: OpenTSDB, InfluxDB.
  - Industry realtime databases: PI System, Aveva Historian.
  - Message queue: Kafka.
  - Others common protocols: OPC-UA/DA, MQTT.

## 2. Prerequisites

taosX uses Rust for its development. You need to install Rust and other required tools and libraries. 

The software installation and script execution mentioned in this and subsequent chapters only support linux system, and will gradually support windows and mac in the future.

You can complete the installation with one click by executing the following shell script:

```bash
wget https://raw.githubusercontent.com/taosdata/TDengine/main/packaging/setup_env.sh
chmod +x setup_env.sh
./setup_dev.sh TDinternal # For taosX and TDinternal, the setup process is the same.
source ~/.bashrc
```

Or you can step through the following steps to complete the environment installation.
Here is a script to install all dependencies with specified versions:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash
cargo install cargo-make toml-cli
```

If you cannot download for a long time when pulling and installing Rust, you can refer to https://rsproxy.cn/ for mirroring source configuration.

```bash
# edit ~/.zshrc or ~/.bashrc
export RUSTUP_DIST_SERVER="https://rsproxy.cn"
export RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"

# install Rust
curl --proto '=https' --tlsv1.2 -sSf https://rsproxy.cn/rustup-init.sh | sh

# Set crates.io mirroring,export content to the file ~/.cargo/config
[source.crates-io]
replace-with = 'rsproxy-sparse'
[source.rsproxy]
registry = "https://rsproxy.cn/crates.io-index"
[source.rsproxy-sparse]
registry = "sparse+https://rsproxy.cn/index/"
[registries.rsproxy]
index = "https://rsproxy.cn/crates.io-index"
[net]
git-fetch-with-cli = true

```

For UI development, you need to install Node.js. We recommend you to install [NVM](https://github.com/nvm-sh/nvm) for Node.js version manager:

```bash
# Install NVM
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
source ~/.bashrc  
# Install Node.js
nvm install 16
nvm use 16
# Check Node.js installed correctly
node --version
# Install Yarn - the package manager for Node.js
npm install -g yarn
```

For external database sources InfluxDB and OpenTSDB, you need Java SDK and [Maven](https://maven.apache.org/) (for Java package management):

```bash
# In Ubuntu 22.04+
sudo apt install openjdk-11-jdk maven
```

For OPC-UA/OPC-DA data sources, you need [Go 1.20+](https://go.dev/doc/install):

```bash
# For amd64/x86_64.
wget https://go.dev/dl/go1.23.4.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.23.4.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
# Check go env.
go version
go env
```



## 3. Build

You can complete the build and install of taosx/agent/taos-explorer/plugins with one click by executing the following shell script:

```bash
chmod +x build_install.sh && ./build_install.sh
```

Or you can step through the following steps to complete build taosx and its plugins.

Clone the code first:

```bash
# Remember username and password of github locally
git config --global credential.helper store
# Clone taosx repository
git clone --depth 1 https://github.com/taosdata/taosx.git
```

Build the system using cargo-make:

```bash
cd taosx
cargo make build-all
```

You can optionally build external plugins such as InfluxDB, OpenTSDB, OPC-UA/DA, by:

```bash
cargo make plugins
```

You can optionally build taosx-agent by:

```bash
cargo make build -p taosx-agent
```

Or build taosx/taos-explorer/taosx-agent all:

```bash
cargo make build-all-with-agent
```

## 4. Packaging

You need python3 environment and some packages from PyPI for packaging.

```bash
pip3 install toml
```

To package taosX ,taos-explorer and plugins, you can type this:

```bash
cd packaging
python3 release.py -o taosx
```
To package taosX-agent and plugins, you can type this:

```bash
cd packaging
python3 release.py -ba 1
```
Check out more packaging options by `python3 release.py --help`.

## 5. Installation

taosX is delivered along with TDengine Enterprise Edition, so you do not need to install it separately. But we do provide a way to install it locally:

```bash
cargo make install-locally
```



## 6. Running

You can run taosx and taos-explorer without installation:

```bash
./target/release/taos-explorer --help
./target/release/taosx --help
```
Before running taosx/taos-explorer, you should install TDengine v3.0+, see the link: [Install TDengine](https://github.com/taosdata/TDinternal?tab=readme-ov-file#6-installing)

After installation, you can start taosx and taos-explorer service with systemd:

```bash
sudo systemctl start taosx
sudo systemctl start taos-explorer
```

Open your web-browser to with url <http://localhost:6060> and find how to create a new agent.


You can also run the following script to start all services, and create a default agent locally:

```bash
chmod +x start_services.sh && ./start_services.sh --agent_name=your_agent_name
```


## 7. Testing

At least 4 cores 16GB of hardware resources are required to run unit tests effectively.

To run Rust all the unitest cases is simple:

```bash
make test
```

If you want to test with specific cases, or start with your own tests, you should read these first:

1. Learn how to run test cases in [Cargo Tests Guide](https://doc.rust-lang.org/cargo/guide/tests.html)
2. Learn the testing tool used in taosX: [cargo-nextest](https://nexte.st/docs/)

To list all test cases:

```bash
cargo nextest list
```

To run the specific test case(s) from above list with `nextest`:

```bash
cargo nextest run --workspace <case-name>
```


Before executing the above command, please confirm that you have correctly remembered the username and password locally in the build step.

To run the e2e test case(s), it can be completed by the following operation.
Please note that before performing this operation, please confirm that the repository code is cloned through the HTTPS protocol. If cloned through the SSH protocol, it may cause an error in the command execution.

```bash
cd tests/e2e && poetry install
```

run all test cases under the directory ```tests/e2e```:

```bash
cd tests/e2e && cp setenv.sh.example setenv.sh && source setenv.sh && poetry run pytest -m sanity

```
More ways to run cases:
```bash
# activate venv, then pytest can be run directly
poetry shell

# run a single case
pytest -sv opcua_test.py::test_sanity

# run all opcua cases
pytest -sv opcua_test.py

# run cases by marker
pytest -sv -m sanity

# run case by keyword
pytest -sv opcua_test.py -k observe
```
To run e2e tests, you need to deploy third-party data sources in advance and modify the tests/e2e/config/env.yaml file to configure the data source environment.
At present, because some test cases rely on external third-party data sources, the test cases depend on the specified testing environment. We are still trying to add third-party data sources to one-click deployment.

## 8. Releasing

taosx and related components, which are released with TDengine Enterprise, don't have separate installer. TDengine Enterprise installer can be found on the corporate NAS server:

- NAS Server URL： http://192.168.1.252:5000/
- Directory: /Release/TDengine/

NAS server write permission is enabled on `192.168.1.131`. To release taosx agent, please follow steps below, take v3.3.4.0 for example:

```bash
# create the release directory first
ssh root@192.168.1.131
mkdir -p /pkgs/TDengine/3.3/v3.3.4.0/enterprise

# copy the installer to release directory
scp <agent_installer> root@192.168.1.131:/pkgs/TDengine/3.3/v3.3.4.0/enterprise/
```

## 9. CI/CD

We use GitHub Actions for CI/CD workflow configuration. See [.github/workflows/pr-ci.yaml](https://github.com/taosdata/taosx/blob/main/.github/workflows/pr-ci.yaml).

Due to the complexity of the data source environment, we have not yet provided a way to run CI tests locally.

## 10. Coverage

We collect code coverage with `cargo-llvm-cov`:

```bash
# Install llvm-cov
cargo install cargo-llvm-cov
# Collect code coverage for taosx unitest
cargo llvm-cov --html --open nextest run --workspace
```

We use GitHub Actions for testing coverage workflow configuration.See [.github/workflows/3.0-qa-ci.yaml](https://github.com/taosdata/taosx/actions/workflows/3.0-qa-ci.yaml)

## 11. Contributing

Contributions are welcome! Please follow these steps:

- Add source code.
- Add various types of test cases.
- Submit a Pull Request.

For detailed contribution guidelines, please refer to [CONTRIBUTING.md](https://github.com/taosdata/taosx/blob/main/CONTRIBUTING.md).

## 12. Documentation

For developers:
- [Development Documentation](docs/dev/README.md) - Development guides and technical documentation
- **Test Architecture**:
  - [Phase 1 Completion Report](docs/dev/PHASE_1_COMPLETION_REPORT.md) - Phase 1 implementation status and results ✅
  - [Test Refactoring Summary](docs/dev/TEST_REFACTORING_SUMMARY.md) - Project overview and timeline
  - [Technical Design Plan](docs/dev/TEST_REFACTORING_PLAN.md) - Complete architecture design
  - [Quick Start Guide](docs/dev/TEST_QUICKSTART.md) - Common commands and development scenarios
  - [Kafka Migration Example](docs/dev/TEST_MIGRATION_EXAMPLE.md) - Reference implementation
