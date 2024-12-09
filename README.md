# taosX - Zero-code Ingestion Pipeline for TDengine

<!-- omit in toc -->

## Table of Contents

<!-- omit in toc -->

- [Table of Contents](#table-of-contents)
- [1. Introduction](#1-introduction)
- [2. Clone Repository](#2-clone-repository)
- [3. Prerequisites](#3-prerequisites)
- [4. Build](#4-build)
- [5. Packaging](#5-packaging)
- [6. Installation](#6-installation)
- [7. Running](#7-running)
- [8. Testing](#8-testing)
- [9. CI/CD](#9-cicd)
- [10. Coverage](#10-coverage)
- [11. Contributing](#11-contributing)

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

## 2. Clone Repository

```bash
git clone --depth 1 https://github.com/taosdata/taosx.git
```

## 3. Prerequisites

taosX uses Rust for its development. You need to install Rust and other required tools and libraries. Here is a script to install all dependencies with specified versions:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash
cargo install cargo-make toml
```

For UI development, you need to install Node.js. We recommend you to install [NVM](https://github.com/nvm-sh/nvm) for Node.js version manager:

```bash
# Install NVM
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
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
sudo apt install openjdk-18-jdk maven
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

## 4. Build

Build the system using cargo-make:

```bash
cd taosx
cargo make build-all
```

## 5. Packaging

You need python3 environment and some packages from PyPI for packaging.

```bash
sudo apt install python3
pip3 install toml
```

To package taosX only, you can type this:

```bash
cd packaging
python3 package.py
```

Check out more packaging options by `python3 package.py --help`.

## 6. Installation

taosX is delivered along with TDengine Enterprise Edition, so you do not need to install it separately. But we do provide a way to install it locally:

```bash
cargo make install-locally
```

## 7. Running

You can run taosx and taos-explorer without installation:

```bash
./target/release/taos-explorer --help
./target/release/taosx --help
```

After installation, you can start taosx and taos-explorer service with systemd:

```bash
sudo systemctl start taosx
sudo systemctl start taos-explorer
```

Open your web-browser to with url <http://localhost:6060> and enjoy!

## 8. Testing

Rust all the test cases is simple:

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

## 9. CI/CD

We use GitHub Actions for CI/CD workflow configuration. See [.github/workflows/pr-ci.yaml](https://github.com/taosdata/taosx/blob/main/.github/workflows/pr-ci.yaml).

## 10. Coverage

We collect code coverage with `cargo-llvm-cov`:

```bash
# Install llvm-cov
cargo install cargo-llvm-cov
# Collect code coverage
cargo llvm-cov --html --open nextest run --workspace
```

## 11. Contributing

Contributions are welcome! Please follow these steps:

- Add source code.
- Add various types of test cases.
- Submit a Pull Request.

For detailed contribution guidelines, please refer to [CONTRIBUTING.md](https://github.com/taosdata/taosx/blob/main/CONTRIBUTING.md).
