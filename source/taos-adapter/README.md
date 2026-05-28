# taosAdapter

[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/taosdata/taosadapter/build.yml)](https://github.com/taosdata/taosadapter/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/taosdata/taosadapter/graph/badge.svg?token=WCN19U180U)](https://codecov.io/gh/taosdata/taosadapter)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/taosdata/taosadapter)
![GitHub License](https://img.shields.io/github/license/taosdata/taosadapter)
![GitHub Tag](https://img.shields.io/github/v/tag/taosdata/taosadapter?label=latest)
<br />
[![Twitter Follow](https://img.shields.io/twitter/follow/tdenginedb?label=TDengine&style=social)](https://twitter.com/tdenginedb)
[![YouTube Channel](https://img.shields.io/badge/Subscribe_@tdengine--white?logo=youtube&style=social)](https://www.youtube.com/@tdengine)
[![Discord Community](https://img.shields.io/badge/Join_Discord--white?logo=discord&style=social)](https://discord.com/invite/VZdSuUg4pS)
[![LinkedIn](https://img.shields.io/badge/Follow_LinkedIn--white?logo=linkedin&style=social)](https://www.linkedin.com/company/tdengine)
[![StackOverflow](https://img.shields.io/badge/Ask_StackOverflow--white?logo=stackoverflow&style=social&logoColor=orange)](https://stackoverflow.com/questions/tagged/tdengine)

English | [简体中文](./README-CN.md)

A REST/WebSocket gateway and protocol adapter service for TDengine.

## Table of Contents

- [1. Introduction](#1-introduction)
- [2. Documentation](#2-documentation)
- [3. Prerequisites](#3-prerequisites)
  - [3.1 System Requirements](#31-system-requirements)
  - [3.2 Installing Build Tools](#32-installing-build-tools)
  - [3.3 TDengine Client Library](#33-tdengine-client-library)
- [4. Building](#4-building)
  - [4.1 Build with Version Info](#41-build-with-version-info)
- [5. Testing](#5-testing)
  - [5.1 Test Execution](#51-test-execution)
  - [5.2 Test Case Addition](#52-test-case-addition)
  - [5.3 Performance Testing](#53-performance-testing)
- [6. Packaging](#6-packaging)
- [7. Configuration & Running](#7-configuration--running)
- [8. CI/CD](#8-cicd)
- [9. Submitting Issues](#9-submitting-issues)
- [10. Submitting PRs](#10-submitting-prs)
- [11. References](#11-references)
- [12. License](#12-license)

## 1. Introduction

taosAdapter is a companion tool for TDengine, serving as a bridge and adapter between the TDengine cluster and
applications. It provides an easy and efficient way to ingest data directly from data collection agents (such as
Telegraf, StatsD, collectd, etc.). It also offers InfluxDB/OpenTSDB compatible data ingestion interfaces, allowing
InfluxDB/OpenTSDB applications to be seamlessly ported to TDengine. The connectors of TDengine in various languages
communicate with TDengine through the WebSocket interface, hence the taosAdapter must be installed.

It is written in Go and built with CGO enabled because it links against TDengine native client libraries.
Before building, testing, or running taosAdapter, install TDengine so that the required native libraries are available on the system.

## 2. Documentation

- To use taosAdapter, please refer to
  the [taosAdapter Reference](https://docs.tdengine.com/tdengine-reference/components/taosadapter/).
- This quick guide is mainly for developers who like to contribute/build/test the taosAdapter by themselves. To learn
  about TDengine, you can visit the [official documentation](https://docs.tdengine.com).

## 3. Prerequisites

### 3.1 System Requirements

- Linux x86_64 or ARM64
- TDengine server or client package **installed** (provides `libtaosnative.so` and header files)
- Go >= 1.23

### 3.2 Installing Build Tools

**Go >= 1.23 is required.**

**Ubuntu/Debian:**
```bash
# Install Go (if not already installed)
case "$(uname -m)" in
  x86_64)  go_arch=amd64 ;;
  aarch64|arm64) go_arch=arm64 ;;
  *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
esac
wget https://go.dev/dl/go1.23.4.linux-${go_arch}.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.23.4.linux-${go_arch}.tar.gz
export PATH=/usr/local/go/bin:$PATH
```

**CentOS/RHEL:**
Same Go installation steps as above.

### 3.3 TDengine Client Library

taosAdapter builds against `libtaosnative.so` (via CGO) and uses TDengine client libraries at runtime. You must install a TDengine server or client package first.

**Option A: Install from official release package (recommended)**

Download from [TDengine Releases](https://github.com/taosdata/TDengine/releases):

```bash
# Example for server package:
tar xf TDengine-server-<version>-Linux-*.tar.gz
cd TDengine-server-<version>/
sudo ./install.sh -e no
```

**Option B: Build TDengine from source**

```bash
git clone https://github.com/taosdata/TDengine.git
cd TDengine
mkdir debug && cd debug
cmake .. -DBUILD_CONTRIB=ON
make -j$(nproc)
cd ..
packaging/pack_community_tar.sh -c debug -n <version>
cd release && tar xf TDengine-server-*.tar.gz
cd TDengine-server-*/ && sudo ./install.sh -e no
```

After installation, verify the library is accessible:

```bash
ls /usr/lib/libtaosnative.so   # symlink should exist
ls /usr/local/taos/include/taos.h
```

If `libtaos.so` is not in your runtime library path, export it explicitly before running taosAdapter or the test suite:

```bash
export LD_LIBRARY_PATH=/usr/local/taos/driver:$LD_LIBRARY_PATH
```

## 4. Building

```bash
git clone https://github.com/taosdata/taosadapter.git
cd taosadapter
go build -o taosadapter
```

The resulting binary is `./taosadapter`.

> **Note:** On Linux, `CGO_ENABLED=1` is the default when building natively.
> If cross-compiling or building in a restricted environment, set `export CGO_ENABLED=1` explicitly.

### 4.1 Build with Version Info

```bash
go build -ldflags "-X main.version=3.x.x.x -X main.commitID=$(git rev-parse HEAD)" -o taosadapter
```

## 5. Testing

### 5.1 Test Execution

1. Before running tests, ensure that the TDengine server is installed and the `taosd` is running.
   The database should be empty.
2. Most tests require access to `libtaos.so`. If needed, export `LD_LIBRARY_PATH=/usr/local/taos/driver:$LD_LIBRARY_PATH` first.
3. In the project directory, run `go test ./...` to execute the tests. The tests will connect to the local TDengine
   server and taosAdapter for testing.
4. For detailed output, run `go test -v ./...`.
5. To run a specific package, for example:

   ```bash
   go test -v ./controller/rest/
   ```

If your environment does not use `systemd`, start `taosd` with your platform's normal service command.

### 5.2 Test Case Addition

Add test cases to the `*_test.go` file to ensure that the test cases cover the new code.

### 5.3 Performance Testing

Performance testing is in progress.

## 6. Packaging

`pack_adapter_tar.sh` creates a Linux-only taosAdapter tar.gz package with a two-layer layout:

- **Inner `package.tar.gz`**: `bin/taosadapter`, `cfg/taosadapter.toml`, `cfg/taosadapter.service`
- **Outer package**: `install_adapter.sh`, `uninstall_adapter.sh`, `package.tar.gz`

Before packaging or installing taosAdapter, install the taos-community package first so that `libtaos.so` is available on the target system.

### 6.1 Build the release binary

```bash
mkdir -p build/bin
CGO_ENABLED=1 go build -ldflags="-s -w" -o build/bin/taosadapter
```

### 6.2 Create the tar.gz package

From the `source/taos-adapter/packaging/` directory:

```bash
bash ./pack_adapter_tar.sh -c ../build -n 3.3.6.0
```

Optional arguments:

- `-m <compat_version>`: keep the community-compatible CLI argument (default `3.0.0.0`)
- `-V stable|beta`: choose the output package type (default `stable`)

Expected output paths:

- Stable: `source/taos-adapter/release/taosAdapter-<version>-Linux-<arch>.tar.gz`
- Beta: `source/taos-adapter/release/taosAdapter-<version>-beta-Linux-<arch>.tar.gz`

### 6.3 Install from the generated package

```bash
tar xzf taosAdapter-3.3.6.0-Linux-<arch>.tar.gz
cd taosAdapter-3.3.6.0
sudo bash ./install_adapter.sh
```

The installer extracts the inner `package.tar.gz`, installs `taosadapter` to `/usr/bin/`, places the config in `/etc/taos/`, installs the systemd unit in `/etc/systemd/system/`, and preserves any existing config as `taosadapter.toml.new`.

If you need a non-interactive install, use:

```bash
sudo bash ./install_adapter.sh -s
```

## 7. Configuration & Running

A sample configuration file is provided at `example/config/taosadapter.toml`. Packaged deployments typically use `/etc/taosadapter/taosadapter.toml`.

```bash
# Run with default config
./taosadapter

# Run with custom config
./taosadapter -c /path/to/taosadapter.toml

# Run as systemd service (after installing TDengine package)
sudo systemctl start taosadapter
```

Default port: 6041 (REST API), 6042 (WebSocket)

For product usage and API details, see the [taosAdapter reference documentation](https://docs.tdengine.com/tdengine-reference/components/taosadapter/).

## 8. CI/CD

- [Build Workflow](https://github.com/taosdata/taosadapter/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/gh/taosdata/taosadapter)

## 9. Submitting Issues

We welcome the submission of [GitHub Issue](https://github.com/taosdata/taosadapter/issues/new?template=Blank+issue).
When submitting, please provide the following information:

- Description of the issue and whether it is consistently reproducible.
- taosAdapter version.
- TDengine version.

## 10. Submitting PRs

We welcome developers to contribute to this project. When submitting PRs, please follow these steps:

1. Fork this project. Please refer to [how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo).
2. Create a new branch from the main branch with a meaningful branch name (`git checkout -b my_branch`).
3. Modify the code, ensure all unit tests pass, and add new unit tests to verify the changes.
4. Push the changes to the remote branch (`git push origin my_branch`).
5. Create a Pull Request on GitHub. Please refer to [how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request).
6. After submitting the PR, you can find your PR through the [Pull Request](https://github.com/taosdata/taosadapter/pulls). Click on the corresponding link to see if the CI for your PR has passed. If it has passed, it will display "All checks have passed". Regardless of whether the CI passes or not, you can click "Show all checks" -> "Details" to view the detailed test case logs.
7. After submitting the PR, if the CI passes, you can find your PR on the [codecov](https://app.codecov.io/gh/taosdata/taosadapter/pulls) page to check the coverage.

## 11. References

- [TDengine Official Website](https://tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 12. License

[MIT License](./LICENSE)
