<!-- omit in toc -->
# TDengine ODBC Connector

![GitHub commit activity](https://img.shields.io/github/commit-activity/m/taosdata/taos-connector-odbc)
![GitHub License](https://img.shields.io/github/license/taosdata/taos-connector-odbc)
![GitHub Tag](https://img.shields.io/github/v/tag/taosdata/taos-connector-odbc?label=latest)
<br />
[![Twitter Follow](https://img.shields.io/twitter/follow/tdenginedb?label=TDengine&style=social)](https://twitter.com/tdenginedb)
[![YouTube Channel](https://img.shields.io/badge/Subscribe_@tdengine--white?logo=youtube&style=social)](https://www.youtube.com/@tdengine)
[![Discord Community](https://img.shields.io/badge/Join_Discord--white?logo=discord&style=social)](https://discord.com/invite/VZdSuUg4pS)
[![LinkedIn](https://img.shields.io/badge/Follow_LinkedIn--white?logo=linkedin&style=social)](https://www.linkedin.com/company/tdengine)
[![StackOverflow](https://img.shields.io/badge/Ask_StackOverflow--white?logo=stackoverflow&style=social&logoColor=orange)](https://stackoverflow.com/questions/tagged/tdengine)

English | [简体中文](./README-CN.md)

<!-- omit in toc -->
## Table of Contents
- [1. Introduction](#1-introduction)
- [2. Documentation](#2-documentation)
- [3. Prerequisites](#3-prerequisites)
  - [3.1 Windows](#31-windows)
  - [3.2 Linux](#32-linux)
  - [3.3 macOS](#33-macos)
- [4. Building and Installing](#4-building-and-installing)
  - [4.1 Windows](#41-windows)
  - [4.2 Linux/macOS](#42-linuxmacos)
- [5. Testing](#5-testing)
  - [5.1 Test Execution](#51-test-execution)
    - [5.1.1 Windows](#511-windows)
    - [5.1.2 Linux/macOS](#512-linuxmacos)
  - [5.2 Test Case Addition](#52-test-case-addition)
  - [5.3 Performance Testing](#53-performance-testing)
- [6. Packaging](#6-packaging)
- [7. CI/CD](#7-cicd)
- [8. Submitting Issues](#8-submitting-issues)
  - [8.1 Required Information](#81-required-information)
  - [8.2 Additional Information (Optional but Helpful)](#82-additional-information-optional-but-helpful)
- [9. Submitting PRs](#9-submitting-prs)
- [10. References](#10-references)
- [11. Appendix](#11-appendix)
- [12. License](#12-license)

## 1. Introduction
`taos-connector-odbc` is the official ODBC connector for TDengine, enabling applications written in various programming languages such as C/C++, C#, Go, Rust, Python, Node.js, and more to interact with the TDengine database. By leveraging the standardized ODBC interface, `taos-connector-odbc` facilitates seamless data writing, querying and parameter binding across different platforms and environments.

The `taos-connector-odbc` supports multiple operating systems, including Windows, Linux, and macOS. 


## 2. Documentation
- To use the TDengine ODBC connector, please check [Reference Manual](https://docs.tdengine.com/tdengine-reference/client-libraries/odbc/), which includes version history, data types, example programs, API descriptions, and FAQs.
- This quick guide is mainly for developers who like to contribute/build/test the ODBC connector by themselves. To learn more about TDengine, you can visit the [Official Documentation](https://docs.tdengine.com).
- The TDengine ODBC connector adheres to the ODBC standard, ensuring compatibility and interoperability across various database systems. For more detailed information about ODBC standards and specifications, please refer to the [Microsoft Open Database Connectivity (ODBC)](https://learn.microsoft.com/en-us/sql/odbc/microsoft-open-database-connectivity-odbc) documentation. This resource provides comprehensive insights into ODBC interfaces, methods, and other relevant details that can help deepen your understanding of how the TDengine ODBC connector operates within the broader context of ODBC applications.


## 3. Prerequisites
First, ensure that TDengine has been deployed locally. For detailed deployment steps, please refer to [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/). Ensure that both taosd and taosAdapter services are up and running.

Afterwards, before installing and using the `taos-connector-odbc`, ensure that you have met the following prerequisites for your specific platform.

- cmake, 3.16 or above, please refer to [cmake](https://cmake.org/).
- a C/C++ compiler toolchain for your platform.
- flex, 2.6.4 or above. NOTE: win_flex_bison on windows platform to be installed.
- bison, 3.5.1 or above. NOTE: win_flex_bison on windows platform to be installed.
- odbc driver manager, such as unixodbc(2.3.6 or above) in linux. NOTE: odbc driver manager is pre-installed on windows platform.
- iconv, should've been already included in libc. NOTE: win_iconv will be automatically downloaded when building this project.
- TDengine client headers and libraries (for example `taos.h` plus `libtaos.so` on Linux, `libtaos.dylib` on macOS, or the TDengine client DLL/import library on Windows) available in your compiler and linker search paths.

If you want to enable valgrind memory checks (enable the `ENABLE_VALGRIND_TEST` option during cmake configuration) or test cases for programming languages other than C/C++, please refer to the following content.

| Tool or Language  | Version Requirement                       | Related Libraries and Versions                                                                          | Reference Links                                                                 |
|-------------------|-------------------------------------------|---------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| valgrind          | v3.20 or above                            |                                                                                                         | [valgrind](https://valgrind.org/)                                               |
| node.js           | v12.0 or above                            | node-odbc, 2.4.4 or above                                                                               | [node-odbc](https://www.npmjs.com/package/odbc/)                                |
| rust              | v1.63 or above                            | odbc, 0.17.0 or above<br>env_logger, 0.8.2 or above<br>json                                             | [rust-odbc](https://docs.rs/odbc/latest/odbc/)<br>[env_logger](https://docs.rs/env_logger/latest/env_logger/)<br>[json](https://docs.rs/json/latest/json/) |
| python            | v3.10 or above                            | pyodbc, 4.0.39 or above                                                                                 | [python-odbc](https://pypi.org/project/pyodbc/)                                 |
| go                | v1.17 or above                            | odbc                                                                                                    | [go-odbc](https://github.com/alexbrainman/odbc)                                 |
| erlang            | v12.2 or above                            | odbc                                                                                                    | [erlang-odbc](https://www.erlang.org/doc/apps/odbc/getting_started.html)        |
| haskell           | cabal v3.6 or above, ghc v9.2 or above    | hsql-odbc                                                                                               | [haskell-odbc](https://hackage.haskell.org/package/hsql-odbc)                   |
| common lisp       | sbcl v2.1.11 or above                     | plain-odbc                                                                                              | [common-lisp-odbc](https://plain-odbc.common-lisp.dev/)                         |
| R                 | v4.3 or above                             | odbc, DBI, assert                                                                                       | [R-odbc](https://cran.r-project.org/web/packages/odbc/index.html)               |



In the following content of this guide, we will use the following versions as examples: Windows 11 for the Windows platform, Ubuntu 20.04 for the Linux platform, and macOS Big Sur for the macOS platform.

### 3.1 Windows
- Install CMake 3.16 or above and a Visual Studio C/C++ toolchain before configuring the project.
- Install TDengine locally so the default `C:/TDengine/include` and `C:/TDengine/taos_odbc/.../lib` paths are available to the build.
- Install win_flex_bison:
  - Download from: [win_flex_bison](https://github.com/lexxmark/winflexbison/releases/).
  - Extract the files and add the directory to your system's PATH environment variable.
- Verify Installation:
  ```
  win_flex --version
  ```
- Install ODBC Driver Manager:
  Ensure that the Microsoft ODBC Driver Manager is installed on your system. It is typically pre-installed on Windows platform.

### 3.2 Linux
- Ubuntu/Debian example:
  ```
  sudo apt update
  sudo apt install -y build-essential cmake flex bison unixodbc unixodbc-dev odbcinst pkg-config
  ```
- RHEL/CentOS/AlmaLinux/Rocky example:
  ```
  sudo yum install -y gcc gcc-c++ make cmake flex bison unixODBC unixODBC-devel
  ```
- Install Required Dependencies, including the ODBC Driver Manager:
  ```
  sudo apt install flex bison unixodbc unixodbc-dev odbcinst && echo -=Done=-
  ```
- Verify that TDengine headers and `libtaos.so` are installed in a standard location such as `/usr/include`, `/usr/lib64`, `/usr/local/include`, or `/usr/local/lib`.

### 3.3 macOS
- Install Required Dependencies, including the ODBC Driver Manager:
  ```
  brew install cmake flex bison unixodbc libiconv && echo -=Done=-
  ```
- Ensure the TDengine client package is installed so that `taos.h` and `libtaos.dylib` are available to the compiler and linker.

## 4. Building and Installing
This section provides detailed instructions for building and installing the ODBC connector on different platforms.
Before proceeding, ensure you are in the root directory of this project.

By default, the examples below use a Debug configuration for local development. For a release-oriented build, replace `Debug` with `Release` (or configure a separate `build` directory with `-DCMAKE_BUILD_TYPE=Release`). If TDengine is installed in a non-standard prefix, make sure its headers and libraries are visible to CMake before configuring.

### 4.1 Windows
- Optionally, setup building environment: If you have installed Visual Studio Community 2022 on a 64-bit Windows platform, run the following command to set up the build environment:
  ```
  "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" x64
  ```
- Generate make files:
  ```
  cmake --no-warn-unused-cli -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE -B build -G "Visual Studio 17 2022" -A x64
  ```
  **Troubleshooting**: If compiler errors occur during the following steps, such as <path_to_winbase.h> warning C5105: macro expansion producing 'defined' has undefined behavior, retry with the following command:
    ```
    cmake --no-warn-unused-cli -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE -B build -G "Visual Studio 17 2022" -A x64 -DDISABLE_C5105:BOOL=ON
    ```
- Building the project:
  ```
  cmake --build build --config Debug -j 4
  ```
- Installing connector:
  This will install taos_odbc.dll into `C:\TDengine\taos_odbc\x64\` by default.
  ```
  cmake --install build --config Debug
  cmake --build build --config Debug --target install_templates
  ```
- Verify installation:
  Check if a new TAOS_ODBC_DSN registry has been set up in the Windows Registry:
  ```
  HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\TDengine
  HKEY_CURRENT_USER\Software\ODBC\Odbc.ini\TAOS_ODBC_DSN
  ```

For a Release build on Windows, use the same generator command and replace `Debug` with `Release` in the build and install steps.

### 4.2 Linux/macOS
- Generate make files:
  ```
  cmake -B debug -DCMAKE_BUILD_TYPE=Debug
  ```
- Build the project:
  ```
  cmake --build debug --parallel
  ```
- Install connector:
  ```
  sudo cmake --install debug
  cmake --build debug --target install_templates
  ```
- Verify installation:
  Check if the ODBC DSN configuration file (e.g., /etc/odbc.ini or ~/.odbc.ini) contains TAOS_ODBC_DSN entry.

For a Release build on Linux or macOS, the equivalent commands are:
```
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
sudo cmake --install build
cmake --build build --target install_templates
```

## 5. Testing
### 5.1 Test Execution
The ODBC Connector testing framework uses ctest for running test cases. The test cases are located in the tests directory of the project root.

#### 5.1.1 Windows
- Setup testing environment variables:
  Set the following environment variables to configure logging levels and destinations:
  ```
  set TAOS_ODBC_LOG_LEVEL=ERROR
  set TAOS_ODBC_LOGGER=stderr
  ```
  Available log levels are: VERBOSE, DEBUG, INFO, WARN, ERROR, FATAL. Lower levels provide more detailed logs.
  Available loggers are:
  - stderr: logs to standard error.
  - temp: logs to a file( `taos_odbc.log`) in the temporary directory.

- Run the tests:
  ```
  ctest --test-dir build --output-on-failure -C Debug
  ```

#### 5.1.2 Linux/macOS
- Setup testing environment variables:
  Set the following environment variables to configure logging levels and destinations:
  ```
  export TAOS_ODBC_LOG_LEVEL=ERROR
  export TAOS_ODBC_LOGGER=stderr
  ```
  Available log levels are: VERBOSE, DEBUG, INFO, WARN, ERROR, FATAL. Lower levels provide more detailed logs.

  Available loggers are:
  - stderr: Logs to standard error.
  - temp: Logs to a file( `taos_odbc.log`) in the temporary directory.
  - syslog: Logs to the system log (only available on Linux platform; not supported on macOS).
  
- Run the tests:
  ```
  pushd debug >/dev/null && ctest --output-on-failure && echo -=Done=-; popd >/dev/null
  ```

### 5.2 Test Case Addition
All tests are located in the `./tests/` directory of the project. This directory contains subdirectories for various languages and types of tests. The basic functionality verification test cases are written in C and are located in the `c` and `taos` folders.
You can add new test files or add test cases in existing test files that comply with the testing framework standards.
For C/C++ tests, ensure they follow the CTest framework conventions.
Place new test files in the appropriate directories within the tests directory.

### 5.3 Performance Testing
Performance testing is in progress.


## 6. Packaging
Use a Release build on Linux when preparing the redistributable tarball for `taos-connector-odbc`.

### 6.1 What gets packaged
The Linux tarball contains:
- `lib/libtaos_odbc.so*` from the staged install prefix.
- `templates/odbc.ini.in` and `templates/odbcinst.ini.in`.
- Outer installer helpers: `install_odbc.sh` and `uninstall_odbc.sh`.

The package uses a two-layer tar layout:
- Outer tarball: installer scripts plus `package.tar.gz`.
- Inner `package.tar.gz`: `lib/` and `templates/`.

### 6.2 Prerequisites
Before packaging or installing the tarball:
- Build the connector on Linux.
- Install `taos-community` / TDengine client libraries first so `libtaos.so` is available on the target machine.
- Install unixODBC (`odbcinst`) on the target machine if you want to register the driver and DSN immediately after installation.

### 6.3 Build the connector
Use the Linux workflow from Section 4.2:
```
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

### 6.4 Stage files into an install prefix
Install the connector into a staging directory, then copy the ODBC templates into the same prefix:
```
cmake --install build --prefix "$PWD/package-root"
mkdir -p "$PWD/package-root/templates"
cp templates/odbc.ini.in templates/odbcinst.ini.in "$PWD/package-root/templates/"
```

The staged prefix should now include `lib/libtaos_odbc.so*` and `templates/`.

### 6.5 Create the tarball
Run the packaging helper from `source/taos-connector-odbc/packaging/`:
```
bash ./packaging/pack_odbc_tar.sh -c "$PWD/package-root" -n 3.3.6.0
```

Optional arguments:
- `-m <compat_version>`: compatible version label (default: `3.0.0.0`).
- `-V stable|beta`: package channel (default: `stable`).

Expected output paths:
- Tarball: `release/taos-connector-odbc-<version>-Linux-<arch>.tar.gz`
- Expanded package dir during packaging: `release/taos-connector-odbc-<version>/`

### 6.6 Install the tarball
On the target Linux machine:
```
tar xzf taos-connector-odbc-<version>-Linux-<arch>.tar.gz
cd taos-connector-odbc-<version>
sudo bash ./install_odbc.sh
```

For unattended installation, use silent mode:
```
sudo bash ./install_odbc.sh -s
```

The installer copies `libtaos_odbc.so*` into the system library directory, installs the ODBC templates into `/etc/taos/odbc/`, refreshes the linker cache with `ldconfig`, and warns if `libtaos.so` or `odbcinst` is missing.

### 6.7 After installation
Review and customize the generated template files before using the driver:
- `/etc/taos/odbc/odbcinst.ini.in`
- `/etc/taos/odbc/odbc.ini.in`

Copy the DSN template into `/etc/odbc.ini` or `~/.odbc.ini`, then update server address, credentials, and any other TDengine connection settings required for your environment.

## 7. CI/CD
- [Build Workflow] -TODO
- [Code Coverage] -TODO


## 8. Submitting Issues
We welcome the submission of [GitHub Issue](https://github.com/taosdata/taos-connector-odbc/issues/new?template=Blank+issue). When submitting an issue, please provide the following information to help us diagnose and resolve the problem more efficiently:

### 8.1 Required Information
- Problem Description:
  Provide a clear and detailed description of the issue you are encountering.
  Indicate whether the issue occurs consistently or intermittently.
  If possible, include a detailed call stack or error message that can help in diagnosing the problem.
- ODBC Connector Version:
  Specify the version of the ODBC Connector you are using. 
- ODBC Connection Parameters:
  Provide your ODBC connection string or DSN (Data Source Name) configuration details. Please do not include sensitive information such as usernames and passwords.
  Example:
  ```
  DSN=TAOS_ODBC_DSN;UID=<your_username>;PWD=<your_password>
  ```
  Replace <your_username> and <your_password> with placeholders or omit them for security reasons.
- TDengine Server Version:
  Specify the version of the TDengine server you are connecting to. 

### 8.2 Additional Information (Optional but Helpful)
- Operating System: Specify the operating system and its version.
- Steps to Reproduce: Provide step-by-step instructions on how to reproduce the issue. This helps us replicate and verify the problem.
- Environment Configuration: Include any relevant environment configurations, such as specific settings in odbc.ini, odbcinst.ini, or other configuration files.
- Logs: Attach any relevant logs that might help in diagnosing the issue. Logs can be found in the location specified by your logging configuration (e.g., stderr, temp, or syslog).


## 9. Submitting PRs
We welcome developers to contribute to this project. When submitting PRs, please follow these steps:

1. Fork this project, refer to ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo)).
2. Create a new branch from the main branch with a meaningful branch name (`git checkout -b my_branch`). Do not modify the main branch directly.
3. Modify the code, ensure all unit tests pass, and add new unit tests to verify the changes.
4. Push the changes to the remote branch (`git push origin my_branch`).
5. Create a Pull Request on GitHub ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)).
6. After submitting the PR, you can find your PR through the [Pull Request](https://github.com/taosdata/taos-connector-odbc/pulls). Click on the corresponding link to see if the CI for your PR has passed. If it has passed, it will display "All checks have passed". Regardless of whether the CI passes or not, you can click "Show all checks" -> "Details" to view the detailed test case logs.
7. After submitting the PR, if CI passes, you can find your PR on the [codecov](https://app.codecov.io/gh/taosdata/taos-connector-odbc/pulls) page to check the test coverage.


## 10. References
- [TDengine Official Website](https://www.tdengine.com/) 
- [TDengine GitHub](https://github.com/taosdata/TDengine) 
- [Microsoft Introduction to ODBC](https://learn.microsoft.com/en-us/sql/odbc/reference/introduction-to-odbc)
- [Microsoft ODBC API Reference](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/odbc-api-reference)


## 11. Appendix
Layout of project source code, directories only:
```
<root>
├── benchmark
├── cmake
├── common
├── inc
├── samples
│   └── c
├── sh
├── src
│   ├── core
│   ├── inc
│   ├── os_port
│   ├── parser
│   ├── tests
│   └── utils
├── templates
├── tests
│   ├── c
│   ├── cpp
│   ├── cs
│   ├── erl
│   ├── go
│   ├── hs
│   │   └── app
│   ├── lisp
│   ├── node
│   ├── python
│   ├── R
│   ├── rust
│   │   └── main
│   │       └── src
│   ├── sh
│   └── taos
└── valgrind
```


## 12. License
[MIT License](./LICENSE)

<!-- omit in toc -->

## originally initiated by freemine@yeah.net

