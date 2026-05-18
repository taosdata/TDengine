# tsdb-builder

TDengine TSDB 多语言编译环境 Docker 镜像构建工具，其生成的镜像用于编译、构建 TDengine TSDB 多种安装包，包括：
- TSDB OSS
- TSDB Enterprise/Lite

也可以用于编译、构建 TDengine TSDB 各个组件：
- TSDB taosAdapter
- TSDB taosX
- TSDB Connectors

基于 **manylinux2014**（glibc 2.17 + GCC 10.x），同时支持 `linux/amd64` 和 `linux/arm64` 两种架构，内置 TDengine 各语言连接器（Go / Java / Python / Rust / .NET / C#）所需的全套工具链。

**仓库地址**：<https://git.tdengine.net/taosdata/rd-dept/platform/platform>

---

## 目录结构

```text
tsdb-builder/
├── Dockerfile              # 多阶段构建主文件
├── build.sh                # 镜像构建脚本
├── build-arm64-tsdb-oss.sh # 构建 ARM64 TSDB OSS 安装包脚本
├── build-amd64-tsdb-oss.sh # 构建 AMD64 TSDB OSS 安装包脚本
├── verify-image.sh         # 镜像组件验证脚本
├── .build-args             # 构建参数默认值（版本号、镜像源等）
└── installers/             # 离线安装包目录（见下方说明）
```

> **installers/ 不存入本仓库。**
> 离线安装包体积较大，统一存放于 NAS：
>
> ```text
> /public/tsdb-builder/
> ```
>
> 构建镜像前，请将所需文件手动下载/拷贝到本地工作目录的 `installers/` 文件夹。
> 所需文件清单见下方[《离线安装包清单》](#离线安装包清单)。

---

## 内置工具链

| 工具 | 版本 | 说明 |
| ---- | ---- | ---- |
| glibc | 2.17 | manylinux2014 基础，广泛兼容旧发行版 |
| GCC / G++ | 10.x | devtoolset-10，需 ≥ 9.3.1 |
| Go | 1.23.4 | GOPROXY=goproxy.cn |
| JDK | 8u441 (ARM64) / 8u144 (AMD64) | OpenJDK 8 |
| Maven | 3.8.4 | |
| CMake | 3.21.5 | |
| Rust / Cargo | 1.90.0 | 镜像源 rsproxy.cn |
| Python | 3.12 | manylinux2014 预装，PyPI 镜像 mirrors.aliyun.com |
| pip / uv | latest | uv：高性能 Python 包管理器 |
| taospy | 2.8.8 | TDengine Python 连接器 |
| taos-ws-py | 0.6.5 | TDengine WebSocket Python 连接器 |
| .NET SDK | 6.0.100 | |
| mold | 2.40.3 | 高速链接器，替代 GNU ld |
| protoc | 33.0 | Protocol Buffers 编译器 |
| tini | v0.19.0 | 容器 init 进程 |

---

## 离线安装包清单

构建前请确保 `installers/` 中包含以下文件（均可从 NAS `/public/tsdb-builder/` 获取）：

| 文件名 | 适用架构 |
| ------ | -------- |
| `go1.23.4.linux-amd64.tar.gz` | amd64 |
| `go1.23.4.linux-arm64.tar.gz` | arm64 |
| `jdk-8u144-linux-x64.tar.gz` | amd64 |
| `jdk-8u441-linux-aarch64.tar.gz` | arm64 |
| `apache-maven-3.8.4-bin.tar.gz` | 通用 |
| `cmake-3.21.5-linux-x86_64.tar.gz` | amd64 |
| `cmake-3.21.5-linux-aarch64.tar.gz` | arm64 |
| `.cargo/config.toml` | 通用（Rust 镜像源配置） |

> 安装包通过 `--mount=type=bind` 在构建时挂载，**不会写入最终镜像层**。

---

## 快速开始

### 前置要求

- Docker ≥ 20.10（含 `docker buildx`，Docker Desktop 自带）
- 已从 NAS 拷贝离线安装包到 `installers/`（见上方清单）
- **`build-push` 前提**：已执行 `docker login` 登录目标 registry，并在执行时设置 `REGISTRY_IMAGE` 环境变量（如 `myregistry.io/tsdb-builder:latest`）

### 构建镜像

```bash
# 构建 ARM64（无参数时的默认行为）
./build.sh build-arm64

# 构建 AMD64
./build.sh build-amd64

# 同时构建两种架构（顺序执行，本地 load）
./build.sh build-all

# 并行构建两种架构并推送 multi-arch manifest 到 registry
REGISTRY_IMAGE=myregistry.io/tsdb-builder:latest ./build.sh build-push

# 覆盖某个工具版本
./build.sh build-custom GO_VERSION=1.24.0

# 指定平台和标签
PLATFORM=linux/amd64 TAG=tsdb-builder:amd64-dev ./build.sh build-custom
```

无参数时默认执行 `build-arm64`：

```bash
./build.sh
```

### 验证镜像

```bash
# 验证默认镜像（tsdb-builder:latest）
./verify-image.sh

# 验证指定架构标签
./verify-image.sh arm64
./verify-image.sh amd64

# 验证任意完整镜像名
./verify-image.sh myregistry/tsdb-builder:v1.0
```

验证脚本自动检测宿主机 / 容器环境，在宿主机上执行时通过 `docker run` 进入容器完成验证，输出包括：

- glibc 版本确认（2.17）
- GCC / G++ 版本确认（≥ 9.3.1）
- C / C++ 实际编译链接测试
- 各工具链版本打印（Go、Java、Maven、CMake、Rust、.NET）
- mold 链接器功能测试
- protoc / tini 可用性检查
- Python SSL 模块健康检查

### 利用 tsdb-builder 构建 TDengine OSS

以 Apple Silicon 平台为例，说明构建 arm64 版本的TDengine OSS 的步骤：

- 前置条件：Docker ≥ 20.10
- tsdb-builder:arm64 镜像已完成构建 
- 将 TDengine 仓库克隆至 ~/TDengine
- 构建 TDengine OSS：运行 ./build-arm64-tsdb-oss.sh

---

## 构建参数说明

所有参数均在 `.build-args` 中集中管理，构建时由 `build.sh` 自动读取并传入 Docker。

| 参数 | 默认值 | 说明 |
| ---- | ------ | ---- |
| `GO_VERSION` | `1.23.4` | Go 版本 |
| `MAVEN_VERSION` | `3.8.4` | Maven 版本 |
| `CMAKE_VERSION` | `3.21.5` | CMake 版本 |
| `JDK_VERSION_AMD64` | `8u144` | AMD64 JDK 版本（自动写入 /etc/environment） |
| `JDK_VERSION_ARM64` | `8u441` | ARM64 JDK 版本（自动写入 /etc/environment） |
| `RUST_VERSION` | `1.90.0` | Rust 工具链版本 |
| `PYTHON_VERSION` | `3.12` | Python 版本（使用 manylinux2014 预装） |
| `DOTNET_VERSION` | `6.0.100` | .NET SDK 版本 |
| `MOLD_VERSION` | `2.40.3` | mold 链接器版本 |
| `PROTOC_VERSION` | `33.0` | protoc 版本 |
| `TINI_VERSION` | `v0.19.0` | tini 版本 |
| `TAOSPY_VERSION` | `2.8.8` | taospy Python 包版本 |
| `TAOS_WS_PY_VERSION` | `0.6.5` | taos-ws-py Python 包版本 |
| `PYPI_MIRROR` | `http://mirrors.aliyun.com/pypi/simple/` | PyPI 镜像源 |
| `PYPI_TRUSTED_HOST` | `mirrors.aliyun.com` | PyPI 可信主机 |
| `GO_PROXY` | `https://goproxy.cn` | Go 模块代理 |
| `TIMEZONE` | `Asia/Shanghai` | 容器时区 |

查看当前所有参数值：

```bash
./build.sh list-args
```

临时覆盖某个参数（不修改 `.build-args`）：

```bash
./build.sh build-custom RUST_VERSION=1.91.0 MOLD_VERSION=2.41.0
```

---

## Dockerfile 设计说明

### 多阶段构建

```text
Stage 1 (builder / Alpine)
  └─ 根据 TARGETPLATFORM 生成架构变量映射 → /etc/environment
       amd64: MOLD_ARCH=x86_64, TINI_ARCH=amd64, PROTOC_ARCH=x86_64 ...
       arm64: MOLD_ARCH=aarch64, TINI_ARCH=arm64, PROTOC_ARCH=aarch_64 ...

Stage 2 (manylinux2014_x86_64 或 manylinux2014_aarch64，由 TARGETARCH 选择)
  ├─ Layer 1 : yum 基础包（git / wget / openssl-devel 等）
  ├─ Layer 4 : Go（离线 tar.gz）
  ├─ Layer 5 : JDK + Maven（离线 tar.gz）
  ├─ Layer 6 : CMake（离线 tar.gz）
  ├─ Layer 7 : Rust（在线 rustup，rsproxy.cn 镜像）
  ├─ Layer 8 : Python 3.12（manylinux2014 预装，symlink 激活）
  │             + pip 镜像配置 + uv + taospy + taos-ws-py
  ├─ Layer 9 : .NET SDK（官方安装脚本）
  ├─ Layer 10: mold + protoc + tini（在线下载，GitHub Releases）
  └─ Layer 11: 环境变量、SSH、时区、git safe.directory 等最终配置
```

### 关键设计决策

| 决策 | 原因 |
| ---- | ---- |
| 离线安装包优先（Go / JDK / Maven / CMake） | 避免构建时依赖外部网络稳定性；`--mount=type=bind` 不写入镜像层 |
| Python 使用 manylinux2014 预装版 | 省去约 13 分钟的源码编译，且内置 OpenSSL 3.x |
| 架构变量在 Stage 1 集中映射 | AMD64 / ARM64 包名差异（`x86_64` vs `aarch64`）统一处理，主构建逻辑不分叉 |
| tini 作为容器 init | 确保子进程正确回收，避免僵尸进程 |
| mold 替代 GNU ld | 大型 C/C++ 项目链接速度显著提升 |

---

## 镜像标签约定

| 标签 | 含义 |
| ---- | ---- |
| `tsdb-builder:latest` | ARM64 最新构建 |
| `tsdb-builder:arm64` | ARM64 架构专用 |
| `tsdb-builder:amd64` | AMD64 架构专用 |
| `tsdb-builder:custom` | 自定义参数构建（本地测试用） |

---

## 注意事项

- **`installers/` 不进入镜像层**：使用 `--mount=type=bind` 仅在构建时挂载，构建完成后可删除本地副本
- **AMD64 / ARM64 JDK 版本不同**：`jdk-8u144-linux-x64.tar.gz`（AMD64）与 `jdk-8u441-linux-aarch64.tar.gz`（ARM64）不可互换，Dockerfile Stage 1 已通过 `JDK_VERSION_AMD64` / `JDK_VERSION_ARM64` 自动处理此差异
- **.NET SDK 运行环境**：通过启用 devtoolset-10 并提供正确的 `LD_LIBRARY_PATH`，.NET SDK 现在可以正常运行
- **Rust 工具链需要网络**：通过 `rsproxy.cn` 加速，首次构建需要访问外网；后续层可利用 Docker 构建缓存
- **需要 `docker buildx`**：Docker Desktop 自带，Linux 裸机需确认 Docker 版本 ≥ 20.10
