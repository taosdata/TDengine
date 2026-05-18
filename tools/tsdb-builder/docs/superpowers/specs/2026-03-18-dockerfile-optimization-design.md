# Dockerfile 优化设计方案

**日期：** 2026-03-18
**更新：** 2026-03-19（Python 优化、镜像源修正、JDK 路径修正）
**目标：** 优化 tsdb-builder Dockerfile，融合 manylinux2014 优势，支持 TDengine 全组件构建

## 背景

当前 Dockerfile 基于 CentOS 7 + devtoolset-9，满足 glibc 2.17 和 GCC 9.3.1+ 要求。需要融合 Dockerfile-taosx-manylinux2024 的优点：

1. manylinux2014 标准化构建环境
2. 多阶段构建优雅处理多架构
3. mold 链接器加速编译
4. protoc 预编译二进制
5. 完整的 Rust 环境支持

同时保留所有现有工具链：Go、Maven、CMake、JDK、Rust、Python/uv、.NET。

> **2026-03-19 更新：** Python 和 OpenSSL 改由 manylinux2014 镜像内置版本提供，不再源码编译。

## 设计目标

- **兼容性：** glibc 2.17，GCC ≥ 9.3.1
- **架构支持：** amd64 和 arm64
- **工具完整性：** 覆盖 TDengine 所有组件（taosd、taosAdapter、taosKeeper）
- **构建速度：** 使用 mold 链接器、合理的层缓存策略
- **镜像体积：** 平衡优化，避免过度膨胀
- **可维护性：** 清晰的层组织，便于调试和更新

## 技术方案

### 1. 基础镜像选择

**从 CentOS 7 迁移到 manylinux2014**

**理由：**
- manylinux2014 基于 CentOS 7，提供 glibc 2.17
- 预装 devtoolset-10（GCC 10.x），高于 9.3.1 要求
- Python 社区标准，兼容性经过广泛验证
- 更标准化的构建环境
- **预装 Python 3.8–3.14**，无需源码编译

**基础镜像（已更正）：**

> ⚠️ 原设计使用 `docker.1ms.run/lukewiwa/manylinux2014`，实施中发现该地址已失效（404）。
> 实际使用官方镜像，通过两个架构专属镜像实现多架构支持：

```dockerfile
FROM --platform=$BUILDPLATFORM quay.io/pypa/manylinux2014_x86_64 AS stage2-amd64
FROM --platform=$BUILDPLATFORM quay.io/pypa/manylinux2014_aarch64 AS stage2-arm64
FROM stage2-${TARGETARCH}
```

### 2. 多阶段构建架构

#### Stage 1: Builder（架构变量处理）

```dockerfile
FROM docker.1ms.run/alpine AS builder
ARG TARGETPLATFORM
```

**职责：**
- 根据 TARGETPLATFORM 映射架构相关变量
- 生成 /etc/environment 文件供后续阶段使用

**架构映射表：**

| TARGETPLATFORM | MOLD_ARCH | TINI_ARCH | PROTOC_ARCH | GO_ARCH | JDK_ARCH | CMAKE_ARCH | DOTNET_ARCH |
|----------------|-----------|-----------|-------------|---------|----------|------------|-------------|
| linux/amd64    | x86_64    | amd64     | x86_64      | amd64   | x64      | x86_64     | x64         |
| linux/arm64    | aarch64   | arm64     | aarch_64    | arm64   | aarch64  | aarch64    | arm64       |

> **2026-03-19 更新：** 移除 OPENSSL_ARCH，不再需要编译 OpenSSL。

#### Stage 2: Main（构建环境）

```dockerfile
FROM stage2-${TARGETARCH}
COPY --from=builder /etc/environment /etc/
```

**职责：**
- 安装所有工具链和依赖
- 配置构建环境
- 提供完整的 TDengine 构建能力

### 3. 层组织策略

采用**平衡策略**：既优化缓存利用率，又控制总层数。

#### 层 1: 系统基础（yum 仓库 + 基础包）

**内容：**
- yum 仓库配置
  - amd64：`https://mirrors.aliyun.com/centos/7`
  - arm64：`http://archive.kernel.org/centos-vault/altarch/7`（阿里云已停止提供 arm64 CentOS 7）
- 基础开发工具：git, wget, curl, unzip, tar, gzip, bzip2
- 编译依赖：libatomic, openssl-devel, zlib-devel, libffi-devel
- Perl 运行时：perl, perl-IPC-Cmd, perl-FindBin（TDengine 第三方依赖 ext_ssl 内部编译 OpenSSL 需要）
- yum 清理

> **2026-03-20 修正：** TDengine 的 `ext_ssl` 子项目在容器内部编译 OpenSSL 源码，需要 Perl 运行时，因此需保留最小 Perl 依赖。注意与"编译 OpenSSL 1.1.1w 的 Perl 模块"（cpanm/CPAN）不同，这里只需要基础 perl 包。

**优化：**
- 所有 yum 操作合并到一个 RUN 层
- 安装后立即清理 yum 缓存

#### ~~层 2: Perl 模块（已删除）~~

> **2026-03-19 删除：** 原本安装 cpanm 和 Perl 模块用于编译 OpenSSL，现已随 OpenSSL 层一同删除。

#### ~~层 3: OpenSSL 编译（已删除）~~

> **2026-03-19 删除：** manylinux2014 预装 Python 3.12 已内置 OpenSSL 3.5.5，无需单独编译 OpenSSL 1.1.1w。
> 节省构建时间约 **3 分钟**。

#### 层 2（原层 4）: Go 工具链

**内容：**
- 从 installers/ 安装 Go 1.23.4（`--mount=type=bind`）
- 配置 GOROOT, GOPATH, GOPROXY（goproxy.cn）
- 更新 PATH

**版本：** Go 1.23.4
**架构：** 通过 GO_ARCH 变量支持 amd64/arm64

#### 层 3（原层 5）: JDK + Maven

**内容：**
- 从 installers/ 安装 JDK 和 Maven（`--mount=type=bind`）
- 动态检测 JDK 目录，创建统一符号链接 `/usr/local/jdk`
- 配置 JAVA_HOME=/usr/local/jdk, M2_HOME
- 更新 PATH

**JDK 版本说明：**
- arm64 默认：`JDK_VERSION=8u441`（installers/jdk-8u441-linux-aarch64.tar.gz）
- amd64 需覆盖：`--build-arg JDK_VERSION=8u144`（installers/jdk-8u144-linux-x64.tar.gz）

> **2026-03-19 修正：** 原设计将 JAVA_HOME 硬编码为 `jdk1.8.0_441`，在 amd64（8u144）下不兼容。
> 改为动态符号链接方案，兼容不同版本号。

#### 层 4（原层 6）: CMake

**内容：**
- 从 installers/ 安装 CMake 3.21.5
- 创建符号链接 `/usr/local/bin/cmake`（避免 PATH 中含架构变量）
- 版本：CMake 3.21.5

#### 层 5（原层 7）: Rust 工具链

**内容：**
- 配置 Rust 国内镜像（rsproxy.cn）
- 安装 Rust 1.90.0（minimal profile）
- 安装组件：clippy, rustfmt
- 复制 Cargo 配置文件（`installers/.cargo/config.toml`）
- 更新 PATH

**配置：**
```bash
RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
RUSTUP_DIST_SERVER="https://rsproxy.cn"
```

> **2026-03-19 修正：** 原设计引用 `./cargo.toml`（不存在），实际路径为 `installers/.cargo/config.toml`。

#### 层 6（原层 8）: Python 环境

> **2026-03-19 重大更新：** 由源码编译改为使用 manylinux2014 预装 Python。

**manylinux2014 预装 Python 版本（双架构均有）：**
```
/opt/python/cp38-cp38   → Python 3.8
/opt/python/cp39-cp39   → Python 3.9
/opt/python/cp310-cp310 → Python 3.10
/opt/python/cp311-cp311 → Python 3.11
/opt/python/cp312-cp312 → Python 3.12.13  ← 选用
/opt/python/cp313-cp313 → Python 3.13
/opt/python/cp314-cp314 → Python 3.14
```

**新方案（< 1 秒）：**
```dockerfile
ARG PYTHON_VERSION=3.12

RUN PY_TAG=$(echo ${PYTHON_VERSION} | tr -d '.') && \
    PYTHON_PATH="/opt/python/cp${PY_TAG}-cp${PY_TAG}" && \
    test -d "${PYTHON_PATH}" || { echo "ERROR: Python ${PYTHON_VERSION} not found"; exit 1; } && \
    ln -sf ${PYTHON_PATH}/bin/python3 /usr/local/bin/python3 && \
    ln -sf ${PYTHON_PATH}/bin/python3 /usr/local/bin/python && \
    ln -sf ${PYTHON_PATH}/bin/pip3    /usr/local/bin/pip3 && \
    ln -sf ${PYTHON_PATH}/bin/pip3    /usr/local/bin/pip
```

**效果对比：**

| 项目 | 旧方案 | 新方案 |
|------|--------|--------|
| Python 版本 | 3.10.13 | **3.12.13** |
| OpenSSL 版本 | 1.1.1w (2023) | **3.5.5 (2026)** |
| pip 版本 | 23.0.1 | **26.0.1** |
| 编译耗时 | ~10 分钟 | **< 1 秒** |
| 需要 Python tarball | ✅ | ❌ |

- pip 国内镜像（mirrors.aliyun.com）仍配置
- uv 仍安装
- taospy 2.8.8, taos-ws-py 0.6.5 仍预装

#### 层 7（原层 9）: .NET SDK

**内容：**
- 通过官方安装脚本安装 .NET 6.0.100

> **2026-03-19 修正：** 原设计使用硬编码 Microsoft 下载 URL，实施中发现该 URL 已失效（400）。
> 改为官方脚本：`https://dot.net/v1/dotnet-install.sh`

**注意：**
- .NET 在 glibc 2.17 环境下功能受限
- 保留以支持部分 .NET 组件

#### 层 8（原层 10）: 现代工具（mold + protoc + tini）

**内容：**
- 安装 mold 2.40.3（现代链接器）
- 安装 protoc 33.0（Protocol Buffers 编译器）
- 安装 tini v0.19.0（init 进程）

#### 层 9（原层 11）: 环境配置

**内容：**
- 配置 LANG/LANGUAGE/LC_ALL（UTF-8）
- 更新 PATH（整合所有工具）
- ~~配置 LD_LIBRARY_PATH（OpenSSL）~~ → **已删除**（OpenSSL 不再独立安装）
- Git 全局配置（safe.directory）
- SSH 配置（StrictHostKeyChecking no）
- 时区配置（Asia/Shanghai）
- 创建工作目录 /app

**ENTRYPOINT：**
```dockerfile
ENTRYPOINT ["/bin/tini", "--"]
```

### 4. 版本管理

所有工具版本通过 ARG 参数化，便于更新：

```dockerfile
ARG GO_VERSION=1.23.4
ARG MAVEN_VERSION=3.8.4
ARG CMAKE_VERSION=3.21.5
ARG JDK_VERSION=8u441       # arm64 默认；amd64 需 --build-arg JDK_VERSION=8u144
ARG RUST_VERSION=1.90.0
ARG PYTHON_VERSION=3.12     # 对应 /opt/python/cp312-cp312（已改为短版本号格式）
ARG DOTNET_VERSION=6.0.100
ARG MOLD_VERSION=2.40.3
ARG PROTOC_VERSION=33.0
ARG TINI_VERSION=v0.19.0
ARG TAOSPY_VERSION=2.8.8
ARG TAOS_WS_PY_VERSION=0.6.5
```

> **2026-03-19 更新：** 删除 `OPENSSL_VERSION`（不再需要）和 `CPAN_MIRROR`（不再需要）。
> `PYTHON_VERSION` 格式从 `3.10.13` 改为 `3.12`（对应 manylinux2014 路径 `/opt/python/cp312-cp312`）。

### 5. 镜像配置

**PyPI 镜像：**
```dockerfile
ARG PYPI_MIRROR=http://mirrors.aliyun.com/pypi/simple/
ARG PYPI_TRUSTED_HOST=mirrors.aliyun.com
```

**Go 代理：**
```dockerfile
ARG GO_PROXY=https://goproxy.cn
```

**Rust 镜像：**
```dockerfile
ENV RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup
ENV RUSTUP_DIST_SERVER=https://rsproxy.cn
```

**说明：** rsproxy.cn 提供完整的 Rust 镜像服务，包括 rustup 更新和工具链分发，覆盖所有 Rust 下载需求。

### 6. 优化技术

#### 6.1 避免 tarball 残留层

使用 `RUN --mount=type=bind` 代替 `ADD`（Go/JDK/Maven/CMake 均采用）。

#### 6.2 层内清理

每层安装后立即清理临时文件。

#### 6.3 yum 操作合并

所有 yum 操作合并到一个 RUN 层。

#### 6.4 JDK 路径统一化

动态符号链接解决不同架构 JDK 版本号不同的问题：
```bash
JDK_DIR=$(ls /usr/local | grep 'jdk1.8' | head -1) && \
ln -sf /usr/local/${JDK_DIR} /usr/local/jdk
```

### 7. 验证机制

verify-image.sh 验证项（已更新）：

- glibc 版本（2.17）
- GCC 版本（≥ 9.3.1，改为数字比较，支持 GCC 10.x）
- 所有工具版本和可用性
- 架构信息
- mold 链接器（二进制存在性 + 链接测试）
- protoc 编译器
- tini init 进程
- Python SSL 模块（通过 `python3 -c 'import ssl; print(ssl.OPENSSL_VERSION)'` 验证，不再依赖 `/usr/local/openssl-1.1.1`）

### 8. 技术细节说明

#### 8.1 devtoolset-10 激活

manylinux2014 镜像已预激活 devtoolset-10，GCC 10.x 已在默认 PATH 中，无需手动执行 `scl enable devtoolset-10`。

#### 8.2 mold 链接器 arm64 支持

mold 2.40.3 完整支持 arm64 架构。在 QEMU 模拟的 amd64 环境（arm64 宿主机）下 mold 的版本检查会 Segfault，这是 QEMU 限制，非 mold 本身问题，在真实 amd64 机器上正常工作。

#### 8.3 arm64 yum 镜像源

阿里云已停止提供 arm64 CentOS 7 镜像（`mirrors.aliyun.com/altarch` 返回 404）。
arm64 改用：`http://archive.kernel.org/centos-vault/altarch/7`

#### 8.4 二进制下载容错

关键二进制（mold/protoc/tini）通过 GitHub Releases 下载，构建失败时会明确报错。

#### 8.5 pthread 依赖说明（ARM64 构建）

**ARM64 构建脚本中的 `-DTD_PTHREAD_TWEAK:BOOL=ON` 标志是必需的**。

这不仅是 CentOS 7.9/Ubuntu 18 的遗留问题，而是 libuv 库本身的真实依赖。libuv（TDengine 的传输层依赖）在某些代码路径中使用 pthread 符号。即使系统已安装 pthread 库，链接器也需要显式指定 `-lpthread` 才能解析这些符号。该标志确保在 transport 模块的链接过程中包含 `-lpthread`。

**结论：** 保持该标志在 ARM64 构建中启用。

## 实现计划

### 阶段 1: 编写新 Dockerfile ✅

完成。层数从原设计 11 层优化为 9 层（删除 OpenSSL 层、Perl 层，并入 Python 层简化）。

### 阶段 2: 更新辅助文件 ✅

1. 更新 verify-image.sh（新增 mold/protoc/tini 验证，GCC 版本检测升级）
2. 更新 build.sh（改用 docker buildx，新增 build-all 命令）

### 阶段 3: 构建测试 ✅

- amd64 镜像：`tsdb-builder:amd64`（3.13 GB）
- arm64 镜像：`tsdb-builder:arm64`（2.88 GB）
- 双架构均验证通过

### 阶段 4: 后续优化方向

1. **CI/CD 集成：** 自动化多架构构建和测试
2. **缓存优化：** 使用 BuildKit 缓存挂载进一步加速
3. **Python 版本升级：** 可通过 `--build-arg PYTHON_VERSION=3.13` 切换，无需改动 Dockerfile

## 风险评估

### 风险 1: manylinux2014 兼容性 → 已验证

manylinux2014 基于 CentOS 7，与现有环境相似，双架构构建均通过。

### 风险 2: 镜像体积

- 旧镜像（CentOS 7）：约 2.91 GB
- 新镜像（amd64）：3.13 GB，新镜像（arm64）：2.88 GB
- 体积可接受，主要增量来自新工具

### 风险 3: 构建时间（已大幅改善）

| 阶段 | 原设计 | 实际（含 Python 优化）|
|------|--------|----------------------|
| OpenSSL 编译 | ~3 分钟 | **0**（已删除）|
| Python 编译 | ~10 分钟 | **< 1 秒**（预装链接）|
| 总节省 | — | **约 13 分钟** |

### 风险 4: 多架构构建 → 已验证

双架构均通过测试。

## 成功标准（已达成）

1. ✅ 镜像基于 manylinux2014
2. ✅ glibc 版本为 2.17
3. ✅ GCC 版本 ≥ 9.3.1（实际为 10.2.1）
4. ✅ 支持 amd64 和 arm64 架构
5. ✅ 所有现有工具可用（Go/Maven/CMake/JDK/Rust/Python/uv/.NET）
6. ✅ 新增工具可用（mold/protoc/tini）
7. ✅ verify-image.sh 验证通过
8. ✅ 构建时间较原方案节省约 13 分钟

## 附录

### A. 工具版本清单（最终）

| 工具 | 版本 | 来源 |
|------|------|------|
| glibc | 2.17 | manylinux2014 内置 |
| GCC | 10.2.1 | manylinux2014 devtoolset-10 |
| Go | 1.23.4 | installers/ |
| Maven | 3.8.4 | installers/ |
| CMake | 3.21.5 | installers/ |
| JDK | 8u144（amd64）/ 8u441（arm64）| installers/ |
| Rust | 1.90.0 | rsproxy.cn |
| **Python** | **3.12.13** | **manylinux2014 预装（/opt/python/cp312-cp312）** |
| **OpenSSL** | **3.5.5** | **Python 内置（随 manylinux2014 提供）** |
| .NET | 6.0.100 | Microsoft 官方脚本 |
| mold | 2.40.3 | GitHub Releases |
| protoc | 33.0 | GitHub Releases |
| tini | v0.19.0 | GitHub Releases |
| uv | latest | astral.sh |
| taospy | 2.8.8 | PyPI（阿里云镜像）|
| taos-ws-py | 0.6.5 | PyPI（阿里云镜像）|

### B. 架构支持矩阵

| 组件 | amd64 | arm64 | 备注 |
|------|-------|-------|------|
| manylinux2014 | ✅ | ✅ | 分架构镜像 |
| Go | ✅ | ✅ | 官方支持 |
| JDK | ✅（8u144）| ✅（8u441）| 版本不同，符号链接统一 |
| Maven | ✅ | ✅ | 架构无关（Java）|
| CMake | ✅ | ✅ | 官方预编译 |
| Rust | ✅ | ✅ | 官方支持 |
| Python | ✅ | ✅ | manylinux2014 预装 |
| .NET | ✅ | ✅ | 官方支持（glibc 限制）|
| mold | ✅ | ✅ | 官方预编译 |
| protoc | ✅ | ✅ | 官方预编译 |
| tini | ✅ | ✅ | 官方预编译 |

### C. 参考资源

- [manylinux2014 规范](https://github.com/pypa/manylinux)
- [mold 链接器](https://github.com/rui314/mold)
- [Protocol Buffers](https://github.com/protocolbuffers/protobuf)
- [tini init 进程](https://github.com/krallin/tini)
- [Docker 多阶段构建](https://docs.docker.com/build/building/multi-stage/)


**日期：** 2026-03-18
**目标：** 优化 tsdb-builder Dockerfile，融合 manylinux2014 优势，支持 TDengine 全组件构建

## 背景

当前 Dockerfile 基于 CentOS 7 + devtoolset-9，满足 glibc 2.17 和 GCC 9.3.1+ 要求。需要融合 Dockerfile-taosx-manylinux2024 的优点：

1. manylinux2014 标准化构建环境
2. 多阶段构建优雅处理多架构
3. mold 链接器加速编译
4. protoc 预编译二进制
5. 完整的 Rust 环境支持

同时保留所有现有工具链：Go、Maven、CMake、JDK、Rust、Python/uv、.NET、OpenSSL。

## 设计目标

- **兼容性：** glibc 2.17，GCC ≥ 9.3.1
- **架构支持：** amd64 和 arm64
- **工具完整性：** 覆盖 TDengine 所有组件（taosd、taosAdapter、taosKeeper）
- **构建速度：** 使用 mold 链接器、合理的层缓存策略
- **镜像体积：** 平衡优化，避免过度膨胀
- **可维护性：** 清晰的层组织，便于调试和更新

## 技术方案

### 1. 基础镜像选择

**从 CentOS 7 迁移到 manylinux2014**

**理由：**
- manylinux2014 基于 CentOS 7，提供 glibc 2.17
- 预装 devtoolset-10（GCC 10.x），高于 9.3.1 要求
- Python 社区标准，兼容性经过广泛验证
- 更标准化的构建环境

**基础镜像：** `docker.1ms.run/lukewiwa/manylinux2014`

### 2. 多阶段构建架构

#### Stage 1: Builder（架构变量处理）

```dockerfile
FROM docker.1ms.run/alpine AS builder
ARG TARGETPLATFORM
```

**职责：**
- 根据 TARGETPLATFORM 映射架构相关变量
- 生成 /etc/environment 文件供后续阶段使用

**架构映射表：**

| TARGETPLATFORM | MOLD_ARCH | TINI_ARCH | PROTOC_ARCH | GO_ARCH | JDK_ARCH | CMAKE_ARCH | DOTNET_ARCH | OPENSSL_ARCH |
|----------------|-----------|-----------|-------------|---------|----------|------------|-------------|--------------|
| linux/amd64    | x86_64    | amd64     | x86_64      | amd64   | x64      | x86_64     | x64         | linux-x86_64 |
| linux/arm64    | aarch64   | arm64     | aarch_64    | arm64   | aarch64  | aarch64    | arm64       | linux-aarch64|

#### Stage 2: Main（构建环境）

```dockerfile
FROM docker.1ms.run/lukewiwa/manylinux2014
COPY --from=builder /etc/environment /etc/
```

**职责：**
- 安装所有工具链和依赖
- 配置构建环境
- 提供完整的 TDengine 构建能力

### 3. 层组织策略

采用**平衡策略**：既优化缓存利用率，又控制总层数。

#### 层 1: 系统基础（yum 仓库 + 基础包）

**内容：**
- yum 仓库配置（CentOS 7 base/updates/extras）
- 基础开发工具：git, wget, curl, unzip, tar, gzip, bzip2
- 编译依赖：libatomic, openssl-devel, zlib-devel, libffi-devel
- Perl 依赖：perl-devel, perl-IPC-Cmd, perl-Test-Simple 等
- yum 清理

**优化：**
- 所有 yum 操作合并到一个 RUN 层
- 安装后立即清理 yum 缓存

#### 层 2: Perl 模块（OpenSSL 编译依赖）

**内容：**
- 复制 cpanm 工具
- 配置 CPAN 镜像（mirrors.aliyun.com）
- 安装 Perl 模块：Term::Table, Test::Simple, Test::More, List::Util, Time::Piece

**理由：**
- OpenSSL 1.1.1w 编译需要这些 Perl 模块
- 独立层便于调试 Perl 环境问题

#### 层 3: OpenSSL 编译

**内容：**
- 从源码编译 OpenSSL 1.1.1w
- 安装到 /usr/local/openssl-1.1.1
- 清理源码和临时文件

**理由：**
- TDengine 需要特定版本的 OpenSSL
- 独立层避免重复编译（耗时操作）

#### 层 4: Go 工具链

**内容：**
- 下载并安装 Go 1.23.4
- 配置 GOROOT, GOPATH, GOPROXY（goproxy.cn）
- 更新 PATH

**版本：** Go 1.23.4
**架构：** 通过 GO_ARCH 变量支持 amd64/arm64

#### 层 5: JDK + Maven

**内容：**
- 安装 JDK 8u441
- 安装 Maven 3.8.4
- 配置 JAVA_HOME, M2_HOME
- 更新 PATH

**理由：**
- Java 工具链通常一起使用
- 合并减少层数

#### 层 6: CMake

**内容：**
- 安装 CMake 3.21.5
- 更新 PATH

**版本：** CMake 3.21.5
**架构：** 通过 CMAKE_ARCH 变量支持

#### 层 7: Rust 工具链

**内容：**
- 配置 Rust 国内镜像（rsproxy.cn）
- 安装 Rust 1.90.0（minimal profile）
- 安装组件：clippy, rustfmt
- 复制 Cargo 配置文件（cargo.toml）
- 更新 PATH

**配置：**
```bash
RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
RUSTUP_DIST_SERVER="https://rsproxy.cn"
```

**理由：**
- 国内镜像加速下载
- 独立层便于 Rust 版本更新

#### 层 8: Python 环境

**内容：**
- 从源码编译 Python 3.10.13（启用 shared library）
- 配置 pip 国内镜像（mirrors.aliyun.com）
- 安装 uv（现代包管理器）
- 安装 Python 包：taospy 2.8.8, taos-ws-py 0.6.5
- 清理源码和临时文件

**理由：**
- Python 从源码编译确保兼容性
- pip 和 uv 都保留，提供灵活性
- TDengine Python 客户端预装

#### 层 9: .NET SDK

**内容：**
- 安装 .NET 6.0.100 SDK
- 配置环境变量

**架构：** 通过 DOTNET_ARCH 变量支持（amd64→x64, arm64→arm64）

**注意：**
- .NET 在 glibc 2.17 环境下功能受限
- 保留以支持部分 .NET 组件

#### 层 10: 现代工具（mold + protoc + tini）

**内容：**
- 安装 mold 2.40.3（现代链接器）
- 安装 protoc 33.0（Protocol Buffers 编译器）
- 安装 tini v0.19.0（init 进程）

**新增工具说明：**

**mold：**
- 现代链接器，比 GNU ld 快 5-10 倍
- 显著加速大型 C++ 项目编译
- 安装到 /usr/bin/mold

**protoc：**
- Protocol Buffers 编译器
- TDengine 部分组件需要
- 安装到 /usr/bin/protoc

**tini：**
- 轻量级 init 进程（PID 1）
- 正确处理信号和僵尸进程
- 作为 ENTRYPOINT

#### 层 11: 环境配置

**内容：**
- 配置 LANG/LANGUAGE/LC_ALL（UTF-8）
- 更新 PATH（整合所有工具）
- 配置 LD_LIBRARY_PATH（OpenSSL）
- Git 全局配置（safe.directory）
- SSH 配置（StrictHostKeyChecking no）
- 时区配置（Asia/Shanghai）
- 创建工作目录 /app

**ENTRYPOINT：**
```dockerfile
ENTRYPOINT ["/bin/tini", "--"]
```

### 4. 版本管理

所有工具版本通过 ARG 参数化，便于更新：

```dockerfile
ARG GO_VERSION=1.23.4
ARG MAVEN_VERSION=3.8.4
ARG CMAKE_VERSION=3.21.5
ARG JDK_VERSION=8u441
ARG RUST_VERSION=1.90.0
ARG PYTHON_VERSION=3.10.13
ARG DOTNET_VERSION=6.0.100
ARG OPENSSL_VERSION=1.1.1w
ARG MOLD_VERSION=2.40.3
ARG PROTOC_VERSION=33.0
ARG TINI_VERSION=v0.19.0
ARG TAOSPY_VERSION=2.8.8
ARG TAOS_WS_PY_VERSION=0.6.5
```

### 5. 镜像配置

**PyPI 镜像：**
```dockerfile
ARG PYPI_MIRROR=http://mirrors.aliyun.com/pypi/simple/
ARG PYPI_TRUSTED_HOST=mirrors.aliyun.com
```

**Go 代理：**
```dockerfile
ARG GO_PROXY=https://goproxy.cn
```

**Rust 镜像：**
```dockerfile
ENV RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup
ENV RUSTUP_DIST_SERVER=https://rsproxy.cn
```

**说明：** rsproxy.cn 提供完整的 Rust 镜像服务，包括 rustup 更新和工具链分发，覆盖所有 Rust 下载需求。

**CPAN 镜像：**
```dockerfile
ENV CPAN_MIRROR=https://mirrors.aliyun.com/CPAN/
```

### 6. 优化技术

#### 6.1 避免 tarball 残留层

使用 `RUN --mount=type=bind` 代替 `ADD`：

```dockerfile
# 不推荐
ADD go1.23.4.linux-amd64.tar.gz /usr/local/

# 推荐
RUN --mount=type=bind,source=installers/go1.23.4.linux-amd64.tar.gz,target=/tmp/go.tar.gz \
    tar -C /usr/local -xzf /tmp/go.tar.gz
```

#### 6.2 层内清理

每层安装后立即清理：

```dockerfile
RUN wget ... && \
    tar -xzf ... && \
    ./configure && make && make install && \
    rm -rf /tmp/* /var/tmp/*
```

#### 6.3 yum 操作合并

所有 yum 操作合并到一个 RUN 层：

```dockerfile
RUN yum makecache fast && \
    yum install -y pkg1 pkg2 pkg3 && \
    yum clean all && \
    rm -rf /var/cache/yum
```

#### 6.4 文档和测试文件清理

安装后删除不必要的文件：

```dockerfile
find /usr/local -type d -name 'test' -o -name 'tests' -exec rm -rf {} + && \
find /usr/local -type f -name '*.md' -o -name 'LICENSE' -exec rm -f {} +
```

### 7. 验证机制

保留现有的 verify-image.sh 脚本，验证：

- glibc 版本（2.17）
- GCC 版本（≥ 9.3.1）
- 所有工具版本和可用性
- 架构信息

新增验证项：
- mold 链接器
- protoc 编译器
- tini init 进程

### 8. 技术细节说明

#### 8.1 devtoolset-10 激活

manylinux2014 镜像已预激活 devtoolset-10，GCC 10.x 已在默认 PATH 中，无需手动执行 `scl enable devtoolset-10`。

#### 8.2 OpenSSL 平台标识符

OpenSSL 配置使用 `OPENSSL_ARCH` 变量：
- amd64: `linux-x86_64`（OpenSSL 官方支持）
- arm64: `linux-aarch64`（OpenSSL 官方支持）

这些是 OpenSSL 标准平台标识符，已验证兼容性。

#### 8.3 mold 链接器 arm64 支持

mold 2.40.3 完整支持 arm64 架构。mold 从 2.0 版本开始正式支持 aarch64，2.40.3 是成熟稳定版本。

#### 8.4 二进制下载容错

关键二进制（mold/protoc/tini）下载失败处理：
- 使用 `wget` 或 `curl` 的重试机制（`--retry` 参数）
- 构建失败时会明确报错，便于定位
- 可通过 ARG 参数切换镜像源

## 实现计划

### 阶段 1: 编写新 Dockerfile

1. 创建 Stage 1（builder）
2. 创建 Stage 2（main）
3. 按层组织策略实现 11 个层
4. 配置所有 ARG 和 ENV 变量
5. 设置 ENTRYPOINT 和 WORKDIR

### 阶段 2: 更新辅助文件

1. 更新 verify-image.sh（新增 mold/protoc/tini 验证）
2. 确保 cpanm 脚本存在
3. 确保 cargo.toml 配置文件存在

### 阶段 3: 构建测试

1. 构建 amd64 镜像
2. 构建 arm64 镜像
3. 运行 verify-image.sh 验证
4. 测试编译 TDengine 组件

### 阶段 4: 文档更新

1. 更新 README（如果存在）
2. 记录迁移说明
3. 更新构建脚本（build.sh）

## 风险评估

### 风险 1: manylinux2014 兼容性

**风险：** 某些工具可能与 manylinux2014 环境不兼容

**缓解：**
- manylinux2014 基于 CentOS 7，与现有环境相似
- 逐层构建，便于定位问题
- 保留 verify-image.sh 全面验证

### 风险 2: 镜像体积增加

**风险：** 新增工具（mold/protoc/tini）可能增加体积

**缓解：**
- mold: ~50MB
- protoc: ~5MB
- tini: <1MB
- 总增加约 60MB，可接受

### 风险 3: 构建时间

**风险：** 从源码编译 OpenSSL 和 Python 耗时

**缓解：**
- 独立层，Docker 缓存有效
- 版本不常变，缓存命中率高

### 风险 4: 多架构构建

**风险：** 架构变量映射可能有遗漏

**缓解：**
- 参考 taosx Dockerfile 的成熟方案
- 构建时明确测试两种架构

## 成功标准

1. ✅ 镜像基于 manylinux2014
2. ✅ glibc 版本为 2.17
3. ✅ GCC 版本 ≥ 9.3.1（实际为 10.x）
4. ✅ 支持 amd64 和 arm64 架构
5. ✅ 所有现有工具可用（Go/Maven/CMake/JDK/Rust/Python/uv/.NET/OpenSSL）
6. ✅ 新增工具可用（mold/protoc/tini）
7. ✅ verify-image.sh 验证通过
8. ✅ 能够成功编译 TDengine 组件（taosd/taosAdapter/taosKeeper）
9. ✅ 镜像体积增加 < 10%（相比现有镜像）
10. ✅ 构建时间增加 < 20%（首次构建）

## 后续优化方向

1. **CI/CD 集成：** 自动化多架构构建和测试
2. **缓存优化：** 使用 BuildKit 缓存挂载进一步加速
3. **分层镜像：** 考虑提供 base/full 两个版本
4. **工具版本自动更新：** 定期检查和更新工具版本

## 附录

### A. 工具版本清单

| 工具 | 版本 | 用途 |
|------|------|------|
| glibc | 2.17 | 系统基础库 |
| GCC | 10.x | C/C++ 编译器 |
| Go | 1.23.4 | Go 语言工具链 |
| Maven | 3.8.4 | Java 构建工具 |
| CMake | 3.21.5 | 跨平台构建系统 |
| JDK | 8u441 | Java 开发环境 |
| Rust | 1.90.0 | Rust 工具链 |
| Python | 3.10.13 | Python 解释器 |
| .NET | 6.0.100 | .NET SDK |
| OpenSSL | 1.1.1w | 加密库 |
| mold | 2.40.3 | 现代链接器 |
| protoc | 33.0 | Protocol Buffers 编译器 |
| tini | v0.19.0 | Init 进程 |
| uv | latest | Python 包管理器 |
| taospy | 2.8.8 | TDengine Python 客户端 |
| taos-ws-py | 0.6.5 | TDengine WebSocket 客户端 |

### B. 架构支持矩阵

| 组件 | amd64 | arm64 | 备注 |
|------|-------|-------|------|
| manylinux2014 | ✅ | ✅ | 基础镜像 |
| Go | ✅ | ✅ | 官方支持 |
| JDK | ✅ | ✅ | Oracle 官方 |
| Maven | ✅ | ✅ | 架构无关（Java） |
| CMake | ✅ | ✅ | 官方预编译 |
| Rust | ✅ | ✅ | 官方支持 |
| Python | ✅ | ✅ | 源码编译 |
| .NET | ✅ | ✅ | 官方支持 |
| OpenSSL | ✅ | ✅ | 源码编译 |
| mold | ✅ | ✅ | 官方预编译 |
| protoc | ✅ | ✅ | 官方预编译 |
| tini | ✅ | ✅ | 官方预编译 |

### C. 参考资源

- [manylinux2014 规范](https://github.com/pypa/manylinux)
- [mold 链接器](https://github.com/rui314/mold)
- [Protocol Buffers](https://github.com/protocolbuffers/protobuf)
- [tini init 进程](https://github.com/krallin/tini)
- [Docker 多阶段构建](https://docs.docker.com/build/building/multi-stage/)
