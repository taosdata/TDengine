# tsdb-builder

TDengine TSDB 多语言编译环境 Docker 镜像构建工具，提供三套独立镜像：

| 镜像 | 用途 | 基础 |
|---|---|---|
| **`harbor.tdengine.net/tsdb-builder/core`** | 核心组件：ENGINE, ENTERPRISE, ADAPTER, KEEPER, TOOLS, GEN, TAOSX | manylinux2014 (glibc 2.17, GCC 7.3) |
| **`harbor.tdengine.net/tsdb-builder/dev`** | 核心组件（开发用）：ENGINE, ENTERPRISE, ADAPTER, KEEPER, TOOLS, GEN, TAOSX | manylinux2014 (glibc 2.17, GCC 9.3.1) |
| **`harbor.tdengine.net/tsdb-builder/others`** | 周边组件：INSIGHT, EXPLORER_UI + 所有 connector | manylinux_2_28 (glibc 2.28) |

三套镜像均支持 `linux/amd64` 和 `linux/arm64` 两种架构。core 镜像额外支持 `linux/riscv64`（基于 Debian trixie）。

> **dev 镜像 vs core 镜像**：dev 镜像与 core 镜像唯一的区别是编译器版本——dev 使用 devtoolset-9 (GCC 9.3.1)，core 使用 devtoolset-7 (GCC 7.3.1)。dev 镜像适用于日常开发，core 镜像兼容麒麟 V10 运行环境。组件范围、工具链、基础系统完全一致。

**仓库地址**：<https://git.tdengine.net/taosdata/rd-dept/platform/platform>

---

## 目录结构

```text
tsdb-builder/
├── Dockerfile.core         # core 镜像构建文件（manylinux2014, glibc 2.17, GCC 7.3）
├── Dockerfile.core-riscv64 # core 镜像构建文件（debian:trixie, riscv64 专用）
├── Dockerfile.dev          # dev 镜像构建文件（manylinux2014, glibc 2.17, GCC 9.3.1）
├── Dockerfile.others       # others 镜像构建文件（manylinux_2_28, glibc 2.28）
├── build-core-image.sh     # 构建并推送 harbor.../tsdb-builder/core
├── build-dev-image.sh      # 构建并推送 harbor.../tsdb-builder/dev
├── build-others-image.sh   # 构建并推送 harbor.../tsdb-builder/others
├── build.sh                # 统一构建入口（--image 必填，支持组件名和 cmake 参数透传）
├── verify-image.sh         # 镜像组件验证脚本
├── scripts/
│   ├── prepare-externals.sh    # 下载并上传外部依赖 tarball 到 GitLab Package Registry
│   └── externals-manifest.txt  # 依赖清单及 SHA256 校验值
├── .cargo/                 # Cargo 配置（Rust 镜像源）
└── .build-args             # 构建参数默认值（版本号、镜像源等）
```

> **离线安装包不存入本仓库。**
> 安装包体积较大，统一存放于宿主机 `/data/packages/`。
>
> 构建镜像前，请确认所需文件已放置在宿主机的 `/data/packages/` 目录下。

---

## 工具链说明

### tsdb-builder-core（glibc 2.17 / riscv64: glibc 2.41+）

| 工具 | 版本 | 说明 |
|---|---|---|
| glibc | 2.17（riscv64: 2.41+） | manylinux2014，兼容旧发行版；riscv64 使用 Debian trixie |
| GCC / G++ | 7.x（riscv64: 14.x） | CentOS 7 devtoolset-7（兼容麒麟 V10）；riscv64 使用 Debian 系统 GCC |
| Go | 1.23.4 | GOPROXY=goproxy.cn |
| CMake | 3.21.5 | |
| Rust / Cargo | 1.90.0 | rsproxy.cn 镜像 |
| Python | 3.12 | manylinux2014 预装 |
| mold | 2.40.3 | 高速链接器（源码编译，兼容 glibc 2.17） |
| protoc | 33.0 | Protocol Buffers |
| tini | v0.19.0 | 容器 init |

### tsdb-builder-dev（glibc 2.17）

与 core 镜像基础完全一致（manylinux2014），唯一区别是使用 devtoolset-9 (GCC 9.3.1) 替代 devtoolset-7 (GCC 7.3.1)。
适用于日常开发场景，不需要兼容麒麟 V10。

| 工具 | 版本 | 说明 |
|---|---|---|
| glibc | 2.17 | manylinux2014 |
| GCC / G++ | 9.x | CentOS 7 devtoolset-9 |
| Go | 1.23.4 | GOPROXY=goproxy.cn |
| CMake | 3.21.5 | |
| Rust / Cargo | 1.90.0 | rsproxy.cn 镜像 |
| Python | 3.12 | manylinux2014 预装 |
| mold | 2.40.3 | 高速链接器（源码编译，兼容 glibc 2.17） |
| protoc | 33.0 | Protocol Buffers |
| tini | v0.19.0 | 容器 init |

### tsdb-builder-others（glibc 2.28）

在 core 全部工具的基础上，额外包含：

| 工具 | 版本 | 说明 |
|---|---|---|
| glibc | 2.28 | manylinux_2_28 / AlmaLinux 8 |
| GCC / G++ | 14.x | AlmaLinux 8 gcc-toolset-14 |
| Node.js | 22.14.0 | 官方二进制，含 yarn + pnpm |
| JDK | 8u144 (amd64) / 8u441 (arm64) | OpenJDK 8 |
| Maven | 3.8.4 | |
| .NET SDK | 6.0.428 | |

---

## 离线安装包清单

两套镜像共用同一份安装包，统一存放于宿主机 `/data/packages/`（各自使用其中需要的文件）：

| 文件名 | 适用架构 | 用于 |
|---|---|---|
| `go1.23.4.linux-amd64.tar.gz` | amd64 | core + others |
| `go1.23.4.linux-arm64.tar.gz` | arm64 | core + others |
| `go1.23.4.linux-riscv64.tar.gz` | riscv64 | core |
| `jdk-8u144-linux-x64.tar.gz` | amd64 | others |
| `jdk-8u441-linux-aarch64.tar.gz` | arm64 | others |
| `apache-maven-3.8.4-bin.tar.gz` | 通用 | others |
| `cmake-3.21.5-linux-x86_64.tar.gz` | amd64 | core + others |
| `cmake-3.21.5-linux-aarch64.tar.gz` | arm64 | core + others |
| `mold-2.40.3.tar.gz` | 通用 | core + dev |

**others 镜像额外需要以下 TDengine 客户端文件**（用于 ODBC 等连接器编译）：

| 文件名 | 容器内路径 | 用于 |
|---|---|---|
| `taos.h` | `/usr/include/taos.h` | others |
| `taosws.h` | `/usr/include/taosws.h` | others |
| `libtaos.so` | `/usr/lib/libtaos.so` | others |
| `libtaosnative.so` | `/usr/lib/libtaosnative.so` | others |
| `libtaosws.so` | `/usr/lib/libtaosws.so` | others |

---

## 快速开始

### 构建 Docker 镜像

```bash
# 先登录 Harbor
docker login harbor.tdengine.net

# 构建并推送 core 镜像（默认 amd64，离线包默认 $HOME/packages）
./build-core-image.sh --version 0.3.0
./build-core-image.sh --arch arm64 --version 0.3.0
./build-core-image.sh --arch riscv64 --version 0.3.0    # 需在 riscv64 主机上原生构建
./build-core-image.sh --version 0.3.0 --packages /data/packages   # 显式指定安装包目录

# 构建并推送 dev 镜像（与 core 相同组件，GCC 9.3.1）
./build-dev-image.sh --version 0.3.0
./build-dev-image.sh --arch arm64 --version 0.3.0

# 构建并推送 others 镜像
./build-others-image.sh --version 0.3.0
./build-others-image.sh --arch arm64 --version 0.3.0
# 也支持旧的位置参数：./build-core-image.sh amd64 --version 0.3.0

# 跳过 Docker 层缓存（修改 Dockerfile 后推荐使用）
./build-core-image.sh --version 0.3.0 --no-cache
./build-dev-image.sh --version 0.3.0 --no-cache
./build-others-image.sh --version 0.3.0 --no-cache

# 仅构建本地镜像，不推送到 Harbor（适合测试 Dockerfile 修改）
./build-core-image.sh --version test --arch amd64 --local
./build-dev-image.sh --version test --arch arm64 --local
./build-others-image.sh --version test --local
```

发布的镜像仓库固定为：

- `harbor.tdengine.net/tsdb-builder/core`
- `harbor.tdengine.net/tsdb-builder/dev`
- `harbor.tdengine.net/tsdb-builder/others`

每次发布都会推送单架构 tag：

- `<version>-amd64`
- `<version>-arm64`
- `<version>-riscv64`
- `latest-amd64`
- `latest-arm64`
- `latest-riscv64`

当另一架构 tag 已存在时，脚本还会尽力更新多架构 manifest（仅 amd64/arm64，riscv64 为独立 tag）：

- `<version>`
- `latest`

加 `--local` 参数可跳过推送，仅在本地构建镜像（不打 `latest-*` tag、不推送、不更新 manifest）。适合测试 Dockerfile 修改。

> **强烈建议在原生架构主机上构建对应镜像。**
> 由于 mold 是源码编译，且 yum/pip/rustup 等步骤较多，跨架构（QEMU 模拟）构建会慢 10–20 倍（实测 8 分钟 → 2 小时）。
> 默认情况下脚本会拒绝跨架构构建：在 amd64 主机上跑 `--arch amd64`，在 arm64 主机上跑 `--arch arm64`，两侧都推送完成后脚本会自动合成多架构 manifest。
> 如确实需要单机跨架构构建，可加 `--allow-emulation` 强制继续。

### 验证镜像

```bash
./verify-image.sh core:amd64
./verify-image.sh core:arm64
./verify-image.sh dev:amd64
./verify-image.sh dev:arm64
./verify-image.sh others:amd64
./verify-image.sh others:arm64

# 版本化 shorthand 或完整镜像名也支持
./verify-image.sh core:0.3.0-amd64
./verify-image.sh dev:0.3.0-amd64
./verify-image.sh harbor.tdengine.net/tsdb-builder/core:0.3.0-amd64
```

### 编译 TSDB 项目

> [!IMPORTANT]
> **首次编译（`.externals/` 不存在）必须加 `-DBUILD_CONTRIB=ON`**，否则构建会因找不到 xxhash、zstd 等外部依赖头文件而失败。
> `.externals/` 构建完成后，后续增量编译可省略此参数（默认 `OFF` 直接复用）。
> 关于 `BUILD_CONTRIB`、`BUILD_ROCKSDB`、`ROCKSDB_USE_DEPS` 三个参数的完整组合说明，参见文末 [RocksDB 编译选项](#rocksdb-编译选项) 章节。
>
> > **升级迁移**：若本地已存在旧版的 `externals-amd64/` 或 `externals-arm64/` 目录，可按需重命名为 `externals-core-{arch}/` 或 `externals-others-{arch}/`，也可直接删除并在下次编译时加 `-DBUILD_CONTRIB=ON` 重新构建。
>
> **不指定 `--src` 时，`build.sh` 默认使用当前工作目录作为 TSDB 源码路径。**
> 运行前请先手动拉取代码并切换到目标分支：
> ```bash
> cd /path/to/TDengine
> git pull
> git checkout <target-branch>
> ```

#### 推荐：使用 `build.sh`（按需选择组件）

`--image` 为必填参数，支持 `core`、`dev`、`others`、`core:<version>`、`dev:<version>`、`others:<version>` 六种 selector。
不带版本时默认解析为 `latest`；脚本会按 `--arch` 自动补成单架构 Harbor tag（例如 `core` + `arm64` → `harbor.tdengine.net/tsdb-builder/core:latest-arm64`）。

```bash
# 切换到 TSDB 源码目录后直接运行
cd /path/to/TDengine

# 基本用法
./path/to/tsdb-builder/build.sh --image core engine taosx
./path/to/tsdb-builder/build.sh --image dev engine taosx          # 使用 dev 镜像（GCC 9）
./path/to/tsdb-builder/build.sh --image others explorer-ui insight jdbc
./path/to/tsdb-builder/build.sh --image core:0.3.0 engine
./path/to/tsdb-builder/build.sh --image others --pull-image explorer-ui

# 预定义组
./build.sh --image core  core-all    # 全部 core 组件
./build.sh --image others others-all # 全部 others 组件
./build.sh --image others all        # 全部 16 个组件

# 跨架构 / 指定选项
./build.sh --image core --arch arm64 engine adapter
./build.sh --image core --clean --arch amd64 taosx   # 全量重编（清除 cmake 缓存）
./build.sh --image core --src /data/tsdb engine      # 显式指定源码目录
./build.sh --image core --cache /data/cache/tsdb-builder engine  # 显式指定缓存目录

# 分离调试信息（Release 构建推荐）
./build.sh --image core --split-debug core-all -DCMAKE_BUILD_TYPE=Release
```

**cmake 参数透传（`-DKEY=VALUE`）**

任意数量的 cmake 参数可追加在命令行末尾，直接传入容器内的 cmake 调用。  
`-D` 参数排在组件名自动生成的开关之后，cmake 以最后一个 `-D` 为准，可覆盖任意组件默认值。

```bash
# 组件名 + 少量覆盖（开发 Debug 模式）
./build.sh --image core engine -DCMAKE_BUILD_TYPE=Debug

# 纯 -D 模式（发版脚本用法，无组件名快捷方式）
cmake_args=(
  -DBUILD_ENGINE=ON
  -DBUILD_ENTERPRISE=ON
  -DBUILD_ADAPTER=ON
  -DBUILD_KEEPER=ON
  -DBUILD_TOOLS=ON
  -DBUILD_TAOSX=ON
  -DBUILD_RUST=ON
  -DCMAKE_BUILD_TYPE=Release
  -DBUILD_VER_NUMBER=3.4.1.3
  -DBUILD_GITINFO="${GIT_HASH}"
  -D"BUILD_VER_DATE=${BUILD_DATE}"
  -DBUILD_GRANT_VALUE=15
)
./build.sh --image core --arch amd64 "${cmake_args[@]}"
```

> **注意**：值含空格（如 `BUILD_VER_DATE`）必须用 bash 数组传参（`"${cmake_args[@]}"`），  
> 不能拼接成字符串，否则空格会导致参数断裂。

编译输出：
- core 组件 → `<src>/debug/`
- dev 组件 → `<src>/debug-dev/`
- others 组件 → `<src>/debug-others/`

#### 全量编译（CI 等效命令）

```bash
# 全部 core 组件（清空构建目录）
./build.sh --image core --clean core-all

# 全部 others 组件（TAOSX Rust 二进制由 core 步骤产出，CI 构建排除 ODBC）
./build.sh --image others --clean others-all -DBUILD_TAOSX=OFF -DBUILD_ODBC=OFF

# 显式指定架构、源码目录、缓存目录
./build.sh --image core --arch arm64 --src /data/tsdb --cache /data/cache/tsdb-builder --clean core-all
./build.sh --image others --arch arm64 --src /data/tsdb --clean others-all -DBUILD_TAOSX=OFF -DBUILD_ODBC=OFF
```

---

## 构建参数说明

所有参数均在 `.build-args` 中集中管理，三个构建脚本均读取此文件（各自使用其中相关的参数）。

| 参数 | 默认值 | 说明 |
|---|---|---|
| `GO_VERSION` | `1.23.4` | Go 版本 |
| `MAVEN_VERSION` | `3.8.4` | Maven 版本（others 用） |
| `CMAKE_VERSION` | `3.21.5` | CMake 版本 |
| `JDK_VERSION_AMD64` | `8u144` | AMD64 JDK 版本（others 用） |
| `JDK_VERSION_ARM64` | `8u441` | ARM64 JDK 版本（others 用） |
| `RUST_VERSION` | `1.90.0` | Rust 工具链版本 |
| `PYTHON_VERSION` | `3.12` | Python 版本（manylinux 预装） |
| `DOTNET_VERSION` | `6.0.428` | .NET SDK 版本（others 用） |
| `NODE_VERSION` | `22.14.0` | Node.js 版本（others 用） |
| `MOLD_VERSION` | `2.40.3` | mold 链接器版本 |
| `PROTOC_VERSION` | `33.0` | protoc 版本 |
| `TINI_VERSION` | `v0.19.0` | tini 版本 |
| `TAOSPY_VERSION` | `2.8.8` | taospy Python 包版本 |
| `TAOS_WS_PY_VERSION` | `0.6.5` | taos-ws-py Python 包版本 |
| `PYPI_MIRROR` | `http://mirrors.aliyun.com/pypi/simple/` | PyPI 镜像源 |
| `PYPI_TRUSTED_HOST` | `mirrors.aliyun.com` | PyPI 可信主机 |
| `GO_PROXY` | `https://goproxy.cn` | Go 模块代理 |
| `TIMEZONE` | `Asia/Shanghai` | 容器时区 |

---

## Dockerfile 设计说明

### Dockerfile.core（双架构，均 glibc 2.17）

```text
Stage 1 (builder / Alpine, 原生架构运行)
  ├─ 架构变量映射 → /etc/environment
  └─ 交叉编译 mage（避免 Go runtime 在 QEMU 下崩溃）

Stage 2: 架构选择
  └─ manylinux2014_x86_64 或 manylinux2014_aarch64

Stage 3 (mold-builder / manylinux2014, devtoolset-10)
  └─ 从 packages/ 解压 mold 源码并编译（需 C++20；主镜像降级到 GCC 7 后无法编译）

Stage 4 (主镜像 / manylinux2014)
  ├─ Layer 1 : yum 基础包 + CentOS 7 镜像源 + GCC 降级至 devtoolset-7
  ├─ Layer 2 : Go（离线 tar.gz）+ mage（COPY from builder）
  ├─ Layer 3 : CMake（离线 tar.gz）
  ├─ Layer 4 : Rust（rustup，rsproxy.cn）
  ├─ Layer 5 : Python 3.12（预装，symlink）+ pip + taospy
  ├─ Layer 6 : mold（COPY from mold-builder，注册为默认链接器）+ protoc + tini
  └─ Layer 7 : 环境变量、SSH、时区等
```

### Dockerfile.others（双架构，均 glibc 2.28）

```text
Stage 1 (builder / Alpine)
  └─ 架构变量映射 → /etc/environment

Stage 2: 架构选择
  └─ manylinux_2_28_x86_64 或 manylinux_2_28_aarch64

Stage 3 (mold-builder / manylinux_2_28, GCC 14)
  └─ 源码编译 mold（release 二进制 arm64 需 glibc ≥ 2.31，但本镜像仅 2.28）

Stage 4 (主镜像 / manylinux_2_28)
  ├─ Layer 1 : yum 基础包（AlmaLinux 8 自带仓库）
  ├─ Layer 2 : Go（离线 tar.gz）+ mage
  ├─ Layer 3 : JDK + Maven（离线 tar.gz）
  ├─ Layer 4 : CMake（离线 tar.gz）
  ├─ Layer 5 : Rust（rustup，rsproxy.cn）
  ├─ Layer 6 : Python 3.12（预装，symlink）+ pip + taospy
  ├─ Layer 7 : .NET SDK（官方安装脚本）
  ├─ Layer 8 : Node.js 22 + yarn + pnpm（官方二进制）
  ├─ Layer 9 : mold（COPY from mold-builder，注册为默认链接器）+ protoc + tini
  ├─ Layer 10: TDengine 客户端文件（taos.h, taosws.h, libtaos.so 等，from packages）
  └─ Layer 11: 环境变量、SSH、时区等
```

### Dockerfile.dev（双架构，均 glibc 2.17，GCC 9.3.1）

与 Dockerfile.core 结构完全一致，唯一差异是将 devtoolset-7 (GCC 7.3.1) 替换为 devtoolset-9 (GCC 9.3.1)。
不需要兼容麒麟 V10 运行环境的开发场景推荐使用 dev 镜像。

```text
Stage 1 (builder / Alpine, 原生架构运行)
  ├─ 架构变量映射 → /etc/environment
  └─ 交叉编译 mage（避免 Go runtime 在 QEMU 下崩溃）

Stage 2: 架构选择
  └─ manylinux2014_x86_64 或 manylinux2014_aarch64

Stage 3 (mold-builder / manylinux2014, devtoolset-10)
  └─ 从 packages/ 解压 mold 源码并编译（需 C++20；主镜像降级到 GCC 9 后无法编译）

Stage 4 (主镜像 / manylinux2014)
  ├─ Layer 1 : yum 基础包 + CentOS 7 镜像源 + GCC 替换为 devtoolset-9
  ├─ Layer 2 : Go（离线 tar.gz）+ mage（COPY from builder）
  ├─ Layer 3 : CMake（离线 tar.gz）
  ├─ Layer 4 : Rust（rustup，rsproxy.cn）
  ├─ Layer 5 : Python 3.12（预装，symlink）+ pip + taospy
  ├─ Layer 6 : mold（COPY from mold-builder，注册为默认链接器）+ protoc + tini
  └─ Layer 7 : 环境变量、SSH、时区等
```

### Dockerfile.core-riscv64（riscv64，glibc 2.41+）

manylinux2014 不支持 riscv64，因此 riscv64 使用独立的 Dockerfile，基于 `debian:trixie`。
工具链从系统仓库安装（cmake、protoc、mold、tini），仅 Go 和 Rust 使用离线包/官方安装器。
构建无需 `docker buildx`，使用原生 `docker build` 即可。

```text
Stage 1 (go-installer / debian:trixie)
  ├─ 从 packages/ 提取 Go 二进制
  └─ 编译 mage

Stage 2 (主镜像 / debian:trixie)
  ├─ Layer 1 : apt 基础包（gcc, g++, cmake, protoc, mold, tini, python3 等）
  │            注：mold 仅注册为可用链接器，不设为默认（GNU ld 保持默认）
  ├─ Layer 2 : Go（COPY from go-installer）+ mage
  ├─ Layer 3 : Rust（rustup，rsproxy.cn）
  ├─ Layer 4 : Python pip + taospy + conan + uv
  └─ Layer 5 : 环境变量、SSH、时区等
```

### 关键设计决策

| 决策 | 原因 |
|---|---|
| core 用 glibc 2.17，others 用 glibc 2.28 | Node 22 arm64 官方二进制需要 glibc >= 2.25；core 不需要 Node，保持 2.17 最大兼容性 |
| others 双架构统一用 manylinux_2_28 | 两个架构 glibc 一致，消除 arm64 特殊处理 |
| core 降级 GCC 至 devtoolset-7 (7.3) | 兼容麒麟 V10 运行环境 |
| dev 使用 devtoolset-9 (9.3.1) | 提供更高版本 GCC 用于日常开发，不需要兼容麒麟 V10 时使用 |
| mold 源码编译（multi-stage） | mold release 二进制需 glibc ≥ 2.24 (x86_64) / ≥ 2.31 (arm64)，超出两套镜像的 glibc 版本。在独立 stage 中用高版本 GCC 编译，`-DMOLD_MOSTLY_STATIC=ON` 静态链接 libstdc++ |
| mold 注册为默认链接器（amd64/arm64） | 两套镜像均通过 `update-alternatives` 和 gcc-toolset alternatives 覆盖将 mold 设为默认链接器。core 镜像额外需要 `-DMOLD_USE_MIMALLOC=OFF` 编译 mold，因为 mimalloc 在 glibc 2.17 arm64 上会 segfault |
| riscv64 mold 不设为默认链接器 | mold 在 riscv64 上链接 Go CGO 二进制时会破坏 Go runtime 的 ELF 布局（pclntab），导致 taosadapter 等组件启动时 SIGSEGV。riscv64 镜像保留 GNU ld 为默认链接器，`build.sh` 通过 `-DCMAKE_LINKER=mold` 仅对 C/C++ 目标使用 mold |
| riscv64 使用独立 Dockerfile | manylinux2014 不支持 riscv64，且 CentOS 7 无 riscv64 生态，因此使用 `debian:trixie` 作为基础镜像，工具链（cmake/protoc/mold/tini）均从 apt 安装 |
| riscv64 无需 buildx | riscv64 主机的 Docker 可能不包含 buildx 插件，使用原生 `docker build` 即可，通过临时构建上下文传递安装包 |
| mage 在 builder stage 交叉编译（core） | Go runtime 在 QEMU amd64 仿真下崩溃，因此在原生架构 builder stage 中交叉编译 |
| 离线安装包优先（Go/JDK/Maven/CMake） | 避免构建时依赖外部网络；`--mount=type=bind` 不写入镜像层 |
| Python 使用 manylinux 预装版 | 省去源码编译时间，且内置 OpenSSL |
| pthread cmake 修复（core/dev 镜像） | manylinux2014 的 FindThreads 会尝试 -lpthreads（不存在），需显式传入五个 cmake 变量；`build.sh` 使用 core 或 dev 镜像时已自动处理 |

---

## 调试信息分离（`--split-debug`）

`build.sh` 支持 `--split-debug` 参数，在编译完成后自动将 DWARF 调试信息分离为独立文件。适用于 Release 构建：发布包体积大幅缩小，同时保留 coredump 调试能力。

### 工作原理

编译完成后，`build.sh` 在容器内对以下文件执行分离操作：

**可执行文件**（`build/bin/`）：

| 二进制 | 语言 | 说明 |
|---|---|---|
| `taosd` | C | 服务端主进程 |
| `taos` / `taosql` | C | 命令行客户端 |
| `taosmqtt` | C | MQTT 组件 |
| `taosudf` | C | UDF 执行器 |
| `taosgen` | C | 数据生成工具 |
| `taosadapter` | Go (CGO) | RESTful 适配器 |
| `taoskeeper` | Go (CGO) | 监控采集器 |

**共享库**（`build/lib/`）：

| 文件 | 说明 |
|---|---|
| `libtaos.so` | 客户端 SDK |
| `libtaosnative.so` | 原生连接库 |

> **Rust 二进制（taosx、taos-explorer）不参与分离**——Rust release profile 默认不包含调试信息。

每个文件执行：
1. `objcopy --only-keep-debug <file> .debug/<file>.debug` — 提取调试信息
2. `strip -s <executable>` 或 `strip --strip-debug <shared-lib>` — 去除调试信息（可执行文件同时去除符号表以最大化缩减体积；共享库保留动态符号以确保链接正常）
3. `objcopy --add-gnu-debuglink=.debug/<file>.debug <file>` — 写入 `.gnu_debuglink` 段

### 输出结构

```text
<src>/debug/build/
├── bin/
│   ├── taosd              ← 已 strip -s，用于发布
│   ├── taos
│   ├── taosadapter
│   ├── ...
│   └── .debug/            ← GNU 标准搜索路径
│       ├── taosd.debug
│       ├── taos.debug
│       └── ...
├── lib/
│   ├── libtaos.so         ← 已 strip --strip-debug
│   ├── libtaosnative.so
│   └── .debug/
│       ├── libtaos.so.debug
│       └── libtaosnative.so.debug
```

### 使用方法

**方式一：随编译自动分离（`--split-debug`）**

```bash
# Release 构建 + 分离调试信息（在容器内自动完成）
./build.sh --image core --split-debug core-all -DCMAKE_BUILD_TYPE=Release

# 发版时只打包 build/bin/ 和 build/lib/ 下的文件（不含 .debug/ 目录）
# 单独归档 .debug/ 目录供后续 coredump 分析使用
```

**方式二：编译后独立执行（`split-debug.sh`）**

适用于已有编译产物、需要在宿主机上单独分离调试信息的场景：

```bash
# 基本用法：对指定构建目录执行分离（.debug/ 生成在 bin/lib 旁边）
./split-debug.sh debug
./split-debug.sh debug-dev
./split-debug.sh debug-others

# 指定调试信息输出目录（生成 <output>/bin/ 和 <output>/lib/ 子目录）
./split-debug.sh debug --debug-dir /data/debug-symbols
./split-debug.sh debug-others --debug-dir ./symbols

# 绝对路径
./split-debug.sh /data/TDengine/debug --debug-dir /data/symbols
```

`split-debug.sh` 的处理逻辑与 `--split-debug` 完全一致，区别在于它直接在宿主机运行，不依赖 Docker 容器。

### GDB 调试 coredump

```bash
# 方式一：自动发现（GDB 自动搜索 .debug/ 子目录）
cp -r .debug/ /path/to/bin/.debug/
gdb /path/to/bin/taosd core.dump

# 方式二：手动指定
gdb -s /path/to/.debug/taosd.debug /path/to/bin/taosd core.dump

# 方式三：运行时加载
gdb /path/to/bin/taosd core.dump
(gdb) symbol-file /path/to/.debug/taosd.debug
```

---

## 编译模式与调试信息

TSDB 的 cmake 体系有多个构建模式参数，它们的交互关系需要理解清楚。

### 主项目编译标志

| 参数 | 来源 | 默认值 | 作用 |
|---|---|---|---|
| `CMAKE_BUILD_TYPE` | cmake 内置 | 空（无优化） | 控制 cmake 内置的 `-O` / `-DNDEBUG` 等优化选项 |
| `BUILD_RELEASE` | TSDB 自定义 | `OFF` | 控制 TSDB 自身的编译标志（是否包含 `-g3 -gdwarf-2`） |

| `CMAKE_BUILD_TYPE` | `BUILD_RELEASE` | C/C++ 优化 | 调试信息 | 典型场景 |
|---|---|---|---|---|
| 未设置 / `Debug` | `OFF`（默认） | 无 `-O` | ✅ `-g3 -gdwarf-2` | 日常开发调试 |
| `Release` | `OFF`（默认） | `-O3` | ✅ `-g3 -gdwarf-2` | 性能测试但保留调试能力 |
| `Release` | `ON` | `-O3` | ❌ 无调试信息 | 最终发版（最小体积） |

> **关键认知**：`-DCMAKE_BUILD_TYPE=Release` 仅添加 `-O3` 优化，**不会**移除调试信息。要完全剔除调试信息，必须同时传 `-DBUILD_RELEASE=ON`。

### 依赖库编译标志

| 参数 | 来源 | 默认值 | 作用 |
|---|---|---|---|
| `TD_ALIGN_EXTERNAL` | `external.cmake` | `ON` | ExternalProject 依赖库跟随主项目的 `CMAKE_BUILD_TYPE` |

`TD_ALIGN_EXTERNAL` 必须保持默认值 `ON`。构建二进制包时，依赖库的编译模式必须与产出组件对齐，否则链接阶段会报错。

### 推荐构建配置

**场景一：日常开发**（保留完整调试信息）
```bash
./build.sh --image core engine enterprise \
    -DCMAKE_BUILD_TYPE=Debug
```

**场景二：Release 构建 + 分离调试信息**（推荐发版配置）
```bash
./build.sh --image core --split-debug core-all \
    -DCMAKE_BUILD_TYPE=Release

# 发布时只打包 build/bin/ 下的二进制（不含 .debug/ 子目录）
# 单独归档 .debug/ 目录，供 coredump 分析时加载
```

**场景三：最终发版**（完全剔除调试信息，无法事后调试 coredump）
```bash
./build.sh --image core core-all \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_RELEASE=ON
```

> 💡 **推荐场景二而非场景三**：`--split-debug` 方案兼顾了发布包体积和事后调试能力。完全剔除调试信息（`BUILD_RELEASE=ON`）后，coredump 将无法还原源码级堆栈。

---

## 切换编译环境时的缓存清理规则

频繁切换 Debug/Release 编译模式、core/dev/others 镜像、或不同版本的镜像（对应不同 GCC 版本）时，部分缓存可能引发 ABI 不兼容或 cmake 配置残留问题。以下是各缓存目录的隔离现状和清理规则。

### 缓存隔离现状

| 缓存目录 | 按镜像隔离 | 按架构隔离 | 说明 |
|---|---|---|---|
| `externals-{core,dev,others}-{arch}/` | ✅ | ✅ | 每个镜像+架构组合独立目录 |
| `ccache-{core,dev,others}-{arch}/` | ✅ | ✅ | 编译缓存，hash 包含编译器选项，Debug/Release 自动隔离 |
| `conan2-{arch}/` | ❌ 三镜像共享 | ✅ | Conan profile 缓存了编译器版本 |
| `go-mod/` | ❌ 共享 | ❌ 共享 | 源码缓存，无编译产物 |
| `cargo-registry/`、`cargo-git/` | ❌ 共享 | ❌ 共享 | 源码缓存，无编译产物 |

> `go-mod/`、`cargo-registry/`、`cargo-git/` 只存储下载的源码，不含编译产物，**所有场景都不需要清除**。
> `ccache-*` 一般也不需要清除——ccache hash key 包含完整编译选项和编译器内容，切换 Debug/Release 或镜像版本不会产生缓存冲突。仅在磁盘空间不足时可安全删除。

### 场景速查表

| 切换场景 | `--clean` | 删 `conan2-{arch}/` | 删 `externals-*` |
|---|---|---|---|
| Debug ↔ Release | ✅ | ❌ | ❌ |
| core ↔ dev（GCC 7 ↔ GCC 9） | ✅ | ✅ | ❌（已隔离） |
| core ↔ others（GCC 7 ↔ GCC 14） | ✅ | ✅ | ❌（已隔离） |
| 同镜像不同版本（GCC 版本变了） | ✅ | ✅ | ❌（已隔离） |
| 同镜像不同版本（GCC 版本没变） | ✅ | ❌ | ❌ |
| amd64 ↔ arm64 | ✅ | ❌（已隔离） | ❌（已隔离） |
| 日常增量编译（同镜像同模式） | ❌ | ❌ | ❌ |

### 详细说明

**切换 Debug ↔ Release：必须带 `--clean`**

cmake 的 `CMakeCache.txt` 会缓存 `CMAKE_BUILD_TYPE`，不清除会继续使用旧值。Conan 缓存不需要清除——Conan 按 build type 分 package ID，不同模式可以共存。

```bash
./build.sh --image core --clean engine -DCMAKE_BUILD_TYPE=Release
```

**切换 core ↔ dev ↔ others 镜像：必须清 conan2**

`conan2-{arch}/` 被三种镜像共享，但 Conan profile 缓存了 `compiler=gcc` + `compiler.version`。不同镜像的 GCC 版本不同（core=7, dev=9, others=14），旧 profile 和已编译的包会导致 ABI 不兼容。

```bash
rm -rf ~/cache/tsdb-builder/conan2-amd64
./build.sh --image others --clean others-all
```

**切换镜像版本（不同 GCC）：必须清 conan2**

如果新版本镜像升级了 GCC，同样需要清除 Conan 缓存以避免编译器版本不匹配。

```bash
rm -rf ~/cache/tsdb-builder/conan2-amd64
./build.sh --image core:0.3.0 --clean engine
```

## 注意事项

- **离线安装包存放于 `$HOME/packages`**（可用 `--packages` 覆盖）：使用 `--mount=type=bind` 在构建时挂载，不写入镜像层
- **镜像发布固定走 Harbor**：`build-core-image.sh` / `build-dev-image.sh` / `build-others-image.sh` 要求显式传 `--version`，并将镜像推送到 `harbor.tdengine.net/tsdb-builder/{core|dev|others}`。如推送失败，请先执行 `docker login harbor.tdengine.net`
- **`build.sh` 解析单架构 Harbor tag**：`--image core|dev|others|core:<version>|dev:<version>|others:<version>` 最终都会按 `--arch` 解析成精确 tag；默认本地优先，缺失时自动 `docker pull`，传 `--pull-image` 可强制拉取
- **构建缓存存放于仓库外**：所有工具的下载缓存默认存放在 `$HOME/cache/tsdb-builder/`，可通过 `--cache` 参数或 `TSDB_CACHE_DIR` 环境变量覆盖。缓存与源码仓库完全分离，`git clean` / 重新 clone 不会丢失缓存

  | 子目录 | 缓存内容 | 适用镜像 |
  |---|---|---|
  | `conan2-{arch}/` | C/C++ Conan 依赖 | core + dev + others |
  | `externals-core-{arch}/` | CMake ExternalProject（core 镜像） | core |
  | `externals-dev-{arch}/` | CMake ExternalProject（dev 镜像） | dev |
  | `externals-others-{arch}/` | CMake ExternalProject（others 镜像） | others |
  | `go-mod/` | Go 模块下载 | core + dev + others |
  | `cargo-registry/` | Rust crate 缓存 | core + dev + others |
  | `cargo-git/` | Rust git 依赖 | core + dev + others |
  | `pnpm-store/` | Node.js pnpm 包 | others |
  | `m2-repository/` | Maven 依赖 | others |
  | `nuget/` | .NET NuGet 包 | others |
  | `ccache-{image}-{arch}/` | ccache 编译缓存 | core + dev + others |
- **`BUILD_CONTRIB` / `BUILD_ROCKSDB` / `ROCKSDB_USE_DEPS`**：控制外部依赖和 RocksDB 的编译方式，详见文末 [RocksDB 编译选项](#rocksdb-编译选项) 章节。
- **`EXTERNALS_USE_CCACHE`**：控制 ExternalProject 构建是否使用 ccache（默认 `ON`）。若 others 镜像 arm64 出现外部依赖 `.o` 文件损坏，设为 `OFF` 可规避，详见 [ExternalProject ccache 开关](#externalproject-ccache-开关)。
- **Conan profile 自动修正**：每次 `build.sh` 启动容器后，会自动修正 `/root/.conan2/profiles/default` 中的两项设置：`compiler.cppstd=gnu14` → `gnu17`；`arch` 修正为容器实际架构（`aarch64` → `armv8`，`x86_64` → `x86_64`）。这可防止 Conan profile 在跨机器迁移或初次检测错误时注入错误的架构标志（如 `-m64`）导致编译失败。若出现此类问题，删除 `$TSDB_CACHE_DIR/conan2-{arch}/` 目录后重新构建即可。
- **others 镜像 GCC 14 编译告警**：others 镜像使用 GCC 14（AlmaLinux 8），比 core 镜像的 GCC 7 更严格。在 others 镜像中编译含 core 组件（如 `BUILD_TOOLS=ON`）时，GCC 14 可能将 `stringop-overflow` 的误报升级为编译错误，需追加以下 cmake 参数规避：

  ```bash
  -DCMAKE_C_FLAGS="-Wno-error=stringop-overflow"
  ```

- **AMD64 / ARM64 JDK 版本不同**：`jdk-8u144-linux-x64.tar.gz`（AMD64）与 `jdk-8u441-linux-aarch64.tar.gz`（ARM64），Dockerfile.others Stage 1 已自动处理
- **构建日志自动写入文件**：`build.sh` 在启动时将全部输出（包括 cmake/make 内容）同步写入 `<src>/build.log`，日志头尾包含时间戳，结束时打印总耗时，方便排查耗时和失败原因。
- **打印完整 `docker run` 命令**：每次执行前脚本会将实际调用的 `docker run` 命令逐参数输出（每个参数单独一行，使用 `printf '%q'` shell 转义），可直接从日志中复制运行，便于手动复现问题。
- **others 组件可能依赖 core 先构建**：部分 connector 需要 taosd client 头文件/库，若报缺少依赖，请先运行 `./build.sh --image core core-all`
- **需要 `docker buildx`**：Docker Desktop 自带，Linux 裸机需确认 Docker 版本 >= 20.10（riscv64 构建除外，使用原生 `docker build`）
- **riscv64 注意事项**：riscv64 core 镜像基于 Debian trixie（glibc 2.41+），与 amd64/arm64 的 glibc 2.17 基线不同。riscv64 构建产物仅保证在现代 riscv64 发行版上运行。riscv64 为独立 tag，不参与 amd64/arm64 的多架构 manifest
- **riscv64 链接器限制**：riscv64 镜像中 mold **不是**默认链接器（GNU ld 保持默认）。原因是 mold 链接 Go CGO 二进制时会破坏 Go runtime 的 pclntab 数据结构，导致 taosadapter、keeper 等 Go 组件启动时 SIGSEGV coredump。`build.sh` 在 riscv64 架构下自动通过 `-DCMAKE_LINKER=mold` 让 cmake 管理的 C/C++ 目标使用 mold 加速，而 Go CGO 组件则自动使用系统默认的 GNU ld

## ccache（编译缓存）

所有构建镜像（core / dev / others / riscv64）均内置 ccache。ccache 透明地缓存 C/C++ 编译产物（`.o` 文件），显著加速增量编译和分支切换场景。

缓存按**镜像 + 架构**隔离（命名规则 `ccache-{image}-{arch}`，如 `ccache-core-amd64`、`ccache-dev-arm64`），与 `.externals/` 隔离策略一致。Debug 和 Release 构建共享同一缓存目录——ccache 的 hash key 包含完整编译器选项（`-O0 -g3` vs `-O3`），不同构建类型不会产生缓存冲突。

环境变量：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `CCACHE_MAXSIZE` | `20G` | 单个缓存目录最大容量 |
| `CCACHE_REMOTE_STORAGE` | 未设置 | 共享缓存后端（可选，适用于 CI 场景） |

共享缓存示例：

```bash
# NFS 方式
export CCACHE_REMOTE_STORAGE="file:///nfs/shared-ccache/core-amd64/"

# HTTP 方式（需部署 ccache 兼容的 HTTP 服务）
export CCACHE_REMOTE_STORAGE="http://<host>:<port>"

./build.sh --image core engine
```

每次构建结束后，`build.sh` 自动输出 ccache 统计信息（命中率、缓存大小等）。

### ExternalProject ccache 开关

cmake 选项 `EXTERNALS_USE_CCACHE` 控制 ExternalProject（OpenSSL、curl、jemalloc 等外部依赖）是否使用 ccache：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `EXTERNALS_USE_CCACHE` | `ON` | `ON` = 外部依赖正常使用 ccache；`OFF` = 外部依赖编译时设置 `CCACHE_DISABLE=1`，跳过 ccache |

> **背景**：ccache 与 gcc-toolset-14（others 镜像）在 arm64 上存在兼容问题，可能导致部分 `.o` 文件损坏（如 OpenSSL 的 `cipher_aria.o` 变为全零），表现为链接阶段 `undefined symbol: ossl_aria*` 错误。该选项仅影响 ExternalProject 构建，**不影响** TSDB 主工程自身的 ccache 行为。

```bash
# 遇到 ExternalProject 编译产物损坏时，关闭外部依赖的 ccache
./build.sh --image others engine -DBUILD_CONTRIB=ON -DEXTERNALS_USE_CCACHE=OFF

# 问题解决、缓存重建后，恢复默认（ON）即可
./build.sh --image others engine -DBUILD_CONTRIB=ON
```

## 外部依赖镜像加速

所有 ExternalProject 依赖的源码下载可通过 cmake 选项 `BUILD_DEPS_MIRROR_URL` 重定向到内网镜像（GitLab Generic Package Registry），避免从 GitHub 下载：

```bash
./build.sh --image core engine -DBUILD_CONTRIB=ON \
    -DBUILD_DEPS_MIRROR_URL="https://git.tdengine.net/api/v4/projects/70/packages/generic/externals/latest"
```

Package Registry 位于 Public 仓库，无需认证即可下载。

未设置 `BUILD_DEPS_MIRROR_URL` 时，cmake 使用原始上游 URL（GitHub）。这是外部贡献者和无内网 GitLab 访问权限环境的默认行为。

镜像托管方案详情参见 `docs/superpowers/specs/2026-05-14-cpp-cache-strategy-design.md`。

## RocksDB 编译选项

`build.sh` 透传三个 cmake 参数来控制 RocksDB 及外部依赖的编译方式：

| 参数 | 说明 |
|---|---|
| `BUILD_CONTRIB` | 外部依赖总开关。`ON` = 下载并编译（ExternalProject）；`OFF` = 复用预构建产物 |
| `BUILD_ROCKSDB` | RocksDB 编译开关。`ON` = 通过 ExternalProject 从源码编译 RocksDB |
| `ROCKSDB_USE_DEPS` | RocksDB 是否从 `deps/` 目录获取预编译产物（而非 `.externals/` 缓存） |

### 平台默认值

| 平台 | `BUILD_CONTRIB` | `BUILD_ROCKSDB` | `ROCKSDB_USE_DEPS` | 行为 |
|---|---|---|---|---|
| Linux | `OFF` | `OFF` | `ON` | ✅ 从 `deps/` 取预编译产物 |
| 非 Linux | `ON` | `ON` | `OFF` | ✅ ExternalProject 下载+编译 |

### 完整组合推演

| `BUILD_CONTRIB` | `BUILD_ROCKSDB` | `ROCKSDB_USE_DEPS` | 最终行为 |
|---|---|---|---|
| `ON` | `ON` | 忽略 | ExternalProject 下载+编译 RocksDB |
| `ON` | `OFF` | `ON` | 其他组件编译，RocksDB 从 `deps/` 取 |
| `ON` | `OFF` | `OFF` | 其他组件编译，RocksDB 从 `.externals/` 缓存取 |
| `OFF` | `OFF` | `ON` | 全部从预构建取，RocksDB 从 `deps/` 取 |
| `OFF` | `OFF` | `OFF` | 全部从预构建取，RocksDB 从 `.externals/` 缓存取 |
| `OFF` | `ON` | `*` | **fatal_error**（不允许 CONTRIB=OFF 时编译 RocksDB） |

### 常用场景

| 场景 | 命令 |
|---|---|
| 用 GCC 7 从源码编译 RocksDB | `-DBUILD_CONTRIB=ON -DBUILD_ROCKSDB=ON` |
| 用 `deps/` 预编译产物（Linux 默认） | `-DBUILD_CONTRIB=OFF`（默认 `ROCKSDB_USE_DEPS=ON`） |
| 用 `.externals/` 缓存 | `-DBUILD_CONTRIB=OFF -DROCKSDB_USE_DEPS=OFF` |
| 非法组合（会 FATAL_ERROR） | `-DBUILD_CONTRIB=OFF -DBUILD_ROCKSDB=ON` |

> **注意**：缺少预编译文件时，现在会直接 `FATAL_ERROR` 报错，不再是静默的 `"No rule to make target"` 错误。
