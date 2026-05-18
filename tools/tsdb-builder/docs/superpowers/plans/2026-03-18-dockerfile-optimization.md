# Dockerfile 优化实施计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 tsdb-builder Dockerfile 从 CentOS 7 迁移到 manylinux2014，融合多阶段构建、mold 链接器、protoc 等优势，支持 amd64/arm64 双架构。

**Architecture:** 采用两阶段构建（Alpine builder 处理架构变量 + manylinux2014 main 安装工具链），9 层平衡策略，保留所有现有工具（Go/Maven/CMake/JDK/Rust/Python/uv/.NET），新增 mold/protoc/tini。Python 和 OpenSSL 直接使用 manylinux2014 预装版本。

**Tech Stack:** Docker multi-stage build, manylinux2014 (quay.io/pypa), devtoolset-10 (GCC 10.x), mold linker, protoc, tini, Python 3.12 (pre-installed)

**参考文档：**
- 设计规范：`docs/superpowers/specs/2026-03-18-dockerfile-optimization-design.md`
- 参考实现：`Dockerfile-taosx-manylinux2024`
- 现有实现：`Dockerfile`
- 会话记录：`docs/2026-03-19-dockerfile-optimization-session.md`

---

## 实施状态：✅ 已完成（2026-03-19）

---

## 文件结构

**已完成的文件变更：**
- `Dockerfile` ✅ — 新 manylinux2014 多阶段构建版本（9 层）
- `Dockerfile.centos7.backup` ✅ — 旧 CentOS 7 版本备份
- `verify-image.sh` ✅ — 新增 mold/protoc/tini 验证，GCC 版本检测升级
- `build.sh` ✅ — 改用 `docker buildx --platform`，新增 `build-all` 命令

**依赖文件（实际使用）：**
- `installers/.cargo/config.toml` — Cargo 配置（非根目录 cargo.toml）
- `installers/` — Go/JDK/Maven/CMake tarball
- **不需要：** `cpanm`（已删除 Perl 层）、Python tarball（已改用预装版本）

---

## 实施过程中的关键修正

### 修正 1：manylinux2014 镜像地址

**原计划：** `docker.1ms.run/lukewiwa/manylinux2014`
**实际使用：** 原地址已失效（404）。改用官方分架构镜像：
```dockerfile
FROM --platform=$BUILDPLATFORM quay.io/pypa/manylinux2014_x86_64 AS stage2-amd64
FROM --platform=$BUILDPLATFORM quay.io/pypa/manylinux2014_aarch64 AS stage2-arm64
FROM stage2-${TARGETARCH}
```

### 修正 2：Perl/cpanm 依赖

**原计划：** 需要 `cpanm` 和 `cargo.toml` 文件，安装 Perl 模块用于 OpenSSL 编译
**实际：** 这两个文件不存在。改为：
- 移除全部 Perl 模块安装
- OpenSSL 层随后也完全删除

### 修正 3：arm64 yum 镜像源

**原计划：** `https://mirrors.aliyun.com/altarch/7/`
**实际：** 阿里云已停止提供 arm64 CentOS 7 镜像（404）。改用：
```bash
http://archive.kernel.org/centos-vault/altarch/7
```

### 修正 4：OpenSSL 编译参数

**原计划：** `./config ${OPENSSL_ARCH} shared`
**实际问题：** `./config` 自动检测平台后参数重复冲突（`target already defined`）。
**修正：** 改用 `./Configure`（大写 C）明确指定平台。
**后续：** OpenSSL 层在 Python 优化后完全删除，此修正已无关。

### 修正 5：JDK 路径硬编码

**原计划：** `ENV JAVA_HOME=/usr/local/jdk1.8.0_441`
**实际问题：** amd64 使用 8u144，目录名为 `jdk1.8.0_144`，导致 java 命令找不到。
**修正：** 动态检测 + 符号链接：
```bash
JDK_DIR=$(ls /usr/local | grep 'jdk1.8' | head -1)
ln -sf /usr/local/${JDK_DIR} /usr/local/jdk
ENV JAVA_HOME=/usr/local/jdk
```

### 修正 6：.NET 下载 URL

**原计划：** 硬编码 Microsoft CDN URL
**实际：** URL 已失效（400 Bad Request）。改用官方安装脚本：
```bash
wget https://dot.net/v1/dotnet-install.sh && ./dotnet-install.sh --version ${DOTNET_VERSION}
```

### 修正 7：Python 改用预装版本（重大优化）

**原计划：** 源码编译 Python 3.10.13（约 10 分钟）+ 编译 OpenSSL 1.1.1w（约 3 分钟）
**实际：** manylinux2014 预装了 Python 3.8–3.14，直接符号链接即可：
```dockerfile
ARG PYTHON_VERSION=3.12
RUN PY_TAG=$(echo ${PYTHON_VERSION} | tr -d '.') && \
    ln -sf /opt/python/cp${PY_TAG}-cp${PY_TAG}/bin/python3 /usr/local/bin/python3 && ...
```
**效果：**
- Python 3.10.13 → **Python 3.12.13**
- OpenSSL 1.1.1w → **OpenSSL 3.5.5**（随 manylinux Python 内置）
- 构建节省约 **13 分钟**

---

## 最终构建命令

```bash
# arm64
./build.sh build-arm64

# amd64（注意 JDK 版本覆盖）
./build.sh build-amd64
# 等同于：
DOCKER_BUILDKIT=1 docker buildx build --platform linux/amd64 --build-arg JDK_VERSION=8u144 \
  -f Dockerfile -t tsdb-builder:amd64 --load .

# 验证
./verify-image.sh arm64
./verify-image.sh amd64
```

---

## 最终验证结果

| 工具 | amd64 | arm64 |
|------|-------|-------|
| glibc | 2.17 ✅ | 2.17 ✅ |
| GCC | 10.2.1 ✅ | 10.2.1 ✅ |
| Python | 3.12.13 ✅ | 3.12.13 ✅ |
| SSL | OpenSSL 3.5.5 ✅ | OpenSSL 3.5.5 ✅ |
| Go | 1.23.4 ✅ | 1.23.4 ✅ |
| JDK | 1.8.0_144 ✅ | 1.8.0_441 ✅ |
| Maven | 3.8.4 ✅ | 3.8.4 ✅ |
| CMake | 3.21.5 ✅ | 3.21.5 ✅ |
| Rust | 1.90.0 ✅ | 1.90.0 ✅ |
| mold | 已安装 ✅ | 已安装 ✅ |
| protoc | 33.0 ✅ | 33.0 ✅ |
| tini | 0.19.0 ✅ | 0.19.0 ✅ |

---

## 回滚计划

```bash
cp Dockerfile.centos7.backup Dockerfile
```

---

## 注意事项

1. **Docker BuildKit：** 必须启用（`DOCKER_BUILDKIT=1` 或 `docker buildx`）
2. **amd64 JDK：** 必须传 `--build-arg JDK_VERSION=8u144`（installers/ 中只有 8u144）
3. **arm64 yum 源：** 使用 archive.kernel.org（较慢，约 20 分钟下载 yum 元数据）
4. **mold 在 QEMU 下：** amd64 镜像在 arm64 宿主机上运行时 mold 会 Segfault，这是 QEMU 限制，非 bug
5. **Python 版本切换：** 通过 `--build-arg PYTHON_VERSION=3.13` 可切换，无需改 Dockerfile


> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 tsdb-builder Dockerfile 从 CentOS 7 迁移到 manylinux2014，融合多阶段构建、mold 链接器、protoc 等优势，支持 amd64/arm64 双架构。

**Architecture:** 采用两阶段构建（Alpine builder 处理架构变量 + manylinux2014 main 安装工具链），11 层平衡策略，保留所有现有工具（Go/Maven/CMake/JDK/Rust/Python/uv/.NET/OpenSSL），新增 mold/protoc/tini。

**Tech Stack:** Docker multi-stage build, manylinux2014, devtoolset-10 (GCC 10.x), mold linker, protoc, tini

**参考文档：**
- 设计规范：`docs/superpowers/specs/2026-03-18-dockerfile-optimization-design.md`
- 参考实现：`Dockerfile-taosx-manylinux2024`
- 现有实现：`Dockerfile`

---

## 文件结构

**创建文件：**
- `Dockerfile.new` - 新的优化 Dockerfile

**修改文件：**
- `verify-image.sh` - 新增 mold/protoc/tini 验证

**依赖文件（必须存在）：**
- `cpanm` - Perl 包管理器
- `cargo.toml` - Cargo 配置文件
- `installers/` - 包含所有工具的 tarball

---

## Task 0: 前置检查

**Files:**
- Check: `cpanm`, `cargo.toml`, `installers/`

- [ ] **Step 1: 检查依赖文件**

```bash
cd /Users/xiaobo/tsdb-builder
test -f cpanm && echo "✓ cpanm exists" || echo "✗ cpanm missing"
test -f cargo.toml && echo "✓ cargo.toml exists" || echo "✗ cargo.toml missing"
test -d installers && echo "✓ installers/ exists" || echo "✗ installers/ missing"
```

Expected: 所有文件都存在

- [ ] **Step 2: 检查 installers 目录内容**

```bash
ls -lh installers/ | grep -E "(go|jdk|maven|cmake|python|dotnet|openssl)" | head -20
```

Expected: 看到必需的 tarball 文件

- [ ] **Step 3: 确认 Docker BuildKit 可用**

```bash
docker buildx version
```

Expected: 显示 buildx 版本信息

---

## Task 1: 创建新 Dockerfile 基础结构

**Files:**
- Create: `Dockerfile.new`

**说明：** 本任务创建完整的 Dockerfile.new，包含所有 11 层。执行者需要根据设计规范和参考文件编写。

- [ ] **Step 1: 创建 Dockerfile.new 的详细指导**

**必须包含的结构：**

1. **Stage 1 (Builder)** - 从 `Dockerfile-taosx-manylinux2024:1-22` 复制并修改：
   - 基础镜像：`FROM docker.1ms.run/alpine AS builder`
   - ARG: TARGETPLATFORM, TARGETARCH, TARGETVARIANT
   - 架构映射 case 语句，输出 8 个变量到 /etc/environment：
     * MOLD_ARCH, TINI_ARCH, PROTOC_ARCH, GO_ARCH, JDK_ARCH, CMAKE_ARCH, DOTNET_ARCH, OPENSSL_ARCH
   - linux/amd64 映射：x86_64, amd64, x86_64, amd64, x64, x86_64, x64, linux-x86_64
   - linux/arm64 映射：aarch64, arm64, aarch_64, arm64, aarch64, aarch64, arm64, linux-aarch64

2. **Stage 2 (Main) 头部** - 参考 `Dockerfile:13-64`：
   - 基础镜像：`FROM docker.1ms.run/lukewiwa/manylinux2014`（替换 centos:7）
   - WORKDIR /home
   - COPY --from=builder /etc/environment /etc/
   - 所有 ARG 定义（GO_VERSION=1.23.4, MAVEN_VERSION=3.8.4, 等）
   - ENV 定义（LANG, RUSTUP_UPDATE_ROOT=https://rsproxy.cn/rustup, RUSTUP_DIST_SERVER=https://rsproxy.cn）

3. **层 1: 系统基础** - 参考 `Dockerfile:73-110`：
   - yum 仓库配置（CentOS 7 base/updates/extras）
   - 安装基础包：git, wget, curl, unzip, tar, gzip, bzip2, libatomic, openssl-devel, zlib-devel, libffi-devel
   - 安装 Perl 依赖：perl-devel, perl-IPC-Cmd, perl-Test-Simple, perl-IO-Zlib, perl-ExtUtils-Manifest, perl-ExtUtils-MakeMaker
   - yum clean all

4. **层 2: Perl 模块** - 参考 `Dockerfile-taosx-manylinux2024:34-37`：
   - COPY ./cpanm /bin/
   - ENV CPAN_MIRROR, PERL_CPANM_OPT
   - cpanm --force --notest Term::Table Test::Simple Test::More List::Util Time::Piece

5. **层 3: OpenSSL** - 参考 `Dockerfile:112-125`：
   - 使用 RUN --mount=type=bind 从 installers/ 解压
   - ./config --prefix=/usr/local/openssl-1.1.1 ${OPENSSL_ARCH} shared
   - make && make install
   - 清理临时文件

6. **层 4: Go** - 参考 `Dockerfile:127-136`：
   - 使用 RUN --mount=type=bind 从 installers/ 解压到 /usr/local
   - 设置 GOROOT, GOPATH, GOPROXY
   - 更新 PATH

7. **层 5: JDK + Maven** - 参考 `Dockerfile:138-154`：
   - JDK 和 Maven 使用 RUN --mount=type=bind 解压
   - 设置 JAVA_HOME, M2_HOME
   - 更新 PATH

8. **层 6: CMake** - 参考 `Dockerfile:156-164`：
   - 使用 RUN --mount=type=bind 解压
   - 更新 PATH

9. **层 7: Rust** - 参考 `Dockerfile:166-177` + `Dockerfile-taosx-manylinux2024:39-44`：
   - ENV RUSTUP_UPDATE_ROOT, RUSTUP_DIST_SERVER（使用 rsproxy.cn）
   - curl rustup.rs 安装 Rust
   - 安装 clippy, rustfmt
   - COPY cargo.toml 到 /root/.cargo/config.toml

10. **层 8: Python** - 参考 `Dockerfile:179-201`：
    - 使用 RUN --mount=type=bind 编译 Python
    - ./configure --enable-shared --prefix=/usr/local
    - 配置 pip 镜像
    - 安装 uv
    - 安装 taospy, taos-ws-py

11. **层 9: .NET** - 参考 `Dockerfile:203-213`：
    - 使用 RUN --mount=type=bind 解压
    - 设置 DOTNET_ROOT
    - 更新 PATH

12. **层 10: 现代工具** - 参考 `Dockerfile-taosx-manylinux2024:47-61`：
    - 安装 tini：wget from GitHub releases, chmod +x, 放到 /bin/tini
    - 安装 mold：wget tar.gz, 解压, cp 到 /usr/bin/mold
    - 安装 protoc：wget zip, unzip 到 /usr/, chmod +x /usr/bin/protoc

13. **层 11: 环境配置** - 参考 `Dockerfile:215-266`：
    - 更新 PATH（整合所有工具）
    - 配置 LD_LIBRARY_PATH（OpenSSL）
    - git config --global --add safe.directory /app
    - SSH 配置
    - 时区配置
    - WORKDIR /app
    - ENTRYPOINT ["/bin/tini", "--"]

**执行命令：**
```bash
cd /Users/xiaobo/tsdb-builder
# 根据上述结构和参考文件，手动编写或使用编辑器创建 Dockerfile.new
# 确保所有 11 层都包含，架构变量正确使用
```

**成功标准：**
- Dockerfile.new 文件存在
- 包含 Stage 1 (builder) 和 Stage 2 (main)
- 所有 8 个架构变量在 Stage 1 中定义
- 所有 11 层按顺序实现
- 使用 manylinux2014 作为基础镜像
- ENTRYPOINT 使用 tini

- [ ] **Step 2: 验证 Dockerfile.new 语法**

```bash
cd /Users/xiaobo/tsdb-builder
docker buildx build --platform linux/amd64 --target builder -f Dockerfile.new -t test-syntax . 2>&1 | head -20
```

Expected:
- 输出包含 "Building for platform: linux/amd64"
- Stage 1 构建成功
- 无 Dockerfile 语法错误
- 可能因缺少 installers 文件失败，但这是预期的

- [ ] **Step 3: 提交初始版本**

```bash
cd /Users/xiaobo/tsdb-builder
git add Dockerfile.new
git commit -m "feat: create new Dockerfile with manylinux2014 and multi-stage build"
```

Expected: Git commit 成功，显示 "1 file changed"

---

## Task 2: 更新 verify-image.sh

**Files:**
- Modify: `verify-image.sh:80-218`

- [ ] **Step 1: 在 verify-image.sh 中添加 mold 验证**

在 Rust 验证部分之后（约第 160 行）插入以下代码：

```bash
# 使用 sed 或编辑器在 Rust 验证后添加
cat >> /tmp/mold-check.sh << 'MOLD_EOF'

# ============================================================================
# mold linker
# ============================================================================
print_header "Checking mold linker"

if command -v mold &> /dev/null; then
    MOLD_VER=$(mold --version 2>&1 | head -1)
    print_success "mold: ${MOLD_VER}"
else
    print_error "mold: NOT FOUND"
fi
MOLD_EOF

# 找到 Rust 验证结束位置，插入 mold 检查
# 手动编辑 verify-image.sh 或使用以下命令
```

Expected: 代码片段准备好，等待插入到 verify-image.sh

- [ ] **Step 2: 添加 protoc 验证**

```bash
cat >> /tmp/protoc-check.sh << 'PROTOC_EOF'

# ============================================================================
# protoc
# ============================================================================
print_header "Checking protoc"

if command -v protoc &> /dev/null; then
    PROTOC_VER=$(protoc --version 2>&1)
    print_success "protoc: ${PROTOC_VER}"
else
    print_error "protoc: NOT FOUND"
fi
PROTOC_EOF
```

Expected: protoc 验证代码准备好

- [ ] **Step 3: 添加 tini 验证**

```bash
cat >> /tmp/tini-check.sh << 'TINI_EOF'

# ============================================================================
# tini
# ============================================================================
print_header "Checking tini"

if test -f /bin/tini; then
    TINI_VER=$(tini --version 2>&1 | head -1)
    print_success "tini: ${TINI_VER}"
else
    print_error "tini: NOT FOUND at /bin/tini"
fi
TINI_EOF
```

Expected: tini 验证代码准备好

- [ ] **Step 4: 将验证代码插入 verify-image.sh**

手动编辑 verify-image.sh，在 .NET SDK 验证之前（约第 210 行）插入上述三段代码。

Expected:
- verify-image.sh 包含 mold/protoc/tini 验证
- 文件语法正确（bash -n verify-image.sh 无错误）

- [ ] **Step 5: 提交验证脚本更新**

```bash
cd /Users/xiaobo/tsdb-builder
git add verify-image.sh
git commit -m "feat: add mold/protoc/tini verification to verify-image.sh"
```

Expected: Git commit 成功，显示 "1 file changed"

---

## Task 3: 构建和测试 amd64 镜像

**Files:**
- Test: `Dockerfile.new`

- [ ] **Step 1: 构建 amd64 镜像**

```bash
cd /Users/xiaobo/tsdb-builder
DOCKER_BUILDKIT=1 docker buildx build \
  --platform linux/amd64 \
  -f Dockerfile.new \
  -t tsdb-builder:amd64-new \
  --load \
  .
```

Expected: 构建成功（可能需要 30-60 分钟）
- 输出最后显示 "Successfully tagged tsdb-builder:amd64-new"
- 无构建错误

- [ ] **Step 2: 运行验证脚本**

```bash
./verify-image.sh tsdb-builder:amd64-new
```

Expected:
- 所有验证通过
- 输出包含 "[PASS]" 标记
- 最后显示 "All verifications passed!"
- Exit code 0

- [ ] **Step 3: 手动验证关键工具**

```bash
docker run --rm tsdb-builder:amd64-new bash -c "
  echo '=== Architecture ==='
  uname -m
  echo '=== glibc ==='
  ldd --version | head -1
  echo '=== GCC ==='
  gcc --version | head -1
  echo '=== Go ==='
  go version
  echo '=== Rust ==='
  rustc --version
  echo '=== Python ==='
  python3 --version
  echo '=== mold ==='
  mold --version | head -1
  echo '=== protoc ==='
  protoc --version
  echo '=== tini ==='
  tini --version | head -1
"
```

Expected:
- Architecture: x86_64
- glibc: 2.17
- GCC: 10.x
- Go: 1.23.4
- Rust: 1.90.0
- Python: 3.10.13
- mold: 2.40.3
- protoc: 33.0
- tini: 0.19.0

- [ ] **Step 4: 记录测试结果**

```bash
cd /Users/xiaobo/tsdb-builder
echo "amd64 build and verification: PASSED" >> build-test-results.txt
git add build-test-results.txt
git commit -m "test: amd64 build verification passed"
```

Expected: Git commit 成功

---

## Task 4: 构建和测试 arm64 镜像

**Files:**
- Test: `Dockerfile.new`

- [ ] **Step 1: 构建 arm64 镜像**

```bash
cd /Users/xiaobo/tsdb-builder
DOCKER_BUILDKIT=1 docker buildx build \
  --platform linux/arm64 \
  -f Dockerfile.new \
  -t tsdb-builder:arm64-new \
  --load \
  .
```

Expected: 构建成功

- [ ] **Step 2: 运行验证脚本**

```bash
./verify-image.sh tsdb-builder:arm64-new
```

Expected: 所有验证通过

- [ ] **Step 3: 手动验证架构**

```bash
docker run --rm tsdb-builder:arm64-new uname -m
```

Expected: 输出 `aarch64`

- [ ] **Step 4: 记录测试结果**

```bash
cd /Users/xiaobo/tsdb-builder
echo "arm64 build and verification: PASSED" >> build-test-results.txt
git add build-test-results.txt
git commit -m "test: arm64 build verification passed"
```

---

## Task 5: 替换旧 Dockerfile

**Files:**
- Rename: `Dockerfile` -> `Dockerfile.centos7.backup`
- Rename: `Dockerfile.new` -> `Dockerfile`

- [ ] **Step 1: 备份旧 Dockerfile**

```bash
cd /Users/xiaobo/tsdb-builder
cp Dockerfile Dockerfile.centos7.backup
git add Dockerfile.centos7.backup
git commit -m "backup: save CentOS 7 Dockerfile as Dockerfile.centos7.backup"
```

- [ ] **Step 2: 启用新 Dockerfile**

```bash
cd /Users/xiaobo/tsdb-builder
mv Dockerfile.new Dockerfile
```

- [ ] **Step 3: 检查 build.sh 是否需要更新**

```bash
cd /Users/xiaobo/tsdb-builder
cat build.sh | grep -E "(Dockerfile|buildx|platform)" | head -10
```

如果 build.sh 使用了硬编码的架构参数，需要更新为支持 buildx

- [ ] **Step 4: 最终提交**

```bash
cd /Users/xiaobo/tsdb-builder
git add Dockerfile build.sh
git commit -m "feat: migrate to manylinux2014 with multi-stage build and modern tools

- Switch from CentOS 7 to manylinux2014 base image
- Add multi-stage build for architecture handling
- Add mold linker (2.40.3) for faster compilation
- Add protoc (33.0) for Protocol Buffers
- Add tini (v0.19.0) as init process
- Support amd64 and arm64 architectures
- Preserve all existing tools (Go/Maven/CMake/JDK/Rust/Python/uv/.NET/OpenSSL)
- Update verify-image.sh with new tool checks
"
```

---

## 验证清单

完成后验证以下项目（使用 verify-image.sh 和手动测试）：

**系统基础：**
- [ ] glibc 版本为 2.17: `ldd --version`
- [ ] GCC 版本 ≥ 9.3.1: `gcc --version`
- [ ] 支持 amd64: `docker run --rm tsdb-builder:amd64-new uname -m`
- [ ] 支持 arm64: `docker run --rm tsdb-builder:arm64-new uname -m`

**现有工具：**
- [ ] Go 1.23.4: `go version`
- [ ] Maven 3.8.4: `mvn --version`
- [ ] CMake 3.21.5: `cmake --version`
- [ ] JDK 8u441: `java -version`
- [ ] Rust 1.90.0: `rustc --version`
- [ ] Python 3.10.13: `python3 --version`
- [ ] uv: `uv --version`
- [ ] .NET 6.0.100: `dotnet --version`
- [ ] OpenSSL 1.1.1w: `/usr/local/openssl-1.1.1/bin/openssl version`

**新增工具：**
- [ ] mold 2.40.3: `mold --version`
- [ ] protoc 33.0: `protoc --version`
- [ ] tini v0.19.0: `tini --version`

**Python 包：**
- [ ] taospy: `python3 -c "import taos; print('taospy OK')"`
- [ ] taos-ws-py: `python3 -c "import taosws; print('taos-ws-py OK')"`

**完整验证：**
- [ ] verify-image.sh 全部通过: `./verify-image.sh`

---

## 回滚计划

如果新 Dockerfile 出现问题：

```bash
cd /Users/xiaobo/tsdb-builder
# 恢复旧 Dockerfile
cp Dockerfile.centos7.backup Dockerfile
git add Dockerfile
git commit -m "revert: restore CentOS 7 Dockerfile due to issues"
```

---

## 注意事项

1. **构建时间：** 首次构建 amd64 和 arm64 各需 30-60 分钟
2. **Docker BuildKit：** 必须启用（`DOCKER_BUILDKIT=1` 或 `docker buildx`）
3. **磁盘空间：** 确保至少 20GB 可用空间
4. **网络：** 需要稳定网络下载工具（已配置国内镜像）
5. **installers 目录：** 必须包含所有必需的 tarball 文件
6. **多架构：** 如果本地不支持 arm64，使用 QEMU 模拟或跳过 arm64 测试

---

## 实施策略

**推荐方式：** 使用 @superpowers:subagent-driven-development 执行此计划

**手动执行：** 按 Task 0 → Task 1 → Task 2 → Task 3 → Task 4 → Task 5 顺序执行

**关键检查点：**
1. Task 0 完成后：确认所有依赖文件存在
2. Task 1 完成后：确认 Dockerfile.new 语法正确
3. Task 3 完成后：确认 amd64 镜像可用
4. Task 4 完成后：确认 arm64 镜像可用
5. Task 5 完成后：确认新 Dockerfile 已启用

