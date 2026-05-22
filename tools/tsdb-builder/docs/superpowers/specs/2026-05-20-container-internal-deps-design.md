# 容器编译内网依赖补全设计

> **日期**：2026-05-20
> **状态**：Draft
> **关联目标**：所有外部依赖切换到内网下载，tarball 优先，至少保证通过内网代理下载

---

## 1. 背景与原则

TSDB 构建系统已将 C/C++（ExternalProject + Conan）、Rust（cargo registry）、
Go 的依赖切换到内网源。但全面审计发现容器编译路径中仍有多处直接访问公网
（含国内公共镜像站），不符合"所有外部依赖走内网"的目标。

**核心原则**：
1. **只有涉及开发编译过程的从外网直接下载行为需要杜绝**，Docker 镜像构建不在此列
   （镜像构建频率低，属于基础设施运维范畴）。
2. 即便是国内公共镜像（阿里云 PyPI、rsproxy.cn），只要不在公司内网，
   开发者在办公网络中也可能受限或不稳定，应统一走内网代理/镜像。

### 1.1 公网访问审计清单

#### A. 容器编译时（build.sh → docker run 内）— 本次修复范围

| # | 组件 | 公网地址 | 影响的构建目标 |
|---|------|---------|---------------|
| 1 | npm/pnpm/yarn | `registry.npmjs.org` | connector-node, explorer-ui, insight |
| 2 | Maven | `repo1.maven.org` | connector-jdbc |
| 3 | NuGet | `api.nuget.org` | connector-dotnet |
| 4 | PyPI/pip | `mirrors.aliyun.com`（阿里云，非内网） | connector-python, maturin |
| 5 | sccache fallback | `github.com/mozilla/sccache`（build.sh 中容器运行时下载） | `--sccache` 启用且镜像中未预装时 |

#### B. Docker 镜像构建时（Dockerfile）— 不在本次范围

以下仅在镜像构建时访问公网，不影响开发编译过程，记录备查：

| # | 组件 | 公网地址 | 说明 |
|---|------|---------|------|
| 6 | rustup 工具链 | `rsproxy.cn` | 安装 Rust 到镜像 |
| 7 | Go 安装包 | `go.dev` | 安装 Go 到镜像 |
| 8 | ccache/uv/tini/protoc/mold/bison | `github.com`、`astral.sh`、`ftp.gnu.org` | 工具安装到镜像 |

## 2. 设计目标

- 消除容器编译过程中（A 类）所有公网/非内网访问
- 新增镜像 URL 集中到 `.build-args`，与 Go/Cargo/Conan 保持一致
- setup 脚本中的硬编码 URL 统一改为从 `.build-args` 读取

## 3. 设计决策

### 3.1 注入策略

| 类别 | 策略 | 理由 |
|------|------|------|
| npm/Maven/NuGet 镜像 | `build.sh` 运行时注入 | 与 Go Proxy / Conan remote 处理方式一致；修改 URL 无需重建镜像 |
| PyPI 镜像 | `.build-args` 改值 + Dockerfile 烘焙 | 已有 `PYPI_MIRROR` 变量和 Dockerfile 逻辑，改值即可 |
| sccache fallback | `build.sh` 中 fallback URL 改为内网 | 镜像已预装 sccache，fallback 仅为安全网 |

### 3.2 URL 集中管理

`.build-args` 变更：

```bash
# 修改现有值（阿里云 → 内网 Nora）
PYPI_MIRROR=https://nora.tdengine.net/simple/
PYPI_TRUSTED_HOST=nora.tdengine.net

# 新增
NPM_REGISTRY_URL=https://nora.tdengine.net/npm/
MAVEN_MIRROR_URL=https://nexus.tdengine.net/repository/maven-public/
NUGET_SOURCE_URL=https://nora.tdengine.net/nuget/v3/index.json
```

`build.sh` 读取方式与现有 `GO_PROXY` 一致：从 `.build-args` 读取，有 fallback 默认值。

`tools/setup/config.sh` 同步读取这些变量，使宿主机 setup 脚本与容器编译使用
相同的镜像源，消除 `modules/*.sh` 中的硬编码 URL。

## 4. 详细方案

### 4.1 npm/pnpm/yarn 内网 registry

**修改文件**：`.build-args`、`build.sh`、`config.sh`、`modules/node.sh`

**`.build-args`** 新增：
```
NPM_REGISTRY_URL=https://nora.tdengine.net/npm/
```

**`build.sh`** — 读取 + 注入（位于 CONTAINER_SCRIPT 中，Conan remote 配置之后）：
```bash
# 读取（与 GO_PROXY 同模式）
NPM_REGISTRY_URL=""
if [[ -f "${SCRIPT_DIR}/.build-args" ]]; then
    NPM_REGISTRY_URL="$(grep -E '^NPM_REGISTRY_URL=' "${SCRIPT_DIR}/.build-args" | cut -d= -f2-)"
fi
NPM_REGISTRY_URL="${NPM_REGISTRY_URL:-https://nora.tdengine.net/npm/}"

# 注入到 CONTAINER_SCRIPT（仅 others 镜像有 npm）
if command -v npm >/dev/null 2>&1; then
    npm config set registry '${NPM_REGISTRY_URL}'
fi
```

pnpm 和 yarn 默认继承 npm 的 registry 配置，无需额外设置。

**`config.sh`** — 读取 `NPM_REGISTRY_URL`：
```bash
NPM_REGISTRY_URL=$(grep '^NPM_REGISTRY_URL=' "$_ba" | cut -d= -f2-)
NPM_REGISTRY_URL="${NPM_REGISTRY_URL:-https://nora.tdengine.net/npm/}"
```

**`modules/node.sh`** — `mod_node_config()` 中用 `$NPM_REGISTRY_URL` 替换硬编码值。

### 4.2 Maven 内网镜像

**修改文件**：`.build-args`、`build.sh`、`config.sh`、`modules/java.sh`

**`.build-args`** 新增：
```
MAVEN_MIRROR_URL=https://nexus.tdengine.net/repository/maven-public/
```

**`build.sh`** — 读取 + 注入（CONTAINER_SCRIPT 中）：
```bash
# 注入到 CONTAINER_SCRIPT（仅 others 镜像有 Maven）
if command -v mvn >/dev/null 2>&1 && [ ! -f /root/.m2/settings.xml ]; then
    mkdir -p /root/.m2
    cat > /root/.m2/settings.xml << MAVEN_EOF
<settings>
  <mirrors>
    <mirror>
      <id>nexus-internal</id>
      <mirrorOf>*</mirrorOf>
      <url>${MAVEN_MIRROR_URL}</url>
    </mirror>
  </mirrors>
</settings>
MAVEN_EOF
fi
```

注意：`settings.xml` 位于 `/root/.m2/` 下，而缓存挂载的是 `/root/.m2/repository/`，
两者不冲突。容器是临时的，每次运行都会重新生成 `settings.xml`。

### 4.3 NuGet 内网 source

**修改文件**：`.build-args`、`build.sh`、`config.sh`、`modules/dotnet.sh`

**`.build-args`** 新增：
```
NUGET_SOURCE_URL=https://nora.tdengine.net/nuget/v3/index.json
```

**`build.sh`** — 注入到 CONTAINER_SCRIPT：
```bash
if command -v dotnet >/dev/null 2>&1; then
    dotnet nuget add source '${NUGET_SOURCE_URL}' --name tdengine-internal 2>/dev/null || true
fi
```

### 4.4 PyPI 从阿里云改为内网 Nora

**修改文件**：`.build-args`

**变更**（改值，不改结构）：
```bash
# Before
PYPI_MIRROR=http://mirrors.aliyun.com/pypi/simple/
PYPI_TRUSTED_HOST=mirrors.aliyun.com

# After
PYPI_MIRROR=https://nora.tdengine.net/simple/
PYPI_TRUSTED_HOST=nora.tdengine.net
```

Dockerfile 中已有 `pip3 config set global.index-url ${PYPI_MIRROR}`，
改变量值即可，无需修改 Dockerfile 逻辑。

`modules/python.sh` 中的硬编码值也统一改为从 `config.sh` 读取 `PYPI_MIRROR`。

### 4.5 sccache fallback URL 改为内网

**修改文件**：`build.sh`

`build.sh` 的 CONTAINER_SCRIPT 中有 sccache fallback 下载逻辑（当镜像中未预装时
从 GitHub 下载）。将 fallback URL 改为内网 GitLab Package Registry：

```bash
# Before
_sccache_url="https://github.com/mozilla/sccache/releases/download/${_sccache_ver}/${_sccache_tar}.tar.gz"

# After
_sccache_url="${DEPS_MIRROR_URL}/sccache-${_sccache_ver}-${_sccache_arch}-unknown-linux-musl.tar.gz"
```

需将 sccache tarball 上传到 GitLab Package Registry（通过
`prepare-externals.sh --add` 或手动上传）。

## 5. 变更文件清单

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `tools/tsdb-builder/.build-args` | 修改 | 改 `PYPI_MIRROR`/`PYPI_TRUSTED_HOST`；新增 `NPM_REGISTRY_URL`、`MAVEN_MIRROR_URL`、`NUGET_SOURCE_URL` |
| `tools/tsdb-builder/build.sh` | 修改 | 读取新变量 + CONTAINER_SCRIPT 注入 npm/maven/nuget 配置 + sccache fallback URL 改内网 |
| `tools/setup/config.sh` | 修改 | 读取 `NPM_REGISTRY_URL`、`MAVEN_MIRROR_URL`、`NUGET_SOURCE_URL`、`PYPI_MIRROR` |
| `tools/setup/modules/node.sh` | 修改 | 用 `$NPM_REGISTRY_URL` 替换硬编码 |
| `tools/setup/modules/java.sh` | 修改 | 用 `$MAVEN_MIRROR_URL` 替换硬编码 |
| `tools/setup/modules/dotnet.sh` | 修改 | 用 `$NUGET_SOURCE_URL` 替换硬编码 |
| `tools/setup/modules/python.sh` | 修改 | 用 `$PYPI_MIRROR` 替换硬编码 |
| `docs/build-optimization-guide.md` | 修改 | 更新各语言状态标记 |

## 6. 验证方案

1. **npm**：容器内运行 `npm config get registry`，确认输出为内网 URL
2. **Maven**：容器内运行 `mvn help:effective-settings`，确认 mirror 指向内网
3. **NuGet**：容器内运行 `dotnet nuget list source`，确认包含内网 source
4. **PyPI**：容器内运行 `pip3 config get global.index-url`，确认输出为 Nora URL
5. **sccache**：确认 build.sh 中 fallback URL 指向内网
6. **端到端**：others 镜像中运行完整编译，确认无公网访问

## 7. 前置条件

实施前需确认以下内网服务已就绪：

| 服务 | URL | 状态 |
|------|-----|------|
| Nora npm 镜像 | `https://nora.tdengine.net/npm/` | 需确认 |
| Nexus Maven 仓库 | `https://nexus.tdengine.net/repository/maven-public/` | 需确认 |
| Nora NuGet 镜像 | `https://nora.tdengine.net/nuget/v3/index.json` | 需确认 |
| Nora PyPI 镜像 | `https://nora.tdengine.net/simple/` | 需确认 |

如某服务尚未部署，对应项可暂保留现有公网镜像源，但 `.build-args` 中应预留变量
以便部署后切换。
