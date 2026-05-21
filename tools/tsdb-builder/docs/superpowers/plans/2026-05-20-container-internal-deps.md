# 容器编译内网依赖补全 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消除容器编译（`build.sh → docker run`）过程中所有公网/非内网依赖下载，使 npm、Maven、NuGet、PyPI 全部走内网镜像，sccache fallback URL 改为内网。

**Architecture:** 在 `.build-args` 集中新增/修改镜像 URL 变量，`build.sh` 读取后通过 `CONTAINER_SCRIPT` 在容器启动时注入配置（npm registry、Maven settings.xml、NuGet source）；PyPI 改变量值即可（Dockerfile 已烘焙）；sccache fallback URL 指向内网 GitLab Package Registry。`tools/setup/config.sh` 同步读取新变量，消除 `modules/*.sh` 中的硬编码 URL。

**Tech Stack:** Bash (build.sh, config.sh, modules/*.sh), Docker, npm/pnpm, Maven, NuGet (.NET CLI), pip

**Spec:** `tools/tsdb-builder/docs/superpowers/specs/2026-05-20-container-internal-deps-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `tools/tsdb-builder/.build-args` | Modify | 新增 `NPM_REGISTRY_URL`、`MAVEN_MIRROR_URL`、`NUGET_SOURCE_URL`；修改 `PYPI_MIRROR`/`PYPI_TRUSTED_HOST` |
| `tools/tsdb-builder/build.sh` | Modify | 读取新变量 + CONTAINER_SCRIPT 注入 npm/maven/nuget 配置 + sccache fallback URL 改内网 |
| `tools/setup/config.sh` | Modify | 从 `.build-args` 读取 `NPM_REGISTRY_URL`、`MAVEN_MIRROR_URL`、`NUGET_SOURCE_URL`、`PYPI_MIRROR`/`PYPI_TRUSTED_HOST` |
| `tools/setup/modules/node.sh` | Modify | `mod_node_config()` 用 `$NPM_REGISTRY_URL` 替换硬编码 URL |
| `tools/setup/modules/java.sh` | Modify | `mod_java_config()` 用 `$MAVEN_MIRROR_URL` 替换硬编码 URL |
| `tools/setup/modules/dotnet.sh` | Modify | `mod_dotnet_config()` 用 `$NUGET_SOURCE_URL` 替换硬编码 URL |
| `tools/setup/modules/python.sh` | Modify | `mod_python_config()` 用 `$PYPI_MIRROR`/`$PYPI_TRUSTED_HOST` 替换硬编码 URL |
| `docs/build-optimization-guide.md` | Modify | 更新 npm/Maven/NuGet/PyPI 状态从 ⚠️ 改为 ✅ |

---

### Task 1: `.build-args` — 新增/修改镜像 URL 变量

**Files:**
- Modify: `tools/tsdb-builder/.build-args`

- [ ] **Step 1: 修改 `.build-args`**

在 `# Mirror Configuration (内网加速)` 区块中，修改 `PYPI_MIRROR` 和 `PYPI_TRUSTED_HOST`，并新增三个变量：

```bash
# Mirror Configuration (内网加速)
PYPI_MIRROR=https://nora.tdengine.net/simple/
PYPI_TRUSTED_HOST=nora.tdengine.net
GO_PROXY=https://nexus.tdengine.net/repository/goproxy/
CARGO_REGISTRY_URL=sparse+https://nora.tdengine.net/cargo/index/
CONAN_REMOTE_URL=https://nexus.tdengine.net/repository/conan/
NPM_REGISTRY_URL=https://nora.tdengine.net/npm/
MAVEN_MIRROR_URL=https://nexus.tdengine.net/repository/maven-public/
NUGET_SOURCE_URL=https://nora.tdengine.net/nuget/v3/index.json
```

具体变更：
- `PYPI_MIRROR`：从 `http://mirrors.aliyun.com/pypi/simple/` 改为 `https://nora.tdengine.net/simple/`
- `PYPI_TRUSTED_HOST`：从 `mirrors.aliyun.com` 改为 `nora.tdengine.net`
- 新增 `NPM_REGISTRY_URL=https://nora.tdengine.net/npm/`
- 新增 `MAVEN_MIRROR_URL=https://nexus.tdengine.net/repository/maven-public/`
- 新增 `NUGET_SOURCE_URL=https://nora.tdengine.net/nuget/v3/index.json`

- [ ] **Step 2: 验证格式**

```bash
grep -E '^(PYPI_|NPM_|MAVEN_|NUGET_)' tools/tsdb-builder/.build-args
```

预期输出：
```
PYPI_MIRROR=https://nora.tdengine.net/simple/
PYPI_TRUSTED_HOST=nora.tdengine.net
NPM_REGISTRY_URL=https://nora.tdengine.net/npm/
MAVEN_MIRROR_URL=https://nexus.tdengine.net/repository/maven-public/
NUGET_SOURCE_URL=https://nora.tdengine.net/nuget/v3/index.json
```

- [ ] **Step 3: Commit**

```bash
git add tools/tsdb-builder/.build-args
git commit -m "build: add npm/maven/nuget mirror URLs, change PyPI to internal Nora

- PYPI_MIRROR: aliyun → nora.tdengine.net
- New: NPM_REGISTRY_URL, MAVEN_MIRROR_URL, NUGET_SOURCE_URL
- All dev compilation deps now have internal mirror URLs in .build-args"
```

---

### Task 2: `build.sh` — 读取新变量并注入到 CONTAINER_SCRIPT

**Files:**
- Modify: `tools/tsdb-builder/build.sh`

- [ ] **Step 1: 在 build.sh 的变量读取区域新增 npm/maven/nuget 变量读取**

在 `build.sh` 中找到 DEPS_MIRROR_URL 读取块（约 line 440-448），在其之后添加三组新变量的读取逻辑，与 `GO_PROXY`/`CONAN_REMOTE_URL` 同模式：

```bash
# Read NPM_REGISTRY_URL from .build-args for container npm/pnpm registry injection.
NPM_REGISTRY_URL=""
if [[ -f "${SCRIPT_DIR}/.build-args" ]]; then
    NPM_REGISTRY_URL="$(grep -E '^NPM_REGISTRY_URL=' "${SCRIPT_DIR}/.build-args" | cut -d= -f2-)"
fi
NPM_REGISTRY_URL="${NPM_REGISTRY_URL:-https://nora.tdengine.net/npm/}"

# Read MAVEN_MIRROR_URL from .build-args for container Maven settings.xml injection.
MAVEN_MIRROR_URL=""
if [[ -f "${SCRIPT_DIR}/.build-args" ]]; then
    MAVEN_MIRROR_URL="$(grep -E '^MAVEN_MIRROR_URL=' "${SCRIPT_DIR}/.build-args" | cut -d= -f2-)"
fi
MAVEN_MIRROR_URL="${MAVEN_MIRROR_URL:-https://nexus.tdengine.net/repository/maven-public/}"

# Read NUGET_SOURCE_URL from .build-args for container NuGet source injection.
NUGET_SOURCE_URL=""
if [[ -f "${SCRIPT_DIR}/.build-args" ]]; then
    NUGET_SOURCE_URL="$(grep -E '^NUGET_SOURCE_URL=' "${SCRIPT_DIR}/.build-args" | cut -d= -f2-)"
fi
NUGET_SOURCE_URL="${NUGET_SOURCE_URL:-https://nora.tdengine.net/nuget/v3/index.json}"
```

- [ ] **Step 2: 在 CONTAINER_SCRIPT 中注入 npm registry 配置**

在 `CONTAINER_SCRIPT` 中，找到 Conan remote 配置块的结尾（`fi` after `conan remote add nexus`，约 line 628），在其之后、`export LIBRARY_PATH` 之前，添加 npm registry 注入：

```bash
# Configure npm/pnpm registry → internal mirror (others image only)
if command -v npm >/dev/null 2>&1; then
    npm config set registry '${NPM_REGISTRY_URL}' 2>/dev/null || true
    echo \"[INFO] npm registry set to '${NPM_REGISTRY_URL}'\"
fi
```

注意：这段代码在 `CONTAINER_SCRIPT` heredoc 内部，所以 `${NPM_REGISTRY_URL}` 不需要转义——它会在 heredoc 被 bash 解析时展开为宿主机上读取的值。

- [ ] **Step 3: 在 CONTAINER_SCRIPT 中注入 Maven settings.xml**

紧接 npm 配置之后添加：

```bash
# Configure Maven mirror → internal Nexus (others image only)
if command -v mvn >/dev/null 2>&1 && [ ! -f /root/.m2/settings.xml ]; then
    mkdir -p /root/.m2
    cat > /root/.m2/settings.xml << 'MAVEN_SETTINGS_EOF'
<?xml version="1.0" encoding="UTF-8"?>
<settings>
  <mirrors>
    <mirror>
      <id>nexus-internal</id>
      <mirrorOf>*</mirrorOf>
      <url>${MAVEN_MIRROR_URL}</url>
    </mirror>
  </mirrors>
</settings>
MAVEN_SETTINGS_EOF
    echo '[INFO] Maven settings.xml written with internal Nexus mirror'
fi
```

**关键注意**：`MAVEN_MIRROR_URL` 的注入方式需要特别小心。因为 settings.xml 是 XML 格式，且在 CONTAINER_SCRIPT heredoc 内部。有两种方式：
- 方式 A：用不转义的 heredoc 让 `${MAVEN_MIRROR_URL}` 在宿主机展开（推荐）
- 方式 B：用 `sed` 替换

推荐方式 A：`MAVEN_SETTINGS_EOF` 前面**不加引号**，让 shell 在构建 CONTAINER_SCRIPT 时展开 `${MAVEN_MIRROR_URL}`。但因为 CONTAINER_SCRIPT 本身是 heredoc，需要确认嵌套 heredoc 的处理。

实际上，`CONTAINER_SCRIPT` 使用双引号字符串（`CONTAINER_SCRIPT="..."`），所以 `${MAVEN_MIRROR_URL}` 会在 build.sh 赋值时被展开。因此 settings.xml 内容中直接写 `${MAVEN_MIRROR_URL}` 即可。但要注意 XML 中的特殊字符——URL 中没有 `&` 等 XML 特殊字符，所以安全。

然而，CONTAINER_SCRIPT 中的 `$` 大量使用 `\$` 来阻止宿主机展开。因此需要审查：在 CONTAINER_SCRIPT 中，需要宿主机展开的变量用 `${VAR}`（不转义），需要容器内展开的变量用 `\${VAR}`。

查看现有模式：`'${CONAN_REMOTE_URL}'` — 这里用了单引号包裹在 CONTAINER_SCRIPT 双引号字符串内，`${}` 不转义，会被宿主机 shell 展开。这是正确的模式。

所以 Maven settings.xml 的注入应该使用与 Conan 相同的模式：

```bash
# Configure Maven mirror → internal Nexus (others image only)
if command -v mvn >/dev/null 2>&1 && [ ! -f /root/.m2/settings.xml ]; then
    mkdir -p /root/.m2
    cat > /root/.m2/settings.xml << MAVEN_SETTINGS_EOF
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<settings>
  <mirrors>
    <mirror>
      <id>nexus-internal</id>
      <mirrorOf>*</mirrorOf>
      <url>${MAVEN_MIRROR_URL}</url>
    </mirror>
  </mirrors>
</settings>
MAVEN_SETTINGS_EOF
    echo '[INFO] Maven settings.xml written with internal Nexus mirror'
fi
```

注意双引号需要转义（`\"`）因为整个 CONTAINER_SCRIPT 是双引号字符串。heredoc 终止符 `MAVEN_SETTINGS_EOF` 不加引号，`${MAVEN_MIRROR_URL}` 会被宿主机 shell 展开。

- [ ] **Step 4: 在 CONTAINER_SCRIPT 中注入 NuGet source**

紧接 Maven 配置之后添加：

```bash
# Configure NuGet source → internal mirror (others image only)
if command -v dotnet >/dev/null 2>&1; then
    dotnet nuget add source '${NUGET_SOURCE_URL}' --name tdengine-internal 2>/dev/null || true
    echo '[INFO] NuGet source added: ${NUGET_SOURCE_URL}'
fi
```

`${NUGET_SOURCE_URL}` 在宿主机展开，与 `${CONAN_REMOTE_URL}` 模式一致。

- [ ] **Step 5: 修改 sccache fallback URL 为内网**

在 CONTAINER_SCRIPT 中找到 sccache fallback 下载块（约 line 570）：

```bash
_sccache_url=\"https://github.com/mozilla/sccache/releases/download/\${_sccache_ver}/\${_sccache_tar}.tar.gz\"
```

替换为：

```bash
_sccache_url=\"${DEPS_MIRROR_URL}/sccache-\${_sccache_ver}-\${_sccache_arch}-unknown-linux-musl.tar.gz\"
```

这里 `${DEPS_MIRROR_URL}` 在宿主机展开（指向 GitLab Package Registry），`\${_sccache_ver}` 和 `\${_sccache_arch}` 在容器内展开。

- [ ] **Step 6: 验证 build.sh 语法**

```bash
bash -n tools/tsdb-builder/build.sh
echo $?
```

预期：退出码 0，无语法错误。

- [ ] **Step 7: Commit**

```bash
git add tools/tsdb-builder/build.sh
git commit -m "build: inject npm/maven/nuget config into container, fix sccache URL

CONTAINER_SCRIPT now configures:
- npm registry → internal Nora mirror
- Maven settings.xml → internal Nexus mirror
- NuGet source → internal Nora mirror
- sccache fallback → internal GitLab Package Registry

All mirror URLs read from .build-args with sensible defaults."
```

---

### Task 3: `config.sh` — 读取新变量供宿主机 setup 脚本使用

**Files:**
- Modify: `tools/setup/config.sh`

- [ ] **Step 1: 在 config.sh 的 `.build-args` 读取块中新增变量**

在 `config.sh` 中找到读取 `CONAN_REMOTE_URL` 的行（line 18），在其之后添加：

```bash
    NPM_REGISTRY_URL=$(grep '^NPM_REGISTRY_URL=' "$_ba" | cut -d= -f2-)
    MAVEN_MIRROR_URL=$(grep '^MAVEN_MIRROR_URL=' "$_ba" | cut -d= -f2-)
    NUGET_SOURCE_URL=$(grep '^NUGET_SOURCE_URL=' "$_ba" | cut -d= -f2-)
    PYPI_MIRROR=$(grep '^PYPI_MIRROR=' "$_ba" | cut -d= -f2-)
    PYPI_TRUSTED_HOST=$(grep '^PYPI_TRUSTED_HOST=' "$_ba" | cut -d= -f2-)
```

- [ ] **Step 2: 在 fallback defaults 区块中新增对应 fallback**

在 `CONAN_REMOTE_URL` 的 fallback（line 24）之后添加：

```bash
NPM_REGISTRY_URL="${NPM_REGISTRY_URL:-https://nora.tdengine.net/npm/}"
MAVEN_MIRROR_URL="${MAVEN_MIRROR_URL:-https://nexus.tdengine.net/repository/maven-public/}"
NUGET_SOURCE_URL="${NUGET_SOURCE_URL:-https://nora.tdengine.net/nuget/v3/index.json}"
PYPI_MIRROR="${PYPI_MIRROR:-https://nora.tdengine.net/simple/}"
PYPI_TRUSTED_HOST="${PYPI_TRUSTED_HOST:-nora.tdengine.net}"
```

- [ ] **Step 3: 验证 config.sh 语法**

```bash
bash -n tools/setup/config.sh
echo $?
```

预期：退出码 0。

- [ ] **Step 4: Commit**

```bash
git add tools/setup/config.sh
git commit -m "build(setup): read npm/maven/nuget/pypi URLs from .build-args

config.sh now exports NPM_REGISTRY_URL, MAVEN_MIRROR_URL,
NUGET_SOURCE_URL, PYPI_MIRROR, PYPI_TRUSTED_HOST with fallback
defaults. Modules can reference these instead of hardcoding URLs."
```

---

### Task 4: `modules/node.sh` — 用变量替换硬编码 URL

**Files:**
- Modify: `tools/setup/modules/node.sh`

- [ ] **Step 1: 修改 `mod_node_config()` 使用 `$NPM_REGISTRY_URL`**

将 `mod_node_config()` 函数中的硬编码 URL 替换为从 `config.sh` 导出的变量：

```bash
mod_node_config() {
    # Configure npm/pnpm registry → internal mirror
    # NPM_REGISTRY_URL is set by config.sh from .build-args
    local nora_npm_url="${NPM_REGISTRY_URL:-https://nora.tdengine.net/npm/}"

    if cmd_exists npm; then
        local current_registry
        current_registry=$(npm config get registry 2>/dev/null)
        if [[ "$current_registry" == *"nora.tdengine.net"* ]] || \
           [[ "$current_registry" == *"nexus.tdengine.net"* ]]; then
            return 0
        fi

        if confirm "Set npm registry → internal mirror?"; then
            npm config set registry "$nora_npm_url"
            ok "npm registry set to $nora_npm_url"
            CHANGES_MADE=$((CHANGES_MADE + 1))
        fi
    fi
}
```

关键变更：`local nora_npm_url="https://nora.tdengine.net/npm/"` → `local nora_npm_url="${NPM_REGISTRY_URL:-https://nora.tdengine.net/npm/}"`

- [ ] **Step 2: 验证语法**

```bash
bash -n tools/setup/modules/node.sh
echo $?
```

- [ ] **Step 3: Commit**

```bash
git add tools/setup/modules/node.sh
git commit -m "build(setup): node.sh uses NPM_REGISTRY_URL from config"
```

---

### Task 5: `modules/java.sh` — 用变量替换硬编码 URL

**Files:**
- Modify: `tools/setup/modules/java.sh`

- [ ] **Step 1: 修改 `mod_java_config()` 使用 `$MAVEN_MIRROR_URL`**

```bash
mod_java_config() {
    # Maven settings.xml — configure internal Nexus mirror if available
    # MAVEN_MIRROR_URL is set by config.sh from .build-args
    local mvn_settings="$HOME/.m2/settings.xml"
    local nexus_maven_url="${MAVEN_MIRROR_URL:-https://nexus.tdengine.net/repository/maven-public/}"

    if [[ -f "$mvn_settings" ]] && grep -qF "nexus.tdengine.net" "$mvn_settings"; then
        return 0
    fi

    if confirm "Configure Maven mirror → internal Nexus in $mvn_settings?"; then
        mkdir -p "$HOME/.m2"
        backup_file "$mvn_settings"
        cat > "$mvn_settings" <<MVN_EOF
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.2.0
                              https://maven.apache.org/xsd/settings-1.2.0.xsd">
  <mirrors>
    <mirror>
      <id>nexus-tdengine</id>
      <mirrorOf>central</mirrorOf>
      <name>TDengine Internal Nexus</name>
      <url>${nexus_maven_url}</url>
    </mirror>
  </mirrors>
</settings>
MVN_EOF
        ok "Maven settings written to $mvn_settings"
        CHANGES_MADE=$((CHANGES_MADE + 1))
    fi
}
```

关键变更：`local nexus_maven_url="https://nexus.tdengine.net/repository/maven-public/"` → `local nexus_maven_url="${MAVEN_MIRROR_URL:-https://nexus.tdengine.net/repository/maven-public/}"`

- [ ] **Step 2: 验证语法**

```bash
bash -n tools/setup/modules/java.sh
echo $?
```

- [ ] **Step 3: Commit**

```bash
git add tools/setup/modules/java.sh
git commit -m "build(setup): java.sh uses MAVEN_MIRROR_URL from config"
```

---

### Task 6: `modules/dotnet.sh` — 用变量替换硬编码 URL

**Files:**
- Modify: `tools/setup/modules/dotnet.sh`

- [ ] **Step 1: 修改 `mod_dotnet_config()` 使用 `$NUGET_SOURCE_URL`**

```bash
mod_dotnet_config() {
    if ! cmd_exists dotnet; then
        return 0
    fi

    # NuGet source → internal mirror (if available)
    # NUGET_SOURCE_URL is set by config.sh from .build-args
    local nora_nuget_url="${NUGET_SOURCE_URL:-https://nora.tdengine.net/nuget/v3/index.json}"

    if dotnet nuget list source 2>/dev/null | grep -qF "nora.tdengine.net"; then
        return 0
    fi

    if confirm "Add internal NuGet source (Nora)?"; then
        dotnet nuget add source "$nora_nuget_url" \
            --name "tdengine-internal" 2>/dev/null || true
        ok "NuGet source added: $nora_nuget_url"
        CHANGES_MADE=$((CHANGES_MADE + 1))
    fi
}
```

关键变更：`local nora_nuget_url="https://nora.tdengine.net/nuget/v3/index.json"` → `local nora_nuget_url="${NUGET_SOURCE_URL:-https://nora.tdengine.net/nuget/v3/index.json}"`

- [ ] **Step 2: 验证语法**

```bash
bash -n tools/setup/modules/dotnet.sh
echo $?
```

- [ ] **Step 3: Commit**

```bash
git add tools/setup/modules/dotnet.sh
git commit -m "build(setup): dotnet.sh uses NUGET_SOURCE_URL from config"
```

---

### Task 7: `modules/python.sh` — 用变量替换硬编码 URL

**Files:**
- Modify: `tools/setup/modules/python.sh`

- [ ] **Step 1: 修改 `mod_python_config()` 使用 `$PYPI_MIRROR` 和 `$PYPI_TRUSTED_HOST`**

```bash
mod_python_config() {
    # Configure pip index → internal PyPI mirror
    # PYPI_MIRROR and PYPI_TRUSTED_HOST are set by config.sh from .build-args
    local nora_pypi_url="${PYPI_MIRROR:-https://nora.tdengine.net/simple/}"
    local trusted_host="${PYPI_TRUSTED_HOST:-nora.tdengine.net}"

    local current_index
    current_index=$(pip3 config get global.index-url 2>/dev/null || echo "")
    if [[ "$current_index" == *"nora.tdengine.net"* ]]; then
        return 0
    fi

    if confirm "Set pip index-url → internal PyPI mirror?"; then
        pip3 config set global.index-url "$nora_pypi_url" 2>/dev/null || \
            python3 -m pip config set global.index-url "$nora_pypi_url"
        pip3 config set global.trusted-host "$trusted_host" 2>/dev/null || \
            python3 -m pip config set global.trusted-host "$trusted_host"
        ok "pip index set to $nora_pypi_url"
        CHANGES_MADE=$((CHANGES_MADE + 1))
    fi
}
```

关键变更：
- `local nora_pypi_url="https://nora.tdengine.net/simple/"` → `local nora_pypi_url="${PYPI_MIRROR:-https://nora.tdengine.net/simple/}"`
- 新增 `local trusted_host="${PYPI_TRUSTED_HOST:-nora.tdengine.net}"` 用于 `trusted-host` 配置
- `"nora.tdengine.net"` 在 `pip3 config set global.trusted-host` 中替换为 `"$trusted_host"`

- [ ] **Step 2: 验证语法**

```bash
bash -n tools/setup/modules/python.sh
echo $?
```

- [ ] **Step 3: Commit**

```bash
git add tools/setup/modules/python.sh
git commit -m "build(setup): python.sh uses PYPI_MIRROR/PYPI_TRUSTED_HOST from config"
```

---

### Task 8: 更新 `docs/build-optimization-guide.md`

**Files:**
- Modify: `docs/build-optimization-guide.md`

- [ ] **Step 1: 更新数据流对比图（优化后）**

找到"优化后"数据流图（约 line 108-118），将 npm/Maven/NuGet 的 ⚠️ 行替换为 ✅：

将：
```
    ├── pnpm install ───────────→ ⚠️ 容器仍走公网 registry.npmjs.org（仅缓存挂载）
    ├── mvn install ────────────→ ⚠️ 容器仍走公网 repo1.maven.org（仅缓存挂载）
```

替换为：
```
    ├── pnpm install ───────────→ ✅ nora.tdengine.net/npm/（build.sh 注入）
    ├── mvn install ────────────→ ✅ nexus.tdengine.net/maven-public/（build.sh 注入 settings.xml）
```

同理更新 NuGet 和 PyPI 行。

- [ ] **Step 2: 更新语言对比总表（约 line 146-149）**

将：
```
| **Node.js** | npm 包 | ⚠️ 容器未配置（宿主机: Nora npm） | ...
| **Java** | Maven artifact | ⚠️ 容器未配置（宿主机: Nexus Maven） | ...
| **Python** | PyPI 包 | ✅ 阿里云 PyPI（容器）/ Nora PyPI（宿主机） | ...
| **.NET** | NuGet 包 | ⚠️ 容器未配置（宿主机: Nora NuGet） | ...
```

替换为：
```
| **Node.js** | npm 包 | ✅ Nora npm（`build.sh` 运行时注入） | ...
| **Java** | Maven artifact | ✅ Nexus Maven（`build.sh` 运行时注入 `settings.xml`） | ...
| **Python** | PyPI 包 | ✅ Nora PyPI（Dockerfile 烘焙） | ...
| **.NET** | NuGet 包 | ✅ Nora NuGet（`build.sh` 运行时注入） | ...
```

- [ ] **Step 3: 更新 4.6 Node.js 依赖章节**

将 `⚠️ **未配置**` 状态和底部的 `> **待改进**` 注释替换为已完成的说明：

```
| 内网源（容器编译） | ✅ `build.sh` 在 `CONTAINER_SCRIPT` 中执行 `npm config set registry`，URL 来自 `.build-args` 的 `NPM_REGISTRY_URL`（默认 `https://nora.tdengine.net/npm/`）。pnpm/yarn 自动继承 npm registry。 |
```

删除底部的 `> **待改进**` 注释。

- [ ] **Step 4: 更新 4.7 Java 依赖章节**

```
| 内网源（容器编译） | ✅ `build.sh` 在 `CONTAINER_SCRIPT` 中生成 `/root/.m2/settings.xml`，配置 Maven mirror 指向 `.build-args` 的 `MAVEN_MIRROR_URL`（默认 `https://nexus.tdengine.net/repository/maven-public/`）。与缓存挂载 `m2-repository/` 不冲突。 |
```

删除底部的 `> **待改进**` 注释。

- [ ] **Step 5: 更新 4.8 Python 依赖章节**

将阿里云相关说明替换为 Nora：

```
| 内网源（容器编译） | ✅ Dockerfile 烘焙 `pip3 config set global.index-url`，URL 来自 `.build-args` 的 `PYPI_MIRROR`（`https://nora.tdengine.net/simple/`） |
```

将底部的 `> **注意**` 替换为说明两端已统一使用 Nora。

- [ ] **Step 6: 更新 4.9 .NET 依赖章节**

```
| 内网源（容器编译） | ✅ `build.sh` 在 `CONTAINER_SCRIPT` 中执行 `dotnet nuget add source`，URL 来自 `.build-args` 的 `NUGET_SOURCE_URL`（默认 `https://nora.tdengine.net/nuget/v3/index.json`）。 |
```

删除底部的 `> **待改进**` 注释。

- [ ] **Step 7: Commit**

```bash
git add docs/build-optimization-guide.md
git commit -m "docs: update build guide — all language deps now use internal mirrors

npm/Maven/NuGet/PyPI all changed from ⚠️ to ✅ status."
```

---

### Task 9: 最终验证

- [ ] **Step 1: 验证所有修改文件的 bash 语法**

```bash
bash -n tools/tsdb-builder/build.sh && \
bash -n tools/setup/config.sh && \
bash -n tools/setup/modules/node.sh && \
bash -n tools/setup/modules/java.sh && \
bash -n tools/setup/modules/dotnet.sh && \
bash -n tools/setup/modules/python.sh && \
echo "All syntax checks passed"
```

预期输出：`All syntax checks passed`

- [ ] **Step 2: 验证 `.build-args` 变量完整性**

```bash
echo "=== Mirror URLs in .build-args ==="
grep -E '^(GO_PROXY|CARGO_|CONAN_|PYPI_|NPM_|MAVEN_|NUGET_)' tools/tsdb-builder/.build-args
echo ""
echo "=== Mirror URLs in config.sh (fallbacks) ==="
grep -E '(NPM_REGISTRY|MAVEN_MIRROR|NUGET_SOURCE|PYPI_MIRROR|PYPI_TRUSTED)' tools/setup/config.sh
```

确认 `.build-args` 和 `config.sh` 中的 URL 完全一致。

- [ ] **Step 3: 验证 build.sh 中新增读取逻辑**

```bash
grep -n 'NPM_REGISTRY_URL\|MAVEN_MIRROR_URL\|NUGET_SOURCE_URL' tools/tsdb-builder/build.sh | head -20
```

确认每个变量有：读取逻辑 + fallback 默认值 + CONTAINER_SCRIPT 中的注入代码。

- [ ] **Step 4: 验证文档中无遗留的 ⚠️ 标记（针对已修复项）**

```bash
grep -n '⚠️' docs/build-optimization-guide.md
```

预期：不应有与 npm/Maven/NuGet/PyPI 相关的 ⚠️。

- [ ] **Step 5: 验证 modules/*.sh 中无硬编码 nora/nexus URL（应全部来自变量）**

```bash
grep -n 'nora\.tdengine\.net\|nexus\.tdengine\.net' tools/setup/modules/node.sh tools/setup/modules/java.sh tools/setup/modules/dotnet.sh tools/setup/modules/python.sh
```

预期：所有匹配应出现在 `${VAR:-fallback}` 的 fallback 部分或 check 函数的 grep 模式中，不应有独立的硬编码赋值。
