# Multi-Arch Parallel Build Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor Dockerfile and build.sh to support parallel multi-arch (amd64 + arm64) builds that push a multi-arch manifest to a registry via a new `build-push` command.

**Architecture:** Fix Dockerfile's Stage 2 FROM platform binding, internalize JDK version selection into the builder stage, and add a `build-push` command to build.sh that uses `docker buildx build --platform linux/amd64,linux/arm64 --push`. Single-arch local builds (`build-arm64`, `build-amd64`) remain unchanged.

**Tech Stack:** Dockerfile (Docker BuildKit multi-stage), Bash, Docker Buildx

---

## File Map

| File | Change |
|------|--------|
| `Dockerfile` | Fix Stage 2 FROM lines; add JDK_VERSION_AMD64/ARM64 ARGs in builder stage; add JDK_VERSION to /etc/environment; remove `ARG JDK_VERSION` from main stage |
| `.build-args` | Replace `JDK_VERSION=8u441` with `JDK_VERSION_AMD64=8u144` and `JDK_VERSION_ARM64=8u441` |
| `build.sh` | Remove manual `--build-arg JDK_VERSION=8u144` from `build_amd64`; add `ensure_builder()` and `build_push()`; update `list_args()`, `show_help()`, and `main()` |
| `README.md` | Add `build-push` to Quick Start and command table; document `REGISTRY_IMAGE` variable |

---

### Task 1: Fix Dockerfile Stage 2 FROM lines

**Files:**
- Modify: `Dockerfile` (lines 62–65)

**What and why:** The current `--platform=$BUILDPLATFORM` on the two named manylinux stages causes architecture mismatch during cross-platform builds (e.g., building amd64 on an arm64 host). Replace with explicit `--platform=linux/amd64` and `--platform=linux/arm64` so each named stage is always bound to its correct target architecture. Also rename the stage prefix from `stage2-` to `base-` to better reflect semantics.

- [ ] **Step 1: Apply the change**

In `Dockerfile`, find and replace the three lines:

```dockerfile
FROM --platform=$BUILDPLATFORM quay.io/pypa/manylinux2014_x86_64 AS stage2-amd64
FROM --platform=$BUILDPLATFORM quay.io/pypa/manylinux2014_aarch64 AS stage2-arm64

FROM stage2-${TARGETARCH}
```

Replace with:

```dockerfile
FROM --platform=linux/amd64 quay.io/pypa/manylinux2014_x86_64 AS base-amd64
FROM --platform=linux/arm64 quay.io/pypa/manylinux2014_aarch64 AS base-arm64

FROM base-${TARGETARCH}
```

Also update the comment block above those three lines (lines 56–61) to read:

```dockerfile
# ============================================================================
# Stage 2: Main - Build environment
# Uses architecture-specific manylinux2014 image:
#   amd64: quay.io/pypa/manylinux2014_x86_64  (--platform=linux/amd64)
#   arm64: quay.io/pypa/manylinux2014_aarch64 (--platform=linux/arm64)
# ============================================================================
```

- [ ] **Step 2: Verify Dockerfile syntax**

```bash
docker buildx build --check -f Dockerfile . 2>&1 || docker build --no-cache --dry-run -f Dockerfile . 2>&1 | head -20
```

Expected: No syntax errors. If `--check` is unavailable, the command falls back gracefully. The key is no `ERROR` lines about unknown platform or stage name.

- [ ] **Step 3: Commit**

```bash
git add Dockerfile
git commit -m "fix(dockerfile): bind manylinux base stages to explicit target platforms

Replace --platform=\$BUILDPLATFORM with --platform=linux/amd64 and
--platform=linux/arm64 on the two named manylinux2014 stages. This
ensures each stage is always pulled as its correct architecture,
regardless of the build machine's native platform.

Rename stage prefix stage2- → base- to better reflect semantics.

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

### Task 2: Move JDK version selection into the Dockerfile builder stage

**Files:**
- Modify: `Dockerfile` (builder stage RUN block; main stage ARG section)

**What and why:** `build-push` uses a single `docker buildx build --platform linux/amd64,linux/arm64` invocation, so `--build-arg JDK_VERSION=8u144` (amd64-only override) can no longer be passed. Instead, let the builder stage write the correct `JDK_VERSION` into `/etc/environment` based on `TARGETPLATFORM`, making it available to all downstream RUN commands via `. /etc/environment`.

- [ ] **Step 1: Add ARGs and JDK_VERSION mapping to the builder stage**

In `Dockerfile`, locate the builder stage ARG declarations (after `FROM docker.1ms.run/alpine AS builder`):

```dockerfile
ARG TARGETPLATFORM
ARG TARGETARCH
ARG TARGETVARIANT
```

Add two new ARGs immediately after these three lines:

```dockerfile
ARG JDK_VERSION_AMD64=8u144
ARG JDK_VERSION_ARM64=8u441
```

Then, in the `RUN case "${TARGETPLATFORM}" in` block, append `JDK_VERSION` to each arch's `/etc/environment` output. The `linux/amd64` block currently ends with:

```dockerfile
    echo MANYLINUX_IMAGE=quay.io/pypa/manylinux2014_x86_64 >> /etc/environment; \
    ;; \
```

Change it to:

```dockerfile
    echo MANYLINUX_IMAGE=quay.io/pypa/manylinux2014_x86_64 >> /etc/environment; \
    echo JDK_VERSION=${JDK_VERSION_AMD64} >> /etc/environment; \
    ;; \
```

The `linux/arm64` block currently ends with:

```dockerfile
    echo MANYLINUX_IMAGE=quay.io/pypa/manylinux2014_aarch64 >> /etc/environment; \
    ;; \
```

Change it to:

```dockerfile
    echo MANYLINUX_IMAGE=quay.io/pypa/manylinux2014_aarch64 >> /etc/environment; \
    echo JDK_VERSION=${JDK_VERSION_ARM64} >> /etc/environment; \
    ;; \
```

- [ ] **Step 2: Remove the now-redundant ARG JDK_VERSION from the main stage**

In the main stage ARG section, find and remove these two lines (they are no longer the source of truth):

```dockerfile
# JDK: amd64 uses 8u144 (available in installers/), arm64 uses 8u441
# Override via --build-arg JDK_VERSION=8u144 for amd64 builds
ARG JDK_VERSION=8u441
```

> **Why it's safe to remove:** Every downstream `RUN . /etc/environment && ...` that uses `${JDK_VERSION}` will correctly receive the value written by the builder stage. The sourced value from `/etc/environment` overrides any shell environment variable of the same name, so removing the ARG has no functional effect on existing per-arch builds — it only prevents confusing external override attempts that would be silently overridden anyway.

- [ ] **Step 3: Verify Dockerfile syntax**

```bash
docker buildx build --check -f Dockerfile . 2>&1 | head -20
```

Expected: No syntax errors.

- [ ] **Step 4: Manually trace the JDK install RUN command**

The Layer 5 RUN in `Dockerfile` looks like:

```dockerfile
RUN --mount=type=bind,source=installers,target=/mnt/installers \
    . /etc/environment && \
    tar -xzf /mnt/installers/jdk-${JDK_VERSION}-linux-${JDK_ARCH}.tar.gz -C /usr/local && \
    ...
```

Confirm that after this task:
- For `linux/amd64`: `/etc/environment` will contain `JDK_VERSION=8u144` and `JDK_ARCH=x64`, so the file expanded is `jdk-8u144-linux-x64.tar.gz` ✓
- For `linux/arm64`: `/etc/environment` will contain `JDK_VERSION=8u441` and `JDK_ARCH=aarch64`, so the file expanded is `jdk-8u441-linux-aarch64.tar.gz` ✓

- [ ] **Step 5: Commit**

```bash
git add Dockerfile
git commit -m "feat(dockerfile): auto-select JDK version per arch in builder stage

Add JDK_VERSION_AMD64 (default: 8u144) and JDK_VERSION_ARM64 (default: 8u441)
ARGs to the builder stage. The builder stage writes the appropriate
JDK_VERSION into /etc/environment so all downstream RUN commands
(. /etc/environment && ...) pick up the correct value automatically.

Remove the now-redundant ARG JDK_VERSION from the main stage.

This enables build-push (single docker buildx invocation for both
platforms) without needing per-platform --build-arg JDK_VERSION overrides.

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

### Task 3: Update .build-args

**Files:**
- Modify: `.build-args`

**What and why:** Align `.build-args` with the Dockerfile changes: the single `JDK_VERSION` parameter is replaced by two separate `JDK_VERSION_AMD64` / `JDK_VERSION_ARM64` parameters. Update the comment section accordingly.

- [ ] **Step 1: Apply the change**

In `.build-args`, find and replace the JDK line and its context comment:

```
# Architecture note:
#   JDK_VERSION is overridden per architecture by build.sh:
#     AMD64 → 8u144   (installers/jdk-8u144-linux-x64.tar.gz)
#     ARM64 → 8u441   (installers/jdk-8u441-linux-aarch64.tar.gz)
```

Replace with:

```
# Architecture note:
#   JDK version is selected automatically by the Dockerfile builder stage:
#     AMD64 → JDK_VERSION_AMD64   (installers/jdk-8u144-linux-x64.tar.gz)
#     ARM64 → JDK_VERSION_ARM64   (installers/jdk-8u441-linux-aarch64.tar.gz)
```

Then in the `# Development Tools Versions` section, replace:

```
JDK_VERSION=8u441
```

With:

```
JDK_VERSION_AMD64=8u144
JDK_VERSION_ARM64=8u441
```

- [ ] **Step 2: Verify the file is well-formed**

```bash
grep -v '^#' .build-args | grep -v '^[[:space:]]*$'
```

Expected output (each line is a `KEY=VALUE` pair, no `JDK_VERSION=` line, two new JDK lines present):

```
GO_VERSION=1.23.4
MAVEN_VERSION=3.8.4
CMAKE_VERSION=3.21.5
JDK_VERSION_AMD64=8u144
JDK_VERSION_ARM64=8u441
RUST_VERSION=1.90.0
PYTHON_VERSION=3.12
DOTNET_VERSION=6.0.100
MOLD_VERSION=2.40.3
PROTOC_VERSION=33.0
TINI_VERSION=v0.19.0
TAOSPY_VERSION=2.8.8
TAOS_WS_PY_VERSION=0.6.5
PYPI_MIRROR=http://mirrors.aliyun.com/pypi/simple/
PYPI_TRUSTED_HOST=mirrors.aliyun.com
GO_PROXY=https://goproxy.cn
TIMEZONE=Asia/Shanghai
```

- [ ] **Step 3: Commit**

```bash
git add .build-args
git commit -m "feat(build-args): replace JDK_VERSION with JDK_VERSION_AMD64/ARM64

JDK version is now selected by the Dockerfile builder stage from
JDK_VERSION_AMD64 (8u144) and JDK_VERSION_ARM64 (8u441).
The single JDK_VERSION override in build.sh is no longer needed.

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

### Task 4: Update build.sh

**Files:**
- Modify: `build.sh`

**What and why:** Four changes:
1. Remove `--build-arg JDK_VERSION=8u144` from `build_amd64` (now handled by Dockerfile)
2. Add `ensure_builder()` helper that guarantees a multi-platform-capable buildx builder exists
3. Add `build_push()` function that uses `--platform linux/amd64,linux/arm64 --push`
4. Update `list_args()`, `show_help()`, and `main()` accordingly

- [ ] **Step 1: Update `build_amd64` — remove manual JDK override**

Find the `build_amd64` function. Remove these lines:

```bash
    # JDK_VERSION is overridden here; Docker uses the last occurrence when a key
    # appears multiple times, so this safely overrides the .build-args default.
    # shellcheck disable=SC2086
    DOCKER_BUILDKIT=1 docker buildx build \
        --platform linux/amd64 \
        $build_args \
        --build-arg JDK_VERSION=8u144 \
        --tag tsdb-builder:amd64 \
        --load \
        -f "$DOCKERFILE" \
        "$SCRIPT_DIR"
```

Replace with:

```bash
    # shellcheck disable=SC2086
    DOCKER_BUILDKIT=1 docker buildx build \
        --platform linux/amd64 \
        $build_args \
        --tag tsdb-builder:amd64 \
        --load \
        -f "$DOCKERFILE" \
        "$SCRIPT_DIR"
```

Also update the `print_info "Note: ..."` line:

```bash
    # Remove this line entirely:
    print_info "Note: AMD64 uses JDK 8u144 (from installers/)"
```

- [ ] **Step 2: Add `ensure_builder()` function**

Add this function after the `parse_build_args` function and before the `# === Build Commands ===` separator:

```bash
# Ensure the current buildx builder supports linux/amd64 and linux/arm64.
# If not (e.g., the default "docker" driver only supports the host arch),
# create a container-driver builder named "tsdb-multiarch" and set it active.
ensure_builder() {
    local platforms
    platforms=$(docker buildx inspect --bootstrap 2>/dev/null | grep -i "platforms:" | head -1)
    if echo "$platforms" | grep -q "linux/amd64" && echo "$platforms" | grep -q "linux/arm64"; then
        return 0
    fi
    print_info "Current buildx builder does not support both platforms."
    print_info "Creating multi-platform builder: tsdb-multiarch"
    docker buildx create --use --name tsdb-multiarch --driver docker-container --bootstrap
    print_info "Builder tsdb-multiarch is ready."
}
```

- [ ] **Step 3: Add `build_push()` function**

Add this function after `build_all()` and before `build_custom()`:

```bash
build_push() {
    if [ -z "${REGISTRY_IMAGE:-}" ]; then
        print_error "REGISTRY_IMAGE is not set."
        print_error "Usage: REGISTRY_IMAGE=myregistry.io/tsdb-builder:latest ./build.sh build-push"
        exit 1
    fi

    print_info "Building multi-arch image (linux/amd64 + linux/arm64) and pushing..."
    print_info "Target: ${REGISTRY_IMAGE}"
    print_info "JDK version selection is automatic per architecture (set in Dockerfile)."

    ensure_builder

    local build_args
    build_args=$(parse_build_args)

    # shellcheck disable=SC2086
    DOCKER_BUILDKIT=1 docker buildx build \
        --platform linux/amd64,linux/arm64 \
        $build_args \
        --tag "${REGISTRY_IMAGE}" \
        --push \
        -f "$DOCKERFILE" \
        "$SCRIPT_DIR"

    print_info "Build and push completed successfully!"
    print_info "Multi-arch manifest: ${REGISTRY_IMAGE}"
    print_info "  - linux/amd64 (JDK 8u144)"
    print_info "  - linux/arm64 (JDK 8u441)"
}
```

- [ ] **Step 4: Update `list_args()` — remove stale AMD64/ARM64 JDK override note**

Find and replace this block in `list_args()`:

```bash
    print_info "Architecture overrides applied automatically by build commands:"
    echo "    AMD64: JDK_VERSION=8u144  (overrides .build-args default)"
    echo "    ARM64: JDK_VERSION from .build-args (default: 8u441)"
```

Replace with:

```bash
    print_info "Architecture-specific values (handled automatically in Dockerfile):"
    echo "    AMD64 JDK: JDK_VERSION_AMD64 (default: 8u144, file: jdk-8u144-linux-x64.tar.gz)"
    echo "    ARM64 JDK: JDK_VERSION_ARM64 (default: 8u441, file: jdk-8u441-linux-aarch64.tar.gz)"
```

- [ ] **Step 5: Update `show_help()` — document build-push and remove JDK_VERSION note**

Find the Commands section in `show_help()`:

```bash
  build-amd64       Build for AMD64 architecture (JDK_VERSION overridden to 8u144)
  build-all         Build both architectures sequentially
  build-custom      Build with custom --build-arg overrides
```

Replace with:

```bash
  build-amd64       Build for AMD64 architecture (JDK version auto-selected)
  build-all         Build both architectures sequentially (local load)
  build-push        Build amd64+arm64 in parallel, push multi-arch manifest to registry
                      Requires: REGISTRY_IMAGE=myregistry.io/tsdb-builder:latest
  build-custom      Build with custom --build-arg overrides
```

Also add a usage example for `build-push`. Find:

```bash
  # Verify a built image
  ./verify-image.sh arm64
```

Add before it:

```bash
  # Build both architectures in parallel and push to registry
  REGISTRY_IMAGE=myregistry.io/tsdb-builder:v1.0 $0 build-push

```

- [ ] **Step 6: Update `main()` — add build-push case**

In the `case "$1" in` block, add after `build-all)`:

```bash
        build-push)
            build_push
            ;;
```

- [ ] **Step 7: Verify bash syntax**

```bash
bash -n build.sh && echo "Syntax OK"
```

Expected: `Syntax OK`

- [ ] **Step 8: Smoke-test the help output**

```bash
./build.sh help 2>&1 | grep -E "build-push|REGISTRY_IMAGE"
```

Expected output contains both:
```
  build-push        Build amd64+arm64 in parallel, push multi-arch manifest to registry
                      Requires: REGISTRY_IMAGE=myregistry.io/tsdb-builder:latest
```

- [ ] **Step 9: Smoke-test build-push error handling**

```bash
unset REGISTRY_IMAGE && ./build.sh build-push 2>&1
```

Expected: exits with error and prints:
```
[ERROR] REGISTRY_IMAGE is not set.
[ERROR] Usage: REGISTRY_IMAGE=myregistry.io/tsdb-builder:latest ./build.sh build-push
```

- [ ] **Step 10: Commit**

```bash
git add build.sh
git commit -m "feat(build.sh): add build-push for parallel multi-arch builds

- Remove manual --build-arg JDK_VERSION=8u144 from build_amd64 (now
  handled automatically by Dockerfile builder stage)
- Add ensure_builder(): guarantees a docker-container driver buildx
  builder exists that supports both linux/amd64 and linux/arm64
- Add build_push(): single docker buildx build invocation with
  --platform linux/amd64,linux/arm64 --push, targeting REGISTRY_IMAGE
- Update list_args(), show_help(), main() to reflect new command

Usage:
  REGISTRY_IMAGE=myregistry.io/tsdb-builder:latest ./build.sh build-push

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

### Task 5: Update README.md

**Files:**
- Modify: `README.md`

**What and why:** Document the new `build-push` command, the `REGISTRY_IMAGE` environment variable, and remove references to the old JDK_VERSION external override.

- [ ] **Step 1: Update the Quick Start — 构建镜像 section**

Find the code block under `### 构建镜像`:

```bash
# 同时构建两种架构（顺序执行）
./build.sh build-all
```

Replace with:

```bash
# 同时构建两种架构（顺序执行，本地 load）
./build.sh build-all

# 并行构建两种架构并推送 multi-arch manifest 到 registry
REGISTRY_IMAGE=myregistry.io/tsdb-builder:latest ./build.sh build-push
```

- [ ] **Step 2: Add REGISTRY_IMAGE prerequisite note**

After the `### 前置要求` block (which mentions Docker ≥ 20.10 and installers/), add:

```markdown
- **`build-push` 前提**：已执行 `docker login` 登录目标 registry，并设置 `REGISTRY_IMAGE` 环境变量
```

- [ ] **Step 3: Update 构建参数说明 table — replace JDK_VERSION rows**

Find the table row:

```markdown
| `JDK_VERSION` | `8u441` | JDK 版本（ARM64 默认；AMD64 由 build.sh 自动覆盖为 8u144） |
```

Replace with:

```markdown
| `JDK_VERSION_AMD64` | `8u144` | AMD64 JDK 版本 |
| `JDK_VERSION_ARM64` | `8u441` | ARM64 JDK 版本 |
```

- [ ] **Step 4: Update 快速开始 command examples to remove JDK_VERSION mention**

Find:

```bash
# 覆盖某个工具版本
./build.sh build-custom GO_VERSION=1.24.0

# 指定平台和标签
PLATFORM=linux/amd64 TAG=tsdb-builder-v2:amd64-dev ./build.sh build-custom
```

Ensure the example does NOT reference `JDK_VERSION=8u144` (if such an example exists, remove it).

- [ ] **Step 5: Verify README renders cleanly**

```bash
grep -n "JDK_VERSION=" README.md
```

Expected: no output (all old references replaced).

```bash
grep -n "build-push\|REGISTRY_IMAGE" README.md
```

Expected: at least 3 matches covering the Quick Start examples and parameter table.

- [ ] **Step 6: Commit**

```bash
git add README.md
git commit -m "docs(readme): document build-push and REGISTRY_IMAGE

- Add build-push usage example to Quick Start
- Document REGISTRY_IMAGE prerequisite for build-push
- Replace JDK_VERSION table row with JDK_VERSION_AMD64 / JDK_VERSION_ARM64
- Remove references to the old external JDK_VERSION override

Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

---

## Self-Review

### Spec coverage

| Spec requirement | Covered by |
|-----------------|------------|
| Fix `--platform=$BUILDPLATFORM` in Stage 2 FROM lines | Task 1 |
| JDK version selection moved into Dockerfile builder stage | Task 2 |
| `.build-args` updated to `JDK_VERSION_AMD64`/`ARM64` | Task 3 |
| `build-amd64` no longer passes `--build-arg JDK_VERSION` | Task 4 Step 1 |
| `ensure_builder()` for multi-platform buildx builder | Task 4 Step 2 |
| `build-push` command with `--platform linux/amd64,linux/arm64 --push` | Task 4 Step 3 |
| `REGISTRY_IMAGE` environment variable validation | Task 4 Step 3 |
| README updated | Task 5 |

### Type / name consistency check

- `ensure_builder` is defined in Task 4 Step 2 and called in Task 4 Step 3 ✓
- `JDK_VERSION_AMD64` / `JDK_VERSION_ARM64`: defined as ARGs in Task 2, added to `.build-args` in Task 3, referenced in Task 5 docs ✓
- Stage names `base-amd64` / `base-arm64`: set in Task 1, referenced by `FROM base-${TARGETARCH}` in same task ✓
- `build_push` (function) vs `build-push` (command): consistent with existing naming convention (`build_arm64` → `build-arm64`) ✓

### Placeholder scan

No TBD, TODO, or "similar to" references found. All code blocks are complete.
