# Multi-Arch Parallel Build Design

**Date**: 2026-04-10  
**Topic**: Dockerfile 重构——并行构建 amd64/arm64 并推送 multi-arch manifest

---

## 问题陈述

当前 `build-all` 命令顺序执行两个架构的构建（先 arm64，再 amd64），耗时翻倍。
同时，Dockerfile 中 Stage 2 使用 `--platform=$BUILDPLATFORM` 引用命名 stage，在跨平台构建（如在 arm64 机器上构建 amd64 目标）时会发生架构错乱。

目标：
1. 单架构构建（本地 load）：保留 `build-arm64` / `build-amd64`，行为不变
2. 双架构并行构建：新增 `build-push`，利用 Docker Buildx 原生多平台能力并行构建，结果合并为 multi-arch manifest 推送到 registry

---

## 方案：Buildx 原生多平台（方案 A）

### 1. Dockerfile 修复

#### 1.1 Stage 2 FROM 行修复

```dockerfile
# 修改前（有问题）
FROM --platform=$BUILDPLATFORM quay.io/pypa/manylinux2014_x86_64 AS stage2-amd64
FROM --platform=$BUILDPLATFORM quay.io/pypa/manylinux2014_aarch64 AS stage2-arm64
FROM stage2-${TARGETARCH}

# 修改后
FROM --platform=linux/amd64 quay.io/pypa/manylinux2014_x86_64 AS base-amd64
FROM --platform=linux/arm64 quay.io/pypa/manylinux2014_aarch64 AS base-arm64
FROM base-${TARGETARCH}
```

每个命名 stage 绑定到其正确的目标平台，无论构建机器架构如何。stage 前缀由 `stage2-` 改为 `base-` 以反映语义。

#### 1.2 JDK 版本自动映射

将 JDK 版本选择从 `build.sh` 外部传参迁移到 Dockerfile builder stage 内部，使 `build-push` 无需为两个平台分别传 `--build-arg`。

Builder stage 新增：
```dockerfile
ARG JDK_VERSION_AMD64=8u144
ARG JDK_VERSION_ARM64=8u441

# 在架构映射 RUN 块中写入 JDK_VERSION
"linux/amd64") ... echo JDK_VERSION=8u144 >> /etc/environment ;;
"linux/arm64") ... echo JDK_VERSION=8u441 >> /etc/environment ;;
```

主 stage 通过 `source /etc/environment` 读取 `JDK_VERSION`，替代外部 `--build-arg JDK_VERSION` 传入。

### 2. build.sh 变更

#### 2.1 新增 `build-push` 命令

```bash
build_push() {
    if [ -z "$REGISTRY_IMAGE" ]; then
        print_error "REGISTRY_IMAGE is not set. Usage: REGISTRY_IMAGE=myregistry/tsdb-builder:latest ./build.sh build-push"
        exit 1
    fi

    # 确保有支持多平台的 buildx builder
    ensure_builder

    DOCKER_BUILDKIT=1 docker buildx build \
        --platform linux/amd64,linux/arm64 \
        $build_args \
        --tag "$REGISTRY_IMAGE" \
        --push \
        -f "$DOCKERFILE" \
        "$SCRIPT_DIR"
}

ensure_builder() {
    # 检查当前 builder 是否支持多平台；若不支持则创建 container driver 的 builder
    if ! docker buildx inspect --bootstrap 2>/dev/null | grep -q "linux/amd64.*linux/arm64\|linux/arm64.*linux/amd64"; then
        docker buildx create --use --name tsdb-multiarch --driver docker-container --bootstrap
    fi
}
```

#### 2.2 `build-amd64` 不再手动覆盖 JDK_VERSION

移除 `--build-arg JDK_VERSION=8u144`，由 Dockerfile 内部自动处理。

#### 2.3 命令表（最终）

| 命令 | 平台 | 输出 | 并行 |
|------|------|------|------|
| `build-arm64` | linux/arm64 | 本地 load (`tsdb-builder:arm64`) | - |
| `build-amd64` | linux/amd64 | 本地 load (`tsdb-builder:amd64`) | - |
| `build-all` | arm64 + amd64 | 本地 load（顺序） | 否 |
| `build-push` | arm64 + amd64 | push 到 `$REGISTRY_IMAGE` | ✅ 是 |
| `build-custom` | 自定义 | 本地 load | - |

### 3. `.build-args` 变更

```diff
- JDK_VERSION=8u441
+ JDK_VERSION_AMD64=8u144
+ JDK_VERSION_ARM64=8u441
```

---

## 数据流

```
./build.sh build-push
      │
      ├─ 校验 REGISTRY_IMAGE
      ├─ ensure_builder（container driver，支持多平台）
      └─ docker buildx build --platform linux/amd64,linux/arm64 --push
              │
              ├─ [并行] linux/amd64 构建
              │       ├─ builder stage: MANYLINUX_IMAGE=manylinux2014_x86_64, JDK_VERSION=8u144
              │       └─ FROM base-amd64 → 安装工具链
              │
              └─ [并行] linux/arm64 构建
                      ├─ builder stage: MANYLINUX_IMAGE=manylinux2014_aarch64, JDK_VERSION=8u441
                      └─ FROM base-arm64 → 安装工具链
                              │
                              ▼
                      registry manifest（包含 amd64 + arm64 两个 digest）
```

---

## 错误处理

- `REGISTRY_IMAGE` 未设置：build-push 提前退出并给出明确提示
- buildx builder 不存在或不支持多平台：`ensure_builder` 自动创建 container driver builder
- 单个架构构建失败：Buildx 整体失败，不会推送不完整的 manifest

---

## 前置要求

- Docker ≥ 20.10（含 buildx）
- 已登录目标 registry（`docker login`）
- 设置 `REGISTRY_IMAGE` 环境变量

---

## 不在本次范围内

- CI/CD pipeline 配置（GitHub Actions / GitLab CI）
- Registry 鉴权管理
- `build-all` 改为并行（保留顺序行为以兼容本地低内存环境）
