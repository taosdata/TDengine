# scripts/ — 外部依赖管理

## 概述

TDengine 的 ExternalProject 依赖源码包托管在 GitLab Generic Package Registry 上，作为
GitHub 上游的国内镜像。`prepare-externals.sh` 从 `external.cmake` 自动提取依赖列表，
然后从上游下载 tarball、计算 SHA256 哈希、上传到 GitLab Registry，并维护清单文件。

**依赖流向**：

```
external.cmake (唯一数据源)
       ↓ --cmake 自动提取
prepare-externals.sh
       ↓ 下载 + 上传
GitHub (上游) → GitLab Package Registry (镜像)
                        ↓
               cmake BUILD_DEPS_MIRROR_URL
```

编译时，cmake 的 `get_from_local_if_exists()` 宏优先从 GitLab 镜像下载依赖，
避免每次构建都直接访问 GitHub。

**关键设计**：`external.cmake` 是依赖列表的唯一数据源（single source of truth），
脚本通过 `--cmake` 参数自动解析其中的 `get_from_local_if_exists()` 调用，无需手动
维护重复的依赖数组。

## 前置条件

| 条件 | 说明 |
|------|------|
| `GITLAB_TOKEN` | GitLab Personal Access Token，需要 `api` 权限 |
| `GITLAB_PROJECT_ID` | 托管仓库的项目 ID（Settings → General 中查看） |
| `curl` | 用于下载和上传 |
| `sha256sum` 或 `shasum` | 计算哈希（脚本自动检测，macOS 兼容） |
| `perl` | 解析 cmake 文件（macOS 和 Linux 均内置） |

设置环境变量：

```bash
export GITLAB_TOKEN="glpat-xxxxxxxxxxxx"
export GITLAB_PROJECT_ID="123"
# 可选：自定义 GitLab 地址（默认 https://git.tdengine.net）
export GITLAB_URL="https://git.tdengine.net"
```

## 常用工作流

### 1. 查看当前全部依赖

```bash
./scripts/prepare-externals.sh --cmake source/taos-community/cmake/external.cmake --list
```

输出示例：

```
[INFO] Extracting deps from: source/taos-community/cmake/external.cmake
[INFO] Found 28 deps

MIRROR FILENAME                                    UPSTREAM URL
──────────────                                     ────────────
zlib-v1.3.1.tar.gz                                 https://github.com/madler/zlib/...
lz4-v1.10.0.tar.gz                                https://github.com/lz4/lz4/...
...
Total: 28 deps
```

### 2. 首次批量上传（初始化）

从 `external.cmake` 自动提取全部依赖并上传到 GitLab Registry：

```bash
# 先试运行，确认下载正常
./scripts/prepare-externals.sh --cmake source/taos-community/cmake/external.cmake

# 确认无误后上传
./scripts/prepare-externals.sh --cmake source/taos-community/cmake/external.cmake --upload
```

脚本会重新生成 `scripts/externals-manifest.txt`。

### 3. 更新单个依赖版本

以升级 zlib 为例：

```bash
# 1) 修改 source/taos-community/cmake/external.cmake 中的版本号
#    get_from_local_if_exists(
#        "https://github.com/madler/zlib/archive/refs/tags/v1.4.0.tar.gz"
#        "zlib-v1.4.0.tar.gz"
#    )

# 2) 上传新版本（只处理这一个依赖，从 cmake 自动读取新 URL）
./scripts/prepare-externals.sh \
    --cmake source/taos-community/cmake/external.cmake \
    --upload zlib-v1.4.0.tar.gz

# 3) 提交所有变更
#    external.cmake 中的变更和 externals-manifest.txt 的更新
```

### 4. 添加新依赖

添加新依赖时，**必须在 `external.cmake` 中使用双参数形式**调用 `get_from_local_if_exists()`，
手动指定一个可读的镜像文件名：

```cmake
# ✅ 正确：双参数，指定镜像文件名
get_from_local_if_exists(
    "https://github.com/google/snappy/archive/32ded457c0b1...tar.gz"
    "snappy-32ded457c0b1.tar.gz"
)

# ⚠️ 不推荐：单参数，文件名取 URL 末段（可能不直观）
get_from_local_if_exists(
    "https://github.com/google/snappy/archive/32ded457c0b1...tar.gz"
)
# → 镜像文件名将是 32ded457c0b1...tar.gz，无法辨识所属项目
```

**文件名命名规则**：

| URL 类型 | 推荐命名 | 示例 |
|----------|---------|------|
| `refs/tags/v1.3.1.tar.gz` | `项目名-tag.tar.gz` | `zlib-v1.3.1.tar.gz` |
| `archive/32ded457...tar.gz` | `项目名-前12位hash.tar.gz` | `snappy-32ded457c0b1.tar.gz` |

**完整步骤**：

```bash
# 1) 在 external.cmake 中添加双参数调用（手动编写镜像文件名）

# 2) 上传到 Registry（脚本从 cmake 自动提取文件名和上游 URL）
./scripts/prepare-externals.sh \
    --cmake source/taos-community/cmake/external.cmake \
    --upload snappy-32ded457c0b1.tar.gz

# 3) 提交 external.cmake 和 externals-manifest.txt 的变更
```

> **为什么需要手动指定文件名？** `prepare-externals.sh` 会用该文件名上传到 GitLab Registry，
> cmake 编译时也会用同一个文件名去镜像下载。两者必须一致，而且只有在 `external.cmake`
> 中写成双参数形式，cmake 才知道去找这个名字。单参数形式虽然也能工作（取 URL 末段），
> 但对 commit hash 类 URL 来说可读性很差。

如果依赖尚未添加到 `external.cmake`，可以用 `--add` 临时上传：

```bash
./scripts/prepare-externals.sh --upload \
    --add "newlib-v2.0.tar.gz|https://github.com/owner/newlib/archive/refs/tags/v2.0.tar.gz"
```

### 5. 验证 Registry 完整性

检查 GitLab Registry 完整性有两种方式：

1. 校验 `externals-manifest.txt` 中已有的条目：

```bash
./scripts/prepare-externals.sh --verify
```

2. 直接校验当前 `external.cmake` 中提取出的依赖（推荐；不会受 manifest 是否过期影响）：

```bash
./scripts/prepare-externals.sh --cmake ~/tsdb/source/taos-community/cmake/external.cmake --verify
```

如果这时发现 `external.cmake` 中的依赖集合和 `externals-manifest.txt` 不一致，脚本会额外打印 warning，提醒刷新 manifest；但 `--verify` 本身仍保持只读，不会改写 manifest。

输出示例：

```
  ✓ zlib-v1.3.1.tar.gz
  ✓ lz4-v1.10.0.tar.gz
  ✗ newlib-v2.0.tar.gz  (MISSING)

Results: 21 OK, 1 missing, 0 errors
```

## 清单文件格式

`scripts/externals-manifest.txt` 每行一个依赖，格式为 `sha256  filename`：

```
17e88863f3600672ab49182f217281b6fc4d3c762bde361935e436a95214d05c  zlib-v1.3.1.tar.gz
537512904744b35e232912055ccf8ec66d768639ff3abe5788d90d792ec5f48b  lz4-v1.10.0.tar.gz
```

- 批量模式（无过滤参数）会重新生成整个文件
- 单个依赖模式只更新/追加该依赖的行

## cmake 集成

在 `source/taos-community/cmake/external.cmake` 中，每个依赖这样声明：

```cmake
get_from_local_if_exists(
    "https://github.com/madler/zlib/archive/refs/tags/v1.3.1.tar.gz"
    "zlib-v1.3.1.tar.gz"
)
```

- 第一个参数：上游 URL（fallback）
- 第二个参数：镜像文件名（必须和上传到 GitLab Registry 的文件名一致）

单参数调用（无镜像文件名）也会被自动提取，文件名取 URL 的末段：

```cmake
get_from_local_if_exists(
    "https://github.com/openssl/openssl/releases/download/openssl-3.1.3/openssl-3.1.3.tar.gz"
)
```

当设置了 `BUILD_DEPS_MIRROR_URL` 时，cmake 优先从镜像地址
`${BUILD_DEPS_MIRROR_URL}/zlib-v1.3.1.tar.gz` 下载；失败则回退到上游 URL。

## 故障排查

| 问题 | 原因 | 解决方法 |
|------|------|----------|
| HTTP 401 Unauthorized | Token 无效或过期 | 重新生成 `GITLAB_TOKEN`，确保有 `api` 权限 |
| HTTP 403 Forbidden | Token 无该项目的写权限 | 确认 Token 所属用户有项目的 Developer 或以上角色 |
| SHA256 不匹配 | 上游内容变更 | 重新上传：`./scripts/prepare-externals.sh --cmake ... --upload <filename>` |
| `sha256sum: command not found` | macOS 未安装 coreutils | 脚本已自动回退到 `shasum -a 256`，无需额外安装 |
| `--verify` 报告 MISSING | 依赖未上传到 Registry，或 manifest 还没更新到最新依赖集合 | 优先运行 `./scripts/prepare-externals.sh --cmake ... --verify` 确认当前 CMake 依赖集合；缺失时再执行 `./scripts/prepare-externals.sh --cmake ... --upload <filename>` |
| `--cmake ... --verify` 打印 `Manifest is stale` | `external.cmake` 和 manifest 依赖集合不一致 | 运行 `./scripts/prepare-externals.sh --cmake ...` 或 `--cmake ... --upload` 刷新 manifest |
| `No deps to process` | 未指定 `--cmake` 或 `--add` | 添加 `--cmake path/to/external.cmake` 参数 |
