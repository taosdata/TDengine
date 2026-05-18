# build.sh cmake 参数透传设计

**日期**: 2026-04-13  
**主题**: 支持发版脚本透传任意 cmake 参数；`--image` 改为必填

---

## 问题陈述

当前 `build.sh` 的 cmake 参数完全由内部组件名映射逻辑生成，仅支持 16 个 `BUILD_*=ON/OFF` 开关。发版脚本需要传入 50~60 个 cmake 参数（版本号、git hash、构建时间、Edition 开关、`CMAKE_BUILD_TYPE` 等），但 `build.sh` 没有透传口。

---

## 设计目标

1. 发版脚本（位于 `tsdb/`）可以将所有 cmake 参数直接传给 `build.sh`，无需修改 `build.sh` 本身来追加新参数。
2. 开发者日常用法（`./build.sh --image core engine taosx`）行为不变。
3. `--image` 改为必填，消除自动推断的歧义。

---

## 接口设计

```bash
./build.sh --image core|others [--arch amd64|arm64] [--src PATH] [--cache PATH] [--clean] \
  [component...] \
  [-DKEY=VALUE ...]
```

### 参数说明

| 参数 | 是否必填 | 说明 |
|---|---|---|
| `--image core\|others` | **必填** | 指定使用哪套镜像，不再自动推断 |
| `--arch amd64\|arm64` | 可选 | 默认 host 架构（`uname -m`） |
| `--src PATH` | 可选 | TSDB 源码目录，默认 `$(pwd)` |
| `--cache PATH` | 可选 | 缓存根目录，默认 `$HOME/cache/tsdb-builder`，也可通过 `TSDB_CACHE_DIR` 环境变量设置 |
| `--clean` | 可选 | 构建前清除 build 目录 |
| `component...` | 可选 | 组件名快捷方式（见下方列表），自动生成对应 `BUILD_*=ON/OFF` cmake flags |
| `-DKEY=VALUE` | 可选 | 直接透传给 cmake 的参数，可出现多次，顺序任意 |

### 组件名快捷方式（保留）

| 组件名 | 自动生成的 cmake flag |
|---|---|
| `engine` | `BUILD_ENGINE=ON` |
| `enterprise` | `BUILD_ENTERPRISE=ON` |
| `adapter` | `BUILD_ADAPTER=ON` |
| `keeper` | `BUILD_KEEPER=ON` |
| `tools` | `BUILD_TOOLS=ON` |
| `gen` | `BUILD_GEN=ON` |
| `taosx` | `BUILD_TAOSX=ON, BUILD_EXPLORER_UI=OFF` |
| `explorer-ui` | `BUILD_TAOSX=ON, BUILD_EXPLORER_UI=ON` |
| `insight` | `BUILD_INSIGHT=ON` |
| `dotnet` | `BUILD_DOTNET=ON` |
| `go` | `BUILD_GO=ON` |
| `jdbc` | `BUILD_JDBC=ON` |
| `node` | `BUILD_NODE=ON` |
| `python` | `BUILD_PYTHON=ON` |
| `rust` | `BUILD_RUST=ON` |
| `odbc` | `BUILD_ODBC=ON` |
| `core-all` | 展开为全部 core 组件 |
| `others-all` | 展开为全部 others 组件 |
| `all` | 展开为全部 16 个组件 |

### cmake 参数组合顺序

cmake 以最后一次 `-D` 赋值为准，三层按以下顺序拼接：

```
[组件名自动生成的 BUILD_*=ON/OFF（均默认 OFF，仅请求的组件设为 ON）]
+ [pthread 修复 5 个变量，仅 --image core 时追加]
+ [-DKEY=VALUE CLI 参数（按命令行顺序追加）]
```

CLI `-D` 参数排在最后，优先级最高。如果发版脚本显式传入 `-DBUILD_ENGINE=OFF`，将覆盖组件名自动生成的 `BUILD_ENGINE=ON`。

---

## 典型用法

### 发版脚本（community amd64）

```bash
cmake_args=(
  -DCMAKE_BUILD_TYPE=Release
  -DBUILD_ENTERPRISE=OFF
  -DBUILD_ENGINE=ON
  -DBUILD_ADAPTER=ON
  -DBUILD_KEEPER=ON
  -DBUILD_TOOLS=ON
  -DBUILD_TAOSX=OFF
  -DBUILD_WEBSOCKET=ON
  -DBUILD_RUST=ON
  -DBUILD_VER_NUMBER="3.4.1.3"
  -DBUILD_VER_COMPATIBLE="3.0.0.0"
  -DBUILD_VER_TYPE="stable"
  -DBUILD_GITINFO="${GIT_HASH}"
  -DBUILD_VER_DATE="${BUILD_DATE}"
)
./tools/tsdb-builder/build.sh --image core --arch amd64 "${cmake_args[@]}"
```

### 发版脚本（enterprise arm64）

```bash
cmake_args=(
  -DCMAKE_BUILD_TYPE=Release
  -DBUILD_ENTERPRISE=ON
  -DBUILD_GRANT_VALUE=15
  -DBUILD_ENGINE=ON
  -DBUILD_TAOSX=ON
  -DBUILD_VER_NUMBER="${VER}"
  -DBUILD_GITINFO="${GIT_HASH}"
  -DBUILD_VER_DATE="${BUILD_DATE}"
)
./tools/tsdb-builder/build.sh --image core --arch arm64 "${cmake_args[@]}"
```

### 开发者日常（行为不变，仅新增 --image 必填）

```bash
# 基本用法
./build.sh --image core engine taosx

# 跨架构
./build.sh --image core --arch arm64 engine adapter

# 组件名 + 少量覆盖（Debug 模式）
./build.sh --image core engine -DCMAKE_BUILD_TYPE=Debug

# others 镜像
./build.sh --image others explorer-ui insight jdbc

# 全量清除重编
./build.sh --image core --clean core-all
```

---

## 实现要点

### arg-parsing 变更

当前 `while [[ $# -gt 0 ]]` 循环需新增两个 case：

```bash
-D*)
    EXTRA_CMAKE_ARGS+=("$1")
    shift
    ;;
```

（`-DKEY=VALUE` 连写形式，单个 token。）

### `--image` 校验

移除现有的自动推断逻辑（`NEEDS_OTHERS` 检测），改为：

```bash
if [[ -z "$IMAGE_OVERRIDE" ]]; then
    echo "ERROR: --image is required. Use --image core or --image others."
    exit 1
fi
```

### cmake 调用变更

```bash
cmake .. ${CMAKE_ARGS} "${EXTRA_CMAKE_ARGS[@]}"
```

`CMAKE_ARGS` 是现有的组件 + pthread 字符串，`EXTRA_CMAKE_ARGS` 是新增的数组，保证带空格的值（如 `BUILD_VER_DATE`）不被 word-split。

---

## 向后兼容说明

| 场景 | 影响 |
|---|---|
| 不传 `--image` 的现有调用 | **破坏性变更**：现在会报错，需补充 `--image core` 或 `--image others` |
| `build-core.sh` / `build-others.sh` | 不受影响，两者不调用 `build.sh` |
| 现有组件名用法 | 不变 |
| `build-core-image.sh` / `build-others-image.sh` | 不受影响 |

---

## 不在本次范围内

- 环境变量透传（已明确不做）
- `build-core.sh` / `build-others.sh` 支持 `-D` 透传
- cmake presets / profile 文件
