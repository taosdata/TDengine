# 在 macOS (Apple Silicon) 上编译 taosBenchmark

## 前提条件

- macOS with Apple Silicon (M1/M2/M3/M4)
- 已安装 arm64 原生的 Homebrew（路径为 `/opt/homebrew`）
- TDengine 源码

## 步骤 1：安装 arm64 原生的 CMake

> ⚠️ **重要：** 必须使用 arm64 原生版本的 cmake，不能使用 x86_64 版本。

检查现有 cmake 的架构：

```bash
file $(which cmake)
```

如果输出包含 `x86_64`（通常安装在 `/usr/local/bin/cmake`），说明是通过 Rosetta 运行的旧版本，会导致外部依赖编译时 arm64 与 x86_64 架构混乱，链接阶段报 `cputype does not match` 错误。

安装 arm64 原生版本：

```bash
/opt/homebrew/bin/brew install cmake
```

验证：

```bash
file /opt/homebrew/bin/cmake
# 期望输出: Mach-O 64-bit executable arm64
```

## 步骤 2：修复 GEOS 依赖的 CMake 兼容性问题

CMake 3.31+ 移除了对 `cmake_minimum_required < 3.5` 的兼容支持，会导致 `ext_geos` 配置失败，报错如下：

```
CMake Error at cmake/Ccache.cmake:10 (cmake_minimum_required):
  Compatibility with CMake < 3.5 has been removed from CMake.
```

**修复方法：** 编辑 `cmake/external.cmake`，找到 `ExternalProject_Add(ext_geos ...)`，在 `CMAKE_ARGS` 中添加一行：

```cmake
ExternalProject_Add(ext_geos
    ...
    CMAKE_ARGS -DBUILD_TESTING:BOOL=OFF
    CMAKE_ARGS -DBUILD_GEOSOP:BOOL=OFF
+   CMAKE_ARGS -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    BUILD_COMMAND
        ...
)
```

> **注意：** 顶层 cmake 传入的 `-DCMAKE_POLICY_VERSION_MINIMUM=3.5` 不会自动传递给 ExternalProject 子项目，必须在 `ExternalProject_Add` 的 `CMAKE_ARGS` 中显式添加。

## 步骤 3：编译

```bash
cd /path/to/TDengine

# 清理旧的构建缓存（首次编译或遇到架构问题时）
rm -rf debug .externals

# 配置（开启 taos-tools 编译，关闭测试）
mkdir debug && cd debug
/opt/homebrew/bin/cmake .. -DBUILD_TOOLS=true -DBUILD_TEST=false

# 只编译 taosBenchmark（而非整个 TDengine）
make taosBenchmark -j$(sysctl -n hw.ncpu)
```

编译产物路径：

```
debug/build/bin/taosBenchmark
```

## 常见问题速查

| 错误信息 | 原因 | 解决方案 |
|---------|------|---------|
| `cputype (16777223) does not match cputype (16777228)` | x86_64 与 arm64 产物混合 | 使用 arm64 原生 cmake，`rm -rf .externals` 重编 |
| `Compatibility with CMake < 3.5 has been removed` | CMake 3.31+ 兼容性变更 | 在 ext_geos 的 CMAKE_ARGS 中添加 `-DCMAKE_POLICY_VERSION_MINIMUM=3.5` |
| `fatal: invalid upstream 'origin/xxx'` | 外部依赖 git 缓存损坏 | `rm -rf .externals/build/ext_xxx` 清除对应缓存 |
| macOS 上 BUILD_TOOLS 默认关闭 | `cmake/define.cmake` 中 Darwin 默认 false | 配置时显式传入 `-DBUILD_TOOLS=true` |
