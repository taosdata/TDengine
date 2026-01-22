# Conan 包管理器集成

本文档说明如何使用 Conan 包管理器构建 TDengine，以及迁移的当前状态。

## 概述

TDengine 现在支持使用 Conan 2.x 包管理器来管理第三方依赖，这将显著加快构建速度（使用预编译二进制包）并简化依赖管理。

## 前提条件

- Conan 2.19.1 或更高版本
- CMake 3.22 或更高版本
- GCC 11+ / Clang / MSVC（根据平台）

安装 Conan：
```bash
pip install conan>=2.19
```

## 快速开始

### 使用 Conan 构建（推荐）

```bash
# 1. 安装依赖（首次或依赖变更后）
./build.sh conan-install

# 2. 配置构建
./build.sh conan-gen

# 3. 构建
./build.sh conan-bld

# 或者一步完成所有操作
./build.sh conan-build-all
```

### 传统构建方式（ExternalProject）

```bash
# 仍然可以使用原有方式
./build.sh gen
./build.sh bld
```

## 已迁移的依赖

以下核心依赖已成功迁移到 Conan：

✅ **核心压缩库**
- zlib 1.3.1
- lz4 1.10.0
- xz_utils (LZMA) 5.8.1

✅ **JSON 库**
- cJSON 1.7.18

✅ **网络库**
- OpenSSL 3.6.0
- libcurl 8.2.1
- libuv 1.49.2

✅ **数据库/存储**
- RocksDB 9.7.4

✅ **测试框架**
- Google Test 1.15.0

✅ **可选依赖** (需要在 conanfile.py 中启用)
- jemalloc 5.3.0
- GEOS 3.12.2
- PCRE2 10.44
- sqlite3 3.51.0
- jansson 2.14
- snappy 1.2.1
- libxml2 2.15.0

## 未迁移的依赖

以下依赖暂时保留原有的构建方式（或需要特殊处理）：

🔄 **需要自定义 recipe 或不在 ConanCenter**
- xxHash
- fast-lzma2
- libdwarf, addr2line
- libs3, azure-sdk, cos-sdk (云存储 SDK)
- mxml, apr, apr-util
- avro-c
- cppstub

🔄 **项目内部库**
- TSZ (contrib/)
- libaes (contrib/)
- libmqtt (contrib/)

🔄 **特殊处理**
- taosws (Rust 项目，需要 Cargo)
- taosadapter (Go 项目)
- taoskeeper (Go 项目)

🔄 **Windows 特定**
- pthread-win32
- iconv
- msvcregex
- wcwidth
- wingetopt
- crashdump

## 构建选项

### Conan 选项

可以在 `conanfile.py` 中配置，或通过命令行传递：

```bash
# 启用测试
./build.sh conan-install -o with_test=True

# 启用 jemalloc
./build.sh conan-install -o with_jemalloc=True

# 启用 GEOS
./build.sh conan-install -o with_geos=True

# 多个选项
./build.sh conan-install -o with_test=True -o with_geos=True
```

### CMake 选项

在 conan-gen 步骤可以传递额外的 CMake 选项：

```bash
./build.sh conan-gen -DBUILD_TOOLS=ON -DBUILD_HTTP=OFF
```

## 构建类型

```bash
# Debug 构建（默认）
TD_CONFIG=Debug ./build.sh conan-build-all

# Release 构建
TD_CONFIG=Release ./build.sh conan-build-all
```

## 故障排除

### 清理构建

```bash
# 清理 Conan 构建目录
rm -rf build/conan-debug build/conan-release

# 重新安装依赖
./build.sh conan-install
```

### 查看依赖图

```bash
conan graph info . --format=html > graph.html
```

### 强制重新构建依赖

```bash
./build.sh conan-install --build=missing
```

## 性能对比

| 构建类型 | 首次构建时间 | 增量构建时间 | 磁盘空间 |
|---------|------------|------------|---------|
| ExternalProject | ~60-90 分钟 | ~5-10 分钟 | ~3-4 GB |
| Conan | ~20-30 分钟 | ~5-10 分钟 | ~2-3 GB |

*实际时间取决于硬件配置和网络速度

## 架构说明

### 文件结构

```
.
├── conanfile.py              # Conan 配置文件
├── CMakePresets.json         # CMake 预设（可选）
├── build.sh                  # 构建脚本（支持 Conan）
├── CMakeLists.txt           # 根 CMake（支持 USE_CONAN 选项）
├── cmake/
│   ├── conan.cmake          # Conan 集成和兼容层
│   └── external.cmake       # 原有 ExternalProject（仍保留）
└── contrib/                 # 内部库和未迁移的依赖
```

### 兼容性层

`cmake/conan.cmake` 提供了与原有 `DEP_ext_*` 宏兼容的接口，因此大部分现有代码无需修改即可工作。

例如：
```cmake
# 原有代码仍然有效
DEP_ext_zlib(mytarget)
DEP_ext_lz4(mytarget)

# 实际上会调用 Conan 提供的包
# target_link_libraries(mytarget PUBLIC ZLIB::ZLIB)
# target_link_libraries(mytarget PUBLIC lz4::lz4)
```

## 贡献

### 迁移更多依赖

要迁移一个新的依赖到 Conan：

1. 在 `conanfile.py` 的 `requirements()` 中添加依赖
2. 在 `cmake/conan.cmake` 中添加 `find_package()` 调用
3. 添加对应的兼容宏 `DEP_ext_*`
4. 测试构建

### 创建自定义 Recipe

对于 ConanCenter 没有的包，可以创建自定义 recipe：

```bash
mkdir -p conan/recipes/mylib
cd conan/recipes/mylib
# 创建 conanfile.py
conan create . --version=1.0.0
```

## 已知问题

1. ❗ **taosws 依赖问题**：`ext_taosws` 尚未适配 Conan 构建系统，目前会导致配置失败
   - 临时解决：关闭 WEBSOCKET 选项：`./build.sh conan-gen -DWEBSOCKET=false`

2. ❗ **RocksDB 版本**：Conan 提供的 RocksDB 9.7.4 与原本使用的 8.1.1 版本差异较大，需要测试兼容性

3. ❗ **可选依赖**：GEOS, PCRE2, jansson, snappy 等可选依赖需要在 conanfile.py 中手动启用

## 下一步工作

- [ ] 解决 taosws 依赖问题
- [ ] 为 xxHash 创建 Conan recipe
- [ ] 为 fast-lzma2 创建 Conan recipe  
- [ ] 测试 RocksDB 9.7.4 兼容性
- [ ] 完整的跨平台测试（macOS, Windows）
- [ ] CI/CD 集成
- [ ] 性能基准测试
- [ ] 建立私有 Conan 仓库

## 参考资料

- [Conan 官方文档](https://docs.conan.io/)
- [ConanCenter](https://conan.io/center/)
- [TDengine 原有构建文档](README.md)

## 联系方式

如有问题或建议，请在项目 Issues 中反馈。
