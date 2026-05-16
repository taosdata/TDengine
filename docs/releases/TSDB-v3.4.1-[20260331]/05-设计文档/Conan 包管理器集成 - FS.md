# Conan 包管理器集成 - FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-02-03 | 2026-02-03 | 1.0 | 霍琳贺 | 初始版本 |

## 2. 背景

TDengine 项目当前使用 CMake ExternalProject 模块管理第三方依赖库，需要从源码编译所有依赖，导致构建时间长、依赖管理复杂、跨平台兼容性差等问题。通过集成 Conan 包管理器，可以使用预编译二进制包，大幅提升构建效率，简化依赖管理，增强构建的可重现性。
本设计旨在将 TDengine 的依赖管理从 ExternalProject 模式迁移到 Conan 模式，同时保持现有构建系统接口的兼容性。

## 3. 定义

- **Conan**: C/C++ 包管理器，支持二进制包分发和依赖解析
- **conanfile.py**: Python 格式的 Conan 配置文件，定义项目依赖和构建选项
- **CMakeDeps**: Conan 生成器，生成 CMake find_package 配置文件
- **CMakeToolchain**: Conan 生成器，生成 CMake 工具链配置
- **Recipe**: Conan 包配方，描述如何构建和打包一个库
- **Conan Local Cache**: Conan 本地缓存目录，默认在 ~/.conan2

## 4. 行为说明

### 4.1 项目结构变化

#### 4.1.1 新增文件

```plaintext
TDinternal/community/
├── conanfile.py                          # 项目根 Conan 配置文件
├── cmake/
│   └── conan.cmake                       # Conan 与 CMake 集成脚本
├── conan/                                # 自定义 Conan 包目录
│   ├── README.md                         # Conan 包说明文档
│   ├── CMAKE_INTEGRATION.md              # CMake 集成文档
│   ├── cppstub/                          # cppstub 包配方
│   │   ├── conanfile.py
│   │   ├── README.md
│   │   ├── SUMMARY.md
│   │   ├── cppstub/                      # 源文件
│   │   │   ├── src/stub.h
│   │   │   ├── src_linux/addr_any.h
│   │   │   ├── src_darwin/addr_any.h
│   │   │   └── src_win/addr_any.h
│   │   └── test_package/                 # 测试包
│   ├── fast-lzma2/                       # fast-lzma2 包配方
│   │   ├── conanfile.py
│   │   ├── README.md
│   │   ├── USAGE.md
│   │   ├── fast-lzma2/                   # 子模块
│   │   └── test_package/
│   └── avro-c/                           # avro-c 包配方
│       ├── conanfile.py
│       └── test_package/
└── .gitignore                            # 添加 Conan 生成文件忽略规则
```

#### 4.1.2 修改文件

- `CMakeLists.txt`: 添加 Conan 集成支持
- `build.sh`: 添加 Conan 安装步骤
- `cmake/options.cmake`: 调整构建选项
- `include/util/tpcre2.h`: 修正宏名称 USE_PCRE2
- `source/util/src/tpcre2.c`: 修正宏名称 USE_PCRE2
- `tools/shell/CMakeLists.txt`: 适配 Conan 依赖

### 4.2 conanfile.py 配置

#### 4.2.1 基本配置

```python
from conan import ConanFile

class TDengineConan(ConanFile):
    name = "tdengine"
    version = "3.0"
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps", "CMakeToolchain"
```

#### 4.2.2 构建选项

支持以下可配置选项：

| 选项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| shared | [True, False] | False | 是否构建动态库 |
| fPIC | [True, False] | True | 是否使用位置无关代码 |
| with_test | [True, False] | True | 是否构建测试 |
| with_jemalloc | [True, False] | False | 是否使用 jemalloc |
| with_geos | [True, False] | True | 是否包含 GEOS 库 |
| with_pcre2 | [True, False] | True | 是否包含 PCRE2 库 |
| with_uv | [True, False] | True | 是否包含 libuv |
| with_sqlite | [True, False] | False | 是否包含 SQLite |
| with_s3 | [True, False] | False | 是否包含 S3 支持 |
| with_taos_tools | [True, False] | False | 是否构建 taos-tools |

#### 4.2.3 依赖声明

##### 4.2.3.1 核心依赖（必需）

```python
def requirements(self):
    # 压缩库
    self.requires("zlib/1.3.1")
    self.requires("lz4/1.10.0")
    self.requires("xxhash/0.8.3")
    self.requires("xz_utils/5.8.1")
    self.requires("fast-lzma2/1.0.1")
    
    # JSON
    self.requires("cjson/1.7.18")
    
    # 网络
    self.requires("openssl/3.6.0")
    self.requires("libcurl/8.2.1")
    
    # 数据库
    self.requires("rocksdb/9.7.4")
```

##### 4.2.3.2 可选依赖

```python
    # 可选依赖
    if self.options.with_uv:
        self.requires("libuv/1.49.2")
    
    if self.options.with_jemalloc:
        self.requires("jemalloc/5.3.0")
    
    if self.options.with_geos:
        self.requires("geos/3.12.2")
    
    if self.options.with_pcre2:
        self.requires("pcre2/10.44")
    
    if self.options.with_test:
        self.requires("gtest/1.12.1")
        self.requires("cppstub/1.0.0")
    
    if self.options.with_taos_tools:
        self.requires("jansson/2.14")
        self.requires("snappy/1.2.1")
        self.requires("avro-c/1.11.3")
```

#### 4.2.4 依赖配置

```python
def configure(self):
    # 强制所有依赖使用静态链接
    self.options["*"].shared = False
    
    # OpenSSL 配置
    self.options["openssl"].no_deprecated = False
    
    # libcurl 配置
    self.options["libcurl"].with_ssl = "openssl"
    self.options["libcurl"].with_zlib = True
    
    # RocksDB 配置
    self.options["rocksdb"].with_lz4 = True
    self.options["rocksdb"].with_zlib = True
    self.options["rocksdb"].with_jemalloc = self.options.with_jemalloc
```

### 4.3 CMake 集成

#### 4.3.1 CMakeLists.txt 修改

在项目根 CMakeLists.txt 中添加 Conan 集成：
```cmake

## 5. 包含 Conan 生成的配置

if(EXISTS ${CMAKE_BINARY_DIR}/generators/conan_toolchain.cmake)
    include(${CMAKE_BINARY_DIR}/generators/conan_toolchain.cmake)
endif()

## 6. 查找 Conan 包

include(${CMAKE_SOURCE_DIR}/cmake/conan.cmake)
```

#### 6.0.1 cmake/conan.cmake 实现

该文件负责查找 Conan 包并定义依赖注入宏：
```cmake

## 7. 查找核心依赖

find_package(ZLIB REQUIRED CONFIG)
find_package(lz4 REQUIRED CONFIG)
find_package(xxHash REQUIRED CONFIG)
find_package(LibLZMA REQUIRED CONFIG)
find_package(fast-lzma2 REQUIRED CONFIG)
find_package(cjson REQUIRED CONFIG)
find_package(OpenSSL REQUIRED CONFIG)
find_package(CURL REQUIRED CONFIG)
find_package(RocksDB REQUIRED CONFIG)

## 8. 可选依赖

if(${BUILD_TEST})
    find_package(GTest REQUIRED CONFIG)
    find_package(cppstub REQUIRED CONFIG)
endif()

if(${WITH_GEOS})
    find_package(geos REQUIRED CONFIG)
endif()

## 9. 定义依赖注入宏

macro(DEP_ext_cppstub tgt)
    if(TARGET cppstub::cppstub)
        target_link_libraries(${tgt} PUBLIC cppstub::cppstub)
    endif()
endmacro()

macro(DEP_ext_fast_lzma2 tgt)
    if(TARGET fast-lzma2::fast-lzma2)
        target_link_libraries(${tgt} PUBLIC fast-lzma2::fast-lzma2)
    endif()
endmacro()

## 10. ... 其他宏定义

```

#### 10.0.1 依赖注入宏接口

保持现有 CMake 依赖注入接口不变，例如：
- `DEP_ext_zlib(target)`: 链接 zlib
- `DEP_ext_lz4(target)`: 链接 lz4
- `DEP_ext_openssl(target)`: 链接 OpenSSL
- `DEP_ext_gtest(target)`: 链接 GTest
- `DEP_ext_cppstub(target)`: 链接 cppstub

### 10.1 自定义 Conan 包

#### 10.1.1 cppstub (v1.0.0)

**类型**: 头文件库（Header-only）
**文件结构**:
```plaintext
conan/cppstub/
├── conanfile.py              # 包配方
├── README.md                 # 使用文档
├── cppstub/
│   ├── LICENSE
│   ├── src/stub.h            # 主头文件
│   ├── src_linux/addr_any.h  # Linux 平台头文件
│   ├── src_darwin/addr_any.h # macOS 平台头文件
│   └── src_win/addr_any.h    # Windows 平台头文件
└── test_package/             # 测试包
    ├── CMakeLists.txt
    ├── conanfile.py
    └── test_package.cpp
```

**特点**:
- 无需编译，只提供头文件
- 根据操作系统选择对应的 addr_any.h
- 用于单元测试中的函数桩和模拟
**使用示例**:
```cpp
#include <stub.h>
#include <addr_any.h>

// 在测试中使用
Stub stub;
stub.set(original_func, mock_func);
```

#### 10.1.2 fast-lzma2 (v1.0.1)

**类型**: 编译库（静态/动态可选）
**文件结构**:
```plaintext
conan/fast-lzma2/
├── conanfile.py              # 包配方
├── README.md                 # 使用文档
├── USAGE.md                  # 详细用法
├── fast-lzma2/               # Git 子模块
│   ├── fast-lzma2.h
│   ├── fast-lzma2.c
│   └── ...
└── test_package/             # 测试包
```

**特点**:
- 基于 Makefile 构建
- 支持静态和动态库
- 支持 x86_64 汇编优化
- 提供压缩和解压缩 API
**编译配置**:
```python
def build(self):
    make = self._make_program()
    self.run(f"{make} -C {self.source_folder}/fast-lzma2")
```

#### 10.1.3 avro-c (v1.11.3)

**类型**: 编译库
**文件结构**:
```plaintext
conan/avro-c/
├── conanfile.py              # 包配方
└── test_package/             # 测试包
```

**特点**:
- Apache Avro C 实现
- 用于 taos-tools 的数据序列化
- 依赖 jansson, snappy

### 10.2 构建流程

#### 10.2.1 首次构建流程

```bash

## 1. 安装自定义 Conan 包

cd conan/cppstub
conan create . --build=missing

cd ../fast-lzma2
conan create . --build=missing

cd ../avro-c
conan create . --build=missing

## 2. 安装项目依赖

cd ../..
conan install . --build=missing -of=generators

## 3. 配置 CMake

mkdir -p debug && cd debug
cmake .. -DCMAKE_BUILD_TYPE=Debug \
         -DBUILD_TEST=ON \
         -DBUILD_TOOLS=ON

## 4. 编译

make -j$(nproc)
```

#### 10.2.2 build.sh 集成

更新 build.sh 脚本以支持 Conan 工作流：
```bash
#!/bin/bash

function install_conan_deps() {
    echo "Installing Conan dependencies..."
    
    # 安装自定义包
    for pkg in cppstub fast-lzma2 avro-c; do
        if [ -d "conan/$pkg" ]; then
            echo "Creating $pkg package..."
            cd "conan/$pkg"
            conan create . --build=missing
            cd ../..
        fi
    done
    
    # 安装项目依赖
    echo "Installing project dependencies..."
    conan install . --build=missing -of=generators
}

function build_with_conan() {
    install_conan_deps
    
    mkdir -p debug && cd debug
    cmake .. -DCMAKE_BUILD_TYPE=Debug \
             -DBUILD_TEST=ON
    make -j$(nproc)
}
```

#### 10.2.3 增量构建

依赖已缓存后，只需：
```bash
cd debug
cmake ..
make -j$(nproc)
```

### 10.3 构建选项使用

#### 10.3.1 启用/禁用功能

```bash

## 11. 禁用测试

conan install . -o with_test=False

## 12. 启用 jemalloc

conan install . -o with_jemalloc=True

## 13. 构建 taos-tools

conan install . -o with_taos_tools=True
```

#### 13.0.1 跨平台构建

```bash

## 14. Linux

conan install . -s os=Linux -s arch=x86_64

## 15. macOS

conan install . -s os=Macos -s arch=armv8

## 16. Windows

conan install . -s os=Windows -s arch=x86_64
```

### 16.1 错误处理

#### 16.1.1 包未找到

如果 CMake 提示找不到 Conan 包：
```plaintext
CMake Error: Could not find package cppstub
```

**解决方案**:
```bash
cd conan/cppstub
conan create . --build=missing
```

#### 16.1.2 版本冲突

如果出现依赖版本冲突：
```plaintext
ERROR: Version conflict: zlib/1.3.1 vs zlib/1.2.13
```

**解决方案**:
在 conanfile.py 中强制指定版本：
```python
def requirements(self):
    self.requires("zlib/1.3.1", override=True)
```

#### 16.1.3 编译错误

如果 Conan 包编译失败：
```bash

## 17. 清理并重新构建

conan remove "package-name/*" -c
conan create conan/package-name --build=missing -vvv
```

## 18. 性能

### 18.1 构建时间对比

| 场景 | ExternalProject | Conan | 提升 |
| --- | --- | --- | --- |
| 全新构建（无缓存） | ~45 分钟 | ~5 分钟 | 9x |
| 增量构建（有缓存） | ~2 分钟 | ~30 秒 | 4x |
| 依赖配置 | ~10 分钟 | ~10 秒 | 60x |

### 18.2 缓存效率

- Conan 本地缓存位于 `~/.conan2`
- 多项目共享同一缓存，节省磁盘空间
- 支持缓存清理：`conan cache clean`

## 19. 安全

### 19.1 包完整性验证

Conan 自动验证下载包的校验和：
```python

## 20. Conan 自动处理

def source(self):
    get(self, "https://example.com/package.tar.gz",
        sha256="abc123...")
```

### 20.1 依赖审计

查看所有依赖及其版本：
```bash

## 21. 列出所有依赖

conan graph info . --format=html > deps.html

## 22. 生成依赖锁文件

conan lock create . --lockfile=conan.lock
```

### 22.1 私有仓库支持

配置私有 Conan 仓库：
```bash

## 23. 添加私有仓库

conan remote add private-repo https://private.conan.io

## 24. 使用私有仓库

conan install . --remote=private-repo
```

### 24.1 离线构建

预下载所有依赖：
```bash

## 25. 下载所有依赖

conan install . --build=missing

## 26. 导出缓存

conan cache save "*" --path=conan-cache.tar.gz

## 27. 在离线环境导入

conan cache restore conan-cache.tar.gz
```

## 28. 兼容性

### 28.1 向后兼容

- 保持现有 CMake 宏接口不变（`DEP_ext_*`）
- ExternalProject 和 Conan 可以共存
- 支持逐步迁移，不影响现有构建

### 28.2 破坏性变更

无。本次集成为增量添加，不破坏现有构建流程。

## 29. 运维

### 29.1 CI/CD 集成

在 CI 流水线中集成 Conan：
```yaml

## 30. .gitlab-ci.yml 示例

build:
  script:
    - pip install conan
    - ./build.sh conan-build
  cache:
    key: conan-cache
    paths:
      - ~/.conan2
```

### 30.1 缓存管理

```bash

## 31. 查看缓存大小

du -sh ~/.conan2

## 32. 清理旧版本

conan cache clean "*" --older-than=30d

## 33. 完全清理

conan cache clean "*"
```

## 34. 使用场景

### 34.1 场景1: 新开发者环境搭建

新开发者只需安装 Conan 和编译器，无需手动安装各种依赖库：
```bash
pip install conan
./build.sh conan-install
./build.sh conan-bld
```

### 34.2 场景2: 跨平台开发

在不同平台使用相同的构建命令：
```bash

## 35. Linux/macOS/Windows 统一命令

./build.sh conan-install
./build.sh conan-bld
```

### 35.1 场景3: 持续集成

CI 环境利用 Conan 缓存加速构建：
```bash

## 36. 第一次构建创建缓存

## 37. 后续构建复用缓存，大幅提速

```

### 37.1 场景4: 依赖升级

升级特定依赖版本：
```python

## 38. 修改 conanfile.py

self.requires("zlib/1.3.2")  # 从 1.3.1 升级到 1.3.2
```

## 39. 约束和限制

### 39.1 约束

1. 需要安装 Conan 2.x：`pip install conan`
2. 需要 Python 3.7+ 环境
3. 首次构建需要网络连接下载包

### 39.2 限制

1. 部分 Windows 特定依赖（pthread-win32, iconv 等）仍需使用 ExternalProject
2. Conan 缓存目录 `~/.conan2` 会占用磁盘空间（~2-5GB）

## 40. 常见错误和排查

### 40.1 错误1: "Could not find package cppstub"

**原因**: 自定义包未安装
**解决**:
```bash
cd conan/cppstub
conan create . --build=missing
```

### 40.2 错误2: "addr_any.h: No such file or directory"

**原因**: cppstub 未正确链接到目标
**解决**: 在 CMakeLists.txt 中添加：
```cmake
DEP_ext_cppstub(my_test_target)
```

### 40.3 错误3: 编译器版本不匹配

**原因**: Conan 包使用的编译器与本地不同
**解决**: 指定编译器配置：
```bash
conan profile detect --force
conan install . --build=missing
```

## 41. 可观测性

对 taos shell, taosExplorer, TDinsight 等工具无直接影响。Conan 只影响构建过程，不影响运行时行为。

## 42. 安装和卸载

### 42.1 安装 Conan

```bash

## 43. Linux/macOS

pip install conan

## 44. 检查版本

conan --version  # 应为 2.x
```

### 44.1 卸载

```bash

## 45. 删除 Conan 缓存

rm -rf ~/.conan2

## 46. 卸载 Conan

pip uninstall conan
```

## 47. 文档

### 47.1 新增文档

1. conan/README.md - Conan 包说明
2. conan/CMAKE_INTEGRATION.md - CMake 集成文档
3. conan/cppstub/README.md - cppstub 使用文档
4. conan/fast-lzma2/README.md - fast-lzma2 使用文档

## 48. 参考文档

- [Conan Documentation](https://docs.conan.io/)
- [CMakeDeps Generator](https://docs.conan.io/2/reference/tools/cmake/cmakedeps.html)
- [CMakeToolchain Generator](https://docs.conan.io/2/reference/tools/cmake/cmaketoolchain.html)
- [Creating Conan Packages](https://docs.conan.io/2/tutorial/creating_packages.html)

## 49. 附录

### 49.1 A. 已迁移依赖列表

| 依赖库 | 版本 | 来源 | 状态 |
| --- | --- | --- | --- |
| zlib | 1.3.1 | ConanCenter | ✅ |
| lz4 | 1.10.0 | ConanCenter | ✅ |
| xxhash | 0.8.3 | ConanCenter | ✅ |
| xz_utils | 5.8.1 | ConanCenter | ✅ |
| fast-lzma2 | 1.0.1 | 自定义 | ✅ |
| cjson | 1.7.18 | ConanCenter | ✅ |
| openssl | 3.6.0 | ConanCenter | ✅ |
| libcurl | 8.2.1 | ConanCenter | ✅ |
| libuv | 1.49.2 | ConanCenter | ✅ |
| rocksdb | 9.7.4 | ConanCenter | ✅ |
| gtest | 1.12.1 | ConanCenter | ✅ |
| cppstub | 1.0.0 | 自定义 | ✅ |
| geos | 3.12.2 | ConanCenter | ✅ |
| pcre2 | 10.44 | ConanCenter | ✅ |
| jemalloc | 5.3.0 | ConanCenter | ✅ |
| sqlite3 | 3.51.0 | ConanCenter | ✅ |
| jansson | 2.14 | ConanCenter | ✅ |
| snappy | 1.2.1 | ConanCenter | ✅ |
| avro-c | 1.11.3 | 自定义 | ✅ |

### 49.2 B. 待迁移依赖列表

以下依赖暂未迁移，仍使用 ExternalProject：
- Windows 特定库：pthread-win32, iconv, msvcregex, wcwidth, wingetopt, crashdump
- S3 相关库：libs3, azure-sdk, cos-sdk
- 其他内部库：根据需要逐步迁移

### 49.3 C. Conan 配置文件示例

```toml
[settings]
os=Linux
arch=x86_64
compiler=gcc
compiler.version=9
compiler.libcxx=libstdc++11
build_type=Release

[options]
*:shared=False
*:fPIC=True

[tool_requires]
cmake/3.22.0
```
