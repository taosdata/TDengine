# Conan 包管理器集成 - TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-02-03 | 2026-02-03 | 1.0 | 霍琳贺 | 初始版本 |

## 2. 测试目标

本测试规格旨在验证 TDengine 项目中 Conan 包管理器集成的正确性、完整性和性能表现，确保：
- Conan 自定义包（cppstub, fast-lzma2, avro-c）能够正确构建和安装
- conanfile.py 配置能够正确解析和安装所有依赖
- CMake 与 Conan 集成正常工作
- 构建流程在不同平台和配置下能够正常运行
- 性能提升符合预期
- 向后兼容性得到保证

## 3. 参考文档

- RS: [Conan包管理器集成-RS](https://taosdata.feishu.cn/wiki/GQCowSTdfiD22ykGrM7ckql1nqd)
- FS: [Conan包管理器集成-FS](https://taosdata.feishu.cn/wiki/JnojwByuYi8v57kDu9ucOZitnyg)
- Conan 文档: conan/README.md, conan/CMAKE_INTEGRATION.md

## 4. 测试结论

| 测试类别 | 测试用例数 | 通过数 | 失败数 | 跳过数 | 通过率 |
| --- | --- | --- | --- | --- | --- |
| 包构建测试 | 9 | TBD | TBD | TBD | TBD |
| 依赖安装测试 | 12 | TBD | TBD | TBD | TBD |
| CMake 集成测试 | 8 | TBD | TBD | TBD | TBD |
| 构建流程测试 | 10 | TBD | TBD | TBD | TBD |
| 性能测试 | 6 | TBD | TBD | TBD | TBD |
| 兼容性测试 | 8 | TBD | TBD | TBD | TBD |
| 安全测试 | 5 | TBD | TBD | TBD | TBD |
| **总计** | **58** | **TBD** | **TBD** | **TBD** | **TBD** |

**总体结论**: TBD

## 5. 测试环境

### 5.1 硬件环境

- CPU: Intel Xeon / AMD EPYC / ARM64
- 内存: 16GB+
- 磁盘: 100GB+ 可用空间

### 5.2 软件环境

| 组件 | 版本 |
| --- | --- |
| OS | Ubuntu 20.04/22.04, CentOS 7/8, macOS 12+, Windows 10+ |
| Python | 3.7+ |
| Conan | 2.x |
| CMake | 3.18.0+ |
| GCC | 9.3+ |
| Clang | 10+ |
| MSVC | 2019+ (Windows) |

### 5.3 测试数据

- TDengine 社区版源码
- 自定义 Conan 包配方
- 标准测试数据集

## 6. 功能测试

### 6.1 Conan 包构建测试

#### 6.1.1 测试目标

验证自定义 Conan 包能够正确构建、打包和安装。

#### 6.1.2 测试用例

| # | 测试用例 | 测试描述 | 测试步骤 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| TC-PKG-001 | cppstub 包创建 | 验证 cppstub 包能够成功创建 | 1. cd conan/cppstub 2. conan create . --build=missing | 包创建成功，输出版本 1.0.0 | TBD |
| TC-PKG-002 | cppstub 包安装 | 验证 cppstub 包能够被安装和引用 | 1. conan list "cppstub/*" 2. 检查包是否在本地缓存 | 包列表显示 cppstub/1.0.0 | TBD |
| TC-PKG-003 | cppstub 测试包 | 验证 cppstub test_package 正常工作 | conan create . --build=missing 自动运行测试 | 测试包编译并运行成功 | TBD |
| TC-PKG-004 | fast-lzma2 包创建 | 验证 fast-lzma2 包能够成功创建 | 1. cd conan/fast-lzma2 2. conan create . --build=missing | 包创建成功，输出版本 1.0.1 | TBD |
| TC-PKG-005 | fast-lzma2 静态库 | 验证 fast-lzma2 静态库构建 | conan create . -o fast-lzma2/*:shared=False | 生成静态库 libfast-lzma2.a | TBD |
| TC-PKG-006 | fast-lzma2 动态库 | 验证 fast-lzma2 动态库构建 | conan create . -o fast-lzma2/*:shared=True | 生成动态库 libfast-lzma2.so/.dylib/.dll | TBD |
| TC-PKG-007 | fast-lzma2 测试包 | 验证 fast-lzma2 test_package 正常工作 | conan create . --build=missing | 测试包编译、链接并运行成功 | TBD |
| TC-PKG-008 | avro-c 包创建 | 验证 avro-c 包能够成功创建 | 1. cd conan/avro-c 2. conan create . --build=missing | 包创建成功，输出版本 1.11.3 | TBD |
| TC-PKG-009 | avro-c 测试包 | 验证 avro-c test_package 正常工作 | conan create . --build=missing | 测试包编译并运行成功 | TBD |

### 6.2 依赖安装测试

#### 6.2.1 测试目标

验证 conanfile.py 能够正确解析和安装所有声明的依赖。

#### 6.2.2 测试用例

| # | 测试用例 | 测试描述 | 测试步骤 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| TC-DEP-001 | 默认依赖安装 | 验证默认配置下的依赖安装 | conan install . --build=missing -of=generators | 所有核心依赖成功安装 | TBD |
| TC-DEP-002 | 测试依赖安装 | 验证启用测试时的依赖 | conan install . -o with_test=True | gtest 和 cppstub 成功安装 | TBD |
| TC-DEP-003 | 禁用测试依赖 | 验证禁用测试时不安装测试依赖 | conan install . -o with_test=False | gtest 和 cppstub 不被安装 | TBD |
| TC-DEP-004 | jemalloc 可选依赖 | 验证 jemalloc 可选安装 | conan install . -o with_jemalloc=True | jemalloc 成功安装 | TBD |
| TC-DEP-005 | GEOS 可选依赖 | 验证 GEOS 可选安装 | conan install . -o with_geos=True | geos 成功安装 | TBD |
| TC-DEP-006 | PCRE2 可选依赖 | 验证 PCRE2 可选安装 | conan install . -o with_pcre2=True | pcre2 成功安装 | TBD |
| TC-DEP-007 | libuv 可选依赖 | 验证 libuv 可选安装 | conan install . -o with_uv=True | libuv 成功安装 | TBD |
| TC-DEP-008 | SQLite 可选依赖 | 验证 SQLite 可选安装 | conan install . -o with_sqlite=True | sqlite3 成功安装 | TBD |
| TC-DEP-009 | taos-tools 依赖 | 验证 taos-tools 相关依赖 | conan install . -o with_taos_tools=True | jansson, snappy, avro-c 成功安装 | TBD |
| TC-DEP-010 | 依赖版本验证 | 验证安装的依赖版本正确 | conan graph info . | 输出的依赖版本与 conanfile.py 声明一致 | TBD |
| TC-DEP-011 | 依赖冲突解决 | 验证依赖版本冲突能够正确解决 | conan install . --build=missing | Conan 自动解决版本冲突或报告错误 | TBD |
| TC-DEP-012 | 传递依赖安装 | 验证传递依赖自动安装 | conan install . | 所有传递依赖自动安装（如 libcurl -> openssl） | TBD |

### 6.3 CMake 集成测试

#### 6.3.1 测试目标

验证 CMake 能够正确查找和使用 Conan 安装的依赖。

#### 6.3.2 测试用例

| # | 测试用例 | 测试描述 | 测试步骤 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| TC-CMAKE-001 | CMakeDeps 生成 | 验证 CMakeDeps 生成器正常工作 | conan install . -of=generators | 在 generators/ 目录生成 *-config.cmake 文件 | TBD |
| TC-CMAKE-002 | CMakeToolchain 生成 | 验证 CMakeToolchain 生成器正常工作 | conan install . -of=generators | 生成 conan_toolchain.cmake | TBD |
| TC-CMAKE-003 | find_package 查找 | 验证 CMake 能够查找 Conan 包 | cmake .. 在 debug/ 目录 | 所有 find_package 命令成功 | TBD |
| TC-CMAKE-004 | 依赖注入宏 | 验证 DEP_ext_* 宏正常工作 | 构建使用 DEP_ext_zlib 的目标 | 目标成功链接 zlib | TBD |
| TC-CMAKE-005 | cppstub 宏 | 验证 DEP_ext_cppstub 宏 | 构建测试目标并调用 DEP_ext_cppstub | 测试目标能够包含 stub.h 和 addr_any.h | TBD |
| TC-CMAKE-006 | fast-lzma2 宏 | 验证 DEP_ext_fast_lzma2 宏 | 构建使用 fast-lzma2 的目标 | 目标成功链接 fast-lzma2 | TBD |
| TC-CMAKE-007 | 条件依赖处理 | 验证条件依赖的 CMake 处理 | cmake .. -DBUILD_TEST=OFF | 不查找 gtest 和 cppstub | TBD |
| TC-CMAKE-008 | 平台特定头文件 | 验证平台特定头文件选择 | 在 Linux/macOS/Windows 上构建 | cppstub 选择正确的 addr_any.h | TBD |

### 6.4 构建流程测试

#### 6.4.1 测试目标

验证完整的构建流程能够正常运行。

#### 6.4.2 测试用例

| # | 测试用例 | 测试描述 | 测试步骤 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| TC-BUILD-001 | 首次完整构建 | 验证从零开始的完整构建 | 1. 安装自定义包<br>2. conan install<br>3. cmake<br>4. make | 构建成功，生成 taosd 等二进制 | TBD |
| TC-BUILD-002 | build.sh first-try | 验证 build.sh first-try 命令 | ./build.sh first-try | 自动安装 Conan 依赖并构建成功 | TBD |
| TC-BUILD-003 | build.sh gen | 验证 build.sh gen 命令 | ./build.sh gen | CMake 配置成功 | TBD |
| TC-BUILD-004 | build.sh bld | 验证 build.sh bld 命令 | ./build.sh bld | 增量编译成功 | TBD |
| TC-BUILD-005 | 增量构建 | 验证依赖已缓存时的增量构建 | 修改源文件后重新 make | 只重新编译修改的文件 | TBD |
| TC-BUILD-006 | 清理后重建 | 验证清理后的重新构建 | make clean && make | 重新编译所有目标成功 | TBD |
| TC-BUILD-007 | 不同构建类型 | 验证 Debug/Release 构建 | 1. cmake -DCMAKE_BUILD_TYPE=Debug<br>2. cmake -DCMAKE_BUILD_TYPE=Release | 两种构建类型都成功 | TBD |
| TC-BUILD-008 | 并行构建 | 验证并行编译 | make -j8 | 并行编译成功，无竞态条件 | TBD |
| TC-BUILD-009 | 测试构建 | 验证测试目标构建 | cmake -DBUILD_TEST=ON && make | 所有测试目标成功构建 | TBD |
| TC-BUILD-010 | 工具构建 | 验证 taos-tools 构建 | cmake -DBUILD_TOOLS=ON && make | taos-tools 相关目标成功构建 | TBD |

### 6.5 跨平台测试

#### 6.5.1 测试目标

验证 Conan 集成在不同操作系统和架构上的兼容性。

#### 6.5.2 测试用例

| # | 测试用例 | 测试描述 | 测试平台 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| TC-PLAT-001 | Ubuntu 20.04 x86_64 | 完整构建流程 | Ubuntu 20.04 x86_64, GCC 9.3 | 构建成功 | TBD |
| TC-PLAT-002 | Ubuntu 22.04 x86_64 | 完整构建流程 | Ubuntu 22.04 x86_64, GCC 11 | 构建成功 | TBD |
| TC-PLAT-003 | CentOS 7 x86_64 | 完整构建流程 | CentOS 7 x86_64, GCC 9 | 构建成功 | TBD |
| TC-PLAT-004 | macOS x86_64 | 完整构建流程 | macOS 12+ x86_64, Clang | 构建成功 | TBD |
| TC-PLAT-005 | macOS ARM64 | 完整构建流程 | macOS 12+ ARM64, Clang | 构建成功 | TBD |
| TC-PLAT-006 | Windows x86_64 | 完整构建流程 | Windows 10+ x86_64, MSVC 2019 | 构建成功（部分依赖使用 ExternalProject） | TBD |
| TC-PLAT-007 | Linux ARM64 | 完整构建流程 | Ubuntu 20.04 ARM64 | 构建成功 | TBD |

## 7. 性能测试

### 7.1 测试目标

验证 Conan 集成带来的性能提升。

### 7.2 测试用例

| # | 测试用例 | 测试描述 | 对比基准 | 性能目标 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| TC-PERF-001 | 首次构建时间 | 测量无缓存的完整构建时间 | ExternalProject: ~45分钟 | Conan: < 10分钟 | TBD |
| TC-PERF-002 | 增量构建时间 | 测量有缓存的配置时间 | ExternalProject: ~2分钟 | Conan: < 1分钟 | TBD |
| TC-PERF-003 | 依赖配置时间 | 测量 CMake 配置阶段时间 | ExternalProject: ~10分钟 | Conan: < 30秒 | TBD |
| TC-PERF-004 | 缓存空间占用 | 测量 Conan 缓存大小 | N/A | < 5GB | TBD |
| TC-PERF-005 | 网络下载时间 | 测量首次下载依赖的时间 | N/A | < 5分钟（1Gbps网络） | TBD |
| TC-PERF-006 | 缓存命中率 | 测量多项目构建的缓存复用 | N/A | > 80% 命中率 | TBD |

### 7.3 性能测试环境

- 网络: 1Gbps 带宽
- 磁盘: SSD
- 缓存状态: 空缓存 vs 完整缓存

## 8. 安全测试

### 8.1 测试目标

验证 Conan 集成的安全性特性。

### 8.2 测试用例

| # | 测试用例 | 测试描述 | 测试步骤 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| TC-SEC-001 | 包完整性验证 | 验证下载包的完整性检查 | 1. 清空缓存 2. conan install | Conan 验证包的 SHA256 校验和 | TBD |
| TC-SEC-002 | 依赖审计 | 验证依赖审计功能 | conan graph info . --format=html | 生成完整的依赖图和版本信息 | TBD |
| TC-SEC-003 | 锁文件生成 | 验证依赖锁文件生成 | conan lock create . | 生成 conan.lock 文件 | TBD |
| TC-SEC-004 | 离线构建 | 验证离线构建能力 | 1. 预下载依赖 2. 断网构建 | 离线状态下构建成功 | TBD |
| TC-SEC-005 | 私有仓库支持 | 验证私有仓库配置 | 配置私有 Conan 仓库 | 能够从私有仓库安装包 | TBD |

## 9. 兼容性测试

### 9.1 测试目标

验证 Conan 集成的向后兼容性和共存能力。

### 9.2 测试用例

| # | 测试用例 | 测试描述 | 测试步骤 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| TC-COMPAT-001 | CMake 宏接口 | 验证 DEP_ext_* 宏接口不变 | 构建现有代码无需修改 | 所有现有目标正常构建 | TBD |
| TC-COMPAT-002 | ExternalProject 共存 | 验证与 ExternalProject 共存 | 部分依赖用 Conan，部分用 ExternalProject | 两种方式共存无冲突 | TBD |
| TC-COMPAT-003 | 老版本兼容 | 验证构建系统向后兼容 | 在没有 Conan 的环境构建 | 回退到 ExternalProject 模式 | TBD |
| TC-COMPAT-004 | 编译器兼容 | 验证不同编译器版本 | GCC 9/10/11, Clang 10/11/12 | 所有编译器版本都能构建 | TBD |
| TC-COMPAT-005 | CMake 版本兼容 | 验证不同 CMake 版本 | CMake 3.18, 3.20, 3.22 | 所有 CMake 版本都能配置 | TBD |
| TC-COMPAT-006 | Conan 版本兼容 | 验证 Conan 2.x 版本 | Conan 2.0, 2.1, 2.2 | 所有 Conan 2.x 版本都能工作 | TBD |
| TC-COMPAT-007 | 运行时兼容 | 验证运行时行为不变 | 运行 taosd 和测试套件 | 功能和性能无回归 | TBD |
| TC-COMPAT-008 | API 兼容性 | 验证客户端 API 不变 | 运行现有客户端应用 | 客户端正常连接和操作 | TBD |

## 10. 易用性测试

### 10.1 测试目标

验证 Conan 集成的易用性和文档完整性。

### 10.2 测试用例

| # | 测试用例 | 测试描述 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- |
| TC-USE-001 | 文档完整性 | 检查 Conan 相关文档 | README, CMAKE_INTEGRATION, 包文档完整 | TBD |
| TC-USE-002 | 错误信息清晰 | 验证错误提示的可理解性 | 错误信息清晰，指引明确 | TBD |
| TC-USE-003 | 构建脚本易用 | 验证 build.sh 命令简单易用 | 一条命令即可构建 | TBD |
| TC-USE-004 | 示例代码完整 | 检查示例代码 | test_package 示例清晰完整 | TBD |

## 11. 长期稳定性测试

### 11.1 测试目标

验证 Conan 集成的长期稳定性。

### 11.2 测试用例

| # | 测试用例 | 测试描述 | 测试步骤 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| TC-STAB-001 | 重复构建稳定性 | 连续构建 100 次 | 重复执行 make clean && make | 100 次构建全部成功 | TBD |
| TC-STAB-002 | 缓存稳定性 | 长期使用缓存 | 30 天内多次构建 | 缓存一直有效无损坏 | TBD |
| TC-STAB-003 | 并发构建稳定性 | 多个项目同时构建 | 同时构建 5 个使用 Conan 的项目 | 无缓存冲突，构建都成功 | TBD |

## 12. 回归测试

### 12.1 测试目标

验证 Conan 集成不影响现有功能。

### 12.2 测试用例

| # | 测试用例 | 测试描述 | 测试步骤 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| TC-REG-001 | 单元测试回归 | 运行所有单元测试 | 执行单元测试套件 | 所有单元测试通过 | TBD |
| TC-REG-002 | 系统测试回归 | 运行系统测试 | 执行 system-test | 所有系统测试通过 | TBD |
| TC-REG-003 | 功能回归 | 验证核心功能 | 运行功能测试用例 | 所有功能正常 | TBD |
| TC-REG-004 | 性能回归 | 验证运行时性能 | 运行性能基准测试 | 性能无明显下降（< 5%） | TBD |

## 13. 已知问题和限制

### 13.1 限制

1. **Windows 特定依赖**: 以下依赖仍使用 ExternalProject
  - pthread-win32
  - iconv
  - msvcregex
  - wcwidth
  - wingetopt
  - crashdump
1. **S3 相关库**: 不在 ConanCenter 中
  - libs3
  - azure-sdk
  - cos-sdk
1. **首次构建网络要求**: 首次构建需要网络连接下载依赖包
2. **缓存空间**: Conan 缓存占用 2-5GB 磁盘空间

### 13.2 已知问题

无

## 14. 测试报告模板

### 14.1 测试执行记录

| 测试日期 | 测试人员 | 测试环境 | 通过用例数 | 失败用例数 | 问题数 |
| --- | --- | --- | --- | --- | --- |
| YYYY-MM-DD | XXX | Ubuntu 22.04 | XX | XX | XX |

### 14.2 缺陷记录

| 缺陷ID | 缺陷描述 | 严重程度 | 状态 | 负责人 | 修复版本 |
| --- | --- | --- | --- | --- | --- |
| BUG-001 | XXX | 高/中/低 | 打开/修复/关闭 | XXX | X.X |

### 14.3 测试总结

- **测试覆盖率**: XX%
- **通过率**: XX%
- **严重问题数**: X
- **中等问题数**: X
- **轻微问题数**: X
- **整体评价**: TBD

## 15. 附录

### 15.1 A. 测试脚本示例

```bash
#!/bin/bash

## 16. 自动化测试脚本示例

## 17. 测试 cppstub 包

test_cppstub() {
    echo "Testing cppstub package..."
    cd conan/cppstub
    conan create . --build=missing
    if [ $? -eq 0 ]; then
        echo "✅ cppstub package test passed"
    else
        echo "❌ cppstub package test failed"
        return 1
    fi
    cd ../..
}

## 18. 测试完整构建

test_full_build() {
    echo "Testing full build..."
    ./build.sh first-try
    if [ $? -eq 0 ]; then
        echo "✅ Full build test passed"
    else
        echo "❌ Full build test failed"
        return 1
    fi
}

## 19. 运行所有测试

test_cppstub
test_full_build
```

### 19.1 B. 性能测试脚本

```bash
#!/bin/bash

## 20. 性能测试脚本

## 21. 测量构建时间

measure_build_time() {
    echo "Measuring build time..."
    rm -rf debug
    time (
        conan install . --build=missing -of=generators
        mkdir debug && cd debug
        cmake .. -DCMAKE_BUILD_TYPE=Release
        make -j$(nproc)
    )
}

measure_build_time
```

### 21.1 C. 测试环境配置

```yaml

## 22. Docker 测试环境示例

version: '3'
services:
  ubuntu-test:
    image: ubuntu:22.04
    volumes:
      - .:/workspace
    working_dir: /workspace
    command: |
      apt-get update
      apt-get install -y python3-pip cmake gcc g++
      pip3 install conan
      ./build.sh first-try
```
