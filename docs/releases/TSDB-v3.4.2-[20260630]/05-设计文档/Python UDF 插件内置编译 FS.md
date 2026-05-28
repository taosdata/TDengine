# Python UDF 插件内置编译 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-05-15 | 2026-05-15 | 1.0 | Simon Guan | 初稿 |
| 2026-05-15 | 2026-05-15 | 1.1 | Simon Guan | 更新实现细节：混合链接策略、Python 运行时自动发现、RC 版本信息修复、错误日志增强 |
| 2026-05-16 | 2026-05-16 | 1.2 | Simon Guan | 多版本自动下载：编译期自动下载 Python SDK，Windows 多版本 DLL，Linux 不再依赖本地 python3-dev |
| 2026-05-20 | 2026-05-20 | 1.3 | Simon Guan | 实现更新为单库加载 + CPython C API Limited API |

## 2. 背景

TDengine 支持用 Python 语言编写 UDF（用户自定义函数）。Python UDF 的运行依赖 `libtaospyudf.so` 动态库，该库通过 CPython C API 将解释器嵌入到 C++ 共享库中，由 UDF daemon（`udfd`）在运行时通过 `dlopen` 加载。

**现状问题**：

- `libtaospyudf.so` 目前作为独立的 PyPI 包 `taospyudf` 发布，用户需要手动执行 `pip3 install taospyudf` + `ldconfig` 才能使用 Python UDF
- 安装过程需要系统具备 C++ 编译环境（cmake、gcc），门槛较高
- Windows 环境下问题尤为突出：用户需要安装 Visual Studio 或 MSVC Build Tools、CMake、Python 开发头文件，并正确配置环境变量，`pip install taospyudf` 的编译过程极易因环境配置不当而失败，导致 Windows 用户几乎无法使用 Python UDF 功能
- 不再采用 pybind 方案：该方案通常需要按 Python 小版本分别编译并维护依赖库（如 3.10/3.11/3.12 各自产物），构建与发布复杂度高，不符合当前单库兼容目标
- 源码位于独立仓库 `source/taos-udf/`，与主工程构建流程脱节
- CI 测试中需要额外执行 `pip install` 步骤，增加了环境准备复杂度

**目标**：

1. 将 `taospyudf` 源码迁入 `source/taos-community/` 内，作为主工程子模块编译，产物 `libtaospyudf.so` 随安装包一起发布。用户安装 TDengine 后即可直接使用 Python UDF，无需额外步骤。
2. 归档原 `source/taos-udf/` 外部仓库，迁移完成后从主工程中移除该子目录，原始仓库标记为 archived，不再维护。
3. **编译期零依赖**：通过 [python-build-standalone](https://github.com/astral-sh/python-build-standalone) 自动下载 Python SDK（头文件 + import lib），编译机器不再需要安装 `python3-dev` 或 Python 运行环境。
4. **Windows 单库模式**：编译单个 `taospyudf.dll`，运行时检测系统 Python（`python3XX.dll`）并加载，减少多 DLL 版本维护成本。

## 3. 定义

| 术语 | 说明 |
| --- | --- |
| `taospyudf` | Python UDF 插件库，编译产物为 `libtaospyudf.so`（Linux）/ `libtaospyudf.dylib`（macOS）/ `taospyudf.dll`（Windows） |
| `udfd` | UDF daemon，TDengine 的 UDF 执行进程，通过 `dlopen` 加载 UDF 插件 |
| `pybind11` | C++ 与 Python 的互操作库（header-only），用于将 CPython 解释器嵌入到 C++ 程序中 |
| `Py_LIMITED_API` | Python Limited API 宏，目标为稳定 ABI（abi3）兼容，减少对具体 Python 小版本符号的绑定 |
| `plog` | 轻量级 C++ 日志库（header-only），taospyudf 用于记录运行日志 |
| `pybind11::embed` | pybind11 的嵌入模式，将完整的 Python 解释器嵌入 C++ 宿主进程 |
| `python-build-standalone` | 自动下载的 Python SDK 来源（头文件 / import lib），用于构建期去除本地 Python 开发包依赖 |

## 4. 行为说明

### 4.1 构建行为

新增 CMake 选项 `BUILD_PYUDF`，默认 **ON**：

```bash
# 默认构建（包含 Python UDF 插件）
cmake -B debug -DCMAKE_BUILD_TYPE=Debug

# 显式关闭
cmake -B debug -DCMAKE_BUILD_TYPE=Debug -DBUILD_PYUDF=OFF
```

当 `BUILD_PYUDF=ON` 时：

1. `BUILD_PYUDF_PYTHON_VERSION` **必须**设置（`options.cmake` 中默认值 `"3.15.0b1"`），否则 CMake 报 `FATAL_ERROR`
2. CMake configure 阶段通过 `ExternalProject_Add` 从 [python-build-standalone](https://github.com/astral-sh/python-build-standalone) 下载预编译的 Python SDK（含头文件和 import lib），**无需本地安装 Python 或 python3-dev**
3. 通过 ExternalProject 下载 plog 到 `.externals/` 缓存目录
4. Windows：编译单个 `taospyudf.dll`
5. Linux/macOS：编译单个 `libtaospyudf.so` / `libtaospyudf.dylib`

构建依赖与 `BUILD_CONTRIB` 相同——所有依赖均通过 `ExternalProject` 从网络下载，首次构建后缓存在 `.externals/` 目录中。

> **注意**：不再需要 `python3-dev` / `python3-devel` 包，也不再使用 `find_package(Python3)`。Python 头文件完全从自动下载的 SDK 中获取。

### 4.2 安装行为

**Linux/macOS**：`make install` 或安装包部署时，`libtaospyudf.so` 安装到 driver 目录并创建系统软链接：

```
/usr/local/taos/driver/libtaospyudf.so    ← 实际文件
/usr/lib/libtaospyudf.so                   ← 软链接
```

**Windows**：`make install` 或安装包部署时，插件安装到 `C:\TDengine\bin\` 目录：

```
C:\TDengine\bin\taospyudf.dll
```

`udfd` 的加载逻辑（`udfd.c`）在加载 `taospyudf` 前会：
- **Linux/macOS**：调用 `udfdPreloadPythonLibrary()` 预加载 libpython
- **Windows**：检测 PATH 中 `python3XX.dll`，自动推导 `PYTHONHOME`，再加载 `taospyudf.dll`

然后通过 `uv_dlopen()` 加载插件库。

### 4.3 用户使用流程变化

**变更前**（手动安装）：

```bash
# 1. 安装 TDengine
# 2. 安装 Python 运行环境
# 3. pip3 install taospyudf    ← 需要 cmake + gcc
# 4. ldconfig
# 5. 启动 taosd
# 6. CREATE FUNCTION ... LANGUAGE 'Python'
```

**变更后**（内置）：

```bash
# 1. 安装 TDengine（Python UDF 插件已随包安装）
# 2. 安装 Python 运行环境
#    Linux: python3 + libpython3.XX.so（apt install python3-dev 或 yum install python3-devel）
#    Windows: Python 3.10–3.15 任一版本，加入 PATH
# 3. 启动 taosd
# 4. CREATE FUNCTION ... LANGUAGE 'Python'
```

用户不再需要 `pip3 install taospyudf`，不再需要 `ldconfig`，不再需要编译工具链。

**Python 版本兼容性**：

- **Linux**：`libtaospyudf.so` 不绑定 Python 版本，一份二进制兼容 Python 3.9–3.15。运行时 `udfd` 通过 `dlopen(RTLD_GLOBAL)` 预加载系统中安装的 `libpython3.XX.so`
- **Windows**：单库 `taospyudf.dll`，运行时按 3.15→3.9 检测 `python3XX.dll`，找到后加载插件。若系统 Python 版本不在可检测范围内，加载将失败并输出错误日志

### 4.4 源码目录结构

```
source/taos-community/
├── cmake/
│   ├── external.cmake          ← 新增 ext_plog、ext_cpython_3_XX 定义
│   └── options.cmake           ← 新增 BUILD_PYUDF、BUILD_PYUDF_PYTHON_VERSION 等选项
├── source/libs/
│   └── pyudf/                  ← 新增目录
│       ├── CMakeLists.txt
│       └── src/
│           ├── taospyudf.cpp   ← 从 source/taos-udf/python/src/ 迁入并切换到 CPython C API
│           └── taospyudf.h     ← 从 source/taos-udf/python/src/ 迁入
└── packaging/tools/
    └── make_install.sh         ← 新增 libtaospyudf 安装逻辑
```

### 4.5 ExternalProject 依赖

在 `external.cmake` 中新增 ExternalProject 依赖，均在 `BUILD_PYUDF=ON` 时启用：

**ext_cpython_3_15**（单个，由 `BUILD_PYUDF_PYTHON_VERSION` 指定）：
- 来源：`https://github.com/astral-sh/python-build-standalone/releases/download/{PBS_RELEASE}/cpython-{VER}+{RELEASE}-{TRIPLE}-install_only.tar.gz`
- PBS release tag：`20260510`（内部变量 `_pyudf_pbs_release`，定义在 `external.cmake` 中，与版本列表绑定）
- 缓存路径：`.externals/build/${CMAKE_BUILD_TYPE}/ext_cpython_3_XX/`
- 平台 triple：`x86_64-pc-windows-msvc`（Windows）、`x86_64-unknown-linux-gnu`（Linux）、`x86_64-apple-darwin`（macOS）、`aarch64-unknown-linux-gnu`（ARM Linux）、`aarch64-apple-darwin`（Apple Silicon）
- 提供：Python 头文件（Windows: `include/`，Linux: `include/python3.XX/`）和 import lib（`libs/python3.lib` / `python3XX.lib`，Windows）
- 用户选项（`options.cmake`）：
  ```cmake
    set(BUILD_PYUDF_PYTHON_VERSION  "3.15.0b1" CACHE STRING
            "Single Python version for pyudf SDK selection (must match PBS release)")
  ```
- **必需**：`BUILD_PYUDF_PYTHON_VERSION` 为空时 `BUILD_PYUDF=ON` 将触发 `FATAL_ERROR`

**ext_plog**（header-only，日志库）：
- 仓库：`https://github.com/SergiusTheBest/plog.git`，Tag `1.1.10`
- 对外宏：`DEP_ext_plog_INC(target)`
- **必需**：`taospyudf.cpp` 中大量使用 `PLOGE`/`PLOGI`/`PLOGD` 宏记录日志

依赖通过 ExternalProject 管理，`INIT_EXT` / `INIT_DIRS` 声明 `INC_DIR include`。

### 4.6 pyudf/CMakeLists.txt 核心逻辑

所有 Python 依赖通过 ExternalProject 自动下载，不使用 `find_package(Python3)`。

```cmake
# _pyudf_add_target(target_name, py_inc, py_lib, ver_short, output_name) 宏：
macro(_pyudf_add_target _tgt _py_inc _py_lib _ver_short _output_name)
    add_library(${_tgt} SHARED src/taospyudf.cpp)
    add_dependencies(${_tgt} ${_ext} ext_plog)
    target_include_directories(${_tgt} PRIVATE
        ${_py_inc}                     # Python headers from auto-download
        ${ext_plog_inc_dirs}           # plog headers
        ...
    )
    target_compile_definitions(${_tgt} PRIVATE BUILDING_DLL Py_LIMITED_API=0x030A0000)
    if(TD_WINDOWS)
        target_link_libraries(${_tgt} PRIVATE "${_py_lib}")  # python3.lib / python3XX.lib
    else()
        target_link_options(${_tgt} PRIVATE "LINKER:--allow-shlib-undefined")
    endif()
endmacro()
```

- **Windows**：产出 `taospyudf.dll`（单库）
- **Linux/macOS**：产出 `libtaospyudf.so` / `libtaospyudf.dylib`

**混合链接策略的原因**：

| 平台 | 策略 | 原因 |
| --- | --- | --- |
| Linux | 不链接 libpython，运行时 dlopen | Python 符号通过运行时解析，libpython 通过 RTLD_GLOBAL 暴露给进程空间即可。一个 .so 兼容多个 Python 3.x |
| Windows | 链接 Python import lib + Limited API | 单库模式下由运行时探测 `python3XX.dll`，减少多 DLL 维护成本，同时通过 Limited API 降低对小版本差异的敏感性 |

### 4.7 运行时 Python 自动发现（udfd.c）

`udfd.c` 在加载 `taospyudf` 插件前，执行平台相关的 Python 运行时发现逻辑：

#### Windows: `udfdEnsurePythonHome()`

启动时检查 `PYTHONHOME` 环境变量是否已设置：
1. 如果 `PYTHONHOME` 已设置 → 直接使用
2. 否则在系统 PATH 中搜索 `python3XX.dll` / `python.exe`，推导并设置 `PYTHONHOME`
3. 如果都找不到 → 输出 WARNING 日志

这确保嵌入式 Python 解释器能正确定位标准库。

#### Windows: `udfdInitializePythonPlugin()` — 单库检测模式

启动时按从高到低的顺序搜索系统 Python 版本（3.15 → 3.9）：
1. 使用 `SearchPathA()` 在系统 PATH 中查找 `python3XX.dll`
2. 找到第一个存在的版本后，推导 `PYTHONHOME`
3. 固定加载 `taospyudf.dll`
4. 如果没有找到任何 `python3XX.dll`，**直接报错**

```c
// 伪代码
for (int minor = 15; minor >= 9; minor--) {
    snprintf(dllName, "python3%d.dll", minor);
    if (SearchPathA(NULL, dllName, ...)) {
        udfdEnsurePythonHome(path, minor);
        found = true;
        break;
    }
}
if (!found) {
    fnError("udf python: no python3XX.dll found in PATH");
    return TSDB_CODE_UDF_LOAD_UDF_FAILURE;
}
uv_dlopen("taospyudf.dll", ...);
```

> **设计决策**：不再维护 `taospyudf_3_XX.dll` 多版本产物，统一单库 `taospyudf.dll`，将版本选择逻辑下沉到运行时 Python 探测阶段，降低安装包复杂度与运维成本。

#### Linux/macOS: `udfdPreloadPythonLibrary()`

搜索 Python 3.15 → 3.9 的 `libpython3.XX.so.1.0` / `libpython3.XX.so`：
1. 先尝试 `PYTHONHOME/lib/` 下的路径
2. 再尝试系统 `dlopen()` 搜索路径

找到后以 `RTLD_NOW | RTLD_GLOBAL` 加载，使 Python 符号对后续 dlopen 的 `libtaospyudf.so` 全局可见。

Linux 插件名固定为 `libtaospyudf.so`（不带版本号）。

### 4.8 错误日志与诊断

当 Python 环境加载失败时，`taosudf` 进程输出分级错误日志：

| 场景 | 日志级别 | 消息内容 |
| --- | --- | --- |
| Windows: PYTHONHOME 未设置且 PATH 无 python.exe/python3XX.dll | WARN | `PYTHONHOME not set and could not be inferred.` |
| Windows: PYTHONHOME 自动推导成功 | INFO | `PYTHONHOME inferred from DLL path/registry: <dir>` |
| Windows: DLL 加载失败 | ERROR | `FAILED to load plugin library 'taospyudf.dll'` + 具体原因指引 |
| Linux: libpython 未找到 | ERROR | `FAILED to pre-load libpython3.XX.so. Install python3 development package.` |
| Linux: 插件加载失败 | ERROR | `FAILED to load plugin library 'libtaospyudf.so'` + 安装建议 |

日志通过 `fnError()` 宏输出到 `taosudf` 进程的日志文件（默认 `/var/log/taos/taosudf.log`）。

### 4.9 Windows .rc 版本信息

`.rc.in` 模板通过 `configure_file()` 在 CMake 配置阶段处理，替换版本变量后生成 `.rc` 文件供 MSVC 资源编译器编译。

`version.cmake` 中从 `BUILD_VER_NUMBER`（如 `3.4.1.6.alpha`）提取前 4 个数字组件，生成 `BUILD_VER_NUMBER_COMMA`（如 `3,4,1,6`），用于 `FILEVERSION` / `PRODUCTVERSION` 二进制字段。

```cmake
# version.cmake 新增
STRING(REGEX MATCHALL "[0-9]+" _ver_components "${BUILD_VER_NUMBER}")
# → BUILD_VER_NUMBER_COMMA = "3,4,1,6"
```

影响范围：`taos.dll`、`taosnative.dll`、`taospyudf.dll` 均获得正确版本属性。

### 4.10 安装脚本变更（make_install.sh）

在 `install_lib()` 函数中新增，参照 `libtaosws.so` 的条件安装模式：

```bash
# Linux
if [ -f ${binary_dir}/build/lib/libtaospyudf.so ]; then
    ${csudo}cp ${binary_dir}/build/lib/libtaospyudf.so \
        ${install_main_dir}/driver/libtaospyudf.so &&
        ${csudo}chmod 777 ${install_main_dir}/driver/libtaospyudf.so
    ${csudo}ln -sf ${install_main_dir}/driver/libtaospyudf.so \
        ${lib_link_dir}/libtaospyudf.so
    if [ -d "${lib64_link_dir}" ]; then
        ${csudo}ln -sf ${install_main_dir}/driver/libtaospyudf.so \
            ${lib64_link_dir}/libtaospyudf.so
    fi
fi

# macOS
if [ -f ${binary_dir}/build/lib/libtaospyudf.dylib ]; then
    ${csudo}cp -Rf ${binary_dir}/build/lib/libtaospyudf.dylib \
        ${install_main_dir}/driver/libtaospyudf.dylib &&
        ${csudo}chmod 777 ${install_main_dir}/driver/libtaospyudf.dylib
    ${csudo}ln -sf ${install_main_dir}/driver/libtaospyudf.dylib \
        ${lib_link_dir}/libtaospyudf.dylib
fi
```

不使用版本号后缀（`udfd.c` 通过裸名加载）。

在 `install_lib()` 的 `remove_links()` 中新增 `"libtaospyudf.*"` pattern。

### 4.11 测试用例变更

现有测试 `source/taos-community/test/cases/12-UDFs/test_udf_main.py` 中的 `install_taospy()` 方法执行 `pip install taospyudf` + `ldconfig`。

变更后：
- 删除 `install_taospy()` 中安装 `taospyudf` 的逻辑
- 库已随 `make install` 安装到系统路径，测试直接使用即可

### 4.12 删除 source/taos-udf 外部仓库

源码迁移完成后：
- 删除 `source/taos-udf/` 目录
- 清理 `.gitmodules` 中对应的 submodule 引用（如有）

## 5. 性能

无性能影响。`libtaospyudf.so` 仅在用户首次使用 Python UDF 时被 `udfd` 动态加载，不影响 taosd 启动和正常查询写入路径。

编译时间增加约 5-15 秒（主要来自 C 扩展桥接代码编译和 ExternalProject 依赖准备）。首次构建需下载 Python SDK 与 plog 源码，后续缓存在 `.externals/` 中。

## 6. 安全

无新增安全风险。`libtaospyudf.so` 嵌入 CPython 解释器执行用户脚本，这一行为与变更前一致。Python UDF 的沙箱隔离（进程分离）由 `udfd` 保证，不在本次变更范围内。

## 7. 兼容性

| 方面 | 说明 |
| --- | --- |
| SQL 语法 | 无变化，`CREATE FUNCTION ... LANGUAGE 'Python'` 行为不变 |
| udfd 加载逻辑 | 单库加载：Linux/macOS 为 `libtaospyudf.so` / `.dylib`，Windows 为 `taospyudf.dll` |
| 配置参数 | 无变化，`UdfdLdLibPath` 配置仍有效 |
| 旧版安装 | 用户若已通过 `pip install taospyudf` 安装，系统路径中存在两份 so 文件，`udfd` 会加载 ld 搜索路径中先找到的那份，一般是安装包放置并 symlink 到系统 lib 的版本，行为正确 |

## 8. 运维

- **构建依赖**：编译机器无需安装 Python 或 `python3-dev`，Python SDK 自动从 GitHub 下载。仅需网络连接（首次构建时）
- **安装包体积**：Linux 增加约 500KB-1MB（单个 `libtaospyudf.so`）；Windows 增加约 500KB-1MB（单个 `taospyudf.dll`）
- **运行时依赖**：目标机器仍需安装 Python3 运行时（`python3`），但不再需要 `python3-dev`

## 9. 使用场景

1. **全新安装 TDengine 后使用 Python UDF**：安装后直接 `CREATE FUNCTION ... LANGUAGE 'Python'`，无需额外步骤
2. **开发编译**：`cmake -B debug && cmake --build debug`，`libtaospyudf.so`（Linux/macOS）或 `taospyudf.dll`（Windows）自动输出到 `debug/build/lib/`
3. **不需要 Python UDF**：`cmake -DBUILD_PYUDF=OFF` 跳过编译，不影响其他组件
4. **CI/CD**：构建流水线启用 `BUILD_PYUDF=ON`，安装包自动包含 Python UDF 支持，无需安装 Python 开发包
5. **Windows 自动匹配**：用户安装 Python 3.10–3.15 并加入 PATH，`taosudf.exe` 自动检测并加载。若 Python 不在可检测范围内，加载失败并输出错误日志
6. **切换构建基线 Python 版本**：`cmake -DBUILD_PYUDF_PYTHON_VERSION:STRING="3.15.0b1"`（默认）
7. **诊断 Python UDF 加载失败**：查看 `taosudf` 日志文件（`/var/log/taos/taosudf.log` 或 `C:\TDengine\log\taosudf.log`），日志中包含明确的错误原因和修复建议

## 10. 约束和限制

**约束**：
- 首次构建需网络连接以下载 Python SDK（每版本约 50MB），后续缓存在 `.externals/` 中
- C++17 编译器要求：GCC 9+、Clang 10+、MSVC 2019+

**限制**：
- Windows 需在 PATH 中可见 `python3XX.dll`（当前探测范围 3.9–3.15）
- macOS 平台尚未完全验证自动下载路径

## 11. 常见错误和排查

| 错误 | 原因 | 排查 |
| --- | --- | --- |
| `CMake Error: Download failed` (external.cmake) | 网络问题导致 Python SDK 下载失败 | 检查网络连通性，或手动下载 tar.gz 放入 `.externals/cpython/` 缓存目录 |
| `can not load library libtaospyudf.so` | 库未安装或 ld 路径错误 | 检查 `/usr/local/taos/driver/` 下是否存在，执行 `ldconfig` |
| `can not load library taospyudf.dll` | Windows 插件不在 DLL 搜索路径或 Python 运行时未就绪 | 检查 `C:\TDengine\bin\taospyudf.dll`，确认 Python 已安装并加入 PATH |
| `ModuleNotFoundError: No module named 'xxx'` | Python 第三方库不在 udfd 搜索路径 | 配置 `UdfdLdLibPath` 包含 site-packages 路径 |
| `ImportError: libpython3.x.so: cannot open shared object file` | 运行时缺少 Python 共享库 | 确认 Python 编译时启用了 `--enable-shared`，或安装 `libpython3.x` |
| Windows: python3XX.dll 未找到 | PATH 中没有可用 Python 运行时 | 安装 Python 3.10–3.15 并加入 PATH |

## 12. 可观测性

无影响。taos shell、taos Explorer、TDinsight 等 UI 组件无行为变化。

## 13. 安装和卸载

**安装**：
- `make install` 时，`libtaospyudf.so` 被复制到 `${install_main_dir}/driver/`，并在 `/usr/lib/` 创建软链接
- 安装包（deb/rpm/tar）需包含此文件

**卸载**：
- 卸载脚本需在 `remove_links()` 中新增 `"libtaospyudf.*"` 的清理 pattern
- 删除 `${install_main_dir}/driver/libtaospyudf.so`

## 14. 文档

- 需要修改官网文档 `docs/zh/07-develop/09-udf.md`：
  - 删除“准备环境”中 `pip3 install taospyudf` 和 `ldconfig` 步骤
  - 说明 Python UDF 插件已随 TDengine 安装包内置
  - 保留 Python 运行时（`python3`）的安装要求

## 15. 参考文档

- [TDengine UDF 开发文档](https://docs.taosdata.com/develop/udf/)
- [pybind11 CMake 集成文档](https://pybind11.readthedocs.io/en/stable/compiling.html)
- [Python C API（Stable ABI / Limited API）](https://docs.python.org/3/c-api/stable.html)
- [原 taospyudf PyPI 包](https://pypi.org/project/taospyudf/)

## 16. 附录

### 附录 A：涉及修改的文件清单

| 文件 | 操作 | 说明 |
| --- | --- | --- |
| `source/taos-community/cmake/options.cmake` | 修改 | 新增 `BUILD_PYUDF` option |
| `source/taos-community/cmake/external.cmake` | 修改 | 新增 `ext_cpython_3_15`、`ext_plog` ExternalProject 定义 |
| `source/taos-community/source/libs/CMakeLists.txt` | 修改 | 新增 `if(BUILD_PYUDF) add_subdirectory(pyudf)` |
| `source/taos-community/source/libs/pyudf/CMakeLists.txt` | 新增 | pyudf 编译脚本 |
| `source/taos-community/source/libs/pyudf/src/taospyudf.cpp` | 新增 | 从 `source/taos-udf/python/src/` 迁入并改为 CPython C API |
| `source/taos-community/source/libs/pyudf/src/taospyudf.h` | 新增 | 从 `source/taos-udf/python/src/` 迁入 |
| `source/taos-community/packaging/tools/make_install.sh` | 修改 | 新增 libtaospyudf 安装逻辑 |
| `source/taos-community/test/cases/12-UDFs/test_udf_main.py` | 修改 | 删除 `pip install taospyudf` 步骤 |
| `source/taos-udf/` | 删除 | 迁移完成后移除外部仓库 |

### 附录 B：Limited API 链接说明

当前实现基于 CPython C API + `Py_LIMITED_API`：

1. Python 头文件和 import lib 由 `ext_cpython_3_XX` ExternalProject 提供（自动下载）
2. Windows：链接 Python import lib（`python3.lib` / `python3XX.lib`）
3. Linux/macOS：不显式链接 libpython，使用 `--allow-shlib-undefined`，运行时通过 `dlopen(RTLD_GLOBAL)` 预加载

该策略在保持运行时动态发现能力的同时，降低了对特定 Python 小版本符号的强绑定风险。
