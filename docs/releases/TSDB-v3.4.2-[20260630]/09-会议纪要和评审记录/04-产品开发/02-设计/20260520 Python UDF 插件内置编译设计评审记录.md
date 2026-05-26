# 20260526 Python UDF 插件内置编译设计评审记录

## 1. 评审信息

1. 评审目的：评估 "Python UDF 插件内置编译 FS" 设计的合理性、安全性、性能、兼容性及可维护性
2. 评审文档：[Python UDF 插件内置编译 FS](../../../05-设计文档/Python%20UDF%20插件内置编译%20FS.md)
3. 会议主持：关胜亮
4. 会议人员：关胜亮、霍琳贺、陈浩然、肖波、金明磊
5. 会议时间：2026-05-20 09:40 - 09:50
6. 会议形式：线下
7. 会议地点：taosX
8. 会议记录人：关胜亮

## 2. 评审记录

评审团队对设计文档（Python UDF 插件内置编译 FS）进行了全面审查，认为整体设计贴合降低 Python UDF 使用门槛与简化安装部署需求、逻辑严谨、可落地性强，具体评审意见如下：
1. 设计目标清晰精准，核心痛点定位明确，紧扣当前 `taospyudf` 作为独立 PyPI 包发布导致用户需手动执行 `pip install` + `ldconfig` 且依赖系统 C++ 编译环境（cmake/gcc/MSVC）、Windows 环境几乎无法使用 Python UDF 的高门槛痛点，明确核心目标为将 `taospyudf` 源码迁入主工程 `source/taos-community/` 作为子模块编译、产物随安装包发布实现安装即可用、通过 python-build-standalone 自动下载 Python SDK 实现编译期零依赖、Windows 采用单库模式运行时自动检测系统 Python 版本，目标聚焦、指引明确。
2. 功能设计全面细致，可落地性强，覆盖核心业务场景：构建行为设计完整（`BUILD_PYUDF` 选项默认 ON/`BUILD_PYUDF_PYTHON_VERSION` 指定 SDK 版本/未设置触发 FATAL_ERROR/ExternalProject 自动下载 Python SDK 和 plog）、安装行为跨平台覆盖（Linux 安装到 driver 目录并创建 `/usr/lib/` 软链接/Windows 安装到 `C:\TDengine\bin\`/macOS 安装到 driver 目录）、运行时自动发现设计精细（Windows 按 3.15→3.9 搜索 `python3XX.dll` 并推导 PYTHONHOME/Linux 通过 `RTLD_GLOBAL` 预加载 libpython 使符号全局可见）、混合链接策略合理（Linux 不链接 libpython 实现单 so 兼容 3.9-3.15/Windows 链接 import lib + Limited API 降低小版本敏感性）、源码目录结构清晰（`source/libs/pyudf/` 新目录/external.cmake 新增依赖定义/options.cmake 新增选项）、CMakeLists.txt 核心宏逻辑完整（`_pyudf_add_target` 覆盖 include/compile definitions/link 全流程）、错误日志与诊断覆盖 5 类场景含明确错误消息和修复建议、Windows .rc 版本信息自动生成、安装脚本和卸载脚本均有对应变更、测试用例同步简化删除 pip install 步骤、外部仓库归档清理方案明确，设计闭环完整。
3. 设计文档结构规范，版本与修订记录清晰：文档包含四版修订记录（1.0 初稿→1.1 混合链接策略与 Python 运行时自动发现→1.2 多版本自动下载编译期零依赖→1.3 切换至单库加载 + CPython C API Limited API）、背景（现状问题 5 项 + 目标 4 项）、定义（7 项术语）、行为说明（构建/安装/用户流程变化/源码目录/ExternalProject 依赖/CMakeLists 核心逻辑/运行时自动发现/错误日志/Windows RC/安装脚本/测试用例/外部仓库删除共 12 大子节）、性能、安全、兼容性、运维、使用场景（7 项）、约束和限制、常见错误和排查（6 类）、可观测性、安装和卸载、文档、参考文档、附录（修改文件清单 + Limited API 链接说明）共 16 大章节，层次分明、约束与限制界定清晰，逻辑清晰、无歧义，符合 TDengine 设计文档规范要求。
4. 安全性、兼容性与性能考虑周全，风险可控：安全方面无新增攻击面、`libtaospyudf.so` 嵌入 CPython 执行用户脚本的行为与变更前一致、进程隔离由 udfd 保证；兼容性方面 SQL 语法无变化（`CREATE FUNCTION ... LANGUAGE 'Python'` 行为不变）、`UdfdLdLibPath` 配置仍有效、用户已通过 pip 安装的旧版本共存时 udfd 加载系统 lib 目录中安装包放置的版本行为正确、单库加载名称固定不带版本号后缀；性能方面对 taosd 启动和正常查询写入路径无影响（仅首次使用 Python UDF 时 udfd 动态加载）、编译时间增加约 5-15 秒且首次构建后缓存在 `.externals/` 中、安装包体积增加约 500KB-1MB。

## 3. 评审结论

设计文档整体设计合理、逻辑清晰，功能覆盖全面，Python UDF 插件通过迁入主工程编译并随安装包发布实现了安装即可用、通过 python-build-standalone 自动下载 SDK 实现了编译期零依赖、通过 CPython Limited API 单库模式结合运行时自动发现实现了跨 Python 版本兼容，性能、安全、兼容性设计符合系统规范，精准解决了用户安装 Python UDF 插件门槛高（需编译工具链 + pip install + ldconfig）且 Windows 几乎无法使用的核心痛点。

## 4. 后续行动项

无
