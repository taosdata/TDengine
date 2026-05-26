# tsdb-build-taosgen

用于指导 Agent 协助用户在 Linux/macOS 环境中从源码构建、测试并安装 taosgen。

## 技能简介

该技能聚焦 taosgen 构建链路，覆盖：
- CMake 配置与构建
- Conan 依赖安装与工具链接入
- 编译报错定位与修复建议
- 测试执行（CTest）与安装步骤
- 平台差异处理（Linux / macOS，x64 / ARM64）

## 适用场景

当用户出现以下诉求时建议使用本技能：
- “如何编译 taosgen / build taosgen”
- “conan install 失败 / cmake 报错”
- “macOS SDK 找不到 / 编译器版本不兼容”
- “想在本机安装 taosgen 并验证可用”

## 输入信息（建议）

为提高定位效率，建议先收集：
- 操作系统与架构（Linux/macOS，x64/ARM64）
- CMake、Conan、编译器版本
- 完整报错片段（命令 + 首个失败栈）
- 构建目标（Debug/Release、是否需要安装）

## 输出内容

技能通常输出以下结果：
1. 问题诊断摘要（失败环节与原因）
2. 可直接执行的修复命令（按平台给出）
3. 验证步骤（如 `cmake --build .`、`ctest`）
4. 若仍失败，下一步排查清单

## 常用流程（示例）

```bash
# 1) 安装依赖
conan install .. --build=missing --output-folder=./conan --settings=build_type=Release

# 2) 配置
cmake .. -DCMAKE_BUILD_TYPE=Release

# 3) 构建
cmake --build .

# 4) 测试（可选）
ctest
```

## 安全与注意事项

- 执行 `sudo` 命令前应明确说明目的。
- 优先提供可回滚、最小影响的修复方案。
- 避免直接给出破坏性命令（如强制删除系统级工具链）。
- 不请求或暴露任何凭据、密钥、令牌信息。

## 文件说明

- `SKILL.md`：Agent 实际执行的完整指令（主文件）
- `README.md`：给团队成员的快速说明（本文件）
