# GDB 单线程调试技能

## 简介

`tsdb-test-gdb-single-thread-debug` 是一个面向单线程原生程序问题排查的 Agent Skill，适用于崩溃、卡死、结果错误、断言失败、状态损坏等场景。

它帮助 Agent 使用 GDB 从用户提供的现象、可选断点提示、core 文件或本地复现入口出发，逐步定位最可疑的代码位置，并输出带证据的调试结论。

## 能力范围

- 支持基于可执行文件、本地复现、core 文件进行调试
- 支持把用户提供的断点提示转换为可复用的 GDB 执行计划
- 优先采用只读调试方式，降低对现场和进程状态的影响
- 输出可疑代码位置、证据链、置信度和下一步建议

## 目录结构

```text
tsdb-test-gdb-single-thread-debug/
|-- SKILL.md
|-- README.zh-CN.md
|-- README.en.md
|-- agents/
|   `-- openai.yaml
|-- references/
|   |-- gdb-command-recipes.md
|   `-- gdb-risk-checklist.md
`-- scripts/
    `-- render_gdb_plan.py
```

## 主要文件

- `SKILL.md`：Skill 主定义，说明适用场景、输入输出、安全边界和标准调试流程
- `scripts/render_gdb_plan.py`：根据二进制、core 文件和断点提示生成 dry-run 调试计划与 `.gdb` 命令文件
- `references/gdb-command-recipes.md`：常见调试命令与场景配方
- `references/gdb-risk-checklist.md`：高风险操作确认清单
- `agents/openai.yaml`：Agent 相关配置

## 适用场景

推荐在以下场景使用：

- 单线程程序崩溃并生成了 core 文件
- 本地可以稳定复现 crash、hang 或 wrong result
- 需要围绕某个断点提示快速生成可执行的 GDB 排查计划
- 需要以较低风险方式先做只读分析

不适合直接用于：

- 多线程复杂并发问题
- 必须在线修改内存或强依赖副作用表达式的调试
- 无法获得二进制、core 或复现路径的场景

## 典型工作流

1. 收集目标二进制、参数、core 文件、现象描述和可选断点提示
2. 使用 `render_gdb_plan.py` 生成 dry-run 计划和 `.gdb` 命令文件
3. 先执行只读检查，例如栈、局部变量、参数和关键内存
4. 逐步缩小到第一个从“正常”变为“异常”的状态转折点
5. 输出带证据的可疑代码位置和下一步探针建议

## 下一步计划

下一阶段将把当前 GDB 调试能力与编译、测试、日志分析、core 收集和修复建议等能力进一步串联起来，形成更完整的自动化排障闭环。

目标方向包括：

- 与编译和符号产物生成流程打通，减少调试前置准备成本
- 与测试和复现流程串联，支持问题复现后自动进入定位阶段
- 结合 core 分析、日志分析和代码变更建议，形成更稳定的问题修复链路
- 逐步演进为能够自动修复 core 问题的机器人

## 开发信息

- Author: Tony Zhang
- Owner Team: Query Group, Engine Group
- Version: 0.1.0