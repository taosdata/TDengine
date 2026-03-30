---
sidebar_label: 分析调试
title: 分析调试
toc_max_heading_level: 4
---
为了更好的分析调试 TDengine TSDB，推荐开发者在操作系统中安装以下分析调试工具：

## gdb

GDB（GNU Debugger）是一个功能强大的命令行调试器，广泛用于调试 C、C++ 和其他编程语言的程序。

## valgrind

valgrind 是一个用于内存调试、内存泄漏检测和性能分析的工具框架。Valgrind 提供了一组工具，帮助开发者检测和修复程序中的内存错误、线程错误和性能问题。

## bpftrace  

bpftrace 是一个高级的动态跟踪工具，基于 eBPF（Extended Berkeley Packet Filter）技术，用于在 Linux 系统上进行性能分析和故障排除。

## perf

perf 是一个强大的 Linux 性能分析工具。它提供了对系统和应用程序的详细性能分析，帮助开发者和系统管理员识别和解决性能瓶颈。

## core dump 文件

taosd 崩溃时会生成 core dump 文件，不同操作系统生成位置不一样。

| 操作系统 | core dump 生成位置 | 加载示例 |
|---------|----------------|-----------|
| Linux   | `sysctl kernel.core_pattern` 定义路径 | gdb /usr/lib/taos/taosd core.12345 |
| MacOs   | `/cores/core.<PID>` | lldb /usr/local/bin/taosd -c /cores/core.12345 |
| Windows | 崩溃栈信息，十几 KB，taosd 所有目录下，格式：taosd_年月日_时分秒_stack.log| 记事本打开查看|
|         | Mindump，通常约 20–80 MB，taosd 所有目录下，格式：taosd_年月日_时分秒.dmp| WinDbg + PDB 文件|
|         | WER 全量 Dump，数百 MB ~ GB，系统配置目录下 | WinDbg + PDB 文件 |

