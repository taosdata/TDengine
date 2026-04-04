---
sidebar_label: Debugging Tools
title: Debugging Tools
toc_max_heading_level: 4
---
The following tools are recommended for debugging TDengine TSDB.

## gdb

GDB (GNU Debugger) is a powerful command-line debugger for C, C++, and other languages.

## valgrind

Valgrind is a framework for memory debugging, leak detection, and profiling. It helps detect memory errors, thread errors, and performance issues.

## bpftrace

bpftrace is a high-level dynamic tracing tool based on eBPF, used for performance analysis and troubleshooting on Linux.

## perf

perf is a Linux performance analysis tool that provides detailed insight into system and application behavior to help identify performance bottlenecks.

## Core Dump Files

When taosd crashes, a core dump file is generated. The location differs by operating system.

| OS | Location | How to load |
|----|----------|-------------|
| Linux | Path defined by `sysctl kernel.core_pattern` | `gdb /usr/bin/taosd core.12345` |
| macOS | `/cores/core.<PID>` | `lldb /usr/local/bin/taosd -c /cores/core.12345` |
| Windows | Stack log (~KB), same dir as taosd.exe: `taosd_YYYYMMDD_HHMMSS_stack.log` | Open with any text editor |
| Windows | MiniDump (~20–80 MB), same dir as taosd.exe: `taosd_YYYYMMDD_HHMMSS.dmp` | WinDbg + PDB |
| Windows | WER full dump (hundreds of MB–GB), `%LOCALAPPDATA%\CrashDumps\` | WinDbg + PDB |
