# TD-32652 默认开启 avx 编译验证 Test Spec

## 1. 测试目标

验证 TDengine 在 x86-64 平台默认开启了 avx/avx2 编译，且能够在支持指令集不同的新旧机器上运行。

## 2. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2024/11/27 | 1.0 | 邝金清 | 初稿 |
|  |  |  |  |

## 3. 测试范围

1. 指令集检查：验证所有 x86-64 机器都会默认编译相关函数的 avx 实现，且编译得到的二进制文件，在且仅在这几个特定函数中会用到 avx/avx2 指令。
2. 功能检查：任何一台 x86_64 机器编译出的包，都能够在其他 x86_64 机器上正常运行，无论支持指令集是否不同。

## 4. 测试结论

测试通过。

## 5. 已知问题和限制

无

## 6. 测试环境

- OS: Linux
- 3 台测试机器：
  - N1: 不支持 avx 指令和 avx2 指令
  - N2: 支持 avx 指令，但不支持 avx2 指令
  - N3: 同时支持 avx 指令和 avx2 指令
补充，这 3 台测试机器实际是 VirtualBox 虚拟机，通过以下命令指定的CPU指令集：
```shell
VBoxManage setextradata ${vm_name} VBoxInternal/CPUM/IsaExts/AVX 0
VBoxManage setextradata ${vm_name} VBoxInternal/CPUM/IsaExts/AVX2 1
```

## 7. 测试数据

<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: M4JMbUXLDozYckxSnDqcjhG8nTb)

</view>

<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: O55bbjJy3odxW3xoW3Mcclt5nlb)

</view>

<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: Ro3tb4a06oJonIxxLJjclANWnRb)

</view>

## 8. 测试用例

| No. | 测试目的 | 测试步骤 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- |
| 1 | 验证不支持 avx 指令的机器可以编译所有 avx 函数实现 | 1. 机器 N1 上编译 release 版 TDengine 1. ./check_avx_instruction /usr/bin/taosd 检查服务端用到的函数 1. ./check_avx_instruction /usr/lib/libtaos.so 检查客户端用到的函数 | 1. 正常编译 1. 服务端检查通过，且 $avx_func_lines > 0，$non_avx_func_lines > 0 1. 客户端检查通过，且 $avx_func_lines > 0，$non_avx_func_lines > 0 | 和预期结果相同，测试通过 |
| 2 | 验证支持 avx 但不支持 avx2 指令的机器可以编译所有 avx/avx2 函数实现 | 1. 机器 N2 上编译 release 版 TDengine 1. ./check_avx_instruction /usr/bin/taosd 检查服务端用到的函数 1. ./check_avx_instruction /usr/lib/libtaos.so 检查客户端用到的函数 | 1. 正常编译 1. 服务端检查通过，且 $avx_func_lines > 0，$non_avx_func_lines > 0 1. 客户端检查通过，且 $avx_func_lines > 0，$non_avx_func_lines > 0 | 和预期结果相同，测试通过 |
| 3 | 验证支持 avx 和 avx2 指令的机器可以编译所有 avx/avx2 函数实现 | 1. 机器 N3 上编译 release 版 TDengine 1. ./check_avx_instruction /usr/bin/taosd 检查服务端用到的函数 1. ./check_avx_instruction /usr/lib/libtaos.so 检查客户端用到的函数 | 1. 正常编译 1. 服务端检查通过，且 $avx_func_lines > 0，$non_avx_func_lines > 0 1. 客户端检查通过，且 $avx_func_lines > 0，$non_avx_func_lines > 0 | 和预期结果相同，测试通过 |
| 4 | 验证不支持 avx 指令的机器出包可以在所有指令集的x86_64机器上运行 | 1. 机器 N1 上编译 release 版 TDengine 1. 在机器 N1, N2, N3 上安装运行 1. taosBenchmark -f insert.json 导入测试数据 1. taos -f query.sql 执行相关查询 | 1. 所有机器都能正常安装运行 1. N1, N2, N3 的查询结果相同 1. N3 上的查询比 N1, N2 上快约 20% | 和预期结果相同，测试通过 |
| 5 | 验证支持 avx 但不支持 avx2 指令的机器出包可以在所有指令集的x86_64机器上运行 | 1. 机器 N1 上编译 release 版 TDengine 1. 在机器 N1, N2, N3 上安装运行 1. taosBenchmark -f insert.json 导入测试数据 1. taos -f query.sql 执行相关查询 | 1. 所有机器都能正常安装运行 1. N1, N2, N3 的查询结果相同 1. N3 上的查询比 N1, N2 上快约 20% | 和预期结果相同，测试通过 |
| 6 | 验证支持 avx 和 avx2 指令的机器出包可以在所有指令集的x86_64机器上运行 | 1. 机器 N1 上编译 release 版 TDengine 1. 在机器 N1, N2, N3 上安装运行 1. taosBenchmark -f insert.json 导入测试数据 1. taos -f query.sql 执行相关查询 | 1. 所有机器都能正常安装运行 1. N1, N2, N3 的查询结果相同 1. N3 上的查询比 N1, N2 上快约 20% | 和预期结果相同，测试通过 |

## 9. 参考文档

<!-- Unsupported block type: 999 -->
