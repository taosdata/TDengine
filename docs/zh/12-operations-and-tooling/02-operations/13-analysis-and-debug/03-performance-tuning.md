---
sidebar_label: 内存优化
title: 内存优化
toc_max_heading_level: 4
---

TDengine 支持按平台选择内存分配器：Linux 使用 `set_taos_malloc.sh` 配置脚本；包含 mimalloc 工具的 Windows 安装包提供 `set_taos_malloc.ps1` 按需启用 mimalloc。更换分配器可能改善内存分配和释放性能，实际效果需要通过业务负载验证。

## Linux：内存分配器配置脚本

`set_taos_malloc.sh` 是 TDengine 提供的内存分配器配置脚本，用于一键切换 taosd/taosadapter 等服务的内存分配策略。

### 作用

- 支持 glibc 默认分配器、tcmalloc、jemalloc 等多种模式。
- 可自动生成 shell 环境变量文件和 systemd 环境变量文件。
- 支持静默模式（不输出提示信息）。

### 基本用法

#### 参数说明

- `-m <mode>`  
  指定内存分配器模式。  
  - 0：glibc 默认分配器  
  - 1：tcmalloc 优化  
  - 2：tcmalloc 定制检查，用于内存泄漏检测
  - 3：jemalloc 优化  
  - 4：jemalloc 定制检查，用于内存泄漏检测

#### 结果说明

- 脚本会自动生成/更新如下环境变量文件：
  - Shell 环境变量文件：`/usr/local/taos/bin/set_taos_malloc_env.sh`
  - systemd taosd 环境变量文件：`/etc/default/taosd`
  - systemd taosadapter 环境变量文件：`/etc/default/taosadapter`
- 日志文件：`/var/log/taos/set_taos_malloc.log`

`/usr/local/taos` 和 `/var/log/taos` 是 TDengine 默认的安装路径和日志路径。

### 注意事项

- 修改后，需要重启 taosd/taosadapter 服务或重新 source 环境变量文件让配置生效。
- 仅支持在 TDengine 安装目录`/usr/local/taos/bin/`下运行。

如需更多高级用法，请参考脚本内注释或联系运维支持。

## Windows：可选 mimalloc

### 适用场景和限制

mimalloc 是独立的内存分配器，适合评估以下场景：频繁分配和释放内存、多线程或跨线程释放对象，以及集中释放大量对象时出现堆锁竞争。它可能降低分配和释放开销，并改善部分负载下的内存碎片和回收表现。

mimalloc 不能修复内存泄漏、无界缓存或任务积压，也不保证降低每种负载的内存占用。它不会改变业务线程同步执行资源清理的行为。

本节适用于包含 mimalloc 工具的 Windows x64 安装包。mimalloc 重定向动态 C 运行时库（CRT）的分配和释放接口，不接管直接调用 Windows `HeapAlloc` 或使用其他分配器的内存。仅修改 `taosd.exe` 不会自动为 taosadapter 等其他进程启用 mimalloc。

### 默认行为和准备工作

安装包中的原始 `taosd.exe` 默认不加载 mimalloc。仅将 DLL 放在安装目录中不会启用它，也无需修改 `taos.cfg`。自行构建时，`BUILD_MIMALLOC` 控制是否构建和打包工具，不表示运行时已经启用。

正式发布包使用以下文件，运行时 DLL 应与 `taosd.exe` 位于同一目录，无需复制到 Windows 系统目录：

- `mimalloc.dll`：内存分配器。
- `mimalloc-redirect.dll`：动态 CRT 重定向组件。
- `minject.exe`：修改 EXE 导入表的工具。
- `set_taos_malloc.ps1`：启用、恢复和查询分配器配置的脚本。
- `mimalloc-LICENSE.txt`：许可证文本。

使用同一安装包中的文件。如果缺少工具，请先确认安装包是否支持该功能。脚本自动识别 `mimalloc.dll` 或 `mimalloc-debug.dll`，只保留与当前构建配套的 DLL，不要混用 Debug 和正式发布包的文件。

### 启用、恢复和查询分配器

使用 Windows PowerShell 5.1 或更高版本，并确保具有安装目录的修改权限。切换前先停止 taosd；脚本会拒绝修改正在运行的目标进程，不会自动停止或启动服务。

在 PowerShell 中进入包含 `taosd.exe` 的目录（请替换示例路径），然后选择需要的操作：

```powershell
Set-Location C:\TDengine
.\set_taos_malloc.ps1 -Mode mimalloc  # 启用
.\set_taos_malloc.ps1 -Status         # 查询分配器配置
.\set_taos_malloc.ps1 -Mode default   # 恢复原始 EXE
```

只执行所需的操作，完成后按原方式启动 taosd。如果执行策略阻止脚本运行，请按所在组织的脚本执行策略处理。对于改名后的 EXE，可增加 `-ExecutablePath .\your-server.exe` 参数。

脚本先修改并校验临时副本，再替换 EXE。它会在同目录创建 `taosd.exe.before-mimalloc` 和 `taosd.exe.mimalloc.json`，复用前校验 SHA-256 哈希，恢复默认分配器后仍保留这两个文件。重复设置相同模式不会覆盖原始备份。请一起保留这两个文件以便回退。

`-Status` 只读取 EXE 导入表，不修改文件。它显示分配器配置，不代表运行中的进程已经成功重定向 CRT 调用。切换在下次启动进程时生效。

### 验证生效和评估效果

导入表包含 mimalloc 只说明 EXE 已修改，不代表运行时重定向一定成功。保持 taosd 服务停止，在当前 PowerShell 中设置诊断环境变量，使用与服务相同的配置目录启动前台进程：

```powershell
$env:MIMALLOC_VERBOSE = '1'
.\taosd.exe -c C:\TDengine\cfg
```

将配置目录替换为实际路径，并确认进程环境中没有设置 `MIMALLOC_DISABLE_REDIRECT=1`。检查启动诊断中是否报告 CRT 重定向成功，以及是否有 DLL 加载或重定向错误；仅看到 DLL 名称不足以判断成功。

验证后使用 `Ctrl+C` 退出前台进程，确认退出完成，再清除临时诊断变量并按原方式启动服务：

```powershell
Remove-Item Env:\MIMALLOC_VERBOSE
```

PowerShell 的临时环境变量仅影响从该会话直接启动的子进程，不会自动传给通过 `Start-Service` 启动的 Windows 服务。若需要验证服务进程本身的启动诊断，应通过实际服务管理方式设置其环境变量并收集输出。生产运行不需要持续开启详细诊断。

在相同数据量、查询或重算任务及并发条件下，对比启用前后的查询响应时间、资源清理耗时、CPU 占用、进程专用字节数（Private Bytes）和工作集峰值。不要仅凭一次任务管理器内存百分比判断效果。

### 回退和升级

回退时先停止 taosd，执行 `-Mode default` 后再启动。该操作恢复完全一致的原始 EXE，DLL 可以继续保留。EXE 尚未恢复时直接删除 DLL 可能导致启动失败。`MIMALLOC_DISABLE_REDIRECT=1` 仅禁用重定向，不移除 DLL 依赖。

升级前先恢复默认分配器，并将备份和 JSON 状态文件一起移出安装目录归档。升级后如需启用 mimalloc，使用新安装包的脚本和工具重新操作。脚本会拒绝处理已变化的 EXE 或不匹配的备份，避免回退到旧版本。如果备份或状态文件丢失、损坏，应从对应版本的安装包恢复原始 EXE，不要使用旧版本备份。此前手动修改过的 EXE，也应先从对应原始安装包恢复，再使用脚本。

工具参数和重定向机制详见 [mimalloc Windows 使用说明](https://github.com/microsoft/mimalloc/blob/v3.6.1/bin/readme.md)。
