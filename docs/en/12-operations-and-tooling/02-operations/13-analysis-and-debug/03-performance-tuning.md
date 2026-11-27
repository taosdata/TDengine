---
sidebar_label: Memory Optimization
title: Memory Optimization
toc_max_heading_level: 4
---

TDengine supports platform-specific memory allocator selection: Linux uses the `set_taos_malloc.sh` configuration script, and Windows packages that include the mimalloc tools provide `set_taos_malloc.ps1` to enable mimalloc on demand. Changing allocators may improve allocation and deallocation performance; evaluate the results with your workload.

## Linux: Memory Allocator Configuration Script

`set_taos_malloc.sh` is a script provided by TDengine to quickly switch the memory allocation strategy for services such as taosd and taosadapter.

### Features

- Supports multiple modes: glibc default allocator, tcmalloc, jemalloc, etc.
- Automatically generates shell environment variable files and systemd environment variable files.
- Supports silent mode (no output messages).

### Basic Usage

#### Parameter Description

- `-m <mode>`  
  Specify the memory allocator mode.  
  - 0: glibc default allocator  
  - 1: tcmalloc optimization  
  - 2: tcmalloc custom check, for memory leak detection  
  - 3: jemalloc optimization  
  - 4: jemalloc custom check, for memory leak detection

#### Result Description

- The script will automatically generate or update the following environment variable files:
  - Shell environment variable file: `/usr/local/taos/bin/set_taos_malloc_env.sh`
  - systemd taosd environment variable file: `/etc/default/taosd`
  - systemd taosadapter environment variable file: `/etc/default/taosadapter`
- Log file: `/var/log/taos/set_taos_malloc.log`

`/usr/local/taos` and `/var/log/taos` are the default installation and log paths for TDengine.

### Notes

- After modification, you need to restart the taosd/taosadapter service or re-source the environment variable file.
- Only supported when running in the TDengine installation directory `/usr/local/taos/bin/`.

For more advanced usage, please refer to the script comments or contact operations support.

## Windows: Optional mimalloc

### Use Cases and Limitations

mimalloc is a separate memory allocator worth evaluating for frequent allocations and deallocations, multithreaded or cross-thread object deallocation, and heap lock contention when freeing many objects at once. It may reduce allocation and deallocation overhead and improve fragmentation and memory reclamation for some workloads.

mimalloc does not fix memory leaks, unbounded caches, or task backlogs, and it does not guarantee lower memory usage for every workload. It does not change application threads that perform resource cleanup synchronously.

This section applies to Windows x64 packages that include the mimalloc tools. mimalloc redirects allocation and deallocation interfaces in the dynamic C runtime (CRT); it does not take over memory allocated directly through Windows `HeapAlloc` or other allocators. Modifying only `taosd.exe` does not automatically enable mimalloc for other processes such as taosadapter.

### Default Behavior and Prerequisites

The original `taosd.exe` in the installation package does not load mimalloc by default. Placing the DLLs in the installation directory does not enable it, and no change to `taos.cfg` is required. For custom builds, `BUILD_MIMALLOC` controls whether the tools are built and packaged, not whether mimalloc is enabled at runtime.

Release packages use the following files. Keep the runtime DLLs beside `taosd.exe`; there is no need to copy them to Windows system directories:

- `mimalloc.dll`: The memory allocator.
- `mimalloc-redirect.dll`: The dynamic CRT redirection component.
- `minject.exe`: The tool that modifies the EXE import table.
- `set_taos_malloc.ps1`: The script for enabling, restoring, and checking the allocator.
- `mimalloc-LICENSE.txt`: The license text.

Use files from the same installation package. If the tools are missing, first check whether the package supports this feature. The script detects `mimalloc.dll` or `mimalloc-debug.dll` automatically. Keep only the DLL matching your build; do not mix Debug and Release files.

### Enable, Restore, or Check the Allocator

Use Windows PowerShell 5.1 or later with permission to modify the installation directory. Stop taosd before switching; the script refuses to modify a running target and does not stop or start services for you.

In PowerShell, change to the directory containing `taosd.exe` (replace the example path), then choose an operation:

```powershell
Set-Location C:\TDengine
.\set_taos_malloc.ps1 -Mode mimalloc  # Enable
.\set_taos_malloc.ps1 -Status         # Inspect the configured allocator
.\set_taos_malloc.ps1 -Mode default   # Restore the original executable
```

Run only the operation you need, then start taosd as usual. If local execution policy blocks the script, follow your organization's script execution policy. For a renamed executable, add `-ExecutablePath .\your-server.exe`.

The script patches and verifies a temporary copy before replacing the EXE. It creates `taosd.exe.before-mimalloc` and `taosd.exe.mimalloc.json` beside the executable, verifies their SHA-256 hashes before reuse, and preserves them when restoring the default allocator. Repeating the same mode does not overwrite the original backup. Keep both files for rollback.

`-Status` reads the EXE import table without changing files. It reports the configured allocator, not whether a running process has successfully redirected CRT calls. The change takes effect on the next process start.

### Verify Activation and Evaluate Results

An import table containing mimalloc only confirms that the EXE was modified; it does not prove that runtime redirection succeeds. Keep the taosd service stopped, set the diagnostic environment variable in the current PowerShell session, and start a foreground process with the same configuration directory as the service:

```powershell
$env:MIMALLOC_VERBOSE = '1'
.\taosd.exe -c C:\TDengine\cfg
```

Replace the configuration directory with the actual path and ensure that `MIMALLOC_DISABLE_REDIRECT=1` is not set in the process environment. Check startup diagnostics for successful CRT redirection and any DLL loading or redirection errors. Seeing a DLL name alone is not sufficient to establish success.

After verification, use `Ctrl+C` to stop the foreground process and confirm that it has exited. Clear the temporary diagnostic variable, then start the service as usual:

```powershell
Remove-Item Env:\MIMALLOC_VERBOSE
```

Temporary PowerShell environment variables affect only child processes started directly from that session. They are not automatically passed to a Windows service started with `Start-Service`. To inspect startup diagnostics from the service process itself, configure its environment and capture its output through the actual service management mechanism. Verbose diagnostics are not required for normal production operation.

Compare query response time, resource cleanup duration, CPU usage, process Private Bytes, and peak working set before and after activation, using the same data volume, queries or recalculation tasks, and concurrency. A single memory percentage reading in Task Manager is not sufficient to evaluate the results.

### Rollback and Upgrades

To roll back, stop taosd, run `-Mode default`, and restart it. This restores the exact original EXE; the DLLs can remain beside it. Deleting the DLLs while the EXE is still patched can prevent startup. `MIMALLOC_DISABLE_REDIRECT=1` disables redirection but does not remove the DLL dependency.

Before upgrading, restore the default allocator and archive the backup and JSON state together outside the installation directory. After upgrading, use the new package's script and tools to enable mimalloc again if needed. The script rejects a changed EXE or mismatched backup to avoid restoring an older version. If backup/state files are missing or damaged, recover the original EXE from the matching installation package rather than using an older backup. Executables previously patched manually must also be restored from their matching original package before using the script.

For tool options and details of redirection, see the [mimalloc Windows instructions](https://github.com/microsoft/mimalloc/blob/v3.6.1/bin/readme.md).
