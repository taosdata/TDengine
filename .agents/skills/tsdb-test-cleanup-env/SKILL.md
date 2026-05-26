---
name: tsdb-test-cleanup-env
description: "Clean up TDengine test environment. Use when: resetting test env, killing taosd processes, clearing sim directory, deleting test run directory, preparing for a fresh test run. Supports both Linux and Windows. Keywords: cleanup, test environment, taosd, sim directory"
metadata:
  author: bmzhang
  version: 1.0.0
  owner_team: engine
---

# Cleanup Test Environment

Kill all running taosd processes, remove all files under the sim directory, and delete the test run directory. Supports both Linux and Windows.

## When to Use

- Before or after running tests to ensure a clean environment
- When taosd processes are stuck or need to be force-killed
- When you need to reset the sim directory to a clean state

## Procedure

### 1. Detect the Operating System

Check the current OS to determine which commands to use. Use platform detection from the terminal (e.g., `uname` on Linux, `$env:OS` on Windows PowerShell).

### 2. Kill All taosd Processes

There may be multiple taosd processes running. Force-kill all of them.

**Linux:**
```bash
pkill -9 taosd || true
# Verify no taosd processes remain
pgrep taosd || echo "All taosd processes killed"
```

**Windows (PowerShell):**
```powershell
Get-Process -Name taosd -ErrorAction SilentlyContinue | Stop-Process -Force
# Verify no taosd processes remain
if (-not (Get-Process -Name taosd -ErrorAction SilentlyContinue)) { Write-Host "All taosd processes killed" }
```

### 3. Clean the sim Directory

Remove all files and subdirectories inside the sim directory. Do NOT delete the sim directory itself.

**Linux:**
```bash
rm -rf /home/zbm/td/sim/*
ls /home/zbm/td/sim/  # Verify it is empty
```

**Windows (PowerShell):**
```powershell
if (Test-Path "D:\td\sim") { Get-ChildItem -Path "D:\td\sim" -Force | Remove-Item -Recurse -Force }
Get-ChildItem -Path "D:\td\sim" -Force  # Verify it is empty
```

### 4. Delete the test run Directory

Remove the entire test run directory (the directory itself and all contents).

**Linux:**
```bash
rm -rf /home/zbm/td/community/test/run
test ! -d /home/zbm/td/community/test/run && echo "test/run directory deleted"
```

**Windows (PowerShell):**
```powershell
if (Test-Path "D:\td\community\test\run") { Remove-Item -Path "D:\td\community\test\run" -Recurse -Force }
if (-not (Test-Path "D:\td\community\test\run")) { Write-Host "test/run directory deleted" }
```

### 5. Confirm Cleanup

Print a summary confirming:
- All taosd processes have been killed
- The sim directory is now empty
- The test run directory has been deleted
## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-test-cleanup-env version=0.1.0 author=bmzhang`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

