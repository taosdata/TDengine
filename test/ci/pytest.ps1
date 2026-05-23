##################################################
#
#  Run pytest on Windows (no ASAN)
#  Usage: .\ci\pytest.ps1 pytest cases\01-DataTypes\test_datatype_bigint.py
#
##################################################

$ErrorActionPreference = "Stop"

function Remove-DirBestEffort {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [int]$MaxRetries = 5,
        [int]$RetryDelayMs = 400
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }

    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            Remove-Item -LiteralPath $Path -Recurse -Force -ErrorAction Stop
            return
        }
        catch {
            # Best effort fallback: skip locked entries, remove what can be removed.
            $items = Get-ChildItem -LiteralPath $Path -Recurse -Force -ErrorAction SilentlyContinue |
                Sort-Object { $_.FullName.Length } -Descending

            foreach ($item in $items) {
                try {
                    Remove-Item -LiteralPath $item.FullName -Force -Recurse -ErrorAction Stop
                }
                catch {
                    # Ignore locked file/dir and continue cleanup.
                }
            }

            try {
                Remove-Item -LiteralPath $Path -Force -Recurse -ErrorAction Stop
                return
            }
            catch {
                if ($attempt -lt $MaxRetries) {
                    Start-Sleep -Milliseconds $RetryDelayMs
                }
            }
        }
    }

    Write-Warning "Cleanup warning: '$Path' still has locked items; continuing with best-effort cleanup."
}

if ($args.Count -eq 0) {
    Write-Error "Usage: .\ci\pytest.ps1 <command> [args...]"
    exit 2
}

# --- Locate directories ---
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$TestCodeDir = Split-Path -Parent $ScriptDir
$TopDir = (Resolve-Path "$TestCodeDir\..\..\..").Path

# Find BUILD_DIR
$BuildDir = Join-Path $TopDir "debug"
$TaosdExe = Get-ChildItem -Path "$BuildDir\build\bin" -Filter "taosd.exe" -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $TaosdExe) {
    Write-Host "ERROR: Cannot find taosd.exe under $BuildDir\build\bin\"
    Write-Host "Please build TDengine first."
    exit 1
}

# --- Kill stale TDengine processes to avoid port/config collisions ---
$procNames = @("taosd", "taosadapter", "taoskeeper")
foreach ($name in $procNames) {
    $procs = Get-Process -Name $name -ErrorAction SilentlyContinue
    if ($procs) {
        Write-Host "Stopping existing $name process(es): $($procs.Count)"
        $procs | Stop-Process -Force -ErrorAction SilentlyContinue
    }
}

# SIM_DIR should live under repository root
$SimDir = Join-Path $TopDir "sim"
$RunDir = Join-Path $SimDir "run"

Write-Host "------------------------------------------------------------------------"
Write-Host "Start TDengine Testing Case ..."
Write-Host "BUILD_DIR: $BuildDir"
Write-Host "SIM_DIR  : $SimDir"
Write-Host "TEST_CODE_DIR : $TestCodeDir"

# --- Clean and create sim directories ---
if (Test-Path $SimDir) { Remove-DirBestEffort -Path $SimDir }
New-Item -ItemType Directory -Path $SimDir -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $SimDir "tsim") -Force | Out-Null
New-Item -ItemType Directory -Path $RunDir -Force | Out-Null

# --- Set environment for the test framework ---
$env:BUILD_DIR = $BuildDir
$env:SIM_DIR = $SimDir
$env:TEST_CODE_DIR = $TestCodeDir
$TaosBinPath = Join-Path $BuildDir "build\bin"
$env:TAOS_BIN_PATH = $TaosBinPath
$env:TAOS_DLL_PATH = $TaosBinPath
$env:WORK_DIR = $SimDir
# Speed up teardown on Windows when taosd does not exit promptly after CTRL_BREAK.
$env:TAOSD_STOP_TIMEOUT_SEC = "1"

function Add-PathIfExists {
    param([string]$PathToAdd)
    if ([string]::IsNullOrWhiteSpace($PathToAdd)) { return }
    if (-not (Test-Path -LiteralPath $PathToAdd)) { return }
    if ($env:PATH -notlike "$PathToAdd*") {
        $env:PATH = "$PathToAdd;$env:PATH"
    }
}

# Ensure local debug binaries (taos.dll/taos.exe/taosd.exe) are resolved first.
if ($env:PATH -notlike "$TaosBinPath*") {
    $env:PATH = "$TaosBinPath;$env:PATH"
}

# Ensure Python runtime DLLs are visible to taospyudf.dll in fresh shells.
# Keep this Python-only and auto-discovered (no hardcoded VS/Windows SDK paths).
try {
    $PyExePath = (& python -c "import sys;print(sys.executable)" 2>$null | Select-Object -First 1).Trim()
    if ($PyExePath -and (Test-Path -LiteralPath $PyExePath)) {
        $PyRuntimeDir = Split-Path -Parent $PyExePath
        Add-PathIfExists -PathToAdd $PyRuntimeDir

        # Keep Python interpreter context explicit for child processes (udfd/taospyudf).
        $env:PYTHONHOME = $PyRuntimeDir

        # Best-effort: discover extra DLL directories near Python runtime.
        # This helps when Python distribution bundles runtime dependencies in subfolders.
        $PyDllCandidates = @("python3.dll", "python314.dll", "python315.dll", "ucrtbased.dll", "vcruntime140d.dll", "msvcp140d.dll")
        foreach ($dll in $PyDllCandidates) {
            $hit = Get-ChildItem -LiteralPath $PyRuntimeDir -Recurse -Filter $dll -File -ErrorAction SilentlyContinue | Select-Object -First 1
            if ($hit) {
                Add-PathIfExists -PathToAdd $hit.Directory.FullName
            }
        }
    }
}
catch {
    # Best effort only; keep existing behavior if probing fails.
}

# Python 3.8+ on Windows uses secure DLL loading, and ctypes LoadLibrary("taos")
# may not search PATH. Inject a temporary startup hook so every Python process
# (including pytest entrypoint) calls os.add_dll_directory(TAOS_DLL_PATH).
$PyHookDir = Join-Path $RunDir "pyhook"
New-Item -ItemType Directory -Path $PyHookDir -Force | Out-Null
$PyHookFile = Join-Path $PyHookDir "sitecustomize.py"
@'
import os

_p = os.environ.get("TAOS_DLL_PATH")
if _p and os.path.isdir(_p):
    try:
        os.add_dll_directory(_p)
    except Exception:
        pass
'@ | Set-Content -Path $PyHookFile -Encoding UTF8

if ([string]::IsNullOrEmpty($env:PYTHONPATH)) {
    $env:PYTHONPATH = $PyHookDir
} else {
    $env:PYTHONPATH = "$PyHookDir;$env:PYTHONPATH"
}

# --- Change to test code directory ---
Set-Location $TestCodeDir

# --- Build command from arguments and run ---
$Cmd = $args[0]
$CmdArgs = @()
if ($args.Count -gt 1) {
    $CmdArgs = $args[1..($args.Count - 1)]
}

Write-Host "ExcuteCmd: $Cmd $($CmdArgs -join ' ')"
$RunLog = Join-Path $RunDir "pytest.log"
Write-Host "RunLog: $RunLog"
Write-Host ""

& $Cmd @CmdArgs 2>&1 | Tee-Object -FilePath $RunLog

$exitCode = $LASTEXITCODE
if ($null -eq $exitCode) {
    $exitCode = if ($?) { 0 } else { 1 }
}

if ($exitCode -eq 0) {
    Write-Host ""
    Write-Host "Execute script successfully"
} else {
    Write-Host ""
    Write-Host "Execute script failure (exit code: $exitCode)"
}

exit $exitCode
