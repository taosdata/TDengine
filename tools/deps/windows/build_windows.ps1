# =============================================================================
# TSDB Windows Build Script (VS2022 + JOM)
#
# Run this script from the build directory (e.g. D:\tsdb\debug):
#   cd D:\tsdb\debug
#   ..\tools\deps\windows\build_windows.ps1              # Configure + build (Debug, all)
#   ..\tools\deps\windows\build_windows.ps1 -Release     # Release build
#   ..\tools\deps\windows\build_windows.ps1 -BuildOnly   # Build only (skip cmake)
#   ..\tools\deps\windows\build_windows.ps1 -Clean       # Clean + rebuild
#   ..\tools\deps\windows\build_windows.ps1 -Jobs 8      # Specify parallel jobs
#   ..\tools\deps\windows\build_windows.ps1 -EngineOnly  # Engine only (skip connectors)
#
# Prerequisites:
#   - Visual Studio 2022 with C++ workload
#   - jom (NMake compatible parallel build tool)
#   - cmake
#   Run tools\deps\windows\install_deps_windows.ps1 to install all dependencies.
# =============================================================================

param(
    [switch]$Release,
    [switch]$BuildOnly,
    [switch]$Clean,
    [switch]$EngineOnly,
    [int]$Jobs = 0
)

$ErrorActionPreference = "Stop"

# ── Resolve paths ─────────────────────────────────────────────────────────
# Script lives at <projectRoot>/tools/deps/windows/build_windows.ps1
$scriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Definition
$projectRoot = (Resolve-Path (Join-Path $scriptDir "..\..\..")).Path

# Build directory = current working directory
$buildPath = (Get-Location).Path

# Sanity check: build dir should NOT be the project root
if ($buildPath -eq $projectRoot) {
    Write-Host "[X] Do not run this script from the project root." -ForegroundColor Red
    Write-Host "    Create a build directory first:  mkdir debug; cd debug" -ForegroundColor Red
    exit 1
}

# ── Locate VS2022 ────────────────────────────────────────────────────────
$vsWhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
if (-not (Test-Path $vsWhere)) {
    Write-Host "[X] vswhere.exe not found. Is Visual Studio 2022 installed?" -ForegroundColor Red
    exit 1
}
$vsPath = (& $vsWhere -latest -property installationPath 2>&1 | Out-String).Trim()
$vcvars = "$vsPath\VC\Auxiliary\Build\vcvars64.bat"
if (-not (Test-Path $vcvars)) {
    Write-Host "[X] vcvars64.bat not found at: $vcvars" -ForegroundColor Red
    Write-Host "    Please install VS2022 C++ workload." -ForegroundColor Red
    exit 1
}

# ── Import VS developer environment into current PowerShell session ───────
function Import-VsEnvironment {
    $tempBat = Join-Path $env:TEMP "tsdb_vcvars.bat"
    $tempEnv = Join-Path $env:TEMP "tsdb_vcvars_env.txt"
    @"
@echo off
call "$vcvars" >nul 2>&1
set > "$tempEnv"
"@ | Set-Content $tempBat -Encoding ASCII
    cmd /c $tempBat
    Get-Content $tempEnv | ForEach-Object {
        if ($_ -match '^([^=]+)=(.*)$') {
            [System.Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process')
        }
    }
    Remove-Item $tempBat, $tempEnv -Force -ErrorAction SilentlyContinue
    Write-Host "[OK] VS2022 developer environment loaded" -ForegroundColor Green
}

# ── Ensure all tool paths are in current PATH ─────────────────────────────
$machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
$userPath    = [Environment]::GetEnvironmentVariable("Path", "User")
$env:Path    = "$machinePath;$userPath"
# Add common tool directories that may not be in registry yet
$extraDirs = @(
    "$env:USERPROFILE\go\bin",
    "$env:USERPROFILE\.cargo\bin",
    "$env:ProgramFiles\Go\bin",
    "$env:ProgramFiles\CMake\bin",
    "$env:ProgramFiles\dotnet",
    "C:\msys64\ucrt64\bin"
)
try { $extraDirs += (npm config get prefix 2>&1 | Out-String).Trim() } catch {}
try { $extraDirs += (python -c "import sysconfig; print(sysconfig.get_path('scripts'))" 2>&1 | Out-String).Trim() } catch {}
foreach ($d in $extraDirs) {
    if ($d -and (Test-Path $d) -and ($env:Path -notlike "*$d*")) {
        $env:Path = "$d;$env:Path"
    }
}

# ── Parallel jobs ─────────────────────────────────────────────────────────
if ($Jobs -le 0) {
    $Jobs = [Environment]::ProcessorCount
}

# ── Build type ────────────────────────────────────────────────────────────
$buildType = if ($Release) { "Release" } else { "Debug" }

# ── Print configuration ──────────────────────────────────────────────────
Write-Host ""
Write-Host "=============================" -ForegroundColor Cyan
Write-Host " TSDB Windows Build" -ForegroundColor Cyan
Write-Host "=============================" -ForegroundColor Cyan
Write-Host "  VS2022:      $vsPath"
Write-Host "  Build type:  $buildType"
Write-Host "  Project root: $projectRoot"
Write-Host "  Build dir:   $buildPath"
Write-Host "  Jobs:        $Jobs"
Write-Host "  Engine only: $EngineOnly"
Write-Host "=============================" -ForegroundColor Cyan
Write-Host ""

# ── Import VS environment ──────────────────────────────────────────────────
Import-VsEnvironment

# ── Clean ─────────────────────────────────────────────────────────────────
if ($Clean) {
    Write-Host "[!] Cleaning $buildPath ..." -ForegroundColor Yellow
    Get-ChildItem -Path $buildPath -Force | Remove-Item -Recurse -Force
    Write-Host "[OK] Cleaned" -ForegroundColor Green
}

# ── CMake configure ───────────────────────────────────────────────────────
if (-not $BuildOnly) {
    Write-Host ""
    Write-Host ">> CMake Configure ..." -ForegroundColor Cyan

    $cmakeArgs = @(
        "-G", "NMake Makefiles JOM",
        "-DCMAKE_BUILD_TYPE=$buildType"
    )

    if ($EngineOnly) {
        $cmakeArgs += "-DBUILD_ADAPTER=OFF"
        $cmakeArgs += "-DBUILD_KEEPER=OFF"
        $cmakeArgs += "-DBUILD_TOOLS=OFF"
        $cmakeArgs += "-DBUILD_GEN=OFF"
        $cmakeArgs += "-DBUILD_TAOSX=OFF"
        $cmakeArgs += "-DBUILD_INSIGHT=OFF"
        $cmakeArgs += "-DBUILD_DOTNET=OFF"
        $cmakeArgs += "-DBUILD_GO=OFF"
        $cmakeArgs += "-DBUILD_JDBC=OFF"
        $cmakeArgs += "-DBUILD_NODE=OFF"
        $cmakeArgs += "-DBUILD_ODBC=OFF"
        $cmakeArgs += "-DBUILD_PYTHON=OFF"
        $cmakeArgs += "-DBUILD_RUST=OFF"
    }

    # Point cmake to the project root
    $cmakeArgs += $projectRoot

    & cmake @cmakeArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[X] CMake configure failed (exit code $LASTEXITCODE)" -ForegroundColor Red
        exit $LASTEXITCODE
    }
    Write-Host "[OK] CMake configure done" -ForegroundColor Green
}

# ── Build ─────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host ">> Building with jom /J $Jobs ..." -ForegroundColor Cyan

& jom /J $Jobs
if ($LASTEXITCODE -ne 0) {
    Write-Host "[X] Build failed (exit code $LASTEXITCODE)" -ForegroundColor Red
    exit $LASTEXITCODE
}
Write-Host ""
Write-Host "=============================" -ForegroundColor Green
Write-Host " Build succeeded!" -ForegroundColor Green
Write-Host "=============================" -ForegroundColor Green
Write-Host "  Output: $buildPath" -ForegroundColor Green
