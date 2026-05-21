#Requires -Version 5.1
<#
.SYNOPSIS
    TSDB Build Environment Setup for Windows

.DESCRIPTION
    Modular toolchain installation and internal dependency source configuration.
    PowerShell equivalent of setup-linux.sh / setup-macos.sh.

.PARAMETER Component
    Setup for specific component(s). Auto-resolves required language modules.
    Use 'all' for every language.

.PARAMETER Lang
    Setup specific language module(s): cpp, go, rust, java, node, python, dotnet

.PARAMETER All
    Setup all language modules.

.PARAMETER CheckOnly
    Check-only mode — report status without making changes.

.PARAMETER Yes
    Auto-confirm all prompts (non-interactive).

.EXAMPLE
    .\setup-windows.ps1 -Component engine, taosx
    .\setup-windows.ps1 -Lang rust
    .\setup-windows.ps1 -All -CheckOnly
    .\setup-windows.ps1 -All -Yes
#>

[CmdletBinding(DefaultParameterSetName = 'Help')]
param(
    [Parameter(ParameterSetName = 'Component')]
    [string[]]$Component,

    [Parameter(ParameterSetName = 'Lang')]
    [ValidateSet('cpp', 'go', 'rust', 'java', 'node', 'python', 'dotnet')]
    [string[]]$Lang,

    [Parameter(ParameterSetName = 'All')]
    [switch]$All,

    [switch]$CheckOnly,
    [switch]$Yes,
    [switch]$Help
)

$ErrorActionPreference = 'Stop'
$script:SetupDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# ── Color helpers ────────────────────────────────────────────────────────────
function Write-Ok    { param([string]$Msg) Write-Host "  ✓ $Msg" -ForegroundColor Green }
function Write-Warn  { param([string]$Msg) Write-Host "  ⚠ $Msg" -ForegroundColor Yellow }
function Write-Fail  { param([string]$Msg) Write-Host "  ✗ $Msg" -ForegroundColor Red }
function Write-Info  { param([string]$Msg) Write-Host "  → $Msg" -ForegroundColor Cyan }
function Write-Header { param([string]$Msg) Write-Host "`n── $Msg ──" -ForegroundColor White }

# ── Globals ──────────────────────────────────────────────────────────────────
$script:ChangesMade = 0
$script:IssuesFound = 0
$script:AutoYes = $Yes.IsPresent
$script:CheckOnlyMode = $CheckOnly.IsPresent

# ── Utility functions ────────────────────────────────────────────────────────
function Test-CommandExists {
    param([string]$Name)
    $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Confirm-Action {
    param([string]$Prompt = 'Continue?')
    if ($script:AutoYes) { return $true }
    if ($script:CheckOnlyMode) { return $false }
    $reply = Read-Host "  $Prompt [Y/n]"
    return ($reply -eq '' -or $reply -match '^[Yy]')
}

function Test-VersionGte {
    param([string]$Current, [string]$Required)
    try {
        [version]$Current -ge [version]$Required
    } catch {
        $false
    }
}

function Test-ChocolateyInstalled {
    Test-CommandExists 'choco'
}

function Install-WithChoco {
    param([string[]]$Packages)
    if (-not (Test-ChocolateyInstalled)) {
        Write-Fail "Chocolatey not found. Install from https://chocolatey.org/install"
        return
    }
    foreach ($pkg in $Packages) {
        choco install $pkg -y --no-progress 2>$null
    }
}

function Install-WithWinget {
    param([string]$Id)
    if (Test-CommandExists 'winget') {
        winget install --id $Id --accept-source-agreements --accept-package-agreements 2>$null
    } else {
        Write-Fail "winget not available"
    }
}

function Test-UrlReachable {
    param([string]$Name, [string]$Url)
    try {
        $response = Invoke-WebRequest -Uri $Url -Method Head -TimeoutSec 10 -UseBasicParsing -ErrorAction Stop
        Write-Ok "$Name ($Url)"
        return $true
    } catch {
        Write-Fail "$Name unreachable ($Url)"
        $script:IssuesFound++
        return $false
    }
}

# ── Config (mirrors from .build-args) ────────────────────────────────────────
$BuilderDir = Join-Path $script:SetupDir '..\tsdb-builder'
$BuildArgs = Join-Path $BuilderDir '.build-args'

$script:GoProxy = 'https://nexus.tdengine.net/repository/goproxy/'
$script:CargoRegistryUrl = 'sparse+https://nora.tdengine.net/cargo/index/'
$script:ConanRemoteUrl = 'https://nexus.tdengine.net/repository/conan/'

if (Test-Path $BuildArgs) {
    $content = Get-Content $BuildArgs
    foreach ($line in $content) {
        if ($line -match '^GO_PROXY=(.+)') { $script:GoProxy = $Matches[1] }
        if ($line -match '^CARGO_REGISTRY_URL=(.+)') { $script:CargoRegistryUrl = $Matches[1] }
        if ($line -match '^CONAN_REMOTE_URL=(.+)') { $script:ConanRemoteUrl = $Matches[1] }
    }
}

# ── Version requirements ─────────────────────────────────────────────────────
$script:RequiredCMakeVersion = '3.21'
$script:RequiredGoVersion = '1.23'
$script:RequiredRustVersion = '1.90'
$script:RequiredJavaVersion = 17
$script:RequiredNodeVersion = '18.0'
$script:RequiredPythonVersion = '3.10'

# ── Component → Language mapping ─────────────────────────────────────────────
$ComponentMap = @{
    'engine'           = @('cpp')
    'enterprise'       = @('cpp')
    'adapter'          = @('go')
    'keeper'           = @('go')
    'taosx'            = @('rust')
    'gen'              = @('cpp')
    'insight'          = @('go', 'node')
    'connector-jdbc'   = @('java')
    'connector-go'     = @('go')
    'connector-node'   = @('node')
    'connector-python' = @('python', 'rust')
    'connector-rust'   = @('rust')
    'connector-dotnet' = @('dotnet')
    'connector-odbc'   = @('cpp')
}

$AllLangModules = @('cpp', 'go', 'rust', 'java', 'node', 'python', 'dotnet')

# ── Resolve languages ────────────────────────────────────────────────────────
$RequestedLangs = @()

if ($Help -or ($PSCmdlet.ParameterSetName -eq 'Help' -and -not $All)) {
    Get-Help $MyInvocation.MyCommand.Path -Detailed
    exit 0
}

if ($All) {
    $RequestedLangs = $AllLangModules
} elseif ($Component) {
    foreach ($comp in $Component) {
        if ($comp -eq 'all') {
            $RequestedLangs = $AllLangModules
            break
        }
        if (-not $ComponentMap.ContainsKey($comp)) {
            Write-Error "Unknown component '$comp'. Known: $($ComponentMap.Keys -join ', ')"
            exit 1
        }
        $RequestedLangs += $ComponentMap[$comp]
    }
} elseif ($Lang) {
    $RequestedLangs = $Lang
}

# Deduplicate
$Langs = $RequestedLangs | Select-Object -Unique

if ($Langs.Count -eq 0) {
    Write-Error "No language modules resolved. Use -Help for usage."
    exit 1
}

# ── Banner ───────────────────────────────────────────────────────────────────
Write-Host ''
Write-Host '╔══════════════════════════════════════════════════════════════════╗'
Write-Host '║        TSDB Build Environment Setup (Windows)                  ║'
Write-Host '╚══════════════════════════════════════════════════════════════════╝'
Write-Host ''
Write-Host "  OS:       Windows $([System.Environment]::OSVersion.Version)"
Write-Host "  Arch:     $env:PROCESSOR_ARCHITECTURE"
Write-Host "  Modules:  $($Langs -join ', ')"
if ($script:CheckOnlyMode) {
    Write-Host '  Mode:     check-only (no modifications)'
}
Write-Host ''

# ── Load and execute modules ─────────────────────────────────────────────────
foreach ($lang in $Langs) {
    $modFile = Join-Path $script:SetupDir "modules-windows\$lang.ps1"
    if (-not (Test-Path $modFile)) {
        Write-Info "Module '$lang' not yet implemented (skipping)"
        continue
    }
    . $modFile

    & "Mod-${lang}-Check"

    if (-not $script:CheckOnlyMode) {
        & "Mod-${lang}-Install"
        & "Mod-${lang}-Config"
    }
}

# ── Connectivity check ───────────────────────────────────────────────────────
Write-Header 'Internal mirror connectivity'

foreach ($lang in $Langs) {
    $modFile = Join-Path $script:SetupDir "modules-windows\$lang.ps1"
    if (-not (Test-Path $modFile)) { continue }
    switch ($lang) {
        'go'   { Test-UrlReachable 'Go Proxy (Nexus)' $script:GoProxy | Out-Null }
        'rust' { Test-UrlReachable 'Cargo Registry (Nora)' 'https://nora.tdengine.net/cargo/index/config.json' | Out-Null }
        'cpp'  { Test-UrlReachable 'Conan Remote (Nexus)' $script:ConanRemoteUrl | Out-Null }
    }
}

# ── Summary ──────────────────────────────────────────────────────────────────
Write-Header 'Summary'
if ($script:CheckOnlyMode) {
    Write-Host "  Check complete. $($script:IssuesFound) issue(s) found."
} elseif ($script:ChangesMade -gt 0) {
    Write-Ok "Done: $($script:ChangesMade) change(s) applied."
} else {
    Write-Ok 'Everything is already configured.'
}
if ($script:IssuesFound -gt 0 -and -not $script:CheckOnlyMode) {
    Write-Warn "$($script:IssuesFound) issue(s) require manual attention (see above)."
}
Write-Host ''
