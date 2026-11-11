# Copyright (c) 2026 TAOS Data, Inc.
# SPDX-License-Identifier: AGPL-3.0-or-later

<#
.SYNOPSIS
Configure optional mimalloc for a stopped taosd executable (Windows PowerShell 5.1+).
.EXAMPLE
.\set_taos_malloc.ps1 -Mode mimalloc
.EXAMPLE
.\set_taos_malloc.ps1 -Mode default
.EXAMPLE
.\set_taos_malloc.ps1 -Status
#>
[CmdletBinding(DefaultParameterSetName = 'Status')]
param(
    [Parameter(Mandatory = $true, ParameterSetName = 'Switch')]
    [ValidateSet('mimalloc', 'default')]
    [string]$Mode,
    [Parameter(ParameterSetName = 'Status')]
    [switch]$Status,
    [string]$ExecutablePath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$candidate = $null
$stateTemp = $null

function Assert-Stopped([string]$Path) {
    $names = @([IO.Path]::GetFileNameWithoutExtension($Path), [IO.Path]::GetFileName($Path))
    foreach ($process in @(Get-Process -Name $names -ErrorAction SilentlyContinue)) {
        if (-not $process.Path -or $process.Path -eq $Path) {
            throw "Stop the target process before switching allocators (PID $($process.Id))."
        }
    }
}

function Invoke-Minject([string[]]$Arguments) {
    # Windows PowerShell may turn native stderr into error records. Use the exit
    # code, not stderr alone, to distinguish diagnostics from command failure.
    $savedPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = 'Continue'
        $output = & $injector @Arguments 2>&1
        $code = $LASTEXITCODE
    } catch {
        throw "Unable to execute minject: $($_.Exception.Message)"
    } finally {
        $ErrorActionPreference = $savedPreference
    }
    if ($code -ne 0) { throw "minject failed ($code): $output" }
    return (($output | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine)
}

function Get-AllocatorImport([string]$Path) {
    $listing = Invoke-Minject -Arguments @('--list', $Path)
    if ($listing -notmatch '(?im)^\s*\d+:\s*[^\s]+\.dll\s*$') {
        throw "Unable to read the import table of $Path"
    }
    $imports = @([regex]::Matches($listing, '(?im)^\s*\d+:\s*(mimalloc(?:-[a-z0-9-]+)?\.dll)\s*$') |
        ForEach-Object { $_.Groups[1].Value.ToLowerInvariant() } | Select-Object -Unique)
    $allocators = @($imports | Where-Object { $_ -ne 'mimalloc-redirect.dll' })
    if ($allocators.Count -gt 1) { throw "Ambiguous mimalloc imports in $Path" }
    if ($allocators.Count -eq 1) { return $allocators[0] }
    if ($imports.Count -ne 0) { throw "mimalloc-redirect.dll is imported without an allocator DLL in $Path" }
    return ''
}

try {
    # Windows PowerShell 5.1 -File does not set PSScriptRoot in parameter defaults.
    if (-not $PSBoundParameters.ContainsKey('ExecutablePath')) {
        $ExecutablePath = Join-Path $PSScriptRoot 'taosd.exe'
    }
    $exe = (Resolve-Path -LiteralPath $ExecutablePath).ProviderPath
    $directory = [IO.Path]::GetDirectoryName($exe)
    $injector = Join-Path $directory 'minject.exe'
    $backup = "$exe.before-mimalloc"
    $statePath = "$exe.mimalloc.json"
    if (-not (Test-Path -LiteralPath $injector -PathType Leaf)) { throw "Missing tool: $injector" }
    if ($PSCmdlet.ParameterSetName -eq 'Switch') { Assert-Stopped $exe }
    $import = Get-AllocatorImport $exe
    if ($PSCmdlet.ParameterSetName -eq 'Status') {
        $configured = if ($import) { 'mimalloc' } else { 'default' }
        Write-Output "Configured allocator: $configured"
        Write-Output 'Runtime redirection: not verified (this command inspects the executable only).'
        exit 0
    }
    if ($Mode -eq 'default' -and -not $import) {
        Write-Output 'Configured allocator: default (unchanged).'
        exit 0
    }

    $originalHash = (Get-FileHash -LiteralPath $exe -Algorithm SHA256).Hash
    $hasBackup = Test-Path -LiteralPath $backup -PathType Leaf
    $hasState = Test-Path -LiteralPath $statePath -PathType Leaf
    if ($hasBackup -ne $hasState) { throw 'Incomplete backup/state pair. Preserve the files and recover the matching original package.' }
    if ($hasState) {
        $state = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
        if ($state.originalHash -notmatch '^[0-9a-fA-F]{64}$' -or $state.patchedHash -notmatch '^[0-9a-fA-F]{64}$') {
            throw 'Invalid allocator state file.'
        }
        if ((Get-FileHash -LiteralPath $backup -Algorithm SHA256).Hash -ne $state.originalHash) {
            throw 'Original executable backup has changed; refusing to overwrite the executable.'
        }
        $expectedHash = if ($import) { $state.patchedHash } else { $state.originalHash }
        if ($originalHash -ne $expectedHash) {
            throw 'Executable has changed or was upgraded. Preserve the old backup/state separately and use the matching original package.'
        }
    } elseif ($import) {
        throw 'This executable was patched outside this script. Restore its matching original package before switching.'
    }

    if ($Mode -eq 'mimalloc') {
        $allocators = @('mimalloc.dll', 'mimalloc-debug.dll' | Where-Object {
            Test-Path -LiteralPath (Join-Path $directory $_) -PathType Leaf
        })
        if ($allocators.Count -ne 1) { throw 'Exactly one matching mimalloc.dll or mimalloc-debug.dll is required.' }
        if (-not (Test-Path -LiteralPath (Join-Path $directory 'mimalloc-redirect.dll') -PathType Leaf)) {
            throw 'Missing mimalloc-redirect.dll.'
        }
        if ($import) {
            if ($import -ne $allocators[0]) { throw 'The configured allocator DLL does not match the packaged DLL.' }
            Write-Output 'Configured allocator: mimalloc (unchanged).'
            exit 0
        }
    }

    # Patch/validate a sibling copy. Never let minject modify the live executable
    # or its original backup; keep both intact if injection fails.
    $candidate = Join-Path $directory ([IO.Path]::GetRandomFileName() + '.exe')
    if ($Mode -eq 'mimalloc') {
        [IO.File]::Copy($exe, $candidate)
        $arguments = @('--force', '--inplace')
        if ($allocators[0] -eq 'mimalloc-debug.dll') { $arguments += '--postfix=debug' }
        $null = Invoke-Minject ($arguments + @($candidate))
        $patchedHash = (Get-FileHash -LiteralPath $candidate -Algorithm SHA256).Hash
        if ((Get-AllocatorImport $candidate) -ne $allocators[0] -or $patchedHash -eq $originalHash) {
            throw 'Patched executable verification failed; original executable is unchanged.'
        }
        if (-not $hasBackup) { [IO.File]::Copy($exe, $backup, $false) }
        if ((Get-FileHash -LiteralPath $backup -Algorithm SHA256).Hash -ne $originalHash) {
            throw 'Backup verification failed; original executable is unchanged.'
        }
        $stateTemp = Join-Path $directory ([IO.Path]::GetRandomFileName() + '.json')
        @{ originalHash = $originalHash; patchedHash = $patchedHash } | ConvertTo-Json |
            Set-Content -LiteralPath $stateTemp -Encoding UTF8
        if ($hasState) {
            [IO.File]::Replace($stateTemp, $statePath, [NullString]::Value)
        } else {
            [IO.File]::Move($stateTemp, $statePath)
        }
    } else {
        [IO.File]::Copy($backup, $candidate)
        if ((Get-FileHash -LiteralPath $candidate -Algorithm SHA256).Hash -ne $state.originalHash -or
            (Get-AllocatorImport $candidate)) { throw 'Original executable verification failed.' }
    }
    Assert-Stopped $exe
    if ((Get-FileHash -LiteralPath $exe -Algorithm SHA256).Hash -ne $originalHash) {
        throw 'Executable changed during switching; refusing to overwrite it.'
    }
    [IO.File]::Replace($candidate, $exe, [NullString]::Value)
    Write-Output "Configured allocator: $Mode. Start taosd normally; verify runtime redirection separately."
} catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
} finally {
    foreach ($temporary in @($candidate, $stateTemp)) {
        if ($temporary -and (Test-Path -LiteralPath $temporary)) { Remove-Item -LiteralPath $temporary -Force }
    }
}
