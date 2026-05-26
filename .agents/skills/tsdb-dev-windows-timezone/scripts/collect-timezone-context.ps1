param(
    [switch]$IncludeRegistry = $true,
    [switch]$IncludeW32tm = $true
)

if (-not $IsWindows) {
    Write-Error "This script must run on Windows."
    exit 1
}

function Write-Section {
    param([string]$Title)

    Write-Host ""
    Write-Host "=== $Title ==="
}

Write-Section "Clock"
Get-Date | Format-List *

Write-Section "Time Zone"
Get-TimeZone | Format-List *

Write-Section "tzutil"
try {
    tzutil /g
} catch {
    Write-Warning "tzutil is not available: $($_.Exception.Message)"
}

Write-Section "OS"
Get-ComputerInfo | Select-Object WindowsProductName, WindowsVersion, OsBuildNumber | Format-List

if ($IncludeW32tm) {
    Write-Section "w32tm status"
    try {
        w32tm /query /status
    } catch {
        Write-Warning "w32tm status query failed: $($_.Exception.Message)"
    }

    Write-Section "w32tm configuration"
    try {
        w32tm /query /configuration
    } catch {
        Write-Warning "w32tm configuration query failed: $($_.Exception.Message)"
    }
}

if ($IncludeRegistry) {
    Write-Section "Registry TimeZoneInformation"
    try {
        Get-ItemProperty 'HKLM:\SYSTEM\CurrentControlSet\Control\TimeZoneInformation' | Format-List *
    } catch {
        Write-Warning "Failed to read TimeZoneInformation registry: $($_.Exception.Message)"
    }
}