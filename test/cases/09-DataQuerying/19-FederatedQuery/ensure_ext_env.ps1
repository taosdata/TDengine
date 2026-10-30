# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

<#
.SYNOPSIS
    ensure_ext_env.ps1  ─ FederatedQuery integration-test external-source setup (Windows)

.DESCRIPTION
    Windows counterpart of ensure_ext_env.sh.
    Downloads, installs, and starts MySQL, PostgreSQL, InfluxDB, SQL Server and
    Oracle AI Database Free on non-default ports so they run alongside any
    locally installed instances.  All operations are idempotent: re-running
    the script while services are already up resets test databases instead of
    restarting them.

    Oracle on Windows has no portable build, so the script installs the
    vendor package silently (administrator required), creates the FREE
    container database with the FREEPDB1 service, provisions the test
    schemas, and registers the Instant Client ODBC driver that the provider
    resolves through ODBCINST.INI.  Only the 26ai version key exists here.

    SQL Server on Windows: the engine is used where it already runs (local
    service or FQ_SQLSERVER_HOST); if the host is local and nothing is
    installed, SQL Server Express is installed silently (administrator
    required).  The Microsoft ODBC Driver 18 is installed from the official
    MSI (the provider loads it through the system ODBC driver manager), and a
    portable go-sqlcmd is staged for the test-side data setup.

    REQUIREMENTS
      Windows 10 21H2 / Windows Server 2022 or later (64-bit only)
      PowerShell 5.1 or PowerShell 7+
      Internet access (or mirrors set via FQ_*_MIRROR env vars)
      Administrator rights  – only needed if WinSW service registration fails
                               and --user workaround is required for MySQL;
                               always required to install Oracle Free, to
                               start/stop its services, and to register the
                               ODBC driver in HKLM.

    WHAT IT DOES (idempotent per-engine-version)
      1. Port open?         → reset test DBs (already running)
      2. Installed, stopped → start; if still failing re-init data dir
      3. Not installed?     → download → install → init → start → configure

    ENVIRONMENT VARIABLES (all optional)
      FQ_BASE_DIR            install/data root   default %LOCALAPPDATA%\taostest\fq
      FQ_MYSQL_VERSIONS      comma list          default "8.0"
      FQ_PG_VERSIONS         comma list          default "16"
      FQ_INFLUX_VERSIONS     comma list          default "3.0"
      FQ_SQLSERVER_VERSIONS  comma list          default "2022" (2019 uses port 1434)
      FQ_SQLSERVER_HOST      engine host         default 127.0.0.1
      FQ_SQLSERVER_USER/PASS credentials         default sa / TaosFq!2026
      FQ_SQLSERVER_INSTANCE  local instance name default FQSQL (used when installing)
      FQ_SQLCMD_URL          go-sqlcmd zip URL   default github release v1.10.0
      FQ_MSODBC_MSI_URL      Driver 18 MSI URL   default Microsoft fwlink
      FQ_SQLSERVER_MEDIA_URL Express installer URL default SQL Server 2022 Express (16.0)
      FQ_MYSQL_MIRROR        base URL for MySQL ZIP downloads
      FQ_PG_MIRROR           base URL for PG ZIP downloads
      FQ_INFLUX_MIRROR       base URL for InfluxDB releases
      FQ_MYSQL_TARBALL_<VV>  full URL override (VV = 57/80/84)
      FQ_PG_TARBALL_<VV>     full URL override (VV = 14/15/16/17)
      FQ_INFLUX_TARBALL_<VV> full URL override (VV = 30/35)
      FQ_ORACLE_VERSIONS     comma list          default "26ai"
      FQ_ORACLE_HOST         engine host         default 127.0.0.1
      FQ_ORACLE_PORT_26AI    listener port       default 18322
      FQ_ORACLE_SERVICE_26AI PDB service name    default FREEPDB1
      FQ_ORACLE_FREE_URL     package URL/path    default Oracle Free 26ai 23.26.3 zip
      FQ_ORACLE_FREE_SHA256  package SHA256      pinned to that zip
      FQ_ORACLE_PKG_DIR      package cache       default <FQ_BASE_DIR>\oracle\packages
      FQ_ORACLE_DRIVER_DIR   Instant Client dir  default build\bin / <install>\driver\oracle
      FQ_ORACLE_USER/PASS    test schema         default taosdata / taosdata
      FQ_ORACLE_ADMIN_USER/PASS provisioning acct default system / Tdengine_123
      FQ_CERT_DIR            cert source dir     default <script_dir>\certs
      FQ_MYSQL_USER/PASS     credentials         default root / taosdata
      FQ_PG_USER/PASS        credentials         default postgres / taosdata
      FQ_INFLUX_TOKEN/ORG    (unused w/ --without-auth) default test-token / test-org

    EXIT CODES
      0 = all requested engines ready
      1 = one or more engines failed
#>

#Requires -Version 5.1
[CmdletBinding()]
param(
    # Action flags from the test framework (--oracle-exec, --oracle-admin-exec,
    # --oracle-stop, --oracle-kill).  Without this parameter a leading "--"
    # argument is treated as a parameter name and binding fails before the
    # script runs.
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$RemainingArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ── 0. Bootstrap ─────────────────────────────────────────────────────────────

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Definition
$FqBase     = if ($env:FQ_BASE_DIR) { $env:FQ_BASE_DIR }
              else { Join-Path $env:LOCALAPPDATA 'taostest\fq' }
$CertSrc    = if ($env:FQ_CERT_DIR) { $env:FQ_CERT_DIR }
              else { Join-Path $ScriptDir 'certs' }

# PowerShell 5.1 (the interpreter the test framework invokes) has no `??`
# null-coalescing operator, so resolve optional environment variables here.
function Get-EnvOr {
    param([string]$Name, $Default)
    $value = [System.Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrEmpty($value)) { return $Default }
    return $value
}

# Version lists: an explicitly EMPTY variable means "do not provision this
# engine" (the test framework sets e.g. FQ_MYSQL_VERSIONS="" for SQL- Server-only
# runs), while an unset variable falls back to the default list.
function Get-EnvList {
    param([string]$Name, [string]$Default)
    # GetEnvironmentVariable() collapses "present but empty" to $null, so look
    # the name up in the raw block to tell "disabled" from "unset".
    $all = [System.Environment]::GetEnvironmentVariables()
    if ($all.Contains($Name)) { $value = [string]$all[$Name] } else { $value = $Default }
    return @($value -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
}

$MysqlVersions  = Get-EnvList 'FQ_MYSQL_VERSIONS'  '8.0'
$PgVersions     = Get-EnvList 'FQ_PG_VERSIONS'     '16'
$InfluxVersions = Get-EnvList 'FQ_INFLUX_VERSIONS' '3.0'

$MysqlUser  = Get-EnvOr 'FQ_MYSQL_USER' 'root'
$MysqlPass  = Get-EnvOr 'FQ_MYSQL_PASS' 'taosdata'
$PgUser     = Get-EnvOr 'FQ_PG_USER'    'postgres'
$PgPass     = Get-EnvOr 'FQ_PG_PASS'    'taosdata'

$OverallOk  = $true

# Command-line actions must keep stdout byte-clean for the caller: the test
# framework parses sqlplus' output from it.  Diagnostics still go to the log
# file and to stderr.
$script:SuppressConsoleLog = $false

# ── 1. Logging ────────────────────────────────────────────────────────────────

# Mirror every line into a file as well: when the script runs under a service
# (the Windows CI runner) the host console often has no reader, so Write-Host
# output is lost and a failure leaves no trace in the test log.
$script:FqLogFile = Join-Path $FqBase 'setup.log'
function Write-FqLogFile {
    param($Msg)
    try {
        $dir = Split-Path -Parent $script:FqLogFile
        if (-not (Test-Path $dir)) { $null = New-Item -ItemType Directory -Force $dir }
        Add-Content -LiteralPath $script:FqLogFile -Value ("{0} {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Msg)
    } catch {
    }
}

function Log   { param($Msg) if (-not $script:SuppressConsoleLog) { Write-Host "[fq-env] $Msg" }; Write-FqLogFile $Msg }
function Info  { param($Msg) if (-not $script:SuppressConsoleLog) { Write-Host "[fq-env] INFO  $Msg" }; Write-FqLogFile ("INFO  " + $Msg) }
function Warn  { param($Msg) if (-not $script:SuppressConsoleLog) { Write-Warning "[fq-env] WARN  $Msg" }; Write-FqLogFile ("WARN  " + $Msg) }
function Err   { param($Msg) Write-Error   "[fq-env] ERROR $Msg" -ErrorAction Continue; Write-FqLogFile ("ERROR " + $Msg) }

# ── 2. Port helpers ───────────────────────────────────────────────────────────

function Test-PortOpen {
    param([int]$Port)
    try {
        $tcp = [System.Net.Sockets.TcpClient]::new('127.0.0.1', $Port)
        $tcp.Close()
        return $true
    } catch {
        return $false
    }
}

function Wait-Port {
    param([int]$Port, [int]$MaxSec = 60)
    $i = 0
    while (-not (Test-PortOpen $Port)) {
        Start-Sleep -Seconds 1
        $i++
        if ($i -ge $MaxSec) { return $false }
    }
    return $true
}

# ── 3. Download helper ────────────────────────────────────────────────────────

function Invoke-Download {
    param([string]$Url, [string]$Dest, [int]$Retries = 3)
    if (Test-Path $Dest) {
        $size = (Get-Item $Dest).Length
        if ($size -gt 1MB) {
            Info "Using cached $Dest"
            return
        }
        Remove-Item $Dest -Force
    }
    Info "Downloading $Url → $Dest"
    # curl.exe first: it is present on every supported Windows, prints progress
    # into the job log, resumes a partial file (`-C -`) and retries stalled
    # transfers -- the CI runner resets long downloads and the WebClient path
    # below has neither resume nor a progress trace.
    $curlFirst = Get-Command curl.exe -ErrorAction SilentlyContinue
    if ($curlFirst) {
        for ($i = 1; $i -le 3; $i++) {
            & $curlFirst.Source -L --fail --retry 5 --retry-delay 5 `
                --connect-timeout 30 --speed-time 120 --speed-limit 2048 `
                -C - -o $Dest $Url
            if ($LASTEXITCODE -eq 0 -and (Test-Path $Dest) -and
                    (Get-Item $Dest).Length -gt 1MB) {
                return
            }
            Warn "curl attempt $i failed for $Url"
            Start-Sleep -Seconds 5
        }
    }
    $attempt = 0
    while ($attempt -lt $Retries) {
        try {
            $attempt++
            # Use BITS if available (background, resume-capable); fall back to WebClient
            if (Get-Command Start-BitsTransfer -ErrorAction SilentlyContinue) {
                Start-BitsTransfer -Source $Url -Destination $Dest
            } else {
                $wc = [System.Net.WebClient]::new()
                $wc.DownloadFile($Url, $Dest)
                $wc.Dispose()
            }
            return
        } catch {
            Warn "Download attempt $attempt failed: $_"
            if ($attempt -lt $Retries) { Start-Sleep -Seconds 5 }
        }
    }
    # Last resort: curl.exe works in non-interactive sessions where BITS is
    # unavailable (e.g. an SSH / remote shell reports 0x800704DD, "the user is
    # not logged on, the network is not available").
    $curl = Get-Command curl.exe -ErrorAction SilentlyContinue
    if ($curl) {
        Warn "Falling back to curl.exe for $Url"
        & $curl.Source -L --fail --retry 3 --connect-timeout 30 -o $Dest $Url
        if ($LASTEXITCODE -eq 0 -and (Test-Path $Dest) -and
                (Get-Item $Dest).Length -gt 1MB) {
            return
        }
        Remove-Item $Dest -Force -ErrorAction SilentlyContinue
    }
    throw "Failed to download $Url after $Retries attempts"
}

# ── 4. Process management ────────────────────────────────────────────────────

function Start-BackgroundProcess {
    param(
        [string]$Exe,
        [string[]]$ArgList,
        [string]$LogFile,
        [string]$PidFile,
        [string[]]$EnvPairs = @()   # "KEY=VALUE" strings
    )
    $null = New-Item -ItemType Directory -Force (Split-Path $LogFile)
    $null = New-Item -ItemType Directory -Force (Split-Path $PidFile)

    # Hand the log FILE to the engine itself instead of pumping its stdout
    # through this process.  Redirecting into PowerShell pipes fails in two
    # ways on Windows:
    #   * powershell.exe 5.1 crashes with PSInvalidOperationException in
    #     ScriptBlock.GetContextFromTLS while the Process.OutputDataReceived
    #     handlers fire from thread-pool threads during engine startup (WER
    #     report 2026-10-29 08:46:45, right after "InfluxDB 3.0: starting on
    #     port 18086 ..."; the batch case failed with exit code 2);
    #   * once this script exits nothing drains those pipes any more, so the
    #     engine blocks on the next 64 KiB of log output and stops answering
    #     requests while its port stays open.
    # cmd.exe's >> redirection gives the child a real file handle that stays
    # valid after both this script and pytest have exited.
    # Quote arguments that contain spaces, but leave arguments that already
    # carry their own quotes intact: cmd.exe forwards the command line to the
    # child verbatim, and nesting one pair of quotes inside another would
    # corrupt the value (e.g. MySQL's --defaults-file="<path with spaces>").
    $argLine = ($ArgList | ForEach-Object {
        if (($_ -match '\s') -and ($_ -notmatch '"')) { '"' + $_ + '"' } else { $_ }
    }) -join ' '
    # /d skips AutoRun entries; /s strips exactly one outer pair of quotes and
    # runs the remainder verbatim, so the quoted paths survive intact.
    $inner = '"' + $Exe + '" ' + $argLine + ' >> "' + $LogFile + '" 2>&1'
    $comspec = if ($env:ComSpec) { $env:ComSpec }
               else { Join-Path $env:SystemRoot 'System32\cmd.exe' }

    $procInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $procInfo.FileName        = $comspec
    $procInfo.Arguments       = '/d /s /c "' + $inner + '"'
    $procInfo.UseShellExecute = $false
    $procInfo.CreateNoWindow  = $true

    foreach ($ep in $EnvPairs) {
        $kv = $ep -split '=', 2
        if ($kv.Count -eq 2) {
            $procInfo.EnvironmentVariables[$kv[0]] = $kv[1]
        }
    }

    $proc = [System.Diagnostics.Process]::new()
    $proc.StartInfo = $procInfo
    $null = $proc.Start()

    # The pid worth recording is the engine's, not cmd.exe's: Stop-ByPidFile
    # must terminate something whose death the engine notices.  cmd.exe also
    # parents a conhost.exe console host, so match the child by executable
    # name rather than taking the first child.  Fall back to the wrapper pid
    # when no match appears (a .bat child runs inside cmd, or WMI is
    # unresponsive); Stop-ByPidFile then kills the whole tree instead.
    $exeName = Split-Path -Leaf $Exe
    $engineId = $null
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(10)
    while ((-not $engineId) -and (-not $proc.HasExited) -and
           ([DateTimeOffset]::UtcNow -lt $deadline)) {
        try {
            $children = @(Get-CimInstance -ClassName Win32_Process `
                -Filter "ParentProcessId=$($proc.Id)" -ErrorAction Stop)
            $engine = $children | Where-Object {
                ($_.Name -eq $exeName) -or
                ($_.ExecutablePath -and ($_.ExecutablePath -eq $Exe))
            } | Select-Object -First 1
            if ($engine) { $engineId = [int]$engine.ProcessId }
        } catch {
            break
        }
        if (-not $engineId) { Start-Sleep -Milliseconds 100 }
    }
    if (-not $engineId) { $engineId = $proc.Id }

    Set-Content -Path $PidFile -Value $engineId
    return $proc
}

function Stop-ByPidFile {
    param([string]$PidFile, [int]$WaitSec = 10)
    if (-not (Test-Path $PidFile)) { return }
    # $PID is a read-only automatic variable in PowerShell, so the decoded PID
    # needs its own name.
    $procId = [int](Get-Content $PidFile -Raw).Trim()
    # Kill the whole tree: engines are launched through cmd.exe (see
    # Start-BackgroundProcess), and the pid file may hold the wrapper's pid
    # when the WMI child lookup was unavailable.
    $killed = $false
    try {
        $null = & taskkill.exe /f /t /pid $procId 2>&1
        $killed = ($LASTEXITCODE -eq 0)
    } catch {
        $killed = $false
    }
    if (-not $killed) {
        $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
        if ($proc) {
            $proc.Kill()
            $proc.WaitForExit($WaitSec * 1000) | Out-Null
        }
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

# ── 5. Port → version mapping ─────────────────────────────────────────────────

function Get-MysqlPort {
    param([string]$Ver)
    switch ($Ver) {
        '5.7' { return [int](Get-EnvOr 'FQ_MYSQL_PORT_57' 13305) }
        '8.0' { return [int](Get-EnvOr 'FQ_MYSQL_PORT_80' 13306) }
        '8.4' { return [int](Get-EnvOr 'FQ_MYSQL_PORT_84' 13307) }
        default { throw "Unknown MySQL version: $Ver" }
    }
}

function Get-PgPort {
    param([string]$Ver)
    switch ($Ver) {
        '14' { return [int](Get-EnvOr 'FQ_PG_PORT_14' 15433) }
        '15' { return [int](Get-EnvOr 'FQ_PG_PORT_15' 15435) }
        '16' { return [int](Get-EnvOr 'FQ_PG_PORT_16' 15434) }
        '17' { return [int](Get-EnvOr 'FQ_PG_PORT_17' 15436) }
        default { throw "Unknown PG version: $Ver" }
    }
}

function Get-InfluxPort {
    param([string]$Ver)
    switch ($Ver -replace '\.', '') {
        '30' { return [int](Get-EnvOr 'FQ_INFLUX_PORT_30' 18086) }
        '35' { return [int](Get-EnvOr 'FQ_INFLUX_PORT_35' 18087) }
        default { throw "Unknown InfluxDB version: $Ver" }
    }
}

# ══════════════════════════════════════════════════════════════════════════════
# 6. MySQL
# ══════════════════════════════════════════════════════════════════════════════

function Get-MysqlUrl {
    param([string]$Ver)
    $tag = $Ver -replace '\.', ''
    $override = [System.Environment]::GetEnvironmentVariable("FQ_MYSQL_TARBALL_$tag")
    if ($override) { return $override }

    $mirror = Get-EnvOr 'FQ_MYSQL_MIRROR' 'https://dev.mysql.com/get/Downloads'

    switch ($Ver) {
        '5.7' {
            $patch   = '5.7.44'
            $zipName = "mysql-$patch-winx64.zip"
            return "$mirror/MySQL-5.7/$zipName"
        }
        '8.0' {
            $patch   = '8.0.45'
            $zipName = "mysql-$patch-winx64.zip"
            return "$mirror/MySQL-8.0/$zipName"
        }
        '8.4' {
            $patch   = '8.4.5'
            $zipName = "mysql-$patch-winx64.zip"
            return "$mirror/MySQL-8.4/$zipName"
        }
        default { throw "Unsupported MySQL version: $Ver" }
    }
}

function Install-Mysql {
    param([string]$Ver, [string]$Base)
    $null = New-Item -ItemType Directory -Force $Base
    $url     = Get-MysqlUrl $Ver
    $zipFile = Join-Path $env:TEMP "fq-mysql-$Ver.zip"
    Invoke-Download $url $zipFile

    Info "MySQL ${Ver}: extracting ..."
    # The ZIP contains a single top-level directory (e.g. mysql-8.0.45-winx64\)
    # Extract to a temp dir then move the inner directory to $Base
    $tmp = Join-Path $env:TEMP "fq-mysql-$Ver-extract"
    if (Test-Path $tmp) { Remove-Item $tmp -Recurse -Force }
    Expand-Archive -Path $zipFile -DestinationPath $tmp
    $inner = Get-ChildItem $tmp -Directory | Select-Object -First 1
    if (-not $inner) { throw "MySQL ZIP had no top-level directory" }
    # Copy contents into $Base (bin\, lib\, share\, ...)
    Get-ChildItem $inner.FullName | Copy-Item -Destination $Base -Recurse -Force
    Remove-Item $tmp -Recurse -Force
}

function Initialize-Mysql {
    param([string]$Ver, [int]$Port, [string]$Base)
    $mysqld  = Join-Path $Base 'bin\mysqld.exe'
    $dataDir = Join-Path $Base 'data'
    $logDir  = Join-Path $Base 'log'
    $null    = New-Item -ItemType Directory -Force $logDir

    # Write minimal my.ini.  MySQL's option-file parser treats backslash as an
    # escape character, so a Windows path containing \t (e.g. ...\Local\taostest)
    # is read as a TAB and the whole datadir/basedir becomes unusable.  Forward
    # slashes are accepted by MySQL and avoid the problem entirely.
    $myIni = Join-Path $Base 'my.ini'
    $baseFwd = $Base -replace '\\', '/'
    $dataFwd = $dataDir -replace '\\', '/'
    $logFwd  = $logDir -replace '\\', '/'
    $iniContent = @"
[mysqld]
basedir=$baseFwd
datadir=$dataFwd
port=$Port
socket=
bind-address=127.0.0.1
log-error=$logFwd/error.log
pid-file=$baseFwd/run/mysqld.pid
max_connections=200
character-set-server=utf8mb4
collation-server=utf8mb4_unicode_ci
"@
    # MySQL's option-file parser rejects a UTF-8 BOM, which PowerShell 5.1
    # adds with -Encoding utf8. Write the file without a BOM.
    [System.IO.File]::WriteAllText($myIni, $iniContent, (New-Object System.Text.UTF8Encoding($false)))

    Info "MySQL ${Ver}: running --initialize-insecure ..."
    $logFile = Join-Path $logDir 'mysqld-init.log'
    $proc = Start-Process -FilePath $mysqld `
        -ArgumentList "--defaults-file=`"$myIni`"", '--initialize-insecure', "--user=root" `
        -NoNewWindow -Wait -PassThru `
        -RedirectStandardError $logFile
    if ($proc.ExitCode -ne 0) {
        throw "MySQL $Ver --initialize-insecure failed (exit $($proc.ExitCode)); see $logFile"
    }
}

function Start-Mysql {
    param([string]$Ver, [int]$Port, [string]$Base)
    $mysqld  = Join-Path $Base 'bin\mysqld.exe'
    $myIni   = Join-Path $Base 'my.ini'
    $logDir  = Join-Path $Base 'log'
    $runDir  = Join-Path $Base 'run'
    $pidFile = Join-Path $runDir 'mysqld.pid'
    $null    = New-Item -ItemType Directory -Force $runDir

    # Check for stale PID
    if (Test-Path $pidFile) {
        $stalePid = [int](Get-Content $pidFile -Raw -ErrorAction SilentlyContinue).Trim()
        $staleProc = Get-Process -Id $stalePid -ErrorAction SilentlyContinue
        if (-not $staleProc) {
            Info "MySQL ${Ver}: removing stale PID file"
            Remove-Item $pidFile -Force
        }
    }

    $libPrivate = Join-Path $Base 'lib\private'
    $envPairs = @()
    if (Test-Path $libPrivate) {
        $envPairs += "PATH=$libPrivate;$env:PATH"
    }

    Info "MySQL ${Ver}: starting on port $Port ..."
    Start-BackgroundProcess `
        -Exe        $mysqld `
        -ArgList    "--defaults-file=`"$myIni`"", "--port=$Port" `
        -LogFile    (Join-Path $logDir 'mysqld.log') `
        -PidFile    $pidFile `
        -EnvPairs   $envPairs | Out-Null
}

function Setup-MysqlAuth {
    param([string]$Ver, [int]$Port, [string]$Base)
    $mysql = Join-Path $Base 'bin\mysql.exe'
    $libPrivate = Join-Path $Base 'lib\private'
    $env_backup = $env:PATH
    if (Test-Path $libPrivate) { $env:PATH = "$libPrivate;$env:PATH" }

    try {
        # Check if password already works
        & $mysql -h 127.0.0.1 -P $Port -u root -p"$MysqlPass" --connect-timeout=5 -e "SELECT 1;" 2>$null
        if ($LASTEXITCODE -eq 0) {
            Info "MySQL ${Ver}: auth already configured."
            return
        }
    } catch { }

    Info "MySQL ${Ver}: configuring root auth ..."
    $major = [int]($Ver.Split('.')[0])
    if ($major -ge 8) {
        $authSql = "ALTER USER IF EXISTS 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '$MysqlPass'; " +
                   "CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED WITH mysql_native_password BY '$MysqlPass'; " +
                   "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION; " +
                   "FLUSH PRIVILEGES;"
    } else {
        $authSql = "UPDATE mysql.user SET authentication_string=PASSWORD('$MysqlPass'), plugin='mysql_native_password' WHERE User='root'; " +
                   "DROP USER IF EXISTS 'root'@'%'; " +
                   "CREATE USER 'root'@'%' IDENTIFIED BY '$MysqlPass'; " +
                   "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION; " +
                   "FLUSH PRIVILEGES;"
    }

    try {
        # TCP no-password connection (fresh --initialize-insecure)
        & $mysql -h 127.0.0.1 -P $Port -u root --connect-timeout=10 -e $authSql 2>$null
        if ($LASTEXITCODE -eq 0) {
            Info "MySQL ${Ver}: auth configured via TCP."
        } else {
            Warn "MySQL ${Ver}: auth setup returned exit code $LASTEXITCODE."
        }
    } catch {
        Warn "MySQL ${Ver}: could not configure auth automatically: $_"
    } finally {
        $env:PATH = $env_backup
    }
}

function Apply-MysqlTls {
    param([string]$Ver, [int]$Port, [string]$Base)
    $certDst = Join-Path $Base 'certs'
    $mysql   = Join-Path $Base 'bin\mysql.exe'

    if (Test-Path (Join-Path $certDst 'ca.pem')) {
        Info "MySQL ${Ver}: TLS certs already present, skipping."
        return
    }
    if (-not (Test-Path (Join-Path $CertSrc 'ca.pem'))) {
        Info "MySQL ${Ver}: no cert source at $CertSrc, skipping TLS."
        return
    }

    Info "MySQL ${Ver}: deploying TLS certificates ..."
    $null = New-Item -ItemType Directory -Force $certDst
    Copy-Item (Join-Path $CertSrc 'ca.pem')                (Join-Path $certDst 'ca.pem') -Force
    Copy-Item (Join-Path $CertSrc 'mysql\server.pem')      (Join-Path $certDst 'server.pem') -Force
    Copy-Item (Join-Path $CertSrc 'mysql\server-key.pem')  (Join-Path $certDst 'server-key.pem') -Force
    Copy-Item (Join-Path $CertSrc 'mysql\client.pem')      (Join-Path $certDst 'client.pem') -Force
    Copy-Item (Join-Path $CertSrc 'mysql\client-key.pem')  (Join-Path $certDst 'client-key.pem') -Force

    $major = [int]($Ver.Split('.')[0])
    $certFwd = $certDst -replace '\\', '/'
    $libPrivate = Join-Path $Base 'lib\private'
    $env_backup = $env:PATH
    if (Test-Path $libPrivate) { $env:PATH = "$libPrivate;$env:PATH" }

    try {
        if ($major -ge 8) {
            & $mysql -h 127.0.0.1 -P $Port -u $MysqlUser -p"$MysqlPass" --connect-timeout=5 `
                -e "SET PERSIST ssl_ca='$certFwd/ca.pem'; SET PERSIST ssl_cert='$certFwd/server.pem'; SET PERSIST ssl_key='$certFwd/server-key.pem';" 2>$null
            Info "MySQL ${Ver}: TLS SET PERSIST applied."
        } else {
            $tlsCnf = Join-Path $Base 'my-tls.cnf'
            @"
[mysqld]
ssl_ca=$certFwd/ca.pem
ssl_cert=$certFwd/server.pem
ssl_key=$certFwd/server-key.pem
"@
            [System.IO.File]::WriteAllText($tlsCnf, $tlsContent, (New-Object System.Text.UTF8Encoding($false)))
            $pidFile = Join-Path $Base 'run\mysqld.pid'
            Stop-ByPidFile $pidFile
            Start-Sleep -Seconds 1
            Start-Mysql $Ver $Port $Base
            if (-not (Wait-Port $Port 30)) {
                Warn "MySQL ${Ver}: did not come back after TLS restart."
            }
        }
    } catch {
        Warn "MySQL ${Ver}: TLS setup failed: $_"
    } finally {
        $env:PATH = $env_backup
    }
}

function Reset-MysqlEnv {
    param([string]$Ver, [int]$Port, [string]$Base)
    $mysql = Join-Path $Base 'bin\mysql.exe'
    $libPrivate = Join-Path $Base 'lib\private'
    $env_backup = $env:PATH
    if (Test-Path $libPrivate) { $env:PATH = "$libPrivate;$env:PATH" }

    $dbs = @(
        'fq_path_m','fq_path_m2','fq_src_m','fq_type_m','fq_sql_m',
        'fq_push_m','fq_local_m','fq_stab_m','fq_perf_m','fq_compat_m'
    )
    $dropSql = ($dbs | ForEach-Object { "DROP DATABASE IF EXISTS ``$_``;" }) -join ' '
    $dropSql += " DROP USER IF EXISTS 'tls_user'@'%';"
    $dropSql += " CREATE USER 'tls_user'@'%' IDENTIFIED BY 'tls_pwd' REQUIRE SSL;"
    $dropSql += " GRANT ALL PRIVILEGES ON *.* TO 'tls_user'@'%';"
    $dropSql += " FLUSH PRIVILEGES;"

    try {
        & $mysql -h 127.0.0.1 -P $Port -u $MysqlUser -p"$MysqlPass" `
            --connect-timeout=5 -e $dropSql 2>$null
        Info "MySQL ${Ver} @ ${Port}: reset complete."
    } catch {
        Warn "MySQL ${Ver} @ ${Port}: reset had warnings: $_"
    } finally {
        $env:PATH = $env_backup
    }
}

# Load a minimal set of named time zones into mysql.time_zone* so that
# CONVERT_TZ(<col>, '<zone>', '+00:00') resolves.  The Unix setup loads them
# with mysql_tzinfo_to_sql; that binary does not exist on Windows, so the same
# rows are generated from the Windows dynamic time zone data.
function New-MysqlTzSql {
    param([string]$Path)
    $zones = @(
        @{ Name = 'Asia/Shanghai';       Win = 'China Standard Time' },
        @{ Name = 'UTC';                 Win = 'UTC' },
        @{ Name = 'America/New_York';    Win = 'Eastern Standard Time' },
        @{ Name = 'America/Los_Angeles'; Win = 'Pacific Standard Time' },
        @{ Name = 'Europe/London';       Win = 'GMT Standard Time' }
    )
    $sb = New-Object System.Text.StringBuilder
    [void]$sb.AppendLine('SET NAMES utf8mb4;')
    $tzId = 1000
    $start = [datetime]::new(1970, 1, 1, 0, 0, 0, [System.DateTimeKind]::Utc)
    $end = [datetime]::new(2040, 1, 1, 0, 0, 0, [System.DateTimeKind]::Utc)
    foreach ($z in $zones) {
        $tzId++
        $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById($z.Win)
        $types = @{}
        $transitions = New-Object System.Collections.ArrayList
        $stepHours = 6
        $t = $start
        $prevKey = $null
        while ($t -lt $end) {
            $offset = [int]$tz.GetUtcOffset($t).TotalSeconds
            $isDst = if ($tz.IsDaylightSavingTime($t)) { 1 } else { 0 }
            $key = "$offset|$isDst"
            if ($null -eq $prevKey) {
                if (-not $types.ContainsKey($key)) { $types[$key] = $types.Count }
                [void]$transitions.Add(@{ Time = [int64]([DateTimeOffset]$t).ToUnixTimeSeconds(); Type = $types[$key] })
                $prevKey = $key
            } elseif ($key -ne $prevKey) {
                $lo = $t.AddHours(-$stepHours)
                $hi = $t
                while (($hi - $lo).TotalSeconds -gt 60) {
                    $mid = $lo.AddSeconds([int]((($hi - $lo).TotalSeconds) / 2))
                    $midOffset = [int]$tz.GetUtcOffset($mid).TotalSeconds
                    $midDst = if ($tz.IsDaylightSavingTime($mid)) { 1 } else { 0 }
                    if ("$midOffset|$midDst" -eq $prevKey) { $lo = $mid } else { $hi = $mid }
                }
                if (-not $types.ContainsKey($key)) { $types[$key] = $types.Count }
                [void]$transitions.Add(@{ Time = [int64]([DateTimeOffset]$hi).ToUnixTimeSeconds(); Type = $types[$key] })
                $prevKey = $key
            }
            $t = $t.AddHours($stepHours)
        }
        [void]$sb.AppendLine("DELETE FROM mysql.time_zone_name WHERE Name='$($z.Name)';")
        [void]$sb.AppendLine("DELETE FROM mysql.time_zone WHERE Time_zone_id=$tzId;")
        [void]$sb.AppendLine("DELETE FROM mysql.time_zone_transition WHERE Time_zone_id=$tzId;")
        [void]$sb.AppendLine("DELETE FROM mysql.time_zone_transition_type WHERE Time_zone_id=$tzId;")
        [void]$sb.AppendLine("INSERT INTO mysql.time_zone (Time_zone_id, Use_leap_seconds) VALUES ($tzId, 'N');")
        [void]$sb.AppendLine("INSERT INTO mysql.time_zone_name (Name, Time_zone_id) VALUES ('$($z.Name)', $tzId);")
        foreach ($k in $types.Keys) {
            $parts = $k.Split('|')
            [void]$sb.AppendLine("INSERT INTO mysql.time_zone_transition_type (Time_zone_id, Transition_type_id, Offset, Is_DST, Abbreviation) VALUES ($tzId, $($types[$k]), $($parts[0]), $($parts[1]), '');")
        }
        foreach ($tr in $transitions) {
            [void]$sb.AppendLine("INSERT INTO mysql.time_zone_transition (Time_zone_id, Transition_time, Transition_type_id) VALUES ($tzId, $($tr.Time), $($tr.Type));")
        }
    }
    [System.IO.File]::WriteAllText($Path, $sb.ToString(), (New-Object System.Text.UTF8Encoding($false)))
}

function Install-MysqlTimeZones {
    param([string]$Ver, [int]$Port, [string]$Base)
    $mysql = Join-Path $Base 'bin\mysql.exe'
    $libPrivate = Join-Path $Base 'lib\private'
    $env_backup = $env:PATH
    # mysql.exe always prints the password warning on stderr; with native
    # commands that surfaces as a NativeCommandError here, so the calls are run
    # without the try/catch and judged by their output instead.
    $ErrorActionPreference = 'SilentlyContinue'
    if (Test-Path $libPrivate) { $env:PATH = "$libPrivate;$env:PATH" }
    $present = & $mysql -h 127.0.0.1 -P $Port -u $MysqlUser -p"$MysqlPass" `
        --connect-timeout=5 -N -B `
        -e "SELECT COUNT(*) FROM mysql.time_zone_name WHERE Name IN ('Asia/Shanghai','UTC');" 2>$null
    $loaded = ($present -ne $null) -and ([string]::Join('', @($present)).Trim() -match '^\d+$') -and
              ([int]([string]::Join('', @($present)).Trim()) -ge 2)
    if (-not $loaded) {
        Info "MySQL ${Ver}: loading named time zones into mysql.time_zone* ..."
        $sqlPath = Join-Path $env:TEMP "fq-mysql-tz-$Ver.sql"
        New-MysqlTzSql -Path $sqlPath
        # Feed the script through stdin (the client's `source` command only
        # exists in interactive mode).
        $loadOut = & cmd /c "`"$mysql`" -h 127.0.0.1 -P $Port -u $MysqlUser -p$MysqlPass --connect-timeout=10 --default-character-set=utf8mb4 < `"$sqlPath`"" 2>&1
        if ($LASTEXITCODE -ne 0) {
            Warn "MySQL ${Ver}: timezone SQL load failed (exit $LASTEXITCODE): $([string]::Join(' ', @($loadOut)))"
        } else {
            Remove-Item $sqlPath -Force -ErrorAction SilentlyContinue
        }
    }
    $check = & $mysql -h 127.0.0.1 -P $Port -u $MysqlUser -p"$MysqlPass" `
        --connect-timeout=5 -N -B `
        -e "SELECT IF(CONVERT_TZ('2024-07-01 05:00:00','Asia/Shanghai','+00:00') IS NULL,'NULL','OK'), IF(CONVERT_TZ('2024-07-01 05:00:00','America/Los_Angeles','+00:00') IS NULL,'NULL','OK');" 2>$null
    $verdict = [string]::Join('', @($check)).Trim()
    if ($verdict -match 'OK.*OK') {
        Info "MySQL ${Ver}: timezone tables ready ($verdict)."
    } else {
        Warn "MySQL ${Ver}: timezone tables not usable ($verdict)."
    }
    $env:PATH = $env_backup
}

function Ensure-Mysql {
    param([string]$Ver)
    $port = Get-MysqlPort $Ver
    $base = Join-Path $FqBase "mysql\$Ver"
    $mysqld = Join-Path $base 'bin\mysqld.exe'
    Info "MySQL ${Ver}: port=$port, base=$base"

    # Already running → reset and return
    if (Test-PortOpen $port) {
        Info "MySQL ${Ver}: port $port open — already running, resetting test env."
        Reset-MysqlEnv $Ver $port $base
        Install-MysqlTimeZones $Ver $port $base
        return
    }

    # Installed but stopped
    if (Test-Path $mysqld) {
        Info "MySQL ${Ver}: installation found, attempting start ..."
        Start-Mysql $Ver $port $base
        if (Wait-Port $port 30) {
            Info "MySQL ${Ver}: started OK."
            Reset-MysqlEnv $Ver $port $base
            Install-MysqlTimeZones $Ver $port $base
            return
        }
        Warn "MySQL ${Ver}: failed to start; reinitializing data dir."
        # Kill any leftover process on that port
        $pidFile = Join-Path $base 'run\mysqld.pid'
        Stop-ByPidFile $pidFile
        Remove-Item (Join-Path $base 'data') -Recurse -Force -ErrorAction SilentlyContinue
    } else {
        Install-Mysql $Ver $base
    }

    Initialize-Mysql $Ver $port $base
    Start-Mysql      $Ver $port $base

    if (-not (Wait-Port $port 90)) {
        Err "MySQL ${Ver}: timed out waiting for port $port."
        $script:OverallOk = $false
        return
    }
    Setup-MysqlAuth $Ver $port $base
    Apply-MysqlTls  $Ver $port $base
    Reset-MysqlEnv  $Ver $port $base
    Install-MysqlTimeZones $Ver $port $base
    Info "MySQL ${Ver}: ready."
}

# ══════════════════════════════════════════════════════════════════════════════
# 7. PostgreSQL
# ══════════════════════════════════════════════════════════════════════════════

function Get-PgUrl {
    param([string]$Ver)
    $tag = $Ver -replace '\.', ''
    $override = [System.Environment]::GetEnvironmentVariable("FQ_PG_TARBALL_$tag")
    if ($override) { return $override }

    $mirror = Get-EnvOr 'FQ_PG_MIRROR' 'https://get.enterprisedb.com/postgresql'

    # EnterpriseDB Windows ZIP (no installer required)
    $minorMap = @{
        '14' = '14.17-1'
        '15' = '15.12-1'
        '16' = '16.8-1'
        '17' = '17.4-1'
    }
    if (-not $minorMap.ContainsKey($Ver)) { throw "Unsupported PG version: $Ver" }
    $patch = $minorMap[$Ver]
    return "$mirror/postgresql-$patch-windows-x64-binaries.zip"
}

function Install-Pg {
    param([string]$Ver, [string]$Base)
    $null = New-Item -ItemType Directory -Force $Base
    $url     = Get-PgUrl $Ver
    $zipFile = Join-Path $env:TEMP "fq-pg-$Ver.zip"
    Invoke-Download $url $zipFile

    Info "PostgreSQL ${Ver}: extracting ..."
    $tmp = Join-Path $env:TEMP "fq-pg-$Ver-extract"
    if (Test-Path $tmp) { Remove-Item $tmp -Recurse -Force }
    Expand-Archive -Path $zipFile -DestinationPath $tmp
    # EDB ZIP has a top-level 'pgsql\' directory
    $inner = Get-ChildItem $tmp -Directory | Select-Object -First 1
    if (-not $inner) { throw "PG ZIP had no top-level directory" }
    Get-ChildItem $inner.FullName | Copy-Item -Destination $Base -Recurse -Force
    Remove-Item $tmp -Recurse -Force
}

function Initialize-Pg {
    param([string]$Ver, [string]$Base)
    $initdb  = Join-Path $Base 'bin\initdb.exe'
    $dataDir = Join-Path $Base 'data'
    $logDir  = Join-Path $Base 'log'
    $null    = New-Item -ItemType Directory -Force $logDir, $dataDir

    $pwFile = Join-Path $env:TEMP 'fq-pg-pwfile.tmp'
    Set-Content -Path $pwFile -Value $PgPass -Encoding ascii -NoNewline
    $logFile = Join-Path $logDir 'initdb.log'
    Info "PostgreSQL ${Ver}: running initdb ..."
    $proc = Start-Process -FilePath $initdb `
        -ArgumentList "-D `"$dataDir`"", "-U $PgUser", "--pwfile=`"$pwFile`"", '--encoding=UTF8', '--locale=C' `
        -NoNewWindow -Wait -PassThru `
        -RedirectStandardError $logFile
    Remove-Item $pwFile -Force -ErrorAction SilentlyContinue
    if ($proc.ExitCode -ne 0) {
        throw "PostgreSQL $Ver initdb failed (exit $($proc.ExitCode)); see $logFile"
    }
}

function Start-Pg {
    param([string]$Ver, [int]$Port, [string]$Base)
    $pgCtl   = Join-Path $Base 'bin\pg_ctl.exe'
    $dataDir = Join-Path $Base 'data'
    $logDir  = Join-Path $Base 'log'
    $null    = New-Item -ItemType Directory -Force $logDir

    Info "PostgreSQL ${Ver}: starting on port $Port ..."
    # pg_ctl start writes its own log via -l; it also creates postmaster.pid.
    # Redirect its stdio to files: the postgres server inherits the handles and
    # would otherwise keep the caller's stdout pipe open (a wrapper using
    # `... | Tee-Object` then waits forever for EOF).  Do NOT use -Wait:
    # PowerShell waits for descendants too, which would also block forever.
    $proc = Start-Process -FilePath $pgCtl `
        -ArgumentList 'start', "-D `"$dataDir`"", "-l `"$(Join-Path $logDir 'pg.log')`"", "-o `"-p $Port`"" `
        -NoNewWindow -PassThru `
        -RedirectStandardOutput (Join-Path $logDir 'pg_ctl.out') `
        -RedirectStandardError  (Join-Path $logDir 'pg_ctl.err')
    if (-not $proc.WaitForExit(60000)) {
        Info "PostgreSQL ${Ver}: pg_ctl did not return within 60s; continuing (port probe follows)"
    }
    # pg_ctl returns 0 even if postmaster hasn't fully started; wait_port handles that
}

function Write-PgSslConf {
    param([string]$DataDir, [string]$CertDir)
    $conf = Join-Path $DataDir 'postgresql.conf'
    $hba  = Join-Path $DataDir 'pg_hba.conf'
    # Idempotent
    if ((Get-Content $conf -Raw -ErrorAction SilentlyContinue) -match '(?m)^ssl = on') { return }
    $certFwd = $CertDir -replace '\\', '/'
    Add-Content $conf @"

ssl = on
ssl_ca_file = '$certFwd/ca.pem'
ssl_cert_file = '$certFwd/server.pem'
ssl_key_file = '$certFwd/server.key'
"@
    if (-not ((Get-Content $hba -Raw -ErrorAction SilentlyContinue) -match 'hostssl.*cert')) {
        Add-Content $hba "`nhostssl all all 0.0.0.0/0 cert clientcert=verify-full"
    }
}

function Reset-PgEnv {
    param([string]$Ver, [int]$Port, [string]$Base)
    $psql    = Join-Path $Base 'bin\psql.exe'
    $dataDir = Join-Path $Base 'data'
    $certDst = Join-Path $dataDir 'certs'

    # Deploy certs on first call
    if (-not (Test-Path $certDst) -and (Test-Path (Join-Path $CertSrc 'ca.pem'))) {
        Info "PostgreSQL ${Ver}: deploying TLS certificates ..."
        $null = New-Item -ItemType Directory -Force $certDst
        Copy-Item (Join-Path $CertSrc 'ca.pem')            (Join-Path $certDst 'ca.pem') -Force
        Copy-Item (Join-Path $CertSrc 'pg\server.pem')     (Join-Path $certDst 'server.pem') -Force
        Copy-Item (Join-Path $CertSrc 'pg\server.key')     (Join-Path $certDst 'server.key') -Force
        Copy-Item (Join-Path $CertSrc 'pg\client.pem')     (Join-Path $certDst 'client.pem') -Force
        Copy-Item (Join-Path $CertSrc 'pg\client-key.pem') (Join-Path $certDst 'client-key.pem') -Force
        Write-PgSslConf $dataDir $certDst
        try {
            $env:PGPASSWORD = $PgPass
            & $psql -h 127.0.0.1 -p $Port -U $PgUser -d postgres `
                -c "SELECT pg_reload_conf();" 2>$null | Out-Null
        } catch { } finally { $env:PGPASSWORD = $null }
    }

    Info "PostgreSQL ${Ver} @ ${Port}: resetting test databases ..."
    # Discover all non-system databases and drop them
    $env:PGPASSWORD = $PgPass
    $env:PGCONNECT_TIMEOUT = '5'
    try {
        $dbsRaw = & $psql -h 127.0.0.1 -p $Port -U $PgUser -d postgres `
            -t -A `
            -c "SELECT datname FROM pg_database WHERE datistemplate = false AND datname <> 'postgres';" `
            2>$null
        $dbs = @($dbsRaw | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' })
        foreach ($db in $dbs) {
            try {
                & $psql -h 127.0.0.1 -p $Port -U $PgUser -d postgres `
                    -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$db' AND pid <> pg_backend_pid();" `
                    2>$null | Out-Null
                & $psql -h 127.0.0.1 -p $Port -U $PgUser -d postgres `
                    -c "DROP DATABASE IF EXISTS `"$db`";" `
                    2>$null | Out-Null
            } catch { <# ignore per-db errors #> }
        }
        Info "PostgreSQL ${Ver} @ ${Port}: reset complete (dropped: $($dbs -join ', '))."
    } catch {
        Warn "PostgreSQL ${Ver} @ ${Port}: reset had warnings: $_"
    } finally {
        $env:PGPASSWORD = $null
        $env:PGCONNECT_TIMEOUT = $null
    }
}

# The federated-query PostgreSQL fixtures create the postgis extension, which
# the EnterpriseDB binaries do not ship.  Install the official PostGIS bundle
# for this major version when it is missing.
function Install-Postgis {
    param([string]$Ver, [string]$Base)
    $marker = Join-Path $Base 'share\extension\postgis.control'
    if (Test-Path $marker) { return }
    Info "PostgreSQL ${Ver}: installing PostGIS extension ..."
    try {
        $index = Invoke-WebRequest -Uri "https://download.osgeo.org/postgis/windows/pg${Ver}/" `
            -UseBasicParsing -TimeoutSec 60
        $href = ($index.Links | Where-Object { $_.href -match 'x64\.zip$' } |
                 Select-Object -First 1 -ExpandProperty href)
        if (-not $href) {
            Warn "PostGIS: no bundle listed for pg${Ver}; geometry tests will fail."
            return
        }
        $url = "https://download.osgeo.org/postgis/windows/pg${Ver}/$href"
        $zip = Join-Path $env:TEMP "postgis-pg${Ver}.zip"
        Invoke-Download $url $zip
        $tmp = Join-Path $env:TEMP "postgis-pg${Ver}-extract"
        if (Test-Path $tmp) { Remove-Item $tmp -Recurse -Force }
        Expand-Archive -Path $zip -DestinationPath $tmp
        $root = Get-ChildItem $tmp -Directory | Select-Object -First 1
        if ($root) {
            # File-by-file: a running postmaster keeps some shared libraries
            # (OpenSSL) open, and a single locked file must not abort the copy
            # of the PostGIS pieces.
            foreach ($sub in @('bin', 'lib', 'share')) {
                $srcDir = Join-Path $root.FullName $sub
                $dstDir = Join-Path $Base $sub
                if (-not (Test-Path $srcDir)) { continue }
                New-Item -ItemType Directory -Force $dstDir | Out-Null
                Get-ChildItem $srcDir -Recurse -File | ForEach-Object {
                    $rel = $_.FullName.Substring($srcDir.Length).TrimStart('\')
                    $target = Join-Path $dstDir $rel
                    $parent = Split-Path $target -Parent
                    if (-not (Test-Path $parent)) {
                        New-Item -ItemType Directory -Force $parent | Out-Null
                    }
                    try {
                        Copy-Item -LiteralPath $_.FullName -Destination $target -Force -ErrorAction Stop
                    } catch {
                        Warn "PostGIS: skipped $sub\$rel (file in use)"
                    }
                }
            }
        }
        Remove-Item $tmp -Recurse -Force
        if (Test-Path $marker) {
            Info "PostgreSQL ${Ver}: PostGIS installed."
        } else {
            Warn "PostGIS: postgis.control still missing after extraction."
        }
    } catch {
        Warn "PostGIS install failed: $_"
    }
}

function Ensure-Pg {
    param([string]$Ver)
    $port  = Get-PgPort $Ver
    $base  = Join-Path $FqBase "pg\$Ver"
    $pgCtl = Join-Path $base 'bin\pg_ctl.exe'
    Info "PostgreSQL ${Ver}: port=$port, base=$base"
    Install-Postgis $Ver $base

    if (Test-PortOpen $port) {
        Info "PostgreSQL ${Ver}: port $port open — already running, resetting test env."
        Reset-PgEnv $Ver $port $base
        return
    }

    if (Test-Path $pgCtl) {
        Info "PostgreSQL ${Ver}: installation found, attempting start ..."
        Start-Pg $Ver $port $base
        if (Wait-Port $port 30) {
            Info "PostgreSQL ${Ver}: started OK."
            Reset-PgEnv $Ver $port $base
            return
        }
        Warn "PostgreSQL ${Ver}: failed to start; reinitializing data dir."
        $dataDir = Join-Path $base 'data'
        # Kill via postmaster.pid before wiping
        $pm = Join-Path $dataDir 'postmaster.pid'
        if (Test-Path $pm) {
            $stPid = [int](Get-Content $pm -Raw -ErrorAction SilentlyContinue).Split("`n")[0].Trim()
            $stProc = Get-Process -Id $stPid -ErrorAction SilentlyContinue
            if ($stProc) { $stProc.Kill(); Start-Sleep 1 }
        }
        Remove-Item $dataDir -Recurse -Force -ErrorAction SilentlyContinue
    } else {
        Install-Pg $Ver $base
    }

    Initialize-Pg $Ver $base
    Start-Pg      $Ver $port $base

    if (-not (Wait-Port $port 90)) {
        Err "PostgreSQL ${Ver}: timed out waiting for port $port."
        $script:OverallOk = $false
        return
    }
    Reset-PgEnv $Ver $port $base
    Info "PostgreSQL ${Ver}: ready."
}

# ══════════════════════════════════════════════════════════════════════════════
# 8. InfluxDB v3
# ══════════════════════════════════════════════════════════════════════════════

function Get-InfluxUrl {
    param([string]$Ver)
    $tag = $Ver -replace '\.', ''
    $override = [System.Environment]::GetEnvironmentVariable("FQ_INFLUX_TARBALL_$tag")
    if ($override) { return $override }

    $patch = switch ($Ver) {
        '3.0' { '3.0.3' }
        '3.5' { '3.4.0' }
        default { "$Ver.0" }
    }
    $mirror = Get-EnvOr 'FQ_INFLUX_MIRROR' 'https://dl.influxdata.com/influxdb/releases'
    # InfluxData names the Windows builds with a hyphen: influxdb3-core-<ver>-windows_amd64.zip
    return "$mirror/influxdb3-core-${patch}-windows_amd64.zip"
}

function Install-Influx {
    param([string]$Ver, [string]$Base)
    $null = New-Item -ItemType Directory -Force $Base
    $url     = Get-InfluxUrl $Ver
    $zipFile = Join-Path $env:TEMP "fq-influx-$Ver.zip"
    Invoke-Download $url $zipFile

    Info "InfluxDB ${Ver}: extracting ..."
    $tmp = Join-Path $env:TEMP "fq-influx-$Ver-extract"
    if (Test-Path $tmp) { Remove-Item $tmp -Recurse -Force }
    Expand-Archive -Path $zipFile -DestinationPath $tmp

    # The ZIP may have a top-level dir; look for influxdb3.exe within it
    $exe = Get-ChildItem $tmp -Recurse -Filter 'influxdb3.exe' | Select-Object -First 1
    if (-not $exe) {
        # Older naming: influxd.exe
        $exe = Get-ChildItem $tmp -Recurse -Filter 'influxd.exe' | Select-Object -First 1
    }
    if (-not $exe) { throw "Could not find influxdb3.exe or influxd.exe in the ZIP" }

    $binDir = Join-Path $Base 'bin'
    if (Test-Path $binDir) { Remove-Item $binDir -Recurse -Force }
    $null = New-Item -ItemType Directory -Force $binDir
    # Copy the whole distribution directory, not just the executable: the
    # Windows build loads python313.dll / python3.dll / vcruntime*.dll from its
    # own directory and ships a python/ tree next to it.
    Copy-Item (Join-Path $exe.Directory.FullName '*') $binDir -Recurse -Force
    Remove-Item $tmp -Recurse -Force
}

function Start-Influx {
    param([string]$Ver, [int]$Port, [string]$Base)
    $influxd  = Get-ChildItem (Join-Path $Base 'bin') -Filter 'influxdb3.exe' -ErrorAction SilentlyContinue
    if (-not $influxd) {
        $influxd = Get-ChildItem (Join-Path $Base 'bin') -Filter 'influxd.exe' -ErrorAction SilentlyContinue
    }
    if (-not $influxd) { throw "InfluxDB $Ver binary not found under $Base\bin" }

    $dataDir = Join-Path $Base 'data'
    $logDir  = Join-Path $Base 'log'
    $runDir  = Join-Path $Base 'run'
    $pidFile  = Join-Path $runDir 'influxd.pid'
    $null    = New-Item -ItemType Directory -Force $dataDir, $logDir, $runDir

    # Check for stale PID
    if (Test-Path $pidFile) {
        $stalePid = [int](Get-Content $pidFile -Raw -ErrorAction SilentlyContinue).Trim()
        $staleProc = Get-Process -Id $stalePid -ErrorAction SilentlyContinue
        if (-not $staleProc) {
            Info "InfluxDB ${Ver}: removing stale PID file"
            Remove-Item $pidFile -Force
        }
    }

    Info "InfluxDB ${Ver}: starting on port $Port ..."
    # Auth-enabled, mirroring ensure_ext_env.sh: catalog tokens are enforced so
    # the FQ auth scenarios can verify that wrong credentials fail.  The admin
    # token is created by Ensure-InfluxToken once the server is up.
    Start-BackgroundProcess `
        -Exe     $influxd.FullName `
        -ArgList 'serve', '--node-id', 'fq-test-node', '--http-bind', "127.0.0.1:$Port",
                 '--object-store', 'file', '--data-dir', $dataDir `
        -LogFile (Join-Path $logDir 'influxd.log') `
        -PidFile $pidFile | Out-Null
}

# True when the server rejects an unauthenticated request with 401.
function Test-InfluxAuthEnabled {
    param([int]$Port)
    $body = '{"db":"_internal","q":"SELECT 1","format":"json"}'
    try {
        $resp = Invoke-WebRequest -Uri "http://127.0.0.1:$Port/api/v3/query_sql" `
            -Method POST -ContentType 'application/json' `
            -Headers @{ Authorization = 'Bearer definitely-not-a-valid-token' } `
            -Body $body -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop
        return $false
    } catch {
        $status = 0
        if ($_.Exception.Response -ne $null) {
            $status = [int]$_.Exception.Response.StatusCode
        }
        return ($status -eq 401)
    }
}

# Create the admin token via the CLI and store it where the tests read it.
function Ensure-InfluxToken {
    param([string]$Ver, [int]$Port, [string]$Base)
    $tokenFile = Join-Path $Base 'admin_token.txt'
    if ((Test-Path $tokenFile) -and ((Get-Content $tokenFile -Raw -ErrorAction SilentlyContinue) -match '^apiv3_')) {
        return
    }
    $exe = Join-Path $Base 'bin\influxdb3.exe'
    if (-not (Test-Path $exe)) {
        Warn "InfluxDB ${Ver}: influxdb3.exe not found; cannot create an admin token."
        return
    }
    $out = & $exe create token --admin --host "http://127.0.0.1:$Port" 2>&1 | Out-String
    $m = [regex]::Match($out, 'apiv3_[A-Za-z0-9_\-]+')
    if (-not $m.Success) {
        Warn "InfluxDB ${Ver}: failed to create admin token. Output: $out"
        return
    }
    [System.IO.File]::WriteAllText($tokenFile, $m.Value, (New-Object System.Text.UTF8Encoding($false)))
    Info "InfluxDB ${Ver}: admin token written to $tokenFile"
}

# True when the running server answers an authenticated catalog request.
# A hung InfluxDB keeps the port open but never answers; without this check
# Reset-InfluxEnv's Invoke-RestMethod would wait forever (observed: the first
# FQ case sat in ensure_ext_env until the 7200s pytest watchdog fired).
function Test-InfluxResponsive {
    param([string]$Ver, [int]$Port, [int]$TimeoutSec = 10)
    $headers = @{}
    $tokenFile = Join-Path (Join-Path $FqBase "influxdb\$Ver") 'admin_token.txt'
    if (Test-Path $tokenFile) {
        $tok = (Get-Content $tokenFile -Raw -ErrorAction SilentlyContinue).Trim()
        if ($tok) { $headers['Authorization'] = "Bearer $tok" }
    }
    try {
        $null = Invoke-RestMethod -Method GET `
            -Uri "http://127.0.0.1:${Port}/api/v3/configure/database?format=json" `
            -Headers $headers -TimeoutSec $TimeoutSec -ErrorAction Stop
        return $true
    } catch {
        return $false
    }
}

function Reset-InfluxEnv {
    param([string]$Ver, [int]$Port)
    Info "InfluxDB ${Ver} @ ${Port}: resetting test databases ..."
    # The server enforces tokens, so the catalog API needs the admin token.
    $headers = @{}
    $tokenFile = Join-Path (Join-Path $FqBase "influxdb\$Ver") 'admin_token.txt'
    if (Test-Path $tokenFile) {
        $tok = (Get-Content $tokenFile -Raw -ErrorAction SilentlyContinue).Trim()
        if ($tok) { $headers['Authorization'] = "Bearer $tok" }
    }
    # Discover all databases via REST API, drop everything except _internal.
    # Every request is bounded: a hung server must not stall provisioning.
    $dropped = @()
    try {
        $result = Invoke-RestMethod -Method GET `
            -Uri "http://127.0.0.1:${Port}/api/v3/configure/database?format=json" `
            -Headers $headers -TimeoutSec 10 -ErrorAction Stop
        foreach ($entry in $result) {
            $db = $entry.'iox::database'
            if ($db -eq '_internal') { continue }
            try {
                Invoke-RestMethod -Method DELETE `
                    -Uri "http://127.0.0.1:${Port}/api/v3/configure/database?db=$db" `
                    -Headers $headers -TimeoutSec 10 -ErrorAction SilentlyContinue | Out-Null
            } catch { <# ignore #> }
            $dropped += $db
        }
    } catch { <# API unavailable; nothing to drop #> }
    Info "InfluxDB ${Ver} @ ${Port}: reset complete (dropped: $($dropped -join ', '))."
}

function Ensure-Influx {
    param([string]$Ver)
    $port   = Get-InfluxPort $Ver
    $base   = Join-Path $FqBase "influxdb\$Ver"
    $binDir = Join-Path $base 'bin'
    $hasExe = (Test-Path (Join-Path $binDir 'influxdb3.exe')) -or
              (Test-Path (Join-Path $binDir 'influxd.exe'))
    Info "InfluxDB ${Ver}: port=$port, base=$base"

    if (Test-PortOpen $port) {
        $authEnabled = Test-InfluxAuthEnabled $port
        $responsive  = $authEnabled -and (Test-InfluxResponsive $Ver $port)
        if ($responsive) {
            Info "InfluxDB ${Ver}: port $port open — already running, resetting test env."
            Reset-InfluxEnv $Ver $port
            Ensure-InfluxToken $Ver $port $base
            return
        }
        if ($authEnabled) {
            Warn "InfluxDB ${Ver}: port $port is open but the server does not answer; restarting it."
        } else {
            # An instance started with --without-auth cannot enforce the
            # credential scenarios; restart it in auth-enabled mode.
            Info "InfluxDB ${Ver}: port $port open but auth is disabled — restarting with auth enabled."
        }
        Stop-ByPidFile (Join-Path $base 'run\influxd.pid')
        $deadline = [DateTimeOffset]::UtcNow.AddSeconds(30)
        while ((Test-PortOpen $port) -and ([DateTimeOffset]::UtcNow -lt $deadline)) {
            Start-Sleep -Seconds 1
        }
        if (Test-PortOpen $port) {
            # The pid file can be stale after a crash; kill whoever still owns
            # the listening socket so the restart below can bind the port.
            $owner = (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
                      Select-Object -First 1).OwningProcess
            if ($owner) {
                Warn "InfluxDB ${Ver}: process $owner still holds port $port; killing it."
                Stop-Process -Id $owner -Force -ErrorAction SilentlyContinue
                Start-Sleep -Seconds 2
            }
        }
    }

    if ($hasExe) {
        Info "InfluxDB ${Ver}: installation found, attempting start ..."
        Start-Influx $Ver $port $base
        if (Wait-Port $port 30) {
            Info "InfluxDB ${Ver}: started OK."
            Reset-InfluxEnv $Ver $port
            Ensure-InfluxToken $Ver $port $base
            return
        }
        Warn "InfluxDB ${Ver}: failed to restart; re-installing ..."
        # Kill any lingering process
        $pidFile = Join-Path $base 'run\influxd.pid'
        Stop-ByPidFile $pidFile
        Remove-Item (Join-Path $base 'data') -Recurse -Force -ErrorAction SilentlyContinue
    }

    Install-Influx $Ver $base
    Start-Influx   $Ver $port $base

    if (-not (Wait-Port $port 90)) {
        Err "InfluxDB ${Ver}: timed out waiting for port $port."
        $script:OverallOk = $false
        return
    }

    # Health check
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(30)
    $healthy  = $false
    while (-not $healthy -and [DateTimeOffset]::UtcNow -lt $deadline) {
        try {
            $resp = Invoke-RestMethod -Uri "http://127.0.0.1:${port}/health" -TimeoutSec 3 -ErrorAction SilentlyContinue
            if ($resp.status -match 'pass|ok') { $healthy = $true }
        } catch { }
        if (-not $healthy) { Start-Sleep -Seconds 2 }
    }
    if (-not $healthy) { Warn "InfluxDB ${Ver}: health endpoint not passing (non-fatal)." }

    Reset-InfluxEnv $Ver $port
    Ensure-InfluxToken $Ver $port $base
    Info "InfluxDB ${Ver}: ready."
}

# ══════════════════════════════════════════════════════════════════════════════
# 9. SQL Server
# ══════════════════════════════════════════════════════════════════════════════
#
# Windows counterpart of ensure_ext_env.sh's SQL Server section.  Two pieces:
#
#   1. sqlcmd (go-sqlcmd, self-contained .exe) — used by the tests to prepare
#      remote data.  It carries its own TDS implementation, so it does not need
#      the ODBC driver.
#   2. "ODBC Driver 18 for SQL Server" — used by taosd's SQL Server provider
#      through the system driver manager (odbc32.dll).  On Windows a driver
#      must be registered with the driver manager, which is what the Microsoft
#      MSI does (the provider's "Driver={ODBC Driver 18 for SQL Server}"
#      fallback then resolves it).  Unlike unixODBC there is no portable
#      per-app registration, so the script installs the official MSI silently.
#
# The engine itself is NOT downloaded/extracted (there is no portable Windows
# build).  Order of resolution per version:
#   a. the configured host/port already answers  -> use it (remote or local);
#   b. a local SQL Server instance is installed  -> start it (service) and probe;
#   c. otherwise, if the host is local and the caller is an administrator,
#      install SQL Server Express silently (see Install-SqlServerEngine).
#      Without administrator rights the script fails with an actionable
#      message instead of pretending the engine is ready.

$SqlServerVersions = Get-EnvList 'FQ_SQLSERVER_VERSIONS' '2022'
$SqlServerHost     = Get-EnvOr 'FQ_SQLSERVER_HOST' '127.0.0.1'
$SqlServerUser     = Get-EnvOr 'FQ_SQLSERVER_USER' 'sa'
$SqlServerPass     = Get-EnvOr 'FQ_SQLSERVER_PASS' 'TaosFq!2026'
$SqlServerInstance = Get-EnvOr 'FQ_SQLSERVER_INSTANCE' 'FQSQL'
$SqlServerTools    = Join-Path $FqBase 'sqlserver\tools'
$SqlServerCache    = Join-Path $FqBase 'sqlserver\packages'
$SqlCmdUrl         = Get-EnvOr 'FQ_SQLCMD_URL' `
    'https://github.com/microsoft/go-sqlcmd/releases/download/v1.10.0/sqlcmd-windows-amd64.zip'
$MsOdbcUrl         = Get-EnvOr 'FQ_MSODBC_MSI_URL' 'https://go.microsoft.com/fwlink/?linkid=2378279'
$MsOdbcName        = 'ODBC Driver 18 for SQL Server'

function Get-SqlServerPort {
    param([string]$Ver)
    switch ($Ver) {
        '2019' { return [int](Get-EnvOr 'FQ_SQLSERVER_PORT_2019' 1434) }
        '2022' { return [int](Get-EnvOr 'FQ_SQLSERVER_PORT_2022' 1433) }
        default { return [int](Get-EnvOr 'FQ_SQLSERVER_PORT' 1433) }
    }
}

function Test-Admin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p  = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Test-LocalHost {
    param([string]$HostName)
    return @('127.0.0.1', 'localhost', '.', '::1') -contains $HostName
}

# ── sqlcmd (go-sqlcmd, portable) ─────────────────────────────────────────────
function Ensure-SqlCmd {
    $exe = Join-Path $SqlServerTools 'sqlcmd.exe'
    if (Test-Path $exe) { return $exe }

    # Prefer the portable go-sqlcmd.  The legacy sqlcmd.exe that ships with the
    # SQL Server client tools (ODBC 170) mis-parses the long UTF-8 batches the
    # FQ helpers send (it reports fragments of the SQL as errors), which made
    # the SQL Server boundary/explain cases fail on CI, so only fall back to it
    # when the download is impossible.
    $null = New-Item -ItemType Directory -Force $SqlServerTools, $SqlServerCache
    $zip = Join-Path $SqlServerCache 'sqlcmd-windows-amd64.zip'
    try {
        Invoke-Download $SqlCmdUrl $zip
        Info "sqlcmd: extracting go-sqlcmd ..."
        Expand-Archive -Path $zip -DestinationPath $SqlServerTools -Force
    } catch {
        Warn "sqlcmd: go-sqlcmd download/extract failed: $($_.Exception.Message)"
    }
    if (-not (Test-Path $exe)) {
        # Some releases nest the binary one directory down.
        $found = Get-ChildItem -Path $SqlServerTools -Filter sqlcmd.exe -Recurse -ErrorAction SilentlyContinue |
                 Select-Object -First 1
        if ($found) {
            Copy-Item $found.FullName $exe -Force
        }
    }
    if (Test-Path $exe) { return $exe }

    $existing = Get-Command sqlcmd.exe -ErrorAction SilentlyContinue
    if ($existing) {
        Warn "sqlcmd: falling back to the legacy client tool $($existing.Source); long UTF-8 batches may fail"
        return $existing.Source
    }
    throw "sqlcmd.exe not found: go-sqlcmd download failed and no system sqlcmd is available"
}

# ── ODBC Driver 18 (system registration required on Windows) ─────────────────
function Test-MsOdbcDriver {
    $drv = Get-OdbcDriver -Name $MsOdbcName -ErrorAction SilentlyContinue |
           Where-Object { $_.Platform -eq '64-bit' }
    return [bool]$drv
}

# Re-register an existing driver whose driver-list value went missing.  The
# driver key and its DLLs survive that damage, so re-add the value instead of
# reinstalling the driver.
function Repair-MsOdbcRegistration {
    if (-not (Test-Admin)) { return $false }
    $key = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\$MsOdbcName"
    if (-not (Test-Path -LiteralPath $key)) { return $false }
    $driver = [string](Get-ItemProperty -Path $key -ErrorAction SilentlyContinue).Driver
    if (-not $driver -or -not (Test-Path -LiteralPath $driver)) { return $false }
    try {
        $driversKey = 'HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers'
        if (-not (Test-Path $driversKey)) { $null = New-Item -Path $driversKey }
        $null = New-ItemProperty -Path $driversKey -Name $MsOdbcName -Value 'Installed' -PropertyType String -Force
    } catch {
        return $false
    }
    return (Test-MsOdbcDriver)
}

function Ensure-MsOdbcDriver {
    if (Test-MsOdbcDriver) {
        Info "ODBC: '$MsOdbcName' (64-bit) is installed."
        return
    }
    if (Repair-MsOdbcRegistration) {
        Info "ODBC: restored the 64-bit registration of '$MsOdbcName'."
        return
    }
    if (-not (Test-Admin)) {
        Err "ODBC: '$MsOdbcName' is not installed and installing it needs administrator rights."
        Err "      Re-run this script from an elevated PowerShell, or install the driver manually: $MsOdbcUrl"
        throw "ODBC Driver 18 for SQL Server is missing"
    }

    $null = New-Item -ItemType Directory -Force $SqlServerCache
    $msi = Join-Path $SqlServerCache 'msodbcsql18.msi'
    Invoke-Download $MsOdbcUrl $msi
    Info "ODBC: installing '$MsOdbcName' ..."
    $log = Join-Path $SqlServerCache 'msodbcsql18-install.log'
    $p = Start-Process msiexec.exe -ArgumentList @(
            '/i', $msi, '/qn', '/norestart',
            'IACCEPTMSODBCSQLLICENSETERMS=YES', '/L*v', $log) -Wait -PassThru
    if ($p.ExitCode -ne 0) {
        throw "msodbcsql18.msi failed (exit=$($p.ExitCode)); see $log"
    }
    if (-not (Test-MsOdbcDriver) -and -not (Repair-MsOdbcRegistration)) {
        throw "ODBC driver installed but '$MsOdbcName' (64-bit) is still not registered"
    }
    Info "ODBC: '$MsOdbcName' installed."
}

# ── engine discovery / installation ──────────────────────────────────────────
function Get-SqlServerInstanceService {
    param([string]$Instance)
    $svc = Get-Service -Name "MSSQL`$$Instance" -ErrorAction SilentlyContinue
    if ($svc) { return $svc }
    if ($Instance -eq 'MSSQLSERVER') {
        return Get-Service -Name 'MSSQLSERVER' -ErrorAction SilentlyContinue
    }
    return $null
}

# The engine runs as a virtual service account (NT SERVICE\MSSQL$<instance>).
# Its default data directory is <FqBase>\sqlserver\data, which this script
# creates before setup runs.  Recreate it if a cleanup removed it and make
# sure the service account may write there; otherwise CREATE DATABASE fails
# with Msg 5133 / "operating system error 5 (access denied)".
function Initialize-SqlServerDataDir {
    param([string]$Instance)
    $dir = Join-Path $FqBase 'sqlserver\data'
    $null = New-Item -ItemType Directory -Force $dir
    $svc = Get-SqlServerInstanceService $Instance
    if (-not $svc) { return }
    $info = Get-CimInstance Win32_Service -Filter "Name='$($svc.Name)'" -ErrorAction SilentlyContinue
    if ($info -and $info.StartName -and $info.StartName -ne 'LocalSystem') {
        & icacls.exe $dir /grant "$($info.StartName):(OI)(CI)M" 2>&1 | Out-Null
    }
}

function Set-SqlServerTcpPort {
    param([string]$Instance, [int]$Port)
    # The instance ID (e.g. MSSQL16.FQSQL) is version-specific; locate it under
    # the Setup key so this keeps working across SQL Server major versions.
    $setupRoot = 'HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server'
    $nameKey = Join-Path $setupRoot "Instance Names\SQL\$Instance"
    # StrictMode makes a missing property access fatal, and Get-ItemProperty
    # returns $null when the instance key does not exist yet; read it defensively.
    $instProps = Get-ItemProperty $nameKey -ErrorAction SilentlyContinue
    $instId = $null
    if ($null -ne $instProps -and
        ($instProps.PSObject.Properties.Name -contains 'SQLInstance')) {
        $instId = $instProps.SQLInstance
    }
    # Fall back to the standard 16.x (SQL Server 2022) instance id.
    if (-not $instId) {
        if ($Instance -eq 'MSSQLSERVER') { $instId = 'MSSQL16.MSSQLSERVER' }
        else { $instId = "MSSQL16.$Instance" }
    }

    $tcp = "$setupRoot\$instId\MSSQLServer\SuperSocketNetLib\Tcp"
    if (-not (Test-Path $tcp)) {
        Warn "SQL Server: TCP key $tcp not found; leaving networking as installed."
        return $false
    }
    $ipAll = "$tcp\IPAll"
    $current = Get-ItemProperty $ipAll -ErrorAction SilentlyContinue
    $curPort = if ($null -ne $current -and
                   ($current.PSObject.Properties.Name -contains 'TcpPort')) { $current.TcpPort } else { $null }
    if ("$curPort" -eq "$Port") {
        Info "SQL Server: TCP already on port $Port (instance $Instance)."
        return $false
    }
    New-ItemProperty -Path $tcp -Name 'Enabled' -Value 1 -PropertyType DWord -Force | Out-Null
    if (-not (Test-Path $ipAll)) { $null = New-Item -Path $ipAll -Force }
    New-ItemProperty -Path $ipAll -Name 'TcpPort' -Value "$Port" -PropertyType String -Force | Out-Null
    New-ItemProperty -Path $ipAll -Name 'TcpDynamicPorts' -Value '' -PropertyType String -Force | Out-Null
    Info "SQL Server: TCP enabled on port $Port (instance $Instance); service restart required."
    return $true
}

# The Express bootstrapper is environment-sensitive: under the CI runner
# service its self-extractor can fail ("The application cannot find one of its
# required files ... not read-only") before writing a single file, even when
# it runs from a short directory outside System32.  The media file itself is a
# plain sequence of single-file CABs, one per media file, in a fixed order
# (see sqlserver_media_manifest.txt), so unpack it ourselves and run the
# extracted SETUP.EXE.  Returns the path of that SETUP.EXE, or $null when the
# media cannot be unpacked (damaged download, unexpected media revision).
function Expand-SqlServerMediaPayload {
    param([string]$Media, [string]$Destination)
    if (-not (Test-Path $Media)) { return $null }
    $manifest = Join-Path $ScriptDir 'sqlserver_media_manifest.txt'
    if (-not (Test-Path $manifest)) {
        Warn "SQL Server: media manifest $manifest is missing"
        return $null
    }
    $paths = @(Get-Content $manifest | Where-Object { $_ -ne '' })
    if ($paths.Count -eq 0) {
        Warn "SQL Server: media manifest $manifest is empty"
        return $null
    }

    # Scan for CAB signatures; each accepted header describes a one-file
    # cabinet (the media packer stores exactly one file per CAB).
    $bytes = [System.IO.File]::ReadAllBytes($Media)
    $length = $bytes.Length
    $cabs = New-Object System.Collections.ArrayList
    $i = -1
    while ($true) {
        $i = [Array]::IndexOf($bytes, [byte]0x4D, ($i + 1))
        if ($i -lt 0) { break }
        if (($i + 36) -ge $length) { continue }
        if (-not ($bytes[$i + 1] -eq 0x53 -and $bytes[$i + 2] -eq 0x43 -and $bytes[$i + 3] -eq 0x46)) { continue }
        $res1 = [BitConverter]::ToUInt32($bytes, $i + 4)
        $size = [BitConverter]::ToUInt32($bytes, $i + 8)
        $res2 = [BitConverter]::ToUInt32($bytes, $i + 12)
        $coffFiles = [BitConverter]::ToUInt32($bytes, $i + 16)
        $res3 = [BitConverter]::ToUInt32($bytes, $i + 20)
        $verMinor = $bytes[$i + 24]
        $cFolders = [BitConverter]::ToUInt16($bytes, $i + 26)
        $cFiles = [BitConverter]::ToUInt16($bytes, $i + 28)
        if ($res1 -eq 0 -and $res2 -eq 0 -and $res3 -eq 0 -and $cFiles -eq 1 -and
            $cFolders -ge 1 -and $cFolders -le 4 -and $verMinor -eq 3 -and
            $coffFiles -ge 36 -and $coffFiles -lt $size -and ($i + $size) -le $length) {
            [void]$cabs.Add([pscustomobject]@{ Offset = $i; Size = $size })
        }
    }
    if ($cabs.Count -ne $paths.Count) {
        Warn ("SQL Server: media has {0} CAB payloads but the manifest lists {1}; the download looks damaged" -f $cabs.Count, $paths.Count)
        return $null
    }

    $expand = Join-Path $env:SystemRoot 'System32\expand.exe'
    $stageIn = Join-Path $Destination '_payload'
    $stageOut = Join-Path $Destination '_unpacked'
    Remove-Item $Destination -Recurse -Force -ErrorAction SilentlyContinue
    for ($k = 0; $k -lt $cabs.Count; $k++) {
        $cab = $cabs[$k]
        Remove-Item $stageIn, $stageOut -Recurse -Force -ErrorAction SilentlyContinue
        $null = New-Item -ItemType Directory -Force $stageIn, $stageOut
        $cabFile = Join-Path $stageIn 'payload.cab'
        $stream = [System.IO.File]::Create($cabFile)
        try { $stream.Write($bytes, $cab.Offset, [int]$cab.Size) } finally { $stream.Dispose() }
        & $expand -F:* $cabFile $stageOut 2>&1 | Out-Null
        $produced = Get-ChildItem $stageOut -File -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $produced) {
            Warn ("SQL Server: CAB {0} of {1} did not expand; the media is damaged" -f $k, $cabs.Count)
            return $null
        }
        $target = Join-Path $Destination $paths[$k]
        $parent = Split-Path $target -Parent
        if ($parent) { $null = New-Item -ItemType Directory -Force $parent }
        Move-Item $produced.FullName $target -Force
    }
    Remove-Item $stageIn, $stageOut -Recurse -Force -ErrorAction SilentlyContinue
    $setup = Join-Path $Destination 'SETUP.EXE'
    if (-not (Test-Path $setup)) {
        Warn "SQL Server: media unpacked but SETUP.EXE is missing"
        return $null
    }
    Info ("SQL Server: media unpacked to {0} ({1} files)" -f $Destination, $paths.Count)
    return $setup
}

function Install-SqlServerEngine {
    param([string]$Instance, [int]$Port)
    if (-not (Test-Admin)) {
        Err "SQL Server: no reachable instance and installing it needs administrator rights."
        Err "      Re-run elevated, or point FQ_SQLSERVER_HOST at an existing SQL Server."
        throw "SQL Server instance is missing"
    }

    $null = New-Item -ItemType Directory -Force $SqlServerCache

    # Version-pinned media: the go.microsoft.com fwlink for the Express
    # bootstrapper is evergreen and now serves SQL Server 2025, which would
    # silently change the engine the suite runs against.  This is the direct
    # SQL Server 2022 Express (16.0) installer, the same version the Linux
    # provisioning pins.  Pair it with the media file name so an unrelated
    # SQLEXPR*.exe in the cache cannot be picked up.
    $setup = Join-Path $SqlServerCache 'SQLEXPR_x64_ENU.exe'
    $setupUrl = Get-EnvOr 'FQ_SQLSERVER_MEDIA_URL' `
        'https://download.microsoft.com/download/3/8/d/38de7036-2433-4207-8eae-06e247e17b25/SQLEXPR_x64_ENU.exe'
    # Prefer an Express media that the image already extracted (the runner was
    # provisioned with SQLEXPR_x64_CHS): running the self-extractor under the
    # runner service fails with "cannot find one of its required files" even
    # with a complete media file and free disk space, while the extracted
    # setup.exe works.
    $extractedSetup = $null
    foreach ($userDir in (Get-ChildItem (Join-Path $env:SystemDrive 'Users') -Directory -ErrorAction SilentlyContinue)) {
        $downloads = Join-Path $userDir.FullName 'Downloads'
        if (-not (Test-Path $downloads)) { continue }
        foreach ($media in (Get-ChildItem $downloads -Directory -Filter 'SQLEXPR*' -ErrorAction SilentlyContinue)) {
            $candidate = Join-Path $media.FullName 'x64\setup\setup.exe'
            if (Test-Path $candidate) { $extractedSetup = $candidate; break }
        }
        if ($extractedSetup) { break }
    }

    $setupCommand = $extractedSetup
    if ($setupCommand) {
        Info "SQL Server: using extracted media $setupCommand"
    } else {
        Invoke-Download $setupUrl $setup
        if (-not (Test-Path $setup)) { throw "SQL Server Express setup executable not found: $setup" }
        # The cache only checks "> 1MB", so an aborted transfer can leave a
        # truncated bootstrapper behind; running it then fails instantly with
        # "The application cannot find one of its required files" and exit 3.
        # The 16.0 Express media is ~250MB.
        $mediaSize = (Get-Item $setup).Length
        if ($mediaSize -lt 100MB) {
            Warn "SQL Server media is only $mediaSize bytes; re-downloading"
            Remove-Item $setup -Force -ErrorAction SilentlyContinue
            Invoke-Download $setupUrl $setup
            $mediaSize = (Get-Item $setup).Length
            if ($mediaSize -lt 100MB) {
                throw "SQL Server Express media download is incomplete ($mediaSize bytes): $setup"
            }
        }
        Info "SQL Server: media $mediaSize bytes"
        $setupCommand = $setup
    }

    Info "SQL Server: installing instance '$Instance' on port $Port (silent, several minutes) ..."
    # Setup logs land in <Program Files>\Microsoft SQL Server\<ver>\Setup
    # Bootstrap\Log automatically; /LOG is NOT a setup parameter (it fails
    # with 0x84B40003 "unrecognized setting").
    $dataDir   = Join-Path $FqBase 'sqlserver\data'
    $backupDir = Join-Path $FqBase 'sqlserver\backup'
    $null = New-Item -ItemType Directory -Force $dataDir, $backupDir
    $setupArgs = @(
        '/Q', '/ACTION=Install', '/FEATURES=SQLENGINE',
        "/INSTANCENAME=$Instance",
        '/SQLSYSADMINACCOUNTS=BUILTIN\Administrators',
        '/SECURITYMODE=SQL', "/SAPWD=$SqlServerPass",
        '/TCPENABLED=1', '/NPENABLED=0',
        '/IACCEPTSQLSERVERLICENSETERMS', '/UpdateEnabled=0',
        # CI runners frequently carry a pending-reboot flag from an earlier
        # update; the rule then aborts the install with exit code 3.  The
        # pending reboot does not affect the engine itself.
        '/SkipRules=RebootRequiredCheck',
        "/SQLBACKUPDIR=$backupDir",
        "/SQLUSERDBDIR=$dataDir",
        "/SQLUSERDBLOGDIR=$dataDir"
    )
    # A previous aborted run can leave the bootstrapper holding the installer
    # state, which makes the next attempt fail within seconds.  Only touch a
    # setup process started from this cache (never sqlservr: a pre-installed
    # instance of the image stays untouched).
    Get-Process -Name 'Setup' -ErrorAction SilentlyContinue | ForEach-Object {
        $path = try { $_.Path } catch { $null }
        if ($path -and $path -like "$SqlServerCache*") {
            Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
        }
    }

    # The Express self-extractor unpacks the media into
    # <working directory>\SQLEXPR_x64_ENU, but it refuses to extract under
    # %SystemRoot%\System32 and then silently falls back to the volume with
    # the most free space (observed: D:\<guid>).  The CI runner runs as the
    # SYSTEM account, so the package cache lives under
    # C:\Windows\system32\config\systemprofile\...; using it as the working
    # directory sends the extraction to D:, the read-only DVD drive, and
    # setup dies within a second with "The application cannot find one of its
    # required files ... not read-only".  Run setup from a short directory
    # outside System32 instead.  TEMP is pointed there as well because child
    # processes (msiexec, the engine) inherit it for their scratch data.
    $extractTemp = "$env:SystemDrive\fq-tmp"
    Remove-Item (Join-Path $extractTemp 'SQLEXPR_x64_ENU') -Recurse -Force -ErrorAction SilentlyContinue
    $null = New-Item -ItemType Directory -Force $extractTemp
    if ($extractTemp -like "$env:SystemRoot\System32*") {
        Warn "SQL Server: extraction directory '$extractTemp' is under System32; setup will refuse it."
    }
    $freeBytes = try { (Get-PSDrive C).Free } catch { $null }
    Info ("SQL Server: extract dir={0} (len={1}); free C:={2}" -f $extractTemp, $extractTemp.Length, $freeBytes)
    $env:TEMP = $extractTemp
    $env:TMP = $extractTemp

    # The Express self-extractor unpacks the media into its working directory
    # (see the System32 note above), so keep that directory outside the deep
    # runner cache.
    $setupOut = Join-Path $SqlServerCache 'setup.out.log'
    $setupErr = Join-Path $SqlServerCache 'setup.err.log'
    $p = Start-Process $setupCommand -ArgumentList $setupArgs -WorkingDirectory $extractTemp -Wait -PassThru `
             -RedirectStandardOutput $setupOut -RedirectStandardError $setupErr
    $mediaRoot = Join-Path $extractTemp 'SQLEXPR_x64_ENU'
    if (($p.ExitCode -ne 0 -and $p.ExitCode -ne 3010) -and $setupCommand -eq $setup) {
        # The bootstrapper's self-extractor refuses to run in some service
        # environments ("... unable to create it in the folder") after writing
        # only a few files, so a partial tree - with SETUP.EXE - may be left
        # behind.  Never trust it: unpack the media ourselves (from an empty
        # directory) and run the extracted SETUP.EXE.
        $partial = @(Get-ChildItem $mediaRoot -Recurse -File -ErrorAction SilentlyContinue).Count
        if ($partial -gt 0) {
            Info "SQL Server: the bootstrapper extracted $partial file(s) before failing; unpacking the media directly ..."
        } else {
            Info "SQL Server: the bootstrapper did not extract the media; unpacking it directly ..."
        }
        $innerSetup = Expand-SqlServerMediaPayload -Media $setup -Destination $mediaRoot
        if ($innerSetup) {
            $setupCommand = $innerSetup
            $p = Start-Process $setupCommand -ArgumentList $setupArgs -WorkingDirectory $mediaRoot -Wait -PassThru `
                     -RedirectStandardOutput $setupOut -RedirectStandardError $setupErr
        }
    }
    if ($p.ExitCode -ne 0 -and $p.ExitCode -ne 3010) {
        # Surface the rule that failed: setup writes Summary.txt under the
        # Setup Bootstrap log and there is no other way to see it remotely.
        foreach ($f in @($setupOut, $setupErr)) {
            if (Test-Path $f) {
                Info "---- $f (tail) ----"
                Get-Content $f -Tail 20 | ForEach-Object { Info "  $_" }
            }
        }
        $sqlRoot = Join-Path $env:ProgramFiles 'Microsoft SQL Server'
        if (Test-Path $sqlRoot) {
            Get-ChildItem $sqlRoot -Recurse -Filter 'Summary.txt' -ErrorAction SilentlyContinue |
                Sort-Object LastWriteTime -Descending | Select-Object -First 1 | ForEach-Object {
                    Info "---- $($_.FullName) (head) ----"
                    Get-Content $_.FullName -TotalCount 40 | ForEach-Object { Info "  $_" }
                }
        }
        Get-ChildItem $env:TEMP -Filter 'SqlSetup*.log' -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTime -Descending | Select-Object -First 2 | ForEach-Object {
                Info "---- $($_.FullName) (tail) ----"
                Get-Content $_.FullName -Tail 30 | ForEach-Object { Info "  $_" }
            }
        Get-PSDrive -PSProvider FileSystem -ErrorAction SilentlyContinue | ForEach-Object {
            Info ("---- drive {0}: free={1}" -f $_.Name, $_.Free)
        }
        throw "SQL Server Express setup failed (exit=$($p.ExitCode)); see the Setup Bootstrap log"
    }
    Set-SqlServerTcpPort $Instance $Port
    $svc = Get-SqlServerInstanceService $Instance
    if ($svc) {
        if ($svc.Status -ne 'Running') { Start-Service $svc.Name }
        Restart-Service $svc.Name -Force -ErrorAction SilentlyContinue
    }
}

function Test-SqlServerLogin {
    param([string]$SqlCmd, [string]$HostName, [int]$Port, [int]$TimeoutSec = 5)
    $out = & $SqlCmd -S "$HostName,$Port" -U $SqlServerUser -P $SqlServerPass `
                     -C -b -l $TimeoutSec -Q 'SELECT 1' 2>&1
    return ($LASTEXITCODE -eq 0)
}

function Reset-SqlServerEnv {
    param([string]$SqlCmd, [string]$HostName, [int]$Port)
    Info "SQL Server @ ${HostName}:${Port}: dropping user databases (clean data state) ..."
    $sql = "DECLARE @sql NVARCHAR(MAX) = N''; " +
           "SELECT @sql += N'ALTER DATABASE [' + name + N'] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [' + name + N'];' " +
           "FROM sys.databases WHERE database_id > 4; EXEC sp_executesql @sql;"
    & $SqlCmd -S "$HostName,$Port" -U $SqlServerUser -P $SqlServerPass `
              -C -b -Q $sql 2>&1 | Out-Null
}

function Ensure-SqlServer {
    param([string]$Ver)
    $port = Get-SqlServerPort $Ver
    Info "SQL Server ${Ver}: host=$SqlServerHost, port=$port"

    $sqlcmd = Ensure-SqlCmd
    Ensure-MsOdbcDriver

    # Quick restart (service-disruption scenarios): bring the instance back
    # without touching its databases, mirroring the Linux quick-restart path.
    if ((Get-EnvOr 'FQ_SQLSERVER_QUICK_RESTART' '0') -eq '1') {
        Info "SQL Server ${Ver}: quick restart (existing data preserved) ..."
        $svc = Get-SqlServerInstanceService $SqlServerInstance
        if ($svc -and $svc.Status -ne 'Running') { Start-Service $svc.Name }
        $deadline = [DateTimeOffset]::UtcNow.AddSeconds(120)
        while ([DateTimeOffset]::UtcNow -lt $deadline) {
            if (Test-SqlServerLogin $sqlcmd $SqlServerHost $port 3) {
                Info "SQL Server ${Ver}: quick restart complete."
                return
            }
            Start-Sleep -Seconds 2
        }
        Warn "SQL Server ${Ver}: quick restart probe failed; falling back to full setup."
    }

    if (-not (Test-LocalHost $SqlServerHost)) {
        # Remote instance: never install/reset anything here; only probe it.
        $reachable = Test-NetConnection -ComputerName $SqlServerHost -Port $port `
                     -InformationLevel Quiet -WarningAction SilentlyContinue
        if ($reachable -and (Test-SqlServerLogin $sqlcmd $SqlServerHost $port)) {
            Info "SQL Server ${Ver}: remote instance at ${SqlServerHost}:${port} is ready (no data reset on a shared host)."
            return
        }
        Err "SQL Server ${Ver}: ${SqlServerHost}:${port} is not reachable or the login failed."
        $script:OverallOk = $false
        return
    }

    # The engine is installed with /SQLUSERDBDIR=<FqBase>\sqlserver\data and
    # the test helpers rely on that default path when they CREATE DATABASE.
    # Recreate it (with the service account's ACL) if a cleanup removed it.
    Initialize-SqlServerDataDir $SqlServerInstance

    if ((Test-PortOpen $port) -and (Test-SqlServerLogin $sqlcmd $SqlServerHost $port)) {
        Info "SQL Server ${Ver}: already running at ${SqlServerHost}:${port}."
        Reset-SqlServerEnv $sqlcmd $SqlServerHost $port
        return
    }

    # Local instance: start it if installed, install it otherwise.
    $svc = Get-SqlServerInstanceService $SqlServerInstance
    if (-not $svc) {
        Install-SqlServerEngine $SqlServerInstance $port
        $svc = Get-SqlServerInstanceService $SqlServerInstance
    } elseif ($svc.Status -ne 'Running') {
        Info "SQL Server ${Ver}: starting service $($svc.Name) ..."
        Start-Service $svc.Name
    }
    # A fresh install may have (re)created the data directory as SYSTEM;
    # grant the instance's virtual service account write access to it.
    Initialize-SqlServerDataDir $SqlServerInstance
    if (Set-SqlServerTcpPort $SqlServerInstance $port) {
        # Registry-only TCP changes take effect on the next service start.
        if ($svc) { Restart-Service $svc.Name -Force -ErrorAction SilentlyContinue }
    }

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(120)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        if ((Test-PortOpen $port) -and (Test-SqlServerLogin $sqlcmd '127.0.0.1' $port 3)) {
            Reset-SqlServerEnv $sqlcmd '127.0.0.1' $port
            Info "SQL Server ${Ver}: ready."
            return
        }
        Start-Sleep -Seconds 2
    }
    Err "SQL Server ${Ver}: instance did not become ready on port $port."
    $script:OverallOk = $false
}

# ==============================================================================
# 9b. Oracle AI Database Free 26ai
# ==============================================================================
#
# Oracle publishes no portable Windows build: the vendor package is a zip that
# runs setup.exe, which installs an Oracle Home, creates the container
# database FREE with the FREEPDB1 pluggable database, and registers the
# database and listener services.  This is the Windows counterpart of the
# vendor container the Linux script starts: one instance on the version's
# port, with the suite's schemas provisioned inside it.
#
# Only the 26ai key exists on this platform - 19c has no Free Windows build -
# but the version axis stays the same because the port and service keys the
# Python side uses (FQ_ORACLE_PORT_26AI, FQ_ORACLE_SERVICE_26AI) apply either
# way.
#
# setup.exe is asynchronous: it returns while msiexec keeps installing, so
# completion is read from the /L*v log ("Installation success or error
# status: 0"), never from setup.exe's exit code.
#
# FQ_ORACLE_FREE_URL may point at a pre-staged copy of the package (http(s),
# file:/// or a plain path).  The pinned SHA256 is always verified before
# setup.exe runs, so a truncated or substituted download is never installed.

$OracleVersions    = Get-EnvList 'FQ_ORACLE_VERSIONS' '26ai'
$OracleHost        = Get-EnvOr 'FQ_ORACLE_HOST' '127.0.0.1'
$OracleAdminUser   = Get-EnvOr 'FQ_ORACLE_ADMIN_USER' 'system'
$OracleAdminPass   = Get-EnvOr 'FQ_ORACLE_ADMIN_PASS' 'Tdengine_123'
$OracleUser        = Get-EnvOr 'FQ_ORACLE_USER' 'taosdata'
$OraclePass        = Get-EnvOr 'FQ_ORACLE_PASS' 'taosdata'
$OraclePoolUser    = Get-EnvOr 'FQ_ORACLE_POOL_TEST_USER' 'fq_pool_test_oracle'
$OraclePoolPass    = Get-EnvOr 'FQ_ORACLE_POOL_TEST_PASS' 'taosdata'
$OracleAltUser     = Get-EnvOr 'FQ_ORACLE_ALT_USER' 'fq_alt_schema'
$OracleAltPass     = Get-EnvOr 'FQ_ORACLE_ALT_PASS' 'taosdata'
$OraclePoolMaxConn = [int](Get-EnvOr 'FQ_POOL_TEST_MAX_CONN' '2')
# Oracle AI Database Free 26ai (23.26.3) for Windows x64.  Pinning both the
# URL and the hash keeps the engine version stable: the download page is
# evergreen and a newer build would silently change the engine the suite
# runs against.
$OracleFreeUrl     = Get-EnvOr 'FQ_ORACLE_FREE_URL' `
    'https://download.oracle.com/otn-pub/otn_software/db-free/oracle-ai-database-free-26ai-23.26.3.windows.x64.zip'
$OracleFreeSha256  = (Get-EnvOr 'FQ_ORACLE_FREE_SHA256' `
    '178d6cfd5a8109ea9d26894a597116db4d95320cd8861d16ca10d4d8c384dab3').ToLowerInvariant()
$OracleCache       = Get-EnvOr 'FQ_ORACLE_PKG_DIR' (Join-Path $FqBase 'oracle\packages')
$OracleDriverName  = 'Oracle in instantclient_23_26'
# Resolved lazily: probing the instance costs a sqlplus login, so the answer
# is reused for the whole run (a stop/start does not invalidate it).
$script:OracleAdminConnect = $null

function Get-OraclePort {
    param([string]$Ver)
    $tag = ($Ver -replace '\.', '').ToUpperInvariant()
    $value = Get-EnvOr "FQ_ORACLE_PORT_$tag" ''
    if ($value) { return [int]$value }
    if ($Ver -eq '26ai') { return 18322 }
    return [int](Get-EnvOr 'FQ_ORACLE_PORT' 18322)
}

function Get-OracleService {
    param([string]$Ver)
    $tag = ($Ver -replace '\.', '').ToUpperInvariant()
    $value = Get-EnvOr "FQ_ORACLE_SERVICE_$tag" ''
    if ($value) { return $value }
    if ($Ver -eq '26ai') { return 'FREEPDB1' }
    return (Get-EnvOr 'FQ_ORACLE_SERVICE' 'FREEPDB1')
}

function Get-OracleBaseDir {
    param([string]$Ver)
    return (Join-Path $FqBase "oracle\$Ver")
}

# Oracle Home of the registered database service.  ImagePath is
# "<home>\bin\ORACLE.EXE <SID>"; a quoted path is accepted too.
function Get-OracleServiceHome {
    $svc = Get-Service -Name 'OracleServiceFREE' -ErrorAction SilentlyContinue
    if ($null -eq $svc) { return $null }
    $key = "HKLM:\SYSTEM\CurrentControlSet\Services\$($svc.Name)"
    $props = Get-ItemProperty -Path $key -ErrorAction SilentlyContinue
    if ($null -eq $props) { return $null }
    if (-not ($props.PSObject.Properties.Name -contains 'ImagePath')) { return $null }
    $image = [string]$props.ImagePath
    if ($image -match '^\s*"([^"]+)"') {
        $exe = $Matches[1]
    } else {
        $exe = ($image -split '\s+')[0]
    }
    if (-not $exe) { return $null }
    $bin = Split-Path -Path $exe -Parent
    if ((Split-Path -Path $bin -Leaf) -ieq 'bin') {
        return (Split-Path -Path $bin -Parent)
    }
    return $bin
}

function Get-OracleHome {
    param([string]$Ver)
    $baseDir = Get-OracleBaseDir $Ver
    # The Free installer puts the home one level below INSTALLDIR; an existing
    # vendor installation may use a different layout, which the service
    # registry entry answers authoritatively.
    foreach ($candidate in @((Join-Path $baseDir 'dbhomeFree'), $baseDir)) {
        if (Test-Path (Join-Path $candidate 'bin\sqlplus.exe')) { return $candidate }
    }
    $svcHome = Get-OracleServiceHome
    if ($svcHome -and (Test-Path (Join-Path $svcHome 'bin\sqlplus.exe'))) { return $svcHome }
    return (Join-Path $baseDir 'dbhomeFree')
}

function Get-OracleListenerServiceName {
    $svc = Get-Service -Name 'OracleOraDB*TNSListener*' -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($null -eq $svc) { return $null }
    return $svc.Name
}

# Passwords and account names are interpolated into DDL that runs inside a
# PL/SQL string literal, so anything that would need quoting there is
# rejected instead of escaped: the suite's values are all simple and a wrong
# one should fail loudly rather than halfway through provisioning.
function Assert-OraclePassword {
    param([string]$Password, [string]$Label)
    if ($Password -notmatch '^[A-Za-z0-9_!#$%^&*()+=.,@?~-]+$') {
        throw "Oracle $Label contains characters the provisioning path cannot quote safely"
    }
}

function Assert-OracleName {
    param([string]$Name)
    if ($Name -notmatch '^[A-Za-z][A-Za-z0-9_$#]*$') {
        throw "invalid Oracle account name: $Name"
    }
}

# Terminate every statement for SQL*Plus and insert "/" after a PL/SQL block.
# Without the terminator SQL*Plus discards the trailing buffer at EXIT and
# still exits 0, so the statement would silently do nothing.  The depth
# counter mirrors the shell helper: BEGIN/DECLARE open a block and a line
# that is exactly "END;" closes one (END IF; / END LOOP; do not).
function ConvertTo-OracleSqlScript {
    param([string]$Sql)
    $normalized = $Sql -replace "`r`n", "`n"
    $lines = @($normalized -split "`n")
    $result = New-Object System.Collections.Generic.List[string]
    $depth = 0
    $declared = $false
    foreach ($line in $lines) {
        $t = $line.Trim()
        $low = $t.ToLowerInvariant()
        if ($depth -eq 0) {
            if ($low -match '^(begin|declare)([ \t(]|$)') {
                $result.Add($line)
                if ($low -match 'end[ \t]*;$') {
                    $result.Add('/')
                } else {
                    $depth = 1
                    $declared = $low.StartsWith('declare')
                }
            } else {
                $result.Add($line)
            }
            continue
        }
        $result.Add($line)
        if ($declared -and ($low -match '^begin([ \t(]|$)')) {
            $declared = $false
            continue
        }
        if ($low -match '^(begin|declare)([ \t(]|$)') {
            $depth++
        } elseif ($low -match '^end[ \t]*;$') {
            $depth--
            if ($depth -eq 0) {
                $result.Add('/')
                $declared = $false
            }
        }
    }
    # A block whose closing END sits on the same line as BEGIN never matches
    # the line-based "END;" rule above, so it would still be buffered when the
    # script ends: SQL*Plus then drops it at EXIT (silent no-op) or, when the
    # caller's stdin stays open, waits for more input forever.  Submit whatever
    # is still buffered with the '/' terminator.
    if ($depth -gt 0) {
        $result.Add('/')
        $depth = 0
    }
    $last = $null
    for ($i = $result.Count - 1; $i -ge 0; $i--) {
        if ($result[$i].Trim() -ne '') { $last = $result[$i].Trim(); break }
    }
    if ($last -and -not ($last.EndsWith(';') -or $last -eq '/')) {
        $result.Add(';')
    }
    return ,$result.ToArray()
}

# Run one SQL batch through sqlplus.  Output is not captured: sqlplus writes
# UTF-8 (NLS_LANG below) straight to the inherited stdout, which keeps the
# bytes intact and lets the Python helpers parse them.  A failing statement
# exits non-zero because of WHENEVER SQLERROR.
function Invoke-OracleSqlplus {
    param(
        [string]$Ver,
        [string]$Connect,
        [string]$Sql,
        [switch]$Quiet
    )
    $oracleHome = Get-OracleHome $Ver
    $sqlplus = Join-Path $oracleHome 'bin\sqlplus.exe'
    if (-not (Test-Path $sqlplus)) {
        Err "Oracle ${Ver}: sqlplus.exe not found under $oracleHome"
        return $false
    }
    $scriptText = ConvertTo-OracleSqlScript $Sql
    $header = "SET DEFINE OFF`r`n" +
              "SET HEADING OFF`r`n" +
              "SET FEEDBACK OFF`r`n" +
              "SET PAGESIZE 0`r`n" +
              "SET LINESIZE 32767`r`n" +
              "SET LONG 100000`r`n" +
              "SET TRIMSPOOL ON`r`n" +
              "SET TAB OFF`r`n" +
              "SET SQLBLANKLINES ON`r`n" +
              "WHENEVER SQLERROR EXIT SQL.SQLCODE`r`n"
    $content = $header + ($scriptText -join "`r`n") + "`r`nEXIT`r`n"
    $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("fq-oracle-" + [guid]::NewGuid().ToString('N') + ".sql")
    [System.IO.File]::WriteAllText($tmp, $content, (New-Object System.Text.UTF8Encoding($false)))
    $savedHome = $env:ORACLE_HOME
    $savedSid  = $env:ORACLE_SID
    $savedNls  = $env:NLS_LANG
    # sqlplus must write to this process's own stdout: invoking it with "&"
    # would make its output part of the function's return value instead of
    # reaching the caller.  A child started with UseShellExecute=false
    # inherits the handle, so its bytes arrive unchanged (no re-encoding).
    $argLine = '-S -L ' + $Connect + ' "@' + $tmp + '"'
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $sqlplus
    $psi.Arguments = $argLine
    $psi.UseShellExecute = $false
    $psi.WorkingDirectory = $oracleHome
    if ($Quiet) { $psi.RedirectStandardOutput = $true }
    $code = 1
    try {
        $env:ORACLE_HOME = $oracleHome
        $env:ORACLE_SID  = 'FREE'
        $env:NLS_LANG    = 'AMERICAN_AMERICA.AL32UTF8'
        $proc = [System.Diagnostics.Process]::Start($psi)
        if ($Quiet) {
            # Drain the pipe before waiting: a full buffer would block the
            # child forever.
            $null = $proc.StandardOutput.ReadToEnd()
        }
        $proc.WaitForExit()
        $code = $proc.ExitCode
    } finally {
        $env:ORACLE_HOME = $savedHome
        $env:ORACLE_SID  = $savedSid
        $env:NLS_LANG    = $savedNls
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
    return ($code -eq 0)
}

function Test-OracleConnect {
    param([string]$Ver, [string]$Connect)
    return (Invoke-OracleSqlplus -Ver $Ver -Connect $Connect -Sql 'SELECT 1 FROM DUAL' -Quiet)
}

# Admin connection.  SYSTEM with the installer password is the documented
# account; OS authentication (bequeath "/ as sysdba") is the fallback for an
# instance whose password was changed.  When only SYSDBA works, SYSTEM's
# password is reset to the configured one so every later admin batch uses the
# same credentials.
function Resolve-OracleAdminConnect {
    param([string]$Ver)
    if ($null -ne $script:OracleAdminConnect) { return $script:OracleAdminConnect }
    $port = Get-OraclePort $Ver
    $service = Get-OracleService $Ver
    $passwordConnect = "$OracleAdminUser/$OracleAdminPass@//127.0.0.1:$port/$service"
    if (Test-OracleConnect $Ver $passwordConnect) {
        $script:OracleAdminConnect = @{ Connect = $passwordConnect; Sysdba = $false }
        return $script:OracleAdminConnect
    }
    if (Test-OracleConnect $Ver '/ as sysdba') {
        $escaped = $OracleAdminPass.Replace('"', '""')
        $reset = "ALTER USER $OracleAdminUser IDENTIFIED BY `"$escaped`" CONTAINER = ALL;"
        if ((Invoke-OracleSqlplus -Ver $Ver -Connect '/ as sysdba' -Sql $reset -Quiet) -and
                (Test-OracleConnect $Ver $passwordConnect)) {
            $script:OracleAdminConnect = @{ Connect = $passwordConnect; Sysdba = $false }
            return $script:OracleAdminConnect
        }
        $script:OracleAdminConnect = @{ Connect = '/ as sysdba'; Sysdba = $true }
        return $script:OracleAdminConnect
    }
    return $null
}

function Invoke-OracleAdminSql {
    param([string]$Ver, [string]$Sql)
    $connect = Resolve-OracleAdminConnect $Ver
    if ($null -eq $connect) {
        Err "Oracle ${Ver}: cannot log in as $OracleAdminUser or SYSDBA"
        return $false
    }
    return (Invoke-OracleSqlplus -Ver $Ver -Connect $connect.Connect -Sql $Sql)
}

# Read the tail of a log written by msiexec.  Windows Installer verbose logs
# are UTF-16LE with a BOM; a plain ANSI/UTF-8 log is handled too.  Only the
# tail is read because the log of a full installation is large and the
# completion marker is written at its end.
function Read-OracleLogTail {
    param([string]$Path, [int]$Bytes = 262144)
    if (-not (Test-Path $Path)) { return '' }
    $fs = [System.IO.File]::Open($Path, 'Open', 'Read', 'ReadWrite')
    try {
        $length = $fs.Length
        if ($length -le 0) { return '' }
        $head = New-Object byte[] 3
        $fs.Position = 0
        $headRead = $fs.Read($head, 0, 3)
        $encoding = [System.Text.Encoding]::Default
        $isUnicode = $false
        if ($headRead -ge 2 -and $head[0] -eq 0xFF -and $head[1] -eq 0xFE) {
            $encoding = [System.Text.Encoding]::Unicode
            $isUnicode = $true
        } elseif ($headRead -ge 3 -and $head[0] -eq 0xEF -and $head[1] -eq 0xBB -and $head[2] -eq 0xBF) {
            $encoding = New-Object System.Text.UTF8Encoding($false)
        }
        $start = [Math]::Max(0, $length - $Bytes)
        if ($isUnicode) { $start = $start - ($start % 2) }
        $fs.Position = $start
        $buffer = New-Object byte[] ($length - $start)
        $total = 0
        while ($total -lt $buffer.Length) {
            $n = $fs.Read($buffer, $total, $buffer.Length - $total)
            if ($n -le 0) { break }
            $total += $n
        }
        return $encoding.GetString($buffer, 0, $total)
    } finally {
        $fs.Dispose()
    }
}

# Return the package zip, verifying its SHA256.  A pre-staged file (CI) and a
# cached download are both accepted; a bad copy is deleted and fetched once
# more before giving up.
function Get-OracleMedia {
    param([string]$Url, [string]$Sha256)
    $name = [System.IO.Path]::GetFileName(($Url -split '\?')[0])
    if (-not $name) {
        $name = 'oracle-ai-database-free-26ai.windows.x64.zip'
    }
    $dest = Join-Path $OracleCache $name
    if (Test-Path $dest) {
        $have = (Get-FileHash -LiteralPath $dest -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($have -eq $Sha256) {
            Info "Oracle: using cached media $dest"
            return $dest
        }
        Warn "Oracle: cached media $dest has SHA256 $have, expected $Sha256; discarding it"
        Remove-Item -LiteralPath $dest -Force
    }
    $null = New-Item -ItemType Directory -Force $OracleCache
    $remote = $true
    if ($Url -match '^file:///') {
        $localPath = ([System.Uri]::new($Url)).LocalPath
        Info "Oracle: copying media from $localPath"
        Copy-Item -LiteralPath $localPath -Destination $dest -Force
        $remote = $false
    } elseif ($Url -notmatch '^[A-Za-z][A-Za-z0-9+.-]*://') {
        Info "Oracle: copying media from $Url"
        Copy-Item -LiteralPath $Url -Destination $dest -Force
        $remote = $false
    } else {
        Invoke-Download $Url $dest
    }
    if (-not (Test-Path $dest)) {
        throw "Oracle media not found after download: $dest"
    }
    $have = (Get-FileHash -LiteralPath $dest -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($have -ne $Sha256 -and $remote) {
        Warn "Oracle: media SHA256 mismatch ($have); downloading once more"
        Remove-Item -LiteralPath $dest -Force
        Invoke-Download $Url $dest
        $have = (Get-FileHash -LiteralPath $dest -Algorithm SHA256).Hash.ToLowerInvariant()
    }
    if ($have -ne $Sha256) {
        Remove-Item -LiteralPath $dest -Force -ErrorAction SilentlyContinue
        throw "Oracle media SHA256 mismatch: $have (expected $Sha256) for $Url"
    }
    Info ("Oracle: media {0} bytes, SHA256 verified" -f (Get-Item $dest).Length)
    return $dest
}

function Expand-OracleMedia {
    param([string]$Zip, [string]$Destination)
    if (Test-Path $Destination) { Remove-Item -LiteralPath $Destination -Recurse -Force }
    $null = New-Item -ItemType Directory -Force $Destination
    $tar = Join-Path $env:SystemRoot 'System32\tar.exe'
    if (Test-Path $tar) {
        & $tar -xf $Zip -C $Destination
        if ($LASTEXITCODE -ne 0) {
            throw "failed to unpack $Zip (tar exit $LASTEXITCODE)"
        }
    } else {
        Expand-Archive -LiteralPath $Zip -DestinationPath $Destination -Force
    }
    return $Destination
}

# Replace "KEY=value" in a response file, appending the key when the template
# does not carry it.  Commented-out defaults ("# KEY=...") are left alone.
function Set-ResponseFileValue {
    param([string[]]$Lines, [string]$Key, [string]$Value)
    $pattern = '^\s*' + [regex]::Escape($Key) + '\s*='
    $out = New-Object System.Collections.Generic.List[string]
    $found = $false
    foreach ($line in $Lines) {
        if ($line -match $pattern) {
            $out.Add("$Key=$Value")
            $found = $true
        } else {
            $out.Add($line)
        }
    }
    if (-not $found) { $out.Add("$Key=$Value") }
    return ,$out.ToArray()
}

function Find-OracleSetupFile {
    param([string]$Media, [string]$Leaf)
    $direct = Join-Path $Media $Leaf
    if (Test-Path $direct) { return $direct }
    $hit = Get-ChildItem -LiteralPath $Media -Recurse -File -Filter $Leaf -ErrorAction SilentlyContinue |
        Sort-Object FullName | Select-Object -First 1
    if ($null -eq $hit) { return $null }
    return $hit.FullName
}

# Silent install of Oracle AI Database Free 26ai.  The package creates the
# container database FREE and the FREEPDB1 service itself; the response file
# only selects the install directory, the port, and the admin password.
function Install-OracleFree {
    param([string]$Ver, [int]$Port, [string]$BaseDir)
    if (-not (Test-Admin)) {
        Err "Oracle ${Ver}: installing Oracle Free needs administrator rights."
        Err "      Re-run elevated, or point FQ_ORACLE_HOST at an existing Oracle instance."
        throw "Oracle Free installation requires administrator rights"
    }
    # The Free home cannot coexist with XE, and the installer aborts late (after
    # several minutes) when it finds one; refuse immediately instead.
    if ($null -ne (Get-Service -Name 'OracleServiceXE' -ErrorAction SilentlyContinue)) {
        throw "Oracle Database XE is installed; Oracle AI Database Free cannot coexist with XE"
    }
    # A space in INSTALLDIR breaks the argument protocol of the vendor
    # launcher (it splits the path when it forwards it to msiexec).
    if ($BaseDir -match '\s') {
        throw "Oracle's installer does not accept spaces in INSTALLDIR ($BaseDir); set FQ_BASE_DIR to a space-free path"
    }
    # ORACLE_HOME / TNS_ADMIN make the installer's prerequisite check see the
    # wrong home; remove them from this process only.
    foreach ($name in @('ORACLE_HOME', 'TNS_ADMIN')) {
        if ([System.Environment]::GetEnvironmentVariable($name)) {
            Warn "Oracle ${Ver}: clearing $name for the installer"
            [System.Environment]::SetEnvironmentVariable($name, $null, 'Process')
        }
    }

    $zip = Get-OracleMedia $OracleFreeUrl $OracleFreeSha256
    $media = Join-Path $BaseDir 'media'
    Info "Oracle ${Ver}: unpacking $zip ..."
    $null = Expand-OracleMedia $zip $media

    $setup = Find-OracleSetupFile $media 'setup.exe'
    if (-not $setup) { throw "setup.exe not found under $media" }
    $rsp = Find-OracleSetupFile $media 'FREEInstall.rsp'
    if (-not $rsp) { $rsp = Find-OracleSetupFile $media '*.rsp' }
    if (-not $rsp) { throw "the Oracle response file (*.rsp) not found under $media" }

    # INSTALLDIR must end with a backslash; the vendor template documents it
    # and the launcher concatenates it with the home directory name.
    $installDir = $BaseDir.TrimEnd('\') + '\'
    $lines = [System.IO.File]::ReadAllLines($rsp, [System.Text.Encoding]::UTF8)
    $lines = Set-ResponseFileValue $lines 'INSTALLDIR' $installDir
    $lines = Set-ResponseFileValue $lines 'PASSWORD' $OracleAdminPass
    $lines = Set-ResponseFileValue $lines 'LISTENER_PORT' "$Port"
    $lines = Set-ResponseFileValue $lines 'CHAR_SET' 'AL32UTF8'
    $lines = Set-ResponseFileValue $lines 'DB_DOMAIN' ''
    $lines = Set-ResponseFileValue $lines 'MODIFY_DIRECTORY_PERMISSIONS' 'TRUE'
    # Creating the database is the whole point of the package; an image that
    # ships SOFTWARE_ONLY=TRUE would only install the binaries.
    $lines = Set-ResponseFileValue $lines 'SOFTWARE_ONLY' 'FALSE'
    [System.IO.File]::WriteAllLines($rsp, $lines, (New-Object System.Text.UTF8Encoding($false)))

    $log = Join-Path $BaseDir 'setup.log'
    Remove-Item -LiteralPath $log -Force -ErrorAction SilentlyContinue
    Info "Oracle ${Ver}: running setup.exe silently (this takes 20-60 minutes) ..."
    $setupArgs = '/s /v"RSP_FILE=' + $rsp + '" /v"/L*v ' + $log + '" /v"/qn"'
    $started = Get-Date
    $proc = Start-Process -FilePath $setup -WorkingDirectory $media `
        -ArgumentList $setupArgs -PassThru

    # setup.exe is only a launcher: poll the MSI log for the completion
    # marker, which is written when the database creation has finished too.
    $deadline = (Get-Date).AddMinutes(90)
    $status = -1
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 10
        if (Test-Path $log) {
            $tail = Read-OracleLogTail $log
            if ($tail -match 'Installation success or error status:\s*(-?\d+)') {
                $status = [int]$Matches[1]
                break
            }
        }
        if ($proc.HasExited -and $proc.ExitCode -ne 0) {
            # The launcher only fails early (bad arguments, missing media);
            # the MSI state is still the authority, so keep waiting for the
            # log until the deadline unless it is clearly absent.
            if (-not (Test-Path $log)) {
                throw "Oracle setup.exe exited with $($proc.ExitCode) and wrote no log at $log"
            }
        }
    }
    # The response file carries the admin password in clear text.
    Remove-Item -LiteralPath $rsp -Force -ErrorAction SilentlyContinue

    if ($status -gt 0) {
        $tail = Read-OracleLogTail $log 65536
        throw "Oracle setup failed with status $status; see $log`n$tail"
    }
    if ($status -lt 0) {
        throw "Oracle setup did not report completion within 90 minutes; see $log"
    }
    $elapsed = ((Get-Date) - $started).TotalMinutes
    Info ("Oracle {0}: installation finished in {1:N1} minutes" -f $Ver, $elapsed)

    # The success marker can precede the service registration by a few
    # seconds.
    $svcDeadline = (Get-Date).AddSeconds(300)
    while ((Get-Date) -lt $svcDeadline) {
        if ($null -ne (Get-Service -Name 'OracleServiceFREE' -ErrorAction SilentlyContinue)) { break }
        Start-Sleep -Seconds 5
    }
    if ($null -eq (Get-Service -Name 'OracleServiceFREE' -ErrorAction SilentlyContinue)) {
        throw "Oracle setup reported success but OracleServiceFREE is not registered; see $log"
    }
    # The unpacked media is only needed during setup and takes several GB.
    Remove-Item -LiteralPath $media -Recurse -Force -ErrorAction SilentlyContinue
}

function Start-OracleServices {
    param([string]$Ver)
    $dbSvc = Get-Service -Name 'OracleServiceFREE' -ErrorAction SilentlyContinue
    if ($null -eq $dbSvc) {
        throw "OracleServiceFREE is not registered"
    }
    if ($dbSvc.Status -ne 'Running') {
        Info "Oracle ${Ver}: starting $($dbSvc.Name) ..."
        Start-Service -Name $dbSvc.Name
    }
    $lsnName = Get-OracleListenerServiceName
    if ($lsnName) {
        $lsn = Get-Service -Name $lsnName -ErrorAction SilentlyContinue
        if ($lsn -and $lsn.Status -ne 'Running') {
            Info "Oracle ${Ver}: starting $lsnName ..."
            Start-Service -Name $lsnName
        }
    } else {
        Warn "Oracle ${Ver}: no TNS listener service found; local connections may still work"
    }
}

function Wait-OracleReady {
    param([string]$Ver, [int]$MaxSec = 900)
    $port = Get-OraclePort $Ver
    $deadline = (Get-Date).AddSeconds($MaxSec)
    while ((Get-Date) -lt $deadline) {
        if (Test-PortOpen $port) {
            if ($null -ne (Resolve-OracleAdminConnect $Ver)) { return $true }
        }
        Start-Sleep -Seconds 5
    }
    return $false
}

# Directory holding sqora32.dll plus the Instant Client base libraries.  The
# provider scores a registration that points into taosd's own directory
# highest, so the build tree (or the package's driver\oracle) wins over a
# database home's own driver.
function Get-OracleDriverDir {
    $candidates = New-Object System.Collections.Generic.List[string]
    $explicit = Get-EnvOr 'FQ_ORACLE_DRIVER_DIR' ''
    if ($explicit) { $candidates.Add($explicit) }
    if ($env:TAOS_BIN_PATH) {
        $candidates.Add($env:TAOS_BIN_PATH)
        $parent = Split-Path -Path $env:TAOS_BIN_PATH -Parent
        if ($parent) { $candidates.Add((Join-Path $parent 'driver\oracle')) }
    }
    # Source tree: <repo>\debug*\build\bin; package tree: any ancestor that
    # carries driver\oracle.
    $dir = $ScriptDir
    while ($dir) {
        $candidates.Add((Join-Path $dir 'driver\oracle'))
        $candidates.Add((Join-Path $dir 'bin'))
        if (Test-Path (Join-Path $dir '.git')) {
            foreach ($buildDir in (Get-ChildItem -Path $dir -Directory -Filter 'debug*' -ErrorAction SilentlyContinue)) {
                $candidates.Add((Join-Path $buildDir.FullName 'build\bin'))
            }
        }
        $parent = Split-Path -Path $dir -Parent
        if (-not $parent -or $parent -eq $dir) { break }
        $dir = $parent
    }
    $candidates.Add('C:\TDengine')
    $candidates.Add('C:\TDengine\driver\oracle')
    foreach ($candidate in $candidates) {
        if (-not $candidate) { continue }
        if (-not (Test-Path (Join-Path $candidate 'sqora32.dll'))) { continue }
        $base = Get-ChildItem -Path $candidate -Filter 'oraociei*.dll' -ErrorAction SilentlyContinue
        if ($null -eq $base) { continue }
        return (Resolve-Path -Path $candidate).Path
    }
    return $null
}

# Register the Instant Client ODBC driver in the 64-bit ODBCINST.INI view.
# Windows has no per-application ODBC registration and the Driver keyword
# cannot carry a DLL path, so the provider resolves the driver through this
# entry (see extConnectorOracle.c).
function Register-OracleOdbcDriver {
    param([string]$Ver)
    $driverDir = Get-OracleDriverDir
    if (-not $driverDir) {
        Err "Oracle ${Ver}: no Instant Client (sqora32.dll + oraociei*.dll) found."
        Err "      Build with BUILD_WITH_ODBC=ON so the driver is staged into build\bin,"
        Err "      or set FQ_ORACLE_DRIVER_DIR to the directory that holds it."
        return $false
    }
    $driverDll = Join-Path $driverDir 'sqora32.dll'
    $setupDll = Join-Path $driverDir 'sqoras32.dll'
    try {
        $key = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\$OracleDriverName"
        if (-not (Test-Path $key)) { $null = New-Item -Path $key }
        $null = New-ItemProperty -Path $key -Name 'Driver' -Value $driverDll -PropertyType String -Force
        if (Test-Path $setupDll) {
            $null = New-ItemProperty -Path $key -Name 'Setup' -Value $setupDll -PropertyType String -Force
        }
        $driversKey = 'HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers'
        # New-Item -Force on an existing registry key drops all of its values,
        # which would unregister every other ODBC driver on the machine.
        if (-not (Test-Path $driversKey)) { $null = New-Item -Path $driversKey }
        $null = New-ItemProperty -Path $driversKey -Name $OracleDriverName -Value 'Installed' -PropertyType String -Force
    } catch {
        Err "Oracle ${Ver}: registering the ODBC driver failed: $_"
        return $false
    }
    Info "Oracle ${Ver}: registered ODBC driver '$OracleDriverName' -> $driverDll"
    return $true
}

# Create the suite's accounts (schema == user in Oracle).  The pool account
# gets its own profile so the pool-exhaustion tests can cap its sessions
# without touching the other schemas.
function Invoke-OracleProvision {
    param([string]$Ver)
    $profileSql = @"
DECLARE
  n NUMBER;
BEGIN
  SELECT COUNT(*) INTO n FROM dba_profiles
   WHERE profile = 'FQ_POOL_TEST' AND resource_name = 'SESSIONS_PER_USER';
  IF n = 0 THEN
    EXECUTE IMMEDIATE 'CREATE PROFILE FQ_POOL_TEST LIMIT SESSIONS_PER_USER $OraclePoolMaxConn';
  ELSE
    EXECUTE IMMEDIATE 'ALTER PROFILE FQ_POOL_TEST LIMIT SESSIONS_PER_USER $OraclePoolMaxConn';
  END IF;
END;
/
"@
    if (-not (Invoke-OracleAdminSql $Ver $profileSql)) {
        Err "Oracle ${Ver}: creating profile FQ_POOL_TEST failed"
        return $false
    }
    $accounts = @(
        @{ User = $OracleUser; Pass = $OraclePass; Profile = 'DEFAULT' },
        @{ User = $OraclePoolUser; Pass = $OraclePoolPass; Profile = 'FQ_POOL_TEST' },
        @{ User = $OracleAltUser; Pass = $OracleAltPass; Profile = 'DEFAULT' }
    )
    foreach ($account in $accounts) {
        $name = [string]$account.User
        $pass = [string]$account.Pass
        $profile = [string]$account.Profile
        Assert-OracleName $name
        Assert-OraclePassword $pass "password for $name"
        $sql = @"
DECLARE
  n NUMBER;
BEGIN
  SELECT COUNT(*) INTO n FROM dba_users WHERE username = UPPER('$name');
  IF n = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER $name IDENTIFIED BY "$pass" PROFILE $profile';
  ELSE
    EXECUTE IMMEDIATE 'ALTER USER $name IDENTIFIED BY "$pass" PROFILE $profile';
  END IF;
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE SYNONYM, UNLIMITED TABLESPACE TO $name';
END;
/
"@
        if (-not (Invoke-OracleAdminSql $Ver $sql)) {
            Err "Oracle ${Ver}: provisioning account $name failed"
            return $false
        }
    }
    return $true
}

# Drop every object owned by the three accounts so each test file starts from
# an empty schema, matching the other engines' clean-data reset.
function Invoke-OracleDropUserObjects {
    param([string]$Ver)
    foreach ($owner in @($OracleUser, $OraclePoolUser, $OracleAltUser)) {
        Assert-OracleName $owner
        $sql = @"
BEGIN
  FOR o IN (SELECT object_name, object_type FROM dba_objects
             WHERE owner = UPPER('$owner')
               AND object_type IN ('TABLE','VIEW','MATERIALIZED VIEW','SEQUENCE','SYNONYM','TYPE','PROCEDURE','FUNCTION')) LOOP
    BEGIN
      IF o.object_type = 'TABLE' THEN
        EXECUTE IMMEDIATE 'DROP TABLE "$owner"."' || o.object_name || '" CASCADE CONSTRAINTS PURGE';
      ELSE
        EXECUTE IMMEDIATE 'DROP ' || o.object_type || ' "$owner"."' || o.object_name || '"';
      END IF;
    EXCEPTION
      WHEN OTHERS THEN NULL;
    END;
  END LOOP;
END;
/
"@
        if (-not (Invoke-OracleAdminSql $Ver $sql)) {
            Warn "Oracle ${Ver}: could not drop objects owned by $owner"
        }
    }
}

function Ensure-Oracle {
    param([string]$Ver)
    if ($Ver -ne '26ai') {
        Err "Oracle ${Ver}: only the 26ai Free instance exists on Windows"
        $script:OverallOk = $false
        return
    }
    $port = Get-OraclePort $Ver
    $service = Get-OracleService $Ver
    $baseDir = Get-OracleBaseDir $Ver
    Info "Oracle ${Ver}: host=$OracleHost, port=$port, service=$service"

    if (-not (Test-LocalHost $OracleHost)) {
        if (Test-PortOpen $port) {
            Info "Oracle ${Ver}: remote instance at ${OracleHost}:${port} is reachable (no install or reset on a shared host)."
            return
        }
        Err "Oracle ${Ver}: ${OracleHost}:${port} is not reachable."
        $script:OverallOk = $false
        return
    }

    $quick = (Get-EnvOr 'FQ_ORACLE_QUICK_RESTART' '0') -eq '1'
    $svc = Get-Service -Name 'OracleServiceFREE' -ErrorAction SilentlyContinue
    $oracleHome = Get-OracleHome $Ver
    $installed = ($null -ne $svc) -and (Test-Path (Join-Path $oracleHome 'bin\sqlplus.exe'))
    if (-not $installed) {
        if ($null -ne $svc) {
            Err "Oracle ${Ver}: OracleServiceFREE is registered but $oracleHome\bin\sqlplus.exe is missing."
            Err "      A partially removed installation cannot be reused; remove the service and reinstall."
            $script:OverallOk = $false
            return
        }
        if ($quick) {
            Err "Oracle ${Ver}: quick restart requested but Oracle Free is not installed."
            $script:OverallOk = $false
            return
        }
        try {
            Install-OracleFree $Ver $port $baseDir
        } catch {
            Err "Oracle ${Ver}: installation failed: $_"
            $script:OverallOk = $false
            return
        }
        $oracleHome = Get-OracleHome $Ver
    }

    try {
        Start-OracleServices $Ver
    } catch {
        Err "Oracle ${Ver}: starting the services failed: $_"
        $script:OverallOk = $false
        return
    }
    if (-not (Wait-OracleReady $Ver 900)) {
        Err "Oracle ${Ver}: the instance did not become ready on port $port (log: $(Join-Path $baseDir 'setup.log'))."
        $script:OverallOk = $false
        return
    }
    if (-not (Register-OracleOdbcDriver $Ver)) {
        $script:OverallOk = $false
        return
    }

    if ($quick) {
        Info "Oracle ${Ver}: quick restart complete (data preserved)."
        return
    }
    if (-not (Invoke-OracleProvision $Ver)) {
        $script:OverallOk = $false
        return
    }
    Invoke-OracleDropUserObjects $Ver
    Info "Oracle ${Ver}: ready."
}

function Stop-OracleInstance {
    param([string]$Ver)
    $svc = Get-Service -Name 'OracleServiceFREE' -ErrorAction SilentlyContinue
    if ($svc -and $svc.Status -ne 'Stopped') {
        Info "Oracle ${Ver}: stopping $($svc.Name) ..."
        Stop-Service -Name $svc.Name -Force -ErrorAction SilentlyContinue
    }
    $lsnName = Get-OracleListenerServiceName
    if ($lsnName) {
        $lsn = Get-Service -Name $lsnName -ErrorAction SilentlyContinue
        if ($lsn -and $lsn.Status -ne 'Stopped') {
            Stop-Service -Name $lsnName -Force -ErrorAction SilentlyContinue
        }
    }
    $port = Get-OraclePort $Ver
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        if (-not (Test-PortOpen $port)) { return }
        Start-Sleep -Seconds 1
    }
    Warn "Oracle ${Ver}: port $port still accepts connections after the stop request"
}

# TerminateProcess is the Windows equivalent of SIGKILL.  Only processes that
# belong to this instance's home are signalled, so an unrelated Oracle
# installation on the same host is never touched.
function Kill-OracleInstance {
    param([string]$Ver)
    $oracleHome = Get-OracleHome $Ver
    $oracleHomePrefix = $oracleHome.TrimEnd('\') + '\'
    foreach ($name in @('oracle', 'tnslsnr')) {
        Get-Process -Name $name -ErrorAction SilentlyContinue | ForEach-Object {
            $path = $null
            try { $path = $_.Path } catch { $path = $null }
            if ($path -and $path.StartsWith($oracleHomePrefix, 'OrdinalIgnoreCase')) {
                Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
            }
        }
    }
    # A killed database process leaves the SCM entry to transition on its own;
    # refresh the service state before the port probe.
    $port = Get-OraclePort $Ver
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        if (-not (Test-PortOpen $port)) { return }
        Start-Sleep -Seconds 1
    }
    Warn "Oracle ${Ver}: port $port still accepts connections after the kill"
}

# --oracle-exec: run the batch file named by FQ_ORACLE_EXEC_SQL_FILE as
# FQ_ORACLE_EXEC_USER/FQ_ORACLE_EXEC_PASS.  Credentials travel through the
# environment because a password containing $, ; or & would be re-parsed by
# PowerShell if it were a command-line argument.
function Invoke-OracleExecAction {
    param([string]$Ver)
    $user = Get-EnvOr 'FQ_ORACLE_EXEC_USER' (Get-EnvOr 'FQ_ORACLE_USER' 'taosdata')
    $pass = Get-EnvOr 'FQ_ORACLE_EXEC_PASS' (Get-EnvOr 'FQ_ORACLE_PASS' 'taosdata')
    $file = Get-EnvOr 'FQ_ORACLE_EXEC_SQL_FILE' ''
    if (-not $file) {
        Err "FQ_ORACLE_EXEC_SQL_FILE is not set"
        return $false
    }
    if (-not (Test-Path $file)) {
        Err "Oracle ${Ver}: SQL file not found: $file"
        return $false
    }
    $hostName = $OracleHost
    if ($hostName -eq 'localhost' -or $hostName -eq '.') { $hostName = '127.0.0.1' }
    $connect = "$user/$pass@//${hostName}:$(Get-OraclePort $Ver)/$(Get-OracleService $Ver)"
    $sql = [System.IO.File]::ReadAllText($file, [System.Text.Encoding]::UTF8)
    return (Invoke-OracleSqlplus -Ver $Ver -Connect $connect -Sql $sql)
}

function Invoke-OracleAdminExecAction {
    param([string]$Ver)
    $file = Get-EnvOr 'FQ_ORACLE_ADMIN_SQL_FILE' ''
    if (-not $file) {
        Err "FQ_ORACLE_ADMIN_SQL_FILE is not set"
        return $false
    }
    if (-not (Test-Path $file)) {
        Err "Oracle ${Ver}: admin SQL file not found: $file"
        return $false
    }
    $sql = [System.IO.File]::ReadAllText($file, [System.Text.Encoding]::UTF8)
    return (Invoke-OracleAdminSql $Ver $sql)
}

# ══════════════════════════════════════════════════════════════════════════════
# 10. Main
# ══════════════════════════════════════════════════════════════════════════════

# --- Command-line actions -----------------------------------------------------
#   ensure_ext_env.ps1 --oracle-exec <version>
#   ensure_ext_env.ps1 --oracle-admin-exec <version>
#   ensure_ext_env.ps1 --oracle-stop <version>
#   ensure_ext_env.ps1 --oracle-kill <version>
# The action flags are used by the Python fixtures for the Oracle disruption
# and SQL-helper paths.  Payload (credentials, SQL batch) arrives through
# FQ_ORACLE_EXEC_* / FQ_ORACLE_ADMIN_SQL_FILE so no value is ever re-parsed
# from the command line.  These paths print nothing but sqlplus' own output.
# The script is normally started without any argument, and @($null) still
# yields a one-element array on Windows PowerShell, which would then be
# taken for an unknown action.  Drop null and empty elements.
$CliArgs = @($RemainingArgs | Where-Object { $null -ne $_ -and '' -ne $_ })
if ($CliArgs.Count -gt 0) {
    $script:SuppressConsoleLog = $true
    $action = [string]$CliArgs[0]
    $actionVer = ''
    if ($CliArgs.Count -ge 2) { $actionVer = [string]$CliArgs[1] }
    switch ($action) {
        '--oracle-exec' {
            if (-not $actionVer) { Err "usage: ensure_ext_env.ps1 --oracle-exec <version>"; exit 2 }
            if (Invoke-OracleExecAction $actionVer) { exit 0 } else { exit 1 }
        }
        '--oracle-admin-exec' {
            if (-not $actionVer) { Err "usage: ensure_ext_env.ps1 --oracle-admin-exec <version>"; exit 2 }
            if (Invoke-OracleAdminExecAction $actionVer) { exit 0 } else { exit 1 }
        }
        '--oracle-stop' {
            if (-not $actionVer) { Err "usage: ensure_ext_env.ps1 --oracle-stop <version>"; exit 2 }
            Stop-OracleInstance $actionVer
            exit 0
        }
        '--oracle-kill' {
            if (-not $actionVer) { Err "usage: ensure_ext_env.ps1 --oracle-kill <version>"; exit 2 }
            Kill-OracleInstance $actionVer
            exit 0
        }
        default {
            Err "unknown argument: $action"
            exit 2
        }
    }
}

# FQ_SERVICES_TO_RESET limits the run to the named engines, matching the
# shell script: a mid-test restart sets it to one service while leaving the
# version lists at their defaults.
$ServicesToReset = Get-EnvOr 'FQ_SERVICES_TO_RESET' ''
function Test-EngineRequested {
    param([string]$Name)
    if (-not $ServicesToReset) { return $true }
    $names = @($ServicesToReset -split '[, ]+' | Where-Object { $_ })
    return ($names -contains $Name)
}

Log "========================================================"
Log "FederatedQuery external environment setup (Windows)"
Log "  Base       : $FqBase"
Log "  MySQL      : $($MysqlVersions -join ', ')"
Log "  PG         : $($PgVersions -join ', ')"
Log "  InfluxDB   : $($InfluxVersions -join ', ')"
Log "  SQL Server : $($SqlServerVersions -join ', ')"
Log "  Oracle     : $($OracleVersions -join ', ')"
Log "========================================================"

$null = New-Item -ItemType Directory -Force $FqBase

if (Test-EngineRequested 'mysql')     { foreach ($ver in $MysqlVersions)  { Ensure-Mysql  $ver } }
if (Test-EngineRequested 'pg')        { foreach ($ver in $PgVersions)     { Ensure-Pg     $ver } }
if (Test-EngineRequested 'influx')    { foreach ($ver in $InfluxVersions) { Ensure-Influx $ver } }
if (Test-EngineRequested 'sqlserver') { foreach ($ver in $SqlServerVersions) { Ensure-SqlServer $ver } }
if (Test-EngineRequested 'oracle')    { foreach ($ver in $OracleVersions) { Ensure-Oracle $ver } }

if (-not $OverallOk) {
    Err "One or more engines failed to start. See messages above."
    exit 1
}
Log "All engines ready."
exit 0
