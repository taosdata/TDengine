<#
.SYNOPSIS
    ensure_ext_env.ps1  ─ FederatedQuery integration-test external-source setup (Windows)

.DESCRIPTION
    Windows counterpart of ensure_ext_env.sh.
    Downloads, installs, and starts MySQL, PostgreSQL and InfluxDB on
    non-default ports so they run alongside any locally installed instances.
    All operations are idempotent: re-running the script while services
    are already up resets test databases instead of restarting them.

    REQUIREMENTS
      Windows 10 21H2 / Windows Server 2022 or later (64-bit only)
      PowerShell 5.1 or PowerShell 7+
      Internet access (or mirrors set via FQ_*_MIRROR env vars)
      Administrator rights  – only needed if WinSW service registration fails
                               and --user workaround is required for MySQL.

    WHAT IT DOES (idempotent per-engine-version)
      1. Port open?         → reset test DBs (already running)
      2. Installed, stopped → start; if still failing re-init data dir
      3. Not installed?     → download → install → init → start → configure

    ENVIRONMENT VARIABLES (all optional)
      FQ_BASE_DIR            install/data root   default %LOCALAPPDATA%\taostest\fq
      FQ_MYSQL_VERSIONS      comma list          default "8.0"
      FQ_PG_VERSIONS         comma list          default "16"
      FQ_INFLUX_VERSIONS     comma list          default "3.0"
      FQ_MYSQL_MIRROR        base URL for MySQL ZIP downloads
      FQ_PG_MIRROR           base URL for PG ZIP downloads
      FQ_INFLUX_MIRROR       base URL for InfluxDB releases
      FQ_MYSQL_TARBALL_<VV>  full URL override (VV = 57/80/84)
      FQ_PG_TARBALL_<VV>     full URL override (VV = 14/15/16/17)
      FQ_INFLUX_TARBALL_<VV> full URL override (VV = 30/35)
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
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ── 0. Bootstrap ─────────────────────────────────────────────────────────────

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Definition
$FqBase     = if ($env:FQ_BASE_DIR) { $env:FQ_BASE_DIR }
              else { Join-Path $env:LOCALAPPDATA 'taostest\fq' }
$CertSrc    = if ($env:FQ_CERT_DIR) { $env:FQ_CERT_DIR }
              else { Join-Path $ScriptDir 'certs' }

$MysqlVersions  = @(($env:FQ_MYSQL_VERSIONS  ?? '8.0')  -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
$PgVersions     = @(($env:FQ_PG_VERSIONS     ?? '16')   -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
$InfluxVersions = @(($env:FQ_INFLUX_VERSIONS ?? '3.0')  -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ })

$MysqlUser  = $env:FQ_MYSQL_USER ?? 'root'
$MysqlPass  = $env:FQ_MYSQL_PASS ?? 'taosdata'
$PgUser     = $env:FQ_PG_USER    ?? 'postgres'
$PgPass     = $env:FQ_PG_PASS    ?? 'taosdata'

$OverallOk  = $true

# ── 1. Logging ────────────────────────────────────────────────────────────────

function Log   { param($Msg) Write-Host "[fq-env] $Msg" }
function Info  { param($Msg) Write-Host "[fq-env] INFO  $Msg" }
function Warn  { param($Msg) Write-Warning "[fq-env] WARN  $Msg" }
function Err   { param($Msg) Write-Error   "[fq-env] ERROR $Msg" -ErrorAction Continue }

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

    # Build environment for the child process
    $procInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $procInfo.FileName               = $Exe
    $procInfo.Arguments              = ($ArgList | ForEach-Object { `
        if ($_ -match '\s') { '"' + $_ + '"' } else { $_ } }) -join ' '
    $procInfo.UseShellExecute        = $false
    $procInfo.RedirectStandardOutput = $true
    $procInfo.RedirectStandardError  = $true
    $procInfo.CreateNoWindow         = $true

    foreach ($ep in $EnvPairs) {
        $kv = $ep -split '=', 2
        if ($kv.Count -eq 2) {
            $procInfo.EnvironmentVariables[$kv[0]] = $kv[1]
        }
    }

    $proc = [System.Diagnostics.Process]::new()
    $proc.StartInfo = $procInfo

    # Async log capture to avoid deadlocks on full pipe buffers
    $logStream = [System.IO.StreamWriter]::new($LogFile, $true)
    $logStream.AutoFlush = $true
    $proc.add_OutputDataReceived({ param($s, $e); if ($e.Data) { $logStream.WriteLine($e.Data) } })
    $proc.add_ErrorDataReceived(  { param($s, $e); if ($e.Data) { $logStream.WriteLine($e.Data) } })

    $null = $proc.Start()
    $proc.BeginOutputReadLine()
    $proc.BeginErrorReadLine()

    Set-Content -Path $PidFile -Value $proc.Id
    return $proc
}

function Stop-ByPidFile {
    param([string]$PidFile, [int]$WaitSec = 10)
    if (-not (Test-Path $PidFile)) { return }
    $pid = [int](Get-Content $PidFile -Raw).Trim()
    $proc = Get-Process -Id $pid -ErrorAction SilentlyContinue
    if ($proc) {
        $proc.Kill()
        $proc.WaitForExit($WaitSec * 1000) | Out-Null
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

# ── 5. Port → version mapping ─────────────────────────────────────────────────

function Get-MysqlPort {
    param([string]$Ver)
    switch ($Ver) {
        '5.7' { return [int]($env:FQ_MYSQL_PORT_57 ?? 13305) }
        '8.0' { return [int]($env:FQ_MYSQL_PORT_80 ?? 13306) }
        '8.4' { return [int]($env:FQ_MYSQL_PORT_84 ?? 13307) }
        default { throw "Unknown MySQL version: $Ver" }
    }
}

function Get-PgPort {
    param([string]$Ver)
    switch ($Ver) {
        '14' { return [int]($env:FQ_PG_PORT_14 ?? 15433) }
        '15' { return [int]($env:FQ_PG_PORT_15 ?? 15435) }
        '16' { return [int]($env:FQ_PG_PORT_16 ?? 15434) }
        '17' { return [int]($env:FQ_PG_PORT_17 ?? 15436) }
        default { throw "Unknown PG version: $Ver" }
    }
}

function Get-InfluxPort {
    param([string]$Ver)
    switch ($Ver -replace '\.', '') {
        '30' { return [int]($env:FQ_INFLUX_PORT_30 ?? 18086) }
        '35' { return [int]($env:FQ_INFLUX_PORT_35 ?? 18087) }
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

    $mirror = $env:FQ_MYSQL_MIRROR ?? 'https://dev.mysql.com/get/Downloads'

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

    # Write minimal my.ini
    $myIni = Join-Path $Base 'my.ini'
    $iniContent = @"
[mysqld]
basedir=$Base
datadir=$dataDir
port=$Port
socket=
bind-address=127.0.0.1
log-error=$logDir\error.log
pid-file=$Base\run\mysqld.pid
max_connections=200
character-set-server=utf8mb4
collation-server=utf8mb4_unicode_ci
"@
    Set-Content -Path $myIni -Value $iniContent -Encoding utf8

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
"@ | Set-Content $tlsCnf -Encoding utf8
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
    $dropSql = ($dbs | ForEach-Object { "DROP DATABASE IF EXISTS ``$_``;") -join ' '
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
        return
    }

    # Installed but stopped
    if (Test-Path $mysqld) {
        Info "MySQL ${Ver}: installation found, attempting start ..."
        Start-Mysql $Ver $port $base
        if (Wait-Port $port 30) {
            Info "MySQL ${Ver}: started OK."
            Reset-MysqlEnv $Ver $port $base
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

    $mirror = $env:FQ_PG_MIRROR ?? 'https://get.enterprisedb.com/postgresql'

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
    # pg_ctl start writes its own log via -l; it also creates postmaster.pid
    $proc = Start-Process -FilePath $pgCtl `
        -ArgumentList 'start', "-D `"$dataDir`"", "-l `"$(Join-Path $logDir 'pg.log')`"", "-o `"-p $Port`"" `
        -NoNewWindow -Wait -PassThru
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
    $dbs = @(
        'fq_path_p','fq_src_p','fq_type_p','fq_sql_p','fq_push_p',
        'fq_local_p','fq_stab_p','fq_perf_p','fq_compat_p'
    )
    $dropSql = ($dbs | ForEach-Object { "DROP DATABASE IF EXISTS ""$_"";" }) -join ' '
    try {
        $env:PGPASSWORD = $PgPass
        & $psql -h 127.0.0.1 -p $Port -U $PgUser -d postgres `
            --connect-timeout=5 -c $dropSql 2>$null
        Info "PostgreSQL ${Ver} @ ${Port}: reset complete."
    } catch {
        Warn "PostgreSQL ${Ver} @ ${Port}: reset had warnings: $_"
    } finally {
        $env:PGPASSWORD = $null
    }
}

function Ensure-Pg {
    param([string]$Ver)
    $port  = Get-PgPort $Ver
    $base  = Join-Path $FqBase "pg\$Ver"
    $pgCtl = Join-Path $base 'bin\pg_ctl.exe'
    Info "PostgreSQL ${Ver}: port=$port, base=$base"

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
    $mirror = $env:FQ_INFLUX_MIRROR ?? 'https://dl.influxdata.com/influxdb/releases'
    return "$mirror/influxdb3-core-${patch}_windows_amd64.zip"
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
    $null = New-Item -ItemType Directory -Force $binDir
    Copy-Item $exe.FullName $binDir -Force
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
    Start-BackgroundProcess `
        -Exe     $influxd.FullName `
        -ArgList 'serve', '--node-id', 'fq-test-node', '--http-bind', "127.0.0.1:$Port",
                 '--object-store', 'file', '--data-dir', $dataDir, '--without-auth' `
        -LogFile (Join-Path $logDir 'influxd.log') `
        -PidFile $pidFile | Out-Null
}

function Reset-InfluxEnv {
    param([string]$Ver, [int]$Port)
    $base = Join-Path $FqBase "influxdb\$Ver"
    $influxBin = Get-ChildItem (Join-Path $base 'bin') -Filter 'influxdb3.exe' -ErrorAction SilentlyContinue |
                 Select-Object -First 1
    $dbs = @(
        'fq_path_i','fq_src_i','fq_type_i','fq_sql_i','fq_push_i',
        'fq_local_i','fq_stab_i','fq_perf_i','fq_compat_i'
    )
    Info "InfluxDB ${Ver} @ ${Port}: resetting test databases ..."
    foreach ($db in $dbs) {
        try {
            Invoke-RestMethod `
                -Method  DELETE `
                -Uri     "http://127.0.0.1:${Port}/api/v3/configure/database?db=$db" `
                -ErrorAction SilentlyContinue | Out-Null
        } catch { <# ignore – db may not exist #> }
        # CLI fallback (more reliable)
        if ($influxBin) {
            try {
                & $influxBin.FullName manage database delete `
                    --host "http://127.0.0.1:${Port}" `
                    --database-name $db --force 2>$null
            } catch { <# ignore #> }
        }
    }
    Info "InfluxDB ${Ver} @ ${Port}: reset complete."
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
        Info "InfluxDB ${Ver}: port $port open — already running, resetting test env."
        Reset-InfluxEnv $Ver $port
        return
    }

    if ($hasExe) {
        Info "InfluxDB ${Ver}: installation found, attempting start ..."
        Start-Influx $Ver $port $base
        if (Wait-Port $port 30) {
            Info "InfluxDB ${Ver}: started OK."
            Reset-InfluxEnv $Ver $port
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
    Info "InfluxDB ${Ver}: ready."
}

# ══════════════════════════════════════════════════════════════════════════════
# 9. Main
# ══════════════════════════════════════════════════════════════════════════════

Log "========================================================"
Log "FederatedQuery external environment setup (Windows)"
Log "  Base     : $FqBase"
Log "  MySQL    : $($MysqlVersions -join ', ')"
Log "  PG       : $($PgVersions -join ', ')"
Log "  InfluxDB : $($InfluxVersions -join ', ')"
Log "========================================================"

$null = New-Item -ItemType Directory -Force $FqBase

foreach ($ver in $MysqlVersions)  { Ensure-Mysql  $ver }
foreach ($ver in $PgVersions)     { Ensure-Pg     $ver }
foreach ($ver in $InfluxVersions) { Ensure-Influx $ver }

if (-not $OverallOk) {
    Err "One or more engines failed to start. See messages above."
    exit 1
}
Log "All engines ready."
exit 0
