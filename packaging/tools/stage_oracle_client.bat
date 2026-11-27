REM Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
REM
REM SPDX-License-Identifier: AGPL-3.0-or-later
REM
REM This program is free software: you can use, redistribute, and/or modify
REM it under the terms of the GNU Affero General Public License, version 3
REM or later ("AGPL"), as published by the Free Software Foundation.
REM
REM This program is distributed in the hope that it will be useful, but WITHOUT
REM ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
REM FITNESS FOR A PARTICULAR PURPOSE.
REM
REM You should have received a copy of the GNU Affero General Public License
REM along with this program. If not, see <http://www.gnu.org/licenses/>.

@echo off
setlocal EnableExtensions
rem Select the Oracle Instant Client generation of a Windows installation,
rem the counterpart of install.sh's --oracle-server-version/--oracle-client.
rem
rem Usage: stage_oracle_client.bat <install-dir-or-driver-oracle-dir> [server-family] [client]
rem
rem   server-family: 11g 12c 18c 19c 21c 23ai 26ai none
rem   client:        19c 23ai
rem
rem Fixed mapping (Oracle FS 7.1): 11g/12c/18c -> 19c; 19c/21c/23ai/26ai ->
rem 23ai by default, 19c on request.  No arbitrary version strings are
rem parsed.  The selected generation is written to driver\oracle\active and
rem ODBC registration points directly at that version directory; DLLs are
rem never copied into the installation root or the ordinary driver directory.
rem A run without a selection keeps the package default.  Exit codes: 0 ok,
rem 1 invalid selection or missing client tree, 2 registration failure.

set "FQ_ORACLE_DIR=%~1"
set "FQ_SERVER=%~2"
set "FQ_CLIENT=%~3"

if "%FQ_ORACLE_DIR%"=="" (
  echo [FQ] usage: stage_oracle_client.bat ^<install-dir^> [server-family] [client] 1>&2
  exit /b 1
)
if not exist "%FQ_ORACLE_DIR%\19c" if not exist "%FQ_ORACLE_DIR%\23ai" (
  set "FQ_ORACLE_DIR=%FQ_ORACLE_DIR%\driver\oracle"
)
if not exist "%FQ_ORACLE_DIR%\19c" if not exist "%FQ_ORACLE_DIR%\23ai" (
  echo [FQ] no versioned Oracle client tree under %FQ_ORACLE_DIR% 1>&2
  exit /b 1
)

rem The package default carries no selection: leave the marker untouched.
if "%FQ_SERVER%"=="" if "%FQ_CLIENT%"=="" exit /b 0

rem A 23ai client cannot serve 11g; reject before touching the marker.
for %%C in (11g 12c 18c) do (
  if /I "%FQ_SERVER%"=="%%C" if /I "%FQ_CLIENT%"=="23ai" (
    echo [FQ] Oracle client 23ai does not support server family %%C 1>&2
    exit /b 1
  )
)

set "FQ_RESOLVED="
for %%C in (11g 12c 18c) do (
  if /I "%FQ_SERVER%"=="%%C" set "FQ_RESOLVED=19c"
)
if /I "%FQ_SERVER%"=="19c" set "FQ_RESOLVED=23ai"
if /I "%FQ_SERVER%"=="21c" set "FQ_RESOLVED=23ai"
if /I "%FQ_SERVER%"=="23ai" set "FQ_RESOLVED=23ai"
if /I "%FQ_SERVER%"=="26ai" set "FQ_RESOLVED=23ai"
if /I "%FQ_SERVER%"=="19c" if /I "%FQ_CLIENT%"=="19c" set "FQ_RESOLVED=19c"
if /I "%FQ_SERVER%"=="21c" if /I "%FQ_CLIENT%"=="19c" set "FQ_RESOLVED=19c"
if /I "%FQ_SERVER%"=="23ai" if /I "%FQ_CLIENT%"=="19c" set "FQ_RESOLVED=19c"
if /I "%FQ_SERVER%"=="26ai" if /I "%FQ_CLIENT%"=="19c" set "FQ_RESOLVED=19c"

if /I "%FQ_SERVER%"=="none" if /I "%FQ_CLIENT%"=="19c" set "FQ_RESOLVED=19c"
if /I "%FQ_SERVER%"=="none" if /I "%FQ_CLIENT%"=="23ai" set "FQ_RESOLVED=23ai"
if /I "%FQ_SERVER%"=="none" (
  if "%FQ_RESOLVED%"=="" set "FQ_RESOLVED=23ai"
  if not exist "%FQ_ORACLE_DIR%\23ai" if "%FQ_RESOLVED%"=="23ai" set "FQ_RESOLVED=19c"
)
if "%FQ_SERVER%"=="" (
  if /I not "%FQ_CLIENT%"=="19c" if /I not "%FQ_CLIENT%"=="23ai" (
    echo [FQ] invalid Oracle client: %FQ_CLIENT% - expected 19c or 23ai 1>&2
    exit /b 1
  )
  set "FQ_RESOLVED=%FQ_CLIENT%"
)

if "%FQ_RESOLVED%"=="" (
  echo [FQ] unsupported Oracle server version family: %FQ_SERVER% 1>&2
  exit /b 1
)
if not "%FQ_SERVER%"=="" if /I not "%FQ_SERVER%"=="none" (
  if not "%FQ_CLIENT%"=="" if /I not "%FQ_CLIENT%"=="19c" if /I not "%FQ_CLIENT%"=="23ai" (
    echo [FQ] invalid Oracle client: %FQ_CLIENT% - expected 19c or 23ai 1>&2
    exit /b 1
  )
)
if not exist "%FQ_ORACLE_DIR%\%FQ_RESOLVED%\sqora32.dll" (
  echo [FQ] Oracle client directory is missing from the package: %FQ_RESOLVED% 1>&2
  exit /b 1
)

rem Only an installed TSDB tree changes machine registration. Package staging
rem and fixture trees select the marker without touching the host registry.
set "FQ_ROOT=%FQ_ORACLE_DIR%\..\.."
powershell -NoProfile -NonInteractive -Command "$ErrorActionPreference='Stop'; $dir=[IO.Path]::GetFullPath($env:FQ_ORACLE_DIR); $marker=Join-Path $dir 'active'; $before=if(Test-Path -LiteralPath $marker){[IO.File]::ReadAllBytes($marker)}else{$null}; $key='HKLM:\SOFTWARE\ODBC\ODBCINST.INI\Oracle in instantclient_23_26'; $drivers='HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers'; $name='Oracle in instantclient_23_26'; $old=$null; $oldName=$null; $changed=$false; try { $root=[IO.Path]::GetFullPath($env:FQ_ROOT); $installed=Test-Path -LiteralPath (Join-Path $root '.package-info.ini'); if($installed){$old=Get-ItemProperty -LiteralPath $key -ErrorAction SilentlyContinue; $oldName=Get-ItemProperty -LiteralPath $drivers -ErrorAction SilentlyContinue; $owned=@((Join-Path $root 'sqora32.dll'),(Join-Path $root 'driver\sqora32.dll'),(Join-Path $dir 'sqora32.dll'),(Join-Path $dir '19c\sqora32.dll'),(Join-Path $dir '23ai\sqora32.dll')); if($old -and $old.Driver -and $old.Driver -notin $owned){throw 'Oracle ODBC registration belongs to another installation; it was not changed.'}}; [IO.File]::WriteAllText($marker,$env:FQ_RESOLVED+[Environment]::NewLine,[Text.UTF8Encoding]::new($false)); if($installed){$changed=$true; $null=New-Item -Path $key -Force; $client=Join-Path $dir $env:FQ_RESOLVED; Set-ItemProperty -LiteralPath $key -Name Driver -Value (Join-Path $client 'sqora32.dll'); $setup=Join-Path $client 'sqoras32.dll'; if(Test-Path -LiteralPath $setup){Set-ItemProperty -LiteralPath $key -Name Setup -Value $setup}else{Remove-ItemProperty -LiteralPath $key -Name Setup -ErrorAction SilentlyContinue}; $null=New-Item -Path $drivers -Force; Set-ItemProperty -LiteralPath $drivers -Name $name -Value Installed} } catch { $failure=$_; try { if($changed){if($old){foreach($v in @('Driver','Setup')){$p=$old.PSObject.Properties[$v]; if($p){Set-ItemProperty -LiteralPath $key -Name $v -Value $p.Value}else{Remove-ItemProperty -LiteralPath $key -Name $v -ErrorAction SilentlyContinue}}}else{Remove-Item -LiteralPath $key -Recurse -Force -ErrorAction SilentlyContinue}; $p=if($oldName){$oldName.PSObject.Properties[$name]}else{$null}; if($p){Set-ItemProperty -LiteralPath $drivers -Name $name -Value $p.Value}else{Remove-ItemProperty -LiteralPath $drivers -Name $name -ErrorAction SilentlyContinue}} } catch { Write-Warning ('Oracle registry rollback failed: '+$_.Exception.Message) } finally { try { if($null -ne $before){[IO.File]::WriteAllBytes($marker,$before)}else{Remove-Item -LiteralPath $marker -ErrorAction SilentlyContinue} } catch { Write-Warning ('Oracle active marker rollback failed: '+$_.Exception.Message) } }; throw $failure }"
if errorlevel 1 exit /b 2
echo [FQ] Oracle: active Instant Client: %FQ_RESOLVED%
echo [FQ] Restart taosd after changing the Oracle client.
exit /b 0
