@echo off
setlocal EnableExtensions
rem Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
rem
rem SPDX-License-Identifier: AGPL-3.0-or-later
rem
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
rem flattened next to taosd.exe, which is where the Windows provider resolves
rem its driver from (the same flattening install.sh does into <install>/lib).
rem A run without a selection keeps the package default.  Exit codes: 0 ok,
rem 1 invalid selection or missing client tree, 2 copy failure.

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
if not exist "%FQ_ORACLE_DIR%\%FQ_RESOLVED%\sqora*.dll" (
  echo [FQ] Oracle client directory is missing from the package: %FQ_RESOLVED% 1>&2
  exit /b 1
)

> "%FQ_ORACLE_DIR%\active" echo %FQ_RESOLVED%
echo [FQ] Oracle: active Instant Client: %FQ_RESOLVED%

rem Flatten the selected generation next to taosd.exe.  The provider resolves
rem the driver through the registry, and this directory is the copy the build
rem stages into the package root; replacing it keeps the flat set in step with
rem driver\oracle\active (the same invariant install.sh keeps in <install>/lib).
set "FQ_ROOT=%FQ_ORACLE_DIR%\..\.."
for %%P in (sqora sqres oraociei orannz orasql oraocci legacy fips extks pkcs11) do del /q "%FQ_ROOT%\%%P*.dll" >nul 2>&1
del /q "%FQ_ROOT%\oci.dll" "%FQ_ROOT%\ociw32.dll" "%FQ_ROOT%\odbc_install.exe" >nul 2>&1
for %%P in (sqora sqres oraociei orannz orasql oraocci legacy fips extks pkcs11) do copy /y "%FQ_ORACLE_DIR%\%FQ_RESOLVED%\%%P*.dll" "%FQ_ROOT%\" >nul 2>&1
copy /y "%FQ_ORACLE_DIR%\%FQ_RESOLVED%\oci*.dll" "%FQ_ROOT%\" >nul 2>&1
copy /y "%FQ_ORACLE_DIR%\%FQ_RESOLVED%\odbc_install.exe" "%FQ_ROOT%\" >nul 2>&1
if not exist "%FQ_ROOT%\sqora*.dll" (
  echo [FQ] failed to flatten the Oracle client next to taosd.exe 1>&2
  exit /b 2
)
exit /b 0
