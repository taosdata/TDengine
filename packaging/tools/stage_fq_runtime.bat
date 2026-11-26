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
rem Stage the Windows federated-query runtime dependencies into a package
rem directory.  The package must carry the provider client DLLs next to taosd,
rem the official Microsoft ODBC Driver 18 installer for SQL Server, and the
rem Oracle Instant Client used by the Oracle ODBC provider.
rem
rem Usage: stage_fq_runtime.bat <build-bin-dir> <package-dir> [require-msodbc] [require-oracle]
rem
rem The first two arguments are required.  require-msodbc defaults to ON and
rem makes the SQL Server ODBC driver installer mandatory; pass OFF for a build
rem that deliberately disables the SQL Server provider.  require-oracle
rem defaults to the require-msodbc value because both providers are built by
rem the same ODBC switch.  The script fails when a required provider runtime is
rem missing so an incomplete dependency package cannot be shipped silently.

set "FQ_BIN=%~1"
set "FQ_DEST=%~2"
set "FQ_REQUIRE_MSODBC=%~3"
if "%FQ_REQUIRE_MSODBC%"=="" set "FQ_REQUIRE_MSODBC=ON"
set "FQ_REQUIRE_ORACLE=%~4"
if "%FQ_REQUIRE_ORACLE%"=="" set "FQ_REQUIRE_ORACLE=%FQ_REQUIRE_MSODBC%"

if "%FQ_BIN%"=="" (
  echo [FQ] usage: stage_fq_runtime.bat ^<build-bin-dir^> ^<package-dir^> 1>&2
  exit /b 2
)
if "%FQ_DEST%"=="" (
  echo [FQ] usage: stage_fq_runtime.bat ^<build-bin-dir^> ^<package-dir^> 1>&2
  exit /b 2
)
if not exist "%FQ_BIN%" (
  echo [FQ] build output directory not found: %FQ_BIN% 1>&2
  exit /b 2
)
if not exist "%FQ_DEST%" mkdir "%FQ_DEST%" >nul 2>&1

set /a FQ_COPIED=0
for %%F in (
  "%FQ_BIN%\libmariadb*.dll"
  "%FQ_BIN%\caching_sha2_password.dll"
  "%FQ_BIN%\sha256_password.dll"
  "%FQ_BIN%\client_ed25519.dll"
  "%FQ_BIN%\dialog.dll"
  "%FQ_BIN%\mysql_clear_password.dll"
  "%FQ_BIN%\pvio_shmem.dll"
  "%FQ_BIN%\libmysqlclient*.dll"
  "%FQ_BIN%\libpq*.dll"
  "%FQ_BIN%\libssl*.dll"
  "%FQ_BIN%\libcrypto*.dll"
  "%FQ_BIN%\libintl*.dll"
  "%FQ_BIN%\libiconv*.dll"
  "%FQ_BIN%\libwinpthread*.dll"
  "%FQ_BIN%\taos_ext_influx_arrow*.dll"
) do (
  if exist "%%~fF" (
    copy /y "%%~fF" "%FQ_DEST%\" >nul 2>&1
    if errorlevel 1 (
      echo [FQ] failed to copy %%~nxF 1>&2
      exit /b 1
    )
    set /a FQ_COPIED+=1
  )
)
if %FQ_COPIED% EQU 0 (
  echo [FQ] no federated-query runtime DLLs found in %FQ_BIN% 1>&2
  exit /b 1
)

if not exist "%FQ_DEST%\driver" mkdir "%FQ_DEST%\driver" >nul 2>&1
if exist "%FQ_BIN%\msodbcsql18.msi" (
  copy /y "%FQ_BIN%\msodbcsql18.msi" "%FQ_DEST%\driver\" >nul 2>&1
  if errorlevel 1 (
    echo [FQ] failed to copy msodbcsql18.msi 1>&2
    exit /b 1
  )
  echo [FQ] staged the SQL Server ODBC driver ^(msodbcsql18.msi^) into %FQ_DEST%\driver
) else (
  if /I "%FQ_REQUIRE_MSODBC%"=="ON" (
    echo [FQ] SQL Server ODBC driver MSI is missing: %FQ_BIN%\msodbcsql18.msi 1>&2
    exit /b 1
  ) else (
    echo [FQ] SQL Server provider is disabled; the ODBC driver MSI is not packaged
  )
)

rem --- Oracle Instant Client -------------------------------------------
rem Keep one copy per generation; registered ODBC paths load dependencies
rem from driver\oracle\<active>. Never publish the build/bin flat copy.
set "FQ_ORACLE_BUILD_TREE=%FQ_BIN%\..\driver\oracle"
set "FQ_ORACLE_DEST=%FQ_DEST%\driver\oracle"
if exist "%FQ_ORACLE_BUILD_TREE%\active" (
  xcopy /E /I /Y /Q "%FQ_ORACLE_BUILD_TREE%" "%FQ_ORACLE_DEST%\" >nul 2>&1
  if errorlevel 1 exit /b 1
) else (
  rem Compatibility with old build outputs containing only a 23ai flat set.
  if exist "%FQ_BIN%\sqora*.dll" (
    if not exist "%FQ_ORACLE_DEST%\23ai" mkdir "%FQ_ORACLE_DEST%\23ai" >nul 2>&1
    for %%P in (sqora sqres oraociei orannz orasql oraocci fips) do copy /y "%FQ_BIN%\%%P*.dll" "%FQ_ORACLE_DEST%\23ai\" >nul 2>&1
    copy /y "%FQ_BIN%\oraons.dll" "%FQ_ORACLE_DEST%\23ai\" >nul 2>&1
    copy /y "%FQ_BIN%\oci.dll" "%FQ_ORACLE_DEST%\23ai\" >nul 2>&1
    copy /y "%FQ_BIN%\ociw32.dll" "%FQ_ORACLE_DEST%\23ai\" >nul 2>&1
    copy /y "%FQ_BIN%\odbc_install.exe" "%FQ_ORACLE_DEST%\23ai\" >nul 2>&1
    > "%FQ_ORACLE_DEST%\active" echo 23ai
  )
)
(call )
if exist "%FQ_ORACLE_DEST%\active" powershell -NoProfile -NonInteractive -Command "$ErrorActionPreference='Stop'; $root=$env:FQ_ORACLE_DEST; $active=(Get-Content -LiteralPath (Join-Path $root 'active') -Raw).Trim(); if ($active -cnotin @('19c','23ai')) { throw 'Invalid Oracle active generation' }; if (-not (Test-Path -LiteralPath (Join-Path $root $active) -PathType Container)) { throw 'Missing active Oracle client' }; foreach ($client in @('19c','23ai')) { $dir=Join-Path $root $client; if (Test-Path -LiteralPath $dir) { if ($client -eq '19c' -and -not (Test-Path -LiteralPath (Join-Path $dir 'oraons.dll') -PathType Leaf)) { throw 'Incomplete Oracle 19c: oraons.dll' }; foreach ($pattern in @('sqora32.dll','sqres*.dll','oraociei*.dll','orannz*.dll','orasql*.dll','oci.dll','odbc_install.exe')) { if (-not (Get-ChildItem -Path (Join-Path $dir $pattern) -File)) { throw ('Incomplete Oracle '+$client+': '+$pattern) } } } }"
if errorlevel 1 exit /b 1
if exist "%FQ_ORACLE_DEST%\active" (
  copy /y "%~dp0stage_oracle_client.bat" "%FQ_ORACLE_DEST%\select-client.bat" >nul 2>&1
  if errorlevel 1 exit /b 1
  echo [FQ] staged versioned Oracle clients into %FQ_ORACLE_DEST%
) else (
  if /I "%FQ_REQUIRE_ORACLE%"=="ON" (
    echo [FQ] Oracle Instant Client is missing 1>&2
    exit /b 1
  )
  echo [FQ] Oracle provider is disabled; the Instant Client is not packaged
)
(call )
echo [FQ] staged %FQ_COPIED% runtime DLLs into %FQ_DEST%
endlocal & exit /b 0
