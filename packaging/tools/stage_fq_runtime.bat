@echo off
setlocal EnableExtensions
rem Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
rem
rem Stage the Windows federated-query runtime dependencies into a package
rem directory.  The package must carry the provider client DLLs next to taosd
rem and the official Microsoft ODBC Driver 18 installer for SQL Server.
rem
rem Usage: stage_fq_runtime.bat <build-bin-dir> <package-dir> [require-msodbc]
rem
rem The first two arguments are required.  require-msodbc defaults to ON and
rem makes the SQL Server ODBC driver installer mandatory; pass OFF for a build
rem that deliberately disables the SQL Server provider.  The script fails when
rem a required provider runtime is missing so an incomplete dependency package
rem cannot be shipped silently.

set "FQ_BIN=%~1"
set "FQ_DEST=%~2"
set "FQ_REQUIRE_MSODBC=%~3"
if "%FQ_REQUIRE_MSODBC%"=="" set "FQ_REQUIRE_MSODBC=ON"

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
  "%FQ_BIN%\libmysqlclient*.dll"
  "%FQ_BIN%\libpq*.dll"
  "%FQ_BIN%\libssl*.dll"
  "%FQ_BIN%\libcrypto*.dll"
  "%FQ_BIN%\libintl*.dll"
  "%FQ_BIN%\libiconv*.dll"
  "%FQ_BIN%\libwinpthread*.dll"
  "%FQ_BIN%\taos_ext_influx_arrow*.dll"
) do (
  copy /y "%%~fF" "%FQ_DEST%\" >nul 2>&1
  if errorlevel 1 (
    echo [FQ] failed to copy %%~nxF 1>&2
    exit /b 1
  )
  set /a FQ_COPIED+=1
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

echo [FQ] staged %FQ_COPIED% runtime DLLs into %FQ_DEST%
endlocal & exit /b 0
