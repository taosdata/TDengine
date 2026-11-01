@echo off
setlocal EnableExtensions
rem Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
rem
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
rem   The ODBC driver (sqora32.dll) loads its Instant Client base libraries
rem   (oraociei, orannz, orasql, oci, ...) by name from its own directory, so
rem   the whole set stays together in driver\oracle.  Windows resolves an ODBC
rem   driver only through its registry registration and the Driver keyword
rem   cannot name a DLL path, so the package also carries odbc_install.exe:
rem   running it from driver\oracle registers the driver with its absolute
rem   path.  ensure_ext_env.ps1 performs the same registration on a
rem   development host; the provider in turn resolves the driver through that
rem   registration and preloads the base DLLs from the registered directory.
rem   Batch files in this repository are stored with LF-only line endings, so
rem   this section uses no goto labels (cmd.exe cannot resolve them here) and
rem   no for loops over wildcard tokens (such a token is matched against the
rem   current directory and silently dropped; copy expands a quoted wildcard
rem   itself).  Only single-level if blocks are used so that exit /b keeps
rem   its exit code.
set "FQ_ORACLE_DEST=%FQ_DEST%\driver\oracle"
rem   Status: 0 staged, 1 driver missing but required, 2 copy incomplete,
rem   3 provider disabled and nothing to stage.
set "FQ_ORACLE_STATUS=0"
if not exist "%FQ_BIN%\sqora*.dll" set "FQ_ORACLE_STATUS=3"
if not exist "%FQ_BIN%\sqora*.dll" if /I "%FQ_REQUIRE_ORACLE%"=="ON" set "FQ_ORACLE_STATUS=1"
if "%FQ_ORACLE_STATUS%"=="1" (
  echo [FQ] Oracle Instant Client driver is missing: %FQ_BIN%\sqora*.dll 1>&2
  exit /b 1
)
if exist "%FQ_BIN%\sqora*.dll" if not exist "%FQ_ORACLE_DEST%" mkdir "%FQ_ORACLE_DEST%" >nul 2>&1
rem   A pattern that matches nothing only sets an error level; the
rem   completeness checks below turn a real gap into a hard failure.
if exist "%FQ_BIN%\sqora*.dll" copy /y "%FQ_BIN%\sqora*.dll" "%FQ_ORACLE_DEST%\" >nul 2>&1
if exist "%FQ_BIN%\sqora*.dll" copy /y "%FQ_BIN%\sqres*.dll" "%FQ_ORACLE_DEST%\" >nul 2>&1
if exist "%FQ_BIN%\sqora*.dll" copy /y "%FQ_BIN%\oraociei*.dll" "%FQ_ORACLE_DEST%\" >nul 2>&1
if exist "%FQ_BIN%\sqora*.dll" copy /y "%FQ_BIN%\orannz*.dll" "%FQ_ORACLE_DEST%\" >nul 2>&1
if exist "%FQ_BIN%\sqora*.dll" copy /y "%FQ_BIN%\orasql*.dll" "%FQ_ORACLE_DEST%\" >nul 2>&1
if exist "%FQ_BIN%\sqora*.dll" copy /y "%FQ_BIN%\oraocci*.dll" "%FQ_ORACLE_DEST%\" >nul 2>&1
if exist "%FQ_BIN%\sqora*.dll" copy /y "%FQ_BIN%\oci.dll" "%FQ_ORACLE_DEST%\" >nul 2>&1
if exist "%FQ_BIN%\sqora*.dll" copy /y "%FQ_BIN%\ociw32.dll" "%FQ_ORACLE_DEST%\" >nul 2>&1
if exist "%FQ_BIN%\sqora*.dll" copy /y "%FQ_BIN%\fips*.dll" "%FQ_ORACLE_DEST%\" >nul 2>&1
if exist "%FQ_BIN%\sqora*.dll" copy /y "%FQ_BIN%\legacy.dll" "%FQ_ORACLE_DEST%\" >nul 2>&1
if exist "%FQ_BIN%\sqora*.dll" copy /y "%FQ_BIN%\extks.dll" "%FQ_ORACLE_DEST%\" >nul 2>&1
if exist "%FQ_BIN%\sqora*.dll" copy /y "%FQ_BIN%\pkcs11.dll" "%FQ_ORACLE_DEST%\" >nul 2>&1
if exist "%FQ_BIN%\sqora*.dll" if exist "%FQ_BIN%\odbc_install.exe" copy /y "%FQ_BIN%\odbc_install.exe" "%FQ_ORACLE_DEST%\" >nul 2>&1
if exist "%FQ_BIN%\sqora*.dll" if not exist "%FQ_ORACLE_DEST%\sqora*.dll" set "FQ_ORACLE_STATUS=2"
if exist "%FQ_BIN%\sqora*.dll" if not exist "%FQ_ORACLE_DEST%\oraociei*.dll" set "FQ_ORACLE_STATUS=2"
if exist "%FQ_BIN%\sqora*.dll" if not exist "%FQ_ORACLE_DEST%\odbc_install.exe" set "FQ_ORACLE_STATUS=2"
if "%FQ_ORACLE_STATUS%"=="2" (
  echo [FQ] failed to stage the Oracle Instant Client into %FQ_ORACLE_DEST%: 1>&2
  echo [FQ] driver sqora*.dll, base library oraociei*.dll and odbc_install.exe are required 1>&2
  exit /b 1
)
if "%FQ_ORACLE_STATUS%"=="0" echo [FQ] staged the Oracle Instant Client into %FQ_ORACLE_DEST%
if "%FQ_ORACLE_STATUS%"=="3" echo [FQ] Oracle provider is disabled; the Instant Client is not packaged
rem   Keep a copy that matched nothing from leaking an error level out.
(call )

echo [FQ] staged %FQ_COPIED% runtime DLLs into %FQ_DEST%
endlocal & exit /b 0
