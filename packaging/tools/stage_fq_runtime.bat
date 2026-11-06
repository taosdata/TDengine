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
set "FQ_ORACLE_BUILD_TREE=%FQ_BIN%\..\driver\oracle"
rem   Versioned build tree: keep driver\oracle for the 19c/23ai directories and
rem   flatten the build's active generation next to taosd (the invariant
rem   install.sh keeps in <install>/lib).  Legacy flat build: the single
rem   client stays in driver\oracle.
set "FQ_ORACLE_DEST=%FQ_DEST%\driver\oracle"
if exist "%FQ_ORACLE_BUILD_TREE%\active" set "FQ_ORACLE_DEST=%FQ_DEST%"
rem   Status: 0 staged, 1 driver missing but required, 2 copy incomplete,
rem   3 provider disabled and nothing to stage.
set "FQ_ORACLE_STATUS=0"
if not exist "%FQ_BIN%\sqora*.dll" if not exist "%FQ_ORACLE_BUILD_TREE%\active" set "FQ_ORACLE_STATUS=3"
if not exist "%FQ_BIN%\sqora*.dll" if not exist "%FQ_ORACLE_BUILD_TREE%\active" if /I "%FQ_REQUIRE_ORACLE%"=="ON" set "FQ_ORACLE_STATUS=1"
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
rem   legacy/extks/pkcs11 are optional client helpers that the Basic + ODBC
rem   packages do not ship; a placeholder under those names must never be
rem   packaged (test_fq_22 rejects a non-PE DLL), so they are not copied.
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

rem --- Versioned Oracle Instant Client trees ---------------------------
rem   The build stages build\driver\oracle\<19c|23ai>\ plus the `active`
rem   marker, the same layout the Linux dependency package carries.  Copy it
rem   as-is; the installer's selection step (stage_oracle_client.bat) rewrites
rem   the marker and flattens the selected generation next to taosd.
set "FQ_ORACLE_BUILD_TREE=%FQ_BIN%\..\driver\oracle"
if exist "%FQ_ORACLE_BUILD_TREE%\active" (
  if not exist "%FQ_DEST%\driver\oracle" mkdir "%FQ_DEST%\driver\oracle" >nul 2>&1
  xcopy /E /I /Y /Q "%FQ_ORACLE_BUILD_TREE%" "%FQ_DEST%\driver\oracle\" >nul 2>&1
  if errorlevel 1 (
    echo [FQ] failed to copy the versioned Oracle Instant Client tree 1>&2
    exit /b 1
  )
  if exist "%FQ_ORACLE_BUILD_TREE%\19c\sqora*.dll" (
    if not exist "%FQ_DEST%\driver\oracle\19c\sqora*.dll" (
      echo [FQ] the staged 19c Oracle client has no sqora driver 1>&2
      exit /b 1
    )
    if not exist "%FQ_DEST%\driver\oracle\19c\oraociei*.dll" (
      echo [FQ] the staged 19c Oracle client has no oraociei base library 1>&2
      exit /b 1
    )
    if not exist "%FQ_DEST%\driver\oracle\19c\odbc_install.exe" (
      echo [FQ] the staged 19c Oracle client has no odbc_install.exe 1>&2
      exit /b 1
    )
  )
  if exist "%FQ_ORACLE_BUILD_TREE%\23ai\sqora*.dll" (
    if not exist "%FQ_DEST%\driver\oracle\23ai\sqora*.dll" (
      echo [FQ] the staged 23ai Oracle client has no sqora driver 1>&2
      exit /b 1
    )
    if not exist "%FQ_DEST%\driver\oracle\23ai\oraociei*.dll" (
      echo [FQ] the staged 23ai Oracle client has no oraociei base library 1>&2
      exit /b 1
    )
    if not exist "%FQ_DEST%\driver\oracle\23ai\odbc_install.exe" (
      echo [FQ] the staged 23ai Oracle client has no odbc_install.exe 1>&2
      exit /b 1
    )
  )
  echo [FQ] staged the versioned Oracle Instant Client tree into %FQ_ORACLE_DEST%
)
rem   Keep a copy that matched nothing from leaking an error level out.
(call )

echo [FQ] staged %FQ_COPIED% runtime DLLs into %FQ_DEST%
endlocal & exit /b 0
