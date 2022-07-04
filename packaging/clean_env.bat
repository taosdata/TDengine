@echo off

set CUR_DIR=%~dp0
set SHELL_DIR=%cd%
set ENTERPRISE_DIR="%SHELL_DIR%\..\.."
set COMMUNITY_DIR="%SHELL_DIR%\.."
set TOOLS_DIR="%SHELL_DIR%\..\src\kit\taos-tools"

cd %ENTERPRISE_DIR%
git checkout -- .
if exist "enterprise\src\plugins\taosainternal\taosadapter" (
    del /f "enterprise\src\plugins\taosainternal\taosadapter"
)
if exist "enterprise\src\plugins\taosainternal\upx.tar.xz" (
   del /f "enterprise\src\plugins\taosainternal\upx.tar.xz"
)

cd %COMMUNITY_DIR%
git checkout -- .

cd %TOOLS_DIR%
git checkout -- .
if exist "packaging\tools\install-khtools.sh" (
  del /f "packaging\tools\install-khtools.sh"
)
if exist "packaging\tools\uninstall-khtools.sh" (
  del /f "packaging/tools/uninstall-khtools.sh"
)

if exist "packaging\tools\install-prodbtools.sh" (
  del /f "packaging\tools\install-prodbtools.sh"
)
if exist "packaging\tools\uninstall-prodbtools.sh" (
  del /f "packaging\tools\uninstall-prodbtools.sh"
)

cd %CUR_DIR%