@echo off

set internal_dir=%~dp0\..\..\
set community_dir=%~dp0\..
set package_dir=%cd%

:: %1 name %2 version
if !%1==! GOTO USAGE
if !%2==! GOTO USAGE

if "%1" == "cluster" (
	set work_dir=%internal_dir%
	set packagServerName_x64=TDengine-enterprise-server-%2-beta-Windows-x64
	@REM set packagServerName_x86=TDengine-enterprise-server-%2-beta-Windows-x86
	set packagClientName_x64=TDengine-enterprise-client-%2-beta-Windows-x64
	set packagClientName_x86=TDengine-enterprise-client-%2-beta-Windows-x86
) else (
	set work_dir=%community_dir%
	set packagServerName_x64=TDengine-server-%2-Windows-x64
	@REM set packagServerName_x86=TDengine-server-%2-Windows-x86
	set packagClientName_x64=TDengine-client-%2-Windows-x64
	set packagClientName_x86=TDengine-client-%2-Windows-x86
)

echo release windows-client for %1, version: %2
if not exist %work_dir%\debug (
	md %work_dir%\debug
)
if not exist %work_dir%\debug\ver-%2-x64 (
	md %work_dir%\debug\ver-%2-x64
) else (
	rd /S /Q %work_dir%\debug\ver-%2-x64
	md %work_dir%\debug\ver-%2-x64
)
if not exist %work_dir%\debug\ver-%2-x86 (
	md %work_dir%\debug\ver-%2-x86
) else (
	rd /S /Q %work_dir%\debug\ver-%2-x86
	md %work_dir%\debug\ver-%2-x86
)
cd %work_dir%\debug\ver-%2-x64
call vcvarsall.bat x64
cmake ../../ -G "NMake Makefiles JOM" -DCMAKE_MAKE_PROGRAM=jom -DBUILD_TOOLS=true -DWEBSOCKET=true -DBUILD_HTTP=false -DBUILD_TEST=false -DVERNUMBER=%2 -DCPUTYPE=x64
cmake --build .
rd /s /Q C:\TDengine
cmake --install .
if not %errorlevel% == 0  ( call :RUNFAILED build x64 failed & exit /b 1)
cd %package_dir%
iscc /DMyAppInstallName="%packagServerName_x64%" /DMyAppVersion="%2"  /DCusName="TDengine" /DCusPrompt="taos" /DMyAppExcludeSource="" tools\tdengine.iss /O..\release
if not %errorlevel% == 0  ( call :RUNFAILED package %packagServerName_x64% failed & exit /b 1)
iscc /DMyAppInstallName="%packagClientName_x64%" /DMyAppVersion="%2"  /DCusName="TDengine" /DCusPrompt="taos" /DMyAppExcludeSource="taosd.exe" tools\tdengine.iss /O..\release
if not %errorlevel% == 0  ( call :RUNFAILED package %packagClientName_x64% failed & exit /b 1)

goto EXIT0

:USAGE
echo Usage: release.bat $verMode $version
goto EXIT0

:EXIT0
exit /b

:RUNFAILED
echo %*
cd %package_dir%
goto :eof
