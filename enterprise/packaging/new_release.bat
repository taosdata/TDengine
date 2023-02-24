@echo off

set internal_dir=%~dp0\..\..\
set community_dir=%~dp0\..
set package_dir=%cd%
set install_dir=C:\TDengine

set cusName=TDengine
set cusPrompt=taos
set cusEmail=support@taosdata.com

:param
if "%1"=="" (
    goto :readfinish
)
if %1 == -v ( set "verType=%2" && shift && shift && goto :param )
if %1 == -n ( set "version=%2" && shift && shift && goto :param )
if %1 == -N ( set "cusName=%2" && shift && shift && goto :param )
if %1 == -M ( set "cusEmail=%2"  && shift && shift && goto :param )
if %1 == -P ( set "cusPrompt=%2" && shift && shift && goto :param )

@REM unused
if %1 == -b ( shift && shift && goto :param )
if %1 == -V ( shift && shift && goto :param )
if %1 == -c ( shift && shift && goto :param )
if %1 == -l ( shift && shift && goto :param )
echo unknown argument %1
goto :eof
:readfinish

if "%verType%" == "cluster" (
	set work_dir=%internal_dir%
	set packagServerName_x64=%cusName%-enterprise-server-%version%-beta-Windows-x64
	@REM set packagServerName_x86=%cusName%-enterprise-server-%version%-beta-Windows-x86
	set packagClientName_x64=%cusName%-enterprise-client-%version%-beta-Windows-x64
	set packagClientName_x86=%cusName%-enterprise-client-%version%-beta-Windows-x86
) else (
	set work_dir=%community_dir%
	set packagServerName_x64=%cusName%-server-%version%-Windows-x64
	@REM set packagServerName_x86=%cusName%-server-%version%-Windows-x86
	set packagClientName_x64=%cusName%-client-%version%-Windows-x64
	set packagClientName_x86=%cusName%-client-%version%-Windows-x86
)

echo release windows-client for %verType%, version: %version%
if not exist %work_dir%\debug (
	md %work_dir%\debug
)
if not exist %work_dir%\debug\ver-%version%-x64 (
	md %work_dir%\debug\ver-%version%-x64
) else (
	rd /S /Q %work_dir%\debug\ver-%version%-x64
	md %work_dir%\debug\ver-%version%-x64
)
if not exist %work_dir%\debug\ver-%version%-x86 (
	md %work_dir%\debug\ver-%version%-x86
) else (
	rd /S /Q %work_dir%\debug\ver-%version%-x86
	md %work_dir%\debug\ver-%version%-x86
)

cd %work_dir%\debug\ver-%version%-x64
call vcvarsall.bat x64
echo "cmake ../../ -G "NMake Makefiles JOM" -DCMAKE_MAKE_PROGRAM=jom -DBUILD_TOOLS=true -DBUILD_TAOSX=true -DWEBSOCKET=true -DBUILD_HTTP=internal -DBUILD_TEST=false -DVERNUMBER=%version% -DCPUTYPE=x64 -DCUS_NAME=%cusName% -DCUS_PROMPT=%cusPrompt% -DCUS_EMAIL=%cusEmail%"
cmake ../../ -G "NMake Makefiles JOM" -DCMAKE_MAKE_PROGRAM=jom -DBUILD_TOOLS=true -DBUILD_TAOSX=true -DWEBSOCKET=true -DBUILD_HTTP=internal -DBUILD_TEST=false -DVERNUMBER=%version% -DCPUTYPE=x64 -DCUS_NAME=%cusName% -DCUS_PROMPT=%cusPrompt% -DCUS_EMAIL=%cusEmail%
cmake --build .
rd /s /Q C:\TDengine
cmake --install .
if not %errorlevel% == 0  ( call :RUNFAILED build x64 failed & exit /b 1)
if "%verType%" == "cluster" (
	md  %install_dir%\connector
	git clone --depth 1 https://github.com/taosdata/driver-go %install_dir%/connector/go
	rm -rf %install_dir%/connector/go/.git*
	git clone --depth 1 https://github.com/taosdata/taos-connector-python %install_dir%/connector/python
	rm -rf %install_dir%/connector/python/.git*
	git clone --depth 1 https://github.com/taosdata/taos-connector-node %install_dir%/connector/nodejs
	rm -rf %install_dir%/connector/nodejs/.git*
	git clone --depth 1 https://github.com/taosdata/taos-connector-dotnet %install_dir%/connector/dotnet
	rm -rf %install_dir%/connector/dotnet/.git*
	git clone --depth 1 https://github.com/taosdata/taos-connector-rust %install_dir%/connector/rust
	rm -rf %install_dir%/connector/rust/.git*

  	md %install_dir%\examples
	
    set examples_dir="%internal_dir%\community\examples"
    echo "xcopy %examples_dir% to %install_dir%\examples"
    xcopy /S %examples_dir%\c %install_dir%\examples\c\*
    xcopy /S %examples_dir%\JDBC  %install_dir%\examples\JDBC\*
    xcopy /S %examples_dir%\matlab  %install_dir%\examples\matlab\*
    xcopy /S %examples_dir%\python  %install_dir%\examples\python\*
    xcopy /S %examples_dir%\R  %install_dir%\examples\R\*
    xcopy /S %examples_dir%\go  %install_dir%\examples\go\*
    xcopy /S %examples_dir%\nodejs  %install_dir%\examples\nodejs\*
    xcopy /S %examples_dir%\C#  %install_dir%\examples\C#\*
)
cd %package_dir%
iscc /DMyAppInstallName="%packagServerName_x64%" /DMyAppVersion="%version%" /DMyAppExcludeSource="" /DCusName="%cusName%" /DCusPrompt="%cusPrompt%" %internal_dir%\community\packaging\tools\tdengine.iss /O..\release
if not %errorlevel% == 0  ( call :RUNFAILED package %packagServerName_x64% failed & exit /b 1)
iscc /DMyAppInstallName="%packagClientName_x64%" /DMyAppVersion="%version%" /DMyAppExcludeSource="taosd.exe" /DCusName="%cusName%" /DCusPrompt="%cusPrompt%" %internal_dir%\community\packaging\tools\tdengine.iss /O..\release
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
