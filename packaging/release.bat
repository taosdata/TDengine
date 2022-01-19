@echo off

set internal_dir=%~dp0\..\..\
set community_dir=%~dp0\..
cd %community_dir%
git checkout -- .
cd %community_dir%\packaging

:: %1 name %2 version
if !%1==! GOTO USAGE
if !%2==! GOTO USAGE
if %1 == taos GOTO TAOS
if %1 == power GOTO POWER
if %1 == tq GOTO TQ
if %1 == pro GOTO PRO
if %1 == kh GOTO KH
if %1 == jh GOTO JH
GOTO USAGE

:TAOS
goto RELEASE

:POWER
call sed_power.bat %community_dir%
goto RELEASE

:TQ
call sed_tq.bat %community_dir%
goto RELEASE

:PRO
call sed_pro.bat %community_dir%
goto RELEASE

:KH
call sed_kh.bat %community_dir%
goto RELEASE

:JH
call sed_jh.bat %community_dir%
goto RELEASE

:RELEASE
echo release windows-client-64 for %1, version: %2
if not exist %internal_dir%\debug\ver-%2-64bit-%1 (
	md %internal_dir%\debug\ver-%2-64bit-%1
) else (
	rd /S /Q %internal_dir%\debug\ver-%2-64bit-%1
	md %internal_dir%\debug\ver-%2-64bit-%1
)
cd %internal_dir%\debug\ver-%2-64bit-%1
call "C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\vcvarsall.bat" amd64
cmake ../../ -G "NMake Makefiles" -DVERNUMBER=%2 -DCPUTYPE=x64
set CL=/MP4
nmake install
goto EXIT0

:USAGE
echo Usage: release.bat $productName $version
goto EXIT0

:EXIT0