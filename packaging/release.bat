@echo off

set internal_dir=%~dp0\..\..\
set community_dir=%~dp0\..
cd %community_dir%
git checkout -- .
cd %community_dir%\packaging

:: %1 name %2 version %3 cpuType
if !%1==! GOTO USAGE
if !%2==! GOTO USAGE
if !%3==! GOTO USAGE
if %1 == taos GOTO TAOS
if %1 == power GOTO POWER
if %1 == tq GOTO TQ
if %1 == pro GOTO PRO
if %1 == kh GOTO KH
if %1 == jh GOTO JH
if %1 == hm GOTO HM
GOTO USAGE

:TAOS
goto RELEASE

:POWER
cd %internal_dir%\enterprise\packaging\oem
call sed_power.bat %community_dir%
cd %community_dir%\packaging
goto RELEASE

:TQ
cd %internal_dir%\enterprise\packaging\oem
call sed_tq.bat %community_dir%
cd %community_dir%\packaging
goto RELEASE

:PRO
cd %internal_dir%\enterprise\packaging\oem
call sed_pro.bat %community_dir%
cd %community_dir%\packaging
goto RELEASE

:KH
cd %internal_dir%\enterprise\packaging\oem
call sed_kh.bat %community_dir%
cd %community_dir%\packaging
goto RELEASE

:JH
cd %internal_dir%\enterprise\packaging\oem
call sed_jh.bat %community_dir%
cd %community_dir%\packaging
goto RELEASE

:HM
cd %internal_dir%\enterprise\packaging\oem
call sed_hm.bat %community_dir%
cd %community_dir%\packaging
goto RELEASE

:RELEASE
echo release windows-client for %1, version: %2, cpyType: %3
if not exist %internal_dir%\debug\ver-%2-%1-%3 (
	md %internal_dir%\debug\ver-%2-%1-%3
) else (
	rd /S /Q %internal_dir%\debug\ver-%2-%1-%3
	md %internal_dir%\debug\ver-%2-%1-%3
)
cd %internal_dir%\debug\ver-%2-%1-%3

if %3% == x64 GOTO X64
if %3% == x86 GOTO X86
GOTO USAGE

:X86
call "C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\vcvarsall.bat" x86
cmake ../../ -G "NMake Makefiles" -DVERNUMBER=%2 -DCPUTYPE=x86
GOTO MAKE_AND_INSTALL

:X64
call "C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\vcvarsall.bat" amd64
cmake ../../ -G "NMake Makefiles" -DVERNUMBER=%2 -DCPUTYPE=x64
GOTO MAKE_AND_INSTALL

:MAKE_AND_INSTALL
set CL=/MP4
nmake install
goto EXIT0

:USAGE
echo Usage: release.bat $productName $version $cpuType
goto EXIT0

:EXIT0