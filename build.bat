REM Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

setlocal

if "%TD_CONFIG%"=="" set "TD_CONFIG=Debug"

for /F "tokens=1,* delims= " %%A in ("%*") do (
    if "%%A" EQU "gen" (
        call :do_gen %%B
        exit /B
    )
    if "%%A" EQU "bld" (
        call :do_bld %%B
        exit /B
    )
    if "%%A" EQU "install" (
        call :do_stop
        call :do_install %%B
        exit /B
    )
    if "%%A" EQU "test" (
        call :do_test %%B
        exit /B
    )
    if "%%A" EQU "start" (
        call :do_start
        exit /B
    )
    if "%%A" EQU "stop" (
        call :do_stop
        exit /B
    )
    if "%%A" EQU "purge" (
        call :do_stop
        call :do_purge
        exit /B
    )
)

echo "env TD_CONFIG denotes which to build for: <Debug/Release>, Debug by default"
echo "to generate make files: .\build.bat gen [cmake options]"
echo "to build:               .\build.bat bld [cmake options for --build]"
echo "to install:             .\build.bat install [cmake options for --install]"
echo "to run test:            .\build.bat test [ctest options]"
echo "to start:               .\build.bat start"
echo "to stop:                .\build.bat stop"
echo "to purge:               .\build.bat purge"
exit /B

:do_gen
cmake -B debug -G "NMake Makefiles" ^
        -DLOCAL_REPO:STRING=%LOCAL_REPO% ^
        -DLOCAL_URL:STRING=%LOCAL_URL% ^
        -DBUILD_TOOLS=true ^
        -DBUILD_KEEPER=true ^
        -DBUILD_HTTP=false ^
        -DBUILD_TEST=true ^
        -DWEBSOCKET=true ^
        -DBUILD_DEPENDENCY_TESTS=false ^
        -DCMAKE_BUILD_TYPE=%TD_CONFIG% ^
        %*
exit /B

:do_bld
cmake --build debug --config %TD_CONFIG% -j4 %*
exit /B

:do_install
cmake --install debug --config %TD_CONFIG% %*
exit /B

:do_start
sc start taosd
sc start taosadapter
sc start taoskeeper
exit /B

:do_stop
sc stop taoskeeper
sc stop taosadapter
sc stop taosd
exit /B

:do_purge
sc delete taoskeeper
sc delete taosadapter
sc delete taosd
rmdir /s /q C:\TDengine
exit /B

:do_test
ctest --test-dir debug -C %TD_CONFIG% --output-on-failure %*
exit /B

endlocal

exit /B
