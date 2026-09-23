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

echo Executing copy_udf.bat
set SCRIPT_DIR=%cd%
echo SCRIPT_DIR: %SCRIPT_DIR%

echo %cd% | grep community > nul && cd ..\..\.. || cd ..\..
set TAOS_DIR=%cd%
echo find udf library in %TAOS_DIR%
set UDF1_DIR=%TAOS_DIR%\debug\build\lib\udf1.dll
set UDF2_DIR=%TAOS_DIR%\debug\build\lib\udf2.dll

echo %UDF1_DIR%
echo %UDF2_DIR%

set UDF_TMP=C:\Windows\Temp\udf
rm -rf %UDF_TMP%
mkdir %UDF_TMP%

echo Copy udf shared library files to %UDF_TMP%

cp %UDF1_DIR% %UDF_TMP%
cp %UDF2_DIR% %UDF_TMP%
