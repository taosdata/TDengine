REM Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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
goto %1
:needAdmin

set source_dir=%2
set source_dir=%source_dir:/=\\%
set binary_dir=%3
set binary_dir=%binary_dir:/=\\%
set osType=%4

set tagert_dir=C:\\TDengine

if not exist %tagert_dir% (
    mkdir %tagert_dir%
)

if exist %binary_dir%\\build\\bin\\taosdump.exe (
    copy %binary_dir%\\build\\bin\\taosdump.exe %tagert_dir% > nul
)

if exist %binary_dir%\\build\\bin\\taosBenchmark.exe (
    copy %binary_dir%\\build\\bin\\taosBenchmark.exe %tagert_dir% > nul
)
