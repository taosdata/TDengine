REM Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

rm -rf %WIN_TDENGINE_ROOT_DIR%\debug
mkdir %WIN_TDENGINE_ROOT_DIR%\debug
mkdir %WIN_TDENGINE_ROOT_DIR%\debug\build
mkdir %WIN_TDENGINE_ROOT_DIR%\debug\build\bin
xcopy C:\TDengine\taos*.exe %WIN_TDENGINE_ROOT_DIR%\debug\build\bin

set case_out_file=%cd%\case.out

cd %WIN_TDENGINE_ROOT_DIR%\tests\system-test
python3 .\test.py -f 0-others\taosShell.py
python3 .\test.py -f 6-cluster\5dnode3mnodeSep1VnodeStopDnodeModifyMeta.py  -N 6 -M 3