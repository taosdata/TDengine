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

set baseVersion=%1%
set version=%2%
set verMode=%3%
set sType=%4%
echo %fileType%
rem stop services
if EXIST C:\TDengine (
	if EXIST C:\TDengine\stop-all.bat (
		call C:\TDengine\stop-all.bat /silent
		echo "***************Stop taos services***************"
	)
	if exist C:\TDengine\unins000.exe (
		call C:\TDengine\unins000.exe /silent
		echo "***************uninstall TDengine***************"
	)
	rd /S /q C:\TDengine
)
if EXIST C:\ProDB (
	if EXIST C:\ProDB\stop-all.bat (
		call C:\ProDB\stop-all.bat /silent
		echo "***************Stop taos services***************"
	)
	if exist C:\ProDB\unins000.exe (
		call C:\ProDB\unins000.exe /silent
		echo "***************uninstall TDengine***************"
	)
	rd /S /q C:\ProDB
)
if "%verMode%"=="enterprise" (
    if "%sType%"=="client" (
        set fileType=enterprise-client
    ) else (
        set fileType=enterprise
    )
) else (
	set fileType=%sType%
)

if "%baseVersion%"=="ProDB" (
    echo %fileType%
    set installer=ProDB-%fileType%-%version%-Windows-x64.exe
) else (
    echo %fileType%
    set installer=TDengine-%fileType%-%version%-Windows-x64.exe
)

if "%baseVersion%"=="ProDB" (
    echo %installer%
    scp root@192.168.1.213:/nas/OEM/ProDB/v%version%/%installer% C:\workspace
) else (
    echo %installer%
    scp root@192.168.1.213:/nas/TDengine/%baseVersion%/v%version%/%verMode%/%installer% C:\workspace
)

echo "***************Finish installer transfer!***************"
C:\workspace\%installer% /silent
echo "***************Finish install!***************"