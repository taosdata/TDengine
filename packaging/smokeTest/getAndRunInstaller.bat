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
if "%verMode%"=="enterprise" (
    if "%sType%"=="client" (
        set fileType=enterprise-client
    ) else (
        set fileType=enterprise
    )
) else (
	set fileType=%sType%
)
echo %fileType%
set installer=TDengine-%fileType%-%version%-Windows-x64.exe
echo %installer%
scp root@192.168.1.213:/nas/TDengine/%baseVersion%/v%version%/%verMode%/%installer% C:\workspace

echo "***************Finish installer transfer!***************"
C:\workspace\%installer% /silent
echo "***************Finish install!***************"