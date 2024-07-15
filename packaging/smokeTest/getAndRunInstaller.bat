set baseVersion=%1%
set version=%2%
set verMode=%3%
set fileType=%4%
if %verMode% == "enterprise" ( fileType = enterprise-client)
set installer=TDengine-%fileType%-%version%-Windows-x64.exe
echo %installer%
scp root@192.168.1.213:/nas/TDengine/%baseVersion%/v%version%/%verMode%/%installer% C:\workspace

echo "***************Finish installer transfer!***************"
C:\workspace\%installer% /silent
echo "***************Finish install!***************"
