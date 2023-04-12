taskkill /IM "AFExplorer.exe" /F
cd C:\Git\td-pi-connector\dist
"%pihome%\AF\regplugin" -u TDEngineDR.dll
"C:\Program Files (x86)\Microsoft SDKs\ClickOnce\SignTool\signtool.exe" sign /debug /f "C:\Git\td-pi-connector\scripts\certificate.pfx" /p "Pa$$w0rd" /t http://timestamp.digicert.com /v "TDEngineDR.dll"
"%pihome%\AF\regplugin" "TDEngineDR.dll" "Newtonsoft.Json.dll" /own:TDEngineDR.dll
cd C:\Git\td-pi-connector\scripts
CopyPdb "C:\Git\td-pi-connector\dist"
pause
