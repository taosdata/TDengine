#define MyAppIco "favicon.ico"
#define MyAppBeforeInstallTxt "info_before_install.txt"

[Setup]
AppName={#TaosXName}
AppVersion={#MyAppVersion}
DefaultDirName={#MyAppSourceDir}
InfoBeforeFile={#MyAppBeforeInstallTxt}
SetupIconFile={#MyAppIco}
Compression=lzma
SolidCompression=yes
DisableDirPage=yes
Uninstallable=yes

[Languages]
Name: "chinesesimp"; MessagesFile: "compiler:Default.isl"

[Files]
Source: "{#MyAppSourceDir}\plugins\*"; DestDir: "{app}\plugins"; Flags: recursesubdirs
Source: "{#MyAppSourceDir}\bin\{#TaosXAgentName}.exe"; DestDir: "{app}\bin"
Source: "{#MyAppSourceDir}\bin\{#TaosXName}.exe"; DestDir: "{app}\bin"
Source: "{#MyAppSourceDir}\bin\taosx-agent-srv.*"; DestDir: "{app}\bin"
Source: "{#MyAppSourceDir}\bin\{#TaosXName}-srv.*"; DestDir: "{app}\bin"
Source: "{#MyAppSourceDir}\config\agent.toml"; DestDir: "{app}\config"; Flags: uninsneveruninstall
Source: "{#MyAppSourceDir}\bin\taos-explorer.exe"; DestDir: "{app}\bin"
Source: "{#MyAppSourceDir}\bin\taos-explorer-srv.*"; DestDir: "{app}\bin"
Source: "{#MyAppSourceDir}\config\explorer.toml"; DestDir: "{app}\config"; Flags: uninsneveruninstall


[run]
Filename: "C:\\Program Files\\taosX\\bin\\taosx-srv.exe"; Parameters: "install" ; Flags: runhidden
Filename: "C:\\Program Files\\taosX\\bin\\taosx-agent-srv.exe"; Parameters: "install" ; Flags: runhidden
Filename: "C:\\Program Files\\taosX\\bin\\taos-explorer-srv.exe"; Parameters: "install" ; Flags: runhidden
Filename: REG.exe; Parameters: "ADD ""HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{#TaosXName}_is1"" /V ""UninstallString""  \
  /T ""REG_SZ"" /D ""\""{app}\uninstall_{#TaosXName}.exe\"""" /F"; StatusMsg: Installing {#TaosXName}...; Flags: RunHidden WaitUntilTerminated
Filename: REG.exe; Parameters: "ADD ""HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{#TaosXName}_is1"" /V ""QuietUninstallString"" \
  /T ""REG_SZ"" /D ""\""{app}\uninstall_{#TaosXName}.exe\"" /SILENT"" /F"; StatusMsg: Installing {#TaosXName}...; Flags: RunHidden WaitUntilTerminated
[UninstallRun]
RunOnceId: "stoptaosx"; Filename: {sys}\sc.exe; Parameters: "stop taosx" ; Flags: runhidden
RunOnceId: "stoptaosx-agent"; Filename: {sys}\sc.exe; Parameters: "stop taosx-agent" ; Flags: runhidden
RunOnceId: "stoptaos-explorer"; Filename: {sys}\sc.exe; Parameters: "stop taos-explorer" ; Flags: runhidden
RunOnceId: "deltaosx"; Filename: "C:\\Program Files\\taosX\\bin\\taosx-srv.exe"; Parameters: "uninstall" ; Flags: runhidden
RunOnceId: "deltaosx-agent"; Filename: "C:\\Program Files\\taosX\\bin\\taosx-agent-srv.exe"; Parameters: "uninstall" ; Flags: runhidden
RunOnceId: "deltaos-explorer"; Filename: "C:\\Program Files\\taosX\\bin\\taos-explorer-srv.exe"; Parameters: "uninstall" ; Flags: runhidden

[CODE]
procedure CurStepChanged(CurStep: TSetupStep);
var
uninspath, uninsname, NewUninsName : string;
begin
if CurStep = ssDone then
begin
NewUninsName := 'uninstall_{#TaosXName}';
uninspath := ExtractFilePath(ExpandConstant('{uninstallexe}'));
uninsname := Copy(ExtractFileName(ExpandConstant('{uninstallexe}')), 1, 8);
RenameFile(uninspath + uninsname + '.exe', uninspath + NewUninsName + '.exe');
RenameFile(uninspath + uninsname + '.dat', uninspath + NewUninsName + '.dat');
end;
end;

[UninstallDelete]
Type: files; Name: "{app}\bin\{#TaosXAgentName}.exe"
Type: files; Name: "{app}\bin\{#TaosXName}.exe"
Type: files; Name: "{app}\xplugins\pi\*.*"
Type: files; Name: "{app}\xplugins\opc\*.*"
Type: files; Name: "{app}\xplugins\mqtt\*.*"
Type: files; Name: "{app}\xplugins\influxdb\*.*"
Type: dirifempty; Name: "{app}\xplugins\pi";
Type: dirifempty; Name: "{app}\xplugins\opc";
Type: dirifempty; Name: "{app}\xplugins\mqtt";
Type: files; Name: "{app}\bin\taosx-agent-srv.*"
Type: files; Name: "{app}\bin\{#TaosXName}-srv.*"

[UninstallRun]
Filename: "{app}\uninstall.exe"; Parameters: "/SILENT"; Check: fileexists('{app}\uninstall.exe')

[Messages]
ConfirmUninstall=Do you really want to uninstall from your computer?%n%nPress [Y] to completely delete %1 and all its components;%nPress [N] to keep the software on your computer.
