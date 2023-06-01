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
Source: "{#MyAppSourceDir}\cfg\agent.example.toml"; DestDir: "{app}\cfg"

[UninstallRun]
RunOnceId: "stoptaosx"; Filename: {sys}\sc.exe; Parameters: "stop taosx-srv" ; Flags: runhidden
RunOnceId: "stoptaosx-agent"; Filename: {sys}\sc.exe; Parameters: "stop taosx-agent-srv" ; Flags: runhidden
RunOnceId: "deltaosx"; Filename: {sys}\sc.exe; Parameters: "delete taosx-srv" ; Flags: runhidden
RunOnceId: "deltaosx-agent"; Filename: {sys}\sc.exe; Parameters: "delete taosx-agent-srv" ; Flags: runhidden

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
Type: files; Name: "{app}\bin\taosx-agent-srv.*"
Type: files; Name: "{app}\bin\{#TaosXName}-srv.*"
Type: files; Name: "{app}\xplugins\pi\*.*"
Type: files; Name: "{app}\xplugins\opc\*.*"
Type: files; Name: "{app}\xplugins\mqtt\*.*"
Type: files; Name: "{app}\xplugins\influxdb\*.*"
Type: dirifempty; Name: "{app}\xplugins\pi";
Type: dirifempty; Name: "{app}\xplugins\opc";
Type: dirifempty; Name: "{app}\xplugins\mqtt";

[UninstallRun]
Filename: "{app}\uninstall.exe"; Parameters: "/SILENT"; Check: fileexists('{app}\uninstall.exe')

[Messages]
ConfirmUninstall=Do you really want to uninstall from your computer?%n%nPress [Y] to completely delete %1 and all its components;%nPress [N] to keep the software on your computer.
