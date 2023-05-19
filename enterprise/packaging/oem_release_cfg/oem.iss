#define MyAppName "TDengine"
#define MyAppPublisher "taosdata"
#define MyAppURL "http://www.taosdata.com/"
#define MyAppBeforeInstallTxt ".\..\..\community\packaging\tools\windows_before_install.txt"
; #define MyAppIco "favicon.ico"
; #define MyAppInstallDir "C:\TDengine"
#define MyAppOutputDir "./"
#define MyAppSourceDir "C:\TDengine"
;#define MyAppAllFile "\*"
#define MyAppCfgName "\cfg\*"
#define MyAppDriverName "\driver\*"
#define MyAppConnectorName "\connector\*"
#define MyAppExamplesName "\examples\*"
#define MyAppIncludeName "\include\*"
#define MyAppExeName "\*.exe"
#define MyAppTaosExeName ".\..\..\community\packaging\tools\taos.bat"
#define MyAppTaosdemoExeName "\taosBenchmark.exe"
#define MyAppDLLName "\driver\*.dll"
;#define MyAppVersion "3.0"
;#define MyAppInstallName "TDengine"
[Setup]
VersionInfoVersion={#MyAppVersion}
AppId={{A0F7A93C-79C4-485D-B2B8-F0D03DF42FAB}
AppName={#CusName}
AppVersion={#MyAppVersion}
;AppVerName={#MyAppName} {#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={#MyAppInstallDir}
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
InfoBeforeFile={#MyAppBeforeInstallTxt}
OutputDir={#MyAppOutputDir}
OutputBaseFilename={#MyAppInstallName}
SetupIconFile={#MyAppIco}
Compression=lzma
SolidCompression=yes
DisableDirPage=yes
Uninstallable=yes

[Languages]
Name: "chinesesimp"; MessagesFile: "compiler:Default.isl"
;Name: "english"; MessagesFile: "compiler:Languages\English.isl"

[Files]
;Source: {#MyAppSourceDir}{#MyAppAllFile}; DestDir: "{code:GetCustomPath}"; Flags: igNoreversion recursesubdirs createallsubdirs 
Source: {#MyAppTaosExeName}; DestDir: "{code:GetCustomPath}\include"; Flags: igNoreversion;
;Source: taosdemo.png; DestDir: "{code:GetCustomPath}\include"; Flags: igNoreversion;
;Source: taosShell.png; DestDir: "{code:GetCustomPath}\include"; Flags: igNoreversion;
Source: {#MyAppIco}; DestDir: "{code:GetCustomPath}\include"; Flags: igNoreversion;
Source: {#MyAppSourceDir}{#MyAppDLLName}; DestDir: "{win}\System32"; Flags: igNoreversion recursesubdirs createallsubdirs 64bit;Check:IsWin64;
Source: {#MyAppSourceDir}{#MyAppCfgName}; DestDir: "{code:GetCustomPath}\cfg"; Flags: igNoreversion recursesubdirs createallsubdirs onlyifdoesntexist uninsneveruninstall
Source: {#MyAppSourceDir}{#MyAppDriverName}; DestDir: "{code:GetCustomPath}\driver"; Flags: igNoreversion recursesubdirs createallsubdirs
;Source: {#MyAppSourceDir}{#MyAppConnectorName}; DestDir: "{code:GetCustomPath}\connector"; Flags: igNoreversion recursesubdirs createallsubdirs
;Source: {#MyAppSourceDir}{#MyAppExamplesName}; DestDir: "{code:GetCustomPath}\examples"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppIncludeName}; DestDir: "{code:GetCustomPath}\include"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppExeName}; DestDir: "{code:GetCustomPath}"; Excludes: {#MyAppExcludeSource}; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppTaosdemoExeName}; DestDir: "{code:GetCustomPath}"; Flags: igNoreversion recursesubdirs createallsubdirs

[run]
Filename: {sys}\sc.exe; Parameters: "create taosd start= DEMAND binPath= ""C:\\TDengine\\taosd.exe --win_service""" ; Flags: runhidden
Filename: {sys}\sc.exe; Parameters: "create taosadapter start= DEMAND binPath= ""C:\\TDengine\\taosadapter.exe""" ; Flags: runhidden

[UninstallRun]
RunOnceId: "stoptaosd"; Filename: {sys}\sc.exe; Parameters: "stop taosd" ; Flags: runhidden
RunOnceId: "stoptaosadapter"; Filename: {sys}\sc.exe; Parameters: "stop taosadapter" ; Flags: runhidden
RunOnceId: "deltaosd"; Filename: {sys}\sc.exe; Parameters: "delete taosd" ; Flags: runhidden
RunOnceId: "deltaosadapter"; Filename: {sys}\sc.exe; Parameters: "delete taosadapter" ; Flags: runhidden

[Registry]
Root: HKLM; Subkey: "SYSTEM\CurrentControlSet\Control\Session Manager\Environment"; \
    ValueType: expandsz; ValueName: "Path"; ValueData: "{olddata};C:\{#CusName}"; \
    Check: NeedsAddPath('C:\{#CusName}')

[Code]
function NeedsAddPath(Param: string): boolean;
var
  OrigPath: string;
begin
  if not RegQueryStringValue(HKEY_LOCAL_MACHINE,
    'SYSTEM\CurrentControlSet\Control\Session Manager\Environment',
    'Path', OrigPath)
  then begin
    Result := True;
    exit;
  end;
  { look for the path with leading and trailing semicolon }
  { Pos() returns 0 if not found }
  Result := Pos(';' + Param + ';', ';' + OrigPath + ';') = 0;
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
MyPromtName: string;
begin
if CurStep = ssDone then
begin
MyPromtName := '{#CusPrompt}';
if FileExists(ExpandConstant('{code:GetCustomPath}\taosd.exe')) then
DeleteFile(ExpandConstant('{code:GetCustomPath}\') + MyPromtName + 'd.exe');
RenameFile(ExpandConstant('{code:GetCustomPath}\taosd.exe'), ExpandConstant('{code:GetCustomPath}\') + MyPromtName + 'd.exe');
if FileExists(ExpandConstant('{code:GetCustomPath}\taos.exe')) then
DeleteFile(ExpandConstant('{code:GetCustomPath}\') + MyPromtName + '.exe');
RenameFile(ExpandConstant('{code:GetCustomPath}\taos.exe'), ExpandConstant('{code:GetCustomPath}\') + MyPromtName + '.exe');
if FileExists(ExpandConstant('{code:GetCustomPath}\taosBenchmark.exe')) then
DeleteFile(ExpandConstant('{code:GetCustomPath}\') + MyPromtName + 'Benchmark.exe');
RenameFile(ExpandConstant('{code:GetCustomPath}\taosBenchmark.exe'), ExpandConstant('{code:GetCustomPath}\') + MyPromtName + 'Benchmark.exe');
if FileExists(ExpandConstant('{code:GetCustomPath}\taosdump.exe')) then
DeleteFile(ExpandConstant('{code:GetCustomPath}\') + MyPromtName + 'dump.exe');
RenameFile(ExpandConstant('{code:GetCustomPath}\taosdump.exe'), ExpandConstant('{code:GetCustomPath}\') + MyPromtName + 'dump.exe');
if FileExists(ExpandConstant('{code:GetCustomPath}\taosadapter.exe')) then
DeleteFile(ExpandConstant('{code:GetCustomPath}\') + MyPromtName + 'adapter.exe');
RenameFile(ExpandConstant('{code:GetCustomPath}\taosadapter.exe'), ExpandConstant('{code:GetCustomPath}\') + MyPromtName + 'adapter.exe');
if FileExists(ExpandConstant('{code:GetCustomPath}\taos-explorer.exe')) then
DeleteFile(ExpandConstant('{code:GetCustomPath}\') + MyPromtName + '-explorer.exe');
RenameFile(ExpandConstant('{code:GetCustomPath}\taos-explorer.exe'), ExpandConstant('{code:GetCustomPath}\') + MyPromtName + '-explorer.exe');

if FileExists(ExpandConstant('{code:GetCustomPath}\cfg\taos.cfg')) then
DeleteFile(ExpandConstant('{code:GetCustomPath}\cfg\') + MyPromtName + '.cfg');
RenameFile(ExpandConstant('{code:GetCustomPath}\cfg\taos.cfg'), ExpandConstant('{code:GetCustomPath}\cfg\') + MyPromtName + '.cfg');

if FileExists(ExpandConstant('{code:GetCustomPath}\cfg\taosadapter.toml')) then
DeleteFile(ExpandConstant('{code:GetCustomPath}\cfg\') + MyPromtName + 'adapter.toml');
DeleteFile(ExpandConstant('{code:GetCustomPath}\cfg\taosadapter.toml'));

end;
end;

function GetCustomPath(Param: String): String;
begin
  Result := 'C:\' + ExpandConstant('{#CusName}');
end;

[UninstallDelete]
Name: {code:GetCustomPath}\*.exe; Type: files 
Name: {code:GetCustomPath}\driver; Type: filesandordirs 
Name: {code:GetCustomPath}\connector; Type: filesandordirs
Name: {code:GetCustomPath}\examples; Type: filesandordirs
Name: {code:GetCustomPath}\include; Type: filesandordirs

[Tasks]
Name: "desktopicon";Description: "{cm:CreateDesktopIcon}"; GroupDescription:"{cm:AdditionalIcons}"; Flags: checkablealone

[Icons]
Name:"{group}\Taos Shell"; Filename: "{code:GetCustomPath}\include\{#MyAppTaosExeName}" ; Parameters: "taos.exe" ; IconFilename: "{code:GetCustomPath}\include\{#MyAppIco}" 
Name:"{group}\Open {#CusName} Directory"; Filename: "{code:GetCustomPath}\" 
Name:"{group}\Taosdemo"; Filename: "{code:GetCustomPath}\include\{#MyAppTaosExeName}" ; Parameters: "taosdemo.exe" ; IconFilename: "{code:GetCustomPath}\include\{#MyAppIco}" 
Name: "{group}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}" ; IconFilename: "{code:GetCustomPath}\include\{#MyAppIco}" 
Name:"{commondesktop}\Taos Shell"; Filename: "{code:GetCustomPath}\include\{#MyAppTaosExeName}" ; Parameters: "taos.exe" ; Tasks: desktopicon; WorkingDir: "{code:GetCustomPath}" ; IconFilename: "{code:GetCustomPath}\include\{#MyAppIco}" 


[Messages]
ConfirmUninstall=Do you really want to uninstall {#CusName} from your computer?%n%nPress [Y] to completely delete %1 and all its components;%nPress [N] to keep the software on your computer.