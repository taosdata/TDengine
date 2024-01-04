#define MyAppName "TDengine"
#define MyAppPublisher "taosdata"
#define MyAppURL "http://www.taosdata.com/"
#define MyAppBeforeInstallTxt "windows_before_install.txt"
#define MyAppIco "favicon.ico"
#define MyAppInstallDir "C:\TDengine"
#define MyAppOutputDir "./"
#define MyAppSourceDir "C:\TDengine"
;#define MyAppAllFile "\*"
#define MyAppCfgName "\cfg\*"
#define MyAppDriverName "\driver\*"
#define MyAppConnectorName "\connector\*"
#define MyAppExamplesName "\examples\*"
#define MyAppIncludeName "\include\*"
#define MyAppExeName "\*.exe"
#define MyAppTaosExeName "\taos.bat"
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
ArchitecturesAllowed=x64
ArchitecturesInstallIn64BitMode=x64

[Languages]
Name: "chinesesimp"; MessagesFile: "compiler:Default.isl"
;Name: "english"; MessagesFile: "compiler:Languages\English.isl"

[Files]
;Source: {#MyAppSourceDir}{#MyAppAllFile}; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs 
Source: taos.bat; DestDir: "{app}\include"; Flags: igNoreversion;
;Source: taosdemo.png; DestDir: "{app}\include"; Flags: igNoreversion;
;Source: taosShell.png; DestDir: "{app}\include"; Flags: igNoreversion;
Source: favicon.ico; DestDir: "{app}\include"; Flags: igNoreversion;
Source: {#MyAppSourceDir}{#MyAppDLLName}; DestDir: "{win}\System32"; Flags: igNoreversion recursesubdirs createallsubdirs 64bit;Check:IsWin64;
Source: {#MyAppSourceDir}{#MyAppCfgName}; DestDir: "{app}\cfg"; Flags: igNoreversion recursesubdirs createallsubdirs onlyifdoesntexist uninsneveruninstall
Source: {#MyAppSourceDir}{#MyAppDriverName}; DestDir: "{app}\driver"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}\taos_odbc\*; DestDir: "{app}\taos_odbc\"; Flags: igNoreversion recursesubdirs createallsubdirs
;Source: {#MyAppSourceDir}{#MyAppConnectorName}; DestDir: "{app}\connector"; Flags: igNoreversion recursesubdirs createallsubdirs
;Source: {#MyAppSourceDir}{#MyAppExamplesName}; DestDir: "{app}\examples"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppIncludeName}; DestDir: "{app}\include"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppExeName}; DestDir: "{app}"; Excludes: {#MyAppExcludeSource} ; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppTaosdemoExeName}; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}\taos.exe; DestDir: "{app}"; DestName: "{#CusPrompt}.exe"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}\taosBenchmark.exe; DestDir: "{app}"; DestName: "{#CusPrompt}Benchmark.exe"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}\taosdump.exe; DestDir: "{app}"; DestName: "{#CusPrompt}dump.exe"; Flags: igNoreversion recursesubdirs createallsubdirs


[run]
Filename: {sys}\sc.exe; Parameters: "create taosd start= DEMAND binPath= ""C:\\TDengine\\taosd.exe --win_service""" ; Flags: runhidden
Filename: {sys}\sc.exe; Parameters: "create taosadapter start= DEMAND binPath= ""C:\\TDengine\\taosadapter.exe""" ; Flags: runhidden
Filename: "{cmd}"; Parameters: "/c odbcconf /F ""C:\TDengine\taos_odbc\win_odbcinst.in"""; WorkingDir: "{app}"; Flags: runhidden; StatusMsg: "Configuring ODBC"

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

function DeleteOdbcDsnRegistry: Boolean;
var
  Names: TArrayOfString;
  I: Integer;
  Value: String;
begin
  if RegGetValueNames(HKCU64, 'SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources', Names) then
  begin
    for I := 0 to GetArrayLength(Names) - 1 do
    begin
      if RegQueryStringValue(HKCU64, 'SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources', Names[I], Value) then
      begin
        if Value = 'TDengine' then
        begin
          RegDeleteKeyIncludingSubkeys(HKCU64, 'SOFTWARE\ODBC\ODBC.INI\' + Names[I]);
          RegDeleteValue(HKCU64, 'SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources\',  Names[I]);
        end;
      end;
    end;
  end;
  Result := True;
end;

function DeleteOdbcDriverRegistry: Boolean;
begin
  RegDeleteKeyIncludingSubkeys(HKLM64, 'SOFTWARE\ODBC\ODBCINST.INI\TDengine');
  RegDeleteValue(HKLM64, 'SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers', 'TDengine');
  Result := True;
end;


procedure DeinitializeUninstall();
begin
	DeleteOdbcDsnRegistry();
	DeleteOdbcDriverRegistry();
end;

[UninstallDelete]
Name: {app}\driver; Type: filesandordirs 
Name: {app}\connector; Type: filesandordirs
Name: {app}\examples; Type: filesandordirs
Name: {app}\include; Type: filesandordirs

[Tasks]
Name: "desktopicon";Description: "{cm:CreateDesktopIcon}"; GroupDescription:"{cm:AdditionalIcons}"; Flags: checkablealone

[Icons]
Name:"{group}\Taos Shell"; Filename: "{app}\include\{#MyAppTaosExeName}" ; Parameters: "taos.exe" ; IconFilename: "{app}\include\{#MyAppIco}" 
Name:"{group}\Open {#CusName} Directory"; Filename: "{app}\" 
Name:"{group}\Taosdemo"; Filename: "{app}\include\{#MyAppTaosExeName}" ; Parameters: "taosdemo.exe" ; IconFilename: "{app}\include\{#MyAppIco}" 
Name: "{group}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}" ; IconFilename: "{app}\include\{#MyAppIco}" 
Name:"{commondesktop}\Taos Shell"; Filename: "{app}\include\{#MyAppTaosExeName}" ; Parameters: "taos.exe" ; Tasks: desktopicon; WorkingDir: "{app}" ; IconFilename: "{app}\include\{#MyAppIco}" 


[Messages]
ConfirmUninstall=Do you really want to uninstall {#CusName} from your computer?%n%nPress [Y] to completely delete %1 and all its components;%nPress [N] to keep the software on your computer.
