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
#define MyAppDLLName "\*.dll"
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
DisableDirPage=no
UsePreviousAppDir=yes
Uninstallable=yes
ArchitecturesAllowed=x64
ArchitecturesInstallIn64BitMode=x64

[Languages]
Name: "chinesesimp"; MessagesFile: "compiler:Default.isl"
;Name: "english"; MessagesFile: "compiler:Languages\English.isl"

[Files]
#ifexist "..\..\..\taos-internal\packaging\vc_redist_files.issinc"
#include "..\..\..\taos-internal\packaging\vc_redist_files.issinc"
#else
#include "..\..\..\enterprise\packaging\vc_redist_files.issinc"
#endif
;Source: {#MyAppSourceDir}{#MyAppAllFile}; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs 
Source: taos.bat; DestDir: "{app}\include"; Flags: igNoreversion;
;Source: taosdemo.png; DestDir: "{app}\include"; Flags: igNoreversion;
;Source: taosShell.png; DestDir: "{app}\include"; Flags: igNoreversion;
Source: favicon.ico; DestDir: "{app}\include"; Flags: igNoreversion;
Source: {#MyAppSourceDir}{#MyAppDLLName}; DestDir: "{win}\System32"; Flags: igNoreversion recursesubdirs createallsubdirs 64bit;Check:IsWin64;
Source: {#MyAppSourceDir}{#MyAppCfgName}; DestDir: "{app}\cfg"; Flags: igNoreversion recursesubdirs createallsubdirs onlyifdoesntexist uninsneveruninstall
Source: {#MyAppSourceDir}{#MyAppDriverName}; DestDir: "{app}\driver"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppDLLName}; DestDir: "{app}\driver"; Flags: igNoreversion
Source: {#MyAppSourceDir}\taos_odbc\*; DestDir: "{app}\taos_odbc"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}\*.dll; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs
;Source: {#MyAppSourceDir}{#MyAppConnectorName}; DestDir: "{app}\connector"; Flags: igNoreversion recursesubdirs createallsubdirs
;Source: {#MyAppSourceDir}{#MyAppExamplesName}; DestDir: "{app}\examples"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppIncludeName}; DestDir: "{app}\include"; Excludes: "taos.bat"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppExeName}; DestDir: "{app}"; Excludes: {#MyAppExcludeSource} ; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppTaosdemoExeName}; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}\taos.exe; DestDir: "{app}"; DestName: "{#CusPrompt}.exe"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}\taosBenchmark.exe; DestDir: "{app}"; DestName: "{#CusPrompt}Benchmark.exe"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}\taosdump.exe; DestDir: "{app}"; DestName: "{#CusPrompt}dump.exe"; Flags: igNoreversion recursesubdirs createallsubdirs


[run]
Filename: "C:\Windows\System32\odbcconf.exe"; Parameters: "/S /F win_odbc_install.ini"; WorkingDir: "{app}\taos_odbc\x64"; Flags: runhidden; StatusMsg: "Configuring ODBC x64"
Filename: "C:\Windows\SysWOW64\odbcconf.exe"; Parameters: "/S /F win_odbc_install.ini"; WorkingDir: "{app}\taos_odbc\x86"; Flags: runhidden; StatusMsg: "Configuring ODBC x86"

[Registry]
Root: HKLM; Subkey: "SYSTEM\CurrentControlSet\Control\Session Manager\Environment"; \
    ValueType: expandsz; ValueName: "Path"; ValueData: "{olddata};{app}"; \
    Check: NeedsAddPath(ExpandConstant('{app}'))

[Code]
#ifexist "..\..\..\taos-internal\packaging\vc_redist_code.issinc"
#include "..\..\..\taos-internal\packaging\vc_redist_code.issinc"
#else
#include "..\..\..\enterprise\packaging\vc_redist_code.issinc"
#endif

function InitializeSetup(): Boolean;
begin
  Result := EnsureVCRuntime();
end;
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

function RemovePath(Param: string): Boolean;
var
  OrigPath: string;
  NewPath: string;
begin
  Result := True;
  if not RegQueryStringValue(HKEY_LOCAL_MACHINE,
    'SYSTEM\CurrentControlSet\Control\Session Manager\Environment',
    'Path', OrigPath)
  then begin
    exit;
  end;

  NewPath := ';' + OrigPath + ';';
  while StringChangeEx(NewPath, ';' + Param + ';', ';', True) > 0 do
  begin
  end;
  while StringChangeEx(NewPath, ';;', ';', True) > 0 do
  begin
  end;
  if (Length(NewPath) > 0) and (Copy(NewPath, 1, 1) = ';') then
    Delete(NewPath, 1, 1);
  if (Length(NewPath) > 0) and (Copy(NewPath, Length(NewPath), 1) = ';') then
    Delete(NewPath, Length(NewPath), 1);

  if NewPath <> OrigPath then
    Result := RegWriteExpandStringValue(HKEY_LOCAL_MACHINE,
      'SYSTEM\CurrentControlSet\Control\Session Manager\Environment',
      'Path', NewPath);
end;

var
  ExistingInstallDir: String;

function GetExistingInstallDir: String;
var
  AppPath: String;
begin
  Result := '';
  if RegQueryStringValue(HKEY_LOCAL_MACHINE, 'SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{A0F7A93C-79C4-485D-B2B8-F0D03DF42FAB}_is1', 'Inno Setup: App Path', AppPath) and DirExists(AppPath) then
    Result := RemoveBackslashUnlessRoot(AppPath);
end;

function NormalizeInstallDir(InstallDir: string): string;
begin
  InstallDir := RemoveBackslashUnlessRoot(InstallDir);
  if CompareText(ExtractFileName(InstallDir), 'TDengine') = 0 then
    Result := InstallDir
  else
    Result := AddBackslash(InstallDir) + 'TDengine';
end;

procedure NormalizeInstallDirectory;
begin
  if WizardForm.DirEdit.Text <> '' then
    WizardForm.DirEdit.Text := NormalizeInstallDir(WizardForm.DirEdit.Text);
end;

function NextButtonClick(CurPageID: Integer): Boolean;
begin
  if CurPageID = wpSelectDir then
    NormalizeInstallDirectory;
  Result := True;
end;

function PrepareToInstall(var NeedsRestart: Boolean): String;
begin
  NormalizeInstallDirectory;
  if (ExistingInstallDir <> '') and (CompareText(WizardDirValue(), ExistingInstallDir) <> 0) then
  begin
    Result := 'An existing TDengine installation was found at ' + ExistingInstallDir + '.' + #13#10 + #13#10 + 'To prevent PATH and configuration conflicts, this upgrade must use the existing installation directory.';
    exit;
  end;
  if ExistingInstallDir <> '' then WizardForm.DirEdit.Text := ExistingInstallDir;
  Result := '';
end;

procedure InitializeWizard;
begin
  ExistingInstallDir := GetExistingInstallDir();
end;

procedure CurPageChanged(CurPageID: Integer);
begin
  if (CurPageID = wpSelectDir) and (ExistingInstallDir <> '') then
  begin
    WizardForm.DirEdit.Text := ExistingInstallDir;
    WizardForm.DirEdit.Enabled := False;
    WizardForm.DirBrowseButton.Enabled := False;
  end;
end;

function DeleteOdbcDsnRegistry: Boolean;
begin
  RegDeleteKeyIncludingSubkeys(HKCU, 'SOFTWARE\ODBC\ODBC.INI\TAOS_ODBC_DSN');  
  RegDeleteKeyIncludingSubkeys(HKCU, 'SOFTWARE\ODBC\ODBC.INI\TAOS_ODBC_WS_DSN')

  RegDeleteValue(HKCU, 'SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources', 'TAOS_ODBC_DSN'); 
  RegDeleteValue(HKCU, 'SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources', 'TAOS_ODBC_WS_DSN'); 

  Result := True;
end;

function DeleteOdbcDriverRegistry: Boolean;
begin
  // Delete 64-bit ODBC driver registry 
  RegDeleteKeyIncludingSubkeys(HKLM64, 'SOFTWARE\ODBC\ODBCINST.INI\TAOS_ODBC_DRIVER');    
  RegDeleteValue(HKLM64, 'SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers', 'TAOS_ODBC_DRIVER');

  // Delete 32-bit ODBC driver registry 
  RegDeleteKeyIncludingSubkeys(HKLM64, 'SOFTWARE\Wow6432Node\ODBC\ODBCINST.INI\TAOS_ODBC_DRIVER');
  RegDeleteValue(HKLM64, 'SOFTWARE\Wow6432Node\ODBC\ODBCINST.INI\ODBC Drivers', 'TAOS_ODBC_DRIVER');

  Result := True;
end;


procedure DeinitializeUninstall();
begin
	DeleteOdbcDsnRegistry();
	DeleteOdbcDriverRegistry();
  RemovePath(ExpandConstant('{app}'));
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
SelectDirLabel3=If the selected path does not end with TDengine, Setup creates a TDengine subdirectory there.
ConfirmUninstall=Do you really want to uninstall {#CusName} from your computer?%n%nPress [Y] to completely delete %1 and all its components;%nPress [N] to keep the software on your computer.
