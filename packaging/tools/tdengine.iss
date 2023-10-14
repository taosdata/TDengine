#define MyAppName "TDengine"
#define OPCGdbaInstallPath "c:\\Windows\SysWOW64"
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
;Source: {#MyAppSourceDir}{#MyAppConnectorName}; DestDir: "{app}\connector"; Flags: igNoreversion recursesubdirs createallsubdirs
;Source: {#MyAppSourceDir}{#MyAppExamplesName}; DestDir: "{app}\examples"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppIncludeName}; DestDir: "{app}\include"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppExeName}; DestDir: "{app}"; Excludes: {#MyAppExcludeSource} ; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppTaosdemoExeName}; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}\taos.exe; DestDir: "{app}"; DestName: "{#CusPrompt}.exe"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}\taosBenchmark.exe; DestDir: "{app}"; DestName: "{#CusPrompt}Benchmark.exe"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}\taosdump.exe; DestDir: "{app}"; DestName: "{#CusPrompt}dump.exe"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: "{#MyAppSourceDir}\plugins\*"; DestDir: "{app}\plugins"; Flags: recursesubdirs
Source: "{#MyAppSourceDir}\bin\*"; DestDir: "{app}\bin"
Source: "{#MyAppSourceDir}\config\agent.toml"; DestDir: "{app}\config"; Flags: uninsneveruninstall onlyifdoesntexist; BeforeInstall: MyBeforeInstall('agent.toml');
Source: "{#MyAppSourceDir}\config\explorer.toml"; DestDir: "{app}\config"; Flags: uninsneveruninstall onlyifdoesntexist skipifsourcedoesntexist; BeforeInstall: MyBeforeInstall('exploerer.toml');
Source: "{#MyAppSourceDir}\append\opc_gdba_32\*"; DestDir: "{#OPCGdbaInstallPath}\"; Flags: uninsneveruninstall onlyifdoesntexist skipifsourcedoesntexist; Check: ShouldInstallOPC

[Components]
Name: "component"; Description: "OPC DLL(OPC Data Access Auto Interface)              http://www.gray-box.net/daawrapper.php?lang=en";

[run]
Filename: {sys}\sc.exe; Parameters: "create taosd start= DEMAND binPath= ""C:\\TDengine\\taosd.exe --win_service""" ; Flags: runhidden
Filename: {sys}\sc.exe; Parameters: "create taosadapter start= DEMAND binPath= ""C:\\TDengine\\taosadapter.exe""" ; Flags: runhidden

Filename: "{app}\\bin\\taosx-srv.exe"; Parameters: "install"; Flags: runhidden; Check: FileExists(ExpandConstant('{app}\bin\taosx-srv.exe'))
Filename: "{app}\\bin\\taosx-agent-srv.exe"; Parameters: "install" ; Flags: runhidden; Check: FileExists(ExpandConstant('{app}\bin\taosx-agent-srv.exe'))
Filename: "{app}\\bin\\taos-explorer-srv.exe"; Parameters: "install" ; Flags: runhidden; Check: FileExists(ExpandConstant('{app}\bin\taos-explorer-srv.exe'))
Filename: REG.exe; Parameters: "ADD ""HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{#AppName}_is1"" /V ""UninstallString""  \
  /T ""REG_SZ"" /D ""\""{app}\uninstall_{#AppName}.exe\"""" /F"; StatusMsg: Installing {#AppName}...; Flags: RunHidden WaitUntilTerminated
Filename: REG.exe; Parameters: "ADD ""HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{#AppName}_is1"" /V ""QuietUninstallString"" \
  /T ""REG_SZ"" /D ""\""{app}\uninstall_{#AppName}.exe\"" /SILENT"" /F"; StatusMsg: Installing {#AppName}...; Flags: RunHidden WaitUntilTerminated
Filename: "C:\Windows\SysWOW64\regsvr32.exe"; Parameters: " ""{#OPCGdbaInstallPath}\gbda_aut.dll"" /s"; Flags: RunHidden WaitUntilTerminated; Check: ShouldInstallOPC
Filename: "C:\Windows\SysWOW64\regsvr32.exe"; Parameters: " ""{#OPCGdbaInstallPath}\gbhda_aw.dll"" /s"; Flags: RunHidden WaitUntilTerminated; Check: ShouldInstallOPC

[UninstallRun]
RunOnceId: "stoptaosd"; Filename: {sys}\sc.exe; Parameters: "stop taosd" ; Flags: runhidden
RunOnceId: "stoptaosadapter"; Filename: {sys}\sc.exe; Parameters: "stop taosadapter" ; Flags: runhidden
RunOnceId: "deltaosd"; Filename: {sys}\sc.exe; Parameters: "delete taosd" ; Flags: runhidden
RunOnceId: "deltaosadapter"; Filename: {sys}\sc.exe; Parameters: "delete taosadapter" ; Flags: runhidden

RunOnceId: "stoptaosx"; Filename: {sys}\sc.exe; Parameters: "stop taosx" ; Flags: runhidden
RunOnceId: "stoptaosx-agent"; Filename: {sys}\sc.exe; Parameters: "stop taosx-agent" ; Flags: runhidden
RunOnceId: "stoptaos-explorer"; Filename: {sys}\sc.exe; Parameters: "stop taos-explorer" ; Flags: runhidden
RunOnceId: "deltaosx"; Filename: "{app}\\bin\\taosx-srv.exe"; Parameters: "uninstall" ; Flags: runhidden
RunOnceId: "deltaosx-agent"; Filename: "{app}\\bin\\taosx-agent-srv.exe"; Parameters: "uninstall" ; Flags: runhidden
RunOnceId: "deltaos-explorer"; Filename: "{app}\\bin\\taos-explorer-srv.exe"; Parameters: "uninstall" ; Flags: runhidden

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

var
  OutputMsgCheckJava: TOutputMsgMemoWizardPage;
  OutputMsgCheckPISDK: TOutputMsgMemoWizardPage;
  JavaVersionString: String;
  PISDKVersionString: string;
  JavaReady: Boolean;
  OPCInstallFileFlag: Boolean;

procedure CurStepChanged(CurStep: TSetupStep);
var
  uninspath, uninsname, NewUninsName : string;
begin
  if CurStep = ssDone then
  begin
    NewUninsName := 'uninstall_{#AppName}';
    uninspath := ExtractFilePath(ExpandConstant('{uninstallexe}'));
    uninsname := Copy(ExtractFileName(ExpandConstant('{uninstallexe}')), 1, 8);
    RenameFile(uninspath + uninsname + '.exe', uninspath + NewUninsName + '.exe');
    RenameFile(uninspath + uninsname + '.dat', uninspath + NewUninsName + '.dat');
  end;
end;

function NextButtonClick(CurPageID: Integer): Boolean;
begin
  //if CurPageID = InputDirWizardPage.ID then begin
  //    WizardForm.DirEdit.Text := InputDirWizardPage.Values[0] + '/{#SubDirectory}';
  //    WizardForm.DirEdit.Update;
  //end;
  Result := True;
end;

procedure MyBeforeInstall(filename: string);
var
  SourceFile: string;
  DestFile: string;
  NewDestFile: string;
begin
  SourceFile := '{#MyAppSourceDir}\config\' + filename;
  DestFile := ExpandConstant('{app}\config') + '\' + filename;
  if FileExists(SourceFile) then
    if FileExists(DestFile) then
      begin
        NewDestFile := ExpandConstant('{app}\config') + '\' + filename + '.new';
        if not FileCopy(SourceFile, NewDestFile, False) then
        begin
          MsgBox('Error copying file.', mbError, MB_OK);
        end;
      end;
end;

function CheckJavaVersion(version: string): Boolean;
var
  tokens: TStringList;
  major, minor: Integer;
begin
  // Split the version number string into a major version number and a minor version number.
  tokens := TStringList.Create;
  try
    tokens.StrictDelimiter := True;
    tokens.Delimiter := '.';
    tokens.DelimitedText := version;
    if tokens.Count < 2 then
    begin
      Result := False; // The version number format is incorrect, return false.
      Exit;
    end;
    major := StrToIntDef(tokens[0], -1);
    minor := StrToIntDef(tokens[1], -1);
    if (major > 1) or ((major = 1) and (minor >= 8)) then
    begin
      Result := True; // The version number is greater than or equal to 1.8, return True.
      Exit;
    end;
    Result := False; // The version number is less than 1.8, return False.
  finally
    tokens.Free;
  end;
end;

function GetJavaVersionDesc(): String;
var
  ResultCode: Integer;
  JavaVersion: String;
  OutputFile: string;
  OutputText: AnsiString;
  FileContent: TArrayOfString;
  StartIndex: Integer;
  EndIndex:   Integer;
begin
  Log('InitializeSetup called');
  OutputFile := ExpandConstant('{tmp}\java_version.txt');
  if not ExecAsOriginalUser('cmd.exe', '/c java -version >> "'+ OutputFile + '" 2>&1', '', SW_HIDE, ewWaitUntilTerminated, ResultCode) then
  begin
    JavaVersionString := 'JAVA 1.8+ required.' + #13#10 + 'No Java version found.';
  end
  else
  begin
    LoadStringsFromFile(OutputFile, FileContent);
    OutputText := FileContent[0];

    StartIndex := Pos('"', OutputText);
    EndIndex := Pos('"', Copy(OutputText, StartIndex+1, Length(OutputText)-StartIndex));
    JavaVersion := Copy(OutputText, StartIndex+1, EndIndex-1);
    JavaReady := CheckJavaVersion(JavaVersion)
    if JavaReady = True then begin
      JavaVersionString := 'JAVA 1.8+ required' + #13#10 + JavaVersion + ' has been installed.' + #13#10 + 'OK.';
    end else
      JavaVersionString := 'JAVA 1.8+ required' + #13#10 + 'No suitable version found.' + #13#10 + 'Please check it.';
  end;
  Result := JavaVersionString;
end;

function ContainsSubstringIgnoreCase(const str, substr: string): Boolean;
  begin
    Result := Pos(AnsiLowerCase(substr), AnsiLowerCase(str)) > 0;
  end;

function GetPISDKVersionDesc() : string;
var
  ResultCode: Integer;
  OutputFile: string;
  OutputText: AnsiString;
  FileContent: TArrayOfString; 
begin
  Log('InitializeSetup called');
  OutputFile := ExpandConstant('{tmp}\pisdk_version.txt');

  if not ExecAsOriginalUser('cmd.exe', '/c taosx-pi.exe -pv >> "'+ OutputFile + '" 2>&1', ExpandConstant('{app}\plugins\pi'), SW_HIDE, ewWaitUntilTerminated, ResultCode) then
  begin
    PISDKVersionString := 'WARNING' + #13#10 + 'PI SDK not found.';
  end
  else
  begin
    LoadStringsFromFile(OutputFile, FileContent);
    OutputText := FileContent[0];
    if ContainsSubstringIgnoreCase(OutputText, 'not found') then  begin
      PISDKVersionString := 'WARNING' + #13#10 + 'PI SDK not found.';
      end
    else begin
      PISDKVersionString := OutputText + #13#10 + 'PI SDK Found' + #13#10 + 'OK';
    end
  end;
  Result := PISDKVersionString;
end;

procedure InitializeWizard;
var
  AfterID: Integer;
begin
  AfterID := wpSelectTasks;
  AfterID := wpInstalling;
  JavaVersionString := GetJavaVersionDesc();
  OutputMsgCheckJava := CreateOutputMsgMemoPage(AfterID, 'Check Java for influxdb/opentsdb Connector', 'The InfluxDB/OpenTSDB connector depends on the Java environment.'
  + ' If you use this connector, please make sure to install the required version.', 'Java 1.8+ required', JavaVersionString);
  AfterID := OutputMsgCheckJava.ID;
end;

procedure CurPageChanged(CurPageID: Integer);
var
  InstallPath: string;
  AfterID: Integer;
begin
  if CurPageID = OutputMsgCheckJava.ID then
  begin
    AfterID := OutputMsgCheckJava.ID;
    if JavaReady = False then  begin
      MsgBox(JavaVersionString, mbInformation, MB_OK);
      end;

    GetPISDKVersionDesc();
    OutputMsgCheckPISDK := CreateOutputMsgMemoPage(AfterID, 'Check PI SDK for PI Connector', 'The PI connector depends on the PI SDK.'
    + ' If you use this connector, please make sure to install it.', 'PI SDK required', PISDKVersionString);
    AfterID := OutputMsgCheckPISDK.ID;
  end;
  if CurPageID = wpReady then
    begin
      if WizardForm.ComponentsList.Checked[0] then
        begin
          OPCInstallFileFlag := True;
        end;
      //if WizardForm.ComponentsList.Checked[1] then   // PI Connector
      //  begin
      //    MsgBox('PI Selected.', mbError, MB_OK);
      //  end;
    end;
end;

function ShouldInstallOPC: Boolean;
begin
  Result := OPCInstallFileFlag;
end;

[UninstallDelete]
Name: {app}\driver; Type: filesandordirs 
Name: {app}\connector; Type: filesandordirs
Name: {app}\examples; Type: filesandordirs
Name: {app}\include; Type: filesandordirs

Type: files; Name: "{app}\xplugins\pi\*.*"
Type: files; Name: "{app}\xplugins\opc\*.*"
Type: files; Name: "{app}\xplugins\mqtt\*.*"
Type: files; Name: "{app}\xplugins\influxdb\*.*"
Type: files; Name: "{app}\xplugins\opentsdb\*.*"
Type: dirifempty; Name: "{app}\xplugins\pi";
Type: dirifempty; Name: "{app}\xplugins\opc";
Type: dirifempty; Name: "{app}\xplugins\mqtt";
Type: dirifempty; Name: "{app}\xplugins\influxdb";
Type: dirifempty; Name: "{app}\xplugins\opentsdb";

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
