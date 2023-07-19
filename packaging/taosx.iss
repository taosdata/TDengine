#define MyAppIco "favicon.ico"

[Setup]
AppName={#AppName}
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
Source: "{#MyAppSourceDir}\bin\*"; DestDir: "{app}\bin"
Source: "{#MyAppSourceDir}\config\agent.toml"; DestDir: "{app}\config"; Flags: uninsneveruninstall onlyifdoesntexist; BeforeInstall: MyBeforeInstall('agent.toml');
Source: "{#MyAppSourceDir}\config\explorer.toml"; DestDir: "{app}\config"; Flags: uninsneveruninstall onlyifdoesntexist skipifsourcedoesntexist; BeforeInstall: MyBeforeInstall('exploerer.toml');

[run]
Filename: "{app}\\bin\\taosx-srv.exe"; Parameters: "install"; Flags: runhidden; Check: FileExists(ExpandConstant('{app}\bin\taosx-srv.exe'))
Filename: "{app}\\bin\\taosx-agent-srv.exe"; Parameters: "install" ; Flags: runhidden; Check: FileExists(ExpandConstant('{app}\bin\taosx-agent-srv.exe'))
Filename: "{app}\\bin\\taos-explorer-srv.exe"; Parameters: "install" ; Flags: runhidden; Check: FileExists(ExpandConstant('{app}\bin\taos-explorer-srv.exe'))
Filename: REG.exe; Parameters: "ADD ""HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{#AppName}_is1"" /V ""UninstallString""  \
  /T ""REG_SZ"" /D ""\""{app}\uninstall_{#AppName}.exe\"""" /F"; StatusMsg: Installing {#AppName}...; Flags: RunHidden WaitUntilTerminated
Filename: REG.exe; Parameters: "ADD ""HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{#AppName}_is1"" /V ""QuietUninstallString"" \
  /T ""REG_SZ"" /D ""\""{app}\uninstall_{#AppName}.exe\"" /SILENT"" /F"; StatusMsg: Installing {#AppName}...; Flags: RunHidden WaitUntilTerminated

[UninstallRun]
RunOnceId: "stoptaosx"; Filename: {sys}\sc.exe; Parameters: "stop taosx" ; Flags: runhidden
RunOnceId: "stoptaosx-agent"; Filename: {sys}\sc.exe; Parameters: "stop taosx-agent" ; Flags: runhidden
RunOnceId: "stoptaos-explorer"; Filename: {sys}\sc.exe; Parameters: "stop taos-explorer" ; Flags: runhidden
RunOnceId: "deltaosx"; Filename: "{app}\\bin\\taosx-srv.exe"; Parameters: "uninstall" ; Flags: runhidden
RunOnceId: "deltaosx-agent"; Filename: "{app}\\bin\\taosx-agent-srv.exe"; Parameters: "uninstall" ; Flags: runhidden
RunOnceId: "deltaos-explorer"; Filename: "{app}\\bin\\taos-explorer-srv.exe"; Parameters: "uninstall" ; Flags: runhidden

[CODE]
var
  OutputMsgCheckJava: TOutputMsgMemoWizardPage;
  OutputMsgCheckPISDK: TOutputMsgMemoWizardPage;
  JavaVersionString: String;
  PISDKVersionString: string;
  JavaReady: Boolean;

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
  // 分割版本号字符串为主要版本号和次要版本号
  tokens := TStringList.Create;
  try
    tokens.StrictDelimiter := True;
    tokens.Delimiter := '.';
    tokens.DelimitedText := version;
    if tokens.Count < 2 then
    begin
      Result := False; // 版本号格式不正确，返回 False
      Exit;
    end;
    major := StrToIntDef(tokens[0], -1);
    minor := StrToIntDef(tokens[1], -1);
    if (major > 1) or ((major = 1) and (minor >= 8)) then
    begin
      Result := True; // 版本号大于等于 1.8，返回 True
      Exit;
    end;
    Result := False; // 版本号小于 1.8，返回 False
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
      JavaVersionString := 'JAVA 1.8+ required' + #13#10 + JavaVersion + ' has been installed.' + #13#10 + 'Please update version.';
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
  AfterID := wpInstalling;
  JavaVersionString := GetJavaVersionDesc();
  OutputMsgCheckJava := CreateOutputMsgMemoPage(AfterID, 'Check Java for influxdb Connector', 'The InfluxDB connector depends on the Java environment.'
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
end;


[UninstallDelete]
Type: files; Name: "{app}\xplugins\pi\*.*"
Type: files; Name: "{app}\xplugins\opc\*.*"
Type: files; Name: "{app}\xplugins\mqtt\*.*"
Type: files; Name: "{app}\xplugins\influxdb\*.*"
Type: dirifempty; Name: "{app}\xplugins\pi";
Type: dirifempty; Name: "{app}\xplugins\opc";
Type: dirifempty; Name: "{app}\xplugins\mqtt";
Type: dirifempty; Name: "{app}\xplugins\influxdb";

[UninstallRun]
Filename: "{app}\uninstall_{#AppName}.exe"; Parameters: "/SILENT"; Check: fileexists('{app}\uninstall_{#AppName}.exe')

[Messages]
ConfirmUninstall=Do you really want to uninstall from your computer?%n%nPress [Y] to completely delete %1 and all its components;%nPress [N] to keep the software on your computer.
