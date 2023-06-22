#define MyAppIco "favicon.ico"
#define MyAppBeforeInstallTxt "info_before_install.txt"

#define MyAppSourceDir "C:\Program Files\taosX"
#define TaosXName "taosx"
#define MyAppVersion "1.0.1"


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
Source: "{#MyAppSourceDir}\bin\*"; DestDir: "{app}\bin"
Source: "{#MyAppSourceDir}\config\agent.toml"; DestDir: "{app}\config"; Flags: uninsneveruninstall onlyifdoesntexist; BeforeInstall: MyBeforeInstall

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
var
  OutputProgressWizardPage: TOutputProgressWizardPage;
  OutputMarqueeProgressWizardPage: TOutputMarqueeProgressWizardPage;
  OutputProgressWizardPagesAfterID: Integer;
  OutputMsgCheckJava: TOutputMsgMemoWizardPage;
  JavaVersionString: String;
  JavaReady: Boolean;

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

procedure MyBeforeInstall();
var
  SourceFile: string;
  DestFile: string;
  NewDestFile: string;
begin
  SourceFile := '{#MyAppSourceDir}\cfg\agent.toml';
  DestFile := ExpandConstant('{app}\cfg') + '\' + 'agent.toml';
  if not FileExists(SourceFile) then
    MsgBox('Source file "' + SourceFile + '" not found', mbError, MB_OK);
  if FileExists(DestFile) then
    NewDestFile := ExpandConstant('{app}\cfg') + '\' + 'agent.toml.new';
    if not FileCopy(SourceFile, NewDestFile, True) then
    begin
        MsgBox('Error copying file.', mbError, MB_OK);
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
end;

procedure InitializeWizard;
var
  AfterID: Integer;
begin
  AfterID := wpSelectTasks;
  JavaVersionString := GetJavaVersionDesc();
  OutputMsgCheckJava := CreateOutputMsgMemoPage(AfterID, 'Check Java for influxdb Connector', 'The InfluxDB connector depends on the Java environment.'
  + ' If you use this connector, please make sure to install the required version.', 'Java 1.8+ required', JavaVersionString);
  
  DNetVersionString := GetDNetVersionDesc();
  OutputMsgCheckDNet := CreateOutputMsgMemoPage(AfterID, 'Check .Net for PI Connector', 'The TD PI connector depends on the .Net environment.'
  + ' If you use this connector, please make sure to install the required version.', '.Net 1.8+ required', DNetVersionString);
  AfterID := OutputMsgCheckDNet.ID;
end;

procedure CurPageChanged(CurPageID: Integer);
begin
  if CurPageID = OutputMsgCheckJava.ID then
  begin
    if JavaReady = False then  begin
      MsgBox(JavaVersionString, mbInformation, MB_OK);
  end;
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

[UninstallRun]
Filename: "{app}\uninstall.exe"; Parameters: "/SILENT"; Check: fileexists('{app}\uninstall.exe')

[Messages]
ConfirmUninstall=Do you really want to uninstall from your computer?%n%nPress [Y] to completely delete %1 and all its components;%nPress [N] to keep the software on your computer.
