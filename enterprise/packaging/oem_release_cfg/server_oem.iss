#define OPCGdbaInstallPath "c:\\Windows\SysWOW64"
#define MyAppName "TDengine"
#define MyAppPublisher "taosdata"
#define MyAppURL "http://www.taosdata.com/"
#define MyAppBeforeInstallTxt "..\windows\windows_before_install.txt"
#define MyAppIco "favicon.ico"
;#define MyAppInstallDir "C:\TDengine"
#define MyAppOutputDir "./"
#define MyAppSourceDir "C:\TDengine"
;#define MyAppAllFile "\*"
#define MyAppCfgName "\cfg\*"
#define MyAppDriverName "\driver\*"
#define MyAppConnectorName "\connector\*"
#define MyAppExamplesName "\examples\*"
#define MyAppIncludeName "\include\*"
#define MyAppPluginsName "\plugins\*"
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
;SetupIconFile={#MyAppIco}
Compression=lzma
CloseApplications=force
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
Source: ..\windows\taos.bat; DestDir: "{app}\include"; Flags: igNoreversion;
Source: {#CusPrompt}.ico; DestDir: "{app}\include"; Flags: igNoreversion;
Source: ..\windows\start-all.bat; DestDir: "{app}"; Flags: igNoreversion;
Source: ..\windows\stop-all.bat; DestDir: "{app}"; Flags: igNoreversion;
Source: {#MyAppSourceDir}\{#CusPrompt}.exe; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs; BeforeInstall: TaskKill('{#CusPrompt}.exe')
Source: {#MyAppSourceDir}\{#CusPrompt}Benchmark.exe; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs; BeforeInstall: TaskKill('{#CusPrompt}Benchmark.exe')
Source: {#MyAppSourceDir}\{#CusPrompt}dump.exe; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs; BeforeInstall: TaskKill('{#CusPrompt}dump.exe')
Source: {#MyAppSourceDir}\{#CusPrompt}d.exe; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs; BeforeInstall: TaskKill('{#CusPrompt}d.exe')
Source: {#MyAppSourceDir}\{#CusPrompt}adapter.exe; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs; BeforeInstall: TaskKill('{#CusPrompt}adapter.exe')
Source: {#MyAppSourceDir}\{#CusPrompt}keeper.exe; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs; BeforeInstall: TaskKill('{#CusPrompt}keeper.exe')
Source: {#MyAppSourceDir}\{#CusPrompt}x.exe; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs; BeforeInstall: TaskKill('{#CusPrompt}x.exe')
Source: {#MyAppSourceDir}\{#CusPrompt}-explorer.exe; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs; BeforeInstall: TaskKill('{#CusPrompt}-explorer.exe')
Source: {#MyAppSourceDir}{#MyAppDLLName}; DestDir: "{win}\System32"; Flags: igNoreversion recursesubdirs createallsubdirs 64bit;Check:IsWin64;
Source: {#MyAppSourceDir}\append\opc_gdba_32\*; DestDir: "{#OPCGdbaInstallPath}\"; Flags: uninsneveruninstall onlyifdoesntexist skipifsourcedoesntexist; Check: ShouldInstallOPC
Source: {#MyAppSourceDir}{#MyAppCfgName}; DestDir: "{app}\cfg"; Flags: igNoreversion recursesubdirs createallsubdirs onlyifdoesntexist uninsneveruninstall
Source: {#MyAppSourceDir}{#MyAppDriverName}; DestDir: "{app}\driver"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppConnectorName}; DestDir: "{app}\connector"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppExamplesName}; DestDir: "{app}\examples"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppIncludeName}; DestDir: "{app}\include"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}\plugins\*; DestDir: "{app}\plugins"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}\{#CusPrompt}_odbc\*; DestDir: "{app}\{#CusPrompt}_odbc"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppExeName}; DestDir: "{app}"; Excludes: {#MyAppExcludeSource} ; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}{#MyAppTaosdemoExeName}; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}\*.dll; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs
Source: {#MyAppSourceDir}\*.xml; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs

[Components]
Name: "component"; Description: "OPC DLL(OPC Data Access Auto Interface)              http://www.gray-box.net/daawrapper.php?lang=en";

[run]
Filename: {sys}\sc.exe; Parameters: "create {#CusPrompt}d start= AUTO binPath= ""{app}\\{#CusPrompt}d.exe --win_service""" ; Flags: runhidden
Filename: {sys}\sc.exe; Parameters: "create {#CusPrompt}adapter start= AUTO binPath= ""{app}\\{#CusPrompt}adapter.exe""" ; Flags: runhidden
Filename: {sys}\sc.exe; Parameters: "create {#CusPrompt}keeper start= AUTO binPath= ""{app}\\{#CusPrompt}keeper.exe""" ; Flags: runhidden
Filename: "{cmd}"; Parameters: "/c ""echo monitorFqdn %computername% >> {app}\\cfg\\{#CusPrompt}.cfg""" ; Flags: runhidden
Filename: "{app}\{#CusPrompt}x-srv.exe"; Parameters: "install"; Flags: runhidden; Check: FileExists(ExpandConstant('{app}\{#CusPrompt}x-srv.exe'))
Filename: "{app}\{#CusPrompt}-explorer-srv.exe"; Parameters: "install" ; Flags: runhidden; Check: FileExists(ExpandConstant('{app}\{#CusPrompt}-explorer-srv.exe'))
Filename: "C:\Windows\SysWOW64\regsvr32.exe"; Parameters: " ""{#OPCGdbaInstallPath}\gbda_aut.dll"" /s"; Flags: RunHidden WaitUntilTerminated; Check: ShouldInstallOPC
Filename: "C:\Windows\SysWOW64\regsvr32.exe"; Parameters: " ""{#OPCGdbaInstallPath}\gbhda_aw.dll"" /s"; Flags: RunHidden WaitUntilTerminated; Check: ShouldInstallOPC
Filename: "C:\Windows\System32\odbcconf.exe"; Parameters: " /S /F win_odbc_install.ini"; WorkingDir: "{app}\{#CusPrompt}_odbc\x64"; Flags: runhidden; StatusMsg: "Configuring ODBC x64"
Filename: "C:\Windows\SysWOW64\odbcconf.exe"; Parameters: " /S /F win_odbc_install.ini"; WorkingDir: "{app}\{#CusPrompt}_odbc\x86"; Flags: runhidden; StatusMsg: "Configuring ODBC x86"


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


procedure TaskKill(FileName: String);
var
  ResultCode: Integer;
begin
  if (FileName = '{#CusPrompt}d.exe') or (FileName = '{#CusPrompt}adapter.exe') or (FileName = '{#CusPrompt}-explorer.exe')  or (FileName = '{#CusPrompt}x.exe')  or (FileName = '{#CusPrompt}keeper.exe') then
  begin
    Exec('sc.exe', ' stop ' + FileName, '', SW_HIDE, ewWaitUntilTerminated, ResultCode); 
    Exec('sc.exe', ' delete ' + FileName, '', SW_HIDE, ewWaitUntilTerminated, ResultCode);  
  end;

  Exec('taskkill.exe', '/f /im ' + '"' + FileName + '"', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;


function IsVC2015x64Installed(): Boolean;
var
  InstallKey: String;
begin
  InstallKey := 'SOFTWARE\Classes\Installer\Dependencies\VC,redist.x64,amd64,14.40,bundle';
  Result := RegKeyExists(HKEY_LOCAL_MACHINE, InstallKey)
end;

function InitializeSetup(): Boolean;
begin
  Result :=True
  if not IsVC2015x64Installed() then  
  begin
    MsgBox('Please install Visual C++ Redistributable 2015-2022 (x64) before install TDengine', mbInformation, MB_OK);
    Result :=False
  end;
end;

var
  OutputMsgCheckJava: TOutputMsgMemoWizardPage;
  InputQueryPage: TInputQueryWizardPage;
  OutputMsgCheckPISDK: TOutputMsgMemoWizardPage;
  JavaVersionString: String;
  PISDKVersionString: string;
  JavaReady: Boolean;
  OPCInstallFileFlag: Boolean;
  ExplorerAddInput: string;
  CustomFinishedLabel: TLabel;
  CustomFinishedLabel1: TLabel;
  CustomFinishedLabel2: TLabel;
  CustomFinishedLabel3: TLabel;


function ReplaceLineInFile(FileName, SearchText, ReplaceText: String): Boolean;
var
  Lines: TArrayOfString;
  I, PosSearch, CommentSearch, FqdnSearch: Integer;
  Found: Boolean;
begin
  Result := False;
  if LoadStringsFromFile(FileName, Lines) then
  begin
    Found := False;
    for I := 0 to GetArrayLength(Lines) - 1 do
    begin
      PosSearch := Pos(SearchText, Lines[I]);
      if PosSearch > 0 then
      begin
        Delete(Lines[I], PosSearch, Length(SearchText));
        Insert(ReplaceText, Lines[I], PosSearch);
        
        CommentSearch := Pos('#', Lines[I]);
        FqdnSearch := Pos('fqdn', Lines[I]);
        if CommentSearch > 0 then
          if FqdnSearch > 0 then
            begin
              Delete(Lines[I], CommentSearch, 1);
            end;
        Found := True;
      end;
    end;

    if Found then
    begin
      Result := SaveStringsToFile(FileName, Lines, False);
    end;
  end;
end;

function NextButtonClick(CurPageID: Integer): Boolean;
begin
  //if CurPageID = InputDirWizardPage.ID then begin
  //    WizardForm.DirEdit.Text := InputDirWizardPage.Values[0] + '/{#SubDirectory}';
  //    WizardForm.DirEdit.Update;
  //end;
  if CurPageID = InputQueryPage.ID then
  begin
    ExplorerAddInput := InputQueryPage.Values[0];
    begin
      ReplaceLineInFile(ExpandConstant('{app}\cfg\') + 'explorer.toml', 'localhost', ExplorerAddInput);
      ReplaceLineInFile(ExpandConstant('{app}\cfg\') + '{#CusPrompt}x.toml', 'localhost', ExplorerAddInput);
    end;
  end;
  Result := True;
end;

procedure MyBeforeInstall(filename: string);
var
  SourceFile: string;
  DestFile: string;
  NewDestFile: string;
begin
  SourceFile := '{#MyAppSourceDir}\cfg\' + filename;
  DestFile := ExpandConstant('{app}\cfg') + '\' + filename;
  if FileExists(SourceFile) then
    if FileExists(DestFile) then
      begin
        NewDestFile := ExpandConstant('{app}\cfg') + '\' + filename + '.new';
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

  if not ExecAsOriginalUser('cmd.exe', '/c {#CusPrompt}x-pi.exe -pv >> "'+ OutputFile + '" 2>&1', ExpandConstant('{app}\plugins\pi'), SW_HIDE, ewWaitUntilTerminated, ResultCode) then
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

procedure ExtendFinishedPageControl(Control: TControl);
begin
  Control.Left := Control.Left - WizardForm.WizardBitmapImage2.Width;
  Control.Width := Control.Width + WizardForm.WizardBitmapImage2.Width;
end;

procedure InitializeWizard;
var
  InstallPath: String;
  ComputerName: String;
  AfterID: Integer;
begin
  AfterID := wpSelectTasks;
  AfterID := wpInstalling;
  JavaVersionString := GetJavaVersionDesc();
  OutputMsgCheckJava := CreateOutputMsgMemoPage(AfterID, 'Check Java for influxdb/opentsdb Connector', 'The InfluxDB/OpenTSDB connector depends on the Java environment.'
  + ' If you use this connector, please make sure to install the required version.', 'Java 1.8+ required', JavaVersionString);
  AfterID := OutputMsgCheckJava.ID;

  InputQueryPage := CreateInputQueryPage(AfterID, 'Config Page', '', 'Set publicly accessible IP address or domain name you want expose to.');
  ComputerName := GetComputerNameString();
  InputQueryPage.Add('&Default: ' + ComputerName, False);
  InputQueryPage.Values[0] := ComputerName;
  AfterID := InputQueryPage.ID;

  InstallPath := WizardDirValue();
  WizardForm.FinishedLabel.Visible := False;
  WizardForm.WizardBitmapImage2.Visible := False;
  ExtendFinishedPageControl(WizardForm.RunList);
  ExtendFinishedPageControl(WizardForm.NoRadio);
  ExtendFinishedPageControl(WizardForm.YesRadio);
  ExtendFinishedPageControl(WizardForm.FinishedLabel);
  ExtendFinishedPageControl(WizardForm.FinishedHeadingLabel);

  CustomFinishedLabel := TLabel.Create(WizardForm);  
  CustomFinishedLabel.Parent := WizardForm.FinishedPage;  
  CustomFinishedLabel.Left := WizardForm.FinishedHeadingLabel.Left;  
  CustomFinishedLabel.Top := WizardForm.FinishedHeadingLabel.Top + WizardForm.FinishedHeadingLabel.Height + ScaleY(8); // Adjust the top position as needed  

  CustomFinishedLabel.WordWrap := True;
  CustomFinishedLabel.Width := WizardForm.FinishedHeadingLabel.Width; 
  
  CustomFinishedLabel.Caption := 'You can use following instructions to edit configuration files and run commands manually in terminal as Administrator:';

  CustomFinishedLabel1 := TLabel.Create(WizardForm);  
  CustomFinishedLabel1.Parent := WizardForm.FinishedPage;  
  CustomFinishedLabel1.Left := WizardForm.FinishedHeadingLabel.Left;
  CustomFinishedLabel1.Top := CustomFinishedLabel.Top + CustomFinishedLabel.Height;
  CustomFinishedLabel1.Width := WizardForm.FinishedHeadingLabel.Width div 2; 
  CustomFinishedLabel1.Height := ScaleY(120);
  CustomFinishedLabel1.Caption := #13#10 + ''
  + #13#10 + 'To configure {#CusName}:'
  + #13#10 + 'To configure {#CusPrompt}adapter:'
  + #13#10 + 'To configure {#CusPrompt}-explorer:   '
  + #13#10 + 'To start {#CusPrompt}d:' 
  + #13#10 + 'To start {#CusPrompt}adapter:' 
  + #13#10 + 'To start {#CusPrompt}keeper:' 
  + #13#10 + 'To start {#CusPrompt}x:' 
  + #13#10 + 'To start {#CusPrompt}-explorer:';


  CustomFinishedLabel2 := TLabel.Create(WizardForm);  
  CustomFinishedLabel2.Parent := WizardForm.FinishedPage;  
  CustomFinishedLabel2.Left := CustomFinishedLabel1.Left + CustomFinishedLabel1.Width;
  CustomFinishedLabel2.Top := CustomFinishedLabel.Top + CustomFinishedLabel.Height;
  CustomFinishedLabel2.Width := WizardForm.FinishedHeadingLabel.Width div 2; 
  CustomFinishedLabel2.Height := ScaleY(120);
  CustomFinishedLabel2.Caption := #13#10 + ''
  + #13#10 + 'edit ' + InstallPath + '\cfg\{#CusPrompt}.cfg'
  + #13#10 + 'edit ' + InstallPath + '\cfg\{#CusPrompt}adapter.toml'
  + #13#10 + 'edit ' + InstallPath + '\cfg\explorer.toml'
  + #13#10 + 'sc.exe start {#CusPrompt}d' 
  + #13#10 + 'sc.exe start {#CusPrompt}sadapter' 
  + #13#10 + 'sc.exe start {#CusPrompt}keeper' 
  + #13#10 + 'sc.exe start {#CusPrompt}x' 
  + #13#10 + 'sc.exe start {#CusPrompt}-explorer';

  CustomFinishedLabel3 := TLabel.Create(WizardForm);  
  CustomFinishedLabel3.Parent := WizardForm.FinishedPage;  
  CustomFinishedLabel3.Left := WizardForm.FinishedHeadingLabel.Left;
  CustomFinishedLabel3.Top := CustomFinishedLabel1.Top + CustomFinishedLabel1.Height + ScaleY(8);
  CustomFinishedLabel3.Width := WizardForm.FinishedHeadingLabel.Width; 
  CustomFinishedLabel3.Caption := 'To use all TDengine services, please run start-all.bat under ' + InstallPath + ' directory';

end;

procedure CurStepChanged(CurStep: TSetupStep);
var
    ResultCode: Integer;
begin  
  if CurStep = ssPostInstall then
  begin
    // Call the ODBC configuration procedure after installation
    Exec('C:\Windows\SysWOW64\odbcconf.exe', ExpandConstant('/S /F {app}\{#CusPrompt}_odbc\x86\win_odbcinst.ini'), '', SW_HIDE, ewWaitUntilTerminated, ResultCode)    
  end;
  if CurStep = ssDone then  
  begin
    Exec('C:\Windows\System32\odbcconf.exe', ExpandConstant('/S /F {app}\{#CusPrompt}_odbc\x64\win_odbcinst.ini'), '', SW_HIDE, ewWaitUntilTerminated, ResultCode)
  end;
end;

procedure CurPageChanged(CurPageID: Integer);
var
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

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  case CurUninstallStep of
    usPostUninstall:
      begin
        if FileExists(ExpandConstant('{app}\{#CusPrompt}d.exe')) then
          begin            
            DelayDeleteFile(ExpandConstant('{app}\{#CusPrompt}d.exe'), 10);
          end;
        
        if FileExists(ExpandConstant('{app}\{#CusPrompt}x.exe')) then
          begin            
            DelayDeleteFile(ExpandConstant('{app}\{#CusPrompt}x.exe'), 5);
          end;

        if FileExists(ExpandConstant('{app}\.{#CusPrompt}_history')) then
          begin            
            DelayDeleteFile(ExpandConstant('{app}\.{#CusPrompt}_history'), 5);
          end;

        if FileExists(ExpandConstant('{app}\output.txt')) then
          begin            
            DelayDeleteFile(ExpandConstant('{app}\output.txt'), 5);
          end;

        if MsgBox('Please confirm if you would like to delete cfg, data and log directory ?', mbConfirmation, MB_YESNO or MB_DEFBUTTON2) = IDYES then
          begin
            if DirExists(ExpandConstant('{app}\cfg')) then
              begin
                DelTree(ExpandConstant('{app}\cfg'), True, True, True);
              end;
            if DirExists(ExpandConstant('{app}\log')) then  
              begin
                DelTree(ExpandConstant('{app}\log'), True, True, True);
              end;
            if DirExists(ExpandConstant('{app}\data')) then  
              begin
                DelTree(ExpandConstant('{app}\data'), True, True, True);
              end;
          end;        
      end;    
  end;
end;

function ToUpperCase(str: String): String;
begin
  Result := UpperCase(str);
end;

function DeleteOdbcDsnRegistry: Boolean;
var
  prefix: String;
begin
  prefix := ToUpperCase('{#CusPrompt}');
  RegDeleteKeyIncludingSubkeys(HKCU, 'SOFTWARE\ODBC\ODBC.INI\' + prefix + '_ODBC_DSN');  
  RegDeleteKeyIncludingSubkeys(HKCU, 'SOFTWARE\ODBC\ODBC.INI\' + prefix + '_ODBC_WS_DSN')
  RegDeleteValue(HKCU, 'SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources', prefix + '_ODBC_DSN'); 
  RegDeleteValue(HKCU, 'SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources', prefix + '_ODBC_WS_DSN'); 
     
  Result := True;
end;

function DeleteOdbcDriverRegistry: Boolean;
var
  prefix: String;
begin
  // Delete 64-bit ODBC driver registry 
  prefix := ToUpperCase('{#CusPrompt}');
  RegDeleteKeyIncludingSubkeys(HKLM64, 'SOFTWARE\ODBC\ODBCINST.INI\' + prefix + '_ODBC_DRIVER');    
  RegDeleteValue(HKLM64, 'SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers', prefix + '_ODBC_DRIVER');

  // Delete 32-bit ODBC driver registry 
  RegDeleteKeyIncludingSubkeys(HKLM64, 'SOFTWARE\Wow6432Node\ODBC\ODBCINST.INI\' + prefix + '_ODBC_DRIVER');
  RegDeleteValue(HKLM64, 'SOFTWARE\Wow6432Node\ODBC\ODBCINST.INI\ODBC Drivers', prefix + '_ODBC_DRIVER');

  Result := True;
end;

procedure DeinitializeUninstall();
begin
	DeleteOdbcDsnRegistry();
	DeleteOdbcDriverRegistry();
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
Name: {app}\plugins\pi; Type: filesandordirs 
Name: {app}\plugins\opc; Type: filesandordirs 
Name: {app}\plugins\mqtt; Type: filesandordirs 
Name: {app}\plugins\influxdb; Type: filesandordirs
Name: {app}\plugins\opentsdb; Type: filesandordirs

[UninstallRun]
RunOnceId: "stopall"; Filename: {app}\stop-all.bat; Flags: runhidden
RunOnceId: "stop{#CusPrompt}keeper"; Filename: {sys}\sc.exe; Parameters: "stop {#CusPrompt}keeper" ; Flags: runhidden
RunOnceId: "del{#CusPrompt}keeper"; Filename: {sys}\sc.exe; Parameters: "delete {#CusPrompt}keeper" ; Flags: runhidden
RunOnceId: "stop{#CusPrompt}adapter"; Filename: {sys}\sc.exe; Parameters: "stop {#CusPrompt}adapter" ; Flags: runhidden
RunOnceId: "del{#CusPrompt}adapter"; Filename: {sys}\sc.exe; Parameters: "delete {#CusPrompt}adapter" ; Flags: runhidden
RunOnceId: "stop{#CusPrompt}x"; Filename: {sys}\sc.exe; Parameters: "stop {#CusPrompt}x" ; Flags: runhidden
RunOnceId: "stop{#CusPrompt}-explorer"; Filename: {sys}\sc.exe; Parameters: "stop {#CusPrompt}-explorer" ; Flags: runhidden
RunOnceId: "del{#CusPrompt}x"; Filename: "{app}\\{#CusPrompt}-srv.exe"; Parameters: "uninstall" ; Flags: runhidden
RunOnceId: "del{#CusPrompt}x-agent"; Filename: "{app}\\{#CusPrompt}-agent-srv.exe"; Parameters: "uninstall" ; Flags: runhidden
RunOnceId: "del{#CusPrompt}-explorer"; Filename: "{app}\\{#CusPrompt}-explorer-srv.exe"; Parameters: "uninstall" ; Flags: runhidden
RunOnceId: "stop{#CusPrompt}d"; Filename: {sys}\sc.exe; Parameters: "stop {#CusPrompt}d" ; Flags: runhidden
RunOnceId: "del{#CusPrompt}d"; Filename: {sys}\sc.exe; Parameters: "delete {#CusPrompt}d" ; Flags: runhidden
RunOnceId: "taskkill{#CusPrompt}"; Filename: "taskkill"; Parameters: "/im ""{#CusPrompt}.exe"" /f"; Flags: runhidden
RunOnceId: "taskkill{#CusPrompt}d"; Filename: "taskkill"; Parameters: "/im ""{#CusPrompt}d.exe"" /f"; Flags: runhidden
RunOnceId: "taskkill{#CusPrompt}adapter"; Filename: "taskkill"; Parameters: "/im ""{#CusPrompt}adapter.exe"" /f"; Flags: runhidden
RunOnceId: "taskkill{#CusPrompt}keeper"; Filename: "taskkill"; Parameters: "/im ""{#CusPrompt}keeper.exe"" /f"; Flags: runhidden
RunOnceId: "taskkill{#CusPrompt}x"; Filename: "taskkill"; Parameters: "/im ""{#CusPrompt}x.exe"" /f"; Flags: runhidden
RunOnceId: "taskkill{#CusPrompt}-explorer"; Filename: "taskkill"; Parameters: "/im ""{#CusPrompt}-explorer.exe"" /f"; Flags: runhidden

RunOnceId: "uninstall"; Filename: "{uninstallexe}"; Parameters: "/SILENT"; Check: fileexists('{uninstallexe}')
RunOnceId: "removeopc1"; Filename: "C:\Windows\SysWOW64\regsvr32.exe"; Parameters: " /u ""{app}\plugins\opc\gbda_aut.dll"" /s"; Flags: RunHidden WaitUntilTerminated; Check: ShouldInstallOPC
RunOnceId: "removeopc2"; Filename: "C:\Windows\SysWOW64\regsvr32.exe"; Parameters: " /u ""{app}\plugins\opc\gbhda_aw.dll"" /s"; Flags: RunHidden WaitUntilTerminated; Check: ShouldInstallOPC

[Tasks]
Name: "desktopicon";Description: "{cm:CreateDesktopIcon}"; GroupDescription:"{cm:AdditionalIcons}"; Flags: checkablealone

[Icons]
Name:"{group}\{#CusPrompt} Shell"; Filename: "{app}\include\{#MyAppTaosExeName}" ; Parameters: "{#CusPrompt}.exe" ; IconFilename: "{app}\include\{#MyAppIco}" 
Name:"{group}\Open {#CusName} Directory"; Filename: "{app}\" 
Name: "{group}\Uninstall {#MyAppName}"; Filename: "{uninstallexe}" ; IconFilename: "{app}\include\{#MyAppIco}" 
Name:"{commondesktop}\{#CusPrompt} Shell"; Filename: "{app}\include\{#MyAppTaosExeName}" ; Parameters: "{#CusPrompt}.exe" ; Tasks: desktopicon; WorkingDir: "{app}" ; IconFilename: "{app}\include\{#MyAppIco}" 


[Messages]
ConfirmUninstall=Do you really want to uninstall {#CusName} from your computer?%n%nPress [Y] to completely delete %1 and all its components;%nPress [N] to keep the software on your computer.
