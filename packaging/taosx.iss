#define MyAppIco "favicon.ico"
#define OPCGdbaInstallPath "c:\\Windows\SysWOW64"

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
Source: "{#MyAppSourceDir}\bin\*"; DestDir: "{app}"
Source: "{#MyAppSourceDir}\config\taosx.toml"; DestDir: "{app}\cfg"; Flags: uninsneveruninstall onlyifdoesntexist skipifsourcedoesntexist; BeforeInstall: MyBeforeInstall('taosx.toml');
Source: "{#MyAppSourceDir}\config\agent.toml"; DestDir: "{app}\cfg"; Flags: uninsneveruninstall onlyifdoesntexist skipifsourcedoesntexist; BeforeInstall: MyBeforeInstall('agent.toml');
Source: "{#MyAppSourceDir}\config\explorer.toml"; DestDir: "{app}\cfg"; Flags: uninsneveruninstall onlyifdoesntexist skipifsourcedoesntexist; BeforeInstall: MyBeforeInstall('exploerer.toml');
Source: "{#MyAppSourceDir}\append\opc_gdba_32\*"; DestDir: "{#OPCGdbaInstallPath}\"; Flags: uninsneveruninstall onlyifdoesntexist skipifsourcedoesntexist; Check: ShouldInstallOPC

[Components]
Name: "component"; Description: "OPC DLL(OPC Data Access Auto Interface)              http://www.gray-box.net/daawrapper.php?lang=en"; Types: full;

[run]
Filename: "{app}\\taosx-srv.exe"; Parameters: "install"; Flags: runhidden; Check: FileExists(ExpandConstant('{app}\taosx-srv.exe'))
Filename: "{app}\\taosx-agent-srv.exe"; Parameters: "install" ; Flags: runhidden; Check: FileExists(ExpandConstant('{app}\taosx-agent-srv.exe'))
Filename: "{app}\\taos-explorer-srv.exe"; Parameters: "install" ; Flags: runhidden; Check: FileExists(ExpandConstant('{app}\taos-explorer-srv.exe'))
Filename: REG.exe; Parameters: "ADD ""HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{#AppName}_is1"" /V ""UninstallString""  \
  /T ""REG_SZ"" /D ""\""{app}\uninstall_{#AppName}.exe\"""" /F"; StatusMsg: Installing {#AppName}...; Flags: RunHidden WaitUntilTerminated
Filename: REG.exe; Parameters: "ADD ""HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{#AppName}_is1"" /V ""QuietUninstallString"" \
  /T ""REG_SZ"" /D ""\""{app}\uninstall_{#AppName}.exe\"" /SILENT"" /F"; StatusMsg: Installing {#AppName}...; Flags: RunHidden WaitUntilTerminated
Filename: "C:\Windows\SysWOW64\regsvr32.exe"; Parameters: " ""{#OPCGdbaInstallPath}\gbda_aut.dll"" /s"; Flags: RunHidden WaitUntilTerminated; Check: ShouldInstallOPC
Filename: "C:\Windows\SysWOW64\regsvr32.exe"; Parameters: " ""{#OPCGdbaInstallPath}\gbhda_aw.dll"" /s"; Flags: RunHidden WaitUntilTerminated; Check: ShouldInstallOPC

[UninstallRun]
RunOnceId: "stoptaosx"; Filename: {sys}\sc.exe; Parameters: "stop taosx" ; Flags: runhidden
RunOnceId: "stoptaosx-agent"; Filename: {sys}\sc.exe; Parameters: "stop taosx-agent" ; Flags: runhidden
RunOnceId: "stoptaos-explorer"; Filename: {sys}\sc.exe; Parameters: "stop taos-explorer" ; Flags: runhidden
RunOnceId: "deltaosx"; Filename: "{app}\\taosx-srv.exe"; Parameters: "uninstall" ; Flags: runhidden
RunOnceId: "deltaosx-agent"; Filename: "{app}\\taosx-agent-srv.exe"; Parameters: "uninstall" ; Flags: runhidden
RunOnceId: "deltaos-explorer"; Filename: "{app}\\taos-explorer-srv.exe"; Parameters: "uninstall" ; Flags: runhidden

[CODE]
var
  OutputMsgCheckJava: TOutputMsgMemoWizardPage;
  InputQueryPage: TInputQueryWizardPage;
  OutputMsgCheckPISDK: TOutputMsgMemoWizardPage;
  JavaVersionString: String;
  PISDKVersionString: string;
  JavaReady: Boolean;
  OPCInstallFileFlag: Boolean;
  ExplorerAddInput: string;

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

function ReplaceLineInFile(FileName, SearchText, ReplaceText: String): Boolean;
var
  Lines: TArrayOfString;
  I, PosSearch: Integer;
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
  if '{#AppName}' = 'taosx' then
  begin
    if CurPageID = InputQueryPage.ID then
    begin
      ExplorerAddInput := InputQueryPage.Values[0];
      begin
        ReplaceLineInFile(ExpandConstant('{app}\cfg\') + 'explorer.toml', 'localhost', ExplorerAddInput);
      end;
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
  SourceFile := '{#MyAppSourceDir}\config\' + filename;
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
  
  if '{#AppName}' = 'taosx' then
  begin
    InputQueryPage := CreateInputQueryPage(AfterID, 'Config Page', '', 'Set publicly accessible IP address or domain name you want expose to.');
    InputQueryPage.Add('&Default:localhost', False);
    InputQueryPage.Values[0] := 'localhost';
    AfterID := InputQueryPage.ID;
  end;
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

function RemoveQuotes(S: String): String;
begin
  // Remove the first and last character if they are quotes
  if (Length(S) > 0) and (S[1] = '"') then
    Delete(S, 1, 1);
  if (Length(S) > 0) and (S[Length(S)] = '"') then
    Delete(S, Length(S), 1);
  Result := S;
end;

function GetValueFromTOMLFile(FileName: String; Key: String): String;
var
  Lines: TArrayOfString;
  Line: String;
  i: Integer;
begin
  Result := '';

  // Load the lines of the file into a string array
  if LoadStringsFromFile(FileName, Lines) then
  begin
    // Loop through each line
    for i := 0 to GetArrayLength(Lines) - 1 do
    begin
      Line := Trim(Lines[i]);

      // Skip comment lines and empty lines
      if (Line = '') or (Pos('#', Line) = 1) then
        Continue;

      // If the line starts with the key, extract the value
      if Pos(Key + ' = ', Line) = 1 then
      begin
        // Extract the value
        Result := Copy(Line, Length(Key) + 4, Length(Line) - Length(Key) - 4);
        Result := Trim(Result); // Remove extra spaces
        Result := RemoveQuotes(Result); // Remove surrounding quotes
        Exit;
      end;
    end;
  end;
end;


procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  logDir: string;
begin
  case CurUninstallStep of
    usPostUninstall:
      begin
        if MsgBox('Please confirm if you would like to delete cfg and log directory ?', mbConfirmation, MB_YESNO or MB_DEFBUTTON2) = IDYES then
          begin
            if DirExists(ExpandConstant('{app}\log')) then
              begin
                DelTree(ExpandConstant('{app}\log'), True, True, True);
              end;
            if DirExists(ExpandConstant('{app}\cfg')) then
              begin
                logDir := GetValueFromTOMLFile(ExpandConstant('{app}\cfg\agent.toml'), 'path');
                if (logDir <> '') and (DirExists(logDir)) then
                  begin
                    DelTree(logDir, True, True, True);
                  end;
                DelTree(ExpandConstant('{app}\cfg'), True, True, True);
              end;
          end;
      end;
  end;
end;


function ShouldInstallOPC: Boolean;
begin
  Result := OPCInstallFileFlag;
end;

[UninstallDelete]
Type: files; Name: "{app}\plugins\pi\*.*"
Type: files; Name: "{app}\plugins\opc\*.*"
Type: files; Name: "{app}\plugins\mqtt\*.*"
Type: files; Name: "{app}\plugins\influxdb\*.*"
Type: files; Name: "{app}\plugins\opentsdb\*.*"
Type: dirifempty; Name: "{app}\plugins\pi";
Type: dirifempty; Name: "{app}\plugins\opc";
Type: dirifempty; Name: "{app}\plugins\mqtt";
Type: dirifempty; Name: "{app}\plugins\influxdb";
Type: dirifempty; Name: "{app}\plugins\opentsdb";

[UninstallRun]
Filename: "{app}\uninstall_{#AppName}.exe"; Parameters: "/SILENT"; Check: fileexists('{app}\uninstall_{#AppName}.exe')
Filename: "C:\Windows\SysWOW64\regsvr32.exe"; Parameters: " /u ""{app}\plugins\opc\gbda_aut.dll"" /s"; Flags: RunHidden WaitUntilTerminated; Check: ShouldInstallOPC
Filename: "C:\Windows\SysWOW64\regsvr32.exe"; Parameters: " /u ""{app}\plugins\opc\gbhda_aw.dll"" /s"; Flags: RunHidden WaitUntilTerminated; Check: ShouldInstallOPC

[Messages]
ConfirmUninstall=Do you really want to uninstall from your computer?%n%nPress [Y] to completely delete %1 and all its components;%nPress [N] to keep the software on your computer.
