; TDGPT Windows Installer Script
; TDengine Analytics Node for Windows

#define MyAppName "TDGPT"
#define MyAppPublisher "taosdata"
#define MyAppURL "http://www.taosdata.com/"
; These will be replaced by the build script
#ifndef MyAppVersion
  #define MyAppVersion "3.3.0.0"
#endif
#ifndef MyAppInstallName
  #define MyAppInstallName "tdengine-tdgpt-oss-3.3.0.0-Windows-x64"
#endif

#define MyProductFullName "TDGPT - TDengine Analytics Node"
#define MyAppInstallDir "C:\TDengine\taosanode"

[Setup]
AppId={{A0F7A93C-79C4-485D-B2B8-F0D03DF42FAB}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
DefaultDirName={#MyAppInstallDir}
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
OutputDir=.\output
OutputBaseFilename={#MyAppInstallName}
Compression=lzma
SolidCompression=yes
CloseApplications=force
DisableDirPage=yes
Uninstallable=yes
ArchitecturesAllowed=x64
ArchitecturesInstallIn64BitMode=x64
SetupLogging=yes

[Languages]
Name: "chinesesimp"; MessagesFile: "compiler:Default.isl"

[Files]
; Configuration files - onlyifdoesntexist to preserve user configs
Source: "install\cfg\*"; DestDir: "{app}\cfg"; Flags: ignoreversion recursesubdirs createallsubdirs onlyifdoesntexist uninsneveruninstall

; Python library files
Source: "install\lib\*"; DestDir: "{app}\lib"; Flags: ignoreversion recursesubdirs createallsubdirs

; Resource files
Source: "install\resource\*"; DestDir: "{app}\resource"; Flags: ignoreversion recursesubdirs createallsubdirs; Check: DirExists(ExpandConstant('{app}\resource'))

; Model files
Source: "install\model\*"; DestDir: "{app}\model"; Flags: ignoreversion recursesubdirs createallsubdirs; Check: DirExists(ExpandConstant('{app}\model'))

; Requirements files
Source: "install\requirements*.txt"; DestDir: "{app}"; Flags: ignoreversion

; Batch scripts
Source: "install\bin\*.bat"; DestDir: "{app}\bin"; Flags: ignoreversion

[Dirs]
Name: "{app}\log"; Permissions: everyone-modify
Name: "{app}\data"; Permissions: everyone-modify
Name: "{app}\venv"; Permissions: everyone-modify

[Run]
; Install service after installation
Filename: "{sys}\sc.exe"; Parameters: "create taosanode start= delayed-auto binPath= ""{app}\bin\start-taosanode.bat"" displayname= ""TDGPT Taosanode Service"""; Flags: runhidden; StatusMsg: "Installing service..."
Filename: "{sys}\sc.exe"; Parameters: "description taosanode ""TDGPT Analytics Node Service"""; Flags: runhidden

[UninstallRun]
; Stop and remove service
Filename: "{sys}\sc.exe"; Parameters: "stop taosanode"; Flags: runhidden; RunOnceId: "stop_svc"
Filename: "{sys}\sc.exe"; Parameters: "delete taosanode"; Flags: runhidden; RunOnceId: "delete_svc"
Filename: "{sys}\taskkill.exe"; Parameters: "/f /im uwsgi.exe 2>nul"; Flags: runhidden; RunOnceId: "kill_uwsgi"
Filename: "{sys}\taskkill.exe"; Parameters: "/f /im python.exe 2>nul"; Flags: runhidden; RunOnceId: "kill_python"

[UninstallDelete]
Name: "{app}\log"; Type: filesandordirs
Name: "{app}\data"; Type: filesandordirs
Name: "{app}\venv"; Type: filesandordirs

[Icons]
Name: "{group}\Start Taosanode"; Filename: "{app}\bin\start-taosanode.bat"
Name: "{group}\Stop Taosanode"; Filename: "{app}\bin\stop-taosanode.bat"
Name: "{group}\Uninstall {#MyAppName}"; Filename: "{uninstallexe}"
Name: "{commondesktop}\Start Taosanode"; Filename: "{app}\bin\start-taosanode.bat"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: checkablealone

[Registry]
; Add to PATH environment variable
Root: HKLM; Subkey: "SYSTEM\CurrentControlSet\Control\Session Manager\Environment"; \
    ValueType: expandsz; ValueName: "Path"; ValueData: "{olddata};{app}\bin"; \
    Check: NeedsAddPath('{app}\bin')

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
  Result := Pos(';' + Param + ';', ';' + OrigPath + ';') = 0;
end;

function DirExists(const Dir: string): Boolean;
begin
  Result := DirectoryExists(Dir);
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  case CurUninstallStep of
    usPostUninstall:
      begin
        // Clean up any remaining files
        if FileExists(ExpandConstant('{app}\taosanode.pid')) then
          DeleteFile(ExpandConstant('{app}\taosanode.pid'));
      end;
  end;
end;

function InitializeSetup(): Boolean;
begin
  Result := True;
  
  // Check if Python is installed
  if not FileExists(ExpandConstant('{commonpf64}\Python39\python.exe')) and
     not FileExists(ExpandConstant('{commonpf}\Python39\python.exe')) and
     not FileExists(ExpandConstant('{localappdata}\Programs\Python\Python39\python.exe')) then
  begin
    MsgBox('Python 3.9+ is recommended for TDGPT. Please install Python and try again.', mbInformation, MB_OK);
    // Don't block installation, just warn
  end;
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  ConfigFile: string;
  Lines: TArrayOfString;
  i: Integer;
  Modified: Boolean;
begin
  if CurStep = ssPostInstall then
  begin
    // Update configuration file paths
    ConfigFile := ExpandConstant('{app}\cfg\taosanode.ini');
    if FileExists(ConfigFile) then
    begin
      if LoadStringsFromFile(ConfigFile, Lines) then
      begin
        Modified := False;
        for i := 0 to GetArrayLength(Lines) - 1 do
        begin
          // Replace placeholder paths with actual installation path
          if Pos('/usr/local/taos/taosanode', Lines[i]) > 0 then
          begin
            StringChangeEx(Lines[i], '/usr/local/taos/taosanode', ExpandConstant('{app}'), True);
            Modified := True;
          end;
          if Pos('/', Lines[i]) > 0 then
          begin
            // Convert forward slashes to backslashes for paths
            StringChangeEx(Lines[i], '/', '\', True);
          end;
        end;
        if Modified then
        begin
          SaveStringsToFile(ConfigFile, Lines, False);
        end;
      end;
    end;
    
    // Create log directory
    if not DirExists(ExpandConstant('{app}\log')) then
      CreateDir(ExpandConstant('{app}\log'));
    if not DirExists(ExpandConstant('{app}\data')) then
      CreateDir(ExpandConstant('{app}\data'));
  end;
end;
