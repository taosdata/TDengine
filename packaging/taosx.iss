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
Source: "{#MyAppSourceDir}\xplugins\*"; DestDir: "{app}\xplugins"; Flags: recursesubdirs
Source: "{#MyAppSourceDir}\bin\taosx.exe"; DestDir: "{app}\bin"

[CODE]
procedure CurStepChanged(CurStep: TSetupStep);
var
uninspath, uninsname, NewUninsName, MyAppName: string;
begin
if CurStep = ssDone then
begin
NewUninsName := 'uninstall_taox';
MyAppName := '{#TaosXName}';
uninspath := ExtractFilePath(ExpandConstant('{uninstallexe}'));
uninsname := Copy(ExtractFileName(ExpandConstant('{uninstallexe}')), 1, 8);
RenameFile(uninspath + uninsname + '.exe', uninspath + NewUninsName + '.exe');
RenameFile(uninspath + uninsname + '.dat', uninspath + NewUninsName + '.dat');
end;
end;

[UninstallDelete]
Type: files; Name: "{app}\bin\taosx.exe"
Type: files; Name: "{app}\plugins\pi\*.*"
Type: files; Name: "{app}\plugins\opc\*.*"
Type: dirifempty; Name: "{app}\plugins\pi";
Type: dirifempty; Name: "{app}\plugins\opc";

[UninstallRun]
Filename: "{app}\uninstall.exe"; Parameters: "/SILENT"; Check: fileexists('{app}\uninstall.exe')

[Messages]
ConfirmUninstall=Do you really want to uninstall from your computer?%n%nPress [Y] to completely delete %1 and all its components;%nPress [N] to keep the software on your computer.
