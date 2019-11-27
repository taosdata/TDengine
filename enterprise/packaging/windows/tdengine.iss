#define MyAppName "TDengine"
#define MyAppPublisher "taosdata"
#define MyAppURL "http://www.taosdata.com/"
#define MyAppBeforeInstallTxt "windows_before_install.txt"
#define MyAppAfterInstallText "windows_after_install.txt"
#define MyAppIco "favicon.ico"
#define MyAppInstallDir "C:\TDengine"
#define MyAppOutputDir "./"
#define MyAppSourceDir "C:\TDengine"
#define MyAppAllFile "\*"
#define MyAppDLLName "\driver\taos.dll"
#define MyAppVersion "1.6"
#define MyAppInstallName "TDengine-enterprise-windows-client-x64-1.6.4.0"

[Setup]
VersionInfoVersion={#MyAppVersion}
AppId={{A0F7A93C-79C4-485D-B2B8-F0D03DF42FAB}
AppName={#MyAppName}
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
; InfoAfterFile={#MyAppAfterInstallText}
OutputDir={#MyAppOutputDir}
OutputBaseFilename={#MyAppInstallName}
SetupIconFile={#MyAppIco}
Compression=lzma
SolidCompression=yes
DisableDirPage=yes
ArchitecturesInstallIn64BitMode=x64

[Languages]
Name: "chinesesimp"; MessagesFile: "compiler:Default.isl"
Name: "english"; MessagesFile: "compiler:Languages\English.isl"

[Files]
Source: {#MyAppSourceDir}{#MyAppAllFile}; DestDir: "{app}"; Flags: igNoreversion recursesubdirs createallsubdirs 
Source: {#MyAppSourceDir}{#MyAppDLLName}; DestDir: "{win}\System32"; Flags: igNoreversion; 

[Icons]
Name: "{group}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}"

