#!/usr/bin/env python3
"""
TDGPT Windows Packaging Script
Generate install package for Windows Platform using Inno Setup

Usage:
    python win_release.py -e <edition> -v <version> [-m <model_dir>] [-a]

Options:
    -e, --edition    : enterprise or community
    -v, --version    : tdgpt version number (e.g., 3.3.6.0)
    -m, --model-dir  : model files directory (optional)
    -a, --all-models : pack all models (optional)
    -o, --output     : output directory (default: D:\\tdgpt-release)
"""

import os
import sys
import argparse
import shutil
import logging
import datetime
import re
import subprocess
import tempfile
import zipfile

# Configure logging
timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)


class TDGPTVersion:
    def __init__(self, ver_type, version):
        self.ver_type = ver_type
        self.version = version


class InstallInfo:
    def __init__(self):
        self.source_dir = ""
        self.release_dir = ""
        self.install_dir = ""
        self.package_name = ""
        self.product_name = ""


tdgpt_version = TDGPTVersion("community", "1.0.0")
install_info = InstallInfo()


def parse_arguments():
    """Parse command line arguments"""
    global tdgpt_version, install_info
    
    parser = argparse.ArgumentParser(description='Release TDGPT on Windows')
    parser.add_argument('-e', '--edition', type=str, required=True,
                        help='Set edition type (enterprise or community)')
    parser.add_argument('-v', '--version', type=str, required=True,
                        help='Set version number (e.g., 3.3.6.0)')
    parser.add_argument('-m', '--model-dir', type=str, default="",
                        help='Set model files directory')
    parser.add_argument('-a', '--all-models', action='store_true',
                        help='Pack all models')
    parser.add_argument('-o', '--output', type=str, default="D:\\tdgpt-release",
                        help='Set output directory (default: D:\\tdgpt-release)')
    parser.add_argument('--iscc-path', type=str, 
                        default=r"C:\Program Files (x86)\Inno Setup 6\ISCC.exe",
                        help='Path to Inno Setup compiler')
    
    version_pattern = re.compile(r'^[0-9]+\.([0-9]+\.){1,3}[0-9]+$')
    args = parser.parse_args()
    
    # Validate version number
    if len(args.version) >= 16:
        logging.error("Failed! Length of the version number should be less than 16")
        sys.exit(1)
    elif version_pattern.match(args.version) is None:
        logging.error("Failed! Only digits and dots are allowed in the version number")
        sys.exit(1)
    
    # Validate edition
    if args.edition not in ["enterprise", "community"]:
        logging.error("Failed! Edition must be 'enterprise' or 'community'")
        sys.exit(1)
    
    tdgpt_version.ver_type = args.edition
    tdgpt_version.version = args.version
    
    # Set product name
    if args.edition == "enterprise":
        install_info.product_name = "tdengine-tdgpt-enterprise"
    else:
        install_info.product_name = "tdengine-tdgpt-oss"
    
    install_info.source_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    install_info.release_dir = args.output
    install_info.install_dir = os.path.join(install_info.release_dir, "install")
    install_info.package_name = f"{install_info.product_name}-{args.version}-Windows-x64"
    install_info.model_dir = args.model_dir
    install_info.all_models = args.all_models
    install_info.iscc_path = args.iscc_path
    
    return args


def print_params():
    """Print configuration parameters"""
    logging.info("=" * 60)
    logging.info("TDGPT Windows Packaging Configuration")
    logging.info("=" * 60)
    logging.info(f"Edition: {tdgpt_version.ver_type}")
    logging.info(f"Version: {tdgpt_version.version}")
    logging.info(f"Product Name: {install_info.product_name}")
    logging.info(f"Source Directory: {install_info.source_dir}")
    logging.info(f"Release Directory: {install_info.release_dir}")
    logging.info(f"Install Directory: {install_info.install_dir}")
    logging.info(f"Package Name: {install_info.package_name}")
    logging.info(f"Model Directory: {install_info.model_dir}")
    logging.info(f"All Models: {install_info.all_models}")
    logging.info("=" * 60)


def clean_release_dir():
    """Clean and recreate release directory"""
    if os.path.exists(install_info.release_dir):
        shutil.rmtree(install_info.release_dir)
        logging.info(f"Cleaned release directory: {install_info.release_dir}")
    
    os.makedirs(install_info.release_dir, exist_ok=True)
    os.makedirs(install_info.install_dir, exist_ok=True)
    logging.info(f"Created release directory: {install_info.release_dir}")


def copy_config_files():
    """Copy configuration files"""
    cfg_dir = os.path.join(install_info.install_dir, "cfg")
    os.makedirs(cfg_dir, exist_ok=True)
    
    source_cfg = os.path.join(install_info.source_dir, "cfg")
    
    # Copy taosanode.ini
    src_ini = os.path.join(source_cfg, "taosanode.ini")
    dst_ini = os.path.join(cfg_dir, "taosanode.ini")
    if os.path.exists(src_ini):
        shutil.copy2(src_ini, dst_ini)
        logging.info(f"Copied taosanode.ini")
        
        # Modify paths for Windows
        with open(dst_ini, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Replace Linux paths with Windows paths
        content = content.replace('/usr/local/taos/taosanode', 'C:\\TDengine\\taosanode')
        content = content.replace('/var/log/taos/taosanode', 'C:\\TDengine\\taosanode\\log')
        content = content.replace('/var/lib/taos/taosanode', 'C:\\TDengine\\taosanode')
        
        with open(dst_ini, 'w', encoding='utf-8') as f:
            f.write(content)
        logging.info(f"Updated paths in taosanode.ini for Windows")
    
    logging.info("Configuration files copied")


def copy_python_files():
    """Copy Python source files"""
    lib_dir = os.path.join(install_info.install_dir, "lib")
    os.makedirs(lib_dir, exist_ok=True)
    
    source_dir = install_info.source_dir
    
    # Copy taosanalytics directory
    src_analytics = os.path.join(source_dir, "taosanalytics")
    dst_analytics = os.path.join(lib_dir, "taosanalytics")
    
    if os.path.exists(dst_analytics):
        shutil.rmtree(dst_analytics)
    
    # Copy directory, excluding __pycache__ and .pyc files
    shutil.copytree(src_analytics, dst_analytics, 
                    ignore=shutil.ignore_patterns('__pycache__', '*.pyc', '.git*'))
    logging.info(f"Copied taosanalytics to {dst_analytics}")
    
    # Update version in __init__.py
    init_file = os.path.join(dst_analytics, "__init__.py")
    if os.path.exists(init_file):
        with open(init_file, 'r', encoding='utf-8') as f:
            content = f.read()
        content = re.sub(r"^__version__ = .*", f"__version__ = '{tdgpt_version.version}'", content)
        with open(init_file, 'w', encoding='utf-8') as f:
            f.write(content)
        logging.info(f"Updated version to {tdgpt_version.version} in __init__.py")
    
    logging.info("Python files copied")


def copy_resource_files():
    """Copy resource files"""
    resource_dir = os.path.join(install_info.install_dir, "resource")
    os.makedirs(resource_dir, exist_ok=True)
    
    source_resource = os.path.join(install_info.source_dir, "resource")
    if os.path.exists(source_resource):
        for item in os.listdir(source_resource):
            src = os.path.join(source_resource, item)
            dst = os.path.join(resource_dir, item)
            if os.path.isdir(src):
                shutil.copytree(src, dst, dirs_exist_ok=True)
            else:
                shutil.copy2(src, dst)
        logging.info(f"Copied resource files")
    
    logging.info("Resource files copied")


def copy_requirements():
    """Copy requirements files"""
    source_dir = install_info.source_dir
    
    for req_file in ["requirements.txt", "requirements_ess.txt", "requirements_docker.txt"]:
        src = os.path.join(source_dir, req_file)
        if os.path.exists(src):
            shutil.copy2(src, install_info.install_dir)
            logging.info(f"Copied {req_file}")
    
    logging.info("Requirements files copied")


def copy_model_files():
    """Copy model files if model directory is specified"""
    if not install_info.model_dir or not os.path.exists(install_info.model_dir):
        logging.info("No model directory specified or directory does not exist, skipping model files")
        return
    
    model_dir = os.path.join(install_info.install_dir, "model")
    os.makedirs(model_dir, exist_ok=True)
    
    # Copy tdtsfm model
    tdtsfm_file = os.path.join(install_info.model_dir, "tdtsfm.tar.gz")
    if os.path.exists(tdtsfm_file):
        shutil.copy2(tdtsfm_file, model_dir)
        logging.info("Copied tdtsfm model")
    
    # Copy timemoe model
    timemoe_file = os.path.join(install_info.model_dir, "timemoe.tar.gz")
    if os.path.exists(timemoe_file):
        shutil.copy2(timemoe_file, model_dir)
        logging.info("Copied timemoe model")
    
    # Copy additional models if --all-models is specified
    if install_info.all_models:
        for model_name in ["chronos", "moment-large", "moirai", "timesfm"]:
            model_file = os.path.join(install_info.model_dir, f"{model_name}.tar.gz")
            if os.path.exists(model_file):
                shutil.copy2(model_file, model_dir)
                logging.info(f"Copied {model_name} model")
    
    logging.info("Model files copied")


def copy_enterprise_files():
    """Copy enterprise-specific files"""
    if tdgpt_version.ver_type != "enterprise":
        return
    
    # Copy enterprise tools from outside
    enterprise_src = os.path.join(install_info.source_dir, "..", "..", "..", "enterprise", "src", "kit", "tools", "tdgpt", "taosanalytics", "misc")
    enterprise_dst = os.path.join(install_info.install_dir, "lib", "taosanalytics", "misc")
    
    if os.path.exists(enterprise_src) and os.path.exists(enterprise_dst):
        for item in os.listdir(enterprise_src):
            src = os.path.join(enterprise_src, item)
            dst = os.path.join(enterprise_dst, item)
            if os.path.isfile(src):
                shutil.copy2(src, dst)
        logging.info("Copied enterprise-specific files")


def create_batch_scripts():
    """Create Windows batch scripts for service management"""
    bin_dir = os.path.join(install_info.install_dir, "bin")
    os.makedirs(bin_dir, exist_ok=True)
    
    # Create start-taosanode.bat
    start_bat = os.path.join(bin_dir, "start-taosanode.bat")
    with open(start_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM TDGPT Taosanode Service Start Script

set INSTALL_DIR=C:\\TDengine\\taosanode
set PYTHON_DIR=%INSTALL_DIR%\\python
set VENV_DIR=%INSTALL_DIR%\\venv

REM Check if Python is installed
if not exist "%PYTHON_DIR%\\python.exe" (
    echo Python not found. Please install Python first.
    exit /b 1
)

REM Create virtual environment if not exists
if not exist "%VENV_DIR%" (
    echo Creating virtual environment...
    "%PYTHON_DIR%\\python.exe" -m venv "%VENV_DIR%"
)

REM Activate virtual environment and install dependencies
call "%VENV_DIR%\\Scripts\\activate.bat"

REM Install requirements if exists
if exist "%INSTALL_DIR%\\requirements.txt" (
    pip install -r "%INSTALL_DIR%\\requirements.txt" --quiet
)

REM Start taosanode service
cd /d "%INSTALL_DIR%\\lib"
set PYTHONPATH=%INSTALL_DIR%\\lib

REM Start using uwsgi or python directly
if exist "%VENV_DIR%\\Scripts\\uwsgi.exe" (
    start "" "%VENV_DIR%\\Scripts\\uwsgi.exe" --ini "%INSTALL_DIR%\\cfg\\taosanode.ini"
) else (
    start "" python "%INSTALL_DIR%\\lib\\taosanalytics\\app.py"
)

echo Taosanode service started.
""")
    logging.info("Created start-taosanode.bat")
    
    # Create stop-taosanode.bat
    stop_bat = os.path.join(bin_dir, "stop-taosanode.bat")
    with open(stop_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul

REM TDGPT Taosanode Service Stop Script

taskkill /f /im uwsgi.exe 2>nul
taskkill /f /im python.exe 2>nul

echo Taosanode service stopped.
""")
    logging.info("Created stop-taosanode.bat")
    
    # Create install-service.bat
    install_svc_bat = os.path.join(bin_dir, "install-service.bat")
    with open(install_svc_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM Install Taosanode as Windows Service

set INSTALL_DIR=C:\\TDengine\\taosanode

REM Create service using sc.exe
sc create taosanode binPath= "cmd.exe /c %INSTALL_DIR%\\bin\\start-taosanode.bat" start= auto displayname= "TDGPT Taosanode Service"
sc description taosanode "TDGPT Analytics Node Service"

echo Service installed. Use 'sc start taosanode' to start the service.
""")
    logging.info("Created install-service.bat")
    
    # Create uninstall-service.bat
    uninstall_svc_bat = os.path.join(bin_dir, "uninstall-service.bat")
    with open(uninstall_svc_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul

REM Uninstall Taosanode Windows Service

sc stop taosanode 2>nul
sc delete taosanode 2>nul

echo Service uninstalled.
""")
    logging.info("Created uninstall-service.bat")


def create_install_script():
    """Create install.bat for post-installation setup"""
    install_bat = os.path.join(install_info.install_dir, "install.bat")
    with open(install_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

echo ==========================================
echo TDGPT Installation Script
echo ==========================================

set INSTALL_DIR=C:\\TDengine\\taosanode

REM Create necessary directories
if not exist "%INSTALL_DIR%\\log" mkdir "%INSTALL_DIR%\\log"
if not exist "%INSTALL_DIR%\\data" mkdir "%INSTALL_DIR%\\data"

REM Install service
call "%INSTALL_DIR%\\bin\\install-service.bat"

echo.
echo ==========================================
echo Installation completed!
echo.
echo To start the service:
echo   sc start taosanode
echo   or
echo   %INSTALL_DIR%\\bin\\start-taosanode.bat
echo.
echo To stop the service:
echo   sc stop taosanode
echo   or
echo   %INSTALL_DIR%\\bin\\stop-taosanode.bat
echo.
echo Configuration file: %INSTALL_DIR%\\cfg\\taosanode.ini
echo ==========================================

pause
""")
    logging.info("Created install.bat")


def create_uninstall_script():
    """Create uninstall.bat for uninstallation"""
    uninstall_bat = os.path.join(install_info.install_dir, "uninstall.bat")
    with open(uninstall_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul

echo ==========================================
echo TDGPT Uninstallation Script
echo ==========================================

set INSTALL_DIR=C:\\TDengine\\taosanode

REM Stop and remove service
call "%INSTALL_DIR%\\bin\\uninstall-service.bat"

REM Stop any running processes
call "%INSTALL_DIR%\\bin\\stop-taosanode.bat"

echo.
echo Uninstallation completed!
echo You may now delete the directory: %INSTALL_DIR%
echo ==========================================

pause
""")
    logging.info("Created uninstall.bat")


def check_inno_setup():
    """Check if Inno Setup is installed"""
    iscc_paths = [
        install_info.iscc_path,
        r"C:\Program Files (x86)\Inno Setup 6\ISCC.exe",
        r"C:\Program Files\Inno Setup 6\ISCC.exe",
        r"C:\Program Files (x86)\Inno Setup 5\ISCC.exe",
        r"C:\Program Files\Inno Setup 5\ISCC.exe",
    ]
    
    for path in iscc_paths:
        if os.path.exists(path):
            install_info.iscc_path = path
            logging.info(f"Found Inno Setup compiler: {path}")
            return True
    
    logging.error("Inno Setup compiler (ISCC.exe) not found!")
    logging.error("Please install Inno Setup from https://jrsoftware.org/isdl.php")
    return False


def create_iss_script():
    """Create Inno Setup script"""
    iss_path = os.path.join(install_info.release_dir, "tdgpt.iss")
    
    iss_content = f"""; TDGPT Windows Installer Script
; Generated by win_release.py

#define MyAppName "TDGPT"
#define MyAppPublisher "taosdata"
#define MyAppURL "http://www.taosdata.com/"
#define MyAppVersion "{tdgpt_version.version}"
#define MyAppInstallName "{install_info.package_name}"
#define MyProductFullName "TDGPT - TDengine Analytics Node"
#define MyAppInstallDir "C:\\TDengine\\taosanode"
#define MyAppSourceDir "{install_info.install_dir.replace('\\', '\\\\')}"

[Setup]
AppId={{{{A0F7A93C-79C4-485D-B2B8-F0D03DF42FAB}}
AppName={{#MyAppName}}
AppVersion={{#MyAppVersion}}
AppPublisher={{#MyAppPublisher}}
AppPublisherURL={{#MyAppURL}}
AppSupportURL={{#MyAppURL}}
DefaultDirName={{#MyAppInstallDir}}
DefaultGroupName={{#MyAppName}}
DisableProgramGroupPage=yes
OutputDir={install_info.release_dir.replace('\\', '\\\\')}
OutputBaseFilename={{#MyAppInstallName}}
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
; Configuration files
Source: "{{#MyAppSourceDir}}\\cfg\\*"; DestDir: "{{app}}\\cfg"; Flags: ignoreversion recursesubdirs createallsubdirs onlyifdoesntexist uninsneveruninstall

; Library files (Python)
Source: "{{#MyAppSourceDir}}\\lib\\*"; DestDir: "{{app}}\\lib"; Flags: ignoreversion recursesubdirs createallsubdirs

; Resource files
Source: "{{#MyAppSourceDir}}\\resource\\*"; DestDir: "{{app}}\\resource"; Flags: ignoreversion recursesubdirs createallsubdirs; Check: DirExists(ExpandConstant('{{#MyAppSourceDir}}\\resource'))

; Model files
Source: "{{#MyAppSourceDir}}\\model\\*"; DestDir: "{{app}}\\model"; Flags: ignoreversion recursesubdirs createallsubdirs; Check: DirExists(ExpandConstant('{{#MyAppSourceDir}}\\model'))

; Requirements files
Source: "{{#MyAppSourceDir}}\\requirements*.txt"; DestDir: "{{app}}"; Flags: ignoreversion

; Batch scripts
Source: "{{#MyAppSourceDir}}\\bin\\*.bat"; DestDir: "{{app}}\\bin"; Flags: ignoreversion
Source: "{{#MyAppSourceDir}}\\install.bat"; DestDir: "{{app}}"; Flags: ignoreversion
Source: "{{#MyAppSourceDir}}\\uninstall.bat"; DestDir: "{{app}}"; Flags: ignoreversion

[Dirs]
Name: "{{app}}\\log"; Permissions: everyone-modify
Name: "{{app}}\\data"; Permissions: everyone-modify
Name: "{{app}}\\venv"; Permissions: everyone-modify

[Run]
; Run install script after installation
Filename: "{{app}}\\install.bat"; Description: "Run installation script"; Flags: postinstall runascurrentuser waituntilidle

[UninstallRun]
; Run uninstall script before uninstallation
Filename: "{{app}}\\uninstall.bat"; Flags: runhidden

[UninstallDelete]
Name: "{{app}}\\log"; Type: filesandordirs
Name: "{{app}}\\data"; Type: filesandordirs
Name: "{{app}}\\venv"; Type: filesandordirs

[Icons]
Name: "{{group}}\\Start Taosanode"; Filename: "{{app}}\\bin\\start-taosanode.bat"
Name: "{{group}}\\Stop Taosanode"; Filename: "{{app}}\\bin\\stop-taosanode.bat"
Name: "{{group}}\\Uninstall {{#MyAppName}}"; Filename: "{{uninstallexe}}"
Name: "{{commondesktop}}\\Start Taosanode"; Filename: "{{app}}\\bin\\start-taosanode.bat"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "{{cm:CreateDesktopIcon}}"; GroupDescription: "{{cm:AdditionalIcons}}"; Flags: checkablealone

[Registry]
; Add to PATH environment variable
Root: HKLM; Subkey: "SYSTEM\\CurrentControlSet\\Control\\Session Manager\\Environment"; \\
    ValueType: expandsz; ValueName: "Path"; ValueData: "{{olddata}};{{app}}\\bin"; \\
    Check: NeedsAddPath('{{app}}\\bin')

[Code]
function NeedsAddPath(Param: string): boolean;
var
  OrigPath: string;
begin
  if not RegQueryStringValue(HKEY_LOCAL_MACHINE,
    'SYSTEM\\CurrentControlSet\\Control\\Session Manager\\Environment',
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
        if FileExists(ExpandConstant('{{app}}\\taosanode.pid')) then
          DeleteFile(ExpandConstant('{{app}}\\taosanode.pid'));
      end;
  end;
end;
"""
    
    with open(iss_path, 'w', encoding='utf-8') as f:
        f.write(iss_content)
    
    logging.info(f"Created Inno Setup script: {iss_path}")
    return iss_path


def build_installer(iss_path):
    """Build the installer using Inno Setup"""
    if not check_inno_setup():
        return False
    
    cmd = f'"{install_info.iscc_path}" "{iss_path}"'
    logging.info(f"Building installer with command: {cmd}")
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            logging.info("Installer built successfully!")
            installer_path = os.path.join(install_info.release_dir, f"{install_info.package_name}.exe")
            if os.path.exists(installer_path):
                logging.info(f"Installer created: {installer_path}")
                return True
            else:
                logging.warning("Installer may have been created, but file not found at expected location")
                return True
        else:
            logging.error(f"Failed to build installer:")
            logging.error(result.stdout)
            logging.error(result.stderr)
            return False
    except Exception as e:
        logging.error(f"Error building installer: {e}")
        return False


def create_zip_package():
    """Create a ZIP package as alternative"""
    zip_path = os.path.join(install_info.release_dir, f"{install_info.package_name}.zip")
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(install_info.install_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, install_info.install_dir)
                zipf.write(file_path, arcname)
    
    logging.info(f"Created ZIP package: {zip_path}")
    return zip_path


def main():
    """Main function"""
    args = parse_arguments()
    print_params()
    
    # Clean and prepare release directory
    clean_release_dir()
    
    # Copy all necessary files
    copy_config_files()
    copy_python_files()
    copy_resource_files()
    copy_requirements()
    copy_model_files()
    copy_enterprise_files()
    
    # Create batch scripts
    create_batch_scripts()
    create_install_script()
    create_uninstall_script()
    
    # Create Inno Setup script and build installer
    iss_path = create_iss_script()
    
    if build_installer(iss_path):
        logging.info("=" * 60)
        logging.info("Packaging completed successfully!")
        logging.info(f"Installer: {os.path.join(install_info.release_dir, install_info.package_name + '.exe')}")
        logging.info("=" * 60)
    else:
        # Fallback: Create ZIP package if Inno Setup is not available
        logging.warning("Creating ZIP package as fallback...")
        zip_path = create_zip_package()
        logging.info("=" * 60)
        logging.info("ZIP package created (Inno Setup not available)")
        logging.info(f"Package: {zip_path}")
        logging.info("=" * 60)
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
