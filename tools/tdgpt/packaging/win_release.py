#!/usr/bin/env python3
"""
TDGPT Windows Packaging Script
Generate install package for Windows Platform using Inno Setup

Usage:
    python win_release.py -e <edition> -v <version> [-m <model_dir>] [-a]

Options:
    -e, --edition    : enterprise or community
    -v, --version    : tdgpt version
    -m, --model-dir  : model files dir
    -a, --all-models : pack all models
"""

import os
import sys
import argparse
import shutil
import logging
import datetime
import re
import subprocess

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
        self.model_dir = ""
        self.all_models = False
        self.offline = False
        self.iscc_path = ""


tdgpt_version = TDGPTVersion("community", "1.0.0")
install_info = InstallInfo()


def check_python_version():
    """Check if Python version meets requirements"""
    version = sys.version_info
    if version.major != 3 or version.minor not in [10, 11, 12]:
        logging.error(f"Python 3.10/3.11/3.12 required, found {version.major}.{version.minor}")
        logging.error("Please install Python 3.10, 3.11, or 3.12 from https://www.python.org/")
        sys.exit(1)
    logging.info(f"Python {version.major}.{version.minor}.{version.micro} detected ✓")
    return True


def parse_arguments():
    """Parse command line arguments"""

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
    parser.add_argument('--offline', action='store_true',
                        help='Include offline installation packages (wheels)')
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
    install_info.offline = args.offline

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
    logging.info(f"Offline Mode: {install_info.offline}")
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

    # Copy taosanode.config.py
    src_config = os.path.join(source_cfg, "taosanode.config.py")
    dst_config = os.path.join(cfg_dir, "taosanode.config.py")
    if os.path.exists(src_config):
        shutil.copy2(src_config, dst_config)
        logging.info("Copied taosanode.config.py")

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
        logging.info("Copied resource files")


def copy_requirements():
    """Copy requirements files"""
    source_dir = install_info.source_dir

    for req_file in ["requirements.txt", "requirements_ess.txt", "requirements_docker.txt"]:
        src = os.path.join(source_dir, req_file)
        if os.path.exists(src):
            shutil.copy2(src, install_info.install_dir)
            logging.info(f"Copied {req_file}")


def prepare_offline_packages():
    """Prepare offline installation packages (wheels)"""
    if not install_info.offline:
        logging.info("Offline mode not enabled, skipping wheel files")
        return

    wheels_dir = os.path.join(install_info.install_dir, "wheels")
    os.makedirs(wheels_dir, exist_ok=True)

    # Look for pre-downloaded wheels in packaging directory
    source_wheels = os.path.join(install_info.source_dir, "packaging", "wheels")

    if os.path.exists(source_wheels):
        wheel_count = 0
        for item in os.listdir(source_wheels):
            src = os.path.join(source_wheels, item)
            dst = os.path.join(wheels_dir, item)
            if os.path.isfile(src) and item.endswith('.whl'):
                shutil.copy2(src, dst)
                wheel_count += 1
            elif os.path.isdir(src):
                shutil.copytree(src, dst, dirs_exist_ok=True)

        if wheel_count > 0:
            logging.info(f"Copied {wheel_count} wheel files for offline installation")
        else:
            logging.warning("No wheel files found in packaging/wheels directory")
    else:
        logging.warning(f"Offline wheels directory not found: {source_wheels}")
        logging.warning("Installation will require internet connection")


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
    enterprise_src = os.path.join(install_info.source_dir, "..", "..", "..", "enterprise",
                                  "src", "kit", "tools", "tdgpt", "taosanalytics", "misc")
    enterprise_dst = os.path.join(install_info.install_dir, "lib", "taosanalytics", "misc")

    if os.path.exists(enterprise_src) and os.path.exists(enterprise_dst):
        for item in os.listdir(enterprise_src):
            src = os.path.join(enterprise_src, item)
            dst = os.path.join(enterprise_dst, item)
            if os.path.isfile(src):
                shutil.copy2(src, dst)
        logging.info("Copied enterprise-specific files")


def copy_service_scripts():
    """Copy unified Python service management scripts"""
    bin_dir = os.path.join(install_info.install_dir, "bin")
    os.makedirs(bin_dir, exist_ok=True)

    source_script_dir = os.path.join(install_info.source_dir, "script")

    # Copy taosanode_service.py (unified service manager)
    src_service = os.path.join(source_script_dir, "taosanode_service.py")
    dst_service = os.path.join(bin_dir, "taosanode_service.py")
    if os.path.exists(src_service):
        shutil.copy2(src_service, dst_service)
        logging.info("Copied taosanode_service.py")

    # Create wrapper batch scripts for Windows
    create_service_wrappers(bin_dir)


def create_service_wrappers(bin_dir):
    """Create Windows batch wrappers for Python service scripts"""

    # start-taosanode.bat
    start_bat = os.path.join(bin_dir, "start-taosanode.bat")
    with open(start_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul
REM Start taosanode service
python "%~dp0taosanode_service.py" start %*
""")
    logging.info("Created start-taosanode.bat")

    # stop-taosanode.bat
    stop_bat = os.path.join(bin_dir, "stop-taosanode.bat")
    with open(stop_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul
REM Stop taosanode service
python "%~dp0taosanode_service.py" stop %*
""")
    logging.info("Created stop-taosanode.bat")

    # status-taosanode.bat
    status_bat = os.path.join(bin_dir, "status-taosanode.bat")
    with open(status_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul
REM Show taosanode status
python "%~dp0taosanode_service.py" status %*
pause
""")
    logging.info("Created status-taosanode.bat")

    # start-model.bat
    start_model_bat = os.path.join(bin_dir, "start-model.bat")
    with open(start_model_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul
REM Start model service
REM Usage: start-model.bat [model_name|all]
if "%~1"=="" (
    echo Usage: start-model.bat [model_name^|all]
    echo Supported models: tdtsfm, timesfm, timemoe, moirai, chronos, moment
    exit /b 1
)
python "%~dp0taosanode_service.py" model-start %*
""")
    logging.info("Created start-model.bat")

    # stop-model.bat
    stop_model_bat = os.path.join(bin_dir, "stop-model.bat")
    with open(stop_model_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul
REM Stop model service
REM Usage: stop-model.bat [model_name|all]
if "%~1"=="" (
    echo Usage: stop-model.bat [model_name^|all]
    exit /b 1
)
python "%~dp0taosanode_service.py" model-stop %*
""")
    logging.info("Created stop-model.bat")

    # status-model.bat
    status_model_bat = os.path.join(bin_dir, "status-model.bat")
    with open(status_model_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul
REM Show model status
python "%~dp0taosanode_service.py" model-status %*
pause
""")
    logging.info("Created status-model.bat")


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
if not exist "%INSTALL_DIR%\\model" mkdir "%INSTALL_DIR%\\model"
if not exist "%INSTALL_DIR%\\data" mkdir "%INSTALL_DIR%\\data"
if not exist "%INSTALL_DIR%\\data\\pids" mkdir "%INSTALL_DIR%\\data\\pids"

echo.
echo ==========================================
echo Installing Python Dependencies...
echo ==========================================
echo.

REM Check if offline wheels are available
if exist "%INSTALL_DIR%\\wheels\\" (
    echo Installing dependencies from local wheels (offline mode)...
    python -m pip install --no-index --find-links="%INSTALL_DIR%\\wheels" -r "%INSTALL_DIR%\\requirements_ess.txt"
) else (
    echo Installing dependencies from PyPI (online mode)...
    python -m pip install -r "%INSTALL_DIR%\\requirements_ess.txt"
)

if errorlevel 1 (
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)

echo.
echo ==========================================
echo Verifying Installation...
echo ==========================================
echo.

REM Verify service can be accessed
python "%INSTALL_DIR%\\bin\\taosanode_service.py" status >nul 2>&1
if errorlevel 1 (
    echo WARNING: Service verification failed
    echo Please check logs at %INSTALL_DIR%\\log
    echo.
) else (
    echo [OK] Service verification passed
    echo.
)

echo ==========================================
echo Installation completed!
echo.
echo Quick Start Commands:
echo   Start taosanode: %INSTALL_DIR%\\bin\\start-taosanode.bat
echo   Stop taosanode:  %INSTALL_DIR%\\bin\\stop-taosanode.bat
echo   Check status:    %INSTALL_DIR%\\bin\\status-taosanode.bat
echo.
echo Model Commands:
echo   Start all models: %INSTALL_DIR%\\bin\\start-model.bat all
echo   Stop all models:  %INSTALL_DIR%\\bin\\stop-model.bat all
echo   Model status:     %INSTALL_DIR%\\bin\\status-model.bat
echo.
echo Configuration file: %INSTALL_DIR%\\cfg\\taosanode.config.py
echo ==========================================

pause
""")
    logging.info("Created install.bat")


def create_uninstall_script():
    """Create uninstall.bat - called by Inno Setup during uninstallation"""
    uninstall_bat = os.path.join(install_info.install_dir, "uninstall.bat")
    with open(uninstall_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul

REM TDGPT Pre-Uninstall Script
REM This script is called by Inno Setup before uninstalling files

echo Stopping TDGPT services...

REM Stop taosanode main service
python "%~dp0bin\\taosanode_service.py" stop 2>nul

REM Stop all model services
python "%~dp0bin\\taosanode_service.py" model-stop all 2>nul

REM Kill only taosanode-related python processes
REM Use tasklist to find processes and filter by command line
for /f "tokens=2" %%i in ('tasklist /FI "IMAGENAME eq python.exe" /FO LIST ^| findstr /C:"PID:"') do (
    wmic process where "ProcessId=%%i and CommandLine like '%%taosanode%%'" delete 2>nul
)

echo Services stopped.
REM Exit without pause - this script is run by the installer silently
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
    """Create Inno Setup script from template"""
    iss_path = os.path.join(install_info.release_dir, "tdgpt.iss")

    # Read template file
    template_path = os.path.join(os.path.dirname(__file__), "installer", "tdgpt.iss")
    with open(template_path, 'r', encoding='utf-8') as f:
        iss_content = f.read()

    # Pre-process paths for Windows (use forward slashes for ISS file)
    install_dir_iss = install_info.install_dir.replace('\\', '/')
    release_dir_iss = install_info.release_dir.replace('\\', '/')

    # Replace placeholders
    iss_content = iss_content.replace('{{VERSION}}', tdgpt_version.version)
    iss_content = iss_content.replace('{{PACKAGE_NAME}}', install_info.package_name)
    iss_content = iss_content.replace('{{SOURCE_DIR}}', install_dir_iss)
    iss_content = iss_content.replace('{{OUTPUT_DIR}}', release_dir_iss)

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
            logging.error("Failed to build installer:")
            logging.error(result.stdout)
            logging.error(result.stderr)
            return False
    except Exception as e:
        logging.error(f"Error building installer: {e}")
        return False



def download_winsw():
    """Download winsw executable for Windows service wrapper (with local cache support)"""
    bin_dir = os.path.join(install_info.install_dir, "bin")
    os.makedirs(bin_dir, exist_ok=True)

    winsw_dest = os.path.join(bin_dir, "taosanode-service.exe")

    # Check local cache first
    local_cache = os.path.join(os.path.dirname(__file__), "bin", "WinSW.exe")
    if os.path.exists(local_cache):
        logging.info(f"Using local cached WinSW: {local_cache}")
        shutil.copy2(local_cache, winsw_dest)
        logging.info(f"Copied WinSW to {winsw_dest}")
        return True

    # Download from GitHub if not in cache
    winsw_url = "https://github.com/winsw/winsw/releases/download/v2.12.0/WinSW.NET461.exe"
    try:
        logging.info(f"Downloading winsw from {winsw_url}")
        import urllib.request
        urllib.request.urlretrieve(winsw_url, winsw_dest)
        logging.info(f"Downloaded winsw to {winsw_dest}")

        # Save to local cache for future use
        os.makedirs(os.path.dirname(local_cache), exist_ok=True)
        shutil.copy2(winsw_dest, local_cache)
        logging.info(f"Cached WinSW to {local_cache}")
        return True
    except Exception as e:
        logging.warning(f"Failed to download winsw: {e}")
        logging.warning("Service registration will not be available")
        return False


def create_winsw_config():
    """Create winsw XML configuration file from template"""
    bin_dir = os.path.join(install_info.install_dir, "bin")
    os.makedirs(bin_dir, exist_ok=True)

    # Read template file
    template_path = os.path.join(os.path.dirname(__file__), "installer", "taosanode-service.xml")
    with open(template_path, 'r', encoding='utf-8') as f:
        config_content = f.read()

    # Write to destination
    config_path = os.path.join(bin_dir, "taosanode-service.xml")
    with open(config_path, 'w', encoding='utf-8') as f:
        f.write(config_content)
    logging.info(f"Created winsw config: {config_path}")


def main():
    """Main function"""
    check_python_version()
    parse_arguments()
    print_params()

    # Clean and prepare release directory
    clean_release_dir()

    # Copy all necessary files
    copy_config_files()
    copy_python_files()
    copy_resource_files()
    copy_requirements()
    prepare_offline_packages()  # Prepare offline wheels if --offline flag is set
    copy_model_files()
    copy_enterprise_files()
    copy_service_scripts()  # New unified service scripts
    download_winsw()  # Download winsw for Windows service support
    create_winsw_config()  # Create winsw configuration

    # Create batch scripts
    create_install_script()
    create_uninstall_script()

    # Create Inno Setup script and build installer
    iss_path = create_iss_script()

    if build_installer(iss_path):
        logging.info("=" * 60)
        logging.info("Packaging completed successfully!")
        logging.info(f"Installer: {os.path.join(install_info.release_dir, install_info.package_name + '.exe')}")
        logging.info("=" * 60)
        return 0
    else:
        logging.error("=" * 60)
        logging.error("Failed to build installer with Inno Setup")
        logging.error("Please check:")
        logging.error("1. Inno Setup is installed and ISCC.exe is in PATH")
        logging.error("2. All required files exist in the staging directory")
        logging.error("3. Check the Inno Setup log for details")
        logging.error("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
