#!/usr/bin/env python3
"""
TDgpt Windows Packaging Script
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
import json
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
        self.app_name = "TDengine TDgpt-OSS"
        self.product_full_name = "TDengine TDgpt-OSS - TDengine Analytics Node"
        self.model_dir = ""
        self.all_models = False
        self.iscc_path = ""
        self.skip_model_check = False
        self.python_runtime_dir = ""
        self.resource_package_url = ""


tdgpt_version = TDGPTVersion("community", "1.0.0")
install_info = InstallInfo()
WINSW_BASENAME = "taosanode-winsw"
DEFAULT_RESOURCE_PACKAGE_URL = "https://downloads.tdengine.com/tdengine-historian/tdgpt-resource-pkg/tdengine-tdgpt-resource-1.0.tar"


def check_python_version():
    """Check if Python version meets requirements"""
    version = sys.version_info
    if version.major != 3 or version.minor not in [10, 11, 12]:
        logging.error(f"Python 3.10/3.11/3.12 required, found {version.major}.{version.minor}")
        logging.error("Please install Python 3.10, 3.11, or 3.12 from https://www.python.org/")
        sys.exit(1)
    logging.info(f"Python {version.major}.{version.minor}.{version.micro} detected")
    return True


def parse_arguments():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Release TDgpt on Windows')
    parser.add_argument('-e', '--edition', type=str, required=True,
                        help='Set edition type (enterprise or community)')
    parser.add_argument('-v', '--version', type=str, required=True,
                        help='Set version number (e.g., 3.4.0.11)')
    parser.add_argument('-m', '--model-dir', type=str, default="",
                        help='Set model files directory')
    parser.add_argument('-a', '--all-models', action='store_true',
                        help='Pack all models')
    parser.add_argument('-o', '--output', type=str, default="D:\\tdgpt-release",
                        help='Set output directory (default: D:\\tdgpt-release)')
    parser.add_argument('--iscc-path', type=str,
                        default=r"C:\Program Files (x86)\Inno Setup 6\ISCC.exe",
                        help='Path to Inno Setup compiler')
    parser.add_argument('--skip-model-check', action='store_true',
                        help='Skip model validation (for testing only, NOT for production)')
    parser.add_argument('--python-runtime-dir', type=str, default="",
                        help='Existing Python runtime directory to bundle into install/python/runtime')
    parser.add_argument('--resource-package-url', type=str, default=DEFAULT_RESOURCE_PACKAGE_URL,
                        help='Default remote TDgpt resource package URL used by the installer UI')

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

    # Set product metadata
    if args.edition == "enterprise":
        install_info.product_name = "tdengine-tdgpt-enterprise"
        install_info.app_name = "TDengine TDgpt-Enterprise"
    else:
        install_info.product_name = "tdengine-tdgpt-oss"
        install_info.app_name = "TDengine TDgpt-OSS"
    install_info.product_full_name = f"{install_info.app_name} - TDengine Analytics Node"

    install_info.source_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    install_info.release_dir = args.output
    install_info.install_dir = os.path.join(install_info.release_dir, "install")
    install_info.package_name = f"{install_info.product_name}-{args.version}-windows-x64"
    install_info.model_dir = args.model_dir
    install_info.all_models = args.all_models
    install_info.iscc_path = args.iscc_path
    install_info.skip_model_check = args.skip_model_check
    install_info.python_runtime_dir = args.python_runtime_dir
    install_info.resource_package_url = args.resource_package_url.strip()

    return args


def print_params():
    """Print configuration parameters"""
    logging.info("=" * 60)
    logging.info("TDgpt Windows Packaging Configuration")
    logging.info("=" * 60)
    logging.info(f"Edition: {tdgpt_version.ver_type}")
    logging.info(f"Version: {tdgpt_version.version}")
    logging.info(f"App Name: {install_info.app_name}")
    logging.info(f"Product Full Name: {install_info.product_full_name}")
    logging.info(f"Product Name: {install_info.product_name}")
    logging.info(f"Source Directory: {install_info.source_dir}")
    logging.info(f"Release Directory: {install_info.release_dir}")
    logging.info(f"Install Directory: {install_info.install_dir}")
    logging.info(f"Package Name: {install_info.package_name}")
    logging.info(f"Model Directory: {install_info.model_dir}")
    logging.info(f"All Models: {install_info.all_models}")
    logging.info(f"Python Runtime Directory: {install_info.python_runtime_dir or '(auto)'}")
    logging.info(f"Resource Package URL: {install_info.resource_package_url or '(none)'}")
    logging.info("=" * 60)


def resolve_python_runtime_source():
    """Resolve the Python runtime directory that should be bundled into the installer."""
    candidates = []
    if install_info.python_runtime_dir:
        candidates.append(os.path.abspath(install_info.python_runtime_dir))

    base_prefix = getattr(sys, "base_prefix", "") or ""
    if base_prefix:
        candidates.append(os.path.abspath(base_prefix))

    executable_dir = os.path.dirname(os.path.abspath(sys.executable))
    candidates.append(executable_dir)

    for candidate in candidates:
        python_exe = os.path.join(candidate, "python.exe")
        lib_dir = os.path.join(candidate, "Lib")
        if os.path.exists(python_exe) and os.path.isdir(lib_dir):
            return candidate

    raise FileNotFoundError(
        "Unable to resolve a Python runtime directory to bundle. "
        "Pass --python-runtime-dir and point it to a directory containing python.exe and Lib/."
    )


def copy_packaged_python_runtime():
    """Copy the packaged Python runtime that the installer always carries."""
    source_runtime = resolve_python_runtime_source()
    runtime_dir = os.path.join(install_info.install_dir, "python", "runtime")
    os.makedirs(os.path.dirname(runtime_dir), exist_ok=True)

    if os.path.exists(runtime_dir):
        shutil.rmtree(runtime_dir)

    shutil.copytree(
        source_runtime,
        runtime_dir,
        ignore=shutil.ignore_patterns('__pycache__', '*.pyc', '*.pyo')
    )
    logging.info(f"Copied packaged Python runtime from {source_runtime} to {runtime_dir}")


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

    metadata = {
        "edition": tdgpt_version.ver_type,
        "app_name": install_info.app_name,
        "product_full_name": install_info.product_full_name,
        "package_name": install_info.package_name,
        "product_name": install_info.product_name,
        "version": tdgpt_version.version,
        "resource_package_url": install_info.resource_package_url,
    }
    metadata_file = os.path.join(cfg_dir, "package-metadata.json")
    with open(metadata_file, "w", encoding="utf-8") as file_obj:
        json.dump(metadata, file_obj, indent=2, ensure_ascii=False)
        file_obj.write("\n")
    logging.info("Created package-metadata.json")

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
    requirements_dir = os.path.join(install_info.install_dir, "requirements")
    os.makedirs(requirements_dir, exist_ok=True)

    copied = 0
    for req_file in os.listdir(source_dir):
        if req_file.startswith("requirements") and req_file.endswith(".txt"):
            src = os.path.join(source_dir, req_file)
            if os.path.isfile(src):
                shutil.copy2(src, requirements_dir)
                copied += 1
                logging.info(f"Copied {req_file}")

    if copied == 0:
        logging.warning("No requirements*.txt files found to package")

def copy_model_files():
    """Copy packaged model archives if model directory is specified."""
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

    # Copy install.py (installation script)
    src_install = os.path.join(source_script_dir, "install.py")
    dst_install = os.path.join(install_info.install_dir, "install.py")
    if os.path.exists(src_install):
        shutil.copy2(src_install, dst_install)
        logging.info("Copied install.py")

    # Copy uninstall.py (uninstallation script)
    src_uninstall = os.path.join(source_script_dir, "uninstall.py")
    dst_uninstall = os.path.join(install_info.install_dir, "uninstall.py")
    if os.path.exists(src_uninstall):
        shutil.copy2(src_uninstall, dst_uninstall)
        logging.info("Copied uninstall.py")

    # Create wrapper batch scripts for Windows
    create_service_wrappers(bin_dir)


def copy_icon_file():
    """Copy icon file from enterprise packaging"""
    # Try to find icon file from enterprise packaging
    enterprise_icon = os.path.join(install_info.source_dir, "..", "..", "..", "enterprise",
                                   "packaging", "windows", "favicon.ico")

    if os.path.exists(enterprise_icon):
        dst_icon = os.path.join(install_info.install_dir, "favicon.ico")
        shutil.copy2(enterprise_icon, dst_icon)
        logging.info(f"Copied icon file from enterprise: {enterprise_icon}")
        return "favicon.ico"
    else:
        logging.warning(f"Icon file not found at: {enterprise_icon}")
        logging.warning("Installer will be created without custom icon")
        return "favicon.ico"  # Use placeholder, will be handled by ISS


def create_service_wrappers(bin_dir):
    """Create Windows batch wrappers for Python service scripts"""

    python_guard = """set "PYTHON_EXE=%~dp0..\\venvs\\venv\\Scripts\\python.exe"
if not exist "%PYTHON_EXE%" (
    echo ERROR: Main Python environment not found: %PYTHON_EXE%
    echo Run install.bat again to recreate the TDgpt environment.
"""

    # start-taosanode.bat
    start_bat = os.path.join(bin_dir, "start-taosanode.bat")
    with open(start_bat, 'w', encoding='utf-8') as f:
        f.write("""@echo off
chcp 65001 >nul
""" + python_guard + """    echo Press Enter to close this window.
    pause >nul
    exit /b 1
)
echo Starting Taosanode...
echo.
set "TDGPT_EXIT_CODE=0"
sc query Taosanode >nul 2>&1
if not errorlevel 1 (
    "%PYTHON_EXE%" "%~dp0taosanode_service.py" start-service %*
    set "TDGPT_EXIT_CODE=%errorlevel%"
    echo.
    echo Taosanode is managed as a Windows service.
    if not errorlevel 1 (
        "%PYTHON_EXE%" "%~dp0taosanode_service.py" wait-ready --timeout 90
        if errorlevel 1 (
            echo Taosanode start command returned, but readiness was not confirmed within 90 seconds.
            echo Check status-taosanode.bat or ..\\log\\taosanode-service.log for details.
        ) else (
            echo Taosanode is ready.
        )
    )
    echo Use status-taosanode.bat or "sc query Taosanode" to verify status.
    echo Start command: start-taosanode.bat
    echo Stop command:  stop-taosanode.bat
) else (
    "%PYTHON_EXE%" "%~dp0taosanode_service.py" start %*
    set "TDGPT_EXIT_CODE=%errorlevel%"
    echo.
    if not errorlevel 1 (
        "%PYTHON_EXE%" "%~dp0taosanode_service.py" wait-ready --timeout 90
        if errorlevel 1 (
            echo Taosanode start command returned, but readiness was not confirmed within 90 seconds.
            echo Check status-taosanode.bat or ..\\log\\taosanode-service.log for details.
        ) else (
            echo Taosanode is ready.
        )
    )
    echo Use status-taosanode.bat to verify whether the process is running.
    echo Start command: start-taosanode.bat
    echo Stop command:  stop-taosanode.bat
)
echo Press Enter to close this window.
pause >nul
exit /b %TDGPT_EXIT_CODE%
""")
    logging.info("Created start-taosanode.bat")

    # stop-taosanode.bat
    stop_bat = os.path.join(bin_dir, "stop-taosanode.bat")
    with open(stop_bat, 'w', encoding='utf-8') as f:
        f.write("""@echo off
chcp 65001 >nul
""" + python_guard + """    exit /b 1
)
sc query Taosanode >nul 2>&1
if not errorlevel 1 (
    "%PYTHON_EXE%" "%~dp0taosanode_service.py" stop-service %*
) else (
    "%PYTHON_EXE%" "%~dp0taosanode_service.py" stop %*
)
exit /b %errorlevel%
""")
    logging.info("Created stop-taosanode.bat")

    # status-taosanode.bat
    status_bat = os.path.join(bin_dir, "status-taosanode.bat")
    with open(status_bat, 'w', encoding='utf-8') as f:
        f.write("""@echo off
chcp 65001 >nul
""" + python_guard + """    pause
    exit /b 1
)
sc query Taosanode >nul 2>&1
if not errorlevel 1 (
    echo Windows service status:
    sc query Taosanode
    echo.
)
"%PYTHON_EXE%" "%~dp0taosanode_service.py" status %*
pause
exit /b %errorlevel%
""")
    logging.info("Created status-taosanode.bat")

    # start-model.bat
    start_model_bat = os.path.join(bin_dir, "start-model.bat")
    with open(start_model_bat, 'w', encoding='utf-8') as f:
        f.write("""@echo off
chcp 65001 >nul
""" + python_guard + """    echo Press Enter to close this window.
    pause >nul
    exit /b 1
)
if "%~1"=="" (
    echo No model name specified. Defaulting to "all".
    echo Existing model directories will be started automatically.
    "%PYTHON_EXE%" "%~dp0taosanode_service.py" model-start all
    set "TDGPT_EXIT_CODE=%errorlevel%"
    echo.
    echo Model start requests have been submitted in background.
    echo You can close this window and verify results with status-model.bat.
    echo Start command: start-model.bat [model_name^|all]
    echo Status command: status-model.bat
    echo Stop command:   stop-model.bat [model_name^|all]
    echo Logs are written under ..\\log\\model_*.log
    echo Press Enter to close this window.
    pause >nul
    exit /b %TDGPT_EXIT_CODE%
)
"%PYTHON_EXE%" "%~dp0taosanode_service.py" model-start %*
set "TDGPT_EXIT_CODE=%errorlevel%"
echo.
echo Model start requests have been submitted in background.
echo You can close this window and verify results with status-model.bat.
echo Start command: start-model.bat [model_name^|all]
echo Status command: status-model.bat
echo Stop command:   stop-model.bat [model_name^|all]
echo Logs are written under ..\\log\\model_*.log
echo Press Enter to close this window.
pause >nul
exit /b %TDGPT_EXIT_CODE%
""")
    logging.info("Created start-model.bat")

    # stop-model.bat
    stop_model_bat = os.path.join(bin_dir, "stop-model.bat")
    with open(stop_model_bat, 'w', encoding='utf-8') as f:
        f.write("""@echo off
chcp 65001 >nul
""" + python_guard + """    exit /b 1
)
if "%~1"=="" (
    echo Usage: stop-model.bat [model_name^|all]
    exit /b 1
)
"%PYTHON_EXE%" "%~dp0taosanode_service.py" model-stop %*
exit /b %errorlevel%
""")
    logging.info("Created stop-model.bat")

    # status-model.bat
    status_model_bat = os.path.join(bin_dir, "status-model.bat")
    with open(status_model_bat, 'w', encoding='utf-8') as f:
        f.write("""@echo off
chcp 65001 >nul
""" + python_guard + """    pause
    exit /b 1
)
"%PYTHON_EXE%" "%~dp0taosanode_service.py" model-status %*
pause
exit /b %errorlevel%
""")
    logging.info("Created status-model.bat")


def create_install_script():
    """Create install.bat for post-installation setup"""
    install_bat = os.path.join(install_info.install_dir, "install.bat")
    all_models_flag = "-a" if install_info.all_models else ""

    with open(install_bat, 'w', encoding='utf-8') as f:
        f.write(f"""@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

set INSTALL_DIR=%~dp0
set INSTALL_DIR=%INSTALL_DIR:~0,-1%
set "LOG_DIR=%INSTALL_DIR%\\log"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOG_FILE=%LOG_DIR%\\install.log"
set "PROGRESS_FILE=%TDGPT_PROGRESS_FILE%"
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
set "TDGPT_LOG_REDIRECTED=1"
set "EXISTING_VENV_PYTHON=%INSTALL_DIR%\\venvs\\venv\\Scripts\\python.exe"
set "PACKAGED_PYTHON=%INSTALL_DIR%\\python\\runtime\\python.exe"
set "PYTHON_CMD="

call :append_progress running 1 "Initializing {install_info.app_name} setup" "Preparing installation environment"
REM Priority 1: packaged runtime Python carried by the installer.
if exist "%PACKAGED_PYTHON%" (
    call :try_python "%PACKAGED_PYTHON%"
    if defined PYTHON_CMD goto :run_install
)

REM Priority 2: existing venv Python from a previous install.
if exist "%EXISTING_VENV_PYTHON%" (
    call :try_python "%EXISTING_VENV_PYTHON%"
    if defined PYTHON_CMD goto :run_install
)

REM Priority 3: system Python in PATH as a last-resort fallback.
call :resolve_python
if defined PYTHON_CMD goto :run_install

echo ERROR: No Python interpreter available.
echo The installer did not find its bundled Python runtime under python\\runtime.
echo Rebuild or reinstall the package, or provide Python 3.10/3.11/3.12 in PATH as a fallback.
call :append_progress error 100 "Installation failed" "No Python interpreter available"
exit /b 1

:run_install
> "%LOG_FILE%" echo [%date% %time%] {install_info.app_name} installation started
>> "%LOG_FILE%" echo Install directory: %INSTALL_DIR%
>> "%LOG_FILE%" echo Python command: %PYTHON_CMD%
>> "%LOG_FILE%" echo PIP_INDEX_URL=%PIP_INDEX_URL%
>> "%LOG_FILE%" echo Command args: %*

if "%PROGRESS_FILE%"=="" (
    "%PYTHON_CMD%" "%INSTALL_DIR%\\install.py" --log-file "%LOG_FILE%" {all_models_flag} %* >> "%LOG_FILE%" 2>&1
) else (
    "%PYTHON_CMD%" "%INSTALL_DIR%\\install.py" --log-file "%LOG_FILE%" {all_models_flag} --progress-file "%PROGRESS_FILE%" %* >> "%LOG_FILE%" 2>&1
)

if errorlevel 1 (
    >> "%LOG_FILE%" echo [%date% %time%] Installation failed
    call :ensure_error_progress "Installation failed" "See install.log for details"
    exit /b 1
)

>> "%LOG_FILE%" echo [%date% %time%] Installation completed successfully
call :append_progress success 100 "Installation complete" "{install_info.app_name} is ready"
exit /b 0

:append_progress
if "%PROGRESS_FILE%"=="" exit /b 0
>> "%PROGRESS_FILE%" echo %~1^|%~2^|%~3^|%~4
exit /b 0

:ensure_error_progress
if "%PROGRESS_FILE%"=="" exit /b 0
if exist "%PROGRESS_FILE%" (
    findstr /B /C:"error|" "%PROGRESS_FILE%" >nul 2>&1
    if not errorlevel 1 exit /b 0
)
call :append_progress error 100 "%~1" "%~2"
exit /b 0

:resolve_python
python --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=python"
    exit /b 0
)
python3 --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=python3"
    exit /b 0
)
set "PYTHON_CMD="
exit /b 1

:try_python
set "PYTHON_CMD="
%~1 --version >nul 2>&1
if errorlevel 1 exit /b 1
set "PYTHON_CMD=%~1"
exit /b 0
""")
    logging.info("Created install.bat")


def create_uninstall_script():
    """Create uninstall.bat - called by Inno Setup during uninstallation"""
    uninstall_bat = os.path.join(install_info.install_dir, "uninstall.bat")
    with open(uninstall_bat, 'w', encoding='utf-8') as f:
        f.write(f"""@echo off
chcp 65001 >nul

set INSTALL_DIR=%~dp0
set INSTALL_DIR=%INSTALL_DIR:~0,-1%
set "LOG_DIR=%INSTALL_DIR%\\log"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOG_FILE=%LOG_DIR%\\uninstall.log"
set "TDGPT_LOG_REDIRECTED=1"
set "PACKAGED_PYTHON=%INSTALL_DIR%\\python\\runtime\\python.exe"
set "EXISTING_VENV_PYTHON=%INSTALL_DIR%\\venvs\\venv\\Scripts\\python.exe"
set "PYTHON_CMD="

> "%LOG_FILE%" echo [%date% %time%] {install_info.app_name} uninstall started

if exist "%PACKAGED_PYTHON%" (
    set "PYTHON_CMD=%PACKAGED_PYTHON%"
) else if exist "%EXISTING_VENV_PYTHON%" (
    set "PYTHON_CMD=%EXISTING_VENV_PYTHON%"
) else (
    call :resolve_python
    if not defined PYTHON_CMD (
        >> "%LOG_FILE%" echo Python not found in PATH, skip uninstall.py
        goto FORCE_KILL
    )
)

REM Check if uninstall.py exists
if exist "%INSTALL_DIR%\\uninstall.py" (
    "%PYTHON_CMD%" "%INSTALL_DIR%\\uninstall.py" --log-file "%LOG_FILE%" >> "%LOG_FILE%" 2>&1
    >> "%LOG_FILE%" echo [%date% %time%] uninstall.py completed
    exit /b 0
)

REM Fallback: Stop services using taosanode_service.py
if exist "%INSTALL_DIR%\\bin\\taosanode_service.py" (
    "%PYTHON_CMD%" "%INSTALL_DIR%\\bin\\taosanode_service.py" stop >> "%LOG_FILE%" 2>&1
    "%PYTHON_CMD%" "%INSTALL_DIR%\\bin\\taosanode_service.py" model-stop all >> "%LOG_FILE%" 2>&1
)

:FORCE_KILL
REM Kill only taosanode-related python processes
for /f "tokens=2" %%i in ('tasklist /FI "IMAGENAME eq python.exe" /FO LIST ^| findstr /C:"PID:"') do (
    wmic process where "ProcessId=%%i and CommandLine like '%%taosanode%%'" delete 2>nul
)

:END
>> "%LOG_FILE%" echo [%date% %time%] Uninstall helper completed
exit /b 0

:resolve_python
python --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=python"
    exit /b 0
)
python3 --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=python3"
    exit /b 0
)
set "PYTHON_CMD="
exit /b 1
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

    # Icon file path (relative to install_dir)
    icon_file = install_dir_iss + '/favicon.ico'

    # Replace placeholders
    iss_content = iss_content.replace('{{VERSION}}', tdgpt_version.version)
    iss_content = iss_content.replace('{{APP_NAME}}', install_info.app_name)
    iss_content = iss_content.replace('{{PRODUCT_FULL_NAME}}', install_info.product_full_name)
    iss_content = iss_content.replace('{{PACKAGE_NAME}}', install_info.package_name)
    iss_content = iss_content.replace('{{SOURCE_DIR}}', install_dir_iss)
    iss_content = iss_content.replace('{{OUTPUT_DIR}}', release_dir_iss)
    iss_content = iss_content.replace('{{ICON_FILE}}', icon_file)
    iss_content = iss_content.replace('{{RESOURCE_PACKAGE_URL}}', install_info.resource_package_url)

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

    winsw_dest = os.path.join(bin_dir, f"{WINSW_BASENAME}.exe")

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
    config_path = os.path.join(bin_dir, f"{WINSW_BASENAME}.xml")
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
    copy_packaged_python_runtime()
    copy_model_files()
    copy_enterprise_files()
    copy_service_scripts()  # New unified service scripts
    copy_icon_file()  # Copy icon file from enterprise
    download_winsw()  # Download winsw for Windows service support
    create_winsw_config()  # Create winsw configuration

    # Create batch scripts
    create_install_script()
    create_uninstall_script()

    if install_info.skip_model_check:
        logging.warning("=" * 60)
        logging.warning("WARNING: Model validation SKIPPED (--skip-model-check)")
        logging.warning("This is for TESTING ONLY - NOT for production!")
        logging.warning("=" * 60)
    else:
        logging.info("Base installer does not require bundled runtime, venv, or model validation.")

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
