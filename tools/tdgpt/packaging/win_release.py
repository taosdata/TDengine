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
import tempfile
from pathlib import Path

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
        self.app_name = "TDgpt-OSS"
        self.product_full_name = "TDgpt-OSS - TDengine Analytics Node"
        self.model_dir = ""
        self.all_models = False
        self.offline = False
        self.package_mode = "online"
        self.iscc_path = ""
        self.skip_model_check = False
        self.uv_exe = ""
        self.python_version = "3.11"


tdgpt_version = TDGPTVersion("community", "1.0.0")
install_info = InstallInfo()
WINSW_BASENAME = "taosanode-winsw"


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
    parser.add_argument('--package-mode', choices=['online', 'full-offline', 'minimal-update'],
                        default='online',
                        help='Package type. online now builds the shared base installer; other modes are transitional.')
    parser.add_argument('--offline', action='store_true',
                        help='Deprecated alias for --package-mode full-offline')
    parser.add_argument('--iscc-path', type=str,
                        default=r"C:\Program Files (x86)\Inno Setup 6\ISCC.exe",
                        help='Path to Inno Setup compiler')
    parser.add_argument('--skip-model-check', action='store_true',
                        help='Skip model validation (for testing only, NOT for production)')
    parser.add_argument('--uv-exe', type=str, default="",
                        help='Path to uv.exe used for full offline packaging')
    parser.add_argument('--python-version', type=str, default="3.11",
                        help='Bundled Python version for full offline packaging (default: 3.11)')

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
        install_info.app_name = "TDgpt-Enterprise"
    else:
        install_info.product_name = "tdengine-tdgpt-oss"
        install_info.app_name = "TDgpt-OSS"
    install_info.product_full_name = f"{install_info.app_name} - TDengine Analytics Node"

    install_info.source_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    install_info.release_dir = args.output
    install_info.install_dir = os.path.join(install_info.release_dir, "install")
    package_mode = args.package_mode
    if args.offline:
        package_mode = "full-offline"

    package_suffix = ""
    if package_mode == "online":
        package_suffix = "-base"
    elif package_mode == "full-offline":
        package_suffix = "-full-offline"
    elif package_mode == "minimal-update":
        package_suffix = "-minimal-update"

    install_info.package_name = f"{install_info.product_name}-{args.version}-Windows-x64{package_suffix}"
    install_info.model_dir = args.model_dir
    install_info.all_models = args.all_models
    install_info.iscc_path = args.iscc_path
    install_info.package_mode = package_mode
    install_info.offline = package_mode == "full-offline"
    install_info.skip_model_check = args.skip_model_check
    install_info.uv_exe = args.uv_exe
    install_info.python_version = args.python_version

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
    logging.info(f"Package Mode: {install_info.package_mode}")
    logging.info(f"Offline Mode: {install_info.offline}")
    logging.info(f"Bundled Python Version: {install_info.python_version}")
    logging.info(f"uv.exe: {install_info.uv_exe or 'auto-detect'}")
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

    metadata = {
        "edition": tdgpt_version.ver_type,
        "app_name": install_info.app_name,
        "product_full_name": install_info.product_full_name,
        "package_name": install_info.package_name,
        "product_name": install_info.product_name,
        "version": tdgpt_version.version,
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


def run_command(cmd, env=None, cwd=None, capture_output=False):
    """Run a command and raise on failure."""
    logging.info("Running command: %s", " ".join(str(item) for item in cmd))
    result = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        capture_output=capture_output,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        if result.stdout:
            logging.error(result.stdout)
        if result.stderr:
            logging.error(result.stderr)
        raise RuntimeError(
            f"Command failed with exit code {result.returncode}: {' '.join(str(item) for item in cmd)}"
        )
    return result.stdout.strip() if capture_output else ""


def ensure_uv_exe():
    """Resolve uv.exe and cache it under packaging/bin for reuse."""
    package_cache = Path(__file__).resolve().parent / "bin" / "uv.exe"
    candidates = []

    if install_info.uv_exe:
        candidates.append(Path(install_info.uv_exe))
    candidates.append(package_cache)

    for candidate in candidates:
        if candidate.exists():
            logging.info(f"Using uv executable: {candidate}")
            return candidate

    try:
        result = subprocess.run(
            ["where", "uv"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            uv_path = Path(result.stdout.splitlines()[0].strip())
            package_cache.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(uv_path, package_cache)
            logging.info(f"Copied uv from PATH to local cache: {package_cache}")
            return package_cache
    except Exception as exc:
        logging.warning(f"Failed to locate uv in PATH: {exc}")

    logging.info("uv.exe was not found locally. Downloading it with the official installer script.")
    with tempfile.TemporaryDirectory(prefix="tdgpt-uv-bootstrap-") as temp_dir:
        escaped_dir = temp_dir.replace("'", "''")
        install_script = (
            f"$env:UV_UNMANAGED_INSTALL='{escaped_dir}'; "
            "Invoke-RestMethod https://astral.sh/uv/install.ps1 | Invoke-Expression"
        )
        run_command(
            [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                install_script,
            ]
        )
        downloaded = Path(temp_dir) / "uv.exe"
        if not downloaded.exists():
            raise FileNotFoundError(f"uv.exe was not created in {temp_dir}")
        package_cache.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(downloaded, package_cache)
        logging.info(f"Downloaded uv to local cache: {package_cache}")
        return package_cache


def find_python_runtime_home(root_dir):
    """Find the base Python directory inside a uv-managed install root."""
    candidates = []
    for python_exe in root_dir.rglob("python.exe"):
        parent = python_exe.parent
        if parent.name.lower() == "scripts":
            continue
        if (parent / "Lib").exists():
            candidates.append(parent)
    if not candidates:
        raise FileNotFoundError(f"Unable to locate a packaged python.exe under {root_dir}")
    candidates.sort(key=lambda item: (len(item.parts), len(str(item))))
    return candidates[0]


def create_offline_venv(base_python, venv_dir, requirements):
    """Create one offline virtual environment from the packaged runtime."""
    if venv_dir.exists():
        shutil.rmtree(venv_dir)
    run_command([str(base_python), "-m", "venv", str(venv_dir)])

    venv_python = venv_dir / "Scripts" / "python.exe"
    run_command([str(venv_python), "-m", "pip", "install", "--upgrade", "pip"])
    for req_name in requirements:
        req_path = Path(install_info.install_dir) / "requirements" / req_name
        if not req_path.exists():
            raise FileNotFoundError(f"Missing requirements file: {req_path}")
        run_command([str(venv_python), "-m", "pip", "install", "-r", str(req_path)])


def prepare_offline_packages():
    """Prepare bundled Python, venvs, and related assets for full offline packaging."""
    if install_info.package_mode != "full-offline":
        logging.info("Package mode is not full-offline, skipping bundled Python preparation")
        return

    logging.info("Preparing bundled offline runtime with uv-managed Python")
    uv_exe = ensure_uv_exe()
    python_root = Path(install_info.install_dir) / "python"
    runtime_dir = python_root / "runtime"
    venvs_dir = Path(install_info.install_dir) / "venvs"
    python_root.mkdir(parents=True, exist_ok=True)
    venvs_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="tdgpt-offline-runtime-", dir=install_info.release_dir) as temp_dir:
        uv_store = Path(temp_dir) / "uv-python-store"
        env = os.environ.copy()
        env["UV_PYTHON_INSTALL_DIR"] = str(uv_store)
        run_command([str(uv_exe), "python", "install", install_info.python_version], env=env)

        runtime_home = find_python_runtime_home(uv_store)
        if runtime_dir.exists():
            shutil.rmtree(runtime_dir)
        shutil.copytree(runtime_home, runtime_dir)
        logging.info(f"Copied uv-managed Python runtime to {runtime_dir}")

    shutil.copy2(uv_exe, python_root / "uv.exe")
    logging.info(f"Copied uv.exe to {python_root / 'uv.exe'}")

    base_python = runtime_dir / "python.exe"
    if not base_python.exists():
        raise FileNotFoundError(f"Packaged runtime python was not created: {base_python}")

    offline_venvs = {
        "venv": ["requirements_windows_core.txt", "requirements_tensorflow.txt"],
        "moirai_venv": ["requirements_moirai.txt"],
        "chronos_venv": ["requirements_chronos.txt"],
        "timesfm_venv": ["requirements_timesfm.txt"],
        "momentfm_venv": ["requirements_moment.txt"],
    }
    for venv_name, requirement_files in offline_venvs.items():
        logging.info(f"Preparing offline virtual environment: {venv_name}")
        create_offline_venv(base_python, venvs_dir / venv_name, requirement_files)


def copy_model_files():
    """Copy model files if model directory is specified"""
    if install_info.package_mode == "minimal-update":
        logging.info("Minimal update package does not include model files")
        return

    if not install_info.model_dir or not os.path.exists(install_info.model_dir):
        logging.info("No model directory specified or directory does not exist, skipping model files")
        return

    model_dir = os.path.join(install_info.install_dir, "model")
    os.makedirs(model_dir, exist_ok=True)

    if install_info.package_mode == "full-offline":
        shutil.copytree(install_info.model_dir, model_dir, dirs_exist_ok=True)
        logging.info(f"Copied bundled model directory from {install_info.model_dir}")
        return

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

    # start-taosanode.bat
    start_bat = os.path.join(bin_dir, "start-taosanode.bat")
    with open(start_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul
set "PYTHON_EXE=%~dp0..\\venvs\\venv\\Scripts\\python.exe"
if not exist "%PYTHON_EXE%" call :resolve_python
if not defined PYTHON_EXE (
    echo Python 3.10/3.11/3.12 was not found.
    echo Install Python and make sure python or python3 is available in PATH.
    echo Press Enter to close this window.
    pause >nul
    exit /b 1
)
REM Start taosanode service
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
        "%PYTHON_EXE%" "%~dp0taosanode_service.py" wait-ready --timeout 30
        if errorlevel 1 (
            echo Taosanode start command returned, but readiness was not confirmed within 30 seconds.
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
        "%PYTHON_EXE%" "%~dp0taosanode_service.py" wait-ready --timeout 30
        if errorlevel 1 (
            echo Taosanode start command returned, but readiness was not confirmed within 30 seconds.
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

:resolve_python
python --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_EXE=python"
    exit /b 0
)
python3 --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_EXE=python3"
    exit /b 0
)
set "PYTHON_EXE="
exit /b 1
""")
    logging.info("Created start-taosanode.bat")

    # stop-taosanode.bat
    stop_bat = os.path.join(bin_dir, "stop-taosanode.bat")
    with open(stop_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul
set "PYTHON_EXE=%~dp0..\\venvs\\venv\\Scripts\\python.exe"
if not exist "%PYTHON_EXE%" call :resolve_python
if not defined PYTHON_EXE (
    echo Python 3.10/3.11/3.12 was not found.
    exit /b 1
)
REM Stop taosanode service
sc query Taosanode >nul 2>&1
if not errorlevel 1 (
    "%PYTHON_EXE%" "%~dp0taosanode_service.py" stop-service %*
) else (
    "%PYTHON_EXE%" "%~dp0taosanode_service.py" stop %*
)
exit /b %errorlevel%

:resolve_python
python --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_EXE=python"
    exit /b 0
)
python3 --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_EXE=python3"
    exit /b 0
)
set "PYTHON_EXE="
exit /b 1
""")
    logging.info("Created stop-taosanode.bat")

    # status-taosanode.bat
    status_bat = os.path.join(bin_dir, "status-taosanode.bat")
    with open(status_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul
set "PYTHON_EXE=%~dp0..\\venvs\\venv\\Scripts\\python.exe"
if not exist "%PYTHON_EXE%" call :resolve_python
if not defined PYTHON_EXE (
    echo Python 3.10/3.11/3.12 was not found.
    pause
    exit /b 1
)
REM Show taosanode status
sc query Taosanode >nul 2>&1
if not errorlevel 1 (
    echo Windows service status:
    sc query Taosanode
    echo.
)
"%PYTHON_EXE%" "%~dp0taosanode_service.py" status %*
pause
exit /b %errorlevel%

:resolve_python
python --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_EXE=python"
    exit /b 0
)
python3 --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_EXE=python3"
    exit /b 0
)
set "PYTHON_EXE="
exit /b 1
""")
    logging.info("Created status-taosanode.bat")

    # start-model.bat
    start_model_bat = os.path.join(bin_dir, "start-model.bat")
    with open(start_model_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul
set "PYTHON_EXE=%~dp0..\\venvs\\venv\\Scripts\\python.exe"
if not exist "%PYTHON_EXE%" call :resolve_python
if not defined PYTHON_EXE (
    echo Python 3.10/3.11/3.12 was not found.
    echo Install Python and make sure python or python3 is available in PATH.
    echo Press Enter to close this window.
    pause >nul
    exit /b 1
)
REM Start model service
REM Usage: start-model.bat [model_name|all]
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

:resolve_python
python --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_EXE=python"
    exit /b 0
)
python3 --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_EXE=python3"
    exit /b 0
)
set "PYTHON_EXE="
exit /b 1
""")
    logging.info("Created start-model.bat")

    # stop-model.bat
    stop_model_bat = os.path.join(bin_dir, "stop-model.bat")
    with open(stop_model_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul
set "PYTHON_EXE=%~dp0..\\venvs\\venv\\Scripts\\python.exe"
if not exist "%PYTHON_EXE%" call :resolve_python
if not defined PYTHON_EXE (
    echo Python 3.10/3.11/3.12 was not found.
    exit /b 1
)
REM Stop model service
REM Usage: stop-model.bat [model_name|all]
if "%~1"=="" (
    echo Usage: stop-model.bat [model_name^|all]
    exit /b 1
)
"%PYTHON_EXE%" "%~dp0taosanode_service.py" model-stop %*
exit /b %errorlevel%

:resolve_python
python --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_EXE=python"
    exit /b 0
)
python3 --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_EXE=python3"
    exit /b 0
)
set "PYTHON_EXE="
exit /b 1
""")
    logging.info("Created stop-model.bat")

    # status-model.bat
    status_model_bat = os.path.join(bin_dir, "status-model.bat")
    with open(status_model_bat, 'w') as f:
        f.write("""@echo off
chcp 65001 >nul
set "PYTHON_EXE=%~dp0..\\venvs\\venv\\Scripts\\python.exe"
if not exist "%PYTHON_EXE%" call :resolve_python
if not defined PYTHON_EXE (
    echo Python 3.10/3.11/3.12 was not found.
    pause
    exit /b 1
)
REM Show model status
"%PYTHON_EXE%" "%~dp0taosanode_service.py" model-status %*
pause
exit /b %errorlevel%

:resolve_python
python --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_EXE=python"
    exit /b 0
)
python3 --version >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_EXE=python3"
    exit /b 0
)
set "PYTHON_EXE="
exit /b 1
""")
    logging.info("Created status-model.bat")


def create_install_script():
    """Create install.bat for post-installation setup"""
    install_bat = os.path.join(install_info.install_dir, "install.bat")
    all_models_flag = "-a" if install_info.all_models else ""
    package_mode_flag = f'--package-mode {install_info.package_mode}'

    with open(install_bat, 'w') as f:
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
set "PACKAGED_PYTHON=%INSTALL_DIR%\\python\\runtime\\python.exe"
set "EXISTING_VENV_PYTHON=%INSTALL_DIR%\\venvs\\venv\\Scripts\\python.exe"
set "PYTHON_CMD="

call :append_progress running 1 "Initializing {install_info.app_name} setup" "Preparing installation environment"

if exist "%PACKAGED_PYTHON%" (
    set "PYTHON_CMD=%PACKAGED_PYTHON%"
) else if exist "%EXISTING_VENV_PYTHON%" (
    set "PYTHON_CMD=%EXISTING_VENV_PYTHON%"
) else (
    call :resolve_python
    if not defined PYTHON_CMD (
        echo ERROR: Python not found in PATH
        echo Please install Python 3.10/3.11/3.12 and make sure python or python3 is available in PATH.
        call :append_progress error 100 "Installation failed" "Python 3.10/3.11/3.12 was not found in PATH"
        exit /b 1
    )
)

> "%LOG_FILE%" echo [%date% %time%] {install_info.app_name} installation started
>> "%LOG_FILE%" echo Install directory: %INSTALL_DIR%
>> "%LOG_FILE%" echo Package mode: {install_info.package_mode}
>> "%LOG_FILE%" echo Python command: %PYTHON_CMD%
>> "%LOG_FILE%" echo PIP_INDEX_URL=%PIP_INDEX_URL%
>> "%LOG_FILE%" echo Command args: %*

if "%PROGRESS_FILE%"=="" (
    "%PYTHON_CMD%" "%INSTALL_DIR%\\install.py" --log-file "%LOG_FILE%" {package_mode_flag} {all_models_flag} %* >> "%LOG_FILE%" 2>&1
) else (
    "%PYTHON_CMD%" "%INSTALL_DIR%\\install.py" --log-file "%LOG_FILE%" {package_mode_flag} {all_models_flag} --progress-file "%PROGRESS_FILE%" %* >> "%LOG_FILE%" 2>&1
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
""")
    logging.info("Created install.bat")


def create_uninstall_script():
    """Create uninstall.bat - called by Inno Setup during uninstallation"""
    uninstall_bat = os.path.join(install_info.install_dir, "uninstall.bat")
    with open(uninstall_bat, 'w') as f:
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
    iss_content = iss_content.replace('{{PACKAGE_MODE}}', install_info.package_mode)

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
    prepare_offline_packages()  # Prepare offline wheels if --offline flag is set
    copy_model_files()
    copy_enterprise_files()
    copy_service_scripts()  # New unified service scripts
    copy_icon_file()  # Copy icon file from enterprise
    download_winsw()  # Download winsw for Windows service support
    create_winsw_config()  # Create winsw configuration

    # Create batch scripts
    create_install_script()
    create_uninstall_script()

    # Validate bundled model directory only for package modes that embed models
    if not install_info.skip_model_check and install_info.package_mode == "full-offline":
        model_dir = os.path.join(install_info.install_dir, "model")

        if not os.path.exists(model_dir) or not os.listdir(model_dir):
            logging.error("=" * 60)
            logging.error("ERROR: Model directory is empty or missing")
            logging.error(f"Expected location: {model_dir}")
            logging.error("")
            logging.error("Bundled model files are required for this package mode.")
            logging.error("Please specify model directory using --model-dir option:")
            logging.error(f"  python {sys.argv[0]} -e {tdgpt_version.ver_type} -v {tdgpt_version.version} -m <model_dir>")
            logging.error("")
            logging.error("For testing only, you can skip this check with --skip-model-check")
            logging.error("=" * 60)
            return 1

        logging.info("Model validation passed")
    elif install_info.package_mode in {"online", "minimal-update"}:
        logging.info("Base/minimal installer skips bundled model validation")
    else:
        logging.warning("=" * 60)
        logging.warning("WARNING: Model validation SKIPPED (--skip-model-check)")
        logging.warning("This is for TESTING ONLY - NOT for production!")
        logging.warning("=" * 60)

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
