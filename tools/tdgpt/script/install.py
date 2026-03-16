#!/usr/bin/env python3
"""
TDGPT Windows Installation Script

Usage:
    python install.py [options]

Options:
    -o, --offline       Offline mode: skip existing virtual environments
    -a, --all-models    Install all model virtual environments
    -h, --help          Show help information

Examples:
    python install.py                    # Online mode, install required models only
    python install.py -o                 # Offline mode
    python install.py -a                 # Install all models
    python install.py -o -a              # Offline mode + all models
"""

import os
import sys
import platform
import subprocess
import argparse
import logging
import shutil
import tarfile
from pathlib import Path
from typing import Optional, List, Tuple

# Ensure this is a Windows platform
if platform.system().lower() != "windows":
    print("Error: This script is for Windows only.")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Color output (Windows 10+ supports ANSI)
try:
    import ctypes
    kernel32 = ctypes.windll.kernel32
    kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    COLORS_ENABLED = True
except:
    COLORS_ENABLED = False

class Colors:
    """ANSI color codes"""
    RED = '\033[0;31m' if COLORS_ENABLED else ''
    GREEN = '\033[1;32m' if COLORS_ENABLED else ''
    YELLOW = '\033[1;33m' if COLORS_ENABLED else ''
    BLUE = '\033[0;34m' if COLORS_ENABLED else ''
    NC = '\033[0m' if COLORS_ENABLED else ''  # No Color

# Default install path: auto-detected from script location (script is in the install root directory)
INSTALL_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

# Model configuration
REQUIRED_MODELS = ["tdtsfm", "timemoe"]
OPTIONAL_MODELS = ["chronos", "timesfm", "moirai", "moment-large"]

# Virtual environment configuration
VENV_CONFIGS = {
    "venv": {
        "requirements": "requirements_ess.txt",
        "description": "Main virtual environment"
    },
    "timesfm_venv": {
        "requirements": "requirements_timesfm.txt",
        "description": "TimesFM model environment"
    },
    "moirai_venv": {
        "requirements": "requirements_moirai.txt",
        "description": "Moirai model environment"
    },
    "chronos_venv": {
        "requirements": "requirements_chronos.txt",
        "description": "Chronos model environment"
    },
    "momentfm_venv": {
        "requirements": "requirements_moment.txt",
        "description": "MomentFM model environment"
    }
}


# Model-to-venv mapping
MODEL_VENV_MAP = {
    "chronos": "chronos_venv",
    "timesfm": "timesfm_venv",
    "moirai": "moirai_venv",
    "moment": "momentfm_venv",
}


class WindowsInstaller:
    """Windows installer"""

    def __init__(self, offline: bool = False, all_models: bool = False, selected_models: list = None):
        self.offline = offline
        self.all_models = all_models
        self.selected_models = selected_models or []
        self.install_dir = INSTALL_DIR
        self.python_cmd = None
        self.python_version = None
        self.pip_index_url = os.environ.get("PIP_INDEX_URL", "")

    def print_info(self, message: str):
        """Print info message"""
        print(f"{Colors.GREEN}[INFO]{Colors.NC} {message}")
        logger.info(message)

    def print_progress(self, current: int, total: int, message: str):
        """Print progress bar"""
        percent = int((current / total) * 100)
        bar_length = 50
        filled = int((current / total) * bar_length)
        bar = '█' * filled + '░' * (bar_length - filled)
        print(f"\r{Colors.BLUE}[{bar}] {percent}%{Colors.NC} {message}", end='', flush=True)
        if current == total:
            print()  # Newline after completion

    def print_warning(self, message: str):
        """Print warning message"""
        print(f"{Colors.YELLOW}[WARNING]{Colors.NC} {message}")
        logger.warning(message)

    def print_error(self, message: str):
        """Print error message"""
        print(f"{Colors.RED}[ERROR]{Colors.NC} {message}")
        logger.error(message)

    def print_success(self, message: str):
        """Print success message"""
        print(f"{Colors.GREEN}[SUCCESS]{Colors.NC} {message}")
        logger.info(message)

    def check_admin_privileges(self) -> bool:
        """Check administrator privileges"""
        try:
            import ctypes
            is_admin = ctypes.windll.shell32.IsUserAnAdmin()
            if not is_admin:
                self.print_warning("It is recommended to run this script with administrator privileges to ensure successful service registration")
                return False
            return True
        except:
            return False

    def check_disk_space(self) -> bool:
        """Check disk space"""
        try:
            drive = self.install_dir.drive
            stat = shutil.disk_usage(drive)
            free_gb = stat.free / (1024 ** 3)

            if free_gb < 20:
                self.print_error(f"Insufficient disk space: {drive} has only {free_gb:.2f} GB free, at least 20 GB recommended")
                return False

            self.print_info(f"Disk space check passed: {drive} has {free_gb:.2f} GB available")
            return True
        except Exception as e:
            self.print_warning(f"Unable to check disk space: {e}")
            return True

    def check_python_version(self) -> bool:
        """Check Python version"""
        self.print_info("Checking Python environment...")

        # Try different Python commands
        python_commands = ["python", "python3", "python3.10", "python3.11", "python3.12"]

        for cmd in python_commands:
            try:
                result = subprocess.run(
                    [cmd, "--version"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )

                if result.returncode == 0:
                    version_str = result.stdout.strip()
                    # Parse version number
                    parts = version_str.split()
                    if len(parts) >= 2:
                        version = parts[1]
                        major, minor = map(int, version.split('.')[:2])

                        if major == 3 and minor in [10, 11, 12]:
                            self.python_cmd = cmd
                            self.python_version = f"{major}.{minor}"
                            self.print_success(f"Found Python {version} ({cmd})")
                            return True
            except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
                continue

        self.print_error("Python 3.10/3.11/3.12 not found")
        self.print_error("Please download and install Python 3.10, 3.11, or 3.12 from https://www.python.org/")
        return False

    def check_pip_version(self) -> bool:
        """Check pip version"""
        try:
            result = subprocess.run(
                [self.python_cmd, "-m", "pip", "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )

            if result.returncode == 0:
                self.print_info(f"pip version: {result.stdout.strip()}")
                return True
            else:
                self.print_error("pip is not installed or unavailable")
                return False
        except Exception as e:
            self.print_error(f"Failed to check pip: {e}")
            return False

    def create_directories(self) -> bool:
        """Create directory structure"""
        self.print_info("Creating directory structure...")

        directories = [
            self.install_dir / "log",
            self.install_dir / "data" / "pids",
            self.install_dir / "model",
        ]

        try:
            for directory in directories:
                directory.mkdir(parents=True, exist_ok=True)
                self.print_info(f"Created directory: {directory}")

            return True
        except Exception as e:
            self.print_error(f"Failed to create directories: {e}")
            return False

    def extract_models(self) -> bool:
        """Extract model files"""
        self.print_info("Extracting model files...")

        model_dir = self.install_dir / "model"
        model_source_dir = self.install_dir / "model"  # Assuming model files are here

        # Required models
        for model_name in REQUIRED_MODELS:
            tar_file = model_source_dir / f"{model_name}.tar.gz"
            if tar_file.exists():
                try:
                    self.print_info(f"Extracting model: {model_name}")
                    with tarfile.open(tar_file, "r:gz") as tar:
                        tar.extractall(model_dir)
                except Exception as e:
                    self.print_error(f"Failed to extract model {model_name}: {e}")
                    return False
            else:
                self.print_warning(f"Model file does not exist: {tar_file}")

        # Optional models (selected via --model or -a)
        models_to_install = OPTIONAL_MODELS if self.all_models else [
            m for m in OPTIONAL_MODELS if m in self.selected_models or
            m.replace("-large", "") in self.selected_models
        ]
        for model_name in models_to_install:
                tar_file = model_source_dir / f"{model_name}.tar.gz"
                if tar_file.exists():
                    try:
                        self.print_info(f"Extracting optional model: {model_name}")
                        with tarfile.open(tar_file, "r:gz") as tar:
                            tar.extractall(model_dir)
                    except Exception as e:
                        self.print_warning(f"Failed to extract optional model {model_name}: {e}")
                else:
                    self.print_info(f"Optional model file does not exist: {tar_file}")

        return True

    def create_venv(self, venv_name: str, requirements_file: str, description: str) -> bool:
        """Create virtual environment"""
        venv_path = self.install_dir / venv_name

        # Offline mode: skip if venv already exists
        if self.offline and venv_path.exists():
            self.print_info(f"Offline mode: skipping existing {description} ({venv_name})")
            return True

        self.print_info(f"Creating {description} ({venv_name})...")

        try:
            # Create virtual environment
            self.print_progress(1, 4, f"Creating virtual environment {venv_name}...")
            subprocess.run(
                [self.python_cmd, "-m", "venv", str(venv_path)],
                check=True,
                timeout=120
            )

            # Activate virtual environment and install dependencies
            pip_cmd = venv_path / "Scripts" / "pip.exe"
            python_venv = venv_path / "Scripts" / "python.exe"

            # Upgrade pip
            self.print_progress(2, 4, f"Upgrading pip...")
            subprocess.run(
                [str(python_venv), "-m", "pip", "install", "--upgrade", "pip"],
                check=True,
                timeout=120,
                capture_output=True
            )

            # Install dependencies
            req_file = self.install_dir / requirements_file
            if req_file.exists():
                self.print_progress(3, 4, f"Installing dependencies...")

                pip_args = [str(pip_cmd), "install", "-r", str(req_file)]
                if self.pip_index_url:
                    pip_args.extend(["-i", self.pip_index_url])

                subprocess.run(
                    pip_args,
                    check=True,
                    timeout=1800  # 30 minute timeout
                )
            else:
                self.print_warning(f"Requirements file does not exist: {req_file}")

            self.print_progress(4, 4, f"{description} creation completed")
            self.print_success(f"{description} created successfully")
            return True

        except subprocess.TimeoutExpired:
            print()  # Newline
            self.print_error(f"Creating {description} timed out")
            return False
        except subprocess.CalledProcessError as e:
            print()  # Newline
            self.print_error(f"Failed to create {description}: {e}")
            return False
        except Exception as e:
            print()  # Newline
            self.print_error(f"Error occurred while creating {description}: {e}")
            return False

    def install_venvs(self) -> bool:
        """Install virtual environments"""
        self.print_info("Starting virtual environment installation...")

        # Main virtual environment (required)
        if not self.create_venv("venv", "requirements_ess.txt", "Main virtual environment"):
            return False

        # Additional virtual environments (selected via --model or -a)
        if self.all_models:
            venvs_to_install = {k: v for k, v in VENV_CONFIGS.items() if k != "venv"}
        else:
            venvs_to_install = {
                MODEL_VENV_MAP[m]: VENV_CONFIGS[MODEL_VENV_MAP[m]]
                for m in self.selected_models
                if m in MODEL_VENV_MAP and MODEL_VENV_MAP[m] in VENV_CONFIGS
            }

        for venv_name, config in venvs_to_install.items():
            self.create_venv(venv_name, config["requirements"], config["description"])

        return True

    def install_service(self) -> bool:
        """Install Windows service"""
        self.print_info("Installing Windows service...")

        service_script = self.install_dir / "bin" / "taosanode_service.py"
        if not service_script.exists():
            self.print_warning("Service management script does not exist, skipping service installation")
            return True

        try:
            # Use taosanode_service.py to install the service
            venv_python = self.install_dir / "venv" / "Scripts" / "python.exe"
            subprocess.run(
                [str(venv_python), str(service_script), "install"],
                check=True,
                timeout=60
            )

            self.print_success("Windows service installed successfully")
            return True
        except subprocess.CalledProcessError as e:
            self.print_warning(f"Service installation failed: {e}")
            self.print_info("You can install the service manually later")
            return True
        except Exception as e:
            self.print_warning(f"Error occurred during service installation: {e}")
            return True

    def generate_install_report(self) -> bool:
        """Generate installation report"""
        self.print_info("Generating installation report...")

        report_file = self.install_dir / "install.log"

        try:
            with open(report_file, "w", encoding="utf-8") as f:
                f.write("=" * 60 + "\n")
                f.write("TDGPT Windows Installation Report\n")
                f.write("=" * 60 + "\n\n")
                f.write(f"Install directory: {self.install_dir}\n")
                f.write(f"Python version: {self.python_version}\n")
                f.write(f"Install mode: {'Offline' if self.offline else 'Online'}\n")
                f.write(f"Model scope: {'All models' if self.all_models else ', '.join(self.selected_models) if self.selected_models else 'Required models only'}\n")
                f.write(f"pip index: {self.pip_index_url or 'Default'}\n")
                f.write("\n")
                f.write("Installed virtual environments:\n")
                for venv_name in VENV_CONFIGS.keys():
                    venv_path = self.install_dir / venv_name
                    if venv_path.exists():
                        f.write(f"  ✓ {venv_name}\n")
                    else:
                        f.write(f"  ✗ {venv_name}\n")
                f.write("\n")
                f.write("=" * 60 + "\n")

            self.print_success(f"Installation report saved: {report_file}")
            return True
        except Exception as e:
            self.print_warning(f"Failed to generate installation report: {e}")
            return True

    def run(self) -> bool:
        """Execute installation"""
        self.print_info("=" * 60)
        self.print_info("TDGPT Windows Installer")
        self.print_info("=" * 60)
        self.print_info(f"Install directory: {self.install_dir}")
        self.print_info(f"Install mode: {'Offline' if self.offline else 'Online'}")
        self.print_info(f"Model scope: {'All models' if self.all_models else ', '.join(self.selected_models) if self.selected_models else 'Required models only'}")
        self.print_info("=" * 60)

        # 1. Environment check
        if not self.check_admin_privileges():
            pass  # Warning only, does not block installation

        if not self.check_disk_space():
            return False

        if not self.check_python_version():
            return False

        if not self.check_pip_version():
            return False

        # 2. Create directory structure
        if not self.create_directories():
            return False

        # 3. Extract model files
        if not self.extract_models():
            return False

        # 4. Create virtual environments
        if not self.install_venvs():
            return False

        # 5. Install service
        self.install_service()

        # 6. Generate installation report
        self.generate_install_report()

        # Done
        self.print_info("=" * 60)
        self.print_success("TDGPT installation completed!")
        self.print_info("=" * 60)
        self.print_info(f"Install directory: {self.install_dir}")
        self.print_info(f"Log directory: {self.install_dir / 'log'}")
        self.print_info(f"Data directory: {self.install_dir / 'data'}")
        self.print_info(f"Model directory: {self.install_dir / 'model'}")
        self.print_info("")
        self.print_info("Start service:")
        self.print_info(f"  {self.install_dir / 'bin' / 'start-taosanode.bat'}")
        self.print_info("")
        self.print_info("Check status:")
        self.print_info(f"  {self.install_dir / 'bin' / 'status-taosanode.bat'}")
        self.print_info("=" * 60)

        return True


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="TDGPT Windows Installation Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python install.py                    # Online mode, install required models only
  python install.py -o                 # Offline mode
  python install.py -a                 # Install all models
  python install.py --model moirai --model moment  # Install specific models
  python install.py -o --model moirai  # Offline mode + specific models
        """
    )

    parser.add_argument(
        "-o", "--offline",
        action="store_true",
        help="Offline mode: skip existing virtual environments"
    )

    parser.add_argument(
        "-a", "--all-models",
        action="store_true",
        help="Install all model virtual environments"
    )

    parser.add_argument(
        "--model",
        action="append",
        dest="models",
        choices=["chronos", "timesfm", "moirai", "moment"],
        help="Install specific optional model (can be repeated)"
    )

    args = parser.parse_args()

    # Create installer and execute
    installer = WindowsInstaller(
        offline=args.offline,
        all_models=args.all_models,
        selected_models=args.models or []
    )

    try:
        success = installer.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nInstallation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nError occurred during installation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
