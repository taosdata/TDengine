#!/usr/bin/env python3
"""
TDGPT Windows installation script.
"""

import argparse
import os
import platform
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path
from typing import Dict, List


if platform.system().lower() != "windows":
    print("Error: this script is for Windows only.")
    sys.exit(1)


try:
    import ctypes

    _kernel32 = ctypes.windll.kernel32
    _kernel32.SetConsoleMode(_kernel32.GetStdHandle(-11), 7)
    COLORS_ENABLED = True
except Exception:
    COLORS_ENABLED = False


class Colors:
    RED = "\033[0;31m" if COLORS_ENABLED else ""
    GREEN = "\033[1;32m" if COLORS_ENABLED else ""
    YELLOW = "\033[1;33m" if COLORS_ENABLED else ""
    BLUE = "\033[0;34m" if COLORS_ENABLED else ""
    NC = "\033[0m" if COLORS_ENABLED else ""


INSTALL_DIR = Path(__file__).resolve().parent
LOG_DIR = INSTALL_DIR / "log"
REQUIREMENTS_DIR = INSTALL_DIR / "requirements"
VENVS_DIR = INSTALL_DIR / "venvs"

REQUIRED_MODELS = ["tdtsfm", "timemoe"]
OPTIONAL_MODELS = ["chronos", "timesfm", "moirai", "moment-large"]

VENV_CONFIGS: Dict[str, Dict[str, str]] = {
    "venv": {
        "requirements": "requirements_ess.txt",
        "description": "Main virtual environment",
    },
    "timesfm_venv": {
        "requirements": "requirements_timesfm.txt",
        "description": "TimesFM virtual environment",
    },
    "moirai_venv": {
        "requirements": "requirements_moirai.txt",
        "description": "Moirai virtual environment",
    },
    "chronos_venv": {
        "requirements": "requirements_chronos.txt",
        "description": "Chronos virtual environment",
    },
    "momentfm_venv": {
        "requirements": "requirements_moment.txt",
        "description": "MomentFM virtual environment",
    },
}

MODEL_VENV_MAP = {
    "chronos": "chronos_venv",
    "timesfm": "timesfm_venv",
    "moirai": "moirai_venv",
    "moment": "momentfm_venv",
}


class WindowsInstaller:
    def __init__(
        self,
        offline: bool = False,
        all_models: bool = False,
        selected_models: List[str] | None = None,
        pip_index_url: str = "",
        pip_trusted_host: str = "",
        skip_service_install: bool = False,
        log_file: str = "",
    ):
        self.offline = offline
        self.all_models = all_models
        self.selected_models = selected_models or []
        self.install_dir = INSTALL_DIR
        self.log_dir = LOG_DIR
        self.requirements_dir = REQUIREMENTS_DIR
        self.venvs_dir = VENVS_DIR
        self.python_cmd = ""
        self.python_version = ""
        self.pip_index_url = pip_index_url or os.environ.get("PIP_INDEX_URL", "")
        self.pip_trusted_host = pip_trusted_host or os.environ.get("PIP_TRUSTED_HOST", "")
        self.skip_service_install = skip_service_install
        self.service_install_success = False
        self.log_file = Path(log_file) if log_file else self.log_dir / "install.log"
        self.console_is_tty = bool(getattr(sys.stdout, "isatty", lambda: False)())

    def _append_file_log(self, level: str, message: str) -> None:
        if not self.console_is_tty:
            return

        self.log_dir.mkdir(parents=True, exist_ok=True)
        with self.log_file.open("a", encoding="utf-8") as file_obj:
            file_obj.write(f"{level}: {message}\n")

    def _emit(self, level: str, prefix: str, color: str, message: str) -> None:
        print(f"{color}{prefix}{Colors.NC} {message}")
        self._append_file_log(level, message)

    def print_info(self, message: str) -> None:
        self._emit("INFO", "[INFO]", Colors.GREEN, message)

    def print_warning(self, message: str) -> None:
        self._emit("WARNING", "[WARNING]", Colors.YELLOW, message)

    def print_error(self, message: str) -> None:
        self._emit("ERROR", "[ERROR]", Colors.RED, message)

    def print_success(self, message: str) -> None:
        self._emit("SUCCESS", "[SUCCESS]", Colors.GREEN, message)

    def print_progress(self, current: int, total: int, message: str) -> None:
        percent = int((current / total) * 100)
        if self.console_is_tty:
            bar_length = 40
            filled = int((current / total) * bar_length)
            bar = "#" * filled + "-" * (bar_length - filled)
            print(
                f"\r{Colors.BLUE}[{bar}] {percent}%{Colors.NC} {message}",
                end="",
                flush=True,
            )
            if current == total:
                print()
        else:
            print(f"[PROGRESS] {percent}% {message}")

    def add_pip_options(self, pip_args: List[str]) -> List[str]:
        if self.pip_index_url:
            pip_args.extend(["-i", self.pip_index_url])
        if self.pip_trusted_host:
            pip_args.extend(["--trusted-host", self.pip_trusted_host])
        return pip_args

    def get_venv_path(self, venv_name: str) -> Path:
        return self.venvs_dir / venv_name

    def get_requirements_file(self, name: str) -> Path:
        return self.requirements_dir / name

    def check_admin_privileges(self) -> None:
        try:
            if not ctypes.windll.shell32.IsUserAnAdmin():
                self.print_warning(
                    "Administrator privileges are recommended for service registration."
                )
        except Exception:
            self.print_warning("Unable to determine administrator privileges.")

    def check_disk_space(self) -> bool:
        try:
            stat = shutil.disk_usage(self.install_dir.drive)
            free_gb = stat.free / (1024 ** 3)
            if free_gb < 20:
                self.print_error(
                    f"Insufficient disk space: {self.install_dir.drive} has only {free_gb:.2f} GB free."
                )
                return False
            self.print_info(
                f"Disk space check passed: {self.install_dir.drive} has {free_gb:.2f} GB free."
            )
            return True
        except Exception as exc:
            self.print_warning(f"Unable to check disk space: {exc}")
            return True

    def check_python_version(self) -> bool:
        self.print_info("Checking Python environment...")
        python_commands = ["python", "python3", "python3.10", "python3.11", "python3.12"]

        for cmd in python_commands:
            try:
                result = subprocess.run(
                    [cmd, "--version"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                    check=False,
                )
                output = (result.stdout or result.stderr).strip()
                if result.returncode != 0 or not output.startswith("Python "):
                    continue

                version = output.split()[1]
                major, minor = map(int, version.split(".")[:2])
                if major == 3 and minor in [10, 11, 12]:
                    self.python_cmd = cmd
                    self.python_version = version
                    self.print_success(f"Found Python {version} via `{cmd}`.")
                    return True
            except (FileNotFoundError, subprocess.TimeoutExpired, ValueError):
                continue

        self.print_error("Python 3.10/3.11/3.12 was not found in PATH.")
        return False

    def check_pip_version(self) -> bool:
        try:
            result = subprocess.run(
                [self.python_cmd, "-m", "pip", "--version"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            if result.returncode == 0:
                self.print_info(result.stdout.strip())
                return True
        except Exception as exc:
            self.print_error(f"Failed to check pip version: {exc}")
            return False

        self.print_error("pip is unavailable.")
        return False

    def create_directories(self) -> bool:
        self.print_info("Creating directory structure...")
        directories = [
            self.log_dir,
            self.install_dir / "data" / "pids",
            self.install_dir / "model",
            self.requirements_dir,
            self.venvs_dir,
        ]
        try:
            for directory in directories:
                directory.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as exc:
            self.print_error(f"Failed to create directories: {exc}")
            return False

    def extract_models(self) -> bool:
        self.print_info("Extracting model archives if present...")
        model_dir = self.install_dir / "model"

        models_to_extract = list(REQUIRED_MODELS)
        if self.all_models:
            models_to_extract.extend(OPTIONAL_MODELS)
        else:
            for model_name in OPTIONAL_MODELS:
                if model_name in self.selected_models or model_name.replace("-large", "") in self.selected_models:
                    models_to_extract.append(model_name)

        for model_name in models_to_extract:
            archive = model_dir / f"{model_name}.tar.gz"
            if not archive.exists():
                self.print_info(f"Model archive not found, skipping: {archive.name}")
                continue

            try:
                self.print_info(f"Extracting {archive.name}...")
                with tarfile.open(archive, "r:gz") as tar_obj:
                    tar_obj.extractall(model_dir)
            except Exception as exc:
                self.print_error(f"Failed to extract {archive.name}: {exc}")
                return False

        return True

    def create_venv(self, venv_name: str, requirements_file: str, description: str) -> bool:
        venv_path = self.get_venv_path(venv_name)
        req_file = self.get_requirements_file(requirements_file)

        if self.offline and venv_path.exists():
            self.print_info(f"Offline mode: reuse existing {description}.")
            return True

        if venv_path.exists() and not self.offline:
            self.print_info(f"Removing existing {description} before reinstall.")
            shutil.rmtree(venv_path, ignore_errors=True)

        try:
            self.print_progress(1, 4, f"Creating {description}")
            subprocess.run(
                [self.python_cmd, "-m", "venv", str(venv_path)],
                check=True,
                timeout=180,
            )

            python_exe = venv_path / "Scripts" / "python.exe"
            pip_exe = venv_path / "Scripts" / "pip.exe"

            self.print_progress(2, 4, "Upgrading pip")
            upgrade_cmd = [str(python_exe), "-m", "pip", "install", "--upgrade", "pip"]
            self.add_pip_options(upgrade_cmd)
            subprocess.run(
                upgrade_cmd,
                check=True,
                timeout=600,
            )

            if req_file.exists():
                self.print_progress(3, 4, f"Installing dependencies from {req_file.name}")
                install_cmd = [str(pip_exe), "install", "-r", str(req_file)]
                self.add_pip_options(install_cmd)
                subprocess.run(
                    install_cmd,
                    check=True,
                    timeout=3600,
                )
            else:
                self.print_warning(f"Requirements file not found: {req_file}")

            self.print_progress(4, 4, f"{description} ready")
            self.print_success(f"{description} created successfully.")
            return True
        except subprocess.TimeoutExpired:
            if self.console_is_tty:
                print()
            self.print_error(f"Timed out while creating {description}.")
            return False
        except subprocess.CalledProcessError as exc:
            if self.console_is_tty:
                print()
            self.print_error(f"Failed to create {description}: {exc}")
            return False
        except Exception as exc:
            if self.console_is_tty:
                print()
            self.print_error(f"Unexpected error while creating {description}: {exc}")
            return False

    def install_venvs(self) -> bool:
        if not self.create_venv("venv", "requirements_ess.txt", "main virtual environment"):
            return False

        if self.all_models:
            targets = {
                key: value
                for key, value in VENV_CONFIGS.items()
                if key != "venv"
            }
        else:
            targets = {}
            for model_name in self.selected_models:
                venv_name = MODEL_VENV_MAP.get(model_name)
                if venv_name and venv_name in VENV_CONFIGS:
                    targets[venv_name] = VENV_CONFIGS[venv_name]

        for venv_name, config in targets.items():
            if not self.create_venv(venv_name, config["requirements"], config["description"]):
                return False

        return True

    def install_service(self) -> bool:
        if self.skip_service_install:
            self.print_info("Service installation skipped by installer option.")
            self.service_install_success = False
            return True

        service_script = self.install_dir / "bin" / "taosanode_service.py"
        python_exe = self.get_venv_path("venv") / "Scripts" / "python.exe"

        if not service_script.exists() or not python_exe.exists():
            self.print_warning("Service installer prerequisites are missing, skip service registration.")
            self.service_install_success = False
            return True

        try:
            subprocess.run(
                [str(python_exe), str(service_script), "install-service"],
                check=True,
                timeout=120,
            )
            self.service_install_success = True
            self.print_success("Windows service installed successfully.")
            return True
        except subprocess.CalledProcessError as exc:
            self.service_install_success = False
            self.print_warning(f"Service installation failed: {exc}")
            return True
        except Exception as exc:
            self.service_install_success = False
            self.print_warning(f"Service installation raised an exception: {exc}")
            return True

    def append_install_summary(self) -> None:
        lines = [
            "",
            "=" * 72,
            "TDGPT Windows Installation Summary",
            "=" * 72,
            f"Install directory: {self.install_dir}",
            f"Python version: {self.python_version or 'Unknown'}",
            f"Install mode: {'Offline' if self.offline else 'Online'}",
            "Model scope: "
            + (
                "All optional models"
                if self.all_models
                else ", ".join(self.selected_models) if self.selected_models else "Required models only"
            ),
            f"Pip index: {self.pip_index_url or 'Default'}",
            f"Pip trusted host: {self.pip_trusted_host or 'Default'}",
            f"Service install requested: {'No' if self.skip_service_install else 'Yes'}",
            f"Service install result: {'Installed' if self.service_install_success else 'Skipped/Not installed'}",
            "Virtual environments:",
        ]
        for venv_name in VENV_CONFIGS:
            python_exe = self.get_venv_path(venv_name) / "Scripts" / "python.exe"
            status = "OK" if python_exe.exists() else "NO"
            lines.append(f"  [{status}] {venv_name}")
        lines.append("=" * 72)

        if self.console_is_tty:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            with self.log_file.open("a", encoding="utf-8") as file_obj:
                file_obj.write("\n".join(lines) + "\n")
        else:
            for line in lines:
                print(line)

    def run(self) -> bool:
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.print_info("=" * 60)
        self.print_info("TDGPT Windows Installer")
        self.print_info("=" * 60)
        self.print_info(f"Install directory: {self.install_dir}")
        self.print_info(f"Requirements directory: {self.requirements_dir}")
        self.print_info(f"Virtual environments directory: {self.venvs_dir}")
        self.print_info(f"Log file: {self.log_file}")
        self.print_info("=" * 60)

        self.check_admin_privileges()

        if not self.check_disk_space():
            return False
        if not self.check_python_version():
            return False
        if not self.check_pip_version():
            return False
        if not self.create_directories():
            return False
        if not self.extract_models():
            return False
        if not self.install_venvs():
            return False
        if not self.install_service():
            return False

        self.append_install_summary()

        self.print_success("TDGPT installation completed.")
        self.print_info(f"Start service: net start Taosanode")
        self.print_info(f"Stop service: net stop Taosanode")
        self.print_info(f"Script start: {self.install_dir / 'bin' / 'start-taosanode.bat'}")
        self.print_info(f"Script stop: {self.install_dir / 'bin' / 'stop-taosanode.bat'}")
        return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="TDGPT Windows installation script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python install.py
  python install.py -o
  python install.py --model moirai --model moment
  python install.py --pip-index-url https://pypi.tuna.tsinghua.edu.cn/simple --pip-trusted-host pypi.tuna.tsinghua.edu.cn
  python install.py --skip-service-install
        """.strip(),
    )
    parser.add_argument(
        "-o",
        "--offline",
        action="store_true",
        help="Reuse existing virtual environments when possible.",
    )
    parser.add_argument(
        "-a",
        "--all-models",
        action="store_true",
        help="Install all optional model virtual environments.",
    )
    parser.add_argument(
        "--model",
        action="append",
        dest="models",
        choices=["chronos", "timesfm", "moirai", "moment"],
        help="Install a specific optional model virtual environment.",
    )
    parser.add_argument(
        "--pip-index-url",
        help="Custom pip index URL used for dependency installation.",
    )
    parser.add_argument(
        "--pip-trusted-host",
        help="Optional trusted host used together with the pip index URL.",
    )
    parser.add_argument(
        "--skip-service-install",
        action="store_true",
        help="Do not register the Windows service during installation.",
    )
    parser.add_argument(
        "--log-file",
        help="Installation log file path. Defaults to <install_dir>\\log\\install.log.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    installer = WindowsInstaller(
        offline=args.offline,
        all_models=args.all_models,
        selected_models=args.models or [],
        pip_index_url=args.pip_index_url or "",
        pip_trusted_host=args.pip_trusted_host or "",
        skip_service_install=args.skip_service_install,
        log_file=args.log_file or "",
    )

    try:
        success = installer.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nInstallation interrupted by user.")
        sys.exit(1)
    except Exception as exc:
        print(f"\nUnexpected installation failure: {exc}")
        raise


if __name__ == "__main__":
    main()
