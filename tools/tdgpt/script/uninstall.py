#!/usr/bin/env python3
"""
TDgpt Windows uninstallation script.
"""

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path


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
    NC = "\033[0m" if COLORS_ENABLED else ""


INSTALL_DIR = Path(__file__).resolve().parent
LOG_DIR = INSTALL_DIR / "log"
VENVS_DIR = INSTALL_DIR / "venvs"
PACKAGE_METADATA_FILE = INSTALL_DIR / "cfg" / "package-metadata.json"


def load_package_metadata() -> dict[str, str]:
    defaults = {
        "edition": "community",
        "app_name": "TDgpt",
        "product_full_name": "TDgpt - TDengine Analytics Node",
    }
    if not PACKAGE_METADATA_FILE.exists():
        return defaults
    try:
        with PACKAGE_METADATA_FILE.open("r", encoding="utf-8") as file_obj:
            payload = json.load(file_obj)
    except Exception:
        return defaults
    if not isinstance(payload, dict):
        return defaults
    defaults.update({str(key): str(value) for key, value in payload.items() if value is not None})
    return defaults


PACKAGE_METADATA = load_package_metadata()
APP_DISPLAY_NAME = PACKAGE_METADATA.get("app_name", "TDgpt")


class WindowsUninstaller:
    def __init__(
        self,
        keep_all: bool = False,
        remove_venv: bool = False,
        remove_data: bool = False,
        remove_model: bool = False,
        log_file: str = "",
    ):
        self.keep_all = keep_all
        self.remove_venv = remove_venv
        self.remove_data = remove_data
        self.remove_model = remove_model
        self.install_dir = INSTALL_DIR
        self.log_dir = LOG_DIR
        self.venvs_dir = VENVS_DIR
        self.log_file = Path(log_file) if log_file else self.log_dir / "uninstall.log"
        self.stdout_redirected_to_log = os.environ.get("TDGPT_LOG_REDIRECTED", "") == "1"

    def _append_file_log(self, level: str, message: str) -> None:
        if self.stdout_redirected_to_log:
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

    def stop_services(self) -> None:
        self.print_info(f"Stopping {APP_DISPLAY_NAME} services...")
        service_script = self.install_dir / "bin" / "taosanode_service.py"
        python_exe = self.venvs_dir / "venv" / "Scripts" / "python.exe"

        if service_script.exists() and python_exe.exists():
            for command in (["stop"], ["model-stop", "all"]):
                try:
                    subprocess.run(
                        [str(python_exe), str(service_script), *command],
                        capture_output=True,
                        timeout=60,
                        check=False,
                    )
                except Exception as exc:
                    self.print_warning(f"Failed to run {' '.join(command)}: {exc}")

        time.sleep(2)

        pid_files = [self.install_dir / "taosanode.pid"]
        pid_dir = self.install_dir / "data" / "pids"
        if pid_dir.exists():
            pid_files.extend(pid_dir.glob("*.pid"))

        for pid_file in pid_files:
            try:
                if not pid_file.exists():
                    continue
                pid = pid_file.read_text(encoding="utf-8").strip()
                if pid.isdigit():
                    subprocess.run(
                        ["taskkill", "/F", "/T", "/PID", pid],
                        capture_output=True,
                        timeout=15,
                        check=False,
                    )
            except Exception as exc:
                self.print_warning(f"Failed to terminate PID from {pid_file.name}: {exc}")

    def uninstall_service(self) -> None:
        self.print_info("Uninstalling Windows service...")
        service_script = self.install_dir / "bin" / "taosanode_service.py"
        python_exe = self.venvs_dir / "venv" / "Scripts" / "python.exe"

        if not service_script.exists() or not python_exe.exists():
            self.print_info("Service wrapper or Python runtime is missing, skip service uninstall.")
            return

        try:
            subprocess.run(
                [str(python_exe), str(service_script), "uninstall-service"],
                capture_output=True,
                timeout=120,
                check=False,
            )
        except Exception as exc:
            self.print_warning(f"Service uninstall command failed: {exc}")

    def remove_files(self) -> None:
        self.print_info("Removing packaged files...")

        if self.keep_all:
            self.print_info("Keep-all mode enabled, skip file deletion.")
            return

        directories = [
            self.install_dir / "bin",
            self.install_dir / "lib",
            self.install_dir / "resource",
            self.install_dir / "requirements",
        ]

        if self.remove_venv:
            directories.append(self.venvs_dir)
        if self.remove_data:
            directories.append(self.install_dir / "data")
        if self.remove_model:
            directories.append(self.install_dir / "model")

        for directory in directories:
            if not directory.exists():
                continue
            try:
                shutil.rmtree(directory, ignore_errors=False)
                self.print_info(f"Removed directory: {directory.name}")
            except Exception as exc:
                self.print_warning(f"Failed to remove {directory}: {exc}")

        root_files = [
            self.install_dir / "install.bat",
            self.install_dir / "install.py",
            self.install_dir / "uninstall.bat",
            self.install_dir / "taosanode.pid",
            self.install_dir / "favicon.ico",
        ]

        for file_path in root_files:
            try:
                if file_path.exists():
                    file_path.unlink()
                    self.print_info(f"Removed file: {file_path.name}")
            except Exception:
                continue

    def append_uninstall_summary(self) -> None:
        lines = [
            "",
            "=" * 72,
            f"{APP_DISPLAY_NAME} Windows Uninstall Summary",
            "=" * 72,
            f"Install directory: {self.install_dir}",
            f"Keep all: {'Yes' if self.keep_all else 'No'}",
            f"Remove venvs: {'Yes' if self.remove_venv else 'No'}",
            f"Remove data: {'Yes' if self.remove_data else 'No'}",
            f"Remove model: {'Yes' if self.remove_model else 'No'}",
            "Preserved directories:",
        ]
        for name, path in (
            ("cfg", self.install_dir / "cfg"),
            ("log", self.log_dir),
            ("venvs", self.venvs_dir),
            ("data", self.install_dir / "data"),
            ("model", self.install_dir / "model"),
        ):
            if path.exists():
                lines.append(f"  [OK] {name}")
        lines.append("=" * 72)

        if self.stdout_redirected_to_log:
            for line in lines:
                print(line)
        else:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            with self.log_file.open("a", encoding="utf-8") as file_obj:
                file_obj.write("\n".join(lines) + "\n")

    def run(self) -> bool:
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.print_info("=" * 60)
        self.print_info(f"{APP_DISPLAY_NAME} Windows Uninstaller")
        self.print_info("=" * 60)
        self.print_info(f"Install directory: {self.install_dir}")
        self.print_info(f"Uninstall log: {self.log_file}")
        self.print_info("=" * 60)

        self.stop_services()
        self.uninstall_service()
        self.remove_files()
        self.append_uninstall_summary()
        self.print_success(f"{APP_DISPLAY_NAME} uninstall flow completed.")
        return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="TDgpt Windows uninstallation script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python uninstall.py
  python uninstall.py --remove-venv
  python uninstall.py --remove-data --remove-model
  python uninstall.py --keep-all
        """.strip(),
    )
    parser.add_argument(
        "--keep-all",
        action="store_true",
        help="Only stop services and keep all files.",
    )
    parser.add_argument(
        "--remove-venv",
        action="store_true",
        help="Remove the venvs directory.",
    )
    parser.add_argument(
        "--remove-data",
        action="store_true",
        help="Remove the data directory.",
    )
    parser.add_argument(
        "--remove-model",
        action="store_true",
        help="Remove the model directory.",
    )
    parser.add_argument(
        "--log-file",
        help="Uninstall log file path. Defaults to <install_dir>\\log\\uninstall.log.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    uninstaller = WindowsUninstaller(
        keep_all=args.keep_all,
        remove_venv=args.remove_venv,
        remove_data=args.remove_data,
        remove_model=args.remove_model,
        log_file=args.log_file or "",
    )

    try:
        success = uninstaller.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nUninstall interrupted by user.")
        sys.exit(1)
    except Exception as exc:
        print(f"\nUnexpected uninstall failure: {exc}")
        raise


if __name__ == "__main__":
    main()
