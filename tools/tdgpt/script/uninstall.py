#!/usr/bin/env python3
"""
TDGPT Windows Uninstallation Script

Usage:
    python uninstall.py [options]

Options:
    --keep-all          Keep all data (cfg, model, data, venv)
    --remove-venv       Remove virtual environment (kept by default)
    --remove-data       Remove data directory (kept by default)
    --remove-model      Remove model directory (kept by default)
    -h, --help          Show help information

Examples:
    python uninstall.py                 # Standard uninstall (keep cfg, model, data, venv)
    python uninstall.py --remove-venv   # Uninstall and remove virtual environment
    python uninstall.py --keep-all      # Only stop services, keep all files
"""

import os
import sys
import platform
import subprocess
import argparse
import logging
import shutil
import time
from pathlib import Path
from typing import List

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

# Color output
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
    NC = '\033[0m' if COLORS_ENABLED else ''

# Default installation path
INSTALL_DIR = Path(r"C:\TDengine\taosanode")


class WindowsUninstaller:
    """Windows uninstaller"""

    def __init__(self, keep_all: bool = False, remove_venv: bool = False,
                 remove_data: bool = False, remove_model: bool = False):
        self.keep_all = keep_all
        self.remove_venv = remove_venv
        self.remove_data = remove_data
        self.remove_model = remove_model
        self.install_dir = INSTALL_DIR

    def print_info(self, message: str):
        """Print info message"""
        print(f"{Colors.GREEN}[INFO]{Colors.NC} {message}")
        logger.info(message)

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

    def stop_services(self) -> bool:
        """Stop all services"""
        self.print_info("Stopping TDGPT services...")

        # Stop main service
        try:
            service_script = self.install_dir / "bin" / "taosanode_service.py"
            if service_script.exists():
                venv_python = self.install_dir / "venv" / "Scripts" / "python.exe"
                if venv_python.exists():
                    subprocess.run(
                        [str(venv_python), str(service_script), "stop"],
                        timeout=30,
                        capture_output=True
                    )
                    self.print_info("Main service stopped")
        except Exception as e:
            self.print_warning(f"Failed to stop main service: {e}")

        # Stop model services
        try:
            service_script = self.install_dir / "bin" / "taosanode_service.py"
            if service_script.exists():
                venv_python = self.install_dir / "venv" / "Scripts" / "python.exe"
                if venv_python.exists():
                    subprocess.run(
                        [str(venv_python), str(service_script), "model-stop", "all"],
                        timeout=30,
                        capture_output=True
                    )
                    self.print_info("Model services stopped")
        except Exception as e:
            self.print_warning(f"Failed to stop model services: {e}")

        # Force kill remaining processes
        time.sleep(2)
        try:
            subprocess.run(
                ["taskkill", "/F", "/IM", "python.exe", "/T"],
                capture_output=True,
                timeout=10
            )
        except:
            pass

        return True

    def uninstall_service(self) -> bool:
        """Uninstall Windows service"""
        self.print_info("Uninstalling Windows service...")

        try:
            service_script = self.install_dir / "bin" / "taosanode_service.py"
            if service_script.exists():
                venv_python = self.install_dir / "venv" / "Scripts" / "python.exe"
                if venv_python.exists():
                    subprocess.run(
                        [str(venv_python), str(service_script), "uninstall"],
                        timeout=30,
                        capture_output=True
                    )
                    self.print_success("Windows service uninstalled")
        except Exception as e:
            self.print_warning(f"Failed to uninstall service: {e}")

        return True

    def remove_from_path(self) -> bool:
        """Remove from PATH environment variable"""
        self.print_info("Removing install directory from PATH...")

        try:
            bin_path = str(self.install_dir / "bin")

            # Read current PATH
            result = subprocess.run(
                ["reg", "query", "HKLM\\SYSTEM\\CurrentControlSet\\Control\\Session Manager\\Environment", "/v", "Path"],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode == 0:
                # Parse PATH
                for line in result.stdout.split('\n'):
                    if 'Path' in line and 'REG_' in line:
                        parts = line.split('REG_EXPAND_SZ', 1)
                        if len(parts) == 2:
                            current_path = parts[1].strip()

                            # Remove our path
                            path_list = [p.strip() for p in current_path.split(';') if p.strip()]
                            new_path_list = [p for p in path_list if bin_path.lower() not in p.lower()]

                            if len(new_path_list) < len(path_list):
                                new_path = ';'.join(new_path_list)

                                # Update PATH
                                subprocess.run(
                                    ["setx", "/M", "Path", new_path],
                                    capture_output=True,
                                    timeout=10
                                )
                                self.print_success("Removed from PATH")
                            else:
                                self.print_info("Install directory not found in PATH")

        except Exception as e:
            self.print_warning(f"Failed to remove from PATH: {e}")

        return True

    def remove_files(self) -> bool:
        """Remove files"""
        self.print_info("Removing files...")

        if self.keep_all:
            self.print_info("Keeping all files (--keep-all mode)")
            return True

        # Directories to remove
        dirs_to_remove = ["bin", "lib", "resource", "log"]

        # Decide whether to remove based on options
        if self.remove_venv:
            dirs_to_remove.extend(["venv", "timesfm_venv", "moirai_venv", "chronos_venv", "momentfm_venv"])
        if self.remove_data:
            dirs_to_remove.append("data")
        if self.remove_model:
            dirs_to_remove.append("model")

        # Remove directories
        for dir_name in dirs_to_remove:
            dir_path = self.install_dir / dir_name
            if dir_path.exists():
                try:
                    shutil.rmtree(dir_path)
                    self.print_info(f"Removed directory: {dir_name}")
                except Exception as e:
                    self.print_warning(f"Failed to remove directory {dir_name}: {e}")

        # Remove files in root directory
        files_to_remove = ["install.log", "taosanode.pid", "*.txt"]
        for pattern in files_to_remove:
            if '*' in pattern:
                for file_path in self.install_dir.glob(pattern):
                    if file_path.is_file():
                        try:
                            file_path.unlink()
                            self.print_info(f"Removed file: {file_path.name}")
                        except:
                            pass
            else:
                file_path = self.install_dir / pattern
                if file_path.exists():
                    try:
                        file_path.unlink()
                        self.print_info(f"Removed file: {pattern}")
                    except:
                        pass

        return True

    def show_preserved_files(self):
        """Show preserved files"""
        self.print_info("=" * 60)
        self.print_info("The following directories have been preserved:")

        preserved = []
        if not self.remove_venv:
            if (self.install_dir / "venv").exists():
                preserved.append("  - venv (virtual environment)")
        if not self.remove_data:
            if (self.install_dir / "data").exists():
                preserved.append("  - data (data directory)")
        if not self.remove_model:
            if (self.install_dir / "model").exists():
                preserved.append("  - model (model directory)")
        if (self.install_dir / "cfg").exists():
            preserved.append("  - cfg (configuration files)")

        if preserved:
            for item in preserved:
                self.print_info(item)
            self.print_info("")
            self.print_info("To completely remove, manually delete the following directory:")
            self.print_info(f"  {self.install_dir}")
        else:
            self.print_info("  No preserved files")

        self.print_info("=" * 60)

    def generate_uninstall_report(self) -> bool:
        """Generate uninstall report"""
        report_file = self.install_dir / "uninstall.log"

        try:
            with open(report_file, "w", encoding="utf-8") as f:
                f.write("=" * 60 + "\n")
                f.write("TDGPT Windows Uninstall Report\n")
                f.write("=" * 60 + "\n\n")
                f.write(f"Uninstall directory: {self.install_dir}\n")
                f.write(f"Keep-all mode: {'Yes' if self.keep_all else 'No'}\n")
                f.write(f"Remove venv: {'Yes' if self.remove_venv else 'No'}\n")
                f.write(f"Remove data: {'Yes' if self.remove_data else 'No'}\n")
                f.write(f"Remove model: {'Yes' if self.remove_model else 'No'}\n")
                f.write("\n")
                f.write("Preserved directories:\n")
                if (self.install_dir / "cfg").exists():
                    f.write("  ✓ cfg\n")
                if not self.remove_venv and (self.install_dir / "venv").exists():
                    f.write("  ✓ venv\n")
                if not self.remove_data and (self.install_dir / "data").exists():
                    f.write("  ✓ data\n")
                if not self.remove_model and (self.install_dir / "model").exists():
                    f.write("  ✓ model\n")
                f.write("\n")
                f.write("=" * 60 + "\n")

            self.print_success(f"Uninstall report saved: {report_file}")
            return True
        except Exception as e:
            self.print_warning(f"Failed to generate uninstall report: {e}")
            return True

    def run(self) -> bool:
        """Run uninstall"""
        self.print_info("=" * 60)
        self.print_info("TDGPT Windows Uninstaller")
        self.print_info("=" * 60)
        self.print_info(f"Install directory: {self.install_dir}")
        self.print_info(f"Keep-all mode: {'Yes' if self.keep_all else 'No'}")
        self.print_info(f"Remove venv: {'Yes' if self.remove_venv else 'No'}")
        self.print_info(f"Remove data: {'Yes' if self.remove_data else 'No'}")
        self.print_info(f"Remove model: {'Yes' if self.remove_model else 'No'}")
        self.print_info("=" * 60)

        # 1. Stop services
        self.stop_services()

        # 2. Uninstall service
        self.uninstall_service()

        # 3. Remove from PATH
        self.remove_from_path()

        # 4. Remove files
        self.remove_files()

        # 6. Generate uninstall report
        self.generate_uninstall_report()

        # 7. Show preserved files
        self.show_preserved_files()

        # Done
        self.print_info("=" * 60)
        self.print_success("TDGPT uninstall complete!")
        self.print_info("=" * 60)

        return True


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="TDGPT Windows Uninstall Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python uninstall.py                 # Standard uninstall (keep cfg, model, data, venv)
  python uninstall.py --remove-venv   # Uninstall and remove virtual environment
  python uninstall.py --keep-all      # Only stop services, keep all files

Note:
  By default, uninstall preserves the following directories:
    - cfg (configuration files)
    - model (model files)
    - data (data directory)
    - venv (virtual environment)

  This is consistent with the Linux uninstall behavior, ensuring user data is not lost.
        """
    )

    parser.add_argument(
        "--keep-all",
        action="store_true",
        help="Keep all data (cfg, model, data, venv)"
    )

    parser.add_argument(
        "--remove-venv",
        action="store_true",
        help="Remove virtual environment (kept by default)"
    )

    parser.add_argument(
        "--remove-data",
        action="store_true",
        help="Remove data directory (kept by default)"
    )

    parser.add_argument(
        "--remove-model",
        action="store_true",
        help="Remove model directory (kept by default)"
    )

    args = parser.parse_args()

    # Create uninstaller and run
    uninstaller = WindowsUninstaller(
        keep_all=args.keep_all,
        remove_venv=args.remove_venv,
        remove_data=args.remove_data,
        remove_model=args.remove_model
    )

    try:
        success = uninstaller.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nUninstall interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nError occurred during uninstall: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
