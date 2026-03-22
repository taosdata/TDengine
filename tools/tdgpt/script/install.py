#!/usr/bin/env python3
"""TDGPT Windows installation script."""

from __future__ import annotations

import argparse
import ctypes
import os
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional

if platform.system().lower() != "windows":
    print("Error: this script is for Windows only.")
    sys.exit(1)

try:
    _kernel32 = ctypes.windll.kernel32
    _kernel32.SetConsoleMode(_kernel32.GetStdHandle(-11), 7)
    COLORS_ENABLED = True
except Exception:
    COLORS_ENABLED = False

try:
    sys.stdout.reconfigure(errors="replace")
    sys.stderr.reconfigure(errors="replace")
except Exception:
    pass


class Colors:
    RED = "\033[0;31m" if COLORS_ENABLED else ""
    GREEN = "\033[1;32m" if COLORS_ENABLED else ""
    YELLOW = "\033[1;33m" if COLORS_ENABLED else ""
    NC = "\033[0m" if COLORS_ENABLED else ""


INSTALL_DIR = Path(__file__).resolve().parent
LOG_DIR = INSTALL_DIR / "log"
REQUIREMENTS_DIR = INSTALL_DIR / "requirements"
VENVS_DIR = INSTALL_DIR / "venvs"
MODEL_DIR = INSTALL_DIR / "model"

DEFAULT_ONLINE_MODELS = ["moirai", "moment"]
ALL_MODELS = ["tdtsfm", "timemoe", "moirai", "chronos", "timesfm", "moment"]
ENABLED_MODELS_FILE = INSTALL_DIR / "cfg" / "enabled_models.txt"

MODEL_SPECS: Dict[str, Dict[str, object]] = {
    "tdtsfm": {
        "display": "TDTSFM",
        "default_model": None,
        "flag_files": ["taos.pth"],
        "venv": "venv",
        "online_supported": False,
        "archive_names": ["tdtsfm.zip", "tdtsfm.tar", "tdtsfm.tar.gz", "tdtsfm.tgz"],
        "download_size": "offline package only",
        "disk_size": "varies",
    },
    "timemoe": {
        "display": "TimeMoE 200M",
        "default_model": "Maple728/TimeMoE-200M",
        "flag_files": ["model.safetensors", "config.json"],
        "venv": "venv",
        "online_supported": True,
        "archive_names": ["timemoe.zip", "timemoe.tar", "timemoe.tar.gz", "timemoe.tgz"],
        "download_size": "~865 MB",
        "disk_size": "~1.4 GB",
    },
    "moirai": {
        "display": "Moirai Small",
        "default_model": "Salesforce/moirai-moe-1.0-R-small",
        "flag_files": ["model.safetensors", "config.json"],
        "venv": "moirai_venv",
        "online_supported": True,
        "archive_names": ["moirai.zip", "moirai.tar", "moirai.tar.gz", "moirai.tgz"],
        "download_size": "~447 MB",
        "disk_size": "~500 MB",
    },
    "chronos": {
        "display": "Chronos Bolt Base",
        "default_model": "amazon/chronos-bolt-base",
        "flag_files": ["model.safetensors", "config.json"],
        "venv": "chronos_venv",
        "online_supported": True,
        "archive_names": ["chronos.zip", "chronos.tar", "chronos.tar.gz", "chronos.tgz"],
        "download_size": "~783 MB",
        "disk_size": "~850 MB",
    },
    "timesfm": {
        "display": "TimesFM 2.0 500M",
        "default_model": "google/timesfm-2.0-500m-pytorch",
        "flag_files": ["torch_model.ckpt", "config.json"],
        "venv": "timesfm_venv",
        "online_supported": True,
        "archive_names": ["timesfm.zip", "timesfm.tar", "timesfm.tar.gz", "timesfm.tgz"],
        "download_size": "~1.90 GB",
        "disk_size": "~2.0 GB",
    },
    "moment": {
        "display": "MOMENT Base",
        "default_model": "AutonLab/MOMENT-1-base",
        "flag_files": ["model.safetensors", "config.json"],
        "venv": "momentfm_venv",
        "online_supported": True,
        "archive_names": [
            "moment.zip", "moment.tar", "moment.tar.gz", "moment.tgz",
            "moment-base.zip", "moment-base.tar", "moment-base.tar.gz", "moment-base.tgz",
            "moment-large.zip", "moment-large.tar", "moment-large.tar.gz", "moment-large.tgz",
        ],
        "download_size": "~433 MB",
        "disk_size": "~500 MB",
    },
}

VENV_CONFIGS: Dict[str, Dict[str, str]] = {
    "venv": {
        "requirements": "requirements_windows_core.txt",
        "description": "Main virtual environment",
        "validation_imports": "numpy,flask,waitress",
    },
    "moirai_venv": {"requirements": "requirements_moirai.txt", "description": "Moirai virtual environment"},
    "chronos_venv": {"requirements": "requirements_chronos.txt", "description": "Chronos virtual environment"},
    "timesfm_venv": {"requirements": "requirements_timesfm.txt", "description": "TimesFM virtual environment"},
    "momentfm_venv": {"requirements": "requirements_moment.txt", "description": "MOMENT virtual environment"},
}


def normalize_model_name(name: str) -> str:
    name = name.strip().lower()
    if name in {"moment-large", "momentfm"}:
        return "moment"
    return name


class ProgressReporter:
    def __init__(self, progress_file: Optional[Path]):
        self.progress_file = progress_file

    @staticmethod
    def _sanitize(value: str) -> str:
        return value.replace("\r", " ").replace("\n", " ").replace("|", "/").strip()

    def update(self, status: str, percent: int, title: str, detail: str) -> None:
        if not self.progress_file:
            return
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
        line = (
            f"{self._sanitize(status)}|"
            f"{max(0, min(100, percent))}|"
            f"{self._sanitize(title)}|"
            f"{self._sanitize(detail)}\n"
        )
        for attempt in range(10):
            try:
                with self.progress_file.open("a", encoding="utf-8") as fh:
                    fh.write(line)
                return
            except PermissionError:
                if attempt == 9:
                    return
                time.sleep(0.1)


class WindowsInstaller:
    def __init__(
        self,
        offline: bool = False,
        all_models: bool = False,
        selected_models: Optional[List[str]] = None,
        model_source: str = "none",
        model_archives: Optional[Dict[str, str]] = None,
        offline_model_package: str = "",
        model_endpoint: str = "",
        pip_index_url: str = "",
        pip_trusted_host: str = "",
        install_tensorflow: bool = True,
        skip_service_install: bool = False,
        log_file: str = "",
        progress_file: str = "",
    ):
        progress_path = progress_file or os.environ.get("TDGPT_PROGRESS_FILE", "")
        self.progress = ProgressReporter(Path(progress_path) if progress_path else None)
        self.offline = offline
        self.all_models = all_models
        self.model_source = model_source
        self.model_endpoint = model_endpoint.strip()
        self.pip_index_url = pip_index_url or os.environ.get("PIP_INDEX_URL", "")
        self.pip_trusted_host = pip_trusted_host or os.environ.get("PIP_TRUSTED_HOST", "")
        self.offline_model_package = offline_model_package.strip().strip('"')
        self.install_tensorflow = install_tensorflow
        self.skip_service_install = skip_service_install
        self.service_install_success = False
        self.install_dir = INSTALL_DIR
        self.log_dir = LOG_DIR
        self.requirements_dir = REQUIREMENTS_DIR
        self.venvs_dir = VENVS_DIR
        self.model_dir = MODEL_DIR
        self.reused_venvs: Dict[str, bool] = {}
        self.log_file = Path(log_file) if log_file else self.log_dir / "install.log"
        self.stdout_redirected_to_log = os.environ.get("TDGPT_LOG_REDIRECTED", "") == "1"
        self.python_cmd = ""
        self.python_version = ""

        raw_models = [normalize_model_name(item) for item in (selected_models or [])]
        if self.all_models:
            raw_models = list(ALL_MODELS)
        elif self.model_source == "online" and not raw_models:
            raw_models = list(DEFAULT_ONLINE_MODELS)
        self.selected_models = self._dedupe(raw_models)
        self.model_archives = {
            normalize_model_name(key): value
            for key, value in (model_archives or {}).items()
            if value
        }

    @staticmethod
    def _dedupe(items: Iterable[str]) -> List[str]:
        result: List[str] = []
        for item in items:
            if item in MODEL_SPECS and item not in result:
                result.append(item)
        return result

    def _emit(self, level: str, prefix: str, color: str, message: str) -> None:
        self.write_output(f"{color}{prefix}{Colors.NC} {message}")
        if not self.stdout_redirected_to_log:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            with self.log_file.open("a", encoding="utf-8") as fh:
                fh.write(f"{level}: {message}\n")

    def write_output(self, text: str) -> None:
        try:
            print(text)
        except UnicodeEncodeError:
            encoded = (text + "\n").encode(getattr(sys.stdout, "encoding", None) or "utf-8", errors="replace")
            buffer = getattr(sys.stdout, "buffer", None)
            if buffer is not None:
                buffer.write(encoded)
                buffer.flush()
            else:
                sys.stdout.write(encoded.decode("ascii", errors="replace"))

    def set_progress(self, percent: int, title: str, detail: str, status: str = "running") -> None:
        self.progress.update(status, percent, title, detail)
        self.write_output(f"[PROGRESS] {percent}% {title} | {detail}")

    def print_info(self, message: str) -> None:
        self._emit("INFO", "[INFO]", Colors.GREEN, message)

    def print_warning(self, message: str) -> None:
        self._emit("WARNING", "[WARNING]", Colors.YELLOW, message)

    def print_error(self, message: str) -> None:
        self._emit("ERROR", "[ERROR]", Colors.RED, message)

    def print_success(self, message: str) -> None:
        self._emit("SUCCESS", "[SUCCESS]", Colors.GREEN, message)

    def get_venv_path(self, name: str) -> Path:
        return self.venvs_dir / name

    def get_venv_python(self, name: str) -> Path:
        return self.get_venv_path(name) / "Scripts" / "python.exe"

    def add_pip_options(self, command: List[str]) -> List[str]:
        if self.pip_index_url:
            command.extend(["-i", self.pip_index_url])
        if self.pip_trusted_host:
            command.extend(["--trusted-host", self.pip_trusted_host])
        return command

    @staticmethod
    def _progress_percent_for_line(line: str, start_percent: int, end_percent: int) -> int:
        match = re.search(r"(?<!\d)(100|[1-9]?\d)%", line)
        if not match or end_percent <= start_percent:
            return start_percent
        line_percent = max(0, min(100, int(match.group(1))))
        return min(end_percent, start_percent + round((end_percent - start_percent) * line_percent / 100))

    def _stream_process_output(self, stream, start_percent: int, end_percent: int, title: str) -> None:
        buffer = ""
        while True:
            chunk = stream.read(256)
            if not chunk:
                break
            for char in chunk:
                if char in ("\r", "\n"):
                    line = buffer.strip()
                    buffer = ""
                    if not line:
                        continue
                    self.write_output(line)
                    self.progress.update(
                        "running",
                        self._progress_percent_for_line(line, start_percent, end_percent),
                        title,
                        line[:240],
                    )
                else:
                    buffer += char
        line = buffer.strip()
        if line:
            self.write_output(line)
            self.progress.update(
                "running",
                self._progress_percent_for_line(line, start_percent, end_percent),
                title,
                line[:240],
            )

    def run_stream(self, command: List[str], label: str, timeout: int, start_percent: int, end_percent: int, title: str,
                   env: Optional[Dict[str, str]] = None) -> bool:
        self.print_info(label)
        merged_env = os.environ.copy()
        merged_env["PYTHONUTF8"] = merged_env.get("PYTHONUTF8", "1")
        merged_env["PYTHONIOENCODING"] = merged_env.get("PYTHONIOENCODING", "utf-8")
        if env:
            merged_env.update(env)
        try:
            proc = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=merged_env,
                bufsize=1,
            )
        except Exception as exc:
            self.print_error(f"{label} failed to start: {exc}")
            return False
        try:
            assert proc.stdout is not None
            self._stream_process_output(proc.stdout, start_percent, end_percent, title)
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            self.print_error(f"Timed out while running: {label}")
            return False
        if proc.returncode != 0:
            self.print_error(f"{label} failed with exit code {proc.returncode}")
            return False
        return True

    def check_admin_privileges(self) -> None:
        try:
            if not ctypes.windll.shell32.IsUserAnAdmin():
                self.print_warning("Administrator privileges are recommended for service registration.")
        except Exception:
            self.print_warning("Unable to determine administrator privileges.")

    def check_disk_space(self) -> bool:
        try:
            stat = shutil.disk_usage(self.install_dir.drive)
            free_gb = stat.free / (1024 ** 3)
            if free_gb < 20:
                self.print_error(f"Insufficient disk space: {free_gb:.2f} GB free on {self.install_dir.drive}.")
                return False
            self.print_info(f"Disk space check passed: {free_gb:.2f} GB free on {self.install_dir.drive}.")
            return True
        except Exception as exc:
            self.print_warning(f"Unable to check disk space: {exc}")
            return True

    def check_python(self) -> bool:
        self.print_info("Checking Python environment...")
        for cmd in ["python", "python3", "python3.10", "python3.11", "python3.12"]:
            try:
                result = subprocess.run([cmd, "--version"], capture_output=True, text=True, timeout=5, check=False)
                output = (result.stdout or result.stderr).strip()
                if result.returncode == 0 and output.startswith("Python "):
                    major, minor = map(int, output.split()[1].split(".")[:2])
                    if major == 3 and minor in [10, 11, 12]:
                        self.python_cmd = cmd
                        self.python_version = output.split()[1]
                        self.print_success(f"Found Python {self.python_version} via `{cmd}`.")
                        return True
            except Exception:
                pass
        self.print_error("Python 3.10/3.11/3.12 was not found in PATH.")
        return False

    def check_pip(self) -> bool:
        try:
            result = subprocess.run([self.python_cmd, "-m", "pip", "--version"], capture_output=True, text=True,
                                    timeout=10, check=False)
            if result.returncode == 0:
                self.print_info(result.stdout.strip())
                return True
        except Exception as exc:
            self.print_error(f"Failed to check pip version: {exc}")
            return False
        self.print_error("pip is unavailable.")
        return False

    def create_directories(self) -> bool:
        try:
            for item in [self.log_dir, self.install_dir / "data" / "pids", self.model_dir, self.requirements_dir, self.venvs_dir]:
                item.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as exc:
            self.print_error(f"Failed to create directories: {exc}")
            return False

    def can_reuse_venv(self, name: str, description: str) -> bool:
        path = self.get_venv_path(name)
        python_exe = path / "Scripts" / "python.exe"
        if not path.exists():
            return False
        if not python_exe.exists():
            self.print_warning(f"{description} is incomplete because python.exe is missing. Recreating it.")
            return False
        try:
            result = subprocess.run(
                [str(python_exe), "-m", "pip", "--version"],
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
        except Exception as exc:
            self.print_warning(f"Unable to validate {description}: {exc}. Recreating it.")
            return False
        if result.returncode != 0:
            detail = (result.stdout or result.stderr or "").strip()
            if detail:
                self.print_warning(f"{description} pip check failed: {detail}")
            else:
                self.print_warning(f"{description} pip check failed. Recreating it.")
            return False
        self.print_info(f"Reusing existing {description}.")
        return True

    def create_venv(self, name: str, description: str) -> bool:
        path = self.get_venv_path(name)
        python_exe = path / "Scripts" / "python.exe"
        if self.can_reuse_venv(name, description):
            self.reused_venvs[name] = True
            return True
        if self.offline and path.exists() and not python_exe.exists():
            self.print_warning(f"Offline mode found an incomplete {description}. Recreating it.")
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            self.print_warning(f"Recreating {description}.")
            shutil.rmtree(path, ignore_errors=True)
        try:
            subprocess.run([self.python_cmd, "-m", "venv", str(path)], check=True, timeout=300)
            self.reused_venvs[name] = False
            return True
        except Exception as exc:
            self.print_error(f"Failed to create {description}: {exc}")
            return False

    def validate_venv_imports(self, venv_name: str, imports_csv: str) -> bool:
        imports = [item.strip() for item in imports_csv.split(",") if item.strip()]
        if not imports:
            return True
        python_exe = self.get_venv_python(venv_name)
        inline = "import " + ", ".join(imports)
        try:
            result = subprocess.run(
                [str(python_exe), "-c", inline],
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
        except Exception as exc:
            self.print_error(f"Failed to validate {venv_name}: {exc}")
            return False
        if result.returncode == 0:
            return True
        detail = (result.stderr or result.stdout or "").strip()
        self.print_warning(
            f"Validation failed for {venv_name}: {detail or 'unable to import required packages'}"
        )
        return False

    def install_req_file(self, venv_name: str, req_name: str, start_percent: int, end_percent: int, title: str) -> bool:
        req_file = self.requirements_dir / req_name
        if not req_file.exists():
            self.print_warning(f"Requirements file not found: {req_file}")
            return True
        python_exe = self.get_venv_python(venv_name)
        pip_exe = self.get_venv_path(venv_name) / "Scripts" / "pip.exe"
        upgrade_end = start_percent + max(1, (end_percent - start_percent) // 4)
        upgrade = [str(python_exe), "-m", "pip", "install", "--upgrade", "pip", "--progress-bar", "on"]
        self.add_pip_options(upgrade)
        if not self.run_stream(upgrade, f"Upgrading pip in {venv_name}", 1800, start_percent, upgrade_end, title):
            return False
        install = [str(pip_exe), "install", "-r", str(req_file), "--progress-bar", "on"]
        self.add_pip_options(install)
        return self.run_stream(install, f"Installing {req_name} in {venv_name}", 7200, upgrade_end, end_percent, title)

    def prepare_venv(self, venv_name: str, start_percent: int, end_percent: int) -> bool:
        config = VENV_CONFIGS[venv_name]
        desc = config["description"]
        validation_imports = str(config.get("validation_imports", "")).strip()

        for attempt in range(2):
            self.set_progress(start_percent, "Preparing Python environments", f"Creating {desc}")
            if not self.create_venv(venv_name, desc):
                return False
            if not self.install_req_file(
                venv_name,
                config["requirements"],
                max(start_percent + 2, start_percent),
                end_percent,
                f"Installing packages for {desc}",
            ):
                return False
            if not validation_imports or self.validate_venv_imports(venv_name, validation_imports):
                self.set_progress(end_percent, "Preparing Python environments", f"{desc} is ready")
                self.print_success(f"{desc} created successfully.")
                return True
            if attempt == 0 and self.reused_venvs.get(venv_name):
                self.print_warning(f"{desc} is incomplete after reuse. Rebuilding it once.")
                self.reused_venvs[venv_name] = False
                shutil.rmtree(self.get_venv_path(venv_name), ignore_errors=True)
                continue
            self.print_error(f"{desc} validation failed after package installation.")
            return False
        return False

    def install_venvs(self) -> bool:
        if not self.prepare_venv("venv", 20, 42):
            return False
        if self.offline:
            self.set_progress(58, "Installing optional TensorFlow support", "Offline package mode includes the packaged Python environment")
        elif self.install_tensorflow:
            self.set_progress(46, "Installing optional TensorFlow support", "Downloading TensorFlow CPU support")
            if not self.install_req_file("venv", "requirements_tensorflow.txt", 46, 58, "Installing TensorFlow CPU support"):
                return False
            self.set_progress(58, "Installing optional TensorFlow support", "TensorFlow CPU support is ready")
        else:
            self.set_progress(58, "Installing optional TensorFlow support", "TensorFlow CPU support was skipped")

        extra_venvs: List[str] = []
        for model_name in self.selected_models:
            venv_name = str(MODEL_SPECS[model_name]["venv"])
            if venv_name != "venv" and venv_name not in extra_venvs:
                extra_venvs.append(venv_name)
        if self.model_source == "offline":
            extra_venvs = []
        if not extra_venvs:
            self.set_progress(70, "Preparing Python environments", "No extra model virtual environments were requested")
            return True

        span = max(1, 22 // len(extra_venvs))
        current = 60
        for venv_name in extra_venvs:
            end_percent = min(82, current + span)
            if not self.prepare_venv(venv_name, current, end_percent):
                return False
            current = end_percent + 1
        return True

    def resolve_offline_archives(self) -> Dict[str, Path]:
        resolved: Dict[str, Path] = {}
        for model_name in ALL_MODELS:
            provided = self.model_archives.get(model_name, "")
            if provided:
                candidate = Path(provided)
                if not candidate.exists():
                    raise FileNotFoundError(f"Archive not found for {model_name}: {candidate}")
                resolved[model_name] = candidate
                continue
            for filename in MODEL_SPECS[model_name]["archive_names"]:
                candidate = self.model_dir / str(filename)
                if candidate.exists():
                    resolved[model_name] = candidate
                    break
        return resolved

    def extract_archive_to_dir(self, archive_path: Path, destination_dir: Path) -> bool:
        try:
            suffixes = "".join(archive_path.suffixes).lower()
            if suffixes.endswith(".zip"):
                with zipfile.ZipFile(archive_path, "r") as zip_obj:
                    zip_obj.extractall(destination_dir)
            elif suffixes.endswith(".tar.gz") or suffixes.endswith(".tgz") or suffixes.endswith(".tar"):
                with tarfile.open(archive_path, "r:*") as tar_obj:
                    tar_obj.extractall(destination_dir)
            else:
                self.print_error(f"Unsupported archive format: {archive_path.name}")
                return False
        except Exception as exc:
            self.print_error(f"Failed to extract {archive_path.name}: {exc}")
            return False
        return True

    def find_payload_root(self, root_dir: Path, model_name: str) -> Optional[Path]:
        for flag_name in MODEL_SPECS[model_name]["flag_files"]:
            if (root_dir / str(flag_name)).exists():
                return root_dir
        for flag_name in MODEL_SPECS[model_name]["flag_files"]:
            for candidate in root_dir.rglob(str(flag_name)):
                if candidate.is_file():
                    return candidate.parent
        return None

    def candidate_model_dir_names(self, model_name: str) -> List[str]:
        names = {model_name}
        for archive_name in MODEL_SPECS[model_name]["archive_names"]:
            archive_label = str(archive_name).lower()
            for suffix in (".tar.gz", ".tgz", ".tar", ".zip"):
                if archive_label.endswith(suffix):
                    archive_label = archive_label[:-len(suffix)]
                    break
            if archive_label:
                names.add(archive_label)
        return sorted(names)

    def find_offline_package_payload(self, root_dir: Path, model_name: str) -> Optional[Path]:
        candidate_names = set(self.candidate_model_dir_names(model_name))
        for candidate_name in candidate_names:
            candidate_dir = root_dir / candidate_name
            if candidate_dir.is_dir():
                payload_root = self.find_payload_root(candidate_dir, model_name)
                if payload_root:
                    return payload_root
        for candidate in root_dir.rglob("*"):
            if candidate.is_dir() and candidate.name.lower() in candidate_names:
                payload_root = self.find_payload_root(candidate, model_name)
                if payload_root:
                    return payload_root
        return None

    def find_offline_package_archive(self, root_dir: Path, model_name: str) -> Optional[Path]:
        archive_names = {str(name).lower() for name in MODEL_SPECS[model_name]["archive_names"]}
        for candidate in root_dir.rglob("*"):
            if candidate.is_file() and candidate.name.lower() in archive_names:
                return candidate
        return None

    def deploy_model_payload(self, payload_root: Path, model_name: str) -> bool:
        target_dir = self.model_dir / model_name
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        target_dir.mkdir(parents=True, exist_ok=True)
        for item in payload_root.iterdir():
            dst = target_dir / item.name
            if dst.exists():
                if dst.is_dir():
                    shutil.rmtree(dst, ignore_errors=True)
                else:
                    dst.unlink()
            shutil.move(str(item), str(dst))
        if not self.find_payload_root(target_dir, model_name):
            self.print_error(f"Imported payload for {model_name} does not contain expected model files.")
            return False
        return True

    def extract_archive(self, archive_path: Path, model_name: str) -> bool:
        with tempfile.TemporaryDirectory(prefix=f"tdgpt-{model_name}-", dir=str(self.model_dir)) as temp_name:
            temp_dir = Path(temp_name)
            if not self.extract_archive_to_dir(archive_path, temp_dir):
                return False
            payload_root = self.find_offline_package_payload(temp_dir, model_name)
            if not payload_root:
                self.print_error(f"Imported archive for {model_name} does not contain expected model files.")
                return False
            if not self.deploy_model_payload(payload_root, model_name):
                return False
        self.print_success(f"Offline model imported: {MODEL_SPECS[model_name]['display']}")
        return True

    def import_offline_model_package(self, package_path: Path) -> bool:
        with tempfile.TemporaryDirectory(prefix="tdgpt-offline-package-", dir=str(self.model_dir)) as temp_name:
            temp_dir = Path(temp_name)
            if not self.extract_archive_to_dir(package_path, temp_dir):
                return False

            imported_models: List[str] = []
            for model_name in ALL_MODELS:
                payload_root = self.find_offline_package_payload(temp_dir, model_name)
                if payload_root:
                    if not self.deploy_model_payload(payload_root, model_name):
                        return False
                    imported_models.append(model_name)
                    self.print_success(
                        f"Offline model imported from package: {MODEL_SPECS[model_name]['display']}"
                    )
                    continue

                nested_archive = self.find_offline_package_archive(temp_dir, model_name)
                if nested_archive:
                    if not self.extract_archive(nested_archive, model_name):
                        return False
                    imported_models.append(model_name)

        if not imported_models:
            self.print_error(
                f"Offline model package {package_path.name} does not contain any recognized model payloads."
            )
            return False
        return True

    def import_offline_models(self) -> bool:
        archives = self.resolve_offline_archives()
        offline_package = Path(self.offline_model_package) if self.offline_model_package else None
        if offline_package and not offline_package.exists():
            raise FileNotFoundError(f"Offline model package not found: {offline_package}")

        if not archives and not offline_package:
            self.set_progress(84, "Importing offline model archives", "No packaged offline model archives were found")
            return True

        tasks: List[tuple[str, str]] = [("archive", name) for name in ALL_MODELS if name in archives]
        if offline_package:
            tasks.append(("package", str(offline_package)))

        total = len(tasks)
        for index, task in enumerate(tasks, start=1):
            percent = 84 + int((index - 1) * 10 / max(1, total))
            task_type, task_value = task
            if task_type == "archive":
                model_name = task_value
                spec = MODEL_SPECS[model_name]
                self.set_progress(percent, "Importing offline model archives",
                                  f"{spec['display']} from {archives[model_name].name}")
                if not self.extract_archive(archives[model_name], model_name):
                    return False
            else:
                package_path = Path(task_value)
                self.set_progress(
                    percent,
                    "Importing offline model package",
                    f"{package_path.name} (imports every recognized model it contains)",
                )
                if not self.import_offline_model_package(package_path):
                    return False
        self.set_progress(94, "Importing offline model archives", "Packaged offline models are ready")
        return True

    def download_model_online(self, model_name: str, start_percent: int, end_percent: int) -> bool:
        spec = MODEL_SPECS[model_name]
        repo_id = str(spec["default_model"] or "")
        if not spec["online_supported"] or not repo_id:
            self.print_error(f"Online download is not available for {model_name}.")
            return False
        target_dir = self.model_dir / model_name
        target_dir.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        if self.model_endpoint:
            env["HF_ENDPOINT"] = self.model_endpoint
        env["HF_HUB_DISABLE_PROGRESS_BARS"] = "0"
        python_exe = self.get_venv_python(str(spec["venv"]))
        inline = (
            "import sys; from huggingface_hub import snapshot_download; "
            "repo_id, local_dir, endpoint = sys.argv[1:4]; "
            "snapshot_download(repo_id=repo_id, local_dir=local_dir, endpoint=(endpoint or None))"
        )
        return self.run_stream(
            [str(python_exe), "-c", inline, repo_id, str(target_dir), self.model_endpoint],
            f"Downloading {spec['display']} ({spec['download_size']})",
            43200,
            start_percent,
            end_percent,
            f"Downloading {spec['display']}",
            env=env,
        )

    def download_online_models(self) -> bool:
        if not self.selected_models:
            self.set_progress(84, "Downloading selected models", "No models were selected")
            return True
        for model_name in self.selected_models:
            if not MODEL_SPECS[model_name]["online_supported"]:
                self.print_error(
                    f"{MODEL_SPECS[model_name]['display']} does not support online download yet. "
                    "Use an offline archive instead."
                )
                return False
        total = len(self.selected_models)
        for index, model_name in enumerate(self.selected_models, start=1):
            spec = MODEL_SPECS[model_name]
            start_percent = 84 + int((index - 1) * 10 / max(1, total))
            end_percent = 84 + int(index * 10 / max(1, total))
            detail = (
                f"{spec['display']} ({spec['download_size']} download, {spec['disk_size']} disk). "
                "Please keep the installer open while files are downloading."
            )
            self.set_progress(start_percent, "Downloading selected models", detail)
            if not self.download_model_online(model_name, start_percent, max(start_percent + 1, end_percent)):
                return False
        self.set_progress(94, "Downloading selected models", "Selected model downloads are complete")
        return True

    def process_models(self) -> bool:
        if self.model_source == "none":
            self.set_progress(84, "Model installation", "Model download and import were skipped")
            return True
        if self.model_source == "offline":
            return self.import_offline_models()
        if self.model_source == "online":
            return self.download_online_models()
        self.print_error(f"Unsupported model source: {self.model_source}")
        return False

    def write_enabled_models(self) -> None:
        self.install_dir.joinpath("cfg").mkdir(parents=True, exist_ok=True)
        if ENABLED_MODELS_FILE.exists():
            ENABLED_MODELS_FILE.unlink()

    def install_service(self) -> bool:
        if self.skip_service_install:
            self.print_info("Service installation skipped by installer option.")
            return True
        service_script = self.install_dir / "bin" / "taosanode_service.py"
        python_exe = self.get_venv_python("venv")
        if not service_script.exists() or not python_exe.exists():
            self.print_warning("Service installer prerequisites are missing, skip service registration.")
            return True
        try:
            self.set_progress(96, "Registering the Windows service", "Installing Taosanode service")
            subprocess.run([str(python_exe), str(service_script), "install-service"], check=True, timeout=180)
            self.service_install_success = True
            self.print_success("Windows service installed successfully.")
            return True
        except Exception as exc:
            self.service_install_success = False
            self.print_warning(f"Service installation failed: {exc}")
            return True

    def append_install_summary(self) -> None:
        lines = [
            "",
            "=" * 72,
            "TDGPT Windows Installation Summary",
            "=" * 72,
            f"Install directory: {self.install_dir}",
            f"Python version: {self.python_version or 'Unknown'}",
            f"Python package mode: {'Offline' if self.offline else 'Online'}",
            f"TensorFlow support: {'Installed' if self.install_tensorflow else 'Skipped'}",
            f"Model source: {self.model_source}",
            "Selected models: " + (", ".join(self.selected_models) if self.selected_models else "None"),
            f"Offline model package: {self.offline_model_package or 'None'}",
            f"Pip index: {self.pip_index_url or 'Default'}",
            f"Pip trusted host: {self.pip_trusted_host or 'Default'}",
            f"Model endpoint: {self.model_endpoint or 'Default Hugging Face'}",
            f"Service install requested: {'No' if self.skip_service_install else 'Yes'}",
            f"Service install result: {'Installed' if self.service_install_success else 'Skipped/Not installed'}",
            "Virtual environments:",
        ]
        if self.model_source == "offline":
            lines.append("Model startup behavior: start-model all checks model directories automatically.")
        elif self.model_source == "online":
            lines.append("Model startup behavior: start-model all starts downloaded model directories automatically.")
        for venv_name in VENV_CONFIGS:
            python_exe = self.get_venv_python(venv_name)
            lines.append(f"  [{'OK' if python_exe.exists() else 'NO'}] {venv_name}")
        lines.append("=" * 72)
        if self.stdout_redirected_to_log:
            for line in lines:
                print(line)
        else:
            with self.log_file.open("a", encoding="utf-8") as fh:
                fh.write("\n".join(lines) + "\n")

    def run(self) -> bool:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.set_progress(1, "Initializing TDGPT setup", "Preparing installation environment")
        self.print_info("=" * 60)
        self.print_info("TDGPT Windows Installer")
        self.print_info("=" * 60)
        self.print_info(f"Install directory: {self.install_dir}")
        self.print_info(f"Requirements directory: {self.requirements_dir}")
        self.print_info(f"Virtual environments directory: {self.venvs_dir}")
        self.print_info(f"Log file: {self.log_file}")
        self.print_info("=" * 60)
        self.check_admin_privileges()
        self.set_progress(5, "Checking system requirements", "Checking disk space")
        if not self.check_disk_space():
            return False
        self.set_progress(8, "Checking system requirements", "Checking Python")
        if not self.check_python():
            return False
        self.set_progress(10, "Checking system requirements", "Checking pip")
        if not self.check_pip():
            return False
        self.set_progress(12, "Preparing directories", "Creating required folders")
        if not self.create_directories():
            return False
        if not self.install_venvs():
            return False
        if not self.process_models():
            return False
        self.write_enabled_models()
        if not self.install_service():
            return False
        self.append_install_summary()
        self.set_progress(100, "Installation complete", "TDGPT is ready", status="success")
        self.print_success("TDGPT installation completed.")
        self.print_info("Start service: net start Taosanode")
        self.print_info("Stop service: net stop Taosanode")
        self.print_info(f"Script start: {self.install_dir / 'bin' / 'start-taosanode.bat'}")
        self.print_info(f"Script stop: {self.install_dir / 'bin' / 'stop-taosanode.bat'}")
        return True


def parse_model_archive_arg(values: Optional[List[str]]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for item in values or []:
        if "=" not in item:
            raise argparse.ArgumentTypeError(f"Invalid --model-archive value: {item}. Expected <model>=<path>.")
        model_name, archive_path = item.split("=", 1)
        model_name = normalize_model_name(model_name)
        if model_name not in MODEL_SPECS:
            raise argparse.ArgumentTypeError(f"Unknown model in --model-archive: {model_name}")
        result[model_name] = archive_path.strip().strip('"')
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="TDGPT Windows installation script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python install.py
  python install.py --skip-tensorflow
  python install.py --model-source online --model moirai --model moment --model-endpoint https://hf-mirror.com
  python install.py -o --model-source offline --offline-model-package D:\offline-models.zip
        """.strip(),
    )
    parser.add_argument("-o", "--offline", action="store_true", help="Reuse existing virtual environments when possible.")
    parser.add_argument("-a", "--all-models", action="store_true", help="Select all supported models for the chosen model source.")
    parser.add_argument("--model-source", choices=["none", "online", "offline"], default="none",
                        help="Choose how selected models should be prepared.")
    parser.add_argument("--model", action="append", dest="models",
                        help="Supported values: tdtsfm, timemoe, moirai, chronos, timesfm, moment.")
    parser.add_argument("--model-archive", action="append", dest="model_archives",
                        help="Offline archive mapping in the form <model>=<path>.")
    parser.add_argument("--offline-model-package",
                        help="Optional offline package archive that can contain multiple model payloads.")
    parser.add_argument("--model-endpoint", help="Optional Hugging Face mirror endpoint used for online model downloads.")
    parser.add_argument("--pip-index-url", help="Custom pip index URL used for dependency installation.")
    parser.add_argument("--pip-trusted-host", help="Optional trusted host used together with the pip index URL.")
    parser.add_argument("--skip-tensorflow", action="store_true",
                        help="Do not install TensorFlow CPU support into the main virtual environment.")
    parser.add_argument("--skip-service-install", action="store_true",
                        help="Do not register the Windows service during installation.")
    parser.add_argument("--log-file", help="Installation log file path. Defaults to <install_dir>\\log\\install.log.")
    parser.add_argument("--progress-file", help="Installer progress state file used by the Windows wizard.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    selected_models = [normalize_model_name(item) for item in (args.models or [])]
    invalid_models = [item for item in selected_models if item not in MODEL_SPECS]
    if invalid_models:
        parser.error(f"Unsupported model(s): {', '.join(invalid_models)}")
    try:
        model_archives = parse_model_archive_arg(args.model_archives)
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))

    installer = WindowsInstaller(
        offline=args.offline,
        all_models=args.all_models,
        selected_models=selected_models,
        model_source=args.model_source,
        model_archives=model_archives,
        offline_model_package=args.offline_model_package or "",
        model_endpoint=args.model_endpoint or "",
        pip_index_url=args.pip_index_url or "",
        pip_trusted_host=args.pip_trusted_host or "",
        install_tensorflow=not args.skip_tensorflow,
        skip_service_install=args.skip_service_install,
        log_file=args.log_file or "",
        progress_file=args.progress_file or "",
    )

    try:
        success = installer.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        installer.set_progress(100, "Installation cancelled", "Installation was interrupted by the user", status="error")
        print("\nInstallation interrupted by user.")
        sys.exit(1)
    except Exception as exc:
        installer.set_progress(100, "Installation failed", str(exc), status="error")
        print(f"\nUnexpected installation failure: {exc}")
        raise


if __name__ == "__main__":
    main()
