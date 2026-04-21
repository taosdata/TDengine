#!/usr/bin/env python3
"""TDgpt Windows installation script."""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.parse
import urllib.request
import winreg
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

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
PACKAGED_PYTHON_DIR = INSTALL_DIR / "python" / "runtime"
PACKAGE_METADATA_FILE = INSTALL_DIR / "cfg" / "package-metadata.json"
INSTALL_STATE_FILE = INSTALL_DIR / "cfg" / "install-state.json"
MIN_VC_RUNTIME_VERSION = "14.44.0.0"
MIN_VC_RUNTIME_FAMILY = "Visual C++ 2015-2022 Redistributable x64"
VC_RUNTIME_DOWNLOAD_URL = "https://aka.ms/vc14/vc_redist.x64.exe"

DEFAULT_ONLINE_MODELS = ["moirai", "moment"]
ALL_MODELS = ["tdtsfm", "timemoe", "moirai", "chronos", "timesfm", "moment"]
ENABLED_MODELS_FILE = INSTALL_DIR / "cfg" / "enabled_models.txt"


def build_package_metadata_defaults(edition: str = "community") -> Dict[str, str]:
    normalized_edition = str(edition or "community").strip().lower()
    if normalized_edition == "enterprise":
        app_name = "TDengine TDgpt-Enterprise"
    else:
        normalized_edition = "community"
        app_name = "TDengine TDgpt-OSS"
    return {
        "edition": normalized_edition,
        "app_name": app_name,
        "product_full_name": f"{app_name} - TDengine Analytics Node",
    }


def load_package_metadata() -> Dict[str, str]:
    defaults = build_package_metadata_defaults()
    if not PACKAGE_METADATA_FILE.exists():
        return defaults
    try:
        with PACKAGE_METADATA_FILE.open("r", encoding="utf-8") as file_obj:
            payload = json.load(file_obj)
    except Exception:
        return defaults
    if not isinstance(payload, dict):
        return defaults
    edition_defaults = build_package_metadata_defaults(str(payload.get("edition", defaults["edition"])))
    defaults.update(edition_defaults)
    defaults.update({str(key): str(value) for key, value in payload.items() if value is not None})
    return defaults


PACKAGE_METADATA = load_package_metadata()
APP_DISPLAY_NAME = PACKAGE_METADATA.get("app_name", "TDengine TDgpt-OSS")
PRODUCT_FULL_NAME = PACKAGE_METADATA.get("product_full_name", f"{APP_DISPLAY_NAME} - TDengine Analytics Node")
DEFAULT_MODEL_RESOURCE_PACKAGE_URL = PACKAGE_METADATA.get("resource_package_url", "").strip()

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
        "flag_files": ["model.safetensors", "config.json", "torch_model.ckpt"],
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

OFFLINE_VENV_ARCHIVE_NAMES: Dict[str, List[str]] = {
    "venv": ["venv-main.tar.gz", "venv-main.tgz", "venv-main.tar", "venv.tar.gz", "venv.tgz", "venv.tar"],
    "moirai_venv": ["venv-moirai.tar.gz", "venv-moirai.tgz", "venv-moirai.tar", "moirai_venv.tar.gz", "moirai_venv.tgz", "moirai_venv.tar"],
    "chronos_venv": ["venv-chronos.tar.gz", "venv-chronos.tgz", "venv-chronos.tar", "chronos_venv.tar.gz", "chronos_venv.tgz", "chronos_venv.tar"],
    "timesfm_venv": ["venv-timesfm.tar.gz", "venv-timesfm.tgz", "venv-timesfm.tar", "timesfm_venv.tar.gz", "timesfm_venv.tgz", "timesfm_venv.tar"],
    "momentfm_venv": ["venv-moment.tar.gz", "venv-moment.tgz", "venv-moment.tar", "venv-momentfm.tar.gz", "venv-momentfm.tgz", "venv-momentfm.tar", "momentfm_venv.tar.gz", "momentfm_venv.tgz", "momentfm_venv.tar"],
}

OFFLINE_VENV_DIR_CANDIDATES: Dict[str, List[str]] = {
    "venv": ["venvs/venv", "venv", "venv-main"],
    "moirai_venv": ["venvs/moirai_venv", "moirai_venv", "venv-moirai"],
    "chronos_venv": ["venvs/chronos_venv", "chronos_venv", "venv-chronos"],
    "timesfm_venv": ["venvs/timesfm_venv", "timesfm_venv", "venv-timesfm"],
    "momentfm_venv": ["venvs/momentfm_venv", "momentfm_venv", "venv-moment", "venv-momentfm"],
}


def normalize_model_name(name: str) -> str:
    name = name.strip().lower()
    if name in {"moment-large", "momentfm"}:
        return "moment"
    return name


def find_reexec_python(current_python: Path) -> Optional[List[str]]:
    candidates: List[List[str]] = [
        ["python"],
        ["python3"],
        ["python3.11"],
        ["py", "-3.11"],
        ["py", "-3"],
    ]
    for command in candidates:
        try:
            result = subprocess.run(
                command + ["-c", "import sys; print(sys.executable)"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
        except Exception:
            continue
        if result.returncode != 0:
            continue
        resolved = (result.stdout or "").strip()
        if not resolved:
            continue
        resolved_path = Path(resolved).resolve()
        if resolved_path == current_python:
            continue
        try:
            resolved_path.relative_to(VENVS_DIR)
            continue
        except ValueError:
            return command
    return None


def is_unsafe_offline_import_python(current_python: Path) -> bool:
    try:
        current_python.relative_to(VENVS_DIR.resolve())
        return True
    except ValueError:
        return False


def maybe_reexec_for_offline_import(args: argparse.Namespace) -> None:
    if not args.offline:
        return
    if not (args.offline_package or args.offline_model_package or args.resource_package_url):
        return
    if os.environ.get("TDGPT_REEXEC_OFFLINE_IMPORT", "") == "1":
        return

    current_python = Path(sys.executable).resolve()
    if not is_unsafe_offline_import_python(current_python):
        return

    packaged_python = (PACKAGED_PYTHON_DIR / "python.exe").resolve()
    if packaged_python.exists() and packaged_python != current_python:
        print(f"INFO: Re-launching offline import with bundled Python: {packaged_python}")
        env = os.environ.copy()
        env["TDGPT_REEXEC_OFFLINE_IMPORT"] = "1"
        result = subprocess.run(
            [str(packaged_python), str(Path(__file__).resolve()), *sys.argv[1:]],
            env=env,
            check=False,
        )
        sys.exit(result.returncode)

    reexec_command = find_reexec_python(current_python)
    if not reexec_command:
        print(
            "ERROR: Offline import needs a Python interpreter outside the current venv tree "
            "because the installer must replace venvs during import."
        )
        print("ERROR: Re-run with the bundled runtime or install Python 3.10/3.11/3.12 in PATH.")
        sys.exit(1)

    print(f"INFO: Re-launching offline import with external Python: {' '.join(reexec_command)}")
    env = os.environ.copy()
    env["TDGPT_REEXEC_OFFLINE_IMPORT"] = "1"
    result = subprocess.run(
        reexec_command + [str(Path(__file__).resolve()), *sys.argv[1:]],
        env=env,
        check=False,
    )
    sys.exit(result.returncode)


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
        offline_package: str = "",
        offline_model_package: str = "",
        resource_package_url: str = "",
        model_endpoint: str = "",
        pip_index_url: str = "",
        pip_trusted_host: str = "",
        install_tensorflow: bool = True,
        log_file: str = "",
        progress_file: str = "",
        existing_install: bool = False,
    ):
        progress_path = progress_file or os.environ.get("TDGPT_PROGRESS_FILE", "")
        self.progress = ProgressReporter(Path(progress_path) if progress_path else None)
        self.offline = offline
        self.all_models = all_models
        self.model_source = model_source
        self.model_endpoint = model_endpoint.strip()
        self.pip_index_url = pip_index_url or os.environ.get("PIP_INDEX_URL", "")
        self.pip_trusted_host = pip_trusted_host or os.environ.get("PIP_TRUSTED_HOST", "")
        self.offline_package = (offline_package or "").strip().strip('"')
        self.offline_model_package = (offline_model_package or "").strip().strip('"')
        self.resource_package_url = (resource_package_url or DEFAULT_MODEL_RESOURCE_PACKAGE_URL).strip()
        self.install_tensorflow = install_tensorflow
        self.service_install_success = False
        self.existing_install_requested = existing_install
        self.install_state = "unknown"
        self.install_state_reasons: List[str] = []
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
        self.phase_timings: List[Tuple[str, float]] = []
        self.install_started_at = time.time()
        self._offline_package_ctx = None
        self._offline_package_root: Optional[Path] = None
        self._offline_package_cached_path = ""
        self._archive_member_cache: Dict[str, set[str]] = {}
        self._offline_models_prepared_from_package = False
        self._downloaded_resource_package: Optional[Path] = None
        self.last_stream_output = ""

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

    @staticmethod
    def format_elapsed(seconds: float) -> str:
        if seconds < 60:
            return f"{seconds:.1f}s"
        minutes, secs = divmod(seconds, 60)
        if minutes < 60:
            return f"{int(minutes)}m {secs:.1f}s"
        hours, minutes = divmod(minutes, 60)
        return f"{int(hours)}h {int(minutes)}m {secs:.1f}s"

    @staticmethod
    def format_size(num_bytes: int) -> str:
        value = float(max(0, num_bytes))
        units = ["B", "KB", "MB", "GB", "TB"]
        for unit in units:
            if value < 1024.0 or unit == units[-1]:
                if unit == "B":
                    return f"{int(value)} {unit}"
                return f"{value:.1f} {unit}"
            value /= 1024.0
        return f"{int(num_bytes)} B"

    def build_download_progress_text(self, downloaded: int, total_size: Optional[int], started_at: float) -> Tuple[str, str]:
        elapsed = max(time.time() - started_at, 0.001)
        speed = downloaded / elapsed
        downloaded_text = self.format_size(downloaded)
        speed_text = self.format_size(int(speed))
        if total_size and total_size > 0:
            total_text = self.format_size(total_size)
            percent = min(100.0, (downloaded / total_size) * 100.0)
            remaining = max(total_size - downloaded, 0)
            if downloaded > 0 and speed > 0:
                eta_text = self.format_elapsed(remaining / speed)
            else:
                eta_text = "estimating..."
            return (
                f"Downloading TDgpt model resource package ({percent:.1f}%)",
                f"{downloaded_text} / {total_text}, {speed_text}/s, ETA {eta_text}",
            )
        return (
            "Downloading TDgpt model resource package",
            f"{downloaded_text} downloaded, {speed_text}/s, ETA unavailable",
        )

    def start_phase_timer(self, label: str) -> float:
        self.print_info(f"[TIMER] {label} started")
        return time.time()

    def finish_phase_timer(self, label: str, started_at: float) -> float:
        elapsed = time.time() - started_at
        self.phase_timings.append((label, elapsed))
        self.print_info(f"[TIMER] {label} completed in {self.format_elapsed(elapsed)}")
        return elapsed

    def get_venv_path(self, name: str) -> Path:
        return self.venvs_dir / name

    def get_venv_python(self, name: str) -> Path:
        return self.get_venv_path(name) / "Scripts" / "python.exe"

    def cleanup_offline_package_cache(self) -> None:
        if self._offline_package_ctx is not None:
            try:
                self._offline_package_ctx.cleanup()
            except Exception:
                pass
        self._offline_package_ctx = None
        self._offline_package_root = None
        self._offline_package_cached_path = ""
        self._archive_member_cache = {}

    def get_offline_package_root(self, package_path: Path) -> Optional[Path]:
        resolved = str(package_path.resolve())
        if self._offline_package_root is not None and self._offline_package_cached_path == resolved:
            self.print_info(f"Reusing extracted offline package: {package_path.name}")
            return self._offline_package_root

        self.cleanup_offline_package_cache()
        self.print_info(f"Extracting offline package once for reuse: {package_path.name}")
        temp_ctx = tempfile.TemporaryDirectory(prefix="tdgpt-offline-package-cache-", dir=str(self.install_dir))
        temp_dir = Path(temp_ctx.name)
        if not self.extract_archive_to_dir(package_path, temp_dir):
            temp_ctx.cleanup()
            return None
        self._offline_package_ctx = temp_ctx
        self._offline_package_root = temp_dir
        self._offline_package_cached_path = resolved
        return temp_dir

    def ensure_model_resource_package_downloaded(self) -> bool:
        if self.offline_model_package:
            return True
        if not self.resource_package_url:
            self.print_error(
                "No local TDgpt model resource package was found, and no model resource package URL is configured."
            )
            return False

        parsed = urllib.parse.urlparse(self.resource_package_url)
        file_name = Path(parsed.path).name or "tdengine-tdgpt-model-resource.tar"
        target_dir = self.log_dir / "downloads"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / file_name
        partial_path = target_path.with_name(target_path.name + ".part")

        self.set_progress(18, "Downloading TDgpt model resource package", f"Connecting to server for {file_name}")
        self.print_info(f"Downloading TDgpt model resource package from {self.resource_package_url}")
        try:
            if partial_path.exists():
                partial_path.unlink()
            request = urllib.request.Request(self.resource_package_url, headers={"User-Agent": "TDgptInstaller/1.0"})
            with urllib.request.urlopen(request, timeout=120) as response, partial_path.open("wb") as fh:
                total_size_text = response.headers.get("Content-Length", "").strip()
                total_size = int(total_size_text) if total_size_text.isdigit() else None
                started_at = time.time()
                downloaded = 0
                last_update_at = 0.0
                last_flush_at = 0.0
                chunk_size = 4 * 1024 * 1024

                if total_size:
                    self.print_info(f"Remote TDgpt model resource package size: {self.format_size(total_size)}")
                progress_title, progress_detail = self.build_download_progress_text(0, total_size, started_at)
                self.set_progress(
                    18,
                    progress_title,
                    progress_detail,
                )

                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    fh.write(chunk)
                    downloaded += len(chunk)

                    now = time.time()
                    if now - last_flush_at >= 1.0:
                        fh.flush()
                        last_flush_at = now
                    if now - last_update_at >= 1.0:
                        progress_title, progress_detail = self.build_download_progress_text(
                            downloaded, total_size, started_at
                        )
                        self.set_progress(
                            18,
                            progress_title,
                            progress_detail,
                        )
                        last_update_at = now

                fh.flush()
                progress_title, progress_detail = self.build_download_progress_text(downloaded, total_size, started_at)
                self.set_progress(
                    18,
                    progress_title,
                    progress_detail,
                )

                if downloaded <= 0:
                    raise RuntimeError(f"Downloaded model resource package is empty: {file_name}")
                if total_size is not None and downloaded != total_size:
                    raise RuntimeError(
                        "Downloaded model resource package is incomplete: "
                        f"expected {self.format_size(total_size)}, got {self.format_size(downloaded)}"
                    )
            partial_path.replace(target_path)
        except Exception as exc:
            try:
                if partial_path.exists():
                    partial_path.unlink()
            except Exception:
                pass
            self.print_error(
                f"No local TDgpt model resource package was found, and the configured model resource package URL could not be downloaded: {self.resource_package_url}. Error: {exc}"
            )
            return False

        self.offline_model_package = str(target_path)
        self._downloaded_resource_package = target_path
        self.print_success(f"Downloaded model resource package: {target_path}")
        return True

    @staticmethod
    def compare_versions(left: str, right: str) -> int:
        def normalize(value: str) -> List[int]:
            result: List[int] = []
            for item in value.strip().split("."):
                if item == "":
                    result.append(0)
                else:
                    result.append(int(item))
            return result

        left_parts = normalize(left)
        right_parts = normalize(right)
        length = max(len(left_parts), len(right_parts))
        left_parts.extend([0] * (length - len(left_parts)))
        right_parts.extend([0] * (length - len(right_parts)))
        if left_parts < right_parts:
            return -1
        if left_parts > right_parts:
            return 1
        return 0

    def is_existing_install(self) -> bool:
        return self.install_state in {"upgrade", "repair"}

    def detect_vc_runtime_version(self) -> Tuple[str, str]:
        runtime_keys = [
            r"SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\x64",
            r"SOFTWARE\WOW6432Node\Microsoft\VisualStudio\14.0\VC\Runtimes\x64",
        ]
        access_modes = [
            winreg.KEY_READ | getattr(winreg, "KEY_WOW64_64KEY", 0),
            winreg.KEY_READ | getattr(winreg, "KEY_WOW64_32KEY", 0),
            winreg.KEY_READ,
        ]

        for subkey in runtime_keys:
            for access in access_modes:
                try:
                    with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, subkey, 0, access) as key:
                        installed = int(winreg.QueryValueEx(key, "Installed")[0])
                        if installed != 1:
                            continue
                        major = int(winreg.QueryValueEx(key, "Major")[0])
                        minor = int(winreg.QueryValueEx(key, "Minor")[0])
                        build = int(winreg.QueryValueEx(key, "Bld")[0])
                        revision = int(winreg.QueryValueEx(key, "Rbld")[0])
                        return f"{major}.{minor}.{build}.{revision}", subkey
                except OSError:
                    continue
        return "", ""

    def check_vc_runtime(self) -> bool:
        detected_version, registry_key = self.detect_vc_runtime_version()
        if not detected_version:
            self.print_error(
                f"{MIN_VC_RUNTIME_FAMILY} {MIN_VC_RUNTIME_VERSION}+ was not detected. "
                f"Install or update it from {VC_RUNTIME_DOWNLOAD_URL}."
            )
            return False

        if self.compare_versions(detected_version, MIN_VC_RUNTIME_VERSION) < 0:
            self.print_error(
                f"Detected {MIN_VC_RUNTIME_FAMILY} {detected_version} via {registry_key}, "
                f"but {MIN_VC_RUNTIME_VERSION}+ is required. Update it from {VC_RUNTIME_DOWNLOAD_URL}."
            )
            return False

        self.print_success(
            f"Detected {MIN_VC_RUNTIME_FAMILY} {detected_version} via {registry_key}."
        )
        return True

    def get_current_interpreter(self) -> Path:
        return Path(sys.executable).resolve()

    def get_runtime_python(self) -> Path:
        return PACKAGED_PYTHON_DIR / "python.exe"

    def repair_pyvenv_cfg(self, venv_name: str) -> bool:
        venv_path = self.get_venv_path(venv_name)
        config_path = venv_path / "pyvenv.cfg"
        runtime_python = self.get_runtime_python()
        if not config_path.exists():
            self.print_error(f"Missing pyvenv.cfg for {venv_name}: {config_path}")
            return False
        if not runtime_python.exists():
            self.print_error(f"Imported Python runtime not found: {runtime_python}")
            return False

        updates = {
            "home": str(PACKAGED_PYTHON_DIR),
            "executable": str(runtime_python),
            "command": f'"{runtime_python}" -m venv "{venv_path}"',
        }
        new_lines: List[str] = []
        seen_keys = set()
        try:
            for raw_line in config_path.read_text(encoding="utf-8").splitlines():
                if "=" not in raw_line:
                    new_lines.append(raw_line)
                    continue
                key, _ = raw_line.split("=", 1)
                normalized_key = key.strip().lower()
                if normalized_key in updates:
                    new_lines.append(f"{key.strip()} = {updates[normalized_key]}")
                    seen_keys.add(normalized_key)
                else:
                    new_lines.append(raw_line)
            for key, value in updates.items():
                if key not in seen_keys:
                    new_lines.append(f"{key} = {value}")
            config_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
        except Exception as exc:
            self.print_error(f"Failed to repair pyvenv.cfg for {venv_name}: {exc}")
            return False
        return True

    def prepare_existing_runtime_reuse(self) -> bool:
        main_venv_python = self.get_venv_python("venv")
        core_req = self.requirements_dir / VENV_CONFIGS["venv"]["requirements"]
        core_stamp = self.get_requirements_stamp_path("venv", VENV_CONFIGS["venv"]["requirements"])
        validation_imports = str(VENV_CONFIGS["venv"].get("validation_imports", "")).strip()

        self.print_info("Offline upgrade requested without an offline package. Checking whether the existing main virtual environment can be reused.")
        self.print_info(f"Reuse check: main venv python = {main_venv_python}")
        self.print_info(f"Reuse check: requirements file = {core_req}")
        self.print_info(f"Reuse check: requirements stamp = {core_stamp}")
        if validation_imports:
            self.print_info(f"Reuse check: validation imports = {validation_imports}")

        if not main_venv_python.exists():
            self.print_error(
                "Offline upgrade cannot reuse the existing environment because "
                "venvs\\venv\\Scripts\\python.exe is missing. Provide an offline package or switch to online mode."
            )
            return False

        if not self.can_reuse_venv("venv", VENV_CONFIGS["venv"]["description"]):
            self.print_error(
                "Offline upgrade cannot reuse the existing main virtual environment. "
                "Provide an offline package or switch to online mode."
            )
            return False

        if core_req.exists() and core_stamp.exists() and not self.has_matching_requirements_stamp(
            "venv", VENV_CONFIGS["venv"]["requirements"], core_req
        ):
            self.print_error(
                "Offline upgrade detected changed Python dependencies for the main virtual environment. "
                "Provide an offline package or switch to online mode so the environment can be refreshed."
            )
            self.print_error(
                f"Reuse check details: requirements stamp mismatch for {core_req.name}. Existing stamp: {core_stamp}"
            )
            return False
        if core_req.exists() and not core_stamp.exists():
            self.print_warning(
                "No dependency stamp was found for the existing main virtual environment. "
                "The installer will reuse it after runtime validation."
            )
            self.print_warning(
                f"Reuse check details: missing requirements stamp file {core_stamp}. Changes to {core_req.name} cannot be verified in offline reuse mode."
            )

        if validation_imports and not self.validate_venv_imports("venv", validation_imports):
            self.print_error(
                "Offline upgrade cannot reuse the existing main virtual environment because validation failed. "
                "Provide an offline package or switch to online mode."
            )
            return False

        self.set_progress(20, "Preparing Python environments", "Reusing the existing Python environments")
        self.python_cmd = str(self.get_current_interpreter())
        self.reused_venvs["venv"] = True
        for venv_name in [name for name in VENV_CONFIGS if name != "venv"]:
            if self.get_venv_python(venv_name).exists():
                self.reused_venvs[venv_name] = True
        self.set_progress(58, "Installing optional TensorFlow support", "Existing Python environments were kept unchanged")
        self.set_progress(82, "Preparing Python environments", "Existing Python environments are ready")
        self.print_success("Existing Python environments were reused successfully.")
        return True

    @staticmethod
    def find_archive_by_name(root_dir: Path, candidate_names: List[str]) -> Optional[Path]:
        normalized = {item.lower() for item in candidate_names}
        for candidate in root_dir.rglob("*"):
            if candidate.is_file() and candidate.name.lower() in normalized:
                return candidate
        return None

    @staticmethod
    def normalize_archive_member_name(name: str) -> str:
        normalized = name.replace("\\", "/").lstrip("./")
        while normalized.startswith("/"):
            normalized = normalized[1:]
        return normalized.rstrip("/")

    def get_archive_member_names(self, archive_path: Path) -> set[str]:
        resolved = str(archive_path.resolve())
        cached = self._archive_member_cache.get(resolved)
        if cached is not None:
            return cached

        members: set[str] = set()
        suffixes = "".join(archive_path.suffixes).lower()
        if suffixes.endswith(".zip"):
            with zipfile.ZipFile(archive_path, "r") as zip_obj:
                for item in zip_obj.namelist():
                    normalized = self.normalize_archive_member_name(item)
                    if normalized:
                        members.add(normalized)
        elif suffixes.endswith(".tar.gz") or suffixes.endswith(".tgz") or suffixes.endswith(".tar"):
            with tarfile.open(archive_path, "r:*") as tar_obj:
                for member in tar_obj.getmembers():
                    normalized = self.normalize_archive_member_name(member.name)
                    if normalized:
                        members.add(normalized)
        else:
            raise RuntimeError(f"Unsupported archive format: {archive_path.name}")

        self._archive_member_cache[resolved] = members
        return members

    @staticmethod
    def has_member_prefix(member_names: set[str], prefix: str) -> bool:
        normalized_prefix = prefix.strip("/").replace("\\", "/")
        return any(item == normalized_prefix or item.startswith(normalized_prefix + "/") for item in member_names)

    def find_direct_directory_prefix(self, member_names: set[str], candidates: List[str], required_files: List[str]) -> Optional[str]:
        for candidate in candidates:
            normalized_candidate = candidate.strip("/").replace("\\", "/")
            if not normalized_candidate:
                continue
            if all(f"{normalized_candidate}/{item}".replace("\\", "/") in member_names for item in required_files):
                return normalized_candidate
        return None

    def find_direct_model_prefix(self, member_names: set[str], model_name: str) -> Optional[str]:
        candidates = [f"model/{model_name}", *self.candidate_model_dir_names(model_name)]
        return self.find_direct_directory_prefix(
            member_names,
            candidates,
            [str(item) for item in MODEL_SPECS[model_name]["flag_files"]],
        )

    def should_skip_system_tar_for_prefix_extract(self, destination_dir: Path, prefixes: List[str]) -> bool:
        """Avoid system tar overwrite on Windows when extracting into an existing install tree.

        Windows tar.exe (libarchive) is fast for fresh extraction, but it is unreliable when
        asked to overwrite an existing python/runtime or venv tree in place. In upgrade flows
        this often emits "Can't unlink already-existing object" and then falls back to Python
        tarfile anyway. Detect that case early and go straight to the Python path.
        """
        try:
            install_root = self.install_dir.resolve()
            dest_root = destination_dir.resolve()
        except Exception:
            install_root = self.install_dir
            dest_root = destination_dir

        if dest_root != install_root:
            return False

        normalized_prefixes: List[str] = []
        for item in prefixes:
            normalized = item.strip("/").replace("\\", "/")
            if normalized and normalized not in normalized_prefixes:
                normalized_prefixes.append(normalized)

        overwrite_sensitive_roots = (
            "python/runtime",
            "venvs/venv",
            "venvs/moirai_venv",
            "venvs/chronos_venv",
            "venvs/timesfm_venv",
            "venvs/momentfm_venv",
        )

        for prefix in normalized_prefixes:
            if any(prefix == root or prefix.startswith(root + "/") for root in overwrite_sensitive_roots):
                return True
        return False

    @staticmethod
    def _rmtree_onerror(func, path, exc_info) -> None:
        try:
            os.chmod(path, stat.S_IWRITE)
        except Exception:
            pass
        func(path)

    def remove_tree_with_retry(self, path: Path, label: str, attempts: int = 5, delay: float = 1.0) -> bool:
        if not path.exists():
            return True

        last_error: Optional[Exception] = None
        retried_after_process_cleanup = False
        for attempt in range(1, attempts + 1):
            try:
                shutil.rmtree(path, onerror=self._rmtree_onerror)
                if not path.exists():
                    return True
            except Exception as exc:
                last_error = exc
                if not retried_after_process_cleanup and isinstance(exc, PermissionError):
                    self.print_warning(
                        f"Removal of {label} was blocked by an in-use file. "
                        "Trying to stop existing installation-tree Python processes once before retrying."
                    )
                    self.stop_existing_install_tree_python_processes()
                    retried_after_process_cleanup = True
            time.sleep(delay)

        detail = f": {last_error}" if last_error else ""
        self.print_error(
            f"Failed to remove existing {label} before offline import{detail}. "
            f"Path: {path}"
        )
        return False

    def extract_selected_prefixes_to_dir(self, archive_path: Path, destination_dir: Path, prefixes: List[str]) -> bool:
        normalized_prefixes = []
        for item in prefixes:
            normalized = item.strip("/").replace("\\", "/")
            if normalized and normalized not in normalized_prefixes:
                normalized_prefixes.append(normalized)
        if not normalized_prefixes:
            return True

        destination_dir.mkdir(parents=True, exist_ok=True)
        tar_exe = shutil.which("tar")
        use_system_tar = tar_exe is not None and not self.should_skip_system_tar_for_prefix_extract(
            destination_dir, normalized_prefixes
        )
        if tar_exe and not use_system_tar:
            self.print_info(
                f"Skipping system tar for selective extraction into existing install tree: "
                f"{archive_path.name} -> {destination_dir}"
            )
        if use_system_tar:
            self.print_info(
                f"Extracting selected package content with system tar: {archive_path.name} -> {destination_dir}"
            )
            result = subprocess.run(
                [tar_exe, "-xf", str(archive_path), "-C", str(destination_dir), *normalized_prefixes],
                capture_output=True,
                text=True,
                timeout=43200,
                check=False,
            )
            if result.returncode == 0:
                return True
            detail = (result.stderr or result.stdout or "").strip()
            self.print_warning(
                f"System tar selective extraction failed for {archive_path.name}"
                + (f": {detail}" if detail else ".")
                + " Falling back to Python tarfile."
            )

        try:
            suffixes = "".join(archive_path.suffixes).lower()
            if suffixes.endswith(".zip"):
                with zipfile.ZipFile(archive_path, "r") as zip_obj:
                    for member_name in zip_obj.namelist():
                        normalized_name = self.normalize_archive_member_name(member_name)
                        if not any(
                            normalized_name == prefix or normalized_name.startswith(prefix + "/")
                            for prefix in normalized_prefixes
                        ):
                            continue
                        zip_obj.extract(member_name, destination_dir)
                return True

            with tarfile.open(archive_path, "r:*") as tar_obj:
                for member in tar_obj.getmembers():
                    normalized_name = self.normalize_archive_member_name(member.name)
                    if not any(
                        normalized_name == prefix or normalized_name.startswith(prefix + "/")
                        for prefix in normalized_prefixes
                    ):
                        continue
                    tar_obj.extract(member, destination_dir)
            return True
        except Exception as exc:
            self.print_error(f"Failed selective extraction for {archive_path.name}: {exc}")
            return False

    def try_prepare_direct_offline_import(self, package_path: Path) -> Optional[bool]:
        try:
            member_names = self.get_archive_member_names(package_path)
        except Exception as exc:
            self.print_warning(f"Failed to inspect offline package {package_path.name}: {exc}")
            return None

        main_venv_prefix = self.find_direct_directory_prefix(member_names, ["venvs/venv"], ["pyvenv.cfg", "Scripts/python.exe"])
        if main_venv_prefix is None:
            self.print_info(
                f"Offline package {package_path.name} does not use the direct venv layout. "
                "Falling back to temporary extraction."
            )
            return None

        venv_prefixes: Dict[str, str] = {"venv": main_venv_prefix}
        for venv_name in [name for name in VENV_CONFIGS if name != "venv"]:
            prefix = self.find_direct_directory_prefix(
                member_names,
                [f"venvs/{venv_name}"],
                ["pyvenv.cfg", "Scripts/python.exe"],
            )
            if prefix:
                venv_prefixes[venv_name] = prefix

        model_prefixes: Dict[str, str] = {}
        if self.model_source == "offline":
            for model_name in ALL_MODELS:
                prefix = self.find_direct_model_prefix(member_names, model_name)
                if prefix:
                    model_prefixes[model_name] = prefix

        install_prefixes = [*venv_prefixes.values(), *model_prefixes.values()]
        self.print_info(
            f"Direct offline import will extract selected package content straight into {self.install_dir}."
        )

        for venv_name in venv_prefixes:
            if not self.remove_tree_with_retry(self.get_venv_path(venv_name), f"{venv_name} directory"):
                return False
        direct_model_root_required = any(prefix.startswith("model/") for prefix in model_prefixes.values())
        if direct_model_root_required:
            self.model_dir.mkdir(parents=True, exist_ok=True)
        for prefix in model_prefixes.values():
            if not prefix.startswith("model/"):
                if not self.remove_tree_with_retry(
                    self.install_dir / prefix.split("/", 1)[0],
                    f"offline model source directory {prefix.split('/', 1)[0]}",
                ):
                    return False
        for model_name in model_prefixes:
            if not self.remove_tree_with_retry(self.model_dir / model_name, f"model directory {model_name}"):
                return False

        if not self.extract_selected_prefixes_to_dir(package_path, self.install_dir, install_prefixes):
            return False

        for venv_name in venv_prefixes:
            python_exe = self.get_venv_python(venv_name)
            config_path = self.get_venv_path(venv_name) / "pyvenv.cfg"
            if not python_exe.exists() or not config_path.exists():
                self.print_error(f"Direct offline import for {venv_name} is incomplete after extraction.")
                return False
            self.reused_venvs[venv_name] = True

        for model_name, prefix in model_prefixes.items():
            target_dir = self.model_dir / model_name
            if prefix.startswith("model/"):
                source_dir = self.install_dir / Path(prefix)
                if not source_dir.exists():
                    self.print_error(f"Direct offline import did not create the expected model directory: {source_dir}")
                    return False
            else:
                source_dir = self.install_dir / prefix.split("/", 1)[0]
                if not source_dir.exists():
                    self.print_error(f"Direct offline import did not create the expected model directory: {source_dir}")
                    return False
                target_dir.parent.mkdir(parents=True, exist_ok=True)
                if target_dir.exists():
                    shutil.rmtree(target_dir, ignore_errors=True)
                shutil.move(str(source_dir), str(target_dir))
            if not self.find_payload_root(target_dir, model_name):
                self.print_error(f"Direct offline import for {model_name} does not contain expected model files.")
                return False
            self.print_success(f"Offline model imported from package: {MODEL_SPECS[model_name]['display']}")

        self._offline_models_prepared_from_package = bool(model_prefixes)
        return True

    @staticmethod
    def find_directory_by_candidates(root_dir: Path, relative_candidates: List[str], required_files: List[str]) -> Optional[Path]:
        for candidate_name in relative_candidates:
            candidate = root_dir / Path(candidate_name)
            if candidate.is_dir() and all((candidate / item).exists() for item in required_files):
                return candidate
        for candidate in root_dir.rglob("*"):
            if not candidate.is_dir():
                continue
            if candidate.name.lower() not in {Path(item).name.lower() for item in relative_candidates}:
                continue
            if all((candidate / item).exists() for item in required_files):
                return candidate
        return None

    def import_venv_payload(self, package_root: Path, venv_name: str, required: bool) -> bool:
        required_files = ["pyvenv.cfg", "Scripts/python.exe"]
        source_dir = self.find_directory_by_candidates(
            package_root,
            OFFLINE_VENV_DIR_CANDIDATES.get(venv_name, [f"venvs/{venv_name}", venv_name]),
            required_files,
        )
        if source_dir is not None:
            return self.deploy_venv_dir(source_dir, venv_name, required)

        archive = self.find_archive_by_name(package_root, OFFLINE_VENV_ARCHIVE_NAMES.get(venv_name, []))
        if archive is None:
            if required:
                self.print_error(
                    f"Offline package does not contain a virtual environment archive for {venv_name}."
                )
            return not required

        with tempfile.TemporaryDirectory(prefix=f"tdgpt-{venv_name}-import-", dir=str(self.install_dir)) as temp_name:
            temp_dir = Path(temp_name)
            if not self.extract_archive_to_dir(archive, temp_dir):
                return False
            source_dir = self.find_directory_by_candidates(
                temp_dir,
                OFFLINE_VENV_DIR_CANDIDATES.get(venv_name, [f"venvs/{venv_name}", venv_name, "."]),
                required_files,
            )
            if source_dir is None:
                self.print_error(f"Imported archive {archive.name} does not contain a valid {venv_name} payload.")
                return False
            return self.deploy_venv_dir(source_dir, venv_name, required)

    def deploy_venv_dir(self, source_dir: Path, venv_name: str, required: bool) -> bool:
        target_dir = self.get_venv_path(venv_name)
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source_dir, target_dir)
        python_exe = self.get_venv_python(venv_name)
        config_path = target_dir / "pyvenv.cfg"
        if not python_exe.exists() or not config_path.exists():
            if required:
                self.print_error(f"Offline import for {venv_name} is incomplete after extraction.")
                return False
            shutil.rmtree(target_dir, ignore_errors=True)
            return True
        self.reused_venvs[venv_name] = True
        return True

    def prepare_external_offline_runtime(self) -> bool:
        if not self.offline_package:
            if self.is_existing_install():
                return self.prepare_existing_runtime_reuse()
            self.print_error(
                "No local TDgpt main venv offline package was provided for first-time offline installation."
            )
            return False
        package_path = Path(self.offline_package)
        if not package_path.exists():
            self.print_error(f"Offline package not found: {package_path}")
            return False

        self.set_progress(20, "Preparing Python environments", f"Importing packaged virtual environments from {package_path.name}")
        package_timer = self.start_phase_timer(f"Import offline venv bundle {package_path.name}")
        direct_result = self.try_prepare_direct_offline_import(package_path)
        if direct_result is None:
            temp_dir = self.get_offline_package_root(package_path)
            if temp_dir is None:
                return False
            if not self.import_venv_payload(temp_dir, "venv", required=True):
                return False
            for venv_name in [name for name in VENV_CONFIGS if name != "venv"]:
                if not self.import_venv_payload(temp_dir, venv_name, required=False):
                    return False
        elif not direct_result:
            return False

        self.python_cmd = str(self.get_current_interpreter())
        for venv_name in VENV_CONFIGS:
            config_path = self.get_venv_path(venv_name) / "pyvenv.cfg"
            if not self.get_venv_python(venv_name).exists() or not config_path.exists():
                continue
            if not self.repair_pyvenv_cfg(venv_name):
                return False

        validation_imports = str(VENV_CONFIGS["venv"].get("validation_imports", "")).strip()
        if validation_imports and not self.validate_venv_imports("venv", validation_imports):
            self.print_error("Offline imported main virtual environment validation failed.")
            return False

        self.finish_phase_timer(f"Import offline venv bundle {package_path.name}", package_timer)
        self.set_progress(58, "Preparing Python environments", "The imported TDgpt venv package already includes the required virtual environments")
        self.set_progress(82, "Preparing Python environments", "Imported Python environments are ready")
        self.print_success("Offline virtual environments were imported successfully.")
        return True

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
                    self.last_stream_output = line
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
            self.last_stream_output = line
            self.write_output(line)
            self.progress.update(
                "running",
                self._progress_percent_for_line(line, start_percent, end_percent),
                title,
                line[:240],
            )

    def build_stream_failure_detail(self, label: str, suffix: str) -> str:
        detail = f"{label} {suffix}"
        if self.last_stream_output:
            detail = f"{detail}. Last output: {self.last_stream_output}"
        return detail

    def run_stream(self, command: List[str], label: str, timeout: int, start_percent: int, end_percent: int, title: str,
                   env: Optional[Dict[str, str]] = None) -> bool:
        self.print_info(label)
        self.last_stream_output = ""
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
            detail = f"{label} failed to start: {exc}"
            self.print_error(detail)
            self.set_progress(100, "Installation failed", detail[:240], status="error")
            return False
        try:
            assert proc.stdout is not None
            self._stream_process_output(proc.stdout, start_percent, end_percent, title)
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            detail = self.build_stream_failure_detail(label, "timed out")
            self.print_error(detail)
            self.set_progress(100, "Installation failed", detail[:240], status="error")
            return False
        if proc.returncode != 0:
            detail = self.build_stream_failure_detail(label, f"failed with exit code {proc.returncode}")
            self.print_error(detail)
            self.set_progress(100, "Installation failed", detail[:240], status="error")
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
        current_python = self.get_current_interpreter()
        try:
            result = subprocess.run(
                [str(current_python), "--version"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            output = (result.stdout or result.stderr).strip()
            if result.returncode == 0 and output.startswith("Python "):
                major, minor = map(int, output.split()[1].split(".")[:2])
                if major == 3 and minor in [10, 11, 12]:
                    self.python_cmd = str(current_python)
                    self.python_version = output.split()[1]
                    self.print_success(f"Using current Python {self.python_version}.")
                    return True
        except Exception:
            pass

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
        last_error = ""
        commands = [
            ([self.python_cmd, "-I", "-m", "pip", "--version"], 20),
            ([self.python_cmd, "-m", "pip", "--version"], 10),
        ]
        for attempt in range(1, 3):
            for command, timeout_seconds in commands:
                try:
                    result = subprocess.run(
                        command,
                        capture_output=True,
                        text=True,
                        timeout=timeout_seconds,
                        check=False,
                    )
                except subprocess.TimeoutExpired:
                    last_error = f"{' '.join(command)} timed out after {timeout_seconds} seconds"
                    self.print_warning(
                        f"pip probe attempt {attempt} timed out with {' '.join(command[1:])} after {timeout_seconds}s; retrying."
                    )
                    continue
                except Exception as exc:
                    last_error = f"{' '.join(command)} failed: {exc}"
                    continue
                if result.returncode == 0:
                    detail = (result.stdout or result.stderr or "").strip()
                    if detail:
                        self.print_info(detail)
                    return True
                last_error = f"{' '.join(command)}: {(result.stderr or result.stdout or f'exited with code {result.returncode}').strip()}"
            if attempt < 2:
                time.sleep(1)
        if last_error:
            self.print_error(f"Failed to check pip version: {last_error}")
        else:
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

    def find_install_tree_python_processes(self) -> List[Tuple[int, str]]:
        prefixes = [
            str((self.install_dir / "python" / "runtime").resolve()).lower(),
            str(self.venvs_dir.resolve()).lower(),
        ]
        prefixes_literal = ", ".join("'" + item.replace("'", "''") + "'" for item in prefixes)
        powershell_exe = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
        script = (
            "$prefixes = @(" + prefixes_literal + "); "
            "$items = @(); "
            "Get-CimInstance Win32_Process | Where-Object { $_.ExecutablePath } | ForEach-Object { "
            "  $exe = $_.ExecutablePath.ToLower(); "
            "  foreach ($prefix in $prefixes) { "
            "    if ($exe.StartsWith($prefix)) { "
            "      $items += [PSCustomObject]@{ ProcessId = $_.ProcessId; ExecutablePath = $_.ExecutablePath }; "
            "      break; "
            "    } "
            "  } "
            "}; "
            "$items | ConvertTo-Json -Compress"
        )
        try:
            result = subprocess.run(
                [
                    str(powershell_exe),
                    "-NoProfile",
                    "-NonInteractive",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-Command",
                    script,
                ],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
        except Exception as exc:
            self.print_warning(f"Unable to inspect existing installation-tree Python processes: {exc}")
            return []

        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            if detail:
                self.print_warning(f"Unable to inspect existing installation-tree Python processes: {detail}")
            return []

        payload = (result.stdout or "").strip()
        if not payload:
            return []
        try:
            decoded = json.loads(payload)
        except Exception:
            return []

        if isinstance(decoded, dict):
            decoded = [decoded]
        if not isinstance(decoded, list):
            return []

        processes: List[Tuple[int, str]] = []
        for item in decoded:
            if not isinstance(item, dict):
                continue
            try:
                pid = int(item.get("ProcessId"))
            except Exception:
                continue
            exe_path = str(item.get("ExecutablePath") or "").strip()
            if pid <= 0 or not exe_path or pid == os.getpid():
                continue
            processes.append((pid, exe_path))
        return processes

    def stop_existing_install_tree_python_processes(self) -> None:
        processes = self.find_install_tree_python_processes()
        if not processes:
            self.print_info("No existing installation-tree Python processes were detected.")
            return

        self.print_warning("Stopping existing Python processes from the current installation tree before offline import...")
        for pid, exe_path in processes:
            self.print_warning(f"Stopping PID {pid}: {exe_path}")
            try:
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(pid)],
                    capture_output=True,
                    text=True,
                    timeout=20,
                    check=False,
                )
            except Exception as exc:
                self.print_warning(f"Unable to stop PID {pid}: {exc}")
        time.sleep(2)

    def stop_existing_service(self) -> None:
        self.print_info("Stopping any existing Taosanode service before installation...")
        try:
            query = subprocess.run(
                ["sc", "query", "Taosanode"],
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
            if query.returncode != 0:
                self.print_info("No existing Taosanode service was detected.")
                return
        except Exception as exc:
            self.print_warning(f"Unable to query existing Taosanode service: {exc}")
            return

        for command in (
            ["sc", "stop", "Taosanode"],
            [str(self.install_dir / "bin" / "taosanode-winsw.exe"), "stop"],
        ):
            try:
                if command[0].endswith(".exe") and not Path(command[0]).exists():
                    continue
                subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
            except Exception:
                pass

        try:
            subprocess.run(
                ["taskkill", "/F", "/IM", "taosanode-winsw.exe"],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
        except Exception:
            pass
        time.sleep(2)
        # Only offline package reimport replaces installation-tree runtimes.
        # Online upgrades and offline reuse must keep the active environment alive.
        if self.offline and bool(self.offline_package):
            self.stop_existing_install_tree_python_processes()

    def service_exists(self, service_name: str = "Taosanode") -> bool:
        try:
            query = subprocess.run(
                ["sc", "query", service_name],
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
        except Exception as exc:
            self.print_warning(f"Unable to query Windows service {service_name}: {exc}")
            return False
        return query.returncode == 0 and service_name.lower() in (query.stdout or "").lower()

    def detect_install_state(self) -> None:
        reasons: List[str] = []
        if self.existing_install_requested:
            reasons.append("setup wizard detected an existing installation")
        if INSTALL_STATE_FILE.exists():
            reasons.append("install-state marker")

        if reasons:
            self.install_state = "upgrade"
            self.install_state_reasons = reasons
            return

        repair_reasons: List[str] = []
        if ENABLED_MODELS_FILE.exists():
            repair_reasons.append("enabled_models.txt")
        if self.service_exists():
            repair_reasons.append("registered Taosanode service")
        if self.get_venv_python("venv").exists():
            repair_reasons.append("existing main virtual environment")
        for model_name in ALL_MODELS:
            if self.find_payload_root(self.model_dir / model_name, model_name):
                repair_reasons.append(f"existing model payload: {model_name}")
                break

        if repair_reasons:
            self.install_state = "repair"
            self.install_state_reasons = repair_reasons
        else:
            self.install_state = "fresh"
            self.install_state_reasons = []

    def can_reuse_venv(self, name: str, description: str) -> bool:
        path = self.get_venv_path(name)
        python_exe = path / "Scripts" / "python.exe"
        if not path.exists():
            self.print_warning(f"{description} is missing: {path}")
            return False
        if not python_exe.exists():
            self.print_warning(f"{description} is incomplete because python.exe is missing. Recreating it.")
            return False
        self.print_info(f"Checking whether {description} can be reused with {python_exe}")
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
        pip_detail = (result.stdout or result.stderr or "").strip()
        if pip_detail:
            self.print_info(f"{description} pip check passed: {pip_detail}")
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
        self.print_info(
            f"Validating {venv_name} imports with {python_exe}: {', '.join(imports)}"
        )
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

    def get_requirements_stamp_path(self, venv_name: str, req_name: str) -> Path:
        stamp_name = req_name.replace("\\", "_").replace("/", "_") + ".stamp"
        return self.get_venv_path(venv_name) / ".tdgpt-stamps" / stamp_name

    def build_requirements_stamp(self, req_file: Path) -> str:
        digest = hashlib.sha256()
        digest.update(req_file.read_bytes())
        digest.update(b"\n---\n")
        digest.update((self.pip_index_url or "").encode("utf-8"))
        digest.update(b"\n")
        digest.update((self.pip_trusted_host or "").encode("utf-8"))
        digest.update(b"\n")
        digest.update((self.python_version or "").encode("utf-8"))
        return digest.hexdigest()

    def has_matching_requirements_stamp(self, venv_name: str, req_name: str, req_file: Path) -> bool:
        stamp_path = self.get_requirements_stamp_path(venv_name, req_name)
        if not stamp_path.exists():
            return False
        try:
            return stamp_path.read_text(encoding="utf-8").strip() == self.build_requirements_stamp(req_file)
        except Exception:
            return False

    def write_requirements_stamp(self, venv_name: str, req_name: str, req_file: Path) -> None:
        stamp_path = self.get_requirements_stamp_path(venv_name, req_name)
        stamp_path.parent.mkdir(parents=True, exist_ok=True)
        stamp_path.write_text(self.build_requirements_stamp(req_file), encoding="utf-8")

    def install_req_file(self, venv_name: str, req_name: str, start_percent: int, end_percent: int, title: str) -> bool:
        req_file = self.requirements_dir / req_name
        if not req_file.exists():
            self.print_warning(f"Requirements file not found: {req_file}")
            return True
        if self.reused_venvs.get(venv_name) and self.has_matching_requirements_stamp(venv_name, req_name, req_file):
            self.print_info(f"Requirements already satisfied for {venv_name}: {req_name}")
            self.set_progress(end_percent, title, f"Using existing packages for {venv_name}")
            return True
        python_exe = self.get_venv_python(venv_name)
        upgrade_end = start_percent + max(1, (end_percent - start_percent) // 4)
        upgrade = [str(python_exe), "-m", "pip", "install", "--upgrade", "pip", "--progress-bar", "on"]
        self.add_pip_options(upgrade)
        upgrade_timer = self.start_phase_timer(f"{venv_name}: upgrade pip")
        if not self.run_stream(upgrade, f"Upgrading pip in {venv_name}", 1800, start_percent, upgrade_end, title):
            return False
        self.finish_phase_timer(f"{venv_name}: upgrade pip", upgrade_timer)
        install = [str(python_exe), "-m", "pip", "install", "-r", str(req_file), "--progress-bar", "on"]
        self.add_pip_options(install)
        install_timer = self.start_phase_timer(f"{venv_name}: install {req_name}")
        if not self.run_stream(install, f"Installing {req_name} in {venv_name}", 7200, upgrade_end, end_percent, title):
            return False
        self.finish_phase_timer(f"{venv_name}: install {req_name}", install_timer)
        self.write_requirements_stamp(venv_name, req_name, req_file)
        return True

    def prepare_venv(self, venv_name: str, start_percent: int, end_percent: int) -> bool:
        config = VENV_CONFIGS[venv_name]
        desc = config["description"]
        validation_imports = str(config.get("validation_imports", "")).strip()
        phase_label = f"Prepare {venv_name} ({desc})"

        for attempt in range(2):
            phase_timer = self.start_phase_timer(phase_label)
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
                self.finish_phase_timer(phase_label, phase_timer)
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
        if self.offline:
            return self.prepare_external_offline_runtime()
        if not self.prepare_venv("venv", 20, 42):
            return False
        if self.install_tensorflow:
            self.set_progress(46, "Installing optional TensorFlow support", "Downloading TensorFlow CPU support")
            tensorflow_timer = self.start_phase_timer("Install TensorFlow CPU support")
            if not self.install_req_file("venv", "requirements_tensorflow.txt", 46, 58, "Installing TensorFlow CPU support"):
                return False
            self.finish_phase_timer("Install TensorFlow CPU support", tensorflow_timer)
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
            destination_dir.mkdir(parents=True, exist_ok=True)
            if suffixes.endswith(".zip"):
                with zipfile.ZipFile(archive_path, "r") as zip_obj:
                    zip_obj.extractall(destination_dir)
            elif suffixes.endswith(".tar.gz") or suffixes.endswith(".tgz") or suffixes.endswith(".tar"):
                tar_exe = shutil.which("tar")
                if tar_exe:
                    self.print_info(f"Extracting {archive_path.name} with system tar: {tar_exe}")
                    result = subprocess.run(
                        [tar_exe, "-xf", str(archive_path), "-C", str(destination_dir)],
                        capture_output=True,
                        text=True,
                        timeout=43200,
                        check=False,
                    )
                    if result.returncode == 0:
                        return True
                    detail = (result.stderr or result.stdout or "").strip()
                    self.print_warning(
                        f"System tar failed for {archive_path.name}"
                        + (f": {detail}" if detail else ".")
                        + " Falling back to Python tarfile."
                    )
                else:
                    self.print_info(f"System tar was not found. Falling back to Python tarfile for {archive_path.name}.")

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
        if not root_dir.exists():
            return None
        flag_files = [str(flag_name) for flag_name in MODEL_SPECS[model_name]["flag_files"]]
        candidate_dirs: List[Path] = [root_dir]
        seen = {str(root_dir.resolve())}
        for flag_name in flag_files:
            for candidate in root_dir.rglob(flag_name):
                if not candidate.is_file():
                    continue
                parent = candidate.parent
                parent_key = str(parent.resolve())
                if parent_key not in seen:
                    seen.add(parent_key)
                    candidate_dirs.append(parent)
        for candidate_dir in candidate_dirs:
            if all((candidate_dir / flag_name).exists() for flag_name in flag_files):
                return candidate_dir
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
        archive_timer = self.start_phase_timer(f"Import offline archive for {model_name}")
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
        self.finish_phase_timer(f"Import offline archive for {model_name}", archive_timer)
        self.print_success(f"Offline model imported: {MODEL_SPECS[model_name]['display']}")
        return True

    def import_offline_model_package(self, package_path: Path) -> bool:
        package_timer = self.start_phase_timer(f"Import offline model package {package_path.name}")
        temp_dir = self.get_offline_package_root(package_path)
        if temp_dir is None:
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

        imported_model_venvs: Set[str] = set()
        for model_name in imported_models:
            venv_name = str(MODEL_SPECS[model_name].get("venv") or "").strip()
            if not venv_name or venv_name == "venv" or venv_name in imported_model_venvs:
                continue
            venv_target = self.get_venv_path(venv_name)
            existed_before = venv_target.exists()
            if not self.import_venv_payload(temp_dir, venv_name, required=False):
                return False
            if venv_target.exists() and not existed_before:
                self.print_success(f"Offline model venv imported from package: {venv_name}")
            imported_model_venvs.add(venv_name)

        self.finish_phase_timer(f"Import offline model package {package_path.name}", package_timer)
        return True

    def import_offline_models(self) -> bool:
        archives = self.resolve_offline_archives()
        offline_package = Path(self.offline_model_package) if self.offline_model_package else None
        if offline_package and not offline_package.exists():
            raise FileNotFoundError(f"Offline model package not found: {offline_package}")

        if offline_package is None and self.resource_package_url:
            if not self.ensure_model_resource_package_downloaded():
                return False
            offline_package = Path(self.offline_model_package) if self.offline_model_package else None

        if offline_package is None and self.offline_package:
            offline_package = Path(self.offline_package)

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
        existing_payload = self.find_payload_root(target_dir, model_name)
        if existing_payload:
            self.print_info(
                f"Existing {spec['display']} model detected in {existing_payload}. Skipping online download."
            )
            self.set_progress(
                end_percent,
                f"Downloading {spec['display']}",
                f"Existing model files detected in {existing_payload.name}; download skipped",
            )
            return True
        target_dir.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        if self.model_endpoint:
            env["HF_ENDPOINT"] = self.model_endpoint
        env["HF_HUB_DISABLE_PROGRESS_BARS"] = "0"
        env["HF_HUB_DISABLE_TELEMETRY"] = "1"
        python_exe = self.get_venv_python(str(spec["venv"]))
        inline = """
import os
import sys
from huggingface_hub import snapshot_download

repo_id, local_dir, endpoint = sys.argv[1:4]
try:
    snapshot_download(repo_id=repo_id, local_dir=local_dir, endpoint=(endpoint or None))
    print(f"Download completed for {repo_id}")
except Exception as exc:
    print(f"Download failed for {repo_id}: {exc}")
    raise
"""
        download_timer = self.start_phase_timer(f"Download model {model_name}")
        success = self.run_stream(
            [str(python_exe), "-c", inline, repo_id, str(target_dir), self.model_endpoint],
            f"Downloading {spec['display']} ({spec['download_size']})",
            43200,
            start_percent,
            end_percent,
            f"Downloading {spec['display']}",
            env=env,
        )
        if success:
            self.finish_phase_timer(f"Download model {model_name}", download_timer)
        return success

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
        if self.is_existing_install() and self.model_source == "none":
            self.set_progress(94, "Model installation", "Existing model files were kept unchanged")
            return True
        if self.is_existing_install() and self.model_source == "offline" and not self.offline_package:
            self.set_progress(94, "Model installation", "Existing model files were kept unchanged")
            return True
        if self._offline_models_prepared_from_package:
            self.set_progress(94, "Model installation", "Offline model files were already imported directly from the package")
            return True
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

    def write_install_state(self) -> None:
        payload = {
            "version": PACKAGE_METADATA.get("version", ""),
            "install_state": self.install_state,
            "installed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "service_installed": self.service_install_success,
        }
        INSTALL_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        INSTALL_STATE_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    def install_service(self) -> bool:
        service_script = self.install_dir / "bin" / "taosanode_service.py"
        python_exe = self.get_venv_python("venv")
        if not service_script.exists() or not python_exe.exists():
            self.service_install_success = False
            self.print_error("Service installer prerequisites are missing. Windows service registration cannot continue.")
            return False
        try:
            self.set_progress(96, "Registering the Windows service", "Installing Taosanode service")
            subprocess.run([str(python_exe), str(service_script), "install-service"], check=True, timeout=180)
            self.service_install_success = True
            self.print_success("Windows service installed successfully.")
            return True
        except Exception as exc:
            self.service_install_success = False
            self.print_error(f"Service installation failed: {exc}")
            return False

    def append_install_summary(self) -> None:
        lines = [
            "",
            "=" * 72,
            f"{APP_DISPLAY_NAME} Windows Installation Summary",
            "=" * 72,
            f"Install directory: {self.install_dir}",
            f"Install type: {self.install_state}",
            "Install state markers: " + (", ".join(self.install_state_reasons) if self.install_state_reasons else "None"),
            f"Python version: {self.python_version or 'Unknown'}",
            "Python package mode: " + ("Offline" if self.offline else "Online"),
            f"TensorFlow support: {'Installed' if self.install_tensorflow else 'Skipped'}",
            f"Model source: {self.model_source}",
            "Selected models: " + (", ".join(self.selected_models) if self.selected_models else "None"),
            f"Main venv package: {self.offline_package or 'None'}",
            f"Model resource package: {self.offline_model_package or 'None'}",
            f"Model resource package URL: {self.resource_package_url or 'None'}",
            f"Pip index: {self.pip_index_url or 'Default'}",
            f"Pip trusted host: {self.pip_trusted_host or 'Default'}",
            f"Model endpoint: {self.model_endpoint or 'Default Hugging Face'}",
            f"Service install result: {'Installed' if self.service_install_success else 'Failed'}",
            f"Total installer runtime: {self.format_elapsed(time.time() - self.install_started_at)}",
            "Virtual environments:",
        ]
        if self.model_source == "offline":
            lines.append("Model startup behavior: start-model all checks imported model directories automatically.")
        elif self.model_source == "online":
            lines.append("Model startup behavior: start-model all starts downloaded model directories automatically.")
        elif self.is_existing_install():
            lines.append("Model startup behavior: start-model all reuses existing model directories automatically.")
        for venv_name in VENV_CONFIGS:
            python_exe = self.get_venv_python(venv_name)
            lines.append(f"  [{'OK' if python_exe.exists() else 'NO'}] {venv_name}")
        if self.phase_timings:
            lines.append("Phase timings:")
            for label, elapsed in self.phase_timings:
                lines.append(f"  - {label}: {self.format_elapsed(elapsed)}")
        lines.append("=" * 72)
        if self.stdout_redirected_to_log:
            for line in lines:
                print(line)
        else:
            with self.log_file.open("a", encoding="utf-8") as fh:
                fh.write("\n".join(lines) + "\n")

    def run(self) -> bool:
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            self.set_progress(1, f"Initializing {APP_DISPLAY_NAME} setup", "Preparing installation environment")
            self.print_info("=" * 60)
            self.print_info(f"{APP_DISPLAY_NAME} Windows Installer")
            self.print_info("=" * 60)
            self.print_info(f"Install directory: {self.install_dir}")
            self.print_info(f"Requirements directory: {self.requirements_dir}")
            self.print_info(f"Virtual environments directory: {self.venvs_dir}")
            self.print_info(f"Log file: {self.log_file}")
            self.print_info("=" * 60)
            self.check_admin_privileges()
            self.set_progress(5, "Checking system requirements", "Checking disk space")
            timer = self.start_phase_timer("Check disk space")
            if not self.check_disk_space():
                return False
            self.finish_phase_timer("Check disk space", timer)
            self.set_progress(7, "Checking system requirements", "Checking Visual C++ runtime")
            timer = self.start_phase_timer("Check Visual C++ runtime")
            if not self.check_vc_runtime():
                return False
            self.finish_phase_timer("Check Visual C++ runtime", timer)
            self.set_progress(8, "Checking system requirements", "Checking Python")
            timer = self.start_phase_timer("Check Python")
            if not self.check_python():
                return False
            self.finish_phase_timer("Check Python", timer)
            self.set_progress(10, "Checking system requirements", "Checking pip")
            timer = self.start_phase_timer("Check pip")
            if not self.check_pip():
                return False
            self.finish_phase_timer("Check pip", timer)
            self.detect_install_state()
            self.print_info(
                f"Detected install state: {self.install_state}"
                + (f" ({', '.join(self.install_state_reasons)})" if self.install_state_reasons else "")
            )
            self.set_progress(12, "Preparing directories", "Creating required folders")
            timer = self.start_phase_timer("Create directories")
            if not self.create_directories():
                return False
            self.finish_phase_timer("Create directories", timer)
            self.set_progress(14, "Preparing installation environment", "Stopping existing Taosanode service")
            timer = self.start_phase_timer("Stop existing Taosanode service")
            self.stop_existing_service()
            self.finish_phase_timer("Stop existing Taosanode service", timer)
            timer = self.start_phase_timer("Install Python environments")
            if not self.install_venvs():
                return False
            self.finish_phase_timer("Install Python environments", timer)
            timer = self.start_phase_timer("Prepare models")
            if not self.process_models():
                return False
            self.finish_phase_timer("Prepare models", timer)
            timer = self.start_phase_timer("Write enabled model configuration")
            self.write_enabled_models()
            self.finish_phase_timer("Write enabled model configuration", timer)
            timer = self.start_phase_timer("Install Windows service")
            if not self.install_service():
                return False
            self.finish_phase_timer("Install Windows service", timer)
            timer = self.start_phase_timer("Write install state marker")
            self.write_install_state()
            self.finish_phase_timer("Write install state marker", timer)
            self.append_install_summary()
            self.set_progress(100, "Installation complete", f"{APP_DISPLAY_NAME} is ready", status="success")
            self.print_success(f"{APP_DISPLAY_NAME} installation completed.")
            self.print_info("Start service: net start Taosanode")
            self.print_info("Stop service: net stop Taosanode")
            self.print_info(f"Script start: {self.install_dir / 'bin' / 'start-taosanode.bat'}")
            self.print_info(f"Script stop: {self.install_dir / 'bin' / 'stop-taosanode.bat'}")
            return True
        finally:
            self.cleanup_offline_package_cache()


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
        description="TDgpt Windows installation script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python install.py
  python install.py --skip-tensorflow
  python install.py --model-source online --model moirai --model moment --model-endpoint https://hf-mirror.com
    python install.py -o --offline-package D:\tdengine-tdgpt-venv-offline-1.0.tar.gz
    python install.py -o --model-source offline --offline-model-package D:\tdengine-tdgpt-model-resource-1.0.tar.gz
    python install.py -o --offline-package D:\tdengine-tdgpt-venv-offline-1.0.tar.gz --model-source offline --resource-package-url https://downloads.example.com/tdengine-tdgpt-model-resource-1.0.tar.gz
  python install.py -o --existing-install
        """.strip(),
    )
    parser.add_argument("-o", "--offline", action="store_true",
                                                help="Offline/resource-package installation mode. Imports virtual environments and model payloads from one external tar archive; upgrade can also reuse the current environment.")
    parser.add_argument("-a", "--all-models", action="store_true", help="Select all supported models for the chosen model source.")
    parser.add_argument("--model-source", choices=["none", "online", "offline"], default="none",
                        help="Choose how selected models should be prepared.")
    parser.add_argument("--model", action="append", dest="models",
                        help="Supported values: tdtsfm, timemoe, moirai, chronos, timesfm, moment.")
    parser.add_argument("--model-archive", action="append", dest="model_archives",
                        help="Offline archive mapping in the form <model>=<path>.")
    parser.add_argument("--offline-package",
                        help="TDgpt main venv offline package tar archive. Legacy combined packages are still accepted.")
    parser.add_argument("--offline-model-package",
                        help="Optional TDgpt model resource package tar archive.")
    parser.add_argument("--resource-package-url",
                        help="Remote TDgpt model resource package URL used when --model-source offline is selected and no local model package is provided.")
    parser.add_argument("--model-endpoint", help="Optional Hugging Face mirror endpoint used for online model downloads.")
    parser.add_argument("--pip-index-url", help="Custom pip index URL used for dependency installation.")
    parser.add_argument("--pip-trusted-host", help="Optional trusted host used together with the pip index URL.")
    parser.add_argument("--skip-tensorflow", action="store_true",
                        help="Do not install TensorFlow CPU support into the main virtual environment.")
    parser.add_argument("--log-file", help="Installation log file path. Defaults to <install_dir>\\log\\install.log.")
    parser.add_argument("--progress-file", help="Installer progress state file used by the Windows wizard.")
    parser.add_argument("--existing-install", action="store_true",
                        help="Internal flag used by the Windows wizard when an existing installation was detected.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    maybe_reexec_for_offline_import(args)
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
        offline_package=args.offline_package or "",
        offline_model_package=args.offline_model_package or "",
        resource_package_url=args.resource_package_url or "",
        model_endpoint=args.model_endpoint or "",
        pip_index_url=args.pip_index_url or "",
        pip_trusted_host=args.pip_trusted_host or "",
        install_tensorflow=not args.skip_tensorflow,
        log_file=args.log_file or "",
        progress_file=args.progress_file or "",
        existing_install=args.existing_install,
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
