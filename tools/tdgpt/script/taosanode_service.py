#!/usr/bin/env python3
"""
TDgpt Taosanode Unified Service Manager
Cross-platform service management for Linux and Windows

Usage:
    python taosanode_service.py [command] [options]

Commands:
    start               Start taosanode main service
    stop                Stop taosanode main service
    status              Show service status
    model-start [name]  Start model service (name: tdtsfm, timemoe, moirai, chronos, timesfm, moment, all)
    model-stop [name]   Stop model service
    model-status        Show model service status
    install             Install as system service (Windows: winsw, Linux: systemd)
    uninstall           Uninstall system service

Examples:
    python taosanode_service.py start
    python taosanode_service.py model-start tdtsfm
    python taosanode_service.py model-start all
    python taosanode_service.py model-stop all
"""

import os
import sys
import ast
import platform
import subprocess
import signal
import time
import argparse
import logging
import re
import urllib.request
import urllib.error
from logging.handlers import RotatingFileHandler
from typing import Optional, List, Dict

# Platform detection
IS_WINDOWS = platform.system().lower() == "windows"
IS_LINUX = platform.system().lower() == "linux"

# Default paths - will be overridden by config
# Auto-detect install dir from script location: <install_dir>/bin/taosanode_service.py
_script_dir = os.path.dirname(os.path.abspath(__file__))
_script_parent = os.path.dirname(_script_dir)
if IS_WINDOWS:
    # Script is in <install_dir>\bin\ when installed via Inno Setup
    DEFAULT_INSTALL_DIR = _script_parent if os.path.basename(_script_dir).lower() == "bin" else r"C:\TDengine\taosanode"
else:
    DEFAULT_INSTALL_DIR = _script_parent if os.path.basename(_script_dir) == "bin" else "/usr/local/taos/taosanode"
DEFAULT_DATA_DIR = os.path.join(DEFAULT_INSTALL_DIR, "data")
DEFAULT_LOG_DIR = os.path.join(DEFAULT_INSTALL_DIR, "log")
SERVICE_LOG_FILE = os.path.join(DEFAULT_LOG_DIR, "taosanode-service.log")
WINSW_EXE_NAME = "taosanode-winsw.exe"
_redirect_stream = None
DEFAULT_MODEL_ORDER = ["tdtsfm", "timemoe", "moirai", "chronos", "timesfm", "moment"]

# PID file paths
PID_FILE = os.path.join(DEFAULT_INSTALL_DIR, "taosanode.pid")
MODEL_PID_DIR = os.path.join(DEFAULT_DATA_DIR, "pids")

# Model configurations - will be loaded from config file
MODELS = {}

# Global logger instance
_logger = None


def get_windows_friendly_log_encoding(log_file: str) -> str:
    """Use UTF-8 with BOM for new Windows log files so common editors detect Unicode correctly."""
    if platform.system().lower() != "windows":
        return "utf-8"
    try:
        if os.path.exists(log_file) and os.path.getsize(log_file) > 0:
            return "utf-8"
    except OSError:
        pass
    return "utf-8-sig"


def setup_logger(name: str, log_file: str, level=logging.INFO) -> logging.Logger:
    """Create and configure a logger instance

    Args:
        name: Logger name (used to distinguish log sources)
        log_file: Log file path
        level: Log level

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # Prefer file logging, but do not fail if the default install path is not writable.
    log_dir = os.path.dirname(log_file)
    try:
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding=get_windows_friendly_log_encoding(log_file),
            errors="replace",
        )
        file_handler.setLevel(level)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    except OSError:
        pass

    if os.environ.get("TAOSANODE_DISABLE_CONSOLE_LOG") != "1":
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_formatter = logging.Formatter('%(levelname)s: %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    if not logger.handlers:
        logger.addHandler(logging.NullHandler())

    return logger


def redirect_stdio_if_needed():
    """Redirect stdout/stderr to a file when running under WinSW."""
    global _redirect_stream
    target = os.environ.get("TAOSANODE_REDIRECT_STDIO", "").strip()
    if not target or _redirect_stream is not None:
        return

    os.makedirs(os.path.dirname(target), exist_ok=True)
    _redirect_stream = open(
        target,
        "a",
        encoding=get_windows_friendly_log_encoding(target),
        errors="replace",
        buffering=1,
    )
    sys.stdout = _redirect_stream
    sys.stderr = _redirect_stream


def get_logger(name: str = "taosanode_service") -> logging.Logger:
    """Get global logger instance"""
    global _logger
    if _logger is None:
        log_file = SERVICE_LOG_FILE
        _logger = setup_logger(name, log_file)
    return _logger


class Config:
    """Configuration manager"""

    def __init__(self, config_path: Optional[str] = None):
        self.install_dir = DEFAULT_INSTALL_DIR
        self.data_dir = DEFAULT_DATA_DIR
        self.log_dir = DEFAULT_LOG_DIR
        self.cfg_dir = os.path.join(self.install_dir, "cfg")

        # All management loggers write to the same service log.
        self.logger = setup_logger(
            'Config',
            SERVICE_LOG_FILE
        )

        # Try to load from taosanode.config.py
        self._load_config(config_path)

    def _load_config(self, config_path: Optional[str] = None):
        """从 taosanode.config.py 加载配置"""
        if config_path is None:
            config_path = os.path.join(self.cfg_dir, "taosanode.config.py")

        # Set attribute defaults so they always exist even if config file is missing
        self.pid_file = PID_FILE
        self.app_log = os.path.join(self.log_dir, "taosanode.app.log")
        self.model_dir = os.path.join(self.data_dir, "model")
        self.venv_dir = os.path.join(self.install_dir, "venvs", "venv")
        self.bind = "0.0.0.0:6035"
        self.workers = 2
        self.timesfm_venv = os.path.join(self.install_dir, "venvs", "timesfm_venv")
        self.moirai_venv = os.path.join(self.install_dir, "venvs", "moirai_venv")
        self.chronos_venv = os.path.join(self.install_dir, "venvs", "chronos_venv")
        self.moment_venv = os.path.join(self.install_dir, "venvs", "momentfm_venv")
        self.models = self._get_default_models()
        self.enabled_models = None

        if not os.path.exists(config_path):
            self.logger.warning(f"Config file not found: {config_path}")
            return

        try:
            # Add cfg dir to path for import
            sys.path.insert(0, self.cfg_dir)

            # Import config module
            import importlib.util
            spec = importlib.util.spec_from_file_location("taosanode_config", config_path)
            config_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(config_module)

            # Extract paths from config
            if hasattr(config_module, 'pidfile'):
                self.pid_file = config_module.pidfile
            else:
                self.pid_file = PID_FILE

            if hasattr(config_module, 'app_log'):
                self.app_log = config_module.app_log
            else:
                self.app_log = os.path.join(self.log_dir, "taosanode.app.log")

            if hasattr(config_module, 'model_dir'):
                self.model_dir = config_module.model_dir
            else:
                self.model_dir = os.path.join(self.data_dir, "model")

            # Get virtualenv path
            if hasattr(config_module, 'virtualenv'):
                self.venv_dir = config_module.virtualenv
            else:
                self.venv_dir = os.path.join(self.install_dir, "venvs", "venv")

            # Bind address
            if hasattr(config_module, 'bind'):
                self.bind = config_module.bind
            else:
                self.bind = "0.0.0.0:6035"

            # Workers
            if hasattr(config_module, 'workers'):
                self.workers = config_module.workers
            else:
                self.workers = 2

            # Model venv paths from config
            self.timesfm_venv = getattr(config_module, 'timesfm_venv',
                                       os.path.join(self.install_dir, "venvs", "timesfm_venv"))
            self.moirai_venv = getattr(config_module, 'moirai_venv',
                                       os.path.join(self.install_dir, "venvs", "moirai_venv"))
            self.chronos_venv = getattr(config_module, 'chronos_venv',
                                        os.path.join(self.install_dir, "venvs", "chronos_venv"))
            self.moment_venv = getattr(config_module, 'momentfm_venv',
                                       os.path.join(self.install_dir, "venvs", "momentfm_venv"))

            # Load model configurations from config file
            if hasattr(config_module, 'models'):
                self.models = config_module.models
            else:
                # Fallback to default models if not in config
                self.models = self._get_default_models()

            enabled_models_file = os.path.join(self.cfg_dir, "enabled_models.txt")
            if os.path.exists(enabled_models_file):
                with open(enabled_models_file, "r", encoding="utf-8") as fh:
                    selected = [line.strip() for line in fh if line.strip()]
                self.enabled_models = [name for name in DEFAULT_MODEL_ORDER if name in selected and name in self.models]
            else:
                self.enabled_models = None

        except Exception as e:
            self.logger.warning(f"Failed to load config: {e}")
            self.pid_file = PID_FILE
            self.app_log = os.path.join(self.log_dir, "taosanode.app.log")
            self.model_dir = os.path.join(self.data_dir, "model")
            self.venv_dir = os.path.join(self.install_dir, "venvs", "venv")
            self.bind = "0.0.0.0:6035"
            self.workers = 2
            self.models = self._get_default_models()
            self.enabled_models = None

    def _get_default_models(self) -> Dict:
        """Get default model configurations"""
        return {
            "tdtsfm": {
                "script": "tdtsfm-server.py",
                "default_model": None,
                "port": 6036,
                "required": True,
            },
            "timemoe": {
                "script": "timemoe-server.py",
                "default_model": "Maple728/TimeMoE-200M",
                "port": 6037,
                "required": True,
            },
            "moirai": {
                "script": "moirai-server.py",
                "default_model": "Salesforce/moirai-moe-1.0-R-small",
                "port": 6039,
                "required": False,
            },
            "chronos": {
                "script": "chronos-server.py",
                "default_model": "amazon/chronos-bolt-base",
                "port": 6038,
                "required": False,
            },
            "timesfm": {
                "script": "timesfm-server.py",
                "default_model": "google/timesfm-2.0-500m-pytorch",
                "port": 6061,
                "required": False,
            },
            "moment": {
                "script": "moment-server.py",
                "default_model": "AutonLab/MOMENT-1-base",
                "port": 6062,
                "required": False,
            },
        }


class ProcessManager:
    """Process lifecycle manager"""

    def __init__(self, config: Config):
        self.config = config
        self.model_pid_dir = os.path.join(self.config.data_dir, "pids")
        self.logger = setup_logger(
            'ProcessManager',
            os.path.join(config.log_dir, 'taosanode-service.log')
        )
        self._ensure_dirs()

    def _ensure_dirs(self):
        """Ensure required directories exist"""
        os.makedirs(self.config.log_dir, exist_ok=True)
        os.makedirs(self.config.data_dir, exist_ok=True)
        os.makedirs(self.model_pid_dir, exist_ok=True)

    def _get_python_exe(self, venv_dir: Optional[str] = None) -> str:
        """Get Python interpreter path"""
        if venv_dir is None:
            venv_dir = self.config.venv_dir

        if IS_WINDOWS:
            python_exe = os.path.join(venv_dir, "Scripts", "python.exe")
        else:
            python_exe = os.path.join(venv_dir, "bin", "python3")

        # Check if venv exists
        if not os.path.exists(python_exe):
            self.logger.error(f"Virtual environment not found: {venv_dir}")
            self.logger.error("Please run installation first or check configuration")
            raise FileNotFoundError(f"Python executable not found: {python_exe}")

        return python_exe

    def _get_pythonw_exe(self, venv_dir: Optional[str] = None) -> str:
        """Get pythonw.exe when available for background Windows processes."""
        python_exe = self._get_python_exe(venv_dir)
        if not IS_WINDOWS:
            return python_exe

        pythonw_exe = os.path.join(os.path.dirname(python_exe), "pythonw.exe")
        if os.path.exists(pythonw_exe):
            return pythonw_exe
        return python_exe

    def _get_pid_file(self, service_name: str = "taosanode") -> str:
        """Get PID file path"""
        if service_name == "taosanode":
            return self.config.pid_file
        else:
            return os.path.join(self.model_pid_dir, f"{service_name}.pid")

    def read_pid(self, service_name: str = "taosanode") -> Optional[int]:
        """Read PID from file"""
        pid_file = self._get_pid_file(service_name)
        try:
            if os.path.exists(pid_file):
                with open(pid_file, 'r') as f:
                    return int(f.read().strip())
        except (ValueError, IOError):
            pass
        return None

    def write_pid(self, pid: int, service_name: str = "taosanode"):
        """Write PID to file"""
        pid_file = self._get_pid_file(service_name)
        os.makedirs(os.path.dirname(pid_file), exist_ok=True)
        with open(pid_file, 'w') as f:
            f.write(str(pid))

    def remove_pid(self, service_name: str = "taosanode"):
        """Remove PID file"""
        pid_file = self._get_pid_file(service_name)
        if os.path.exists(pid_file):
            os.remove(pid_file)

    def _get_windows_process_commandline(self, pid: int) -> str:
        """Return the command line for a Windows process when available."""
        ps_script = (
            f"$p = Get-CimInstance Win32_Process -Filter \\\"ProcessId = {pid}\\\" "
            f"-ErrorAction SilentlyContinue; if ($p) {{ $p.CommandLine }}"
        )
        try:
            result = subprocess.run(
                ["powershell", "-NoProfile", "-Command", ps_script],
                capture_output=True,
                text=True,
                timeout=8,
                check=False,
            )
            if result.returncode == 0:
                return (result.stdout or "").strip()
        except Exception:
            pass
        return ""

    def _matches_expected_process(self, service_name: str, pid: int) -> bool:
        """Best-effort validation that the PID belongs to the expected taosanode/model process."""
        if not IS_WINDOWS:
            return True

        commandline = self._get_windows_process_commandline(pid).lower()
        if not commandline:
            return True

        if service_name == "taosanode":
            markers = [
                "taosanode_service.py",
                "from waitress import serve",
                "taosanalytics.app",
                "gunicorn",
            ]
            return any(marker in commandline for marker in markers)

        if service_name.startswith("model-"):
            model_name = service_name[len("model-"):]
            model_config = self.config.models.get(model_name, {})
            script_name = str(model_config.get("script", "")).lower()
            markers = [script_name, model_name.lower(), "taosanalytics\\tsfmservice", "taosanalytics/tsfmservice"]
            if script_name and script_name in commandline:
                return True
            if any(marker and marker in commandline for marker in markers[1:]) and "python" in commandline:
                return True
            return False

        return True

    def is_running(self, service_name: str = "taosanode") -> bool:
        """Check if service is running"""
        pid = self.read_pid(service_name)
        if pid is None:
            return False

        try:
            if IS_WINDOWS:
                # Windows: use tasklist with precise parsing
                result = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                    capture_output=True, text=True
                )
                # Parse output more precisely to avoid false positives
                # tasklist output format: "imagename.exe    PID    Session Name    Session#    Memory Usage"
                lines = result.stdout.strip().split('\n')
                for line in lines:
                    if line.strip():
                        # Split by whitespace and check if PID matches exactly
                        parts = line.split()
                        if len(parts) >= 2:
                            try:
                                line_pid = int(parts[1])
                                if line_pid == pid:
                                    return self._matches_expected_process(service_name, pid)
                            except ValueError:
                                continue
                return False
            else:
                # Unix: send signal 0
                os.kill(pid, 0)
                return True
        except (OSError, ProcessLookupError):
            return False

    def wait_for_service(self, service_name: str, timeout: int = 10) -> bool:
        """Wait for service to start using polling"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.is_running(service_name):
                return True
            time.sleep(0.5)
        return False

    def kill_process(self, pid: int, force: bool = False):
        """Kill a process by PID"""
        try:
            if IS_WINDOWS:
                if force:
                    subprocess.run(["taskkill", "/F", "/PID", str(pid)],
                                 capture_output=True)
                else:
                    subprocess.run(["taskkill", "/PID", str(pid)],
                                 capture_output=True)
            else:
                sig = signal.SIGKILL if force else signal.SIGTERM
                os.kill(pid, sig)
        except (OSError, ProcessLookupError):
            pass


class TaosanodeService:
    """Taosanode main service manager"""

    def __init__(self, config: Config, process_mgr: ProcessManager):
        self.config = config
        self.process_mgr = process_mgr
        self.logger = setup_logger(
            'TaosanodeService',
            os.path.join(config.log_dir, 'taosanode-service.log')
        )

    def _get_waitress_settings(self) -> tuple:
        """Build the waitress launch settings for Windows."""
        bind_host, bind_port = self.config.bind.split(':')
        wc = getattr(self.config, 'waitress_config', {
            'threads': 4,
            'channel_timeout': 1200,
            'connection_limit': 1000,
            'cleanup_interval': 30,
            'log_socket_errors': True
        })
        return bind_host, int(bind_port), wc

    def _ensure_waitress(self, python_exe: str):
        """Ensure waitress is available in the main virtual environment."""
        try:
            result = subprocess.run(
                [python_exe, "-c", "import waitress"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=True
            )
            if (result.stdout or "").strip():
                self.logger.info(f"Waitress import probe stdout: {(result.stdout or '').strip()}")
            if (result.stderr or "").strip():
                self.logger.warning(f"Waitress import probe stderr: {(result.stderr or '').strip()}")
        except subprocess.CalledProcessError as exc:
            self.logger.info("Installing waitress...")
            if (exc.stdout or "").strip():
                self.logger.warning(f"Waitress import failed stdout: {(exc.stdout or '').strip()}")
            if (exc.stderr or "").strip():
                self.logger.warning(f"Waitress import failed stderr: {(exc.stderr or '').strip()}")
            install_result = subprocess.run(
                [python_exe, "-m", "pip", "install", "waitress"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False
            )
            if (install_result.stdout or "").strip():
                self.logger.info(f"Waitress install stdout: {(install_result.stdout or '').strip()}")
            if (install_result.stderr or "").strip():
                self.logger.warning(f"Waitress install stderr: {(install_result.stderr or '').strip()}")
            if install_result.returncode != 0:
                raise subprocess.CalledProcessError(
                    install_result.returncode,
                    install_result.args,
                    output=install_result.stdout,
                    stderr=install_result.stderr,
                )

    def _build_runtime_env(self) -> Dict[str, str]:
        """Build a consistent runtime environment for import checks and service start."""
        env = os.environ.copy()
        lib_root = os.path.join(self.config.install_dir, "lib")
        existing_pythonpath = env.get("PYTHONPATH", "").strip()
        env["PYTHONPATH"] = (
            lib_root if not existing_pythonpath else lib_root + os.pathsep + existing_pythonpath
        )
        env["PYTHONIOENCODING"] = "utf-8"
        env.setdefault("PYTHONUTF8", "1")
        return env

    @staticmethod
    def _format_exit_code(returncode: int) -> str:
        if returncode < 0:
            return str(returncode)
        return f"{returncode} (0x{returncode & 0xffffffff:08x})"

    def _run_python_probe(self, python_exe: str, env: Dict[str, str], cwd: str, label: str, code: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [python_exe, "-X", "faulthandler", "-c", code],
            env=env,
            cwd=cwd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False
        )

    def _log_probe_result(self, label: str, result: subprocess.CompletedProcess) -> None:
        stdout_text = (result.stdout or "").strip()
        stderr_text = (result.stderr or "").strip()
        if result.returncode == 0:
            self.logger.info(f"Diagnostic probe passed: {label}")
            if stdout_text:
                self.logger.info(f"{label} stdout: {stdout_text}")
            if stderr_text:
                self.logger.warning(f"{label} stderr: {stderr_text}")
            return

        self.logger.error(
            f"Diagnostic probe failed: {label} exited with {self._format_exit_code(result.returncode)}"
        )
        if stdout_text:
            self.logger.warning(f"{label} stdout: {stdout_text}")
        if stderr_text:
            self.logger.warning(f"{label} stderr: {stderr_text}")

    def _collect_preflight_diagnostics(self, python_exe: str, env: Dict[str, str], cwd: str) -> None:
        """Run focused import probes so startup failures identify the exact failing dependency."""
        self.logger.info("Collecting taosanode startup diagnostics...")
        lib_root = os.path.join(self.config.install_dir, "lib").replace("\\", "\\\\")
        discovered_modules = self._discover_runtime_dependencies("taosanalytics.app")
        fallback_modules = ["waitress", "numpy", "pandas", "flask", "torch", "tensorflow", "keras"]
        probe_modules = []
        for module_name in discovered_modules + fallback_modules:
            if module_name not in probe_modules:
                probe_modules.append(module_name)

        self.logger.info(
            "Dependency probes derived from taosanalytics.app: " +
            (", ".join(discovered_modules) if discovered_modules else "none discovered")
        )

        probes = [
            (f"import {module_name}", f"import {module_name}; print('{module_name} ok')")
            for module_name in probe_modules
        ]
        probes.extend([
            (
                "import taosanalytics.conf",
                f"import sys; sys.path.insert(0, r'{lib_root}'); import taosanalytics.conf; print('conf ok')"
            ),
            (
                "import taosanalytics.app",
                f"import sys; sys.path.insert(0, r'{lib_root}'); from taosanalytics.app import app; print('app ok')"
            ),
        ])
        for label, code in probes:
            try:
                result = self._run_python_probe(python_exe, env, cwd, label, code)
            except Exception as exc:
                self.logger.exception(f"Diagnostic probe failed to execute for {label}: {exc}")
                continue
            self._log_probe_result(label, result)

    @staticmethod
    def _env_flag_enabled(value: str) -> bool:
        return value.strip().lower() in {"1", "true", "yes", "on", "full"}

    def _should_use_full_preflight(self, full_preflight: bool = False) -> bool:
        if full_preflight:
            return True
        mode = os.environ.get("TAOSANODE_PREFLIGHT_MODE", "").strip().lower()
        if mode:
            return mode in {"full", "heavy"}
        return self._env_flag_enabled(os.environ.get("TAOSANODE_FULL_PREFLIGHT", ""))

    def _preflight_light_dependencies(self, python_exe: str, env: Dict[str, str], cwd: str) -> bool:
        """Run a lightweight preflight to avoid importing the full app twice on Windows."""
        self.logger.info("Running taosanode lightweight preflight check...")
        lib_root = os.path.join(self.config.install_dir, "lib").replace("\\", "\\\\")
        probes = [
            ("import waitress", "import waitress; print('waitress ok')"),
            ("import flask", "import flask; print('flask ok')"),
            ("import numpy", "import numpy; print('numpy ok')"),
            (
                "import taosanalytics.conf",
                f"import sys; sys.path.insert(0, r'{lib_root}'); import taosanalytics.conf; print('conf ok')"
            ),
        ]

        for label, code in probes:
            try:
                result = self._run_python_probe(python_exe, env, cwd, label, code)
            except Exception as exc:
                self.logger.exception(f"Lightweight preflight probe failed to execute for {label}: {exc}")
                self._collect_preflight_diagnostics(python_exe, env, cwd)
                return False
            self._log_probe_result(label, result)
            if result.returncode != 0:
                self.logger.error(f"Lightweight preflight failed on probe: {label}")
                self._collect_preflight_diagnostics(python_exe, env, cwd)
                return False

        self.logger.info(
            "Taosanode lightweight preflight succeeded. "
            "Use --full-preflight or TAOSANODE_PREFLIGHT_MODE=full for a full app import preflight."
        )
        return True

    def _extract_root_cause(self, stderr_text: str) -> Optional[str]:
        patterns = [
            r"ModuleNotFoundError: (.+)",
            r"ImportError: (.+)",
            r"OSError: (.+)",
            r"WinError \d+[^\\r\\n]*",
            r"Error loading [^\r\n]+",
        ]
        for pattern in patterns:
            match = re.search(pattern, stderr_text, re.MULTILINE)
            if match:
                return match.group(0).strip()
        return None

    def _is_stdlib_module(self, module_name: str) -> bool:
        stdlib_names = getattr(sys, "stdlib_module_names", set())
        return module_name in stdlib_names

    def _resolve_local_module_path(self, module_name: str, package_root: str) -> Optional[str]:
        if not module_name.startswith("taosanalytics"):
            return None
        relative_parts = module_name.split(".")[1:]
        base_path = os.path.join(package_root, *relative_parts) if relative_parts else package_root
        file_candidate = base_path + ".py"
        init_candidate = os.path.join(base_path, "__init__.py")
        if os.path.isfile(file_candidate):
            return file_candidate
        if os.path.isfile(init_candidate):
            return init_candidate
        return None

    def _resolve_relative_module_name(self, current_module: str, level: int, module_name: str) -> Optional[str]:
        parts = current_module.split(".")
        if level > len(parts):
            return None
        prefix_parts = parts[:-level]
        if module_name:
            prefix_parts.extend(module_name.split("."))
        return ".".join(part for part in prefix_parts if part)

    def _discover_runtime_dependencies(self, entry_module: str = "taosanalytics.app") -> List[str]:
        package_root = os.path.join(self.config.install_dir, "lib", "taosanalytics")
        entry_path = self._resolve_local_module_path(entry_module, package_root)
        if not entry_path:
            return []

        stdlib_exclusions = {"__future__"}
        external_modules = set()
        visited_modules = set()

        def visit(module_name: str):
            if module_name in visited_modules:
                return
            visited_modules.add(module_name)
            module_path = self._resolve_local_module_path(module_name, package_root)
            if not module_path or not os.path.isfile(module_path):
                return
            try:
                with open(module_path, "r", encoding="utf-8", errors="replace") as fh:
                    tree = ast.parse(fh.read(), filename=module_path)
            except Exception as exc:
                self.logger.warning(f"Unable to analyze imports for {module_name}: {exc}")
                return

            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imported = alias.name
                        top_level = imported.split(".")[0]
                        if top_level == "taosanalytics":
                            visit(imported)
                        elif top_level not in stdlib_exclusions and not self._is_stdlib_module(top_level):
                            external_modules.add(top_level)
                elif isinstance(node, ast.ImportFrom):
                    if node.level > 0:
                        resolved = self._resolve_relative_module_name(module_name, node.level, node.module or "")
                    else:
                        resolved = node.module or ""
                    if not resolved:
                        continue
                    top_level = resolved.split(".")[0]
                    if top_level == "taosanalytics":
                        visit(resolved)
                    elif top_level not in stdlib_exclusions and not self._is_stdlib_module(top_level):
                        external_modules.add(top_level)

        visit(entry_module)
        return sorted(external_modules)

    def _preflight_app_import(self, python_exe: str, env: Dict[str, str], cwd: str) -> bool:
        """Validate that taosanalytics.app can be imported before starting the service."""
        self.logger.info("Running taosanode import preflight check...")
        check_script = (
            "from taosanalytics.app import app; "
            "print('taosanalytics.app import ok')"
        )

        try:
            result = subprocess.run(
                [python_exe, "-c", check_script],
                env=env,
                cwd=cwd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False
            )
        except Exception as exc:
            self.logger.exception(f"Taosanode import preflight failed to execute: {exc}")
            return False

        stdout_text = (result.stdout or "").strip()
        stderr_text = (result.stderr or "").strip()
        if stdout_text:
            self.logger.info(f"Preflight stdout: {stdout_text}")
        if stderr_text:
            self.logger.warning(f"Preflight stderr: {stderr_text}")

        if result.returncode != 0:
            self.logger.error(
                f"Taosanode import preflight failed with exit code {self._format_exit_code(result.returncode)}"
            )
            root_cause = self._extract_root_cause(stderr_text)
            if root_cause:
                self.logger.error(f"Preflight root cause: {root_cause}")
            self._collect_preflight_diagnostics(python_exe, env, cwd)
            return False

        self.logger.info("Taosanode import preflight succeeded")
        return True

    def _run_preflight(self, python_exe: str, env: Dict[str, str], cwd: str, full_preflight: bool = False) -> bool:
        if self._should_use_full_preflight(full_preflight):
            self.logger.info("Selected preflight mode: full")
            return self._preflight_app_import(python_exe, env, cwd)
        self.logger.info("Selected preflight mode: light")
        return self._preflight_light_dependencies(python_exe, env, cwd)

    def _run_windows_foreground(self, full_preflight: bool = False) -> bool:
        """Run taosanode in the current process for WinSW-managed service mode."""
        redirect_stdio_if_needed()
        python_exe = self.process_mgr._get_python_exe()
        self._ensure_waitress(python_exe)

        lib_root = os.path.join(self.config.install_dir, "lib")
        lib_dir = os.path.join(self.config.install_dir, "lib", "taosanalytics")
        env = self._build_runtime_env()
        if not self._run_preflight(python_exe, env, lib_dir, full_preflight=full_preflight):
            return False

        if lib_root not in sys.path:
            sys.path.insert(0, lib_root)
        os.environ.update(env)

        bind_host, bind_port, wc = self._get_waitress_settings()

        try:
            from waitress import serve
            from taosanalytics.app import app
        except Exception as e:
            self._collect_preflight_diagnostics(python_exe, env, lib_dir)
            self.logger.exception(f"Error starting taosanode in foreground: {e}")
            return False

        try:
            pid = os.getpid()
            self.process_mgr.write_pid(pid, "taosanode")
            self.logger.info(f"Taosanode started in foreground with PID {pid}")
            self.logger.info(f"Listening on {self.config.bind}")

            serve(
                app,
                host=bind_host,
                port=bind_port,
                threads=wc.get('threads', 4),
                channel_timeout=wc.get('channel_timeout', 1200),
                connection_limit=wc.get('connection_limit', 1000),
                cleanup_interval=wc.get('cleanup_interval', 30),
                log_socket_errors=wc.get('log_socket_errors', True),
            )
            return True
        except Exception as e:
            self.logger.exception(f"Error while serving taosanode in foreground: {e}")
            return False
        finally:
            self.process_mgr.remove_pid("taosanode")

    def start(self, foreground: bool = False, full_preflight: bool = False) -> bool:
        """Start taosanode service"""
        if self.process_mgr.is_running("taosanode"):
            self.logger.info("Taosanode is already running")
            return True

        self.logger.info("Starting taosanode service...")

        try:
            python_exe = self.process_mgr._get_python_exe()
            lib_dir = os.path.join(self.config.install_dir, "lib", "taosanalytics")
            env = self._build_runtime_env()

            if IS_WINDOWS and foreground:
                return self._run_windows_foreground(full_preflight=full_preflight)

            if IS_WINDOWS and not self._run_preflight(python_exe, env, lib_dir, full_preflight=full_preflight):
                self.logger.error("Taosanode startup aborted because the import preflight failed")
                return False

            if IS_WINDOWS:
                self._ensure_waitress(python_exe)
                bind_host, bind_port, wc = self._get_waitress_settings()

                cmd = [
                    python_exe, "-c",
                    f"from waitress import serve; from taosanalytics.app import app; "
                    f"serve(app, "
                    f"host='{bind_host}', "
                    f"port={bind_port}, "
                    f"threads={wc.get('threads', 4)}, "
                    f"channel_timeout={wc.get('channel_timeout', 1200)}, "
                    f"connection_limit={wc.get('connection_limit', 1000)}, "
                    f"cleanup_interval={wc.get('cleanup_interval', 30)}, "
                    f"log_socket_errors={wc.get('log_socket_errors', True)})"
                ]
            else:
                # Linux: use gunicorn
                cmd = [
                    python_exe, "-m", "gunicorn",
                    "-c", os.path.join(self.config.cfg_dir, "taosanode.config.py"),
                    "-D",  # Daemon mode
                ]

            if IS_WINDOWS:
                # Windows: redirect output to log file for debugging
                log_file = os.path.join(self.config.log_dir, "taosanode-service.log")
                with open(log_file, 'a') as log:
                    proc = subprocess.Popen(
                        cmd,
                        env=env,
                        cwd=lib_dir,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                    )
            else:
                # Linux: daemon mode handled by gunicorn
                proc = subprocess.Popen(
                    cmd,
                    env=env,
                    cwd=lib_dir,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )

            # Write PID immediately so is_running() can find the process
            self.process_mgr.write_pid(proc.pid, "taosanode")

            # Wait for service to start using polling (process existence check)
            if self.process_mgr.wait_for_service("taosanode", timeout=30):
                pid = self.process_mgr.read_pid("taosanode")
                self.logger.info(f"Taosanode started with PID {pid}")
                self.logger.info(f"Listening on {self.config.bind}")
                return True
            else:
                exit_code = proc.poll()
                if exit_code is None:
                    self.logger.error("Failed to start taosanode - service did not start within timeout")
                else:
                    self.logger.error(
                        f"Failed to start taosanode - child process exited early with code {exit_code}"
                    )
                    if IS_WINDOWS:
                        self._collect_preflight_diagnostics(python_exe, env, lib_dir)
                self.process_mgr.remove_pid("taosanode")
                return False

        except Exception as e:
            self.process_mgr.remove_pid("taosanode")
            self.logger.exception(f"Error starting taosanode: {e}")
            return False

    def stop(self) -> bool:
        """Stop taosanode service"""
        if not self.process_mgr.is_running("taosanode"):
            self.logger.info("Taosanode is not running")
            self.process_mgr.remove_pid("taosanode")
            return True

        self.logger.info("Stopping taosanode service...")
        pid = self.process_mgr.read_pid("taosanode")

        if pid:
            # Try graceful shutdown first
            self.process_mgr.kill_process(pid, force=False)
            time.sleep(2)

            # Check if still running
            if self.process_mgr.is_running("taosanode"):
                self.logger.info("Force killing taosanode...")
                self.process_mgr.kill_process(pid, force=True)

        self.process_mgr.remove_pid("taosanode")
        self.logger.info("Taosanode stopped")
        return True

    def status(self) -> Dict:
        """Get service status"""
        is_running = self.process_mgr.is_running("taosanode")
        pid = self.process_mgr.read_pid("taosanode")
        if pid and not is_running:
            self.process_mgr.remove_pid("taosanode")
            pid = None

        return {
            "service": "taosanode",
            "running": is_running,
            "pid": pid,
            "bind": self.config.bind,
        }

    def wait_ready(self, timeout: int = 30, interval: float = 1.0) -> bool:
        """Wait until the taosanode HTTP status endpoint is reachable."""
        bind_host, bind_port, _ = self._get_waitress_settings()
        probe_host = "127.0.0.1" if bind_host in {"0.0.0.0", "::", "*", ""} else bind_host
        status_url = f"http://{probe_host}:{bind_port}/status"

        self.logger.info(f"Waiting for taosanode readiness on {status_url}")
        deadline = time.time() + max(1, timeout)
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(status_url, timeout=3) as response:
                    body = response.read().decode("utf-8", errors="replace")
                    if response.status == 200 and "ready" in body.lower():
                        self.logger.info(f"Taosanode readiness check succeeded: {status_url}")
                        return True
            except Exception:
                pass

            if not self.process_mgr.is_running("taosanode"):
                self.logger.warning("Taosanode process is not running while waiting for readiness")
                return False
            time.sleep(max(0.2, interval))

        self.logger.warning(f"Taosanode readiness check timed out after {timeout} seconds: {status_url}")
        return False

    def install_service(self) -> bool:
        """Install Windows service"""
        if not IS_WINDOWS:
            self.logger.error("Service installation is only supported on Windows")
            return False

        service_exe = os.path.join(self.config.install_dir, "bin", WINSW_EXE_NAME)
        if not os.path.exists(service_exe):
            self.logger.error("ERROR: Service wrapper not found. Please reinstall.")
            return False

        try:
            query = subprocess.run(
                ["sc", "query", "Taosanode"],
                capture_output=True,
                text=True,
                check=False,
            )
            if query.returncode == 0:
                subprocess.run([service_exe, "stop"], capture_output=True, cwd=self.config.install_dir)
                self.logger.info("Service already installed; keeping existing registration.")
                self.logger.info("Updated XML settings will be picked up on the next service start.")
                return True

            subprocess.run([service_exe, "install"], check=True, cwd=self.config.install_dir)
            self.logger.info("Service installed successfully")
            self.logger.info("To start the service: net start Taosanode")
            self.logger.info("Or use: taosanode_service.py start-service")
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"ERROR: Failed to install service: {e}")
            return False

    def uninstall_service(self) -> bool:
        """Uninstall Windows service"""
        if not IS_WINDOWS:
            self.logger.error("Service uninstallation is only supported on Windows")
            return False

        service_exe = os.path.join(self.config.install_dir, "bin", WINSW_EXE_NAME)
        if not os.path.exists(service_exe):
            self.logger.warning("Service wrapper not found")
            return True

        try:
            # Stop service first
            subprocess.run([service_exe, "stop"], capture_output=True)
            time.sleep(2)

            # Uninstall service
            subprocess.run([service_exe, "uninstall"], check=True, cwd=self.config.install_dir)
            self.logger.info("Service uninstalled successfully")
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"ERROR: Failed to uninstall service: {e}")
            return False

    def start_service(self) -> bool:
        """Start Windows service"""
        if not IS_WINDOWS:
            self.logger.error("Service management is only supported on Windows")
            return False

        try:
            subprocess.run(["net", "start", "Taosanode"], check=True)
            self.logger.info("Service started successfully")
            return True
        except subprocess.CalledProcessError:
            self.logger.error("ERROR: Failed to start service")
            return False

    def stop_service(self) -> bool:
        """Stop Windows service"""
        if not IS_WINDOWS:
            self.logger.error("Service management is only supported on Windows")
            return False

        try:
            subprocess.run(["net", "stop", "Taosanode"], check=True)
            self.logger.info("Service stopped successfully")
            return True
        except subprocess.CalledProcessError:
            self.logger.error("ERROR: Failed to stop service")
            return False


class ModelService:
    """Model service manager"""

    def __init__(self, config: Config, process_mgr: ProcessManager):
        self.config = config
        self.process_mgr = process_mgr
        self.logger = setup_logger(
            'ModelService',
            os.path.join(config.log_dir, 'taosanode-service.log')
        )

    def _get_model_venv(self, model_name: str) -> str:
        """Get virtual environment path for a model"""
        if model_name == "timesfm":
            return self.config.timesfm_venv
        elif model_name == "moirai":
            return self.config.moirai_venv
        elif model_name == "chronos":
            return self.config.chronos_venv
        elif model_name == "moment":
            return self.config.moment_venv
        else:
            return self.config.venv_dir

    def _ordered_model_names(self) -> List[str]:
        return [name for name in DEFAULT_MODEL_ORDER if name in self.config.models]

    def _build_model_args(self, model_name: str) -> List[str]:
        """构建模型启动参数"""
        model_config = self.config.models.get(model_name)
        if not model_config:
            raise ValueError(f"Unknown model: {model_name}")

        model_dir = os.path.join(self.config.model_dir, model_name)
        args = []

        if model_name == "tdtsfm":
            args = [model_dir, "--action", "server"]
        elif model_config.get("default_model"):
            args = [model_dir, model_config["default_model"], "False"]

        return args

    def start(self, model_name: str) -> bool:
        """Start model service"""
        if model_name == "all":
            return self._start_all()

        if model_name not in self.config.models:
            self.logger.error(f"Unknown model: {model_name}")
            self.logger.error(f"Supported models: {', '.join(self.config.models.keys())}")
            return False

        if self.process_mgr.is_running(f"model-{model_name}"):
            self.logger.info(f"Model {model_name} is already running")
            return True

        model_dir = os.path.join(self.config.model_dir, model_name)
        if not os.path.exists(model_dir):
            self.logger.info(f"Skipping model {model_name} - directory not found: {model_dir}")
            return True

        self.logger.info(f"Starting model service: {model_name}")

        # Get configuration
        venv_dir = self._get_model_venv(model_name)
        python_exe = self.process_mgr._get_pythonw_exe(venv_dir)
        model_config = self.config.models[model_name]

        # Build command
        service_dir = os.path.join(
            self.config.install_dir, "lib", "taosanalytics", "tsfmservice"
        )
        script_path = os.path.join(service_dir, model_config["script"])

        if not os.path.exists(script_path):
            self.logger.error(f"Script not found: {script_path}")
            return False

        args = self._build_model_args(model_name)
        cmd = [python_exe, script_path] + args

        # Log file with rotation
        log_file = os.path.join(
            self.config.log_dir, f"model_{model_name}.log"
        )

        # Setup rotating file handler for logging
        logger = logging.getLogger(f"model-{model_name}")
        logger.setLevel(logging.DEBUG)

        # Remove existing handlers to avoid duplicates
        logger.handlers.clear()

        # Create rotating file handler (10MB per file, keep 5 backups)
        handler = RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding=get_windows_friendly_log_encoding(log_file),
            errors="replace",
        )
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        try:
            # Start process
            with open(
                log_file,
                'a',
                encoding=get_windows_friendly_log_encoding(log_file),
                errors="replace",
            ) as log:
                if IS_WINDOWS:
                    startupinfo = subprocess.STARTUPINFO()
                    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                    startupinfo.wShowWindow = 0
                    creation_flags = (
                        getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) |
                        getattr(subprocess, "CREATE_NO_WINDOW", 0)
                    )
                    proc = subprocess.Popen(
                        cmd,
                        cwd=service_dir,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        creationflags=creation_flags,
                        startupinfo=startupinfo,
                    )
                else:
                    proc = subprocess.Popen(
                        cmd,
                        cwd=service_dir,
                        stdout=log,
                        stderr=subprocess.STDOUT
                    )

                # Write initial PID immediately after process creation
                service_name = f"model-{model_name}"
                self.process_mgr.write_pid(proc.pid, service_name)

            # Wait and check using polling (timeout 15 seconds for model loading)
            if self.process_mgr.wait_for_service(service_name, timeout=15):
                pid = self.process_mgr.read_pid(service_name)
                self.logger.info(f"Model {model_name} started with PID {pid}")
                return True
            else:
                self.logger.error(f"Failed to start model {model_name} - service did not start within timeout")
                return False

        except Exception as e:
            self.logger.error(f"Error starting model {model_name}: {e}")
            return False

    def _start_all(self) -> bool:
        """Start all models concurrently and skip ones without model directories."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        self.logger.info("Starting all models...")
        results = {}
        started_models = []
        failed_models = []
        skipped_models = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_model = {
                executor.submit(self.start, model_name): model_name
                for model_name in self._ordered_model_names()
            }

            for future in as_completed(future_to_model):
                model_name = future_to_model[future]
                try:
                    result = future.result()
                    results[model_name] = result

                    model_dir = os.path.join(self.config.model_dir, model_name)
                    if not os.path.exists(model_dir):
                        skipped_models.append(model_name)
                    elif result:
                        started_models.append(model_name)
                    else:
                        failed_models.append(model_name)
                except Exception as e:
                    self.logger.error(f"Error starting model {model_name}: {e}")
                    results[model_name] = False
                    failed_models.append(model_name)

        started = len(started_models)
        total = len(results)
        summary_lines = [
            "=" * 50,
            "Model Startup Summary:",
            "=" * 50,
        ]

        if skipped_models:
            summary_lines.append(f"✓ Skipped (optional): {', '.join(skipped_models)}")
        if failed_models:
            summary_lines.append(f"✗ Failed: {', '.join(failed_models)}")

        summary_lines.append(f"✓ Started: {started}/{total - len(skipped_models)} models")
        summary_lines.append("=" * 50)

        if started_models:
            summary_lines.append(f"Started: {', '.join(started_models)}")
        else:
            summary_lines.append("Started: none")
        if skipped_models:
            summary_lines.append(f"Skipped (missing model directory): {', '.join(skipped_models)}")
        if failed_models:
            summary_lines.append(f"Failed: {', '.join(failed_models)}")
        summary_lines.append(f"Started: {started}/{max(0, total - len(skipped_models))} models")
        summary_lines.append("=" * 50)

        summary_lines = [
            "=" * 50,
            "Model Startup Summary:",
            "=" * 50,
        ]
        if started_models:
            summary_lines.append(f"Started: {', '.join(started_models)}")
        else:
            summary_lines.append("Started: none")
        if skipped_models:
            summary_lines.append(f"Skipped (missing model directory): {', '.join(skipped_models)}")
        if failed_models:
            summary_lines.append(f"Failed: {', '.join(failed_models)}")
        summary_lines.append(
            f"Summary: {len(started_models)} started, {len(skipped_models)} skipped, {len(failed_models)} failed"
        )
        summary_lines.append("=" * 50)

        for line in summary_lines:
            self.logger.info(line)

        return len(failed_models) == 0

    def stop(self, model_name: str) -> bool:
        """Stop model service"""
        if model_name == "all":
            return self._stop_all()

        if model_name not in self.config.models:
            self.logger.error(f"Unknown model: {model_name}")
            return False

        service_name = f"model-{model_name}"

        if not self.process_mgr.is_running(service_name):
            self.logger.info(f"Model {model_name} is not running")
            self.process_mgr.remove_pid(service_name)
            return True

        self.logger.info(f"Stopping model service: {model_name}")
        pid = self.process_mgr.read_pid(service_name)

        if pid:
            # Try graceful shutdown first
            self.process_mgr.kill_process(pid, force=False)
            time.sleep(3)

            # Check if still running
            if self.process_mgr.is_running(service_name):
                self.logger.info(f"Force killing model {model_name}...")
                self.process_mgr.kill_process(pid, force=True)

        self.process_mgr.remove_pid(service_name)
        self.logger.info(f"Model {model_name} stopped")
        return True

    def _stop_all(self) -> bool:
        """Stop all models concurrently"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        self.logger.info("Stopping all models...")
        results = {}
        running_before = {
            model_name: self.process_mgr.is_running(f"model-{model_name}")
            for model_name in self._ordered_model_names()
        }
        stopped_models = []
        already_stopped_models = []
        failed_models = []

        # Use ThreadPoolExecutor for concurrent shutdown
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Submit all model stop tasks
            future_to_model = {
                executor.submit(self.stop, model_name): model_name
                for model_name in self._ordered_model_names()
            }

            # Collect results as they complete
            for future in as_completed(future_to_model):
                model_name = future_to_model[future]
                try:
                    result = future.result()
                    results[model_name] = result
                    if result:
                        if running_before.get(model_name, False):
                            stopped_models.append(model_name)
                        else:
                            already_stopped_models.append(model_name)
                    else:
                        failed_models.append(model_name)
                except Exception as e:
                    self.logger.error(f"Error stopping model {model_name}: {e}")
                    results[model_name] = False
                    failed_models.append(model_name)

        summary_lines = [
            "=" * 50,
            "Model Stop Summary:",
            "=" * 50,
        ]
        if stopped_models:
            summary_lines.append(f"Stopped: {', '.join(stopped_models)}")
        else:
            summary_lines.append("Stopped: none")
        if already_stopped_models:
            summary_lines.append(f"Already stopped: {', '.join(already_stopped_models)}")
        if failed_models:
            summary_lines.append(f"Failed: {', '.join(failed_models)}")
        summary_lines.append(
            f"Summary: {len(stopped_models)} stopped, {len(already_stopped_models)} already stopped, {len(failed_models)} failed"
        )
        summary_lines.append("=" * 50)
        for line in summary_lines:
            self.logger.info(line)
        return all(results.values())

    def status(self) -> List[Dict]:
        """Get all model statuses"""
        statuses = []
        for model_name in self._ordered_model_names():
            service_name = f"model-{model_name}"
            is_running = self.process_mgr.is_running(service_name)
            pid = self.process_mgr.read_pid(service_name)
            if pid and not is_running:
                self.process_mgr.remove_pid(service_name)
                pid = None
            model_config = self.config.models.get(model_name, {})
            port = model_config.get("port", 0)

            statuses.append({
                "model": model_name,
                "running": is_running,
                "pid": pid,
                "port": port if port else None,
            })

        return statuses


def print_status(status):
    """Print service status"""
    if isinstance(status, list):
        print("-" * 50)
        print(f"{'Model':<15} {'Status':<10} {'PID':<10} {'Port':<8}")
        print("-" * 50)
        for s in status:
            state = "Running" if s["running"] else "Stopped"
            pid = str(s["pid"]) if s["pid"] else "-"
            port = str(s.get("port")) if s.get("port") else "-"
            print(f"{s['model']:<15} {state:<10} {pid:<10} {port:<8}")
        print("-" * 50)
    else:
        print("-" * 50)
        print(f"Service: {status['service']}")
        print(f"Status: {'Running' if status['running'] else 'Stopped'}")
        if status['pid']:
            print(f"PID: {status['pid']}")
        print(f"Bind: {status['bind']}")
        print("-" * 50)


def main():
    parser = argparse.ArgumentParser(
        description="TDgpt Taosanode Service Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    %(prog)s start                    # Start taosanode
    %(prog)s stop                     # Stop taosanode
    %(prog)s status                   # Show status
    %(prog)s model-start tdtsfm       # Start tdtsfm model
    %(prog)s model-start all          # Start all models
    %(prog)s model-stop all           # Stop all models
    %(prog)s wait-ready               # Wait until taosanode /status reports ready
    %(prog)s install-service          # Install Windows service
    %(prog)s uninstall-service        # Uninstall Windows service
    %(prog)s start-service            # Start Windows service
    %(prog)s stop-service             # Stop Windows service
        """
    )

    parser.add_argument(
        "command",
        choices=["start", "stop", "status", "model-start", "model-stop", "model-status", "wait-ready",
                 "install-service", "uninstall-service", "start-service", "stop-service"],
        help="Command to execute"
    )
    parser.add_argument(
        "target",
        nargs="?",
        help="Target for model commands (model name or 'all')"
    )
    parser.add_argument(
        "-c", "--config",
        help="Path to taosanode.config.py"
    )
    parser.add_argument(
        "--foreground",
        action="store_true",
        help="Run in foreground (for service mode)"
    )
    parser.add_argument(
        "--full-preflight",
        action="store_true",
        help="Run the full taosanalytics.app import preflight before startup"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout in seconds for wait-ready"
    )

    args = parser.parse_args()

    # Initialize
    config = Config(args.config)
    process_mgr = ProcessManager(config)
    taosanode = TaosanodeService(config, process_mgr)
    models = ModelService(config, process_mgr)
    logger = get_logger()

    # Execute command
    if args.command == "start":
        success = taosanode.start(foreground=args.foreground, full_preflight=args.full_preflight)
        sys.exit(0 if success else 1)

    elif args.command == "stop":
        success = taosanode.stop()
        sys.exit(0 if success else 1)

    elif args.command == "status":
        status = taosanode.status()
        print_status(status)

    elif args.command == "model-start":
        if not args.target:
            logger.error("model-start requires a target (model name or 'all')")
            logger.error(f"Supported models: {', '.join(config.models.keys())}")
            sys.exit(1)
        success = models.start(args.target)
        sys.exit(0 if success else 1)

    elif args.command == "model-stop":
        if not args.target:
            logger.error("model-stop requires a target (model name or 'all')")
            sys.exit(1)
        success = models.stop(args.target)
        sys.exit(0 if success else 1)

    elif args.command == "model-status":
        status = models.status()
        print_status(status)

    elif args.command == "wait-ready":
        success = taosanode.wait_ready(timeout=args.timeout)
        sys.exit(0 if success else 1)

    elif args.command == "install-service":
        success = taosanode.install_service()
        sys.exit(0 if success else 1)

    elif args.command == "uninstall-service":
        success = taosanode.uninstall_service()
        sys.exit(0 if success else 1)

    elif args.command == "start-service":
        success = taosanode.start_service()
        sys.exit(0 if success else 1)

    elif args.command == "stop-service":
        success = taosanode.stop_service()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
