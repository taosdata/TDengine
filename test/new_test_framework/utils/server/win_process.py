# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import os
import ntpath
import subprocess
import logging
import signal
import sys
import psutil
from time import sleep
import time

logger = logging.getLogger(__name__)

# PID -> last CTRL_BREAK send timestamp (monotonic seconds)
_LAST_CTRL_BREAK_TS = {}


def _resolve_stop_timeout(default_timeout):
    raw = os.environ.get("TAOSD_STOP_TIMEOUT_SEC")
    if not raw:
        return default_timeout
    try:
        val = float(raw)
        if val <= 0:
            return default_timeout
        # avoid accidental extreme values
        return max(1.0, min(val, 300.0))
    except Exception:
        return default_timeout


def _send_ctrl_break_event(pid):
    """Send CTRL_BREAK inside the private console created for this taosd."""
    # os.kill can terminate the target when GenerateConsoleCtrlEvent fails.
    # Call the WinAPI directly and check every operation instead.
    script = (
        "import ctypes,os,sys,time\n"
        "k=ctypes.WinDLL('kernel32',use_last_error=True)\n"
        "k.FreeConsole()\n"
        "if not k.AttachConsole(int(sys.argv[1])):\n"
        "    raise ctypes.WinError(ctypes.get_last_error())\n"
        "processes=(ctypes.c_ulong*256)()\n"
        "count=k.GetConsoleProcessList(processes,len(processes))\n"
        "if not count or count>len(processes):\n"
        "    raise RuntimeError('Cannot verify target console ownership')\n"
        "if set(processes[:count]) != {int(sys.argv[1]),os.getpid()}:\n"
        "    raise RuntimeError('Refusing CTRL_BREAK on a shared console')\n"
        "handler=ctypes.WINFUNCTYPE(ctypes.c_int,ctypes.c_ulong)(lambda event: True)\n"
        "if not k.SetConsoleCtrlHandler(handler,True):\n"
        "    raise ctypes.WinError(ctypes.get_last_error())\n"
        "if not k.GenerateConsoleCtrlEvent(1,0):\n"
        "    raise ctypes.WinError(ctypes.get_last_error())\n"
        "time.sleep(0.2)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(pid)],
        creationflags=subprocess.CREATE_NEW_CONSOLE,
        capture_output=True, text=True, errors='replace', timeout=5,
    )
    if result.returncode:
        raise RuntimeError(f'Cannot signal taosd process {pid}: {result.stderr}')


def start_taosd_windows(taosd_path, config_dir, log=None):
    """Give taosd its own console so stop signals cannot reach other tests."""
    _log = log or logger
    startup = subprocess.STARTUPINFO()
    startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startup.wShowWindow = subprocess.SW_HIDE
    # No caller drains console pipes. Keep diagnostics without blocking startup
    # when the default configuration dump fills the Windows pipe buffer.
    console_path = os.path.join(config_dir, "taosd-console.log")
    with open(console_path, "ab") as console:
        proc = subprocess.Popen(
            [taosd_path, "-c", config_dir],
            creationflags=subprocess.CREATE_NEW_CONSOLE, startupinfo=startup,
            stdout=console, stderr=subprocess.STDOUT,
        )
    _log.info(f"taosd started with PID: {proc.pid}")
    return proc


def stop_taosd_windows(dnode_index=None, config_dir=None, timeout=30, log=None):
    """Windows 平台通过 CTRL_BREAK_EVENT 优雅停止指定的 taosd

    Args:
        dnode_index: dnode 编号，用于在 cmdline 中匹配 dnode{index}
        config_dir: config 目录路径，用于在 cmdline 中匹配
        timeout: 等待优雅退出的超时时间（秒）
        log: 可选的 logger 实例

    两种匹配方式至少传一个。优先使用 config_dir 精确匹配。

    Returns:
        True 表示优雅退出，False 表示未找到进程或超时强杀
    """
    _log = log or logger
    timeout = _resolve_stop_timeout(timeout)
    try:
        pid = _find_taosd_pid(dnode_index=dnode_index, config_dir=config_dir)

        if not pid:
            match_info = config_dir or f"dnode{dnode_index}"
            _log.info(f"No taosd process found for {match_info}")
            return False

        now = time.monotonic()
        last_sent = _LAST_CTRL_BREAK_TS.get(pid)
        if last_sent is None or (now - last_sent) > 2.0:
            _log.info(f"Sending CTRL_BREAK_EVENT to taosd process (PID: {pid})")
            _send_ctrl_break_event(pid)
            _LAST_CTRL_BREAK_TS[pid] = now
        else:
            _log.info(
                f"CTRL_BREAK_EVENT already sent recently for taosd process (PID: {pid}), skip duplicate signal"
            )

        waited = 0
        interval = 0.5
        while waited < timeout:
            if not psutil.pid_exists(pid):
                _log.info(f"taosd process {pid} has exited gracefully")
                return True
            sleep(interval)
            waited += interval

        _log.info(f"taosd process {pid} did not exit in {timeout}s, force killing...")
        try:
            psutil.Process(pid).kill()
        except Exception:
            # fallback
            os.kill(pid, signal.SIGTERM)
        return False

    except Exception as e:
        _log.error(f"Error stopping taosd: {e}")
        return False


def force_stop_taosd_windows(config_dir, timeout=30, log=None):
    """Kill only taosd instances using this configuration and await their exit."""
    if not config_dir:
        raise ValueError("A config directory is required to force stop taosd")
    _log = log or logger
    target_config = ntpath.normcase(ntpath.normpath(config_dir))
    targets = []
    for proc in psutil.process_iter(["pid", "name"]):
        try:
            if (proc.info["name"] or "").lower() not in {"taosd", "taosd.exe"}:
                continue
            arguments = proc.cmdline()
            for index, argument in enumerate(arguments):
                if argument == "-c" and index + 1 < len(arguments):
                    config = arguments[index + 1]
                elif argument.startswith("-c") and len(argument) > 2:
                    config = argument[2:]
                else:
                    continue
                config = ntpath.normcase(ntpath.normpath(config.strip('"')))
                if config == target_config:
                    targets.append(proc)
                    break
        except psutil.NoSuchProcess:
            continue
        except psutil.AccessDenied as error:
            raise RuntimeError(f"Cannot inspect taosd process {proc.pid}") from error

    for proc in targets:
        try:
            _log.info(f"Force killing taosd process {proc.pid} for {config_dir}")
            proc.kill()
        except psutil.NoSuchProcess:
            continue
        except psutil.AccessDenied as error:
            raise RuntimeError(f"Cannot force kill taosd process {proc.pid}") from error

    _, alive = psutil.wait_procs(targets, timeout=timeout)
    if alive:
        raise RuntimeError(
            f"Force stop failed for {config_dir}: processes "
            f"{[proc.pid for proc in alive]} still running after {timeout}s"
        )
    _log.info(f"Force stop confirmed for {config_dir}: {len(targets)} process(es) exited")
    return len(targets)


def _find_taosd_pid(dnode_index=None, config_dir=None):
    """查找指定 taosd 进程 PID

    支持两种匹配方式：
    - config_dir: 在 cmdline 中匹配 config 目录路径（精确）
    - dnode_index: 在 cmdline 中匹配 dnode{index}（兼容）
    """
    match_key = ntpath.normcase(ntpath.normpath(config_dir.strip('"'))) if config_dir else None
    # 先只按 name 快速过滤，避免遍历所有进程的 cmdline（Windows 上获取 cmdline 很慢）
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if (proc.info['name'] or '').lower() not in {'taosd', 'taosd.exe'}:
                continue
            # 只对 taosd 进程获取 cmdline
            cmdline = proc.cmdline()
            if not cmdline:
                continue
            for index, argument in enumerate(cmdline):
                if argument == '-c' and index + 1 < len(cmdline):
                    config = cmdline[index + 1]
                elif argument.startswith('-c') and len(argument) > 2:
                    config = argument[2:]
                else:
                    continue
                config = ntpath.normcase(ntpath.normpath(config.strip('"')))
                if match_key is not None:
                    if config == match_key:
                        return proc.info['pid']
                elif dnode_index is not None and f'dnode{dnode_index}' in config.split('\\'):
                    return proc.info['pid']
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None
