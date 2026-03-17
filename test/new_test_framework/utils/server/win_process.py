import os
import subprocess
import logging
import psutil
from time import sleep

try:
    import signal
    HAS_SIGNAL = True
except ImportError:
    HAS_SIGNAL = False

logger = logging.getLogger(__name__)


def _send_ctrl_break_event(pid):
    """通过独立子进程发送 CTRL_BREAK_EVENT，避免信号泄漏到当前 Python 进程"""
    # 在独立 console 中执行，这样 CTRL_BREAK_EVENT 不会影响当前进程
    script = f"import os, signal; os.kill({pid}, signal.CTRL_BREAK_EVENT)"
    p = subprocess.Popen(
        ["python", "-c", script],
        creationflags=subprocess.CREATE_NEW_CONSOLE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    p.wait(timeout=5)


def start_taosd_windows(taosd_path, config_dir, log=None):
    """Windows 平台启动 taosd，使用 CREATE_NEW_PROCESS_GROUP 以支持优雅停止"""
    _log = log or logger
    proc = subprocess.Popen(
        [taosd_path, "-c", config_dir],
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
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
    try:
        pid = _find_taosd_pid(dnode_index=dnode_index, config_dir=config_dir)

        if not pid:
            match_info = config_dir or f"dnode{dnode_index}"
            _log.info(f"No taosd process found for {match_info}")
            return False

        _log.info(f"Sending CTRL_BREAK_EVENT to taosd process (PID: {pid})")
        _send_ctrl_break_event(pid)

        waited = 0
        interval = 0.5
        while waited < timeout:
            if not psutil.pid_exists(pid):
                _log.info(f"taosd process {pid} has exited gracefully")
                return True
            sleep(interval)
            waited += interval

        _log.info(f"taosd process {pid} did not exit in {timeout}s, force killing...")
        os.kill(pid, signal.SIGTERM)
        return False

    except Exception as e:
        _log.error(f"Error stopping taosd: {e}")
        return False


def _find_taosd_pid(dnode_index=None, config_dir=None):
    """查找指定 taosd 进程 PID

    支持两种匹配方式：
    - config_dir: 在 cmdline 中匹配 config 目录路径（精确）
    - dnode_index: 在 cmdline 中匹配 dnode{index}（兼容）
    """
    match_key = config_dir.replace('\\', '/').lower() if config_dir else None
    # 先只按 name 快速过滤，避免遍历所有进程的 cmdline（Windows 上获取 cmdline 很慢）
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if 'taosd' not in (proc.info['name'] or '').lower():
                continue
            # 只对 taosd 进程获取 cmdline
            cmdline = proc.cmdline()
            if not cmdline:
                continue
            # 将 cmdline 各参数用空格拼接，统一为正斜杠并转小写，避免 Windows 路径格式不一致
            cmdline_str = ' '.join(cmdline).replace('\\', '/').lower()
            if match_key and match_key in cmdline_str:
                return proc.info['pid']
            if dnode_index is not None and f'dnode{dnode_index}' in cmdline_str:
                return proc.info['pid']
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None
