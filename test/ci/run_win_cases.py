# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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
import subprocess
import time
import shutil
import logging
import psutil
import sys
import zipfile
import signal
from datetime import datetime

from windows_case_utils import (
    is_excluded_command, parse_case_line, pytest_command_args, pytest_log_name,
    is_pytest_only_skip,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
failed_cases = 0  # 全局变量，记录失败用例数量
exit_flag = False  # 全局退出标志
current_process = None  # 当前运行的子进程


def signal_handler(signum, frame):
    """处理 Ctrl+C 信号"""
    global exit_flag, current_process
    logger.info("接收到中断信号，正在退出...")
    exit_flag = True
    # 终止当前子进程（使用 kill_process_tree 确保子进程也被清理）
    if current_process is not None:
        try:
            kill_process_tree(current_process)
        except:
            pass


# 注册信号处理
signal.signal(signal.SIGINT, signal_handler)


def get_git_commit_id():
    try:
        commit_id = (
            subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=os.path.dirname(os.path.realpath(__file__)),
                stderr=subprocess.STDOUT,
            )
            .decode("utf-8")
            .strip()
        )
        return commit_id
    except Exception:
        return "unknown"


def load_exclusion_list(exclusion_file):
    """加载排除用例列表"""
    exclusion_list = []
    if exclusion_file and os.path.exists(exclusion_file):
        with open(exclusion_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    exclusion_list.append(line)
    return exclusion_list


def get_work_dir():
    if os.environ.get("WORK_DIR"):
        return os.path.abspath(os.environ["WORK_DIR"])
    self_path = os.path.dirname(os.path.realpath(__file__))
    norm_path = self_path.replace("\\", "/")

    if "source/taos-community/" in norm_path:
        repo_root = norm_path.split("source/taos-community/")[0].rstrip("/")
    elif "community/" in norm_path:
        repo_root = norm_path.split("community/")[0].rstrip("/")
    elif "test/" in norm_path:
        repo_root = norm_path.split("test/")[0].rstrip("/")
    else:
        repo_root = os.path.dirname(self_path)

    return os.path.join(os.path.normpath(repo_root), "sim")


def clean_taos_process(keywords=None):
    """
    清理与指定关键字模糊匹配的进程，并确认进程被成功删除。

    :param keywords: List[str]，用于匹配进程命令行的关键字列表。如果为 None，则默认匹配 'taos'。
    """
    if keywords is None:
        keywords = [
            "taos",
            "taosd",
            "taosadapter",
            "taoskeeper",
            "taos-explorer",
            "taosdump",
            "taosBenchmark",
            "tmq_",
            "sml_",
            "write_raw_block_test",
            "get_db_name_test",
            "taosudf",
        ]
    keywords = [k.lower() for k in keywords]

    current_pid = os.getpid()

    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        pid = None
        try:
            if proc.info["pid"] == current_pid:
                continue

            cmdline_parts = proc.info.get("cmdline") or []
            cmdline = " ".join(cmdline_parts).lower()
            proc_name = (proc.info.get("name") or "").lower()
            pid = proc.info["pid"]

            # 跳过 Jenkins agent，避免断开 remoting channel 导致当前用例被系统回收
            if "agent.jar" in cmdline or "jenkins" in cmdline:
                continue

            # 清理 taos 相关进程，同时避免误杀 Jenkins/agent
            if any(
                keyword in proc_name for keyword in keywords
            ) or proc_name.startswith("taos"):
                logger.debug(f"Found matching process: {proc.info}")
                proc.terminate()  # 优雅终止进程
                try:
                    proc.wait(timeout=5)  # 等待进程终止
                    logger.info(f"Process {pid} terminated successfully.")
                except psutil.TimeoutExpired:
                    logger.warning(
                        f"Process {pid} did not terminate in time. Forcing termination."
                    )
                    proc.kill()  # 强制终止进程
                    try:
                        proc.wait(timeout=5)  # 再次等待进程终止
                        logger.info(f"Process {pid} killed successfully.")
                    except psutil.TimeoutExpired:
                        logger.error(
                            f"Process {pid} still alive after kill(), trying system command."
                        )
                        _force_kill_process(pid, proc_name)
        except psutil.NoSuchProcess:
            logger.info(f"Process {pid} does not exist or already terminated.")
        except Exception as e:
            logger.error(f"Error while terminating process {pid}: {e}")


def kill_process_tree(proc):
    """Kill a process and all its descendants recursively using psutil.
    This is necessary because shell=True spawns a cmd.exe shell whose children
    (pytest, python test scripts) are NOT killed when the shell is killed."""
    if proc is None:
        return
    try:
        parent = psutil.Process(proc.pid)
        children = parent.children(recursive=True)
        for child in children:
            try:
                child.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        proc.kill()
        gone, still_alive = psutil.wait_procs([parent] + children, timeout=5)
        for p in still_alive:
            try:
                p.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        try:
            proc.wait(timeout=3)
        except Exception:
            pass
    except psutil.NoSuchProcess:
        try:
            proc.kill()
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Error killing process tree for PID {proc.pid}: {e}")
        try:
            proc.kill()
        except Exception:
            pass


def _force_kill_process(pid, proc_name=None):
    """
    使用系统命令强制终止顽固进程。
    当 psutil 的 terminate/kill 都失败时作为备选方案。
    """
    import subprocess
    import platform

    if pid is None:
        return

    name_str = f" ({proc_name})" if proc_name else ""
    logger.info(f"Force killing process {pid}{name_str} using system command...")

    try:
        if platform.system() == "Windows":
            # Windows: 先尝试 taskkill /t (包括子进程)
            result = subprocess.run(
                ["taskkill", "/f", "/t", "/pid", str(pid)],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                logger.info(f"Process {pid} killed by taskkill.")
                return
            else:
                logger.warning(f"taskkill failed: {result.stderr.strip()}")

            # 如果 taskkill 失败，使用 wmic（更强力的方法）
            result = subprocess.run(
                ["wmic", "process", "where", f"processid={pid}", "delete"],
                capture_output=True,
                text=True,
            )
            if (
                "Instance deletion successful" in result.stdout
                or result.returncode == 0
            ):
                logger.info(f"Process {pid} killed by wmic.")
            else:
                logger.error(
                    f"wmic failed: {result.stderr.strip() if result.stderr else result.stdout}"
                )
        else:
            # Linux/Mac: 使用 kill -9
            result = subprocess.run(
                ["kill", "-9", str(pid)], capture_output=True, text=True
            )
            if result.returncode == 0:
                logger.info(f"Process {pid} killed by kill -9.")
            else:
                logger.error(f"kill -9 failed: {result.stderr.strip()}")
    except Exception as e:
        logger.error(f"Error during force kill of {pid}: {e}")


def zip_dir(dir_path, zip_path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                abs_path = os.path.join(root, file)
                rel_path = os.path.relpath(abs_path, dir_path)
                zipf.write(abs_path, rel_path)


def preserve_case_server_logs(work_dir, destination):
    """Keep failed-case diagnostics before the owned sim directory is removed."""
    source_root = os.path.normcase(os.path.realpath(work_dir))
    if not os.path.isdir(source_root):
        return
    for node in os.listdir(source_root):
        if not (node.startswith("dnode") and node[5:].isdigit()):
            continue
        for section in ("cfg", "log"):
            source = os.path.join(source_root, node, section)
            if os.path.normcase(os.path.realpath(source)) != os.path.normcase(os.path.abspath(source)):
                continue
            for directory, dirs, files in os.walk(source, followlinks=False):
                dirs[:] = [name for name in dirs if
                           os.path.normcase(os.path.realpath(os.path.join(directory, name))) ==
                           os.path.normcase(os.path.abspath(os.path.join(directory, name)))]
                for name in files:
                    path = os.path.join(directory, name)
                    resolved = os.path.normcase(os.path.realpath(path))
                    if resolved != os.path.normcase(os.path.abspath(path)):
                        continue
                    target = os.path.join(destination, os.path.relpath(path, source_root))
                    try:
                        os.makedirs(os.path.dirname(target), exist_ok=True)
                        shutil.copy2(path, target)
                    except OSError as error:
                        logger.warning("Cannot preserve failed-case diagnostic %s: %s", path, error)


def _terminate_work_dir_taos_processes(work_dir):
    """Reap late TAOS children only when they hold files in this test directory."""
    prefix = os.path.normcase(os.path.abspath(work_dir)).rstrip(os.sep) + os.sep
    for proc in psutil.process_iter(["pid", "name"]):
        try:
            name = (proc.info.get("name") or "").lower()
            if proc.pid == os.getpid() or not name.startswith("taos"):
                continue
            held = [
                item.path for item in proc.open_files()
                if os.path.normcase(os.path.abspath(item.path)).startswith(prefix)
            ]
            if held:
                logger.info(f"Terminating late process {proc.pid} holding test files: {held}")
                kill_process_tree(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue


def safe_rmtree(path, retries=10, delay=2, on_retry=None):
    """
    安全删除目录，支持重试机制。
    :param path: 要删除的目录路径。
    :param retries: 重试次数。
    :param delay: 每次重试之间的延迟时间（秒）。
    :param on_retry: 删除失败后调用的清理函数。
    """
    import sys

    # Windows 上进程终止后句柄可能未立即释放，先等待一下
    if sys.platform == "win32":
        time.sleep(1)
    for i in range(retries):
        try:
            if os.path.exists(path):
                shutil.rmtree(path)
            return
        except Exception as e:
            logger.warning(
                f"Retry {i + 1}/{retries}: Failed to remove {path}. Error: {e}"
            )
            if on_retry is not None:
                on_retry()
            time.sleep(delay)
    raise Exception(f"Failed to remove {path} after {retries} retries.")


def process_pytest_file(
    input_file,
    log_path="C:\\CI_logs",
    exclusion_file=os.path.join(os.path.dirname(__file__), "win_ignore_cases"),
):
    global failed_cases, exit_flag  # 声明使用全局变量
    # 初始化统计变量
    total_cases = 0
    success_cases = 0
    skipped_cases = 0
    failed_case_list = []
    start_time = time.time()

    exclusion_list = load_exclusion_list(exclusion_file)
    work_dir = get_work_dir()
    commit_id = get_git_commit_id()
    start_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_prefix = f"log_{commit_id}_{start_time_str}"
    result_file = f"result_{commit_id}_{start_time_str}.txt"

    # 创建日志目录
    log_dir = log_prefix
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)
    os.makedirs(log_dir, exist_ok=True)

    # 定义清理函数（失败时抛出异常，终止测试）
    def cleanup_environment(phase="", evidence_dir=None):
        """清理进程和目录，失败则抛出异常终止测试"""
        logger.info(f"Cleaning up environment ({phase})...")

        # 1. 结束残留进程
        clean_taos_process()

        if evidence_dir is not None:
            try:
                preserve_case_server_logs(work_dir, evidence_dir)
            except OSError as error:
                logger.warning("Cannot preserve failed-case server logs: %s", error)

        # 2. 等待句柄释放（Windows 需要更长时间）
        time.sleep(3)

        # 3. 删除 sim 目录，使用 safe_rmtree 带重试机制
        if os.path.exists(work_dir):
            try:
                safe_rmtree(
                    work_dir, retries=15, delay=2,
                    on_retry=(lambda: _terminate_work_dir_taos_processes(work_dir))
                    if sys.platform == "win32" else None,
                )
                logger.info(f"Removed {work_dir}")
            except Exception as e:
                logger.error(f"CRITICAL: Failed to remove {work_dir}: {e}")
                # 列出占用文件的进程，帮助诊断
                if sys.platform == "win32":
                    try:
                        logger.info(
                            "Attempting to identify processes holding handles to work_dir..."
                        )
                        # 使用 psutil 检查是否有进程仍在使用该目录
                        for proc in psutil.process_iter(["pid", "name"]):
                            try:
                                for item in proc.open_files():
                                    if work_dir in item.path:
                                        logger.warning(
                                            f"Process {proc.pid} ({proc.name()}) is holding: {item.path}"
                                        )
                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                pass
                    except Exception as diag_e:
                        logger.error(f"Failed to diagnose handle holders: {diag_e}")
                raise RuntimeError("Cleanup failed: cannot remove work_dir") from e

    seen_commands = set()
    with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # 先用 win_ignore_cases 匹配，统一处理需要跳过的 case
            # 提取第五列 command，检查其路径部分是否在排除列表中
            parts = line.split(",", 4)
            if len(parts) != 5:
                raise ValueError(f"Invalid task row: {line}")
            command = parts[4].strip()
            should_skip = is_excluded_command(command, exclusion_list, os.environ.get("WIN_TEST_SUITE", "full"))

            if should_skip:
                skipped_cases += 1
                logger.info(f"Case in win_ignore_cases, skip: {line}")
                result_str = f"Skip\t{line}\t\t\t\tlisted in win_ignore_cases\n"
                with open(result_file, "a", encoding="utf-8") as rf:
                    rf.write(result_str)
                continue

            pytest_cmd, case_timeout = parse_case_line(line)
            if pytest_cmd in seen_commands:
                logger.info(f"Duplicate task, skip: {pytest_cmd}")
                continue
            seen_commands.add(pytest_cmd)

            total_cases += 1
            log_file = os.path.join(log_dir, pytest_log_name(pytest_cmd))

            logger.info(f"Running case {pytest_cmd}")
            case_start = time.time()
            return_code = None

            # ========== 用例前清理（失败则终止） ==========
            try:
                cleanup_environment("pre-case")
            except RuntimeError as e:
                logger.error(f"Pre-case cleanup failed, stopping test suite: {e}")
                failed_cases += 1
                failed_case_list.append(pytest_cmd)
                result_str = (
                    f"ERROR\t{pytest_cmd}\t\t\t0.00s\tPre-case cleanup failed: {e}\n"
                )
                with open(result_file, "a", encoding="utf-8") as rf:
                    rf.write(result_str)
                break  # 清理失败，终止整个测试

            # ========== 执行用例 ==========
            try:
                if exit_flag:
                    logger.info("检测到退出标志，终止测试")
                    break

                global current_process
                pytest_cmd_clean = pytest_command_args(pytest_cmd, timeout=case_timeout)

                with open(log_file, "w", buffering=1) as log:
                    case_environment = os.environ.copy()
                    case_environment["PYTHONIOENCODING"] = "utf-8"
                    backup_evidence = None
                    if "/06-TaosBackup/" in pytest_cmd:
                        backup_evidence = os.path.abspath(os.path.join(log_dir, f"{os.path.splitext(os.path.basename(log_file))[0]}-evidence"))
                        try:
                            os.makedirs(backup_evidence, exist_ok=True)
                            case_environment["TAOSBACKUP_DIAGNOSTICS_DIR"] = backup_evidence
                        except OSError as error:
                            logger.warning("Cannot create backup evidence directory: %s", error)
                            backup_evidence = None
                    current_process = subprocess.Popen(
                        pytest_cmd_clean,
                        shell=False,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        text=True,
                        env=case_environment,
                    )

                    # 轮询等待
                    waited = 0
                    while waited < case_timeout:
                        ret = current_process.poll()
                        if ret is not None:
                            return_code = ret
                            break
                        if exit_flag:
                            logger.info("测试被中断")
                            kill_process_tree(current_process)
                            sys.exit(130)
                        time.sleep(0.5)
                        waited += 0.5
                    else:
                        # 超时
                        kill_process_tree(current_process)
                        return_code = -1
                        logger.info(
                            f"Case {pytest_cmd} running timeout, killed process."
                        )
                    current_process = None

                # Preserve the mirror before another case clears tmp/. Chinese
                # restore uses subprocess directly; its output is in log_file.
                if backup_evidence:
                    for mirror in ("tmp/backup.log", "restore.log"):
                        if os.path.isfile(mirror) and os.path.getmtime(mirror) >= case_start:
                            try:
                                shutil.copyfile(mirror, os.path.join(backup_evidence, os.path.basename(mirror)))
                            except OSError as error:
                                logger.warning("Cannot preserve %s: %s", mirror, error)

                # 确保日志刷盘
                if os.path.exists(log_file):
                    with open(log_file, "a") as f:
                        f.flush()
                        try:
                            os.fsync(f.fileno())
                        except:
                            pass

            except KeyboardInterrupt:
                exit_flag = True
                if current_process:
                    kill_process_tree(current_process)
                sys.exit(130)
            except Exception as e:
                return_code = -2  # 执行异常
                logger.error(f"Case execution error: {e}")

            # ========== 用例后清理（失败则终止） ==========
            try:
                evidence_dir = None
                if return_code != 0:
                    evidence_dir = os.path.join(
                        log_dir, os.path.splitext(os.path.basename(log_file))[0] + "-evidence", "sim")
                cleanup_environment("post-case", evidence_dir)
            except RuntimeError as e:
                logger.error(f"Post-case cleanup failed, stopping test suite: {e}")
                failed_cases += 1
                failed_case_list.append(pytest_cmd)
                result_str = (
                    f"ERROR\t{pytest_cmd}\t\t\t{time.time() - case_start:.2f}s"
                    f"\tPost-case cleanup failed: {e}; pytest return code: {return_code}\n"
                )
                with open(result_file, "a", encoding="utf-8") as rf:
                    rf.write(result_str)
                break  # 清理失败，终止整个测试

            # ========== 记录用例结果 ==========
            case_end = time.time()
            execution_time = case_end - case_start
            all_skipped = False
            if return_code in {0, 5}:
                with open(log_file, encoding='utf-8', errors='replace') as output:
                    all_skipped = is_pytest_only_skip(return_code, output.read())

            if all_skipped:
                skipped_cases += 1
                logger.info(
                    f"Case {pytest_cmd} Skipped (no tests ran). Time cost: {execution_time:.2f}s"
                )
                result_str = f"Skip\t{pytest_cmd}\t\t\t{execution_time:.2f}s\tall pytest tests skipped\n"
            elif return_code == 0:
                success_cases += 1
                os.remove(log_file)
                logger.info(
                    f"Case {pytest_cmd}: Success. Time cost: {execution_time:.2f}s"
                )
                result_str = f"SUCCESS\t{pytest_cmd}\t\t\t{execution_time:.2f}s\n"
            elif return_code == -1:
                failed_cases += 1
                failed_case_list.append(pytest_cmd)
                logger.info(
                    f"Case {pytest_cmd} Failed (timeout). Time cost: {execution_time:.2f}s"
                )
                result_str = (
                    f"FAILED\t{pytest_cmd}\t\t\t{execution_time:.2f}s\ttimeout\n"
                )
            elif return_code == -2:
                failed_cases += 1
                failed_case_list.append(pytest_cmd)
                logger.info(
                    f"Case {pytest_cmd} Exception. Time cost: {execution_time:.2f}s"
                )
                result_str = (
                    f"ERROR\t{pytest_cmd}\t\t\t{execution_time:.2f}s\texecution error\n"
                )
            else:
                failed_cases += 1
                failed_case_list.append(pytest_cmd)
                logger.info(
                    f"Case {pytest_cmd} Failed. Time cost: {execution_time:.2f}s"
                )
                result_str = f"FAILED\t{pytest_cmd}\t\t\t{execution_time:.2f}s\n"

            with open(result_file, "a", encoding="utf-8") as rf:
                rf.write(result_str)

            # Surface the failure in the CI trace immediately, so a failing
            # round can be diagnosed without waiting for the job artifacts.
            if return_code != 0 and os.path.exists(log_file):
                logger.info(f"---- tail of {log_file} ----")
                try:
                    with open(log_file, "r", encoding="utf-8", errors="replace") as failed_log:
                        for line in failed_log.readlines()[-60:]:
                            logger.info(line.rstrip())
                except Exception as read_error:
                    logger.info(f"unable to read {log_file}: {read_error}")
                logger.info("---- end of case log ----")

    # ========== 收尾 ==========
    end_time = time.time()
    total_execution_time = end_time - start_time

    logger.info("All cases run finished:")
    logger.info(f"Total cost time: {total_execution_time:.2f}s")
    logger.info(f"Total cases: {total_cases}")
    logger.info(f"Success cases: {success_cases}")
    logger.info(f"Failed cases: {failed_cases}")
    logger.info(f"Windows skip cases: {skipped_cases}")
    if failed_cases > 0:
        logger.info("\nFailed cases list:")
        for cmd in failed_case_list:
            logger.info(cmd)

    with open(result_file, "a", encoding="utf-8") as rf:
        rf.write("\nAll cases run finished:\n")
        rf.write(f"Total cost time: {total_execution_time:.2f}s\n")
        rf.write(f"Total cases: {total_cases}\n")
        rf.write(f"Success cases: {success_cases}\n")
        rf.write(f"Failed cases: {failed_cases}\n")
        rf.write(f"Windows skip cases: {skipped_cases}\n")
        if failed_cases > 0:
            rf.write("\nFailed cases list:\n")
            for cmd in failed_case_list:
                rf.write(f"{cmd}\n")

    if not os.path.exists(log_path):
        os.makedirs(log_path, exist_ok=True)
    zip_dir(log_dir, f"{log_prefix}.zip")
    shutil.move(f"{log_prefix}.zip", os.path.join(log_path, f"{log_prefix}.zip"))
    shutil.move(result_file, os.path.join(log_path, result_file))


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        logger.info("用法: python pytest_runner.py <输入文件> <日志路径>")
        sys.exit(1)

    input_file = sys.argv[1]
    if len(sys.argv) >= 3:
        log_path = sys.argv[2]
        process_pytest_file(input_file, log_path)
    else:
        process_pytest_file(input_file)
    # 根据 failed_cases 的值决定退出码
    sys.exit(1 if failed_cases > 0 else 0)
