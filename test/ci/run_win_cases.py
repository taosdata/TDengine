import os
import subprocess
import time
import shutil
from pathlib import Path
import logging
import psutil
import sys
import zipfile
import signal
from datetime import datetime

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
failed_cases = 0  # 全局变量，记录失败用例数量
exit_flag = False  # 全局退出标志
current_process = None  # 当前运行的子进程

def signal_handler(signum, frame):
    """处理 Ctrl+C 信号"""
    global exit_flag, current_process
    logger.info("接收到中断信号，正在退出...")
    exit_flag = True
    # 终止当前子进程
    if current_process is not None:
        try:
            current_process.terminate()
        except:
            pass

# 注册信号处理
signal.signal(signal.SIGINT, signal_handler)

def get_git_commit_id():
    try:
        commit_id = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=os.path.dirname(os.path.realpath(__file__)),
            stderr=subprocess.STDOUT
        ).decode("utf-8").strip()
        return commit_id
    except Exception as e:
        return "unknown"


def load_exclusion_list(exclusion_file):
    """加载排除用例列表"""
    exclusion_list = []
    if exclusion_file and os.path.exists(exclusion_file):
        with open(exclusion_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    exclusion_list.append(line)
    return exclusion_list


def get_work_dir():
    selfPath = os.path.dirname(os.path.realpath(__file__))
    if ("community" in selfPath):
        projPath = selfPath.split("community")[0]
    else:
        projPath = selfPath.split("test")[0]
    return os.path.join(projPath, "sim")


def clean_taos_process(keywords=None):
    """
    清理与指定关键字模糊匹配的进程，并确认进程被成功删除。

    :param keywords: List[str]，用于匹配进程命令行的关键字列表。如果为 None，则默认匹配 'taos'。
    """
    if keywords is None:
        keywords = ["taos", "taosd", "taosadapter", "taoskeeper", "taos-explorer", "taosx", "tmq_sim", "taosdump", "taosBenchmark" ]

    current_pid = os.getpid()

    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info["pid"] == current_pid:
                continue

            cmdline_parts = proc.info.get("cmdline") or []
            cmdline = " ".join(cmdline_parts).lower()
            proc_name = (proc.info.get("name") or "").lower()

            # 跳过 Jenkins agent，避免断开 remoting channel 导致当前用例被系统回收
            if "agent.jar" in cmdline or "jenkins" in cmdline:
                continue

            # 清理 taos 相关进程，同时避免误杀 Jenkins/agent
            if any(keyword in proc_name for keyword in keywords) or proc_name.startswith("taos"):
                logger.debug(f"Found matching process: {proc.info}")
                proc.terminate()  # 优雅终止进程
                try:
                    proc.wait(timeout=5)  # 等待进程终止
                    logger.info(f"Process {proc.info['pid']} terminated successfully.")
                except psutil.TimeoutExpired:
                    logger.warning(f"Process {proc.info['pid']} did not terminate in time. Forcing termination.")
                    proc.kill()  # 强制终止进程
                    proc.wait(timeout=5)  # 再次等待进程终止
                    logger.info(f"Process {proc.info['pid']} killed successfully.")
        except psutil.NoSuchProcess:
            logger.info(f"Process {proc.info.get('pid')} does not exist or already terminated.")
        except Exception as e:
            logger.error(f"Error while terminating process {proc.info.get('pid')}: {e}")

def zip_dir(dir_path, zip_path):
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                abs_path = os.path.join(root, file)
                rel_path = os.path.relpath(abs_path, dir_path)
                zipf.write(abs_path, rel_path)

def safe_rmtree(path, retries=10, delay=2):
    """
    安全删除目录，支持重试机制。
    :param path: 要删除的目录路径。
    :param retries: 重试次数。
    :param delay: 每次重试之间的延迟时间（秒）。
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
            logger.warning(f"Retry {i+1}/{retries}: Failed to remove {path}. Error: {e}")
            time.sleep(delay)
    raise Exception(f"Failed to remove {path} after {retries} retries.")

def process_pytest_file(input_file, log_path="C:\\CI_logs",
                        exclusion_file=os.path.join(os.path.dirname(__file__), "win_ignore_cases")):
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
    def cleanup_environment(phase=""):
        """清理进程和目录，失败则抛出异常终止测试"""
        logger.info(f"Cleaning up environment ({phase})...")
        
        # 1. 结束残留进程
        clean_taos_process()
        
        # 2. 等待句柄释放
        time.sleep(1)
        
        # 3. 删除 sim 目录，失败则终止
        if os.path.exists(work_dir):
            try:
                shutil.rmtree(work_dir)
                logger.info(f"Removed {work_dir}")
            except Exception as e:
                logger.error(f"CRITICAL: Failed to remove {work_dir}: {e}")
                raise RuntimeError(f"Cleanup failed: cannot remove work_dir") from e

    with open(input_file, 'r', encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            # 解析 pytest 命令 - 格式: priority,rerunTimes,sanitizer(y/n),path,command
            # 检查格式: 第三列是 y/n，第五列决定使用 ./ci/pytest.sh 还是 pytest
            parts = line.split(',')
            if len(parts) < 5:
                logger.error(f"格式错误: 列数不足(应有5列,实际{len(parts)}列): {line}")
                continue
            
            sanitizer = parts[2].strip()  # 第三列: y 或 n
            if sanitizer not in ('y', 'n'):
                logger.error(f"格式错误: 第三列必须是 y 或 n, 实际为 '{sanitizer}': {line}")
                continue
            
            # 根据原逻辑解析
            if "ci/pytest.sh " in line:
                pytest_cmd = line.split("ci/pytest.sh ")[1]
            else:
                pytest_cmd = line.split(",,n,.,")[1]

            if not pytest_cmd.startswith("pytest"):
                logger.warning(f"异常pytest命令: {pytest_cmd}")
                continue

            case_base_name = pytest_cmd.split(" ")[1]
            if case_base_name and len(exclusion_list) > 0 and case_base_name in exclusion_list:
                skipped_cases += 1
                logger.info(f"Case {case_base_name} not support runnning on Windows. Skip test.")
                result_str = f"Skip\t{pytest_cmd}\t\t\t\n"
                with open(result_file, "a", encoding="utf-8") as rf:
                    rf.write(result_str)
                continue

            case_name = pytest_cmd.split("/")[-1].replace(" ", "_")
            total_cases += 1
            log_file = os.path.join(log_dir, f"{case_name}.log")

            logger.info(f"Running case {pytest_cmd}")
            case_start = time.time()
            return_code = None

            # ========== 用例前清理（失败则终止） ==========
            try:
                cleanup_environment("pre-case")
            except RuntimeError as e:
                logger.error(f"Pre-case cleanup failed, stopping test suite: {e}")
                result_str = f"ERROR\t{pytest_cmd}\t\t\t0.00s\tPre-case cleanup failed: {e}\n"
                with open(result_file, "a", encoding="utf-8") as rf:
                    rf.write(result_str)
                break  # 清理失败，终止整个测试

            # ========== 执行用例 ==========
            try:
                if exit_flag:
                    logger.info("检测到退出标志，终止测试")
                    break

                global current_process
                pytest_cmd_clean = f"{pytest_cmd} --clean"
                
                with open(log_file, 'w', buffering=1) as log:
                    current_process = subprocess.Popen(
                        pytest_cmd_clean,
                        shell=True,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        text=True
                    )

                    # 轮询等待
                    waited = 0
                    while waited < 1200:
                        ret = current_process.poll()
                        if ret is not None:
                            return_code = ret
                            break
                        if exit_flag:
                            logger.info("测试被中断")
                            current_process.kill()
                            current_process.wait()
                            sys.exit(130)
                        time.sleep(0.5)
                        waited += 0.5
                    else:
                        # 超时
                        current_process.kill()
                        return_code = -1
                        logger.info(f"Case {pytest_cmd} running timeout, killed process.")
                    current_process = None

                # 确保日志刷盘
                if os.path.exists(log_file):
                    with open(log_file, 'a') as f:
                        f.flush()
                        try:
                            os.fsync(f.fileno())
                        except:
                            pass

            except KeyboardInterrupt:
                exit_flag = True
                if current_process:
                    current_process.kill()
                sys.exit(130)
            except Exception as e:
                return_code = -2  # 执行异常
                logger.error(f"Case execution error: {e}")

            # ========== 用例后清理（失败则终止） ==========
            try:
                cleanup_environment("post-case")
            except RuntimeError as e:
                logger.error(f"Post-case cleanup failed, stopping test suite: {e}")
                # 记录当前用例结果（如果执行过）
                if return_code is not None:
                    if return_code == 0:
                        result_str = f"SUCCESS\t{pytest_cmd}\t\t\t{time.time() - case_start:.2f}s\n"
                    else:
                        result_str = f"FAILED\t{pytest_cmd}\t\t\t{time.time() - case_start:.2f}s\n"
                else:
                    result_str = f"ERROR\t{pytest_cmd}\t\t\t{time.time() - case_start:.2f}s\tExecution error\n"
                with open(result_file, "a", encoding="utf-8") as rf:
                    rf.write(result_str)
                break  # 清理失败，终止整个测试

            # ========== 记录用例结果 ==========
            case_end = time.time()
            execution_time = case_end - case_start

            if return_code == 0:
                success_cases += 1
                os.remove(log_file)
                logger.info(f"Case {pytest_cmd}: Success. Time cost: {execution_time:.2f}s")
                result_str = f"SUCCESS\t{pytest_cmd}\t\t\t{execution_time:.2f}s\n"
            elif return_code == -1:
                failed_cases += 1
                failed_case_list.append(pytest_cmd)
                logger.info(f"Case {pytest_cmd} Failed (timeout). Time cost: {execution_time:.2f}s")
                result_str = f"FAILED\t{pytest_cmd}\t\t\t{execution_time:.2f}s\ttimeout\n"
            elif return_code == -2:
                failed_cases += 1
                failed_case_list.append(pytest_cmd)
                logger.info(f"Case {pytest_cmd} Exception. Time cost: {execution_time:.2f}s")
                result_str = f"ERROR\t{pytest_cmd}\t\t\t{execution_time:.2f}s\texecution error\n"
            else:
                failed_cases += 1
                failed_case_list.append(pytest_cmd)
                logger.info(f"Case {pytest_cmd} Failed. Time cost: {execution_time:.2f}s")
                result_str = f"FAILED\t{pytest_cmd}\t\t\t{execution_time:.2f}s\n"

            with open(result_file, "a", encoding="utf-8") as rf:
                rf.write(result_str)

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
        rf.write(f"\nAll cases run finished:\n")
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