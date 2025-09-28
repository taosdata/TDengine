import os
import subprocess
import time
import shutil
from pathlib import Path
import logging
import psutil
import sys
import zipfile
from datetime import datetime

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
failed_cases = 0  # 全局变量，记录失败用例数量

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
        keywords = ['taos']  # 默认关键字为 'taos'

    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            # 检查进程命令行是否包含指定关键字
            if proc.info['cmdline'] and any(keyword in ' '.join(proc.info['cmdline']) for keyword in keywords):
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

def safe_rmtree(path, retries=5, delay=1):
    """
    安全删除目录，支持重试机制。
    :param path: 要删除的目录路径。
    :param retries: 重试次数。
    :param delay: 每次重试之间的延迟时间（秒）。
    """
    for i in range(retries):
        try:
            if os.path.exists(path):
                shutil.rmtree(path)
            return
        except Exception as e:
            logger.warning(f"Retry {i+1}/{retries}: Failed to remove {path}. Error: {e}")
            time.sleep(delay)
    raise Exception(f"Failed to remove {path} after {retries} retries.")

def safe_rmtree(path, retries=5, delay=1):
    """
    安全删除目录，支持重试机制。

    :param path: 要删除的目录路径。
    :param retries: 重试次数。
    :param delay: 每次重试之间的延迟时间（秒）。
    """
    for i in range(retries):
        try:
            if os.path.exists(path):
                shutil.rmtree(path)
            break
        except Exception as e:
            logger.warning(f"Retry {i + 1}/{retries}: Failed to remove {path}. Error: {e}")
            time.sleep(delay)
    else:
        raise Exception(f"Failed to remove {path} after {retries} retries.")


def process_pytest_file(input_file, log_path="C:\\CI_logs",
                        exclusion_file=os.path.join(os.path.dirname(__file__), "win_ignore_cases")):
    global failed_cases  # 声明使用全局变量
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

    with open(input_file, 'r', encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            # 跳过空行和注释行
            if not line or line.startswith('#'):
                continue

            # 解析pytest命令
            if "ci/pytest.sh " in line:
                pytest_cmd = line.split("ci/pytest.sh ")[1]
            else:
                pytest_cmd = line.split(",,n,.,")[1]

            # 确保是pytest命令
            if not pytest_cmd.startswith("pytest"):
                logger.warning(f"异常pytest命令: {pytest_cmd}")
                continue

            case_base_name = pytest_cmd.split(" ")[1]  # 获取用例无参数名称用于与排除用例列表比对
            if case_base_name and len(exclusion_list) > 0 and case_base_name in exclusion_list:
                skipped_cases += 1
                logger.info(f"Case {case_base_name} not support runnning on Windows. Skip test.")
                result_str = f"Skip\t{pytest_cmd}\t\t\t\n"
                with open(result_file, "a", encoding="utf-8") as rf:
                    rf.write(result_str)
                continue

            case_name = pytest_cmd.split("/")[-1].replace(" ", "_")  # 获取用例名称
            total_cases += 1
            log_file = os.path.join(log_dir, f"{case_name}.log")

            logger.info(f"Running case {pytest_cmd}")
            case_start = time.time()

            try:
                # 清理环境，kill残留进程，删除sim目录
                clean_taos_process()
                if os.path.exists(work_dir):
                    safe_rmtree(work_dir)
                # 执行pytest命令，设置超时为300秒（5分钟）
                with open(log_file, 'w') as log:
                    process = subprocess.Popen(
                        pytest_cmd,
                        shell=True,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        text=True
                    )

                    try:
                        return_code = process.wait(timeout=3000)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        return_code = -1
                        logger.info(f"Case {pytest_cmd} running timeout, killed process.")

                case_end = time.time()
                execution_time = case_end - case_start

                if return_code == 0:
                    success_cases += 1
                    os.remove(log_file)
                    logger.info(f"Case {pytest_cmd}: Success. Time cost: {execution_time:.2f}s")
                    result_str = f"SUCCESS\t{pytest_cmd}\t\t\t{execution_time:.2f}s\n"
                else:
                    failed_cases += 1
                    failed_case_list.append(pytest_cmd)
                    logger.info(f"Case {pytest_cmd} Failed. Time cost: {execution_time:.2f}s")
                    result_str = f"FAILED\t{pytest_cmd}\t\t\t{execution_time:.2f}s\n"

            except Exception as e:
                case_end = time.time()
                execution_time = case_end - case_start
                failed_cases += 1
                failed_case_list.append(pytest_cmd)
                logger.info(f"Case {total_cases} Exception: {str(e)}. Time cost: {execution_time:.2f}s")
                result_str = f"ERROR\t{pytest_cmd}\t\t\t{execution_time:.2f}s\t{str(e)}\n"
            # 每条用例执行完都写入结果文件
            with open(result_file, "a", encoding="utf-8") as rf:
                rf.write(result_str)

    # 计算总执行时间
    end_time = time.time()
    total_execution_time = end_time - start_time

    # 输出统计信息
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
        rf.write(
            f"\nAll cases run finished:\nTotal cost time: {total_execution_time:.2f}s\nTotal cases: {total_cases}\nSuccess cases: {success_cases}\nFailed cases: {failed_cases}\nWindows skip cases: {skipped_cases}\n")
        if failed_cases > 0:
            rf.write("\nFailed cases list:\n")
            for cmd in failed_case_list:
                rf.write(f"{cmd}\n")

    if not os.path.exists(log_path):
        os.makedirs(log_path, exist_ok=True)
    zip_dir(log_dir, f"{log_prefix}.zip")
    shutil.move(f"{log_prefix}.zip", os.path.join(log_path, f"{log_prefix}.zip"))
    shutil.move(result_file, os.path.join(log_path, result_file))

    # 如果没有失败用例，删除日志目录
    # if failed_cases == 0 and os.path.exists(log_dir):
    #    shutil.rmtree(log_dir)


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