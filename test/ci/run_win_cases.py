import os
import subprocess
import time
import shutil
from pathlib import Path
import logging
import psutil
import sys

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

def load_exclusion_list(exclusion_file):
    """加载排除用例列表"""
    exclusion_list = []
    if exclusion_file and os.path.exists(exclusion_file):
        with open(exclusion_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    exclusion_list.add(line)
    return exclusion_list

def get_work_dir():
    selfPath = os.path.dirname(os.path.realpath(__file__))
    if ("community" in selfPath):
        projPath = selfPath.split("community")[0]
    else:
        projPath = selfPath.split("test")[0]
    return os.path.join(projPath, "sim")

def clean_taos_process():
    pid = None
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        if ('mintty' in  proc.info['name']
            and proc.info['cmdline']  # 确保 cmdline 非空
            and any('taos' in arg for arg in proc.info['cmdline'])
        ):
            logger.debug(proc.info)
            logger.debug("Found taos process with PID: %s", proc.info['pid'])
            pid = proc.info['pid']
            killCmd = f"taskkill /PID {pid} /T /F"
            os.system(killCmd)


def process_pytest_file(input_file, exclusion_file="win_ignore_cases"):
    # 初始化统计变量
    total_cases = 0
    success_cases = 0
    failed_cases = 0
    skipped_cases = 0
    failed_case_list = []
    start_time = time.time()

    exclusion_list = load_exclusion_list(exclusion_file)
    work_dir = get_work_dir()
    
    # 创建日志目录
    log_dir = "pytest_logs"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)
    os.makedirs(log_dir, exist_ok=True)
    
    with open(input_file, 'r') as f:
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
                    shutil.rmtree(work_dir)
                
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
                    # os.remove(log_file)
                    logger.info(f"Case {pytest_cmd}: Success. Time cost: {execution_time:.2f}s")
                else:
                    failed_cases += 1
                    failed_case_list.append((total_cases, pytest_cmd))
                    logger.info(f"Case {pytest_cmd} Failed. Time cost: {execution_time:.2f}s")
                    
            except Exception as e:
                case_end = time.time()
                execution_time = case_end - case_start
                failed_cases += 1
                failed_case_list.append((total_cases, pytest_cmd))
                logger.info(f"Case {total_cases} Exception: {str(e)}. Time cost: {execution_time:.2f}s")
                
    # 计算总执行时间
    end_time = time.time()
    total_execution_time = end_time - start_time
    
    # 输出统计信息
    logger.info("\nAll cases run finished:")
    logger.info(f"Total cost time: {total_execution_time:.2f}s")
    logger.info(f"Total cases: {total_cases}")
    logger.info(f"Success cases: {success_cases}")
    logger.info(f"Failed cases: {failed_cases}")
    logger.info(f"Windows skip cases: {skipped_cases}")
    
    if failed_cases > 0:
        logger.info("\nFailed cases list:")
        for case_num, cmd in failed_case_list:
            logger.info(f"{case_num}: {cmd}")
            
    # 如果没有失败用例，删除日志目录
    #if failed_cases == 0 and os.path.exists(log_dir):
    #    shutil.rmtree(log_dir)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        logger.info("用法: python pytest_runner.py <输入文件>")
        sys.exit(1)
        
    input_file = sys.argv[1]
    process_pytest_file(input_file)