import concurrent.futures
import csv
import getopt
import os
import subprocess
import sys
from datetime import datetime

from loguru import logger

change_file_list = ""
scan_dir = ""
web_server = ""
branch_name = ""

opts, args = getopt.gnu_getopt(
    sys.argv[1:], "b:f:w:d:", ["branch_name=", "filesName=", "webServer=", "dir="]
)
for key, value in opts:
    if key in ["-h", "--help"]:
        print("Usage: python3 scan.py -b <branch_name> -f <file_list>")
        print("-b  branch name or PR ID to scan")
        print("-f  change files list")
        print("-w  web server")
        print("-d  directory to scan")
        sys.exit(0)

    if key in ["-b", "--branchName"]:
        branch_name = value
    if key in ["-f", "--filesName"]:
        change_file_list = value
    if key in ["-w", "--webServer"]:
        web_server = value
    if key in ["-d", "--dir"]:
        scan_dir = value


# the base source code file path
self_path = os.path.dirname(os.path.realpath(__file__))

# if ("community" in self_path):
#     TD_project_path = self_path[:self_path.find("community")]
#     work_path = TD_project_path[:TD_project_path.find("TDinternal")]

# else:
#     TD_project_path = self_path[:self_path.find("tests")]
#     work_path = TD_project_path[:TD_project_path.find("TDengine")]

# Check if "community"  or "tests" is in self_path
index_community = self_path.find("community")
if index_community != -1:
    TD_project_path = self_path[:index_community]
    community_path = os.path.join(TD_project_path, "community")
    index_TDinternal = TD_project_path.find("TDinternal")
    # Check if index_TDinternal is valid and set work_path accordingly
    if index_TDinternal != -1:
        work_path = TD_project_path[:index_TDinternal]
else:
    index_tests = self_path.find("test")
    if index_tests != -1:
        TD_project_path = self_path[:index_tests]
        # Check if index_TDengine is valid and set work_path accordingly
        index_TDengine = TD_project_path.find("TDengine")
        if index_TDengine != -1:
            work_path = TD_project_path[:index_TDengine]
            community_path = TD_project_path


# log file path
current_time = datetime.now().strftime("%Y%m%d-%H%M%S")
log_file_path = f"{work_path}/scan_log/scan_{branch_name}_{current_time}/"

os.makedirs(log_file_path, exist_ok=True)

scan_log_file = f"{log_file_path}/scan_log.txt"
logger.add(scan_log_file, rotation="10MB", retention="7 days", level="DEBUG")

# scan result base path
scan_result_base_path = f"{log_file_path}/clang_scan_result/"

# the compile commands json file path
compile_commands_path = f"{TD_project_path}/debug/compile_commands.json"


# the ast parser rule for c file
clang_scan_rules_path = f"{self_path}/filter_for_return_values"
scan_dir_list = ["source", "include", "docs/examples", "src/plugins"]

#
# all the c files path will be checked
all_file_path = []
file_res_path = ""

SCAN_DIRS = ["source", "include", "docs/examples", "src/plugins"]
SCAN_SKIP_FILE_LIST = [
    "tools/taosws-rs/target/release/build/openssl-sys-7811e597b848e397/out/openssl-build/install/include/openssl",
    "/test/",
    "contrib",
    "debug",
    "deps",
    "source/libs/parser/src/sql.c",
    "source/client/jni/windows/win32/bridge/AccessBridgeCalls.c",
    "source/libs/decimal/",
    "source/libs/azure",
]


class CommandExecutor:
    def __init__(self):
        self._process = None

    def execute(self, command, timeout=None):
        try:
            self._process = subprocess.Popen(
                command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout, stderr = self._process.communicate(timeout=timeout)
            return stdout.decode("utf-8"), stderr.decode("utf-8")
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.communicate()
            raise Exception("Command execution timeout")
        except Exception as e:
            raise Exception("Command execution failed: %s" % e)


def scan_files_path(source_file_path):
    """Walk directory and append candidate files under SCAN_DIRS."""
    for root, dirs, files in os.walk(source_file_path):
        if not any(item in root for item in SCAN_DIRS):
            continue
        for file in files:
            file_path = os.path.join(root, file)
            if file_path.endswith(('.c', '.h', '.cpp')) and all(skip not in file_path for skip in SCAN_SKIP_FILE_LIST):
                all_file_path.append(file_path)
    logger.info("Found %s files" % len(all_file_path))


def add_candidate_file(file_name):
    """Normalize a single file token from change list and append if valid.
    - If absolute path provided: append it directly (if exists and not skipped).
    - Otherwise, build path relative to TD_project_path/community or TD_project_path.
    """
    # skip empty
    if not file_name:
        return

    # If absolute path given, use it directly (but validate extension/skip rules)
    if os.path.isabs(file_name):
        if not os.path.exists(file_name):
            logger.warning(f"Changed file not found (abs): {file_name}")
            return
        if not file_name.endswith(('.c', '.h', '.cpp')):
            return
        if any(skip in file_name for skip in SCAN_SKIP_FILE_LIST):
            return
        all_file_path.append(file_name)
        return

    # only care tokens that contain interesting dirs
    if not any(dir_name in file_name for dir_name in SCAN_DIRS):
        return

    # skip by pattern
    if not file_name.endswith(('.c', '.h', '.cpp')):
        return
    if any(skip in file_name for skip in SCAN_SKIP_FILE_LIST):
        return

    # build path relative to repo layout
    if index_community != -1:
        if "enterprise" in file_name:
            candidate = os.path.join(TD_project_path, file_name)
        else:
            candidate = os.path.join(community_path, file_name)
    else:
        candidate = os.path.join(community_path, file_name)

    if not os.path.exists(candidate):
        logger.warning(f"Changed file not found: {candidate}")
        return

    all_file_path.append(candidate)


def input_files(change_files):
    """Read change list and process each token by add_candidate_file"""
    with open(change_files, "r") as file:
        for line in file:
            for file_name in line.strip().split():
                add_candidate_file(file_name)
    logger.info(f"all_file_path:{all_file_path}")
    logger.info("Found %s files" % len(all_file_path))


def save_scan_res(res_base_path, file_path, out, err):
    rel_path = file_path.replace(f"{work_path}", "")
    base, ext = os.path.splitext(rel_path)
    ext_map = {".c": "-c", ".h": "-h", ".cpp": "-cpp"}
    suffix = ext_map.get(ext, ext)
    file_res_path = os.path.join(res_base_path, base.lstrip(os.sep) + suffix + ".txt")
    os.makedirs(os.path.dirname(file_res_path), exist_ok=True)
    logger.info("Save scan result to: %s" % file_res_path)
    with open(file_res_path, "w") as f:
        f.write(err)
        f.write(out)
    return file_res_path


def write_csv(file_path, data):
    try:
        with open(file_path, "w") as f:
            writer = csv.writer(f)
            writer.writerows(data)
    except Exception as ex:
        raise Exception(
            "Failed to write the csv file: {} with msg: {}".format(file_path, repr(ex))
        )


def scan_one_file(file):
    cmd = f"clang-query-16 -p {compile_commands_path} {file} -f {clang_scan_rules_path} 2>&1 | grep -v 'error:' | grep -v 'warning:'"
    logger.debug(f"cmd:{cmd}")
    try:
        stdout, stderr = CommandExecutor().execute(cmd)
        lines = stdout.split("\n")
        scan_valid = len(lines) >= 2 and (
            lines[-2].endswith("matches.") or lines[-2].endswith("match.")
        )
        if scan_valid:
            match_num = int(lines[-2].split(" ")[0])
            logger.info("The match lines of file %s: %s" % (file, match_num))
            this_file_res_path = save_scan_res(log_file_path, file, stdout, stderr)
            return [
                file,
                this_file_res_path,
                match_num,
                "Pass" if match_num == 0 else "Fail",
            ]
        else:
            logger.warning("The result of scan is invalid for: %s" % file)
            this_file_res_path = save_scan_res(log_file_path, file, stdout, stderr)
            return [file, this_file_res_path, 0, "Invalid"]
    except Exception as e:
        logger.error("Execute command failed: %s" % e)
        return [file, "", 0, "Error"]


if __name__ == "__main__":
    command_executor = CommandExecutor()
    # get all the c files path
    # scan_files_path("/root/TDinternal/community/source/")
    # input_files(change_file_list)
    # print(f"all_file_path:{all_file_path}")
    res = []
    web_path = []
    res.append(["scan_source_file", "scan_result_file", "match_num", "check_result"])
    # create dir
    # current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    # scan_result_path = os.path.join(scan_result_base_path, current_time)
    # scan_result_path = scan_result_base_path
    # if not os.path.exists(scan_result_path):
    #     os.makedirs(scan_result_path)

    # 优先用 -d 指定目录，否则用 -f 文件列表，否则默认目录
    if change_file_list:
        input_files(change_file_list)
    elif scan_dir:
        scan_files_path(scan_dir)
    else:
        for sub_dir in scan_dir_list:
            abs_dir = os.path.join(TD_project_path, "community", sub_dir)
            print(abs_dir)
            if os.path.exists(abs_dir):
                scan_files_path(abs_dir)

    all_file_path = list(set(all_file_path))
    # 多进程并发扫描
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        results = list(executor.map(scan_one_file, all_file_path))
    res.extend(results)

    # for file in all_file_path:
    #     cmd = f"clang-query-16 -p {compile_commands_path} {file} -f {clang_scan_rules_path} 2>&1 | grep -v 'error:' | grep -v 'warning:'"
    #     logger.debug(f"cmd:{cmd}")
    #     try:
    #         stdout, stderr = command_executor.execute(cmd)
    #         print(stderr)
    #         lines = stdout.split("\n")
    #         scan_valid = len(lines) >= 2 and (lines[-2].endswith("matches.") or lines[-2].endswith("match."))
    #         if scan_valid:
    #             match_num = int(lines[-2].split(" ")[0])
    #             logger.info("The match lines of file %s: %s" % (file, match_num))
    #             this_file_res_path = save_scan_res(log_file_path, file, stdout, stderr)
    #             if match_num > 0:
    #                 # logger.info(f"log_file_path: {log_file_path} ,file:{file}")
    #                 index_tests = this_file_res_path.find("scan_log")
    #                 if index_tests != -1:
    #                     web_path_file = this_file_res_path[index_tests:]
    #                     web_path_file = os.path.join(web_server, web_path_file)
    #                     web_path.append(web_path_file)
    #             res.append([file, this_file_res_path, match_num, 'Pass' if match_num == 0 else 'Fail'])
    #         else:
    #             logger.warning("The result of scan is invalid for: %s" % file)
    #             this_file_res_path = save_scan_res(log_file_path, file, stdout, stderr)
    #             res.append([file, this_file_res_path, 0, 'Invalid'])
    #     except Exception as e:
    #         logger.error("Execute command failed: %s" % e)
    #         this_file_res_path = ""
    #         res.append([file, this_file_res_path, 0, 'Error'])
    # data = ""
    # for item in res:
    #     data += item[0] + "," + str(item[1]) + "\n"
    # logger.info("Csv data: %s" % data)
    write_csv(os.path.join(log_file_path, "scan_res.txt"), res)
    scan_result_log = f"{log_file_path}/scan_res.txt"
    # delete the first element of res
    res.pop(0)
    logger.info("The result of scan: \n")
    logger.info("Total scan files: %s" % len(res))
    logger.info("Total match lines: %s" % sum([item[2] for item in res]))
    logger.info(f"scan log file : {scan_result_log}")
    logger.info("Pass files: %s" % len([item for item in res if item[3] == "Pass"]))
    logger.info("Fail files: %s" % len([item for item in res if item[3] == "Fail"]))
    fail_files = [item for item in res if item[3] in {'Fail', 'Invalid', 'Error'}]

    if len(fail_files) > 0:
        logger.error(f"Total failed files: {len(fail_files)}")
    else:
        logger.info("All files passed the scan.")
    
    if web_server:
        # 打印 web_path
        for index, fail_item in enumerate(fail_files):
            file_res_path = fail_item[1]
            index_tests = file_res_path.find("scan_log")
            if index_tests != -1:
                web_path_file = os.path.join(web_server, file_res_path[index_tests:])
            else:
                web_path_file = file_res_path
            logger.error(
                f"failed number: {index + 1}, failed_result_file: {web_path_file}"
            )
    else:
        # 打印本地路径
        for index, fail_item in enumerate(fail_files):
            logger.error(
                f"failed number: {index + 1}, failed_result_file: {fail_item[1]}"
            )

    if len(fail_files) > 0:
        logger.error(f"Scan failed, please check the log file: {scan_result_log}")
        sys.exit(1)