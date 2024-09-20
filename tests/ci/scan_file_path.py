import os
import sys
import subprocess
import csv
from datetime import datetime
from loguru import logger
import getopt


opts, args = getopt.gnu_getopt(sys.argv[1:], 'b:f:w:', [
    'branch_name='])
for key, value in opts:
    if key in ['-h', '--help']:
        print(
            'Usage: python3 scan.py -b <branch_name> -f <file_list>')
        print('-b  branch name or PR ID to scan')
        print('-f  change files list')
        print('-w  web server')

        sys.exit(0)

    if key in ['-b', '--branchName']:
        branch_name = value
    if key in ['-f', '--filesName']:
        change_file_list = value
    if key in ['-w', '--webServer']:
        web_server = value


# the base source code file path
self_path =  os.path.dirname(os.path.realpath(__file__))

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
    index_TDinternal = TD_project_path.find("TDinternal")
    # Check if index_TDinternal is valid and set work_path accordingly
    if index_TDinternal != -1:
        work_path = TD_project_path[:index_TDinternal]
else:
    index_tests = self_path.find("tests")
    if index_tests != -1:
        TD_project_path = self_path[:index_tests]
    # Check if index_TDengine is valid and set work_path accordingly
        index_TDengine = TD_project_path.find("TDengine")
        if index_TDengine != -1:
            work_path = TD_project_path[:index_TDengine]


# log file path
current_time = datetime.now().strftime("%Y%m%d-%H%M%S")
log_file_path = f"{work_path}/scan_log/scan_{branch_name}_{current_time}/"

os.makedirs(log_file_path, exist_ok=True)

scan_log_file = f"{log_file_path}/scan_log.txt"
logger.add(scan_log_file, rotation="10MB", retention="7 days", level="DEBUG")
#if error happens, open this to debug
# print(self_path,work_path,TD_project_path,log_file_path,change_file_list)

# scan result base path
scan_result_base_path = f"{log_file_path}/clang_scan_result/"


# the compile commands json file path
# compile_commands_path = f"{work_path}/debugNoSan/compile_commands.json"
compile_commands_path = f"{TD_project_path}/debug/compile_commands.json"

#if error happens, open this to debug
# print(f"compile_commands_path:{compile_commands_path}")

# # replace the docerk worf path with real work path in compile_commands.json
# docker_work_path = "home"
# replace_path= work_path[1:-1]
# replace_path = replace_path.replace("/", "\/")
# sed_command = f"sed -i 's/{docker_work_path}/{replace_path}/g' {compile_commands_path}"
# print(sed_command)
# result = subprocess.run(sed_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
# logger.debug(f"STDOUT: {result.stdout} STDERR: {result.stderr}")

# the ast parser rule for c file
clang_scan_rules_path = f"{self_path}/filter_for_return_values"

#
# all the c files path will be checked
all_file_path = []

class CommandExecutor:
    def __init__(self):
        self._process = None

    def execute(self, command, timeout=None):
        try:
            self._process = subprocess.Popen(command,
                                            shell=True,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE)
            stdout, stderr = self._process.communicate(timeout=timeout)
            return stdout.decode('utf-8'), stderr.decode('utf-8')
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.communicate()
            raise Exception("Command execution timeout")
        except Exception as e:
            raise Exception("Command execution failed: %s" % e)

def scan_files_path(source_file_path):
    # scan_dir_list = ["source", "include", "docs/examples", "tests/script/api", "src/plugins"]
    scan_dir_list = ["source", "include", "docs/examples", "src/plugins"]
    scan_skip_file_list = ["/root/charles/TDinternal/community/tools/taosws-rs/target/release/build/openssl-sys-7811e597b848e397/out/openssl-build/install/include/openssl",
                           "/test/", "contrib", "debug", "deps", "/root/charles/TDinternal/community/source/libs/parser/src/sql.c", "/root/charles/TDinternal/community/source/client/jni/windows/win32/bridge/AccessBridgeCalls.c"]
    for root, dirs, files in os.walk(source_file_path):
        for file in files:
            if any(item in root for item in scan_dir_list):
                file_path = os.path.join(root, file)
                if (file_path.endswith(".c") or file_path.endswith(".h") or file_path.endswith(".cpp")) and all(item not in file_path for item in scan_skip_file_list):
                    all_file_path.append(file_path)
    logger.info("Found %s files" % len(all_file_path))

def input_files(change_files):
    # scan_dir_list = ["source", "include", "docs/examples", "tests/script/api", "src/plugins"]
    scan_dir_list = ["source", "include", "docs/examples", "src/plugins"]
    scan_skip_file_list = [f"{TD_project_path}/TDinternal/community/tools/taosws-rs/target/release/build/openssl-sys-7811e597b848e397/out/openssl-build/install/include/openssl", "/test/", "contrib", "debug", "deps", f"{TD_project_path}/TDinternal/community/source/libs/parser/src/sql.c",f"{TD_project_path}/TDinternal/community/source/libs/azure/",f"{TD_project_path}/TDinternal/community/source/client/jni/windows/win32/bridge/AccessBridgeCalls.c"]
    with open(change_files, 'r') as file:
        for line in file:
            file_name = line.strip()
            if any(dir_name in file_name for dir_name in scan_dir_list):
                if (file_name.endswith(".c")  or file_name.endswith(".cpp")) and all(dir_name not in file_name for dir_name in scan_skip_file_list):
                    if "enterprise" in file_name:
                        file_name = os.path.join(TD_project_path, file_name)
                    else: 
                        tdc_file_path = os.path.join(TD_project_path, "community/")
                        file_name = os.path.join(tdc_file_path, file_name)                    
                    all_file_path.append(file_name)
                    # print(f"all_file_path:{all_file_path}")
    logger.info("Found %s files" % len(all_file_path))
file_res_path = ""

def save_scan_res(res_base_path, file_path, out, err):
    global file_res_path
    file_res_path = os.path.join(res_base_path, file_path.replace(f"{work_path}", "").split(".")[0] + ".txt")
    # print(f"file_res_path:{file_res_path},res_base_path:{res_base_path},file_path:{file_path}")
    if not os.path.exists(os.path.dirname(file_res_path)):
        os.makedirs(os.path.dirname(file_res_path))
    logger.info("Save scan result to: %s" % file_res_path)
    
    # save scan result
    with open(file_res_path, "w") as f:
        f.write(err)
        f.write(out)
    logger.debug(f"file_res_file: {file_res_path}")

def write_csv(file_path, data):
    try:
        with open(file_path, 'w') as f:
            writer = csv.writer(f)
            writer.writerows(data)
    except Exception as ex:
        raise Exception("Failed to write the csv file: {} with msg: {}".format(file_path, repr(ex)))

if __name__ == "__main__":
    command_executor = CommandExecutor()
    # get all the c files path
    # scan_files_path(TD_project_path)
    input_files(change_file_list)
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

    for file in all_file_path:
        cmd = f"clang-query-16 -p {compile_commands_path} {file} -f {clang_scan_rules_path}"
        logger.debug(f"cmd:{cmd}")
        try:
            stdout, stderr = command_executor.execute(cmd)
            #if "error" in stderr:
            print(stderr)
            lines = stdout.split("\n")
            if lines[-2].endswith("matches.") or lines[-2].endswith("match."): 
                match_num = int(lines[-2].split(" ")[0])
                logger.info("The match lines of file %s: %s" % (file, match_num))
                if match_num > 0:
                    logger.info(f"log_file_path: {log_file_path} ,file:{file}")
                    save_scan_res(log_file_path, file, stdout, stderr)
                    index_tests = file_res_path.find("scan_log")
                    if index_tests != -1:
                        web_path_file = file_res_path[index_tests:]
                        web_path_file = os.path.join(web_server, web_path_file)
                        web_path.append(web_path_file)
                res.append([file, file_res_path, match_num, 'Pass' if match_num == 0 else 'Fail'])
                
            else:
                logger.warning("The result of scan is invalid for: %s" % file)            
        except Exception as e:
            logger.error("Execute command failed: %s" % e)
    # data = ""
    # for item in res:
    #     data += item[0] + "," + str(item[1]) + "\n"
    # logger.info("Csv data: %s" % data)
    write_csv(os.path.join(log_file_path, "scan_res.txt"), res)
    scan_result_log = f"{log_file_path}/scan_res.txt"
    # delete the first element of res
    res= res[1:]
    logger.info("The result of scan: \n")
    logger.info("Total scan files: %s" % len(res))
    logger.info("Total match lines: %s" % sum([item[2] for item in res]))
    logger.info(f"scan log file : {scan_result_log}")
    logger.info("Pass files: %s" % len([item for item in res if item[3] == 'Pass']))
    logger.info("Fail files: %s" % len([item for item in res if item[3] == 'Fail']))
    if len([item for item in res if item[3] == 'Fail']) > 0:
        logger.error(f"Scan failed,please check the log file:{scan_result_log}")
        for index, failed_result_file in enumerate(web_path):
            logger.error(f"failed number: {index}, failed_result_file: {failed_result_file}")
        exit(1)
