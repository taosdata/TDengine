import os
import sys
import subprocess
import csv
from datetime import datetime
from loguru import logger
import getopt

opts, args = getopt.gnu_getopt(sys.argv[1:], 'b:f:', [
    'branch_name='])
for key, value in opts:
    if key in ['-h', '--help']:
        print(
            'Usage: python3 scan.py -b <branch_name> -f <file_list>')
        print('-b  branch name or PR ID to scan')
        print('-f  change files list')

        sys.exit(0)

    if key in ['-b', '--branchName']:
        branch_name = value
    if key in ['-f', '--filesName']:
        change_file_list = value


# the base source code file path
self_path =  os.path.dirname(os.path.realpath(__file__))

if ("community" in self_path):
    source_path = self_path[:self_path.find("community")]
    work_path = source_path[:source_path.find("TDinternal")]

else:
    source_path = self_path[:self_path.find("tests")]
    work_path = source_path[:source_path.find("TDengine")]

# Check if "community"  or "tests" is in self_path
index_community = self_path.find("community")
if index_community != -1:
    source_path = self_path[:index_community]
    index_TDinternal = source_path.find("TDinternal")
    # Check if index_TDinternal is valid and set work_path accordingly
    if index_TDinternal != -1:
        work_path = source_path[:index_TDinternal]
else:
    index_tests = self_path.find("tests")
    if index_tests != -1:
        source_path = self_path[:index_tests]
    # Check if index_TDengine is valid and set work_path accordingly
        index_TDengine = source_path.find("TDengine")
        if index_TDengine != -1:
            work_path = source_path[:index_TDengine]


# log file path
log_file_path = f"{source_path}/../log/{branch_name}/"
os.makedirs(log_file_path, exist_ok=True)

scan_log_file = f"{log_file_path}/scan.log"
logger.add(scan_log_file, rotation="10MB", retention="7 days", level="DEBUG")
print(self_path,work_path,source_path,log_file_path)

# scan result base path
scan_result_base_path = f"{log_file_path}/clang_scan_result/"


# the compile commands json file path
compile_commands_path = f"{source_path}/debugNoSan/compile_commands.json"
sed_command = r"sed -i 's/home/var\\lib\\jenkins\\workspace/g' compile_commands.json"
result = subprocess.run(sed_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
logger.debug(f"STDOUT: {result.stdout} STDERR: {result.stderr}")
# compile_commands_path = f"{source_path}/debug/compile_commands.json"

# the ast parser rule for c file
clang_scan_rules_path = f"{self_path}/filter_for_return_values"

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
    scan_skip_file_list = [f"{source_path}/TDinternal/community/tools/taosws-rs/target/release/build/openssl-sys-7811e597b848e397/out/openssl-build/install/include/openssl", "/test/", "contrib", "debug", "deps", f"{source_path}/TDinternal/community/source/libs/parser/src/sql.c", f"{source_path}/TDinternal/community/source/client/jni/windows/win32/bridge/AccessBridgeCalls.c"]
    with open(change_files, 'r') as file:
        for line in file:
            file_name = line.strip()
            if any(dir_name in file_name for dir_name in scan_dir_list):
                if (file_name.endswith(".c") or file_name.endswith(".h") or line.endswith(".cpp")) and all(dir_name not in file_name for dir_name in scan_skip_file_list):
                    if "enterprise" in file_name:
                        file_name = os.path.join(source_path, file_name)
                    else: 
                        tdc_file_path = os.path.join(source_path, "community/")
                        file_name = os.path.join(tdc_file_path, file_name)                    
                    all_file_path.append(file_name)
                    print(f"all_file_path:{all_file_path}")
    # for file_path in change_files:
    #     if (file_path.endswith(".c") or file_path.endswith(".h") or file_path.endswith(".cpp")) and all(item not in file_path for item in scan_skip_file_list):
    #         all_file_path.append(file_path)
    logger.info("Found %s files" % len(all_file_path))
file_res_path = ""

def save_scan_res(res_base_path, file_path, out, err):
    global file_res_path
    file_res_path = os.path.join(res_base_path, file_path.replace(f"{work_path}", "").split(".")[0] + ".res")
    print(f"file_res_path:{file_res_path},res_base_path:{res_base_path},file_path:{file_path}")
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
    # scan_files_path(source_path)
    input_files(change_file_list)
    print(f"all_file_path:{all_file_path}")
    res = []
    res.append(["scan_source_file", "scan_result_file", "match_num", "check_result"])
    # create dir
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    scan_result_path = os.path.join(scan_result_base_path, current_time)
    if not os.path.exists(scan_result_path):
        os.makedirs(scan_result_path)
    for file in all_file_path:

        cmd = f"clang-query-10 -p {compile_commands_path} {file} -f {clang_scan_rules_path}"
        print(f"cmd:{cmd}")
        try:
            stdout, stderr = command_executor.execute(cmd)
            lines = stdout.split("\n")
            if lines[-2].endswith("matches.") or lines[-2].endswith("match."): 
                match_num = int(lines[-2].split(" ")[0])
                logger.info("The match lines of file %s: %s" % (file, match_num))
                if match_num > 0:
                    logger.info(f"scan_result_path: {scan_result_path} ,file:{file}")
                    save_scan_res(scan_result_path, file, stdout, stderr)
                res.append([file, file_res_path, match_num, 'Pass' if match_num == 0 else 'Fail'])
            else:
                logger.warning("The result of scan is invalid for: %s" % file)            
        except Exception as e:
            logger.error("Execute command failed: %s" % e)
    # data = ""
    # for item in res:
    #     data += item[0] + "," + str(item[1]) + "\n"
    # logger.info("Csv data: %s" % data)
    write_csv(os.path.join(scan_result_path, "scan_res.csv"), res)
    scan_result_log = f"{scan_result_path}/scan_res.csv"
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
        exit(1)