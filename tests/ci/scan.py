import os
import subprocess
import csv
from datetime import datetime
from loguru import logger

# log file path
log_file_path = "/root/charles/scan.log"
logger.add(log_file_path, rotation="10MB", retention="7 days", level="DEBUG")
# scan result base path
scan_result_base_path = "/root/charles/clang_scan_result/"
# the base source code file path
source_path = "/root/charles/TDinternal/"
# the compile commands json file path
compile_commands_path = "/root/charles/TDinternal/debug/compile_commands.json"
# the ast parser rule for c file
clang_scan_rules_path = "/root/charles/clang_scan_rules"
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

def save_scan_res(res_base_path, file_path, out, err):
    file_res_path = os.path.join(res_base_path, file_path.replace("/root/charles/", "").split(".")[0] + ".res")
    if not os.path.exists(os.path.dirname(file_res_path)):
        os.makedirs(os.path.dirname(file_res_path))
    logger.info("Save scan result to: %s" % file_res_path)
    # save scan result
    with open(file_res_path, "w") as f:
        f.write(out)
        f.write(err)

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
    scan_files_path(source_path)
    res = []
    # create dir
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    scan_result_path = os.path.join(scan_result_base_path, current_time)
    if not os.path.exists(scan_result_path):
        os.makedirs(scan_result_path)
    for file in all_file_path:
        cmd = "clang-query -p %s %s -f %s" % (compile_commands_path, file, clang_scan_rules_path)
        try:
            stdout, stderr = command_executor.execute(cmd)
            lines = stdout.split("\n")
            if lines[-2].endswith("matches.") or lines[-2].endswith("match."): 
                match_num = int(lines[-2].split(" ")[0])
                logger.info("The match lines of file %s: %s" % (file, match_num))
                if match_num > 0:
                    save_scan_res(scan_result_path, file, stdout, stderr)
                res.append([file, match_num, 'Pass' if match_num == 0 else 'Fail'])
            else:
                logger.warning("The result of scan is invalid for: %s" % file)            
        except Exception as e:
            logger.error("Execute command failed: %s" % e)
    # data = ""
    # for item in res:
    #     data += item[0] + "," + str(item[1]) + "\n"
    # logger.info("Csv data: %s" % data)
    write_csv(os.path.join(scan_result_path, "scan_res.csv"), res)
    logger.info("The result of scan: \n")
    logger.info("Total files: %s" % len(res))
    logger.info("Total match lines: %s" % sum([item[1] for item in res]))
    logger.info("Pass files: %s" % len([item for item in res if item[2] == 'Pass']))
    logger.info("Fail files: %s" % len([item for item in res if item[2] == 'Fail']))

