# -*- coding: utf-8 -*-
import requests
import os
import sys
import subprocess
import getopt
import re
import json
import socket
import datetime
import getpass
class Msg:
    def __init__(self):
        # crash_gen warn group
        self.group_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/56c333b5-eae9-4c18-b0b6-7e4b7174f5c9'
        self.owner = "Jayden Jia"
        self.test_scope = "stream stability test"
        # disk fill warn group
        # self.group_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/14cc4cf2-0b84-4ca2-8577-a46f7530559d'

    def get_msg(self, text):
        return {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": "test report",
                        "content": [
                            [{
                                "tag": "text",
                                "text": text
                            }
                            ]]
                    }
                }
            }
        }

    def send_msg(self, json):
        headers = {
        'Content-Type': 'application/json'
    }
        req = requests.post(url=self.group_url, headers=headers, json=json)
        inf = req.json()
        if "StatusCode" in inf and inf["StatusCode"] == 0:
            pass
        else:
            print(inf)

class TaosBenchmark:
    def __init__(self):
        pass

    def replace_host(self, json_file, taosd_host):
        with open(json_file, "r") as f:
            json_info = json.load(f)
            if taosd_host != "":
                json_info["host"] = taosd_host

        with open(json_file, "w") as f:
            json.dump(json_info, f)

        with open(json_info["result_file"], "w") as f:
            f.truncate()

        return json_info

    def confirm_res(self, result_file):
        error_msg_list = ["Unable to establish connection", "Unable to resolve FQDN", "Port already in use", "Conn is broken", "Conn read timeout", "some vnode/qnode/mnode(s) out of service", "rpc open too many session", "No enough disk space"]
        with open(result_file, "r") as f:
            files = f.read()
            pat = r"insert delay,.*|Spent.*insert.*"
            success_pat = re.findall(pat, files)
            if len(success_pat) > 0:
                return "success"
            else:
                for error_msg in error_msg_list:
                    if error_msg in str(files):
                        return f'fail ({error_msg})'
                    else:
                        return "fail (other errors)"

if __name__ == "__main__":
    opts, args = getopt.gnu_getopt(sys.argv[1:], 'j:t:p:s', ['json_file=', 'taosd_host=', 'src_path', 'help'])
    hostname_list = list()
    hostname_list.append(socket.gethostname())
    src_path = "/root/TDengine"
    for key, value in opts:
        if key in ['-h', '--help']:
            print('-j taosBenchmark json')
            print('-t taosd host')
            print('-s TDengine src path')
            sys.exit(0)
        if key in ['-j', '--json_file']:
            taosBenchmark_json_file_name = value
        if key in ['-t', '--taosd_host']:
            taosd_host = value
        if key in ['-s', '--src_path']:
            src_path = value
    msg = Msg()
    taosBenchmark = TaosBenchmark()
    exec_cmd = "python3 " + ' '.join(sys.argv[::])
    taosBenchmark_run_cmd = f"taosBenchmark -f {taosBenchmark_json_file_name}"
    json_info = taosBenchmark.replace_host(taosBenchmark_json_file_name, taosd_host)
    start_time = datetime.datetime.now()
    os.popen(taosBenchmark_run_cmd).read()
    end_time = datetime.datetime.now()
    res_msg = taosBenchmark.confirm_res(json_info["result_file"])
    if taosd_host != "" and taosd_host != socket.gethostname():
        hostname_list.append(taosd_host)
    git_commit = subprocess.Popen(f"cd {src_path} && git log | head -n1", shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")[7:16]
    log_dir = f'{getpass.getuser()}@{hostname_list[0]}:{json_info["result_file"]}'
    core_dir = subprocess.Popen("cat /proc/sys/kernel/core_pattern", shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8").strip()
    text = f'''result: {res_msg}
test scope: {msg.test_scope}
owner: {msg.owner}
hostname: {",".join(hostname_list)}
start time: {start_time}
end time: {end_time}
git commit :  {git_commit}
log dir: {log_dir}
core dir: {core_dir}
cmd: {exec_cmd}
others: none'''
    # print(text)

    msg.send_msg(msg.get_msg(text))