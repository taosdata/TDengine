import requests
import subprocess

"""This file is used to send notice to feishu robot with daily arm64 ci test result
"""
group_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/56c333b5-eae9-4c18-b0b6-7e4b7174f5c9'

def get_msg(text):
    return {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": "ARM64 CI test report",
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

def send_msg(result, testScope, owner, hostname, logDir, others):
    text = f'''result: {result}
    test scope: {testScope}
    owner: {owner}
    hostname: {hostname}
    log dir: {logDir}
    others: {others}\n'''

    json = get_msg(text)
    headers = {
        'Content-Type': 'application/json'
    }

    req = requests.post(url=group_url, headers=headers, json=json)
    inf = req.json()
    if "StatusCode" in inf and inf["StatusCode"] == 0:
        pass
    else:
        print(inf)

def check_build_failed():
    path = "/home/arm64/log/"
    res = False
    try:
        r = subprocess.Popen("ls | wc -l", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=path)
        stdout, stderr = r.communicate()
        if r.returncode != 0:
            print("Failed to execute 'ls -l' for path '{}' with error: {}".format(path, stderr))
        else:
            if int(stdout) == 0:
                res = True
        return res
    except Exception as ex:
        raise Exception("Failed to check build failed with error: {}".format(str(ex)))

def check_run_case_status():
    path = "/home/arm64/log/*/stat.txt"
    r = subprocess.Popen("cat {}".format(path), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = r.communicate()
    if r.returncode != 0:
        print("Failed to execute 'cat {}' with error: {}".format(path, stderr))
    else:
        for line in stdout.decode().split("\n"):
            if "Failed:" in line:
                failed_case_num = int(line.split(":")[1].strip())
                if 0 == failed_case_num:
                    return "All cases passed"
                else:
                    return stdout.decode()

def send_notice():
    if check_build_failed():
        send_msg("Build failed", "arm64_ci_test", "charles", "ecs-3e78", "/home/arm64/log", "arm 64 ci build failed")
    run_res = check_run_case_status()
    if "All cases passed" != run_res:
        send_msg(run_res, "arm64_ci_test", "charles", "ecs-3e78", "/home/arm64/log", "arm 64 ci run case failed")

if __name__ == '__main__':
    send_notice()
