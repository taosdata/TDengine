
# -*- coding: utf-8 -*-
import os
import socket
import requests
import sys
import random
import argparse
import subprocess
import time
import platform
from datetime import datetime

# crash_gen warn group
#group_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/56c333b5-eae9-4c18-b0b6-7e4b7174f5c9'

# disk fill warn group
group_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/14cc4cf2-0b84-4ca2-8577-a46f7530559d'

def get_msg(text):
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

def send_msg(result, testScope, owner, hostname, startTime, endTime, gitCommit, logDir, others):
    text = f'''result: {result}
    test scope: {testScope}
    owner: {owner}
    hostname: {hostname}
    start time: {startTime}
    end time: {endTime}
    git commit: {gitCommit}
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


def main():
    result = 'success'
    testScope= 'query'
    owner = 'Platform TSDB Test'
    hostname = socket.gethostname()    
    startTime =  datetime.now()
    gitCommit = 'baf098267f4083fb53b56'
    logDir = '/root/taos-test-framework/TestNG/run/Query/queryscript'
    #cmd = 'nohup taostest --use=query_stability_local3_64.yaml --case=Query/queryscript/stable_query/split/stable_query_union_1.py --keep --disable_collection &'
    cmd = '/root/nohup3_py.sh' 
    os.system(cmd)
    endTime = datetime.now()

    try:
        send_msg(result, testScope, owner, hostname, startTime, endTime, gitCommit, logDir, cmd)
    except Exception as e:
        print("exception:", e)
    exit(1)


if __name__ == '__main__':
    main()
