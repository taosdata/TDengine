
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
import csv
from datetime import datetime

# crash_gen warn group
group_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/56c333b5-eae9-4c18-b0b6-7e4b7174f5c9'

# # disk fill warn group
# group_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/14cc4cf2-0b84-4ca2-8577-a46f7530559d'

def get_msg(text):
    return {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": "Fulltest report",
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

def send_msg(result, result_detail, test_scope, owner, hostname, start_time, end_time, community_commit_id, enterprise_commit_id, log_dir, others):
    text = f'''result: {result}
    result_detail: {result_detail}
    test scope: {test_scope}
    owner: {owner}
    hostname: {hostname}
    start time: {start_time}
    end time: {end_time}
    enterprise commit: {enterprise_commit_id}
    community commit: {community_commit_id}
    log dir: {log_dir}
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
    inputfile = "case_status.txt"
    logfile = open("case_status.txt","r",encoding="utf-8")
    lines = logfile.readlines(1000)
    for line in lines:
        line=line.strip('\n').split(':')
        # print(f"{line}")
        if line[0] == "result":
            result = line[1]
        elif line[0] == "result_detail":
            result_detail = line[1]
        elif line[0] == "test_scope":
            test_scope = line[1]
        elif line[0] == "owner" :
            owner = line[1]
        elif line[0] == "start_time":
            start_time = line[1]
        elif line[0] == "end_time":
            end_time = line[1]
        elif line[0] == "enterprise_commit_id":
            enterprise_commit_id = line[1]
        elif line[0] == "community_commit_id":
            community_commit_id = line[1]
        elif line[0] == "log_dir":
            log_dir = line[1]
        else:
            print("read all file")
    hostname = socket.gethostname()   
    try:
        send_msg(result=result, result_detail=result_detail, test_scope=test_scope, owner=owner, hostname=hostname, start_time=start_time, end_time=end_time, enterprise_commit_id=enterprise_commit_id, community_commit_id=community_commit_id, log_dir=log_dir, others="")
    except Exception as e:
        print("exception:", e)
    exit(1)


if __name__ == '__main__':
    main()