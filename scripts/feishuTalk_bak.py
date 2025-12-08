
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
# group_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/56c333b5-eae9-4c18-b0b6-7e4b7174f5c9'

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

def get_query_msg(text):
    return {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": "TestNG-Query Test Notification",
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
    
def get_insert_stream_msg(text):
    return {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": "TestNG-Insert&Stream Test Notification",
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
    text = f'''Result: {result}\n\n
    Result_detail: {result_detail}
    Scope: {test_scope}
    Owner: {owner}
    Hostname: {hostname}
    Start time: {start_time}
    End time: {end_time}
    Commit(enterprise): {enterprise_commit_id}
    Commit(community): {community_commit_id}
    Log dir: {log_dir}
    Others: {others}\n'''

    #json = get_msg(text)
    if owner == "Platform TSDB Test":
        json = get_query_msg(text)
    else:    
        json = get_insert_stream_msg(text)
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
        if line[0] == "Result":
            Result = line[1]
        elif line[0] == "Result_detail":
            Result_detail = line[1]
        elif line[0] == "Scope":
            Scope = line[1]
        elif line[0] == "Owner" :
            Owner = line[1]
        elif line[0] == "Start time":
            Start_time = line[1]
        elif line[0] == "End time":
            End_time = line[1]
        elif line[0] == "Commit(enterprise)":
            enterprise_commit_id = line[1]
        elif line[0] == "Commit(community)":
            community_commit_id = line[1]
        elif line[0] == "Log dir":
            Log_dir = line[1]
        else:
            print("read all file")
    hostname = socket.gethostname()   
    try:
        send_msg(result=Result, result_detail=Result_detail, test_scope=Scope, owner=Owner, hostname=hostname, start_time=Start_time, end_time=End_time, enterprise_commit_id=enterprise_commit_id, community_commit_id=community_commit_id, log_dir=Log_dir, others="")
    except Exception as e:
        print("exception:", e)
    exit(1)


if __name__ == '__main__':
    main()
