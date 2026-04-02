
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
# group_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/7e409a8e-4390-4043-80d0-4e0dd2cbae7d'
test_url = (
    "https://open.feishu.cn/open-apis/bot/v2/hook/7e409a8e-4390-4043-80d0-4e0dd2cbae7d"
)
notification_robot_url = (
    "https://open.feishu.cn/open-apis/bot/v2/hook/56c333b5-eae9-4c18-b0b6-7e4b7174f5c9"
)
alert_robot_url = (
    "https://open.feishu.cn/open-apis/bot/v2/hook/02363732-91f1-49c4-879c-4e98cf31a5f3"
)

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
                    "title": "TestNG-Query Monitor",
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
                    "title": "TestNG-Insert Monitor",
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
    
def send_msg(url, result, result_detail, test_scope, owner, hostname, start_time, end_time, community_commit_id, enterprise_commit_id, log_dir, others):
#def send_msg(url: str, data: dict):
    text = f'''
Result: {result}\n
Details
Owner: {owner}
Start time: {start_time}
End time: {end_time}
Status: {result_detail}
Scope: {test_scope}
Hostname: {hostname}
Commit(enterprise): {enterprise_commit_id}
Commit(community): {community_commit_id}
Log dir: {log_dir}
Others: {others}\n
    '''

    #json = get_msg(text)
    if "Query" in owner:
        json = get_query_msg(text)
    else:
        json = get_insert_stream_msg(text)
    headers = {
        'Content-Type': 'application/json'
    }

    req = requests.post(url=url, headers=headers, json=json)
    inf = req.json()
    if "StatusCode" in inf and inf["StatusCode"] == 0:
        pass
    else:
        print(inf)

def read_file_to_dict(filename):
    data_dict = {}
    with open(filename, "r", encoding="utf-8") as file:
        lines = file.readlines()
        for line in lines:
            key, value = line.strip().split(":", 1)
            data_dict[key] = value
    return data_dict

def main():
    inputfile = "case_status.txt"
    logfile = open("case_status.txt","r",encoding="utf-8")
    lines = logfile.readlines(1000)
    for line in lines:
        line=line.strip('\n').split(':')
        # print(f"{line}")
        if line[0] == "Result":
            Result = line[1]
        elif line[0] == "Detail":
            Detail = line[1]
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
        if Result == "success":
            send_msg(url=notification_robot_url, result=Result, result_detail=Detail, test_scope=Scope, owner=Owner, hostname=hostname, start_time=Start_time, end_time=End_time, enterprise_commit_id=enterprise_commit_id, community_commit_id=community_commit_id, log_dir=Log_dir, others="")
        else:
            send_msg(url=notification_robot_url, result=Result, result_detail=Detail, test_scope=Scope, owner=Owner, hostname=hostname, start_time=Start_time, end_time=End_time, enterprise_commit_id=enterprise_commit_id, community_commit_id=community_commit_id, log_dir=Log_dir, others="")
            send_msg(url=alert_robot_url, result=Result, result_detail=Detail, test_scope=Scope, owner=Owner, hostname=hostname, start_time=Start_time, end_time=End_time, enterprise_commit_id=enterprise_commit_id, community_commit_id=community_commit_id, log_dir=Log_dir, others="")
        #send_msg(result=Result, result_detail=Result_detail, test_scope=Scope, owner=Owner, hostname=hostname, start_time=Start_time, end_time=End_time, enterprise_commit_id=enterprise_commit_id, community_commit_id=community_commit_id, log_dir=Log_dir, others="")
    except Exception as e:
        print("exception:", e)
    exit(1)
    
    
    # data = read_file_to_dict(inputfile)
    # hostname = socket.gethostname() 
    # try:
    #     if data["result"] == "success":
    #         send_msg(notification_robot_url, data)
    #     else:
    #         send_msg(alert_robot_url, data)
    # except Exception as e:
    #     print("exception:", e)
    #     exit(1)


if __name__ == '__main__':
    main()
