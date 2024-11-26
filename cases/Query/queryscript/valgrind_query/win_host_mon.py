###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import subprocess
import requests
import json
import time

def send_to_feishu(send_to_feishu_url, message):
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "msg_type": "text",
        "content": {
            "text": message
        }
    }
    response = requests.post(send_to_feishu_url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        print("消息发送成功")
    else:
        print(f"消息发送失败，错误代码: {response.status_code}, 错误信息：{response.text}")

def ping_host(host):
    try:
        output = subprocess.check_output(["ping", "-c", "1", host], stderr=subprocess.STDOUT, universal_newlines=True)
        return True
    except subprocess.CalledProcessError:
        return False

def monitor_hosts(hosts, send_to_feishu_url):
    while True:
        for host in hosts:
            if not ping_host(host):
                message = f"无法 ping 通主机: {host}"
                print(message)
                send_to_feishu(send_to_feishu_url, message)
        time.sleep(60)  # 每分钟检查一次

if __name__ == "__main__":
    
    # 获取飞书 Webhook URL
    send_to_feishu_url = "https://open.feishu.cn/open-apis/bot/v2/hook/7e409a8e-4390-4043-80d0-4e0dd2cbae7d"
    
    # 监控主机
    host_to_monitor = ["192.168.0.191", "192.168.0.192", "192.168.0.193", "192.168.0.194"]
    monitor_hosts(host_to_monitor, send_to_feishu_url)