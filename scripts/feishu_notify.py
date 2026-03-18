#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书通知脚本 - 精简版
用法：
  python3 feishu_notify.py --from-file case_status.txt
  python3 feishu_notify.py --build-failed --branch main --build 123
"""

import argparse
import os
import socket
import sys
from datetime import datetime

import requests

# 飞书机器人
NOTIFY_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/56c333b5-eae9-4c18-b0b6-7e4b7174f5c9"
ALERT_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/02363732-91f1-49c4-879c-4e98cf31a5f3"


def send(title: str, content: str, alert: bool = False):
    """发送飞书消息"""
    payload = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": title,
                    "content": [[{"tag": "text", "text": content}]]
                }
            }
        }
    }
    
    headers = {"Content-Type": "application/json"}
    
    # 成功只发通知群，失败同时发告警群
    urls = [NOTIFY_URL]
    if alert:
        urls.append(ALERT_URL)
    
    for url in urls:
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=10)
            ok = r.json().get("StatusCode") == 0
            print(f"{'✓' if ok else '✗'} {url[:50]}...")
        except Exception as e:
            print(f"✗ Error: {e}")


def parse_status_file(filepath: str) -> tuple:
    """解析 case_status.txt，返回 (title, content, is_failed)"""
    # 读取文件到字典
    data = {}
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if ':' not in line:
                continue
            key, val = line.strip().split(':', 1)
            data[key.strip()] = val.strip()
    
    # 字段映射：Detail -> Status
    field_map = {
        'Result': 'Result',
        'Owner': 'Owner',
        'Start time': 'Start time',
        'End time': 'End time',
        'Detail': 'Status',  # 关键映射
        'Scope': 'Scope',
        'Commit(enterprise)': 'Commit(enterprise)',
        'Commit(community)': 'Commit(community)',
        'Log dir': 'Log dir',
    }
    
    # 组装内容（保持和原 feishuTalk 一样的格式）
    lines = [f"Result: {data.get('Result', 'unknown')}", ""]
    lines.append("Details")
    
    for src, dst in field_map.items():
        if src == 'Result':
            continue
        val = data.get(src, '')
        lines.append(f"{dst}: {val}")
    
    # 添加 Hostname
    lines.append(f"Hostname: {socket.gethostname()}")
    lines.append("Others: ")
    
    # 确定标题
    result = data.get('Result', 'unknown')
    owner = data.get('Owner', '')
    if 'Query' in owner:
        title = "TestNG-Query Monitor"
    elif 'Insert' in owner:
        title = "TestNG-Insert Monitor"
    else:
        title = "TestNG Report"
    
    return title, "\n".join(lines), result == 'failed'


def build_failed_msg(branch: str, build: str) -> tuple:
    """生成编译失败消息"""
    now = datetime.now().strftime("%Y_%m%d_%H%M%S")
    hostname = socket.gethostname()
    
    # Log 指向 enterprise 下的构建日志（new_ver_release.sh 的 tee 输出）
    internal_root = os.getenv("INTERNAL_ROOT", "/var/data/jenkins/workspace/TDinternal")
    log_file = f"{internal_root}/enterprise/ver-3.0.0.100.txt"
    
    lines = [
        "Result: failed",
        "",
        "Details",
        "Owner: Platform TSDB-Build",
        f"Build time: {now}",
        "Status: tsdb build 失败",
        f"Scope: {branch} , buildNumber-[{build}]",
        f"Hostname: {hostname}",
        f"Log dir: {log_file}",
        "Others: ",
    ]
    
    return "🚨 TDengine 编译失败", "\n".join(lines), True


def main():
    parser = argparse.ArgumentParser(description="飞书通知")
    parser.add_argument("--from-file", help="从状态文件读取")
    parser.add_argument("--build-failed", action="store_true", help="编译失败通知")
    parser.add_argument("--branch", default="unknown")
    parser.add_argument("--build", default="unknown")
    
    args = parser.parse_args()
    
    if args.from_file:
        title, content, is_failed = parse_status_file(args.from_file)
    elif args.build_failed:
        title, content, is_failed = build_failed_msg(args.branch, args.build)
    else:
        print("Usage: python3 feishu_notify.py --from-file case_status.txt")
        print("       python3 feishu_notify.py --build-failed --branch main --build 123")
        sys.exit(1)
    
    send(title, content, alert=is_failed)


if __name__ == "__main__":
    main()
