import requests
import time
import json
import os
from datetime import datetime

def get_repo_coverage(repo_name):
    """
    获取指定仓库的覆盖率信息
    :param repo_name: 仓库名称，例如 "taos-connector-rust"
    :return: 仓库名称和覆盖率
    """
    url = f"https://api.codecov.io/api/v2/gh/taosdata/repos/{repo_name}/report"
    headers = {
        "Authorization": "Bearer 8a306b8a-ecd6-4abb-8076-24f8a2aea906",  
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        coverage = data.get("totals", {}).get("coverage", "N/A")
        return repo_name, coverage
    else:
        print(f"请求失败，状态码: {response.status_code}")
        print(f"错误信息: {response.text}")
        return repo_name, 0  # 请求失败时将覆盖率设置为 0

def send_to_feishu(message, webhook_url):
    """
    将消息发送到飞书
    :param message: 要发送的消息内容
    :param webhook_url: 飞书 Webhook URL
    """
    headers = {
        "Content-Type": "application/json"
    }
    payload = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": "TDengine 各连接器覆盖率信息",
                    "content": [
                        [
                            {"tag": "text", "text": message}
                        ]
                    ]
                }
            }
        }
    }
    response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        print("消息已成功发送到飞书")
    else:
        print(f"发送消息失败，状态码: {response.status_code}")
        print(f"错误信息: {response.text}")


def send_to_feishu_alarm(message, send_to_feishu_url, notifier,coverage_result_url,owner):
    """
    发送消息到飞书
    :param send_to_feishu_url: 飞书机器人webhook地址
    :param message: 消息内容
    :param coverage_result_url: 覆盖率结果url
    """
    headers = {"Content-Type": "application/json"}
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    data = {
        "msg_type": "interactive",
        "card": {
            "type": "template",
            "data": 
                {
                "template_id": "AAqR3L31FLVa8",
                "template_version_name": "1.0.2",
                "template_variable": {
                "title": "TDengine 各连接器覆盖率信息",
                "tag": "text",
                "message": message,
                "notifier":notifier,
                "coverage_result_url": coverage_result_url,
                "current_date": current_date,
                "author": owner,
                "bg_color": "green"
            }
        }
    }
    }
    
    print("发送到飞书的数据:", json.dumps(data, ensure_ascii=False, indent=4))

    response = requests.post(send_to_feishu_url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        print("消息发送成功")
        print("飞书响应:", response.text)
    else:
        print(
            f"消息发送失败，错误代码: {response.status_code}, 错误信息：{response.text}"
        )


if __name__ == "__main__":
    repo_list = [
        "taos-connector-jdbc",
        "taos-connector-rust",
        "flink-connector-tdengine",
        "taos-connector-odbc",
        "driver-go",
        "taos-connector-dotnet",
        "taosadapter",
        "taos-connector-node",
        "taos-connector-python",
        "kafka-connect-tdengine"
    ]

    # 加载现有的 JSON 文件
    json_file_path = "coverage_results.json"
    if os.path.exists(json_file_path):
        with open(json_file_path, "r", encoding="utf-8") as json_file:
            previous_results = json.load(json_file)
    else:
        previous_results = {}

    results = {}  # 用于存储当前运行结果的字典
    alerts = []  # 用于存储告警信息
    message = "\n \n"  # 用于存储飞书消息内容

    print("开始获取覆盖率信息...")
    for repo_name in repo_list:
        try:
            name, coverage = get_repo_coverage(repo_name)
            # 将覆盖率值转换为字符串并添加 "%" 符号
            current_coverage = float(coverage) if coverage != "N/A" else 0
            previous_coverage = float(previous_results.get(name, "0%").strip("%"))

            if current_coverage > previous_coverage:
                improvement = current_coverage - previous_coverage
                results[name] = f"{current_coverage}%"
                alerts.append(
                    f"仓库{name}: 最新覆盖率 {current_coverage}% 创下新的最高覆盖率记录！"
                    f" 提高了 {improvement:.2f}%"
                )
            elif current_coverage < previous_coverage:
                results[name] = f"{previous_coverage}%"
                difference = previous_coverage - current_coverage
                alerts.append(
                    f"仓库{name}: 最新覆盖率 {current_coverage}% 低于之前最高的覆盖率 {previous_coverage}%，"
                    f"还差 {difference:.2f}%"
                )
            else:
                # 覆盖率保持不变时，仅记录结果，不生成告警
                results[name] = f"{current_coverage}%"

            # 如果覆盖率为 0，添加到告警信息
            if current_coverage == 0:
                alerts.append(f"仓库{name}: 当前的覆盖率为 0%，请检查！")

            # 构造消息内容
            message += (
                f"仓库名称: {name}, 最新覆盖率: {current_coverage}%, "
                f"历史最高覆盖率记录: {results[name]}\n"
            )

            print(f"仓库名称: {name}, 最新覆盖率: {current_coverage}%, 历史最高覆盖率记录: {results[name]}")
            time.sleep(1)  # 避免触发 API 速率限制
        except Exception as e:
            print(f"处理仓库 {repo_name} 时出错: {e}")
            results[repo_name] = "0%"  # 异常时也将覆盖率设置为 "0%"
            alerts.append(f"仓库{repo_name}: 当前的覆盖率为 0%，请检查！（请求失败或异常）")

    # 将结果写入 JSON 文件
    with open(json_file_path, "w", encoding="utf-8") as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=4)

    print("覆盖率信息已保存到 coverage_results.json 文件中。")

    # 添加告警信息到消息内容
    if alerts:
        message += "\n告警信息：\n" + "\n".join(alerts)

    # 发送消息到飞书
    coverage_result_url = "https://app.codecov.io/github/taosdata"
    if alerts:
        # 有告警时发送到告警群
        feishu_webhook_url_alarm_online = "https://open.feishu.cn/open-apis/bot/v2/hook/02363732-91f1-49c4-879c-4e98cf31a5f3"
        feishu_webhook_url_alarm_test = "https://open.feishu.cn/open-apis/bot/v2/hook/11e9e452-34a0-4c88-b014-10e21cb521dd"
        author = "lhhuo@taosdata.com"
        owner = "guoxy"
        send_to_feishu_alarm(message, feishu_webhook_url_alarm_online, author, coverage_result_url, owner)
    else:
        # 无告警时发送到普通群
        feishu_webhook_url_normal_online = "https://open.feishu.cn/open-apis/bot/v2/hook/56c333b5-eae9-4c18-b0b6-7e4b7174f5c9"
        feishu_webhook_url_normal_test = "https://open.feishu.cn/open-apis/bot/v2/hook/c2bebe49-2cc1-4566-81ce-893d029aefb1"
        author = "xyguo@taosdata.com"
        owner = "guoxy"
        send_to_feishu_alarm(message, feishu_webhook_url_normal_online, author, coverage_result_url, owner)

