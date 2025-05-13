import requests
import time
import json
import os
from datetime import datetime
from bs4 import BeautifulSoup

def get_coverage_data():
    """
    获取覆盖率数据
        """
    try:
        # 1. 获取JSON数据
        url = "https://coveralls.io/github/taosdata/TDengine?branch=3.0"
        response = requests.get(url)
        response.raise_for_status()  # 确保请求成功
    except requests.exceptions.RequestException as e:
        # 将网站访问异常也作为一种告警返回
        return [{
            "alerts": [f"覆盖率网站访问异常: {str(e)}"],
            "coverage_result_url": "https://coveralls.io/github/taosdata/TDengine?branch=3.0"
        }]
    except Exception as e:
        return [{
            "alerts": [f"获取覆盖率数据时发生错误: {str(e)}"],
            "coverage_result_url": "https://coveralls.io/github/taosdata/TDengine?branch=3.0"
        }]

    # 检查响应内容是否为空
    if not response.content:
        raise ValueError("响应内容为空，请检查URL或网络连接")

    # 解析HTML数据
    soup = BeautifulSoup(response.text, "html.parser")
    
    # 3. 提取覆盖率url链接  
    builds = soup.find_all("div", class_="show-item")
    for build in builds:
        coverage_result_url_element = build.find("a", class_="btn btn-micro")
        if coverage_result_url_element:
            coverage_result_url = coverage_result_url_element.get("href")
            coverage_result_url = "https://coveralls.io" + coverage_result_url
            print("coverage_result_url = ", coverage_result_url)
            break
        

    # 提取覆盖率所需数据
    coverage_data = []
    alerts = []  # 添加告警列表
    builds = soup.find_all("div", class_="show-item")
    for build in builds:
        build_text = build.get_text(strip=True)
        if (
            "Build #" in build_text
            and "branch: " in build_text
            and "Commit Message" in build_text
            and "coverage: " in build_text
        ):
            try:
                build_number = build_text.split("Build #")[1].split("Build")[0]
                print("build_number = ", build_number)
                coverage_branch = build_text.split("branch: ")[1].split("CHANGE")[0]
                print("coverage_branch = ", coverage_branch)
                commit_message = (
                    build_text.split("Commit Message")[1]
                    .split("Run Details")[0]
                    .replace("\n", " ")
                )
                print("commit_message = ", commit_message)
                coverage_number = build_text.split("coverage: ")[1].split(" ")[0]
                print("coverage_number = ", coverage_number)
                # 添加覆盖率判断逻辑
                try:
                    coverage_value = float(coverage_number.strip('%'))
                    if coverage_value == 0:
                        alerts.append(f"TDengine 主仓库覆盖率为 0%，构建号：{build_number}，分支：{coverage_branch}，请检查！")
                except ValueError:
                    print(f"无法解析覆盖率数值: {coverage_number}")                
                
                created_time = build_text.split("Committed")[1].split("coverage:")[0]
                print("created_time = ", created_time)

                coverage_data.append(
                    {
                        "build_number": build_number,
                        "coverage_branch": coverage_branch,
                        "commit_message": commit_message,
                        "coverage_number": coverage_number,
                        "created_time": created_time,
                        "coverage_result_url": coverage_result_url,
                        "alerts": alerts  # 将告警信息添加到返回数据中
                    }
                )
                print(coverage_data)
                break  # 只处理第一条数据
            except IndexError as e:
                print(f"解析 build_text 时出错: {e}======={build_text}===")
                continue

    # 5. 返回coverage_data
    return coverage_data


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
                "template_id": "AAqj7BO8oMQYF",
                "template_version_name": "1.0.2",
                "template_variable": {
                "title": "Taosd && taosc coverage report",
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
    print("开始获取覆盖率信息...")
    # 获取 TDengine 主仓库覆盖率数据
    tdengine_coverage = get_coverage_data()
    
    # 根据覆盖率数据确定使用哪个 webhook URL
    if tdengine_coverage and tdengine_coverage[0].get('alerts'):
        # 有告警时使用告警 webhook
        feishu_webhook_url = "https://open.feishu.cn/open-apis/bot/v2/hook/11e9e452-34a0-4c88-b014-10e21cb521dd"
        feishu_webhook_url_alarm_test = "https://open.feishu.cn/open-apis/bot/v2/hook/11e9e452-34a0-4c88-b014-10e21cb521dd"
        feishu_webhook_url_alarm_online = "https://open.feishu.cn/open-apis/bot/v2/hook/02363732-91f1-49c4-879c-4e98cf31a5f3"
        author = "xyguo@taosdata.com"
    else:
        # 无告警时使用普通 webhook
        feishu_webhook_url = "https://open.feishu.cn/open-apis/bot/v2/hook/c2bebe49-2cc1-4566-81ce-893d029aefb1"
        feishu_webhook_url_alarm_test = "https://open.feishu.cn/open-apis/bot/v2/hook/c2bebe49-2cc1-4566-81ce-893d029aefb1" 
        feishu_webhook_url_alarm_online = "https://open.feishu.cn/open-apis/bot/v2/hook/56c333b5-eae9-4c18-b0b6-7e4b7174f5c9"
        author = "xyguo@taosdata.com"

    owner = "guoxy"
    # 直接使用 get_coverage_data 返回的告警信息
    message = "\n".join(tdengine_coverage[0].get('alerts', [])) if tdengine_coverage else "无告警信息"
    coverage_result_url = tdengine_coverage[0].get('coverage_result_url') if tdengine_coverage else "https://app.codecov.io/github/taosdata"
    
    send_to_feishu_alarm(message, feishu_webhook_url, author, coverage_result_url, owner)