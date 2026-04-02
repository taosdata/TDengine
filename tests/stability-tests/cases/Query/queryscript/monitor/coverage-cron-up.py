import requests
from datetime import datetime
import glob
import json


# 替换为你的实际凭证和表格信息
feishu_head = "Testing coverage failed log"

send_to_feishu_url = (
    #覆盖率crash-gen群
    "https://open.feishu.cn/open-apis/bot/v2/hook/c2bebe49-2cc1-4566-81ce-893d029aefb1"
)

def extract_failed_info(log_file_paths):
    failed_info = []
    for log_file_path in log_file_paths:
        with open(log_file_path, 'r') as file:
            for line in file:
                if 'failed' in line:
                    failed_info.append(line.strip())
    return failed_info


def send_to_feishu(send_to_feishu_url, message):
    """
    发送消息到飞书
    :param send_to_feishu_url: 飞书机器人webhook地址
    :param message: 消息内容
    """
    headers = {"Content-Type": "application/json"}
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


    data = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": feishu_head,
                    "content": [
                        [
                            {
                                "tag": "text",
                                "text": f"Result: {message}\n\n Time: {current_date}\n",
                            }
                        ]
                    ],
                }
            }
        },
    }

    response = requests.post(send_to_feishu_url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        print("消息发送成功")
    else:
        print(
            f"消息发送失败，错误代码: {response.status_code}, 错误信息：{response.text}"
        )


if __name__ == "__main__":
    log_file_paths = glob.glob('/root/coverage_test_2*.log')
    
    failed_info = extract_failed_info(log_file_paths)
    if failed_info:
        message = "\n".join(failed_info)
        print(message)
        send_to_feishu(send_to_feishu_url, message)
    else:
        print("没有找到包含 'failed' 的信息")