from datetime import date
from datetime import timedelta
import os
import re
import requests
from dotenv import load_dotenv

# load .env
load_dotenv()

# define version
version = "3.3.*"

ip = os.getenv("EXCLUDE_IP")
server_ip = os.getenv("SERVER_IP")
owner = os.getenv("OWNER")

# feishu-msg url
feishu_msg_url = os.getenv("FEISHU_MSG_URL")

today = date.today()
#today = date(2023,8,7)
path="/data/telemetry/crash-report/"

# get files for the past 7 days
def get_files():
    files = ""
    for i in range(1,8):
        #print ((today - timedelta(days=i)).strftime("%Y%m%d"))
        files = files + path + (today - timedelta(days=i)).strftime("%Y%m%d") + ".txt "

    return files

# for none-taosAssertDebug
filter1_cmd = '''grep '"version":"%s"'  %s \
| grep  "taosd(" \
| awk -F "stackInfo" '{print $2}' \
| grep -v "taosAssertDebug" \
| grep -v %s \
| awk -F "taosd" '{print $3}' \
| cut -d")" -f 1 \
| cut -d"(" -f 2 \
| sort | uniq -c ''' % (version, get_files(), ip)

# for taosAssertDebug
filter2_cmd = '''grep '"version":"%s"'  %s \
| grep  "taosd(" \
| awk -F "stackInfo" '{print $2}' \
| grep "taosAssertDebug" \
| grep -v %s \
| awk -F "taosd" '{print $3}' \
| cut -d")" -f 1 \
| cut -d"(" -f 2 \
| sort | uniq -c ''' % (version, get_files(), ip)

# get msg info
def get_msg(text):
    return {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": "Telemetry Statistics",
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

# post msg
def send_msg(json):
    headers = {
        'Content-Type': 'application/json'
    }

    req = requests.post(url=group_url, headers=headers, json=json)
    inf = req.json()
    if "StatusCode" in inf and inf["StatusCode"] == 0:
        pass
    else:
        print(inf)

# exec cmd and return res
def get_output(cmd):
    text = os.popen(cmd)
    lines = text.read()
    text.close()
    return lines

# get sum
def get_count(output):
    res = re.findall(" \d+ ", output)
    sum1 = 0
    for r in res:
        sum1 = sum1 + int(r.strip())
    return sum1

# print total crash count
def print_result():
    #print(f"Files for statistics: {get_files()}\n")
    sum1 = get_count(get_output(filter1_cmd))
    sum2 = get_count(get_output(filter2_cmd))
    total = sum1 + sum2
    #print(f"total crashes: {total}")
    return total

# send report to feishu
def send_report():
    content = f'''
    test scope: Telemetry Statistics
    owner: {owner}
    ip: {server_ip}
    from: {get_files().split(" ")[6].split("/")[4].split(".")[0]}
    to: {get_files().split(" ")[0].split("/")[4].split(".")[0]}
    filter1 result: {get_output(filter1_cmd)}
    filter2 result: {get_output(filter2_cmd)}
    total crashes: {print_result()}
    '''
    #send_msg(get_msg(content))
    print(content)

print_result()
send_report()
