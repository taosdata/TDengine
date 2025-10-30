import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import sys
from datetime import datetime
import glob
import argparse

# 替换为你的实际凭证和表格信息
author = "guoxy"

coverage_url = "https://coveralls.io/github/taosdata/TDengine?branch=cover/3.0"

feishu_head = "Testing coverage report for taosd/taosc"

send_to_feishu_notify_url_test = (
    #覆盖率Platform notify群-test
    "https://open.feishu.cn/open-apis/bot/v2/hook/11e9e452-34a0-4c88-b014-10e21cb521dd"
)

send_to_feishu_alert_url_test = (
    #覆盖率Platform Alerts群-test
    "https://open.feishu.cn/open-apis/bot/v2/hook/c2bebe49-2cc1-4566-81ce-893d029aefb1"
)

send_to_feishu_notify_url = (
    #覆盖率Platform notify群-true
    "https://open.feishu.cn/open-apis/bot/v2/hook/56c333b5-eae9-4c18-b0b6-7e4b7174f5c9"
)

send_to_feishu_alert_url = (
    #覆盖率Platform Alerts群-true
    "https://open.feishu.cn/open-apis/bot/v2/hook/02363732-91f1-49c4-879c-4e98cf31a5f3"
)

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='TDengine覆盖率告警脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python3 tdengine_coveage_alarm.py                    # 正常模式
  python3 tdengine_coveage_alarm.py --test-mode        # 测试模式
  python3 tdengine_coveage_alarm.py --preview          # 预览模式
  python3 tdengine_coveage_alarm.py --data-clean       # 启用数据清洗
  python3 tdengine_coveage_alarm.py -p -clean -test    # 组合使用
  python3 tdengine_coveage_alarm.py -url https://coveralls.io/jobs/172828865  # 外部使用
        """
    )
    
    parser.add_argument('--test-mode', '-test', 
                       action='store_true', 
                       help='使用测试模式（发送到测试群组）')
    
    parser.add_argument('--data-clean', '-clean', 
                       action='store_true',
                       help='启用数据清洗，过滤异常波动的覆盖率数据')
    
    parser.add_argument('--outlier-threshold', '-threshold', 
                       type=float, 
                       default=3.0,
                       metavar='FLOAT',
                       help='异常值阈值（百分点），默认3.0%%')
    
    parser.add_argument('--preview', '-p', 
                       action='store_true',
                       help='预览模式：只打印最新5条覆盖率数据并发送到测试告警群')
    
    parser.add_argument('-url', 
                       type=str,
                       help='获取指定URL的详细覆盖率信息，返回JSON格式')
    
    # 添加版本信息
    parser.add_argument('--version', '-v', 
                       action='version', 
                       version='TDengine覆盖率告警脚本 v1.0')
    
    return parser.parse_args()

# 修改URL选择逻辑
def get_webhook_urls(test_mode=False):
    """根据模式获取对应的webhook URLs"""
    if test_mode:
        return {
            'notify': send_to_feishu_notify_url_test,
            'alert': send_to_feishu_alert_url_test
        }
    else:
        return {
            'notify': send_to_feishu_notify_url,
            'alert': send_to_feishu_alert_url
        }

def get_coverage_data():
    """
    获取Recent builds表格中的所有覆盖率数据
    return: 返回按时间排序的覆盖率数据列表，包含完整的commit信息和详细链接
    """
    url = coverage_url
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    
    coverage_data = []
    
    # 查找 Recent builds 表格
    table = None
    
    # 方法1: 查找包含 Builds、Branch、Commit 等列标题的表格
    tables = soup.find_all("table")
    for t in tables:
        header_row = t.find("tr")
        if header_row:
            header_text = header_row.get_text()
            if "Builds" in header_text and "Coverage" in header_text:
                table = t
                break
    
    # 方法2: 如果没找到，尝试查找class包含builds相关的表格
    if not table:
        table = soup.find("table", class_=lambda x: x and ("builds" in x.lower() or "recent" in x.lower()))
    
    # 方法3: 查找所有表格，取最可能的一个
    if not table and tables:
        table = tables[0]  # 通常第一个表格就是builds表格
    
    if not table:
        print("未找到 Recent builds 表格")
        return coverage_data
    
    print(f"找到表格，开始解析...")
    
    # 解析表格行（跳过表头）
    rows = table.find_all("tr")[1:]  # 跳过第一行表头
    
    print(f"找到 {len(rows)} 条构建记录")
    
    for idx, row in enumerate(rows):
        try:
            cells = row.find_all(["td", "th"])
            if len(cells) < 8:  # 确保有足够的列
                continue
                
            # 根据表格结构解析：
            # Builds	Branch	Commit	Type	Ran	Committer	Via	Coverage
            
            # 1. Build Number 和详细链接
            build_cell = cells[0]
            build_number = build_cell.get_text(strip=True).replace("#", "")
            
            # 获取Build详细链接 (类似 https://coveralls.io/builds/76002268)
            build_detail_url = ""
            link_element = build_cell.find("a")
            if link_element:
                href = link_element.get("href")
                if href:
                    if href.startswith("/"):
                        build_detail_url = "https://coveralls.io" + href
                    else:
                        build_detail_url = href
            
            # 2. Branch
            coverage_branch = cells[1].get_text(strip=True)
            
            # 3. Commit - 获取完整的commit信息
            commit_cell = cells[2]
            commit_message = commit_cell.get_text(strip=True)
            
            # 4. 其他信息
            build_type = cells[3].get_text(strip=True)
            created_time = cells[4].get_text(strip=True)
            committer = cells[5].get_text(strip=True)
            via = cells[6].get_text(strip=True)
            coverage_number = cells[7].get_text(strip=True)
            
            # 确保coverage_number包含%符号
            if "%" not in coverage_number:
                coverage_number = coverage_number + "%"
            
            coverage_data.append({
                "build_number": build_number,
                "build_detail_url": build_detail_url,  # 新增：Build详细链接
                "coverage_branch": coverage_branch,
                "commit_message": commit_message,      # 完整的commit信息
                "coverage_number": coverage_number,
                "created_time": created_time,
                "build_type": build_type,
                "committer": committer,
                "via": via,
                "index": idx,  # 添加索引，0表示最新的
                "coverage_result_url": build_detail_url  # 保持兼容性，与build_detail_url相同
            })
            
            print(f"记录 {idx + 1}: Build #{build_number}, 覆盖率: {coverage_number}, 时间: {created_time}")
            print(f"    Commit: {commit_message[:200]}{'...' if len(commit_message) > 200 else ''}")
            print(f"    详细链接: {build_detail_url}")
            
        except (IndexError, AttributeError) as e:
            print(f"解析第 {idx + 1} 条记录时出错: {e}")
            continue

    print(f"成功解析 {len(coverage_data)} 条覆盖率记录")
    return coverage_data


def clean_coverage_data(coverage_data, outlier_threshold=5.0):
    """
    清洗覆盖率数据，移除异常波动的数据点
    
    策略：
    1. 移除55%以下的无效数据
    2. 使用移动平均检测异常值
    3. 使用IQR方法检测离群值
    
    :param coverage_data: 原始覆盖率数据列表
    :param outlier_threshold: 异常值阈值（百分点）
    :return: 清洗后的数据列表
    """
    if len(coverage_data) < 3:
        return coverage_data
    
    print(f"🧹 开始数据清洗，异常值阈值: {outlier_threshold}%")
    
    # 提取覆盖率数值
    coverage_values = []
    valid_data = []
    
    for data in coverage_data:
        coverage_num = float(data['coverage_number'].replace('%', ''))
        # 策略1: 移除55%以下的无效数据
        if coverage_num > 55.0:
            coverage_values.append(coverage_num)
            valid_data.append(data)
        else:
            print(f"🚫 移除低覆盖率数据: Build #{data['build_number']} ({coverage_num}%) - 低于55%阈值")
    
    if len(valid_data) < 3:
        print("⚠️ 有效数据不足3条，跳过清洗")
        return coverage_data
    
    print(f"📊 原始数据: {len(coverage_data)} 条，有效数据: {len(valid_data)} 条")
    
    # 策略2: 使用移动平均检测异常值
    cleaned_data = []
    
    for i, data in enumerate(valid_data):
        current_value = coverage_values[i]
        
        # 获取前后窗口的数据进行平滑
        window_start = max(0, i - 2)
        window_end = min(len(coverage_values), i + 3)
        window_values = coverage_values[window_start:window_end]
        
        # 计算窗口内的中位数和标准差
        if len(window_values) >= 3:
            window_median = sorted(window_values)[len(window_values)//2]
            window_mean = sum(window_values) / len(window_values)
            
            # 计算与中位数的偏差
            deviation_from_median = abs(current_value - window_median)
            deviation_from_mean = abs(current_value - window_mean)
            
            # 如果偏差超过阈值，标记为异常
            if deviation_from_median > outlier_threshold and deviation_from_mean > outlier_threshold:
                print(f"🚫 检测到异常值: Build #{data['build_number']} ({current_value}%) - 偏差中位数 {deviation_from_median:.2f}%, 偏差均值 {deviation_from_mean:.2f}%")
                continue
        
        cleaned_data.append(data)
    
    # 打印清洗后的数据概览
    if cleaned_data:
        print("📋 清洗后的数据:")
        for i, data in enumerate(cleaned_data[:10]):  # 只显示前10条
            print(f"   {i+1}. Build #{data['build_number']}: {data['coverage_number']}")
    
    return cleaned_data

def find_stable_comparison_data(coverage_data, current_data, threshold=3.0):
    """
    寻找稳定的对比数据，避免与异常值对比
    
    :param coverage_data: 覆盖率数据列表
    :param current_data: 当前数据
    :param threshold: 稳定性阈值
    :return: 最适合对比的历史数据
    """
    if len(coverage_data) < 2:
        return None
    
    current_value = float(current_data['coverage_number'].replace('%', ''))
    
    # 寻找最近的稳定数据点
    for i in range(1, min(len(coverage_data), 5)):  # 检查最近5条记录
        candidate = coverage_data[i]
        candidate_value = float(candidate['coverage_number'].replace('%', ''))
        
        # 如果找到相对稳定的数据点，使用它
        deviation = abs(current_value - candidate_value)
        if deviation <= threshold:
            print(f"📊 找到稳定对比数据: Build #{candidate['build_number']} ({candidate['coverage_number']}) - 偏差 {deviation:.2f}%")
            return candidate
    
    # 如果没找到稳定的，使用最近的有效数据
    return coverage_data[1] if len(coverage_data) > 1 else None

def extract_failed_info(log_file_paths):
    """提取过滤后的失败测试用例"""
    failed_info = []
    for log_file_path in log_file_paths:
        with open(log_file_path, 'r') as file:
            for line in file:
                line = line.strip()
                # 检查行是否以 "case" 开头并且包含 "failed"
                if line.startswith("cases") and  "failed" in line:
                    if ('taostest' not in line ):
                        # 'passed' not in line and 
                        # 'warning' not in line and 
                        # 'ql' not in line and 
                        #'0;31m failed' not in line ):
                        failed_info.append(line.strip())
                # if ('failed' in line and 
                #     'taostest' not in line and 
                #     'passed' not in line and 
                #     'warning' not in line and 
                #     'output' not in line and 
                #     'args' not in line and 
                #     'Total' not in line and 
                #     'Exception' not in line and 
                #     'stream' not in line and 
                #     'tsma' not in line and 
                #     'cases/' not in line and 
                #     'ql' not in line and 
                #     #'valgrind' not in line and 
                #     '31m failed' not in line ):
                #     failed_info.append(line.strip())
                    
                    # # 使用正则表达式匹配文件路径（.py/.sim 结尾）
                    # match = re.match(r'.*?/(.*?\.py|.*?\.sim)\b', line)
                    # if match:
                    #     test_case = match.group(1).lstrip('/')
                    #     failed_info.append(test_case)
    return failed_info


def compare_cells(data1, data2, webhook_urls):
    """
    对比两次结果
    :param data1: 当前数据
    :param data2: 上一次数据
    :param webhook_urls: webhook URLs字典
    """

    data1_1 = float(str(data1).replace("%", "")) / 100
    data2_1 = float(str(data2).replace("%", "")) / 100

    
    # 🔧 修复：从文件读取历史最高覆盖率
    highest_coverage = get_highest_coverage()
    highest_coverage_value = highest_coverage  # 保持原始的历史最高值
    
    # 当前覆盖率（百分比形式）
    current_coverage_value = data1_1 * 100
 
    # 检查是否产生新的历史最高覆盖率
    if current_coverage_value > highest_coverage_value:
        print(f"🎉 新的覆盖率记录！当前覆盖率 {current_coverage_value:.3f}% 超过历史最高覆盖率 {highest_coverage_value:.3f}%")
        update_highest_coverage(current_coverage_value)
        # 🔧 更新后，highest_coverage_value 应该等于当前值
        highest_coverage_value = current_coverage_value
    else:
        difference = round(highest_coverage_value - current_coverage_value, 3)
        print(f"当前覆盖率 {current_coverage_value:.3f}% 离历史最高覆盖率 {highest_coverage_value:.3f}% 还差 {difference}%")

    log_file_paths = glob.glob('/root/coverage_test_2*.log')
    failed_info = extract_failed_info(log_file_paths)
    if failed_info:
        fail_case_message = "\n".join(failed_info)
        print(fail_case_message)
    else:
        fail_case_message = 'NULL'
        print("没有找到包含 'failed' 的信息")
        
    # 🔧 修复：所有返回都使用正确的 highest_coverage_value（历史最高值）
    if data1_1 < 0.56:
        message = " Taosd && taosc 代码覆盖率低于56%，请密切关注！" 
        notifier = "xyguo@taosdata.com"
        return message, notifier, highest_coverage_value, webhook_urls['alert']
    elif (data1_1 - data2_1) > 0 and (current_coverage_value > highest_coverage):
        # 注意：这里比较的是更新前的历史最高值
        message = f" Taosd && taosc 代码覆盖率由{data2}上升到{data1}，产生新的覆盖率记录，当前覆盖率为 {current_coverage_value:.3f}% 超过历史最高覆盖率 {highest_coverage:.3f}%，继续加油!失败的case:{fail_case_message}" 
        notifier = "slguan@taosdata.com"
        return message, notifier, highest_coverage_value, webhook_urls['alert']
    elif (data1_1 - data2_1) > 0 and (current_coverage_value <= highest_coverage):
        message = f" Taosd && taosc 代码覆盖率由{data2}上升到{data1}，当前覆盖率为 {current_coverage_value:.3f}% 离历史最高覆盖率 {highest_coverage_value:.3f}% 还差 {round(highest_coverage_value - current_coverage_value, 3)}%，继续加油!失败的case:{fail_case_message}" 
        notifier = "slguan@taosdata.com"
        return message, notifier, highest_coverage_value, webhook_urls['alert']
    elif (data1_1 - data2_1) < -0.003:
        message = f" Taosd && taosc 代码覆盖率由{data2}下降到{data1}，当前覆盖率为 {current_coverage_value:.3f}% 离历史最高覆盖率 {highest_coverage_value:.3f}% 还差 {round(highest_coverage_value - current_coverage_value, 3)}%，请关注!失败的case:{fail_case_message}" 
        notifier = "slguan@taosdata.com"
        return message, notifier, highest_coverage_value, webhook_urls['alert']
    else:
        message = f" Taosd && taosc 本次代码覆盖率基本不变（<0.3%），当前覆盖率为 {current_coverage_value:.3f}% 离历史最高覆盖率 {highest_coverage_value:.3f}% 还差 {round(highest_coverage_value - current_coverage_value, 3)}%，请继续保持!" 
        notifier = "xyguo@taosdata.com"
        return message, notifier, highest_coverage_value, webhook_urls['notify']
    
def compare_cells_alarm(data1, data2, current_data, webhook_urls):
    """
    对比两次结果并发送告警
    :param data1: 当前数据
    :param data2: 上一次数据  
    :param current_data: 当前记录的完整数据
    :param webhook_urls: webhook URLs字典
    """
    data1_1 = float(str(data1).replace("%", "")) / 100
    data2_1 = float(str(data2).replace("%", "")) / 100
   
    message, notifier, highest_coverage_thistime_value, webhook_url  = compare_cells(data1, data2, webhook_urls)
    
    print("message=", message)    
    print("notifier=", notifier)
    print("highest_coverage_thistime_value=", highest_coverage_thistime_value)
    print("webhook_url=", webhook_url)
    
    # 直接使用当前数据中的URL和commit信息
    coverage_result_url = current_data.get('coverage_result_url', '')
    coverage_git_commit = current_data.get('commit_message', '')

    # 判断是否需要发送告警
    if data1_1 < 0.56:
        print('覆盖率低于56%，发送紧急告警')
        send_to_feishu(webhook_url, message, notifier, coverage_result_url, coverage_git_commit)
    elif (data1_1 - data2_1) > 0 and (data1_1 * 100 > highest_coverage_thistime_value):
        print('覆盖率上升且产生新的覆盖率记录')
        send_to_feishu(webhook_url, message, notifier, coverage_result_url, coverage_git_commit)
    elif (data1_1 - data2_1) > 0 and (data1_1 * 100 <= highest_coverage_thistime_value):
        print('覆盖率上升但未产生新的覆盖率记录')
        send_to_feishu(webhook_url, message, notifier, coverage_result_url, coverage_git_commit)
    elif (data1_1 - data2_1) < -0.003:
        print('覆盖率下降超过0.5%，发送警告消息')
        send_to_feishu(webhook_url, message, notifier, coverage_result_url, coverage_git_commit)
    else:
        print('覆盖率保持不变，发送到Notify群')
        send_to_feishu(webhook_url, message, notifier, coverage_result_url, coverage_git_commit)


def send_to_feishu(send_to_feishu_url, message, notifier, coverage_result_url,coverage_git_commit):
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
                "coverage_git_commit": coverage_git_commit,
                "current_date": current_date,
                "author": author,
                "bg_color": "green"
            }
        }
    }
    }

    response = requests.post(send_to_feishu_url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        print("消息发送成功")
    else:
        print(
            f"消息发送失败，错误代码: {response.status_code}, 错误信息：{response.text}"
        )


def get_highest_coverage():
    """
    获取历史最高覆盖率
    """
    try:
        with open("highest_coverage.json", "r") as file:
            data = json.load(file)
            return data.get("highest_coverage", 0)
    except FileNotFoundError:
        return 0

def update_highest_coverage(new_coverage):
    """
    更新历史最高覆盖率
    """
    data = {"highest_coverage": new_coverage}
    with open("highest_coverage.json", "w") as file:
        json.dump(data, file)

def get_detailed_coverage_info(build_detail_url):
    """
    获取构建详细页面的关键覆盖率信息（简化版）
    :param build_detail_url: 构建详细页面URL
    :return: 详细覆盖率信息字符串
    """
    if not build_detail_url:
        return None
    
    try:
        response = requests.get(build_detail_url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "html.parser")
        text_content = soup.get_text()
        
        import re
        
        # 提取关键信息并格式化成一行
        details = []
        
        # 分支覆盖率: 155455 of 324369 branches covered (47.93%)
        branch_match = re.search(r'(\d+)\s+of\s+(\d+)\s+branches\s+covered\s+\(([\d.]+)%\)', text_content)
        if branch_match:
            details.append(f"{branch_match.group(1)} of {branch_match.group(2)} branches covered ({branch_match.group(3)}%)")
        
        # 🔧 修复：新增代码覆盖率的正则表达式，匹配单数和复数形式
        new_lines_match = re.search(r'(\d+)\s+of\s+(\d+)\s+new\s+or\s+added\s+lines?\s+(?:in\s+\d+\s+files?\s+)?covered[^(]*\(([\d.]+)%\)', text_content)
        if new_lines_match:
            details.append(f"{new_lines_match.group(1)} of {new_lines_match.group(2)} new or added line covered ({new_lines_match.group(3)}%)")
        
        # 未覆盖代码: 2521 existing lines in 113 files now uncovered
        uncovered_match = re.search(r'(\d+)\s+existing\s+lines\s+in\s+(\d+)\s+files?\s+now\s+uncovered', text_content)
        if uncovered_match:
            details.append(f"{uncovered_match.group(1)} existing lines in {uncovered_match.group(2)} files now uncovered")
        
        # 相关代码行: 207702 of 269535 relevant lines covered (77.06%)
        relevant_lines_match = re.search(r'(\d+)\s+of\s+(\d+)\s+relevant\s+lines\s+covered\s+\(([\d.]+)%\)', text_content)
        if relevant_lines_match:
            details.append(f"{relevant_lines_match.group(1)} of {relevant_lines_match.group(2)} relevant lines covered ({relevant_lines_match.group(3)}%)")
        
        # 命中数: 126661557.72 hits per line
        hits_match = re.search(r'([\d.]+)\s+hits\s+per\s+line', text_content)
        if hits_match:
            details.append(f"{hits_match.group(1)} hits per line")
        
        return ". ".join(details) if details else None
        
    except Exception as e:
        print(f"⚠️ 获取详细信息失败: {e}")
        return None

def send_preview_to_feishu(coverage_data, webhook_url):
    """
    发送覆盖率预览数据到飞书测试告警群（包含前3条的详细信息和链接）
    :param coverage_data: 最新的覆盖率数据列表
    :param webhook_url: 飞书webhook URL
    """
    if not coverage_data:
        print("❌ 没有覆盖率数据可预览")
        return
    
    # 构建预览消息 - 显示5条基本信息，前3条包含详细信息
    preview_data = coverage_data[:5]
    
    message_lines = ["📊 TDengine 最新覆盖率数据预览:\n"]
    
    for i, data in enumerate(preview_data):
        build_num = data['build_number']
        coverage = data['coverage_number']
        commit = data['commit_message'][:35] + "..." if len(data['commit_message']) > 35 else data['commit_message']
        created_time = data['created_time']
        detail_url = data.get('build_detail_url', '')
        
        # 添加趋势指示符
        if i == 0:
            trend = "🔥 最新"
        elif i < len(preview_data) - 1:
            current_val = float(coverage.replace('%', ''))
            next_val = float(preview_data[i + 1]['coverage_number'].replace('%', ''))
            if current_val > next_val:
                trend = "📈 上升"
            elif current_val < next_val:
                trend = "📉 下降"
            else:
                trend = "➡️ 持平"
        else:
            trend = "📋 基准"
        
        message_lines.append(f"{i+1}. Build #{build_num}: {coverage} {trend}")
        message_lines.append(f"   时间: {created_time}")
        message_lines.append(f"   提交: {commit}")
        
        # 🆕 添加详细链接
        if detail_url:
            message_lines.append(f"   🔗 详情: {detail_url}")
        
        # 🆕 为前3条记录添加详细信息（每类信息换行显示）
        if i < 3 and detail_url:
            detailed_info = get_detailed_coverage_info(detail_url)
            if detailed_info:
                message_lines.append(f"   📊 详细信息:")
                
                # 将详细信息按句号分割，每类信息换行显示
                detail_parts = detailed_info.split('. ')
                for part in detail_parts:
                    if part.strip():  # 确保不是空字符串
                        # 为每行详细信息添加适当的缩进
                        message_lines.append(f"      {part.strip()}")
        
        message_lines.append("")
    
    # 添加统计信息
    coverage_values = [float(d['coverage_number'].replace('%', '')) for d in preview_data]
    max_coverage = max(coverage_values)
    min_coverage = min(coverage_values)
    avg_coverage = sum(coverage_values) / len(coverage_values)
    
    message_lines.append(f"📈 统计信息:")
    message_lines.append(f"   最高: {max_coverage}% | 最低: {min_coverage}% | 平均: {avg_coverage:.2f}%")
    message_lines.append(f"   波动范围: {max_coverage - min_coverage:.2f}%")
    
    # 添加趋势分析
    if len(coverage_values) >= 2:
        recent_trend = coverage_values[0] - coverage_values[1]
        if abs(recent_trend) >= 1.0:
            if recent_trend > 0:
                message_lines.append(f"\n🚨 注意: 最新覆盖率较上次上升 {recent_trend:.2f}%")
            else:
                message_lines.append(f"\n⚠️ 警告: 最新覆盖率较上次下降 {abs(recent_trend):.2f}%")
        else:
            message_lines.append(f"\n✅ 覆盖率变化平稳 ({recent_trend:+.2f}%)")
    
    message = "\n".join(message_lines)
    
    print("📤 发送预览数据到飞书测试告警群...")
    
    # 发送到飞书
    headers = {"Content-Type": "application/json"}
    
    data = {
        "msg_type": "text",
        "content": {
            "text": message
        }
    }
    
    try:
        response = requests.post(webhook_url, headers=headers, data=json.dumps(data), timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("code") == 0:
                print("✅ 预览数据发送成功到测试告警群")
            else:
                print(f"❌ 飞书API返回错误: {result}")
        else:
            print(f"❌ HTTP请求失败: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"❌ 发送预览数据时出错: {e}")

def print_preview_data(coverage_data):
    """
    在控制台打印最新的5条覆盖率数据（显示前3条的详细信息，每类信息换行）
    :param coverage_data: 覆盖率数据列表
    """
    if not coverage_data:
        print("❌ 没有覆盖率数据可显示")
        return
    
    print("\n📊 TDengine 最新覆盖率数据预览:")
    print("="*120)
    
    preview_data = coverage_data[:5]
    
    for i, data in enumerate(preview_data):
        build_num = data['build_number']
        coverage = data['coverage_number']
        commit = data['commit_message']
        created_time = data['created_time']
        detail_url = data.get('build_detail_url', '')
        
        # 添加趋势标签
        if i == 0:
            trend = "🔥 最新"
        elif i < len(preview_data) - 1:
            current_val = float(coverage.replace('%', ''))
            next_val = float(preview_data[i + 1]['coverage_number'].replace('%', ''))
            if current_val > next_val:
                trend = "📈 上升"
            elif current_val < next_val:
                trend = "📉 下降"
            else:
                trend = "➡️ 持平"
        else:
            trend = "📋 基准"
        
        print(f"\n{i+1}. Build #{build_num}: {coverage} {trend}")
        print(f"   时间: {created_time}")
        print(f"   提交: {commit}")
        print(f"   详细链接: {detail_url}")
        
        # 🆕 为前3条记录获取详细信息（每类信息换行显示）
        if i < 3 and detail_url:
            print(f"   🔍 获取详细覆盖率信息...")
            detailed_info = get_detailed_coverage_info(detail_url)
            if detailed_info:
                print(f"   📊 详细信息:")
                
                # 将详细信息按句号分割，每类信息换行显示
                detail_parts = detailed_info.split('. ')
                for part in detail_parts:
                    if part.strip():  # 确保不是空字符串
                        print(f"      {part.strip()}")
            else:
                print(f"   ⚠️ 无法获取详细信息")
        
        print("-" * 120)


def get_build_time_from_url(url):
    """
    从构建页面获取构建时间
    :param url: 构建URL
    :return: 构建时间字符串，格式如 "16 Oct 2025 01:24PM UTC"
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 方法1: 查找时间相关的元素
        # 寻找包含时间信息的元素，可能的选择器：
        time_selectors = [
            'time',  # HTML5 time 元素
            '[datetime]',  # 有 datetime 属性的元素
            '.build-time',  # 可能的class名
            '.created-at',  # 可能的class名
            '.timestamp'    # 可能的class名
        ]
        
        for selector in time_selectors:
            time_element = soup.select_one(selector)
            if time_element:
                # 尝试从datetime属性获取
                datetime_attr = time_element.get('datetime')
                if datetime_attr:
                    # 转换ISO格式到所需格式
                    try:
                        dt = datetime.fromisoformat(datetime_attr.replace('Z', '+00:00'))
                        return dt.strftime("%d %b %Y %I:%M%p UTC")
                    except:
                        pass
                
                # 从文本内容获取
                time_text = time_element.get_text(strip=True)
                if time_text and ('UTC' in time_text or 'GMT' in time_text or any(month in time_text for month in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])):
                    return time_text
        
        # 方法2: 从页面文本中查找时间模式
        page_text = soup.get_text()
        
        # 匹配类似 "16 Oct 2025 01:24PM UTC" 的模式
        import re
        time_patterns = [
            r'\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}\s+\d{1,2}:\d{2}(?:AM|PM)\s+UTC',
            r'\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+UTC',
            r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4}\s+\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)?\s*UTC'
        ]
        
        for pattern in time_patterns:
            match = re.search(pattern, page_text)
            if match:
                return match.group(0)
        
        # 方法3: 如果还是找不到，尝试从URL对应的数据中获取
        # 如果URL是build页面，尝试从已获取的coverage_data中查找
        if "builds/" in url:
            build_id = url.split("builds/")[-1]
            # 这里可以查找之前获取的coverage_data
            # 但由于这是独立调用，我们需要重新获取
            coverage_data = get_coverage_data()
            for data in coverage_data:
                if data.get('build_detail_url') == url:
                    return data.get('created_time', '')
        
        return None
        
    except Exception as e:
        print(f"⚠️ 获取构建时间失败: {e}")
        return None
   
def get_coverage_change_info(build_detail_url):
    """
    从构建页面获取覆盖率变化信息
    :param build_detail_url: 构建详细页面URL
    :return: 覆盖率变化信息，格式如 "coverage: 53.935% (-7.1%) from 61.083%"
    """
    if not build_detail_url:
        return None
    
    try:
        response = requests.get(build_detail_url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "html.parser")
        page_text = soup.get_text()
        
        import re
        
        # 匹配覆盖率变化信息的模式
        coverage_patterns = [
            # 标准格式：coverage: XX% (±XX%) from XX%
            r'coverage:\s*([\d.]+%)\s*\(([+-][\d.]+%)\)\s*from\s*([\d.]+%)',
            # 可能的变体格式
            r'Coverage:\s*([\d.]+%)\s*\(([+-][\d.]+%)\)\s*from\s*([\d.]+%)',
            # 更宽松的匹配
            r'([\d.]+%)\s*\(([+-][\d.]+%)\)\s*from\s*([\d.]+%)'
        ]
        
        for pattern in coverage_patterns:
            match = re.search(pattern, page_text)
            if match:
                current_cov = match.group(1)
                change = match.group(2)
                from_cov = match.group(3)
                
                return f"coverage: {current_cov} ({change}) from {from_cov}"
        
        return None
        
    except Exception as e:
        print(f"⚠️ 获取覆盖率变化信息失败: {e}")
        return None 
    
def get_build_details_by_url(url):
    """
    根据传入的URL获取对应构建的详细信息
    :param url: coveralls构建URL
    :return: 详细信息JSON
    """
    try:
        # print(f"🔍 处理URL: {url}")
        
        # 获取详细覆盖率信息
        detailed_info = get_detailed_coverage_info(url)
        
        # 获取构建时间
        build_time = get_build_time_from_url(url)
        
        # 获取覆盖率变化信息
        coverage_change = get_coverage_change_info(url)
        
        if detailed_info:
            # 格式化返回详细信息
            detail_parts = detailed_info.split('. ')
            formatted_details = []
            for part in detail_parts:
                if part.strip():
                    formatted_details.append(part.strip())
            
            return {
                "status": "success",
                "url": url,
                "coverage_change": coverage_change,
                "detailed_info": {
                    # "raw": detailed_info,
                    "formatted": formatted_details
                },
                "build_time": build_time
            }
        else:
            return {
                "status": "error",
                "message": "无法获取详细信息",
                "url": url,
                "coverage_change": coverage_change,
                "build_time": build_time
            }
            
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "url": url,
            "coverage_change": None,
            "build_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        
if __name__ == "__main__":
    # 解析命令行参数
    args = parse_arguments()
    
    # 🆕 URL模式：处理单个URL请求
    if args.url:
        # print(f"🔌 API模式 - 处理URL: {args.url}")
        result = get_build_details_by_url(args.url)
        
        # 输出JSON结果
        print(json.dumps(result, indent=2, ensure_ascii=False))
        exit(0)
    
    # 根据参数选择URL
    webhook_urls = get_webhook_urls(args.test_mode)
    
    # 显示当前模式
    mode_text = "测试模式" if args.test_mode else "正式模式"
    clean_text = "启用" if args.data_clean else "禁用"
    preview_text = "启用" if args.preview else "禁用"
    
    print(f"🚀 运行模式: {mode_text}")
    print(f"🧹 数据清洗: {clean_text}")
    print(f"👁️ 预览模式: {preview_text}")
    
    if args.data_clean:
        print(f"📊 异常值阈值: {args.outlier_threshold}%")
    if args.preview:
        print(f"📤 预览数据将发送到: {send_to_feishu_alert_url_test}")
    else:
        print(f"📢 通知群URL: {webhook_urls['notify']}")
        print(f"🚨 告警群URL: {webhook_urls['alert']}")
    
    coverage_data = get_coverage_data()
        
    # 🆕 预览模式：只显示和发送最新数据
    if args.preview:
        print_preview_data(coverage_data)
        send_preview_to_feishu(coverage_data, send_to_feishu_alert_url_test)
        print("\n✅ 预览模式完成，程序结束")
        exit(0)
        
    # 数据清洗（如果启用）
    if args.data_clean and coverage_data:
        coverage_data = clean_coverage_data(coverage_data, args.outlier_threshold)
        
    if len(coverage_data) >= 2:
        # 获取最新的两条记录进行对比
        current_data = coverage_data[0]  # 最新的记录
        
        # 智能选择对比数据
        if args.data_clean:
            comparison_data = find_stable_comparison_data(coverage_data, current_data, args.outlier_threshold)
        else:
            comparison_data = coverage_data[1]  # 直接使用上一条记录
        
        if comparison_data:
            current_coverage = current_data['coverage_number']
            previous_coverage = comparison_data['coverage_number']
            
            print(f"当前覆盖率: {current_coverage}")
            print(f"对比覆盖率: {previous_coverage} (Build #{comparison_data['build_number']})")
            
            # 进行对比和告警
            compare_cells_alarm(current_coverage, previous_coverage, current_data, webhook_urls)
        else:
            print("⚠️ 无法找到合适的对比数据")
        
    elif len(coverage_data) == 1:
        # 只有一条记录，与默认值对比或跳过对比
        current_data = coverage_data[0]
        current_coverage = current_data['coverage_number']
        print(f"只有一条记录，当前覆盖率: {current_coverage}")
        
        # 可以与历史最高覆盖率对比
        highest_coverage = get_highest_coverage()
        if highest_coverage > 0:
            compare_cells_alarm(current_coverage, f"{highest_coverage}%", current_data, webhook_urls)
            
    else:
        print("未获取到任何覆盖率数据")