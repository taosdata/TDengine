import subprocess
import time
import sys
import re
import concurrent.futures
from datetime import datetime

def run_single_test(test_id):
    """执行单次测试"""
    try:
        print(f"\n开始测试 #{test_id} 于 {datetime.now().strftime('%H:%M:%S')}")
        result = subprocess.run(['python3', '6267.py'], 
                             capture_output=True, 
                             text=True)
        return test_id, result
    except Exception as e:
        return test_id, f"执行错误: {str(e)}"

def run_query_test():
    """并发运行查询测试"""
    iteration = 1
    max_workers = 10  # 并发执行的数量，可以根据需要调整
    
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                # 提交多个并发任务
                future_to_test = {
                    executor.submit(run_single_test, i): i 
                    for i in range(iteration, iteration + max_workers)
                }
                
                # 处理完成的任务结果
                for future in concurrent.futures.as_completed(future_to_test):
                    test_id, result = future.result()
                    
                    if isinstance(result, str):  # 处理错误情况
                        print(f"测试 #{test_id} 失败: {result}")
                        continue
                        
                    # 打印时间范围和阈值信息
                    for line in result.stdout.split('\n'):
                        if '使用时间范围:' in line or '使用val' in line:
                            print(f"测试 #{test_id}: {line}")
                    
                    # 检查结果一致性
                    if '所有表的查询结果都一致！' in result.stdout:
                        print(f"测试 #{test_id} 通过：所有查询结果一致")
                    else:
                        print(f"\n测试 #{test_id} 发现不一致！")
                        print("详细信息:")
                        lines = result.stdout.split('\n')
                        for i, line in enumerate(lines):
                            if '的查询结果不一致' in line:
                                print("\n".join(lines[i:i+10]))
                        
                        # 发现不一致时停止所有测试
                        print("\n测试停止：发现不一致的查询结果")
                        return iteration
                
                iteration += max_workers
                time.sleep(0.5)  # 短暂暂停
                
    except KeyboardInterrupt:
        print("\n\n用户中断测试")
    except Exception as e:
        print(f"\n执行出错: {e}")
    finally:
        print(f"\n总共执行了 {iteration} 轮测试")
    
    return iteration

if __name__ == "__main__":
    print(f"开始并发测试查询结果 (并发数: 10)...")
    run_query_test()