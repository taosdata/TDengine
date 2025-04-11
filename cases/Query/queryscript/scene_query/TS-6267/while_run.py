import subprocess
import time
import sys
import re

def run_query_test():
    """运行查询测试，直到发现不一致的结果"""
    iteration = 1
    
    try:
        while True:
            print(f"\n开始第 {iteration} 轮测试")
            print("=" * 50)
            
            # 运行6267.py并捕获输出
            result = subprocess.run(['python3', '6267.py'], 
                                 capture_output=True, 
                                 text=True)
            
            # 打印时间范围和阈值信息
            for line in result.stdout.split('\n'):
                if '使用时间范围:' in line or '使用val' in line:
                    print(line)
            
            # 检查是否有不一致的情况
            if '所有表的查询结果都一致！' in result.stdout:
                print("本轮测试通过：所有查询结果一致")
                print(f"耗时: {time.strftime('%H:%M:%S')}")
                print("-" * 50)
            else:
                print("\n发现查询结果不一致！")
                print("详细信息:")
                # 提取并打印不一致的表信息
                lines = result.stdout.split('\n')
                for i, line in enumerate(lines):
                    if '的查询结果不一致' in line:
                        # 打印不一致的详细信息
                        print("\n".join(lines[i:i+10]))
                        
                print("\n测试停止：发现不一致的查询结果")
                break
            
            iteration += 1
            time.sleep(1)  # 稍作暂停，避免太快
            
    except KeyboardInterrupt:
        print("\n\n用户中断测试")
    except Exception as e:
        print(f"\n执行出错: {e}")
    finally:
        print(f"\n总共执行了 {iteration} 轮测试")

if __name__ == "__main__":
    print("开始持续测试查询结果...")
    run_query_test()