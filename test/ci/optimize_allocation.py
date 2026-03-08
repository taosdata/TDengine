#!/usr/bin/env python3
import sys
import os

def parse_cases_list(cases_file):
    """解析测试用例列表文件，返回运行时间映射字典"""
    runtime_map = {}
    with open(cases_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            # 分割行，最后一列是运行时间
            # 格式：y,.,./ci/pytest.sh pytest cases/02-Databases/01-Create/test_db_tables.py|success|34
            # 先按|分割
            pipe_parts = line.split('|')
            if len(pipe_parts) < 3:
                continue
            
            # 前半部分包含命令信息
            cmd_part = pipe_parts[0].strip()
            runtime_str = pipe_parts[-1].strip()
            
            try:
                runtime = int(runtime_str)
            except ValueError:
                continue
            
            # 按逗号分割命令部分
            # 格式：y,.,./ci/pytest.sh pytest cases/02-Databases/01-Create/test_db_tables.py
            comma_parts = cmd_part.split(',', 2)
            if len(comma_parts) >= 3:
                cmd = comma_parts[2].strip()
                runtime_map[cmd] = runtime
    return runtime_map

def parse_cases_task(task_file, runtime_map):
    """解析测试任务文件，结合运行时间映射"""
    cases = []
    with open(task_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            # 分割行
            # 格式：,,y,.,./ci/pytest.sh pytest cases/01-DataTypes/test_column_tag_boundary.py
            parts = line.split(',', 4)
            if len(parts) < 5:
                continue
            cmd = parts[4].strip()
            # 获取运行时间，如果没有则默认使用 60 秒
            runtime = runtime_map.get(cmd, 60)
            # 保存完整的用例行和运行时间
            cases.append((line, runtime))
    return cases

def optimize_allocation(cases, machine_processes):
    """优化测试用例分配，使用贪心算法"""
    # 按运行时间降序排序用例
    sorted_cases = sorted(cases, key=lambda x: x[1], reverse=True)
    
    # 初始化每台机器的进程负载
    # machine_loads[i] 表示第i台机器的各个进程的负载
    machine_loads = []
    for i, processes in enumerate(machine_processes):
        machine_loads.append([0] * processes)
    
    # 分配用例到负载最小的进程
    allocation = [[] for _ in machine_processes]
    
    for case, runtime in sorted_cases:
        # 找到当前负载最小的机器和进程
        min_load = float('inf')
        target_machine = -1
        target_process = -1
        
        for i, processes_load in enumerate(machine_loads):
            for j, load in enumerate(processes_load):
                if load < min_load:
                    min_load = load
                    target_machine = i
                    target_process = j
        
        # 分配用例到目标进程
        allocation[target_machine].append(case)
        machine_loads[target_machine][target_process] += runtime
    
    # 计算每台机器的总负载和最大负载
    machine_totals = []
    for i, processes_load in enumerate(machine_loads):
        total = sum(processes_load)
        max_process_load = max(processes_load)
        machine_totals.append((total, max_process_load))
        print(f"Machine {i+1}: Total load = {total}s, Max process load = {max_process_load}s")
    
    # 计算总运行时间（取每台机器的最大进程负载的最大值）
    total_runtime = max([max_load for _, max_load in machine_totals])
    print(f"Estimated total runtime: {total_runtime}s")
    
    return allocation

def write_allocation_files(allocation, output_dir):
    """将分配结果写入文件"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    for i, machine_cases in enumerate(allocation):
        output_file = os.path.join(output_dir, f"machine_{i+1}_cases.txt")
        with open(output_file, 'w') as f:
            for case in machine_cases:
                f.write(case + '\n')
        print(f"Written {len(machine_cases)} cases to {output_file}")

def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <cases-list.txt> <cases.task>")
        sys.exit(1)
    
    cases_list_file = sys.argv[1]
    cases_task_file = sys.argv[2]
    
    if not os.path.exists(cases_list_file):
        print(f"File not found: {cases_list_file}")
        sys.exit(1)
    
    if not os.path.exists(cases_task_file):
        print(f"File not found: {cases_task_file}")
        sys.exit(1)
    
    # 解析运行时间映射
    runtime_map = parse_cases_list(cases_list_file)
    print(f"Parsed {len(runtime_map)} runtime entries")
    
    # 解析测试任务
    cases = parse_cases_task(cases_task_file, runtime_map)
    print(f"Parsed {len(cases)} test cases")
    
    # 定义每台机器的进程数：第一台40进程，其余三台10进程
    machine_processes = [40, 10, 10, 10]
    
    # 优化分配
    allocation = optimize_allocation(cases, machine_processes)
    
    # 写入分配文件
    output_dir = os.path.join(os.path.dirname(cases_task_file), "allocations")
    write_allocation_files(allocation, output_dir)
    
    print(f"\nAllocation completed. Files written to {output_dir}")

if __name__ == "__main__":
    main()
