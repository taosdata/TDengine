import subprocess
import re

# 执行 git fetch 命令并捕获输出
def git_fetch():
    result = subprocess.run(['git', 'fetch'], capture_output=True, text=True)
    return result

# 解析分支名称
def parse_branch_name_type1(error_output):
    # 使用正则表达式匹配 'is at' 前的分支名称
    match = re.search(r"error: cannot lock ref '(refs/remotes/origin/[^']+)': is at", error_output)
    if match:
        return match.group(1)
    return None

# 解析第二种错误中的分支名称
def parse_branch_name_type2(error_output):
    # 使用正则表达式匹配 'exists' 前的第一个引号内的分支名称
    match = re.search(r"'(refs/remotes/origin/[^']+)' exists;", error_output)
    if match:
        return match.group(1)
    return None

# 执行 git update-ref -d 命令
def git_update_ref(branch_name):
    if branch_name:
        subprocess.run(['git', 'update-ref', '-d', f'{branch_name}'], check=True)

# 解析错误类型并执行相应的修复操作
def handle_error(error_output):
    # 错误类型1：本地引用的提交ID与远程不一致
    if "is at" in error_output and "but expected" in error_output:
        branch_name = parse_branch_name_type1(error_output)
        if branch_name:
            print(f"Detected error type 1, attempting to delete ref for branch: {branch_name}")
            git_update_ref(branch_name)
        else:
            print("Error parsing branch name for type 1.")
    # 错误类型2：尝试创建新的远程引用时，本地已经存在同名的引用
    elif "exists; cannot create" in error_output:
        branch_name = parse_branch_name_type2(error_output)
        if branch_name:
            print(f"Detected error type 2, attempting to delete ref for branch: {branch_name}")
            git_update_ref(branch_name)
        else:
            print("Error parsing branch name for type 2.")

# 主函数
def main():
    fetch_result = git_fetch()
    if fetch_result.returncode != 0:  # 如果 git fetch 命令失败
        error_output = fetch_result.stderr
        handle_error(error_output)
    else:
        print("Git fetch successful.")

if __name__ == "__main__":
    main()