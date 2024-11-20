import subprocess
import re

# 执行 git fetch 命令并捕获输出
def git_fetch():
    result = subprocess.run(['git', 'fetch'], capture_output=True, text=True)
    return result

def git_prune():
    # git remote prune origin
    print("git remote prune origin")
    result = subprocess.run(['git', 'remote', 'prune', 'origin'], capture_output=True, text=True)
    return result
    
def parse_branch_name_type1(error_output):
    # error: cannot lock ref 'refs/remotes/origin/fix/3.0/TD-32817': is at 7af5 but expected eaba
    # 使用正则表达式匹配 'is at' 前的分支名称
    match = re.search(r"error: cannot lock ref '(refs/remotes/origin/[^']+)': is at", error_output)
    if match:
        return match.group(1)
    return None

def parse_branch_name_type2(error_output):
    # 使用正则表达式匹配 'exists; cannot create' 前的第一个引号内的分支名称
    match = re.search(r"'(refs/remotes/origin/[^']+)' exists;", error_output)
    if match:
        return match.group(1)
    return None

# parse branch name from error output of git remote prune origin
def parse_branch_name_type3(error_output):
    # 使用正则表达式匹配 'Unable to' 前的第一个引号内的分支名称
    # git error: could not delete references: cannot lock ref 'refs/remotes/origin/test/3.0/TS-4893': Unable to create 'D:/workspace/main/TDinternal/community/.git/refs/remotes/origin/test/3.0/TS-4893.lock': File exists
    match = re.search(r"references: cannot lock ref '(refs/remotes/origin/[^']+)': Unable to", error_output)
    if match:
        return match.group(1)
    return None


# 执行 git update-ref -d 命令
def git_update_ref(branch_name):
    if branch_name:
        subprocess.run(['git', 'update-ref', '-d', f'{branch_name}'], check=True)

# 解析错误类型并执行相应的修复操作
# parse error type and execute corresponding repair operation
def handle_error(error_output):
    error_types = [
        ("is at", "but expected", parse_branch_name_type1, "type 1"),
        ("exists; cannot create", None, parse_branch_name_type2, "type 2"),
        ("Unable to create", "File exists", parse_branch_name_type3, "type 3")
    ]

    for error_type in error_types:
        if error_type[0] in error_output and (error_type[1] is None or error_type[1] in error_output):
            branch_name = error_type[2](error_output)
            if branch_name:
                print(f"Detected error {error_type[3]}, attempting to delete ref for branch: {branch_name}")
                git_update_ref(branch_name)
            else:
                print(f"Error parsing branch name for {error_type[3]}.")
            break

# 主函数
def main():
    # fetch_result = git_fetch()
    # if fetch_result.returncode != 0:  # 如果 git fetch 命令失败
    #     error_output = fetch_result.stderr
    #     handle_error(error_output)
    # else:
    #     print("Git fetch successful.")

    prune_result = git_prune()
    print(prune_result.returncode)
    if prune_result.returncode != 0:  # 如果 git fetch 命令失败
        error_output = prune_result.stderr
        print(error_output)
        handle_error(error_output)
    else:
        print("Git prune successful.")

if __name__ == "__main__":
    main()