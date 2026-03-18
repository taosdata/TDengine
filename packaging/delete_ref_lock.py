import subprocess
import re
import os
from typing import Optional
from abc import ABC, abstractmethod


class RefLockErrorHandler(ABC):
    """抽象基类，定义处理接口"""

    @abstractmethod
    def match(self, error_output: str) -> bool:
        pass

    @abstractmethod
    def parse_branch(self, error_output: str) -> Optional[str]:
        pass

    def handle(self, error_output: str):
        branch = self.parse_branch(error_output)
        if branch:
            print(f"Detected error, attempting to delete ref for branch: {branch}")
            self.delete_ref(branch)
        else:
            print("Error parsing branch name.")

    def delete_ref(self, branch_name: str):
        try:
            subprocess.run(["git", "update-ref", "-d", branch_name], check=True)
        except subprocess.CalledProcessError as e:
            print(f"git update-ref failed: {e}")
            lock_files = [f".git/{branch_name}.lock", f".git/logs/{branch_name}.lock"]
            for lock_file in lock_files:
                if os.path.exists(lock_file):
                    try:
                        os.remove(lock_file)
                        print(f"Removed lock file: {lock_file}")
                    except Exception as ex:
                        print(f"Failed to remove lock file {lock_file}: {ex}")
            # retry
            try:
                subprocess.run(["git", "update-ref", "-d", branch_name], check=True)
            except subprocess.CalledProcessError as e2:
                print(f"Still failed to delete ref after removing lock: {e2}")


class Type1Handler(RefLockErrorHandler):
    # error: cannot lock ref 'refs/remotes/origin/fix/3.0/TD-32817': is at 7af5 but expected eaba
    # match the branch name before ‘is at’ with a regular expression
    def match(self, error_output: str) -> bool:
        return "is at" in error_output and "but expected" in error_output

    def parse_branch(self, error_output: str) -> str:
        # 匹配 cannot lock ref 部分，兼容中英文
        match = re.search(
            r"cannot lock ref '(refs/remotes/origin/[^']+)': is at", error_output
        )
        return match.group(1) if match else None


class Type2Handler(RefLockErrorHandler):
    # match the branch name before ‘exists; cannot create’ with a regular expression
    def match(self, error_output: str) -> bool:
        return "exists; cannot create" in error_output

    def parse_branch(self, error_output: str) -> str:
        match = re.search(r"'(refs/remotes/origin/[^']+)' exists;", error_output)
        return match.group(1) if match else None


class Type3Handler(RefLockErrorHandler):
    # match the branch name before the first single quote before 'Unable to' with a regular expression
    # git error: could not delete references: cannot lock ref 'refs/remotes/origin/test/3.0/TS-4893': Unable to create 'D:/workspace/main/TDinternal/community/.git/refs/remotes/origin/test/3.0/TS-4893.lock': File exists
    def match(self, error_output: str) -> bool:
        return "Unable to create" in error_output and "File exists" in error_output

    def parse_branch(self, error_output: str) -> str:
        match = re.search(
            r"(?:error|references): cannot lock ref '(refs/remotes/origin/[^']+)': Unable to",
            error_output,
        )
        return match.group(1) if match else None


class RefLockErrorHandlerFactory:
    """工厂类，返回合适的处理器"""

    handlers = [Type1Handler(), Type2Handler(), Type3Handler()]

    @classmethod
    def get_handler(cls, error_output: str):
        for handler in cls.handlers:
            if handler.match(error_output):
                return handler
        return None


def handle_error(error_output):
    handler = RefLockErrorHandlerFactory.get_handler(error_output)
    if handler:
        handler.handle(error_output)
    else:
        print("No handler found for this error.")


def git_fetch():
    print("Running: git fetch")
    result = subprocess.run(["git", "fetch"], capture_output=True, text=True)
    if result.returncode != 0:
        print("git fetch failed:")
        print(result.stderr)
    else:
        print("git fetch successful.")
    return result


def git_prune():
    print("Running: git remote prune origin")
    result = subprocess.run(
        ["git", "remote", "prune", "origin"], capture_output=True, text=True
    )
    if result.returncode != 0:
        print("git remote prune origin failed:")
        print(result.stderr)
    else:
        print("git remote prune origin successful.")
    return result


def main():
    fetch_result = git_fetch()
    if fetch_result.returncode != 0:
        handle_error(fetch_result.stderr)
        return

    prune_result = git_prune()
    if prune_result.returncode != 0:
        handle_error(prune_result.stderr)


if __name__ == "__main__":
    main()
