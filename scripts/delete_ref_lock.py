#!/usr/bin/env python3

import os
import re
import subprocess
import sys
from abc import ABC, abstractmethod
from typing import Optional


def remove_empty_parents(path: str, stop_dir: str):
    current = os.path.dirname(path)
    stop_dir = os.path.normpath(stop_dir)
    while current and os.path.normpath(current).startswith(stop_dir):
        if os.path.normpath(current) == stop_dir:
            break
        try:
            os.rmdir(current)
            print(f"Removed empty directory: {current}")
        except OSError:
            break
        current = os.path.dirname(current)


class RefLockErrorHandler(ABC):
    @abstractmethod
    def match(self, error_output: str) -> bool:
        pass

    @abstractmethod
    def parse_branch(self, error_output: str) -> Optional[str]:
        pass

    def handle(self, error_output: str) -> bool:
        branch = self.parse_branch(error_output)
        if branch:
            print(f"Detected error, attempting to delete ref for branch: {branch}")
            return self.delete_ref(branch)
        print("Error parsing branch name.")
        return False

    def delete_ref(self, branch_name: str) -> bool:
        try:
            subprocess.run(["git", "update-ref", "-d", branch_name], check=True)
            self.cleanup_ref_dirs(branch_name)
            return True
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
            try:
                subprocess.run(["git", "update-ref", "-d", branch_name], check=True)
                self.cleanup_ref_dirs(branch_name)
                return True
            except subprocess.CalledProcessError as e2:
                print(f"Still failed to delete ref after removing lock: {e2}")
                return False

    def cleanup_ref_dirs(self, branch_name: str):
        ref_file = os.path.join(".git", branch_name)
        reflog_file = os.path.join(".git", "logs", branch_name)

        if not os.path.exists(ref_file):
            remove_empty_parents(ref_file, os.path.join(".git", "refs"))
        if not os.path.exists(reflog_file):
            remove_empty_parents(reflog_file, os.path.join(".git", "logs", "refs"))


class Type1Handler(RefLockErrorHandler):
    def match(self, error_output: str) -> bool:
        return "is at" in error_output and "but expected" in error_output

    def parse_branch(self, error_output: str) -> Optional[str]:
        match = re.search(
            r"cannot lock ref '(refs/remotes/origin/[^']+)': is at", error_output
        )
        return match.group(1) if match else None


class Type2Handler(RefLockErrorHandler):
    def match(self, error_output: str) -> bool:
        return "exists; cannot create" in error_output

    def parse_branch(self, error_output: str) -> Optional[str]:
        match = re.search(r"'(refs/remotes/origin/[^']+)' exists;", error_output)
        return match.group(1) if match else None

    def handle(self, error_output: str) -> bool:
        match = re.search(
            r"cannot lock ref '(refs/remotes/origin/[^']+)': '(refs/remotes/origin/[^']+)' exists; cannot create '(refs/remotes/origin/[^']+)'",
            error_output,
        )
        if match:
            target_ref = match.group(1)
            blocking_ref = match.group(2)
            print(
                f"Detected conflict: blocking ref {blocking_ref} prevents creating {target_ref}"
            )
            fixed = self.delete_ref(blocking_ref)
            self.cleanup_ref_dirs(target_ref)
            return fixed
        return super().handle(error_output)


class Type3Handler(RefLockErrorHandler):
    def match(self, error_output: str) -> bool:
        return "Unable to create" in error_output and "File exists" in error_output

    def parse_branch(self, error_output: str) -> Optional[str]:
        match = re.search(
            r"(?:error|references): cannot lock ref '(refs/remotes/origin/[^']+)': Unable to",
            error_output,
        )
        return match.group(1) if match else None


class RefLockErrorHandlerFactory:
    handlers = [Type1Handler(), Type2Handler(), Type3Handler()]

    @classmethod
    def get_handler(cls, error_output: str):
        for handler in cls.handlers:
            if handler.match(error_output):
                return handler
        return None


def handle_error(error_output: str) -> bool:
    handler = RefLockErrorHandlerFactory.get_handler(error_output)
    if handler:
        return handler.handle(error_output)
    print("No handler found for this error.")
    return False


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


def main() -> bool:
    max_retries = 2
    fetch_ok = False

    for attempt in range(max_retries + 1):
        fetch_result = git_fetch()
        if fetch_result.returncode == 0:
            fetch_ok = True
            break

        error_output = "\n".join(
            part for part in [fetch_result.stderr, fetch_result.stdout] if part
        )
        fixed = handle_error(error_output)
        if not fixed or attempt == max_retries:
            return False
        print(f"Retrying git fetch... ({attempt + 1}/{max_retries})")

    if not fetch_ok:
        return False

    prune_result = git_prune()
    if prune_result.returncode == 0:
        return True

    if not handle_error(prune_result.stderr):
        return False

    retry_prune = git_prune()
    return retry_prune.returncode == 0


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
