import os
import re

# List of source directories to search
source_dirs = [
    "community/source",
    "community/include",
    "enterprise/src/plugins/"
]

# List of directories to exclude
exclude_dirs = [
    "community/source/client/jni"
]

# List of files to exclude
exclude_source_files = [
    "community/source/libs/parser/src/sql.c",
    "community/source/util/src/tlog.c",
    "community/include/util/tlog.h"
]

def grep_asserts_in_file(file_path, summary_list, detaild_list):
    """Search for assert, ASSERTS, or ASSERT function calls in a file and print them."""
    match_count = 0
    with open(file_path, 'r') as file:
        for line_number, line in enumerate(file, start=1):
            if re.search(r'\bassert\(.*\)|\bASSERT\(.*\)|\bASSERTS\(.*\)|\bASSERT_CORE\(.*\)', line):
                detaild_list.append(f"{file_path}:{line_number}: {line.strip()}")
                match_count += 1
    if match_count > 0:
        summary_list.append(f"Total matches in {file_path}: {match_count}")

def traverse_and_grep(source_dirs, exclude_dirs, exclude_source_files):
    """Traverse directories and grep for assert, ASSERTS, or ASSERT function calls in .h and .c files."""
    summary_list = []
    detaild_list = []
    for source_dir in source_dirs:
        for root, _, files in os.walk(source_dir):
            # Skip directories named 'test' or 'tests' and directories in exclude_dirs
            if 'test' in root.split(os.sep) or 'tests' in root.split(os.sep) or any(excluded in root for excluded in exclude_dirs):
                continue
            for file in files:
                if file.endswith((".h", ".c")):
                    file_path = os.path.join(root, file)
                    if file_path not in exclude_source_files:
                        grep_asserts_in_file(file_path, summary_list, detaild_list)
    return summary_list, detaild_list

if __name__ == "__main__":
    summary_list, detaild_list = traverse_and_grep(source_dirs, exclude_dirs, exclude_source_files)
    print("\n".join(summary_list))
    # print("\n".join(detaild_list))