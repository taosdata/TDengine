import os
import re
from loguru import logger

# List of source directories to search

self_path =  os.path.dirname(os.path.realpath(__file__))

# Check if "community"  or "tests" is in self_path
index_community = self_path.find("community")
if index_community != -1:
    TD_project_path = self_path[:index_community]
    index_TDinternal = TD_project_path.find("TDinternal")
    # Check if index_TDinternal is valid and set work_path accordingly
    if index_TDinternal != -1:
        work_path = TD_project_path[:index_TDinternal]
else:
    index_tests = self_path.find("tests")
    if index_tests != -1:
        TD_project_path = self_path[:index_tests]
    # Check if index_TDengine is valid and set work_path accordingly
        index_TDengine = TD_project_path.find("TDengine")
        if index_TDengine != -1:
            work_path = TD_project_path[:index_TDengine]
TD_project_path = TD_project_path.rstrip('/')
print(TD_project_path)
source_dirs = [
    f"{TD_project_path}/community/source",
    f"{TD_project_path}/community/include",
    f"{TD_project_path}/enterprise/src/plugins/"
]

# List of directories to exclude
exclude_dirs = [
    f"{TD_project_path}/community/source/client/jni"
]

# List of files to exclude
exclude_source_files = [
    f"{TD_project_path}/community/source/libs/parser/src/sql.c",
    f"{TD_project_path}/community/source/util/src/tlog.c",
    f"{TD_project_path}/community/include/util/tlog.h"
]

def grep_asserts_in_file(file_path, summary_list, detaild_list):
    """Search for assert, ASSERTS, or ASSERT function calls in a file and print them."""
    match_count = 0
    with open(file_path, 'r') as file:
        for line_number, line in enumerate(file, start=1):
            if re.search(r'\bassert\(.*\)|\bASSERT\(.*\)|\bASSERTS\(.*\)|\bASSERT_CORE\(.*\)', line):
                detaild_list.append(f"{file_path}:{line.strip()}:{line_number}")
                match_count += 1
    if match_count > 0:
        summary_list.append(f"Total matches in {file_path}:{match_count}")

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

def check_list_result(result_list,detaild_list):
    logger.debug("check assert in source code")
    error_message = "ERROR: do not add `assert` statements in new code."
    error_message2 = "ERROR: Please check the detailed information below: assert statement with file name and line number"
    remove_detail_items = [
            f"{TD_project_path}/community/source/dnode/vnode/src/tsdb/tsdbCommit2.c:ASSERT_CORE(tsdb->imem == NULL, \"imem should be null to commit mem\");",
            f"{TD_project_path}/community/include/util/types.h:assert(sizeof(float) == sizeof(uint32_t));",
            f"{TD_project_path}/community/include/util/types.h:assert(sizeof(double) == sizeof(uint64_t));"
        ]
    expected_strings = [
        f"Total matches in {TD_project_path}/community/source/dnode/vnode/src/tsdb/tsdbCommit2.c:1",
        f"Total matches in {TD_project_path}/community/include/util/types.h:2"
        ]
    logger.debug(len(result_list))
    if len(result_list) != 2:
        logger.error(f"{error_message}")
        for item in expected_strings:
            if item in result_list:
                result_list.remove(item)
        logger.error("\n" + "\n".join(result_list))
        logger.error(f"{error_message2}")
        for item in remove_detail_items:
            if item in detaild_list:
                detaild_list.remove(item)       
        logger.error("\n" + "\n".join(detaild_list))
        exit(1)
    else:
        # check if all expected strings are in the result list 
        if all(item in result_list for item in expected_strings):
            logger.debug(result_list)
            logger.debug(detaild_list)
            if all(any(remove_detail_item in detaild for remove_detail_item in remove_detail_items) for detaild in detaild_list):
                logger.info("Validation successful.")
        else:
            logger.error(f"{error_message}")
            for item in expected_strings:
                if item in result_list:
                    logger.debug(item)
                    result_list.remove(item)
            logger.error("\n" + "\n".join(result_list))
            logger.error(f"{error_message2}")
            for item in remove_detail_items:
                if item in detaild_list:
                    detaild_list.remove(item)       
            logger.error("\n" + "\n".join(detaild_list))
            exit(1)
if __name__ == "__main__":
    summary_list, detaild_list = traverse_and_grep(source_dirs, exclude_dirs, exclude_source_files)
    logger.debug("\n" + "\n".join(summary_list))
    logger.debug("\n" + "\n".join(detaild_list))

    check_list_result(summary_list,detaild_list)

