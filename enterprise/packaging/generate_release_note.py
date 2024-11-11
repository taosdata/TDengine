"""
This module generates release notes from JIRA issues.

It connects to a JIRA instance, retrieves issues based on a JQL query,
and formats them into a release note document.
"""

import argparse
import re
import os
from loguru import logger
from jira import JIRA


class FileHandler:
    def __init__(self, file_path):
        self.file_path = file_path
        self.file = None

    def open(self):
        self.file = open(self.file_path, "a", encoding="utf-8")

    def write(self, data):
        if type(data) == list:
            self.file.writelines(data)
        elif type(data) == str:
            self.file.write(data)

    def close(self):
        if self.file:
            self.file.close()
            self.file = None

    def __del__(self):
        if self.file:
            self.file.close()


# set log
logger.level("INFO")
# set default args
jira_server = "https://jira.taosdata.com:18080"
replacements = {
    "taosd": "taosd",
    "taosadapter": "taosAdapter",
    "tdengine": "TDengine",
    "taos": "taos",
    "taosrestful": "taosRestful",
    "taosexplorer": "taosExplorer",
    "tsma": "TSMA",
    "taosconnector": "taosConnector",
    "taosx": "taosX",
    "wal": "WAL",
}


def clear_file(file_path: str):
    if not os.path.exists(file_path):
        return True
    with open(file_path, "w+", encoding="utf-8") as file:
        content = ""
        file.write(content)


def print_file_content(file_name):
    # open file and print content
    with open(file_name, "r", encoding="utf-8") as file:
        content = file.read()
    logger.info(f"\n{content}")


def process_content(content: str, replacements_dict: dict) -> str:
    """
    Process the content by replacing multiple substrings.

    :param content: The original text content.
    :param replacements: A dictionary of replacements where key is the search
    term and value is the replacement term.
    :return: The processed content.
    """
    # Sort the replacements by the length of the search term in descending order to avoid partial matches
    for search_term, replace_term in sorted(
        replacements.items(), key=lambda item: len(item[0]), reverse=True
    ):
        pattern = re.compile(re.escape(search_term), re.IGNORECASE)
        content = pattern.sub(replace_term, content)
    return content


def get_release_note(user: str, passwd: str, release_version: str, office_note: str):
    """
    Generate release notes for a given release version.

    This function connects to JIRA using the provided user credentials,
    fetches all issues related to the specified release version, and
    writes the release notes to the appropriate files.

    Args:
        user (str): JIRA username.
        passwd (str): JIRA password.
        release_version (str): The release version for which to generate notes.

    Returns:
        None
    """
    jira = JIRA(server=jira_server, basic_auth=(user, passwd))
    jql = create_jql_query(release_version)
    logger.info(f"filter jql:{jql}")
    if office_note.lower() == "true":
        logger.info(f"release notes will remove TS/TD")
    else:
        logger.info(f"release notes will include TS/TD")
    logger.info(f"generate release_version-{release_version} release notes")
    all_issues = fetch_all_issues(jira, jql)
    logger.info(f"total filter {release_version} issues:{len(all_issues)}")
    zh_file, en_file = prepare_files(release_version)
    process_and_write_issues(all_issues, zh_file, en_file, office_note)
    logger.info(f"generate release_version-{release_version} release notes done")


def create_jql_query(release_version: str) -> str:
    return (
        f'status in ("Releasing","Checking","Verifying","Done") '
        f'and  project in ("Taos Support","Taos Development") '
        f"AND fixVersion = {release_version} "
        f"ORDER BY priority DESC, key ASC"
    )


def fetch_all_issues(jira, jql):
    all_issues = []
    start_at = 0
    max_results = 50

    while True:
        issues = jira.search_issues(jql, startAt=start_at, maxResults=max_results)
        if not issues:
            break
        all_issues.extend(issues)
        start_at += len(issues)
    return all_issues


def prepare_files(release_version: str):
    zh_file = f"release_note_{release_version}_zh.txt"
    en_file = f"release_note_{release_version}_en.txt"
    clear_file(zh_file)
    clear_file(en_file)
    return zh_file, en_file

def process_and_write_issues(all_issues, zh_file, en_file, office_note:str):
    file_handler_zh = FileHandler(zh_file)
    file_handler_en = FileHandler(en_file)
    file_handler_zh.open()
    file_handler_en.open()
    file_zh_line_count = 0
    file_en_line_count = 0
    logger.info(f"office_note:{office_note}")
    for issue in all_issues:
        if (
            issue.fields.customfield_12330 is not None
            and issue.fields.customfield_12330 != "-"
        ):
            if office_note.lower() == "true":
                content_zh = f"{issue.fields.customfield_12330} \n"
            else:
                content_zh = f"{issue.key} {issue.fields.customfield_12330} \n"


            processed_content_zh = process_content(content_zh, replacements)
            file_handler_zh.write(processed_content_zh)
            file_zh_line_count += 1

        if (
            issue.fields.customfield_12331 is not None
            and issue.fields.customfield_12331 != "-"
        ):  
            if office_note.lower() == "true":
                content_en = f"{issue.fields.customfield_12331} \n"
            else:
                content_en = f"{issue.key} {issue.fields.customfield_12331} \n"
            processed_content_en = process_content(content_en, replacements)
            file_handler_en.write(processed_content_en)
            file_en_line_count += 1

    file_handler_zh.close()
    file_handler_en.close()
    # get release note content number
    logger.info(f"release note zh file:{zh_file} content line count:{file_zh_line_count}")
    logger.info(f"release note en file:{en_file} content line count:{file_en_line_count}")


def sort_file_by_category(file_path):
    """
    Sort the contents of a file into categories.

    This function reads the contents of the specified file and sorts the lines
    into three categories: fixes, optimizations, and new features. The sorted
    lines are stored in separate lists.

    Args:
        file_path (str): The path to the file to be sorted.

    Returns:
        tuple: A list containing three lists: fixes, optimizations, and new features.
    """
    with open(file_path, "r", encoding="utf-8") as file:
        lines = file.readlines()

    # Debug: Print the lines read from the file
    # print("Lines read from file:")
    # print(lines)

    # Initialize lists for each category
    fixes = []
    optimizations = []
    new_features = []

    # Categorize each line
    for line in lines:
        if "修复" in line:
            fixes.append(line)
        elif "优化" in line:
            optimizations.append(line)
        elif "新功能" in line:
            new_features.append(line)
        elif "fix" in line:
            fixes.append(line)
        elif "enh" in line:
            optimizations.append(line)
        elif "feat" in line:
            new_features.append(line)

    if "zh" in file_path:
        line_index = ["[修复] \n", "[优化]\n", "[新功能]\n"]
    elif "en" in file_path:
        line_index = ["[Fixed issues] \n", "[Optimizations]\n", "[New Features/Improvements]\n"]
    else:
        logger.error("file name should contain zh or en")
        exit(1)

    # Combine the lists in the desired order
    sorted_lines = [line_index[2],*new_features,'\n',
                line_index[1],*optimizations,'\n',
                line_index[0],*fixes   
                ]

    # Write sorted lines back to the file
    with open(file_path, "w", encoding="utf-8") as file:
        file.writelines(sorted_lines)


def main():
    """
    Main function to parse arguments and get release note.

    This function parses command-line arguments for JIRA user, password, and release version.
    It then calls the get_release_note function with these arguments.
    """
    parser = argparse.ArgumentParser(description="Example script.")
    parser.add_argument("user", help="jira user")
    parser.add_argument("passwd", help="jira passwd")
    parser.add_argument("version", help="release version:3.3.4.0")
    parser.add_argument("--office_note", help="True or False,True is that file will remove TS/TD" ,default="false")

    args = parser.parse_args()
    # get release note:
    try:
        get_release_note(args.user, args.passwd, args.version, args.office_note)
    except Exception as e:
        logger.error(f"get release note failed:{e}")

    # sort release note by category
    sort_file_by_category(f"release_note_{args.version}_zh.txt")
    sort_file_by_category(f"release_note_{args.version}_en.txt")

    # # mv release note to /pkgs/TDengine/smoking/v{args.version}/ and rm original release note
    # cmd = f"cp release_note_{args.version}_zh.txt release_note_{args.version}_en.txt /pkgs/TDengine/smoking/v{args.version}/enterprise/ && rm release_note_{args.version}_zh.txt release_note_{args.version}_en.txt"
    # os.system(cmd)


if __name__ == "__main__":
    main()
