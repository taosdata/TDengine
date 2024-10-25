from jira import JIRA
import argparse
import re
import os
import logging
from loguru import logger


class FileHandler:
    def __init__(self, file_path):
        self.file_path = file_path
        self.file = None

    def open(self):
        self.file = open(self.file_path, 'a')

    def write(self, data):
        self.file.write(data)

    def write_line(self, data):
        self.file.writelines(data)
            
    def close(self):
        if self.file:
            self.file.close()
            self.file = None
            
    def __del__(self):
        if self.file:
            self.file.close()

jira_server = "https://jira.taosdata.com:18080"
logger.level("INFO")


def append_to_file(file_path, content):
    """
    Append content to a file.
    
    :param file_path: Path to the file.
    :param content: Content to be written to the file.
    """
    with open(file_path, 'a') as file:
        file.write(content + '\n') 


def write_line_to_file(file_path, content):
    """
    Write str list content to a file.
    
    :param file_path: Path to the file.
    :param content: Content to be written to the file.
    """
    with open(file_path, 'a') as file:
        file.writelines(content) 

def clear_file(file_path):
    """
    Clear the file if it's not empty
    
    :param file_path: Path to the file.
    :param content: Content to be written to the file.
    """
    if not os.path.exists(file_path):
        return True
    with open(file_path, 'w+') as file:  
        content = ""
        file.write(content) 

def print_file_content(file_name):
    # open file and print content
    with open(file_name, 'r') as file:
        content = file.read()
    logger.info(f"\n{content}")

def get_release_note(user, passwd, release_version):
    jira = JIRA(server=jira_server, basic_auth=(user, passwd))

    #jql = "\"Epic Link\" = TD-27435 and status = DONE  AND (assignee in membersof(\"application group 1\") or assignee in membersOf(\"application group 2\"))"
    #jql = "project = \"Taos Support\" and type = Bug and status = DONE  and created >= 2024-7-1 and created  < 2024-9-30"

    jql = f"statusCategory = indeterminate AND project in (\"Taos Support\",\"Taos Development\") AND fixVersion = {release_version}   ORDER BY priority DESC, key ASC"
    # print(f"jql:{jql}")
    logger.info(f"generate release_version-{release_version} release note")
    # jql = "key in (TS-4785,TS-5383,TS-5384,TS-5532,TS-5537,TS-5529,TS-5531,TS-5540,TS-4785)"
    zh_file = f'release_note_{release_version}_zh.txt'
    en_file = f'release_note_{release_version}_en.txt'
    issues = jira.search_issues(jql)

    replacements = {
        "taosd": "taosd",
        "taosadapter": "taosAdapter",
        "tdengine": "TDengine",
        "taos": "taos",
        "taosrestful": "taosRestful",
        "taosexplorer": "taosExplorer",
    }

    clear_file(zh_file)
    clear_file(en_file)
    file_handler_zh = FileHandler(zh_file)
    file_handler_en = FileHandler(en_file)
    file_handler_zh.open()
    file_handler_en.open()
    contents_zh=[]
    contents_en=[]

    for issue in issues:
        # issue.fields.customfield_12330  is relsease note in chinese
        if issue.fields.customfield_12330 is not None and issue.fields.customfield_12330 != "-" :
            content_zh= f"{issue.key} {issue.fields.customfield_12330} \n"
            processed_content_zh = process_content(content_zh, replacements)
            # append_to_file(zh_file, processed_content_zh)
            # contents_zh.append(processed_content_zh)
            file_handler_zh.write(processed_content_zh)
        # issue.fields.customfield_12331  is relsease note in english
        if issue.fields.customfield_12331 is not None and issue.fields.customfield_12331 != "-" :
            content_en= f"{issue.key} {issue.fields.customfield_12331} \n"
            processed_content_en = process_content(content_en, replacements)
            # append_to_file(en_file, processed_content_en)
            # contents_en.append(processed_content_en)
            file_handler_en.write(processed_content_en)


    # write_line_to_file(zh_file, contents_zh)
    # write_line_to_file(en_file, contents_en)
    # file_handler_en.write_line(contents_en)
    # file_handler_zh.write_line(contents_zh)

    file_handler_zh.close()
    file_handler_en.close()
    print_file_content(zh_file)
    print_file_content(en_file)
    logger.info(f"generate release_version-{release_version} release note done")

def process_content(content, replacements):
    """
    Process the content by replacing multiple substrings.
    
    :param content: The original text content.
    :param replacements: A dictionary of replacements where key is the search term and value is the replacement term.
    :return: The processed content.
    """
    # Sort the replacements by the length of the search term in descending order to avoid partial matches
    for search_term, replace_term in sorted(replacements.items(), key=lambda item: len(item[0]), reverse=True):
        pattern = re.compile(re.escape(search_term), re.IGNORECASE)
        content = pattern.sub(replace_term, content)
    return content

def main():
    parser = argparse.ArgumentParser(description="Example script.")
    parser.add_argument("user", help="jenkins user")
    parser.add_argument("passwd", help="jenkins passwd")
    parser.add_argument("version", help="release_version")

    args = parser.parse_args()    
    # get release note:    
    get_release_note(args.user,args.passwd, args.version)

if __name__ == "__main__":
    main()
    


