from jira import JIRA
import argparse
import re
import os



def append_to_file(file_path, content):
    """
    Append content to a file.
    
    :param file_path: Path to the file.
    :param content: Content to be written to the file.
    """
    with open(file_path, 'a') as file:
        file.write(content + '\n')  # 添加换行符确保内容在新行

def clear_file(file_path):
    """
    Clear the file if it's not empty
    
    :param file_path: Path to the file.
    :param content: Content to be written to the file.
    """
    # 检查文件是否存在且不为空
    if not os.path.exists(file_path):
        return True# 文件不存在，不需要清空
    with open(file_path, 'r+') as file:  # 使用 'r+' 模式，允许读写
        content_now = file.read()
        if len(content_now.strip()) > 0:
            # 文件不为空，清空文件内容
            file.seek(0)  # 回到文件开头
            file.truncate()  # 清空文件

def open_file(file_name):
    # open file and print content
    with open(file_name, 'r') as file:
        content = file.read()
    print(content)

def replace_case_insensitive(text, search_term, replace_term):
    # 编译一个正则表达式模式，忽略大小写
    pattern = re.compile(re.escape(search_term), re.IGNORECASE)
    # 替换所有匹配的字符串
    replaced_text = pattern.sub(replace_term, text)
    return replaced_text


def get_release_note(user, passwd, release_version):
    jira = JIRA(server='https://jira.taosdata.com:18080', basic_auth=(user, passwd))

    #jql = "\"Epic Link\" = TD-27435 and status = DONE  AND (assignee in membersof(\"application group 1\") or assignee in membersOf(\"application group 2\"))"
    #jql = "project = \"Taos Support\" and type = Bug and status = DONE  and created >= 2024-7-1 and created  < 2024-9-30"

    jql = f"statusCategory = indeterminate AND project in (\"Taos Support\",\"Taos Development\") AND fixVersion = {release_version}   ORDER BY priority DESC, key ASC"
    # print(f"jql:{jql}")
    print(f"generate release_version-{release_version} release note")
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

    for issue in issues:
        if issue.fields.customfield_12330 is not None and issue.fields.customfield_12330 != "-" :
            content = issue.key + ": " + issue.fields.customfield_12330 
            processed_content = process_content(content, replacements)
            append_to_file(zh_file, processed_content)

    for issue in issues:
        if issue.fields.customfield_12331 is not None and issue.fields.customfield_12331 != "-" :
            content = issue.key + ": " + issue.fields.customfield_12331
            processed_content = process_content(content, replacements)
            append_to_file(en_file, processed_content)

    open_file(zh_file)
    open_file(en_file)  

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

    # 解析命令行参数
    args = parser.parse_args()

    # 使用参数执行操作
    get_release_note(args.passwd, args.version)

if __name__ == "__main__":
    main()
    


