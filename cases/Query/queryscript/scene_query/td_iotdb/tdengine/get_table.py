import re
import subprocess

def extract_create_table_command(input_string):
    match = re.search(r'Create Table:\s*(CREATE TABLE.*)', input_string, re.DOTALL)
    if match:
        return match.group(1)
    return None

def get_table_create_command(table_name):
    result = subprocess.run(['taos', '-s', f'show create table test.{table_name}\G;'], stdout=subprocess.PIPE, text=True)
    output_lines = result.stdout.split('\n')
    filtered_output = '\n'.join(line for line in output_lines if not line.startswith('Query OK'))
    return filtered_output

# 循环获取多个表的创建命令
for i in range(100):
    table_name = f'd{i}'
    #print(f'Getting create table command for table {table_name}')
    input_string = get_table_create_command(table_name)
    create_table_command = extract_create_table_command(input_string)
    print(create_table_command)