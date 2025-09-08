import os
import re
import subprocess
import sys

def camel_to_snake(name):
    """将驼峰命名转为下划线命名"""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def snake_to_camel(name):
    """下划线转驼峰"""
    return ''.join(word.capitalize() for word in name.split('_'))

def fix_filename(filename):
    """将文件名改为test开头、下划线格式、无-"""
    name, ext = os.path.splitext(filename)
    name = name.replace('-', '_')
    name = camel_to_snake(name)
    if not name.startswith('test_'):
        name = 'test_' + name
    return name + ext

def process_file(filepath, new_filepath):
    """处理文件内容"""
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # 1. 替换 import 语句
    import_pattern = re.compile(r'^(import |from .+ import .+)')
    new_import = 'from new_test_framework.utils import tdLog, tdSql, etool\n'
    new_lines = []
    import_replaced = False

    # 2. 获取新类名和新函数名
    base_name = os.path.splitext(os.path.basename(new_filepath))[0]
    class_name = snake_to_camel(base_name)
    func_name = base_name

    for i, line in enumerate(lines):
        # 替换 import
        if import_pattern.match(line):
            if not import_replaced:
                new_lines.append(new_import)
                import_replaced = True
            continue  # 跳过原import
        # 替换 class TDTestCase(TBase)
        if re.match(r'class TDTestCase\s*\(TBase\)\s*:', line):
            new_lines.append(f'class {class_name}:\n')
            continue
        # 替换 tdSql.res 为 tdSql.queryResult
        line = line.replace('tdSql.res', 'tdSql.queryResult')
        # 替换 run 函数
        if re.match(r'\s*def run\s*\(', line):
            indent = re.match(r'^(\s*)', line).group(1)
            new_lines.append(f'{indent}def {func_name}(self):\n')
            new_lines.append(f'{indent}    """summary: xxx\n\n')
            new_lines.append(f'{indent}    description: xxx\n\n')
            new_lines.append(f'{indent}    Since: xxx\n\n')
            new_lines.append(f'{indent}    Labels: xxx\n\n')
            new_lines.append(f'{indent}    Jira: xxx\n\n')
            new_lines.append(f'{indent}    Catalog:\n')
            new_lines.append(f'{indent}        - xxx:xxx\n')
            new_lines.append(f'{indent}    History:')
            new_lines.append(f'{indent}        - xxx\n')
            new_lines.append(f'{indent}        - xxx\n')
            new_lines.append(f'{indent}    """\n')
            continue
        # 删除 stop 函数定义
        if re.match(r'\s*def stop\s*\(', line):
            continue
        # 删除 tdSql.close()
        if re.search(r'tdSql\.close\s*\(\)', line):
            continue
        # 删除 tdCases.addWindows/tdCases.addLinux
        if re.search(r'tdCases\.addWindows|tdCases\.addLinux', line):
            continue
        new_lines.append(line)

    with open(new_filepath, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)

def main(target_dir):
    for root, dirs, files in os.walk(os.path.join("../tests", target_dir)):
        print(f"root: {root}, dirs: {dirs}, files: {files}")
        for filename in files:
            if filename.endswith('.py'):
                old_path = os.path.join(root, filename)
                new_filename = fix_filename(filename)
                new_path = os.path.join("cases/uncatalog", target_dir, new_filename)
                # 用 git mv 保留历史
                if old_path != new_path:
                    subprocess.run(['git', 'mv', old_path, new_path])
                #else:
                #    new_path = old_path
                #new_path = os.path.join(root, filename)
                process_file(new_path, new_path)
                print(f'Processed: {new_path}')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python migrate_cases.py <target_dir>')
        sys.exit(1)
    main(sys.argv[1])