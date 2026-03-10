import sys
import os
import ast
import re

def check_required_field(lines, field, method_node, class_node, file_path, other_fields):
    """
    检查必填字段（如 Since)，支持多行内容。
    """
    index = next((i for i, line in enumerate(lines) if re.match(rf'^{field}\s*:\s*.*$', line, re.IGNORECASE)), None)
    has_errors = False
    if index is None:
        print(f"The docstring in method {method_node.name} in class {class_node.name} in {file_path} does not contain a valid '{field}:' section.")
        has_errors = True
    else:
        if index > 0 and lines[index - 1].strip():
            print(f"The docstring in method {method_node.name} in class {class_node.name} in {file_path} must have a blank line before '{field}'.")
            has_errors = True
        value_lines = []
        first_line_value = re.sub(rf'^{field}\s*:\s*', '', lines[index], flags=re.IGNORECASE).strip()
        if first_line_value:
            value_lines.append(first_line_value)
        for i in range(index + 1, len(lines)):
            if lines[i].strip() and not re.match(rf'^({"|".join(other_fields)})\s*:\s*.*$', lines[i], re.IGNORECASE):
                value_lines.append(lines[i].strip())
            else:
                break
        if not value_lines:
            print(f"The docstring in method {method_node.name} in class {class_node.name} in {file_path} does not contain a value for the '{field}:' field.")
            has_errors = True
    return has_errors

def check_optional_field(lines, field, method_node, class_node, file_path, other_fields):
    """
    检查可选字段（如 Labels、Jira、History, Catalog)，支持多行内容。
    """
    index = next((i for i, line in enumerate(lines) if re.match(rf'^{field}\s*:\s*.*$', line, re.IGNORECASE)), None)
    has_errors = False
    if index is not None:
        if index > 0 and lines[index - 1].strip():
            print(f"The docstring in method {method_node.name} in class {class_node.name} in {file_path} must have a blank line before '{field}'.")
            has_errors = True
        value_lines = []
        first_line_value = re.sub(rf'^{field}\s*:\s*', '', lines[index], flags=re.IGNORECASE).strip()
        if first_line_value:
            value_lines.append(first_line_value)
        for i in range(index + 1, len(lines)):
            if lines[i].strip() and not re.match(rf'^({"|".join(other_fields)})\s*:\s*.*$', lines[i], re.IGNORECASE):
                value_lines.append(lines[i].strip())
            else:
                break
        if not value_lines:
            print(f"The docstring in method {method_node.name} in class {class_node.name} in {file_path} does not contain a value for the '{field}' field.")
            has_errors = True
    return has_errors

def check_required_field_shell(lines, field, file_path, other_fields):
    """
    检查 shell 脚本中的必填字段（如 Since)，支持多行内容。
    """
    index = next((i for i, line in enumerate(lines) if re.match(rf'^{field}\s*:\s*.*$', line, re.IGNORECASE)), None)
    has_errors = False
    if index is None:
        print(f"The docstring in shell script {file_path} does not contain a valid '{field}:' section.")
        has_errors = True
    else:
        if index > 0 and lines[index - 1].strip():
            print(f"The docstring in shell script {file_path} must have a blank line before '{field}'.")
            has_errors = True
        value_lines = []
        first_line_value = re.sub(rf'^{field}\s*:\s*', '', lines[index], flags=re.IGNORECASE).strip()
        if first_line_value:
            value_lines.append(first_line_value)
        for i in range(index + 1, len(lines)):
            if lines[i].strip() and not re.match(rf'^({"|".join(other_fields)})\s*:\s*.*$', lines[i], re.IGNORECASE):
                value_lines.append(lines[i].strip())
            else:
                break
        if not value_lines:
            print(f"The docstring in shell script {file_path} does not contain a value for the '{field}:' field.")
            has_errors = True
    return has_errors

def check_optional_field_shell(lines, field, file_path, other_fields):
    """
    检查 shell 脚本中的可选字段（如 Labels、Jira、History, Catalog)，支持多行内容。
    """
    index = next((i for i, line in enumerate(lines) if re.match(rf'^{field}\s*:\s*.*$', line, re.IGNORECASE)), None)
    has_errors = False
    if index is not None:
        if index > 0 and lines[index - 1].strip():
            print(f"The docstring in shell script {file_path} must have a blank line before '{field}'.")
            has_errors = True
        value_lines = []
        first_line_value = re.sub(rf'^{field}\s*:\s*', '', lines[index], flags=re.IGNORECASE).strip()
        if first_line_value:
            value_lines.append(first_line_value)
        for i in range(index + 1, len(lines)):
            if lines[i].strip() and not re.match(rf'^({"|".join(other_fields)})\s*:\s*.*$', lines[i], re.IGNORECASE):
                value_lines.append(lines[i].strip())
            else:
                break
        if not value_lines:
            print(f"The docstring in shell script {file_path} does not contain a value for the '{field}' field.")
            has_errors = True
    return has_errors

def validate_shell_test_file(file_path):
    """
    验证 shell 脚本中的 HERE document 格式的文档字符串。
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    # 匹配 HERE document docstring
    docstring_pattern = re.compile(
        r': <<\'DOC\'\s*\n(.*?)\nDOC', 
        re.MULTILINE | re.DOTALL
    )
    
    match = docstring_pattern.search(content)
    if not match:
        print(f"Shell script {file_path} does not contain a HERE document docstring (: <<'DOC' ... DOC).")
        return False

    docstring_content = match.group(1).strip()
    lines = docstring_content.splitlines()
    
    if not lines:
        print(f"The docstring in shell script {file_path} is empty.")
        return False

    has_errors = False

    # Check for summary (first non-empty line)
    first_line = lines[0].strip() if lines else ""
    if not first_line or re.match(r'^(Since|Labels|History|Jira|Description|Catalog)\s*:', first_line, re.IGNORECASE):
        print(f"The docstring in shell script {file_path} does not start with a summary.")
        has_errors = True

    # Check for Description section
    description_index = next((i for i, line in enumerate(lines) if re.match(r'^Description\s*:\s*.*$', line, re.IGNORECASE)), None)
    if description_index is None:
        print(f"The docstring in shell script {file_path} does not contain a 'Description:' section.")
        has_errors = True
    else:
        # Check blank line before Description
        if description_index > 1 and lines[description_index - 1].strip():
            print(f"The docstring in shell script {file_path} must have a blank line before 'Description:'.")
            has_errors = True
        
        # Check if Description has content
        description_lines = []
        first_line_value = re.sub(r'^Description\s*:\s*', '', lines[description_index], flags=re.IGNORECASE).strip()
        if first_line_value:
            description_lines.append(first_line_value)
        
        all_fields = ['Since', 'Catalog', 'Labels', 'Jira', 'History', 'Description']
        for i in range(description_index + 1, len(lines)):
            if lines[i].strip() and not re.match(rf'^({"|".join([f for f in all_fields if f != "Description"])})\s*:\s*.*$', lines[i], re.IGNORECASE):
                description_lines.append(lines[i].strip())
            else:
                break
        
        if not description_lines:
            print(f"The docstring in shell script {file_path} does not contain content for the 'Description:' field.")
            has_errors = True

    # Required fields
    required_fields = ['Since']
    all_fields = ['Since', 'Catalog', 'Labels', 'Jira', 'History', 'Description']
    for field in required_fields:
        if check_required_field_shell(lines, field, file_path, [f for f in all_fields if f != field]):
            has_errors = True

    # Optional fields
    optional_fields = ['Labels', 'Jira', 'History', 'Catalog']
    for field in optional_fields:
        if check_optional_field_shell(lines, field, file_path, [f for f in all_fields if f != field]):
            has_errors = True

    if has_errors:
        return False
    
    return True

def validate_test_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    # Parse the file content into an abstract syntax tree (AST)
    try:
        tree = ast.parse(content)
    except SyntaxError as e:
        print(f"Syntax error in file {file_path}: {e}")
        return False
    
    has_errors = False
    # Check for classes named 'Test...'
    test_classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name.startswith('Test')]
    if not test_classes:
        print(f"File {file_path} does not contain a class named 'Test...'.")
        return False

    for class_node in test_classes:
        # Check for methods prefixed with 'test_'
        test_methods = [node for node in class_node.body if isinstance(node, ast.FunctionDef) and node.name.startswith('test_')]
        if not test_methods:
            print(f"Class {class_node.name} in {file_path} does not contain a method named 'test_'.")
            return False

        for method_node in test_methods:
            # Extract the docstring
            docstring = ast.get_docstring(method_node)
            if not docstring or not docstring.strip():
                print(f"Method {method_node.name} in class {class_node.name} in {file_path} does not contain a docstring.")
                has_errors = True
                continue

            # Validate the docstring structure
            lines = docstring.strip().splitlines()

            # Check for summary
            if not lines[0].strip() or re.match(r'^(Since|Labels|History|Jira)\s*:', lines[0].strip(), re.IGNORECASE):
                print(f"The docstring in method {method_node.name} in class {class_node.name} in {file_path} does not start with a summary.")
                has_errors = True
            else:
                # Ensure summary is a single line
                if len(lines[0].splitlines()) > 1:
                    print(f"The summary in method {method_node.name} in class {class_node.name} in {file_path} must be a single line.")
                    has_errors = True

            # Check for description
            if len(lines) < 2:
                print(f"The docstring in method {method_node.name} in class {class_node.name} in {file_path} does not contain a description.")
                has_errors = True
            else:
                # Ensure there is a blank line between summary and description
                if len(lines) > 1 and lines[1].strip():
                    print(f"The docstring in method {method_node.name} in class {class_node.name} in {file_path} must have a blank line between the summary and the description.")
                    has_errors = True
                else:
                    # Check for description (can be multi-line)
                    description_end = next((i for i, line in enumerate(lines[2:], start=2) if not line.strip()), len(lines))
                    if description_end == 2 or re.match(r'^(Since|Labels|History|Jira|Catalog)\s*:', lines[2].strip(), re.IGNORECASE):
                        print(f"The docstring in method {method_node.name} in class {class_node.name} in {file_path} does not contain a description.")
                        has_errors = True

            # Required fields
            required_fields = ['Since']
            all_fields = ['Since', 'Catalog', 'Labels', 'Jira', 'History']
            for field in required_fields:
                if check_required_field(lines, field, method_node, class_node, file_path, [f for f in all_fields if f != field]):
                    has_errors = True

            # Optional fields
            optional_fields = ['Labels', 'Jira', 'History', 'Catalog']
            for field in optional_fields:
                if check_optional_field(lines, field, method_node, class_node, file_path, [f for f in all_fields if f != field]):
                    has_errors = True

    if has_errors:
        return False
    
    return True


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python validate_tests.py <file_path>")
        sys.exit(1)

    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        sys.exit(1)

    # 根据文件扩展名选择验证方式
    if file_path.endswith('.sh'):
        if not validate_shell_test_file(file_path):
            sys.exit(1)
    elif file_path.endswith('.py'):
        if not validate_test_file(file_path):
            sys.exit(1)
    else:
        print(f"Unsupported file type: {file_path}. Only .py and .sh files are supported.")
        sys.exit(1)