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
    
    #print(f"File {file_path} passed validation.")
    return True


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python validate_tests.py <file_path>")
        sys.exit(1)

    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        sys.exit(1)

    if not validate_test_file(file_path):
        sys.exit(1)