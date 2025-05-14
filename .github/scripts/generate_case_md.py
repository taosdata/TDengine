import os
import re
import ast
import sys

def process_markdown_files(doc_dir, content, catalog):
    catalog_set = set(catalog.split(','))
    modified = False

    # 遍历 doc_dir 中的所有 Markdown 文件
    for root, _, files in os.walk(doc_dir):
        for file in files:
            if file.endswith(".md"):
                md_file = os.path.join(root, file)

                # 检查文件是否包含 content
                with open(md_file, "r", encoding="utf-8") as f:
                    lines = f.readlines()

                if any(content in line for line in lines):
                    # 去掉 doc_dir 和 .md 后缀，提取相对路径
                    module = os.path.relpath(md_file, doc_dir).replace(".md", "").replace("/", ":")

                    if module in catalog_set:
                        catalog_set.remove(module)  # 移除匹配的项
                        continue

                    # 删除文件中包含 content 的行
                    with open(md_file, "w", encoding="utf-8") as f:
                        for line in lines:
                            if content not in line:
                                f.write(line)
                            else:
                                modified = True

    if catalog_set:
        for item in catalog_set:
            item = item.replace(":", "/")
            md_file = os.path.join(doc_dir, f"{item}.md")
            if not os.path.exists(md_file):
                print(f"Warning: File {md_file} does not exist")
                continue
            with open(md_file, "a", encoding="utf-8") as f:
                f.write(f"{content}\n")
            print(f"Added content to {md_file}")
    
    return modified
            
def get_md_file(base_dir, search_term):
    normalized_search_term = search_term.replace(":", "/")
    matched_file = os.path.join(base_dir, f"{normalized_search_term}.md")
    return matched_file

def extract_functions_with_docstring(file_path):
    """提取 Python 文件中的以 test_ 开头的函数、其 docstring、所属类名和文件名"""
    with open(file_path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())
        add_parent_references(tree)  # 确保为 AST 节点添加父节点引用

    functions = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test_") and ast.get_docstring(node):
            class_name = None
            parent = node
            while parent:
                parent = getattr(parent, 'parent', None)
                if isinstance(parent, ast.ClassDef):
                    class_name = parent.name
                    break
            functions.append((node.name, ast.get_docstring(node), class_name, file_path))
    return functions

# Add parent references to AST nodes
def add_parent_references(tree):
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            child.parent = node

def parse_catalog(module_name, docstring):
    catalog_pattern = re.compile(
        r"Catalog:\s*((?:- .+?\n?)+)(?:\n\s*\n|(?=\n(?:Since:|Labels:|Jira:|History:)))", 
        re.DOTALL
    )
    match = catalog_pattern.search(docstring)
    catalogs = []
    if match:
        # 提取 Catalog 块并按行分割
        catalog_block = match.group(1)
        catalogs = [line.strip("- ").strip() for line in catalog_block.splitlines() if line.strip()]
    
    if module_name not in catalogs:
        catalogs.append(module_name)

    return catalogs if catalogs else ["Uncategorized"]

def process_file(doc_dir, file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())
        add_parent_references(tree)
    functions = extract_functions_with_docstring(file_path)
    modified = False
    for function_name, docstring, class_name, file_name in functions:
        # 提取 file_name 中 cases 后的字段，去掉 .py 后缀
        relative_path = file_name.split("test/cases/")[1].replace(".py", "").replace("/", ".")
        content = f"::: {relative_path}.{class_name}.{function_name}" if class_name else f"{relative_path}.{function_name}"
        
        # 解析 Catalog
        module_path = file_path.replace("/", ":").removeprefix("test:cases:")
        module_path = ":".join(module_path.split(":")[:-1])
        # 去掉每一级目录中的前两位数字和 '-'
        module_path = ":".join([re.sub(r"^\d{2}-", "", part) for part in module_path.split(":")])
        catalog = parse_catalog(module_path, docstring)
        print(f"function: {function_name} in class: {class_name} in file: {file_name} with catalog: {catalog}, relative path: {relative_path}")
        
        if process_markdown_files(doc_dir, content, ",".join(catalog)):
            modified = True
    
    return modified

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python generate_function_docs.py <doc_dir> <case_list>")
        sys.exit(1)
    
    doc_dir = sys.argv[1]
    case_list = sys.argv[2]
    modified = False
    if case_list:
        files = [file.strip() for file in case_list.split(",") if file.strip()]
    else:
        source_dir = "test/cases"
        files = []
        for root, dirs, file_names in os.walk(source_dir):
            dirs[:] = sorted([d for d in dirs if re.match(r"^\d{2}-", d)])
            file_names = sorted(file_names)
            files.extend(os.path.join(root, file) for file in file_names if file.endswith(".py"))

    for file_path in files:
        print(f"Processing file: {file_path}")
        if process_file(doc_dir, file_path):
            modified = True
    
    if modified:
        sys.exit(0)
    else:
        sys.exit(1)