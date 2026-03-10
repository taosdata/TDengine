import os
import re
import ast
import sys
import yaml

def generate_md_files_from_nav(mkdocs_file, doc_dir):
    """
    根据 mkdocs.yml 中的 nav 配置生成 case_list_docs 目录下的 .md 文件
    :param mkdocs_file: mkdocs.yml 文件路径
    :param doc_dir: case_list_docs 目录路径
    """
    os.makedirs(doc_dir, exist_ok=True)
    with open(mkdocs_file, "r", encoding="utf-8") as f:
        content = re.sub(r'!ENV\s*\[[^\]]+\]', 'main', f.read())
        mkdocs_config = yaml.safe_load(content)

    nav = mkdocs_config.get("nav", [])
    case_list_docs = None

    for item in nav:
        if isinstance(item, dict) and "Case list docs" in item:
            case_list_docs = item["Case list docs"]
            break

    if not case_list_docs:
        print("Error: 'Case list docs' not found in nav configuration.")
        return

    def process_nav(nav_item, base_path=""):
        if isinstance(nav_item, dict):
            for key, value in nav_item.items():
                if isinstance(value, str): 
                    md_file_path = os.path.join(doc_dir, value.replace("case_list_docs/", ""))
                    os.makedirs(os.path.dirname(md_file_path), exist_ok=True)
                    if not os.path.exists(md_file_path):
                        with open(md_file_path, "w", encoding="utf-8") as f:
                            f.write(f"# {key}\n")
                        print(f"Created: {md_file_path}")
                elif isinstance(value, list): 
                    process_nav(value, base_path=os.path.join(base_path, key))
        elif isinstance(nav_item, list):
            for sub_item in nav_item:
                process_nav(sub_item, base_path)

    process_nav(case_list_docs)
    
def process_markdown_files(doc_dir, content, catalog):
    catalog_set = set(catalog.split(','))

    for item in catalog_set:
        item = item.replace(":", "/")
        md_file = os.path.join(doc_dir, f"{item}.md")
        if not os.path.exists(md_file):
            print(f"Warning: File {md_file} does not exist")
            continue
        with open(md_file, "a", encoding="utf-8") as f:
            f.write(f"{content}\n")
        print(f"Added content to {md_file}")
            
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

def parse_labels(docstring):
    """解析 docstring 中的 Labels 字段"""
    labels_pattern = re.compile(
        r"Labels:\s*(.*?)(?=\n\s*(?:Since|Catalog|Jira|History):\s*|\Z)", 
        re.DOTALL | re.IGNORECASE
    )
    match = labels_pattern.search(docstring)
    labels = []
    if match:
        # 提取 Labels 块并按行分割
        labels_content = match.group(1).strip()
        labels_line = re.sub(r'\s+', ' ', labels_content)
        labels = [label.strip() for label in labels_line.split(",") if label.strip()]
    
    return labels

def has_special_labels(labels, special_markers=None):
    """检查是否有特殊标记"""
    if special_markers is None:
        special_markers = ["ignore"] 
    
    for label in labels:
        if any(marker.lower() in label.lower() for marker in special_markers):
            return True
    return False

def process_file(doc_dir, file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())
        add_parent_references(tree)
    functions = extract_functions_with_docstring(file_path)
    for function_name, docstring, class_name, file_name in functions:
        labels = parse_labels(docstring)
        
        # 提取 file_name 中 cases 后的字段，去掉 .py 后缀
        relative_path = file_name.split("test/cases/")[1].replace(".py", "").replace("/", ".")
        content = f"::: {relative_path}.{class_name}.{function_name}" if class_name else f"{relative_path}.{function_name}"
        
        # 解析 Catalog
        module_path = file_path.replace("/", ":")
        if module_path.startswith("test:cases:"):
            module_path = module_path[len("test:cases:"):]
        module_path = ":".join(module_path.split(":")[:-1])
        # 去掉每一级目录中的前两位数字和 '-'
        module_path = ":".join([re.sub(r"^\d{2}-", "", part) for part in module_path.split(":")])
        catalog = parse_catalog(module_path, docstring)
        
        if has_special_labels(labels):
            # lables 中有特殊标记 ignore，跳过默认路径
            print(f"function: {function_name} in class: {class_name} in file: {file_name} with catalog: {catalog}, relative path: {relative_path} (special labels, skipping default path)")
            catalog_only = [cat for cat in catalog if cat != module_path]
            if catalog_only:
                process_markdown_files(doc_dir, content, ",".join(catalog_only))
        else: 
            print(f"function: {function_name} in class: {class_name} in file: {file_name} with catalog: {catalog}, relative path: {relative_path}")
            process_markdown_files(doc_dir, content, ",".join(catalog))
    

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python generate_function_docs.py <mkdocs yaml file> <doc_dir> <case_list>")
        sys.exit(1)
    
    mkdocs_file = sys.argv[1]
    doc_dir = sys.argv[2]
    case_list = sys.argv[3]
    
    generate_md_files_from_nav(mkdocs_file, doc_dir)
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
        process_file(doc_dir, file_path)