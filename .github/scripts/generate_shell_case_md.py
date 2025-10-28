import re
import os
import argparse
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime

class CatalogShellDocExtractor:
    """Extract docstring from shell script HERE documents and organize by catalog."""
    
    def __init__(self):
        # Pattern to match HERE document docstring
        self.docstring_pattern = re.compile(
            r': <<\'DOC\'\s*\n(.*?)\nDOC', 
            re.MULTILINE | re.DOTALL
        )
    
    def extract_docstring(self, script_path: str) -> Optional[Dict[str, str]]:
        """
        Extract docstring from shell script HERE document.
        
        Args:
            script_path: Path to shell script file
            
        Returns:
            Dictionary containing parsed docstring sections
        """
        with open(script_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        match = self.docstring_pattern.search(content)
        if not match:
            return None
        
        docstring_content = match.group(1).strip()
        doc_data = self._parse_docstring(docstring_content)
        doc_data['script_path'] = script_path
        doc_data['script_name'] = Path(script_path).stem
        doc_data['relative_path'] = os.path.relpath(script_path)
        
        return doc_data
    
    def _parse_docstring(self, content: str) -> Dict[str, str]:
        """Parse docstring content into sections."""
        sections = {}
        lines = content.split('\n')
        
        # Extract title (first non-empty line)
        for line in lines:
            line = line.strip()
            if line:
                sections['title'] = line
                break
        
        # Parse sections
        current_section = None
        current_content = []
        
        for line in lines[1:]:  # Skip title
            line = line.strip()
            
            # Check if this line starts a new section (ends with colon and not indented)
            if line.endswith(':') and not line.startswith(' ') and not line.startswith('-'):
                # Save previous section if exists
                if current_section:
                    section_key = current_section.lower()
                    sections[section_key] = '\n'.join(current_content).strip()
                
                # Start new section
                current_section = line[:-1].strip()
                current_content = []
            elif line:
                # Add content to current section
                if current_section:
                    current_content.append(line)
        
        # Save last section
        if current_section:
            section_key = current_section.lower()
            sections[section_key] = '\n'.join(current_content).strip()
        
        return sections

def parse_catalog(module_name, docstring):
    """解析 docstring 中的 Catalog 字段，返回多个 catalog"""
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
    
    # 如果没有找到 catalog，使用 module_name
    if not catalogs and module_name:
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

def get_module_path_from_file(file_path):
    """从文件路径提取模块路径"""
    # 提取 file_path 中 cases 后的字段
    if "test/cases/" in file_path:
        module_path = file_path.split("test/cases/")[1]
        # 去掉文件名，只保留目录路径
        module_path = "/".join(module_path.split("/")[:-1])
        # 去掉每一级目录中的前两位数字和 '-'
        module_path = "/".join([re.sub(r"^\d{2}-", "", part) for part in module_path.split("/") if part])
        return module_path.replace("/", ":")
    return ""

def get_output_path(catalog: str, base_output_dir: str) -> str:
    """
    Generate output path based on catalog.
    
    Args:
        catalog: Single catalog value (can have multiple levels separated by colons)
        base_output_dir: Base output directory
        
    Returns:
        Full path for the output markdown file
    """
    if ':' in catalog:
        # Split by colon to get all levels
        parts = [part.strip() for part in catalog.split(':')]
        
        if len(parts) == 2:
            # Two levels: category:subcategory -> category/subcategory.md
            category = parts[0]
            subcategory = parts[1]
            return os.path.join(base_output_dir, category, f"{subcategory}.md")
        elif len(parts) > 2:
            # Multiple levels: Table:NormalTable:Create -> Table/NormalTable/Create.md
            # All parts except the last become directory structure
            directory_parts = parts[:-1]
            filename = parts[-1]
            
            # Create nested directory path
            dir_path = os.path.join(base_output_dir, *directory_parts)
            return os.path.join(dir_path, f"{filename}.md")
        else:
            # Single part with colon (shouldn't happen, but handle gracefully)
            return os.path.join(base_output_dir, f"{catalog}.md")
    else:
        # Single category, create markdown file directly
        return os.path.join(base_output_dir, f"{catalog}.md")

def append_shell_markdown(doc_data: Dict[str, str], output_path: str):
    """Append shell script documentation to markdown file."""
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Check if file exists to determine if we need header
    file_exists = os.path.exists(output_path)
    
    with open(output_path, 'a', encoding='utf-8') as f:
        if not file_exists:
            # Create file header based on catalog name from output path
            path_parts = Path(output_path).parts
            
            # Find the part that starts from case_list_docs
            try:
                docs_index = path_parts.index('case_list_docs')
                catalog_parts = path_parts[docs_index + 1:]  # Get parts after case_list_docs
            except ValueError:
                # Fallback if case_list_docs not found
                catalog_parts = path_parts[-3:] if len(path_parts) >= 3 else path_parts
            
            # Remove .md extension from the last part
            if catalog_parts:
                catalog_parts = list(catalog_parts)
                catalog_parts[-1] = Path(catalog_parts[-1]).stem
            
            if len(catalog_parts) >= 2:
                # Has nested structure like Table/NormalTable/Create
                if len(catalog_parts) == 2:
                    # Two levels: category/subcategory
                    category = catalog_parts[0]
                    subcategory = catalog_parts[1]
                    f.write(f"# {category}: {subcategory}\n\n")
                else:
                    # Multiple levels: Table/NormalTable/Create -> Table: NormalTable: Create
                    title = ": ".join(catalog_parts)
                    f.write(f"# {title}\n\n")
            else:
                # Single category
                catalog_name = catalog_parts[0] if catalog_parts else Path(output_path).stem
                f.write(f"# {catalog_name}\n\n")
            
            f.write("---\n\n")
        
        # Add script documentation
        title = doc_data.get('title', f'{doc_data["script_name"].title()} Script')
        
        f.write(f"## {title}\n\n")
        
        # Description with dark background
        description = doc_data.get('description', '').strip()
        if description:
            f.write(f"```\n{description}\n```\n\n")
        else:
            f.write(f"```\n{title}\n```\n\n")
        
        # Path reference
        f.write(f"**Path:** [`{doc_data['relative_path']}`](https://github.com/taosdata/TDengine/blob/{GITHUB_BRANCH}/{doc_data['relative_path']})\n\n")
        
        f.write("---\n\n")

def process_markdown_files(doc_dir: str, doc_data: Dict[str, str], catalogs_str: str):
    """处理多个 catalog，为每个 catalog 生成对应的 markdown 文件"""
    catalogs = [cat.strip() for cat in catalogs_str.split(",") if cat.strip()]
    
    generated_files = []
    for catalog in catalogs:
        output_path = get_output_path(catalog, doc_dir)
        append_shell_markdown(doc_data, output_path)
        generated_files.append(output_path)
        print(f"  -> Documentation appended to: {output_path}")
    
    return generated_files
        
def process_file(doc_dir: str, file_path: str) -> List[str]:
    """
    Process a single shell script file and append to catalog-based markdown files.
    
    Args:
        doc_dir: Documentation output directory
        file_path: Path to shell script file
        
    Returns:
        List of paths to markdown files or empty list if no docstring found or not a shell file
    """
    # Only process shell script files
    if not file_path.endswith('.sh'):
        print(f"  Skipping non-shell file: {file_path}")
        return []
    
    extractor = CatalogShellDocExtractor()
    doc_data = extractor.extract_docstring(file_path)
    
    if not doc_data:
        print(f"  No HERE document docstring found in {file_path}")
        return []
    
    # 获取模块路径（基于文件目录结构）
    module_path = get_module_path_from_file(file_path)
    
    # 解析原始 docstring 内容来获取完整的 docstring
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    match = extractor.docstring_pattern.search(content)
    if not match:
        return []
    
    docstring_content = match.group(1).strip()
    
    # 解析 catalog 和 labels
    catalogs = parse_catalog(module_path, docstring_content)
    labels = parse_labels(docstring_content)
    
    print(f"Processing script: {doc_data['script_name']} in file: {file_path} with catalog: {catalogs}")
    
    if has_special_labels(labels):
        # labels 中有特殊标记 ignore，跳过默认路径
        print(f"  Special labels found, skipping default module path")
        catalog_only = [cat for cat in catalogs if cat != module_path]
        if catalog_only:
            return process_markdown_files(doc_dir, doc_data, ",".join(catalog_only))
        else:
            return []
    else: 
        return process_markdown_files(doc_dir, doc_data, ",".join(catalogs))

def main():
    """Main function with command line argument parsing."""
    parser = argparse.ArgumentParser(
        description="Generate shell script documentation from HERE document docstrings"
    )
    parser.add_argument(
        "--doc_dir", 
        type=str, 
        default="./test/docs/case_list_docs",
        help="Documentation output directory (default: ./test/docs/case_list_docs)"
    )
    parser.add_argument(
        "--case_list", 
        type=str, 
        default=None,
        help="Comma-separated list of specific files to process (default: process all shell scripts in test/cases)"
    )
    parser.add_argument(
        "--branch", 
        type=str, 
        default=None,
        help="GitHub branch name for links (default: get from BRANCH_NAME environment variable or 'main')"
    )
    
    args = parser.parse_args()
    
    # Set branch name globally for use in append_shell_markdown
    global GITHUB_BRANCH
    GITHUB_BRANCH = args.branch or os.environ.get('BRANCH_NAME', 'main')
    
    doc_dir = args.doc_dir
    case_list = args.case_list
    
    print(f"Documentation output directory: {doc_dir}")
    
    if case_list:
        # Process specific files from case_list
        files = [file.strip() for file in case_list.split(",") if file.strip()]
        print(f"Processing {len(files)} specific files...")
    else:
        # Process all shell scripts in test/cases directory
        source_dir = "test/cases"
        files = []
        for root, dirs, file_names in os.walk(source_dir):
            dirs[:] = sorted([d for d in dirs if re.match(r"^\d{2}-", d)])
            file_names = sorted(file_names)
            files.extend(os.path.join(root, file) for file in file_names 
                        if file.startswith('test_') and file.endswith(".sh"))
        
        print(f"Found {len(files)} shell script files in {source_dir}")
    
    # Process files
    all_generated_files = set()
    processed_count = 0
    
    for file_path in files:
        print(f"Processing file: {file_path}")
        output_files = process_file(doc_dir, file_path)
        if output_files:
            all_generated_files.update(output_files)
            processed_count += 1
    
    print(f"\nProcessed {processed_count} files successfully")
    print(f"Generated {len(all_generated_files)} documentation files:")
    for file_path in sorted(all_generated_files):
        print(f"  - {file_path}")

if __name__ == "__main__":
    main()