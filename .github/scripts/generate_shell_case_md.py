"""
Shell script documentation generator with catalog-based path generation.
Extracts HERE documents as docstrings and appends to catalog-based markdown files.
"""

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
            
            if line.endswith(':') and not line.startswith(' ') and not line.startswith('-'):
                # Save previous section
                if current_section:
                    sections[current_section.lower()] = '\n'.join(current_content).strip()
                
                # Start new section
                current_section = line[:-1].strip()
                current_content = []
            elif line:
                current_content.append(line)
        
        # Save last section
        if current_section:
            sections[current_section.lower()] = '\n'.join(current_content).strip()
        
        return sections
    
    def get_output_path(self, doc_data: Dict[str, str], base_output_dir: str) -> str:
        """
        Generate output path based on catalog structure.
        
        Args:
            doc_data: Parsed docstring data
            base_output_dir: Base output directory
            
        Returns:
            Full path for the output markdown file
        """
        catalog = doc_data.get('catalog', '').strip()
        
        if catalog:
            # Parse catalog - handle formats like "- SuperTables:Create", "SuperTables:Create", or "DocsExamplesTest"
            for line in catalog.split('\n'):
                line = line.strip()
                if line.startswith('- '):
                    line = line[2:].strip()
                
                if ':' in line:
                    # Split by colon to get category and subcategory
                    parts = line.split(':', 1)
                    category = parts[0].strip()
                    subcategory = parts[1].strip()
                    
                    # Create path like: base_output_dir/SuperTables/Create.md
                    return os.path.join(base_output_dir, category, f"{subcategory}.md")
                elif line:
                    # Single category, create markdown file directly
                    # For "DocsExamplesTest" -> "case_list_docs/DocsExamplesTest.md"
                    return os.path.join(base_output_dir, f"{line}.md")
        
        # Fallback to scripts directory
        script_name = doc_data['script_name']
        return os.path.join(base_output_dir, "scripts", f"{script_name}.md")

def append_shell_markdown(doc_data: Dict[str, str], output_path: str):
    """Append shell script documentation to markdown file."""
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Check if file exists to determine if we need header
    file_exists = os.path.exists(output_path)
    
    # Debug: print what's being written
    print(f"DEBUG: Writing to {output_path}, file_exists: {file_exists}")
    print(f"DEBUG: doc_data keys: {list(doc_data.keys())}")
    
    with open(output_path, 'a', encoding='utf-8') as f:
        if not file_exists:
            # Create simple file header based on catalog - NO metadata fields
            catalog = doc_data.get('catalog', '')
            catalog_clean = catalog.replace('- ', '').strip()
            
            print(f"DEBUG: Creating file header for catalog: {catalog_clean}")
            
            if ':' in catalog_clean:
                parts = catalog_clean.split(':', 1)
                category = parts[0].strip()
                subcategory = parts[1].strip()
                f.write(f"# {category}: {subcategory}\n\n")
            else:
                # For single catalog like "DocsExamplesTest" - only write the title
                f.write(f"# {catalog_clean}\n\n")
            
            # ABSOLUTELY NO METADATA - only separator
            f.write("---\n\n")
        
        # Add script documentation
        title = doc_data.get('title', f'{doc_data["script_name"].title()} Script')
        print(f"DEBUG: Writing section for title: {title}")
        
        f.write(f"## {title}\n\n")
        
        # Description with dark background
        if 'description' in doc_data and doc_data['description'].strip():
            f.write(f"```\n{doc_data['description']}\n```\n\n")
        else:
            f.write(f"```\n{title}\n```\n\n")
        
        # Path reference
        f.write(f"**Path:** [`{doc_data['relative_path']}`](https://github.com/taosdata/TDengine/blob/{{{{ config.github_branch }}}}/{doc_data['relative_path']})\n\n")
        
        f.write("---\n\n")
        
def process_file(doc_dir: str, file_path: str) -> Optional[str]:
    """
    Process a single shell script file and append to catalog-based markdown file.
    
    Args:
        doc_dir: Documentation output directory
        file_path: Path to shell script file
        
    Returns:
        Path to markdown file or None if no docstring found or not a shell file
    """
    # Only process shell script files
    if not file_path.endswith('.sh'):
        print(f"  Skipping non-shell file: {file_path}")
        return None
    
    extractor = CatalogShellDocExtractor()
    doc_data = extractor.extract_docstring(file_path)
    
    if not doc_data:
        print(f"  No HERE document docstring found in {file_path}")
        return None
    
    output_path = extractor.get_output_path(doc_data, doc_dir)
    append_shell_markdown(doc_data, output_path)
    
    print(f"  -> Documentation appended to: {output_path}")
    return output_path

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
    
    args = parser.parse_args()
    
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
    generated_files = set()
    processed_count = 0
    
    for file_path in files:
        print(f"Processing file: {file_path}")
        output_file = process_file(doc_dir, file_path)
        if output_file:
            generated_files.add(output_file)
            processed_count += 1
    
    print(f"\nProcessed {processed_count} files successfully")
    print(f"Generated {len(generated_files)} documentation files:")
    for file_path in sorted(generated_files):
        print(f"  - {file_path}")

if __name__ == "__main__":
    main()