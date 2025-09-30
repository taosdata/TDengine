import sys
import os
import subprocess
import argparse
from clang import cindex
from clang.cindex import CursorKind

def extract_enums_with_values(filepath):
    index = cindex.Index.create()
    tu = index.parse(filepath, args=['-x', 'c', '-std=c11'])

    enums = {}

    def visit(node):
        if node.kind == CursorKind.ENUM_DECL:
            name = node.spelling or "<anonymous>"
            items = []
            for c in node.get_children():
                if c.kind == CursorKind.ENUM_CONSTANT_DECL:
                    val = c.enum_value
                    items.append((c.spelling, val))
            enums[name] = items
        for child in node.get_children():
            visit(child)

    visit(tu.cursor)
    return enums

def get_git_version_with_values(filepath, base_branch):
    try:
        content = subprocess.check_output(['git', 'show', f'{base_branch}:{filepath}'], text=True)
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".c", delete=False) as tmp:
            tmp.write(content.encode())
            tmp_path = tmp.name
        enums = extract_enums_with_values(tmp_path)
        os.unlink(tmp_path)
        return enums
    except subprocess.CalledProcessError:
        print(f"Warning: file '{filepath}' not found in {base_branch}. Treating as new file.")
        return {}

def check_enum_values_preserved(old_list, new_list):
    old_dict = dict(old_list)
    new_dict = dict(new_list)

    for name, val in old_dict.items():
        if name not in new_dict:
            return False, f"enum '{name}' has been deleted"
        if new_dict[name] != val:
            return False, f"enum '{name}' value changed from {val} to {new_dict[name]}"
    return True, None

def check_file(filepath, base_branch):
    print(f"Checking {filepath} against {base_branch}...")
    new_enums = extract_enums_with_values(filepath)
    old_enums = get_git_version_with_values(filepath, base_branch)

    all_ok = True
    for enum_name, new_items in new_enums.items():
        if enum_name in old_enums:
            old_items = old_enums[enum_name]
            ok, err_msg = check_enum_values_preserved(old_items, new_items)
            if not ok:
                print(f"ERROR: Violation in enum '{enum_name}': {err_msg}")
                all_ok = False
    return all_ok

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--base-branch', required=True, help='Base branch to compare against')
    parser.add_argument('files', nargs='+', help='Files to check')
    args = parser.parse_args()

    all_ok = True
    for f in args.files:
        if not f.endswith(('.c', '.h')):
            continue
        if not os.path.isfile(f):
            print(f"Warning: file '{f}' does not exist, skipping.")
            continue
        if not check_file(f, args.base_branch):
            all_ok = False

    if not all_ok:
        print("ERROR: Enum check failed: old enum members\' names and values must not be changed or deleted.")
        sys.exit(1)
    else:
        print("All enum checks passed.")
        sys.exit(0)
