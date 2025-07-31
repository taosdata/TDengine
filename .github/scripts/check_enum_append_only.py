import sys
import os
import subprocess
from clang.cindex import Index, CursorKind, Config

# Use the libclang shared object installed via pip (libclang package)

try:
    from libclang import get_library_path
    Config.set_library_path(get_library_path())
except ImportError:
    print("libclang package not installed. Please run: pip install libclang")
    sys.exit(1)

def extract_enums_with_values(filepath):
    index = Index.create()
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

def get_git_version_with_values(filepath):
    try:
        content = subprocess.check_output(['git', 'show', f'HEAD:{filepath}'], text=True)
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".c", delete=False) as tmp:
            tmp.write(content.encode())
            tmp_path = tmp.name
        enums = extract_enums_with_values(tmp_path)
        os.unlink(tmp_path)
        return enums
    except subprocess.CalledProcessError:
        print(f"Warning: file '{filepath}' not found in Git HEAD. Treating as new file.")
        return {}

def check_enum_values_preserved(old_list, new_list):
    old_dict = dict(old_list)
    new_dict = dict(new_list)

    for name, val in old_dict.items():
        if name not in new_dict:
            return False, f"旧枚举成员 '{name}' 被删除"
        if new_dict[name] != val:
            return False, f"旧枚举成员 '{name}' 的值从 {val} 改为 {new_dict[name]}"
    return True, None

def check_file(filepath):
    print(f"Checking {filepath}...")
    new_enums = extract_enums_with_values(filepath)
    old_enums = get_git_version_with_values(filepath)

    all_ok = True
    for enum_name, new_items in new_enums.items():
        if enum_name in old_enums:
            old_items = old_enums[enum_name]
            ok, err_msg = check_enum_values_preserved(old_items, new_items)
            if not ok:
                print(f"❌ Violation in enum '{enum_name}': {err_msg}")
                all_ok = False
    return all_ok

if __name__ == "__main__":
    files = sys.argv[1:]
    if not files:
        print("Usage: python3 check_enum_append_only.py <file1.c> <file2.h> ...")
        sys.exit(1)

    all_ok = True
    for f in files:
        if not f.endswith(('.c', '.h')):
            continue
        if not os.path.isfile(f):
            print(f"Warning: file '{f}' does not exist, skipping.")
            continue
        if not check_file(f):
            all_ok = False

    if not all_ok:
        print("❌ Enum check failed: old enum members' names and values must not be changed or deleted.")
        sys.exit(1)
    else:
        print("✅ All enum checks passed.")
        sys.exit(0)
