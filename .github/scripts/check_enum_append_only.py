# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import argparse
import os
import re
import subprocess
import sys
import tempfile

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


def _git_show_bytes(ref):
    return subprocess.check_output(['git', 'show', ref])


def _decode_for_compare(raw):
    # latin-1 is a lossless round-trip for any byte sequence; old C/H is not
    # always UTF-8 (benchCsv.h used to fail git show with text=True).
    return raw.decode('latin-1').replace('\r\n', '\n').replace('\r', '\n')


def strip_copyright_preamble(text):
    """Drop leading copyright/SPDX comments so header-only diffs can be skipped."""
    lines = text.split('\n')
    i = 0
    n = len(lines)
    changed = True
    while changed and i < n:
        changed = False
        if i < n and not lines[i].strip():
            i += 1
            changed = True
            continue
        if i < n and lines[i].lstrip().startswith('/*'):
            j = i
            while j < n and '*/' not in lines[j]:
                j += 1
            if j < n:
                block = '\n'.join(lines[i:j + 1])
                if re.search(r'Copyright|SPDX-License-Identifier', block, re.I):
                    i = j + 1
                    changed = True
                    continue
            break
        if i < n and lines[i].lstrip().startswith('//'):
            j = i
            while j < n and lines[j].lstrip().startswith('//'):
                j += 1
            block = '\n'.join(lines[i:j])
            if re.search(r'Copyright|SPDX-License-Identifier', block, re.I):
                i = j
                changed = True
                continue
            break
        if i < n and lines[i].lstrip().startswith('#'):
            j = i
            while j < n and lines[j].lstrip().startswith('#'):
                j += 1
            block = '\n'.join(lines[i:j])
            if re.search(r'Copyright|SPDX-License-Identifier', block, re.I):
                i = j
                changed = True
                continue
            break
        break
    return '\n'.join(lines[i:])


def is_header_only_change(filepath, base_branch):
    try:
        old_raw = _git_show_bytes(f'{base_branch}:{filepath}')
    except subprocess.CalledProcessError:
        return False
    with open(filepath, 'rb') as f:
        new_raw = f.read()
    return strip_copyright_preamble(_decode_for_compare(old_raw)) == strip_copyright_preamble(
        _decode_for_compare(new_raw)
    )


def get_git_version_with_values(filepath, base_branch):
    try:
        content = _git_show_bytes(f'{base_branch}:{filepath}')
        with tempfile.NamedTemporaryFile(suffix=".c", delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name
        try:
            return extract_enums_with_values(tmp_path)
        finally:
            os.unlink(tmp_path)
    except subprocess.CalledProcessError:
        print(f"Warning: file '{filepath}' not found in {base_branch}. Treating as new file.")
        return {}


def check_enum_values_preserved(old_list, new_list, ignore_list):
    old_dict = dict(old_list)
    new_dict = dict(new_list)

    for name, val in old_dict.items():
        if name not in new_dict:
            return False, f"enum '{name}' has been deleted"
        if name in ignore_list or '*' in ignore_list:
            continue
        if new_dict[name] != val:
            return False, f"enum '{name}' value changed from {val} to {new_dict[name]}"
    return True, None

# ignore lists for specific enums, value changes are allowed for these members
ignore_lists = {
    "_mgmt_table": {"TSDB_MGMT_TABLE_MAX"},
    "EShowType": {"TSDB_MGMT_TABLE_MAX"},
    "ESdbType": {"SDB_MAX"},
    "EQueueType": {"QUEUE_MAX"},
    "EDriverType": {"DRIVER_MAX"},
    "EGrantState": {"GRANT_STATE_MAX"},
    "EOperType": {"MND_OPER_MAX"},
    "TSFormatKeywordId": {"*"},
    "ESTriggerPullType": {"STRIGGER_PULL_TYPE_MAX"},
    "EGrantConnType": {"CONN_TYPE_DYN_MAX"},
    "SGrantOpt": {"GRANT_OPT_DYN_MAX"},
}


def check_file(filepath, base_branch):
    print(f"Checking {filepath} against {base_branch}...")
    new_enums = extract_enums_with_values(filepath)
    old_enums = get_git_version_with_values(filepath, base_branch)

    all_ok = True
    for enum_name, new_items in new_enums.items():
        if enum_name in old_enums:
            old_items = old_enums[enum_name]
            ignore_list = ignore_lists.get(enum_name, set())
            ok, err_msg = check_enum_values_preserved(old_items, new_items, ignore_list)
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
        if 'include/util/tpriv.h' in f:
            print(f"Skipping enum check for file: {f} in this release.") # PRIV_TODO
            continue
        if is_header_only_change(f, args.base_branch):
            print(f"Skipping header-only change: {f}")
            continue
        if not check_file(f, args.base_branch):
            all_ok = False

    if not all_ok:
        print("ERROR: Enum check failed: old enum members\' names and values must not be changed or deleted.")
        sys.exit(1)
    else:
        print("All enum checks passed.")
        sys.exit(0)
