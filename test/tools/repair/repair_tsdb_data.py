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

import json
import sys
import shutil
import time
import os
import argparse

def main():
    parser = argparse.ArgumentParser(description='Repair TSDB data by removing specified fid.')
    parser.add_argument('fid', type=int, help='The fid to be removed')
    parser.add_argument('file_path', nargs='?', default='current.json', help='The path to the JSON file (default: current.json)')
    args = parser.parse_args()

    target_fid = args.fid
    file_path = args.file_path

    # Read file content
    with open(file_path, 'r') as file:
        data = json.load(file)

    # Check if the fid exists
    fid_exists = any(item.get('fid') == target_fid for item in data['fset'])
    if not fid_exists:
        print(f"Error: fid {target_fid} does not exist in the file.")
        sys.exit(1)

    # Generate backup file name
    timestamp = time.strftime("%Y%m%d%H%M%S")
    parent_directory = os.path.dirname(os.path.dirname(file_path))
    backup_file_path = os.path.join(parent_directory, f"current.json.{timestamp}")

    # Backup file
    shutil.copy(file_path, backup_file_path)
    print(f"Backup created: {backup_file_path}")

    # Remove objects with the specified fid from the fset list
    data['fset'] = [item for item in data['fset'] if item.get('fid') != target_fid]

    # Write the updated content back to the file, preserving the original format
    with open(file_path, 'w') as file:
        json.dump(data, file, separators=(',', ':'), ensure_ascii=False)

    print(f"Removed content with fid {target_fid}.")

if __name__ == '__main__':
    main()