import json
import sys
import shutil
import time
import os

if len(sys.argv) < 2:
    print("Usage: python3 delete_fid.py <fid> [file_path]")
    sys.exit(1)

target_fid = int(sys.argv[1])
file_path = sys.argv[2] if len(sys.argv) > 2 else "current.json"

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