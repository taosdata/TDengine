#!/usr/bin/env python3
import os
import sys
import json
import re
import subprocess
from datetime import datetime

def update_json(file, key, new_value):
    with open(file, 'r+') as f:
        content = f.read()
        pattern = f'"{key}": ".*?"'
        replacement = f'"{key}": "{new_value}"'
        content = re.sub(pattern, replacement, content)
        f.seek(0)
        f.write(content)
        f.truncate()

newv = sys.argv[1]
print(newv)

ci = os.path.realpath(os.path.dirname(sys.argv[0]))

update_json('package.json', 'version', newv)
update_json('src/plugin.json', 'version', newv)
update_json('src/plugin.json', 'updated', datetime.now().strftime('%Y-%m-%d'))

# Uncomment the following lines if you want to use git-cliff
# subprocess.run(['wget', '-c', 'https://github.com/orhun/git-cliff/releases/download/v0.7.0/git-cliff-0.7.0-x86_64-unknown-linux-musl.tar.gz'])
# subprocess.run(['tar', 'xvf', 'git-cliff-0.7.0-x86_64-unknown-linux-musl.tar.gz'])
# subprocess.run(['install', 'git-cliff-0.7.0/git-cliff', '/usr/bin/git-cliff'])

# subprocess.run(['git', 'cliff', '-t', f'v{newv}'], stdout=subprocess.DEVNULL)

subprocess.run(['git', 'config', 'user.email'], check=False) or subprocess.run(['git', 'config', 'user.email', 'github-actions@github.com'])
subprocess.run(['git', 'config', 'user.name'], check=False) or subprocess.run(['git', 'config', 'user.name', 'GitHub Actions [bot]'])

subprocess.run(['git', 'commit', '-a', '-m', f'chore(release): bump v{newv}'])
subprocess.run(['git', 'tag', f'v{newv}'])
