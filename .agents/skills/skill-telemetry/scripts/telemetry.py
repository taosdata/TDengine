#!/usr/bin/env python3
"""skill-telemetry client. Usage: telemetry.py <name> <version> <author>"""
import json, os, platform, socket, subprocess, sys, urllib.request

def _agent():
    env = {
        'CLAUDE_AGENT_NAME': None, 'CODEX_AGENT_NAME': None,
        'COPILOT_AGENT_NAME': None, 'CLAUDECODE': 'claude-code',
        'CURSOR_TRACE_ID': 'cursor', 'AIDER': 'aider',
        'WARP_CLIENT_VERSION': 'warp', 'ZED_TERM': 'zed',
        'GEMINI_CLI': 'gemini-cli',
    }
    for k, fixed in env.items():
        v = os.environ.get(k, '')
        if v:
            return v if fixed is None else fixed
    tp = os.environ.get('TERM_PROGRAM', '')
    if tp in ('Windsurf', 'WindsurfNext'):
        return 'windsurf'
    return 'unknown'

def _local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
        s.close()
        if ip and ip != '127.0.0.1':
            return ip
    except Exception:
        pass
    return 'unknown'

def _distro():
    s = platform.system().lower()
    if s == 'linux':
        try:
            with open('/etc/os-release') as f:
                kv = dict(l.strip().split('=', 1) for l in f if '=' in l)
            return '{} {}'.format(
                kv.get('NAME', 'unknown').strip('"'),
                kv.get('VERSION_ID', '').strip('"')).strip()
        except Exception:
            return 'unknown'
    if s == 'darwin':
        def _sw(key):
            try:
                return subprocess.check_output(
                    ['sw_vers', key], timeout=2,
                    stderr=subprocess.DEVNULL).decode().strip()
            except Exception:
                return ''
        d = '{} {}'.format(_sw('-productName'), _sw('-productVersion')).strip()
        return d or 'unknown'
    return 'unknown'

def send(name, version, author):
    url = os.environ.get('SKILL_TELEMETRY_URL',
                         'https://teleskills.tdengine.net')
    payload = json.dumps({
        'name': name, 'version': version, 'author': author,
        'agent': _agent(), 'os': platform.system().lower(),
        'distro': _distro(), 'local_ip': _local_ip(),
    }).encode()
    try:
        req = urllib.request.Request(
            url + '/api/v1/skills/telemetry',
            data=payload, headers={'Content-Type': 'application/json'})
        urllib.request.urlopen(req, timeout=3)
    except Exception:
        pass

if __name__ == '__main__':
    if len(sys.argv) < 4:
        sys.exit(0)
    send(sys.argv[1], sys.argv[2], sys.argv[3])
