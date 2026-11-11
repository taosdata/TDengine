#!/usr/bin/env python3
"""Install a TDengine TSDB package and bring the server up.

Supported package types:
  Windows  .exe                     (Inno Setup silent install)
  Linux    .deb / .rpm              (dpkg / rpm)
  Linux    .tar.gz / .tar.xz        (extract, then ./install.sh -e no)

The module is import-safe; `main.py` calls `install()` and `ensure_running()`.
"""

import os
import re
import sys
import time
import glob
import shutil
import subprocess
import tarfile
import tempfile


def log(msg):
    print(msg, flush=True)


def _run(cmd, **kw):
    log('  $ ' + (cmd if isinstance(cmd, str) else ' '.join(cmd)))
    return subprocess.run(cmd, shell=isinstance(cmd, str), **kw)


def detect_home():
    """Return (home_dir, bin_dir, cfg_dir) for the installed TDengine."""
    if sys.platform.startswith('win'):
        home = os.environ.get('TDENGINE_HOME') or r'C:\TDengine'
        return home, home, os.path.join(home, 'cfg')
    for home in ('/usr/local/taos', '/opt/taos'):
        if os.path.isdir(os.path.join(home, 'bin')):
            return home, os.path.join(home, 'bin'), '/etc/taos'
    return '/usr/local/taos', '/usr/local/taos/bin', '/etc/taos'


def installed_version(bin_dir):
    exe = os.path.join(bin_dir, 'taosd.exe' if sys.platform.startswith('win') else 'taosd')
    if not os.path.exists(exe):
        return None
    try:
        out = subprocess.run([exe, '-V'], capture_output=True, text=True, timeout=30).stdout
        m = re.search(r'(\d+\.\d+\.\d+\.\d+)', out)
        return m.group(1) if m else out.strip().splitlines()[-1]
    except Exception:
        return None


def _install_windows(pkg):
    log('installing (Inno Setup silent): %s' % pkg)
    ps = ('Start-Process -FilePath "%s" -ArgumentList '
          "'/VERYSILENT','/SUPPRESSMSGBOXES','/NORESTART','/SP-' -Wait -PassThru "
          '| ForEach-Object { "installer_exit=" + $_.ExitCode }') % pkg
    r = subprocess.run(['powershell', '-NoProfile', '-Command', ps],
                       capture_output=True, text=True)
    log('  ' + (r.stdout or '').strip() or '  (no output)')
    return r.returncode == 0


def _install_tar(pkg):
    work = tempfile.mkdtemp(prefix='tdinstall_')
    log('extracting %s -> %s' % (pkg, work))
    if pkg.endswith('.xz') or pkg.endswith('.txz'):
        subprocess.run(['tar', '-xJf', pkg, '-C', work], check=True)
    elif pkg.endswith('.tar.gz') or pkg.endswith('.tgz'):
        subprocess.run(['tar', '-xzf', pkg, '-C', work], check=True)
    else:
        with tarfile.open(pkg) as t:
            t.extractall(work)
    cands = glob.glob(os.path.join(work, '**', 'install.sh'), recursive=True)
    if not cands:
        log('  ERROR: install.sh not found inside the package')
        return False
    script = cands[0]
    os.chmod(script, 0o755)
    r = subprocess.run([script, '-e', 'no'], cwd=os.path.dirname(script))
    return r.returncode == 0


def _install_deb(pkg):
    r = _run(['dpkg', '-i', pkg])
    if r.returncode != 0:
        _run(['apt-get', '-f', 'install', '-y'])
    return r.returncode == 0


def _install_rpm(pkg):
    return _run(['rpm', '-Uvh', '--replacepkgs', pkg]).returncode == 0


def install(pkg):
    """Install `pkg`. Returns True on success."""
    if not pkg:
        return True
    pkg = os.path.abspath(os.path.expanduser(pkg))
    if not os.path.exists(pkg):
        log('ERROR: package not found: %s' % pkg)
        return False
    log('=== install: %s ===' % pkg)
    low = pkg.lower()
    if sys.platform.startswith('win'):
        if low.endswith('.exe'):
            return _install_windows(pkg)
        if low.endswith('.zip'):
            home, _, _ = detect_home()
            os.makedirs(home, exist_ok=True)
            import zipfile
            with zipfile.ZipFile(pkg) as z:
                z.extractall(home)
            return True
        log('ERROR: unsupported Windows package type')
        return False
    if low.endswith('.deb'):
        return _install_deb(pkg)
    if low.endswith('.rpm'):
        return _install_rpm(pkg)
    if any(low.endswith(s) for s in ('.tar.gz', '.tgz', '.tar.xz', '.txz', '.tar')):
        return _install_tar(pkg)
    log('ERROR: unsupported package type')
    return False


def is_running():
    if sys.platform.startswith('win'):
        r = subprocess.run(['powershell', '-NoProfile', '-Command',
                            '(Get-Service taosd).Status'],
                           capture_output=True, text=True)
        return 'Running' in (r.stdout or '')
    return subprocess.run(['pgrep', '-x', 'taosd'],
                          capture_output=True).returncode == 0


def start(home=None):
    home = home or detect_home()[0]
    if is_running():
        return True
    log('starting taosd ...')
    if sys.platform.startswith('win'):
        subprocess.run(['powershell', '-NoProfile', '-Command',
                        'Start-Service taosd'], capture_output=True)
    else:
        r = subprocess.run(['systemctl', 'start', 'taosd'], capture_output=True)
        if r.returncode != 0:
            daemon = os.path.join(home, 'bin', 'taosd')
            subprocess.Popen([daemon], stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, start_new_session=True)
    for _ in range(60):
        if is_running():
            time.sleep(3)
            return True
        time.sleep(2)
    return is_running()


def stop(home=None):
    home = home or detect_home()[0]
    if sys.platform.startswith('win'):
        subprocess.run(['powershell', '-NoProfile', '-Command',
                        'Stop-Service taosd -Force'], capture_output=True)
    else:
        if subprocess.run(['systemctl', 'stop', 'taosd'],
                          capture_output=True).returncode != 0:
            subprocess.run(['pkill', '-x', 'taosd'], capture_output=True)
    for _ in range(60):
        if not is_running():
            return True
        time.sleep(2)
    return not is_running()


def restart(home=None):
    stop(home)
    return start(home)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('usage: installer.py <package>')
        sys.exit(2)
    ok = install(sys.argv[1]) and start()
    home, b, c = detect_home()
    print('home=%s version=%s' % (home, installed_version(b)))
    sys.exit(0 if ok else 1)
