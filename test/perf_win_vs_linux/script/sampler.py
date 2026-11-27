#!/usr/bin/env python3
"""Cross-platform resource sampler for the TDengine perf suite.

Both back-ends expose the same interface:

    s = make_sampler(data_dir, interval=2.0)
    s.start()
    ... run taosBenchmark ...
    m = s.stop()        # dict of aggregate metrics, or None on failure

Linux reads /proc directly (cheap, 2 s granularity).
Windows drives typeperf (5 s granularity, 8 counters) and parses its CSV.
"""

import os
import re
import sys
import time
import shutil
import subprocess

# ---------------------------------------------------------------- utilities


def _read(path, default=None):
    try:
        with open(path) as f:
            return f.read()
    except Exception:
        return default


def _pids(name):
    """PIDs whose executable name matches (Linux only)."""
    try:
        out = subprocess.check_output(['pgrep', '-x', name], text=True)
        return [int(x) for x in out.split()]
    except Exception:
        return []


# ---------------------------------------------------------------- Linux


def _block_device_for(path):
    """Resolve the parent block device name (e.g. 'sda') backing `path`."""
    path = os.path.realpath(path)
    best_mnt, best_dev = '', None
    for line in (_read('/proc/mounts', '') or '').splitlines():
        f = line.split()
        if len(f) < 2:
            continue
        dev, mnt = f[0], f[1].replace('\\040', ' ')
        if dev.startswith('/dev/') and (path == mnt or path.startswith(mnt.rstrip('/') + '/')):
            if len(mnt) > len(best_mnt):
                best_mnt, best_dev = mnt, dev
    if not best_dev:
        return None
    part = os.path.basename(best_dev)
    real = os.path.realpath(f'/sys/class/block/{part}')
    m = re.search(r'/block/([^/]+)/', real)
    return m.group(1) if m else part


def _diskstats(dev):
    """(writes_completed, bytes_written, ms_writing) for a whole disk."""
    for line in (_read('/proc/diskstats', '') or '').splitlines():
        f = line.split()
        if len(f) > 10 and f[2] == dev:
            return int(f[7]), int(f[9]) * 512, int(f[10])
    return 0, 0, 0


def _cpu_seconds(pid):
    st = _read(f'/proc/{pid}/stat')
    if not st:
        return None
    try:
        after = st.rsplit(')', 1)[1].split()
        return (int(after[11]) + int(after[12])) / os.sysconf('SC_CLK_TCK')
    except Exception:
        return None


def _status_field(pid, key):
    for line in (_read(f'/proc/{pid}/status', '') or '').splitlines():
        if line.startswith(key):
            try:
                return int(line.split()[1])
            except Exception:
                return 0
    return 0


class LinuxSampler:
    def __init__(self, data_dir, interval=2.0, out_csv=None):
        self.data_dir = data_dir
        self.interval = interval
        self.out_csv = out_csv
        self.samples = []
        self._dev = None
        self._stop = False

    def start(self):
        self._dev = _block_device_for(self.data_dir)
        self._t0 = time.time()
        self._tp = (_pids('taosd') or [None])[0]
        self._tc0 = _cpu_seconds(self._tp) if self._tp else None
        self._d0 = _diskstats(self._dev) if self._dev else (0, 0, 0)
        self._bc0 = None
        self._bc_last = None
        self._bp = None
        self._thread = None
        import threading
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _sample_once(self):
        if self._bp is None:
            cand = _pids('taosBenchmark')
            if cand:
                self._bp = cand[0]
                self._bc0 = _cpu_seconds(self._bp)
        tc = _cpu_seconds(self._tp) if self._tp else None
        bc = _cpu_seconds(self._bp) if self._bp else None
        if bc is not None:
            self._bc_last = bc
        d = _diskstats(self._dev) if self._dev else (0, 0, 0)
        self.samples.append((round(time.time() - self._t0, 1), tc,
                             _status_field(self._tp, 'VmRSS') if self._tp else None,
                             _status_field(self._tp, 'Threads') if self._tp else None,
                             bc, d[0], d[1], d[2]))

    def _loop(self):
        while not self._stop:
            self._sample_once()
            deadline = time.time() + self.interval
            while time.time() < deadline and not self._stop:
                time.sleep(0.1)

    def stop(self):
        self._stop = True
        if self._thread:
            self._thread.join(timeout=self.interval + 5)
        el = time.time() - self._t0
        tc1 = _cpu_seconds(self._tp) if self._tp else None
        bc1 = _cpu_seconds(self._bp) if self._bp else None
        if bc1 is None:
            bc1 = self._bc_last
        d1 = _diskstats(self._dev) if self._dev else (0, 0, 0)

        if self.out_csv and self.samples:
            with open(self.out_csv, 'w') as f:
                f.write('t,taosd_cpu_s,rss_kb,threads,client_cpu_s,'
                        'disk_writes,disk_bytes,disk_ms_write\n')
                for s in self.samples:
                    f.write(','.join('' if v is None else str(v) for v in s) + '\n')

        m = {'wall': el, 'samples': len(self.samples), 'device': self._dev}
        if self._tc0 is not None and tc1 is not None:
            m['taosd_cpu_cores'] = (tc1 - self._tc0) / el
        if self._bc0 is not None and bc1 is not None:
            m['client_cpu_cores'] = (bc1 - self._bc0) / el
        rss = [s[2] for s in self.samples if s[2]]
        thr = [s[3] for s in self.samples if s[3]]
        if rss:
            m['rss_avg_mb'] = sum(rss) / len(rss) / 1024
            m['rss_max_mb'] = max(rss) / 1024
        if thr:
            m['threads_avg'] = sum(thr) / len(thr)
            m['threads_max'] = max(thr)
        dw, db, dms = d1[0] - self._d0[0], d1[1] - self._d0[1], d1[2] - self._d0[2]
        if el > 0:
            m['disk_iops'] = dw / el
            m['disk_mb_s'] = db / 1024 ** 2 / el
        if dw > 0:
            m['disk_write_ms'] = dms / dw
        m['ncpu'] = os.cpu_count()
        return m

    def cancel(self):
        self._stop = True


# ---------------------------------------------------------------- Windows

def _ps(cmd):
    try:
        r = subprocess.run(['powershell', '-NoProfile', '-Command', cmd],
                           capture_output=True, text=True)
        return (r.stdout or '').strip()
    except Exception:
        return ''


def win_counters(disk_instance):
    return [
        r'\Processor(_Total)\% Processor Time',
        r'\Process(taosd)\% Processor Time',
        r'\Process(taosd)\Working Set',
        r'\Process(taosd)\Thread Count',
        r'\Process(taosBenchmark)\% Processor Time',
        r'\PhysicalDisk(%s)\Disk Writes/sec' % disk_instance,
        r'\PhysicalDisk(%s)\Disk Bytes/sec' % disk_instance,
        r'\PhysicalDisk(%s)\Avg. Disk sec/Write' % disk_instance,
    ]


class WindowsSampler:
    def __init__(self, data_dir, interval=2.0, out_csv=None):
        self.data_dir = data_dir
        self.interval = interval
        self.out_csv = out_csv
        self._proc = None
        self.t0 = None
        self.counters = None
        self.disk_instance = '_Total'

    def _typeperf(self):
        exe = shutil.which('typeperf') or r'C:\Windows\System32\typeperf.exe'
        return exe

    def _resolve_disk_instance(self):
        """Pick the PhysicalDisk perf instance that carries the data drive."""
        drive = os.path.splitdrive(self.data_dir)[0].rstrip(':').upper()
        if not drive:
            return '_Total'
        out = _ps("Get-CimInstance Win32_PerfFormattedData_PerfDisk_PhysicalDisk | "
                  "Select-Object -ExpandProperty Name")
        for name in out.splitlines():
            name = name.strip()
            if drive + ':' in name.upper():
                return name
        return '_Total'

    def _data_rows(self):
        try:
            with open(self.out_csv, 'r', errors='replace') as f:
                return max(0, sum(1 for ln in f if ln.startswith('"')) - 0)
        except OSError:
            return 0

    def start(self):
        csv = self.out_csv or os.path.join(os.environ.get('TEMP', '.'), 'counters.csv')
        self.out_csv = csv
        if os.path.exists(csv):
            os.remove(csv)
        self.disk_instance = self._resolve_disk_instance()
        self.counters = win_counters(self.disk_instance)
        argv = [self._typeperf()] + self.counters + [
            '-si', str(int(self.interval)), '-o', csv, '-y']
        self._proc = subprocess.Popen(argv,
                                      stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL)
        # PDH emits an all-blank first sample; wait for the first valid row so
        # that even a very short benchmark still yields resource data.
        deadline = time.time() + max(20.0, self.interval * 6)
        while time.time() < deadline:
            if self._data_rows() >= 2:
                break
            time.sleep(0.5)
        self.t0 = time.time()

    def stop(self):
        if self._proc:
            try:
                self._proc.terminate()
                self._proc.wait(timeout=10)
            except Exception:
                try:
                    self._proc.kill()
                except Exception:
                    pass
        time.sleep(2)
        el = (time.time() - self.t0) if self.t0 else 0.0
        m = {'wall': el, 'samples': 0, 'ncpu': os.cpu_count()}
        if not self.out_csv or not os.path.exists(self.out_csv):
            return m
        try:
            with open(self.out_csv, 'r', errors='replace') as f:
                lines = f.read().splitlines()
        except Exception:
            return m
        if len(lines) < 2:
            return m
        names = re.findall(r'"([^"]*)"', lines[0])
        cols = [[] for _ in names]
        rows_seen = 0
        for row in lines[1:]:
            if not row.startswith('"'):
                continue
            vals = re.findall(r'"([^"]*)"', row)
            rows_seen += 1
            for i in range(1, min(len(names), len(vals))):
                cell = vals[i].strip()
                if not cell:
                    continue
                try:
                    cols[i].append(float(cell))
                except ValueError:
                    pass
        keys = ['sys_cpu_pct', 'taosd_cpu_pct', 'rss_bytes', 'threads',
                'client_cpu_pct', 'disk_iops', 'disk_bytes_s', 'disk_sec_write']
        for i, k in enumerate(keys, start=1):
            if i < len(cols) and cols[i]:
                avg = sum(cols[i]) / len(cols[i])
                m[k + '_avg'] = avg
                m[k + '_max'] = max(cols[i])
        m['samples'] = rows_seen
        m['disk_instance'] = self.disk_instance
        if 'taosd_cpu_pct_avg' in m:
            m['taosd_cpu_cores'] = m['taosd_cpu_pct_avg'] / 100.0
        if 'client_cpu_pct_avg' in m:
            m['client_cpu_cores'] = m['client_cpu_pct_avg'] / 100.0
        if 'rss_bytes_avg' in m:
            m['rss_avg_mb'] = m['rss_bytes_avg'] / 1024 ** 2
            m['rss_max_mb'] = m.get('rss_bytes_max', 0) / 1024 ** 2
        if 'threads_avg' in m:
            m['threads_max'] = m.get('threads_max', m['threads_avg'])
        if 'disk_bytes_s_avg' in m:
            m['disk_mb_s'] = m['disk_bytes_s_avg'] / 1024 ** 2
        if 'disk_iops_avg' in m:
            m['disk_iops'] = m['disk_iops_avg']
        if 'disk_sec_write_avg' in m:
            m['disk_write_ms'] = m['disk_sec_write_avg'] * 1000.0
        return m

    def cancel(self):
        self.stop()


def make_sampler(data_dir, interval=None, out_csv=None):
    if sys.platform.startswith('win'):
        return WindowsSampler(data_dir, interval or 2.0, out_csv)
    return LinuxSampler(data_dir, interval or 2.0, out_csv)
