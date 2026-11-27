#!/usr/bin/env python3
"""TDengine TSDB performance suite - single entry point.

Runs the complete Windows/Linux comparison workload and emits a Markdown
report. The same command works on both platforms; everything platform
specific is resolved internally.

Typical use:

    python3 script/main.py --package /path/to/TDengine-server-3.x-Linux-x64.tar.gz

    python  script\\main.py --package C:\\pkgs\\TDengine-server-3.x-Windows-x64.exe

Phases (all can be skipped):
    install   install the given package (skipped with --skip-install)
    writes    W1 taosc + W2 stmt2, each cold then hot, 1e9 rows per round
    queries   Q1..Q5 on the single warm dataset left by the last write round
    report    write results/<tag>/result.json and report/<tag>.md

Examples:
    # only re-run the query section against existing data (~25 min)
    python3 script/main.py --skip-install --only queries

    # quick smoke test of two queries
    python3 script/main.py --skip-install --only queries --queries Q3,Q4
"""

import argparse
import datetime
import json
import os
import platform as pyplatform
import re
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import benchlog            # noqa: E402
import installer           # noqa: E402
import report as reportmod  # noqa: E402
import sampler as samplermod  # noqa: E402

CREATE_SQL = (
    "CREATE DATABASE perfdb PRECISION 'ms' VGROUPS 8 REPLICA 1 "
    "DURATION 10d KEEP 3650d CACHEMODEL 'none' CACHESIZE 1 BUFFER 256 "
    "PAGES 256 PAGESIZE 4 STT_TRIGGER 1 MINROWS 100 MAXROWS 4096 WAL_LEVEL 1")

IS_WIN = sys.platform.startswith('win')

QUERY_ORDER = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']


# ------------------------------------------------------------------ helpers

def log(msg=''):
    print(msg, flush=True)


def now():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _run(cmd, env=None, timeout=None, capture=True):
    log('  $ ' + (cmd if isinstance(cmd, str) else ' '.join(cmd)))
    try:
        return subprocess.run(
            cmd, shell=isinstance(cmd, str), env=env, timeout=timeout,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.STDOUT if capture else None, text=True)
    except subprocess.TimeoutExpired:
        log('  ! timed out')
        return None


def dir_size_mb(path):
    if not os.path.isdir(path):
        return 0.0
    total = 0
    for dirpath, _dirnames, filenames in os.walk(path):
        for fn in filenames:
            try:
                total += os.path.getsize(os.path.join(dirpath, fn))
            except OSError:
                pass
    return total / 1024 ** 2


# ------------------------------------------------------------------ environment

def _ps(cmd):
    r = subprocess.run(['powershell', '-NoProfile', '-Command', cmd],
                       capture_output=True, text=True)
    return (r.stdout or '').strip()


def gather_env(home, bin_dir, cfg_dir):
    env = {
        'platform': 'Windows' if IS_WIN else 'Linux',
        'ncpu': os.cpu_count(),
        'data_dir': os.path.join(home, 'data') if IS_WIN else '/var/lib/taos',
    }
    if IS_WIN:
        env['os'] = _ps("(Get-CimInstance Win32_OperatingSystem).Caption") or pyplatform.platform()
        env['kernel'] = 'build ' + (_ps("(Get-CimInstance Win32_OperatingSystem).BuildNumber") or '?')
        env['cpu'] = _ps("(Get-CimInstance Win32_Processor | Select-Object -First 1).Name")
        try:
            kb = int(_ps("(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory") or 0)
            env['mem_gb'] = kb / 1024 ** 3
        except ValueError:
            pass
        env['disk'] = _ps(
            "$dn = (Get-Partition -DriveLetter %s -EA SilentlyContinue).DiskNumber; "
            "Get-CimInstance Win32_DiskDrive | Where-Object { $_.Index -eq $dn } | "
            "Select-Object -First 1 | ForEach-Object { "
            "$_.Model + ' / ' + [math]::Round($_.Size/1GB) + ' GB' }"
            % os.path.splitdrive(env['data_dir'])[0].rstrip(':'))
        if not env.get('disk'):
            env['disk'] = _ps(
                "Get-CimInstance Win32_DiskDrive | Select-Object -First 1 | ForEach-Object { "
                "$_.Model + ' / ' + [math]::Round($_.Size/1GB) + ' GB' }")
        bt = _ps("(Get-CimInstance Win32_OperatingSystem).LastBootUpTime"
                 ".ToString('yyyy-MM-dd HH:mm:ss')")
        if bt:
            env['uptime'] = 'boot ' + bt
        env['governor'] = _ps(
            "(Get-CimInstance -Namespace root/cimv2/power -ClassName Win32_PowerPlan "
            "-Filter \"IsActive=True\" -EA SilentlyContinue).ElementName")
    else:
        try:
            for line in open('/etc/os-release'):
                if line.startswith('PRETTY_NAME='):
                    env['os'] = line.split('=', 1)[1].strip().strip('"')
        except OSError:
            env['os'] = pyplatform.platform()
        env['kernel'] = pyplatform.release()
        try:
            for line in open('/proc/cpuinfo'):
                if 'model name' in line:
                    env['cpu'] = line.split(':', 1)[1].strip()
                    break
        except OSError:
            pass
        try:
            for line in open('/proc/meminfo'):
                if line.startswith('MemTotal'):
                    env['mem_gb'] = int(line.split()[1]) / 1024 ** 2
                    break
        except OSError:
            pass
        try:
            dev = samplermod._block_device_for(env['data_dir'])
            out = subprocess.run(['lsblk', '-dno', 'MODEL,SIZE,ROTA', '/dev/' + dev],
                                 capture_output=True, text=True).stdout.strip()
            env['disk'] = '%s (/dev/%s) %s' % (env['disk'], dev, out) if env.get('disk') else \
                '/dev/%s %s' % (dev, out)
        except Exception:
            pass
        try:
            env['governor'] = open(
                '/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor').read().strip()
        except OSError:
            env['governor'] = 'n/a'
        try:
            env['uptime'] = subprocess.run(['uptime', '-p'], capture_output=True,
                                           text=True).stdout.strip()
        except Exception:
            pass
    env['version'] = installer.installed_version(bin_dir)
    bench = os.path.join(bin_dir, 'taosBenchmark.exe' if IS_WIN else 'taosBenchmark')
    if os.path.exists(bench):
        try:
            out = subprocess.run([bench, '-V'], capture_output=True, text=True, timeout=30).stdout
            m = re.search(r'(\d+\.\d+\.\d+\.\d+[^\s]*)', out)
            env['bench_version'] = m.group(1) if m else out.strip().splitlines()[-1]
        except Exception:
            env['bench_version'] = '?'
    return env


def bench_env(home):
    env = os.environ.copy()
    if not IS_WIN:
        driver = os.path.join(home, 'driver')
        ld = env.get('LD_LIBRARY_PATH', '')
        parts = [p for p in ld.split(':') if p]
        for p in (driver, os.path.join(home, 'lib')):
            if os.path.isdir(p) and p not in parts:
                parts.insert(0, p)
        if parts:
            env['LD_LIBRARY_PATH'] = ':'.join(parts)
    return env


# ------------------------------------------------------------------ database

def sql(home, statement, timeout=180):
    exe = os.path.join(home, 'taos.exe' if IS_WIN else 'bin/taos')
    if not IS_WIN:
        exe = os.path.join(home, 'bin', 'taos')
    r = _run([exe, '-s', statement], env=bench_env(home), timeout=timeout)
    return r


def recreate_db(home, data_dir, cold):
    log('  dropping database ...')
    sql(home, 'DROP DATABASE IF EXISTS perfdb')
    deadline = time.time() + 420
    interval = 15 if IS_WIN else 5
    while time.time() < deadline:
        size = dir_size_mb(data_dir)
        if size < 2048:
            break
        time.sleep(interval)
    else:
        log('  ! drop did not reclaim space in time, continuing')

    if cold:
        log('  cold start: restarting taosd' + ('' if IS_WIN else ' + dropping page cache'))
        installer.restart(home)
        if not IS_WIN:
            _run('sync')
            try:
                with open('/proc/sys/vm/drop_caches', 'w') as f:
                    f.write('3\n')
            except OSError as e:
                log('  ! cannot drop caches: %s' % e)
        time.sleep(3)

    log('  creating database ...')
    r = sql(home, CREATE_SQL)
    if r and r.stdout and 'OK' not in r.stdout:
        log('  ! create output: ' + r.stdout.strip()[:200])


# ------------------------------------------------------------------ configs

def materialize(src, dst, cfg_dir):
    with open(src, encoding='utf-8') as f:
        cfg = json.load(f)
    cfg['cfgdir'] = cfg_dir
    with open(dst, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, indent=2)
    return dst


# ------------------------------------------------------------------ bench run

def run_bench(cfg, label, workdir, home, data_dir, recreate=False, cold=False):
    """Run one taosBenchmark config under resource sampling."""
    log('=== %s ===' % label)
    if recreate:
        recreate_db(home, data_dir, cold)
    elif cold:
        log('  cold start: restarting taosd')
        installer.restart(home)
        if not IS_WIN:
            try:
                with open('/proc/sys/vm/drop_caches', 'w') as f:
                    f.write('3\n')
            except OSError:
                pass

    bench = os.path.join(home, 'taosBenchmark.exe' if IS_WIN else 'bin/taosBenchmark')
    if not IS_WIN:
        bench = os.path.join(home, 'bin', 'taosBenchmark')
    logfile = os.path.join(workdir, label + '.log')
    csvfile = os.path.join(workdir, label + '.samples.csv')

    s = samplermod.make_sampler(data_dir, out_csv=csvfile)
    t0 = now()
    s.start()
    t_start = time.time()
    r = _run([bench, '-f', cfg, '-o', logfile], env=bench_env(home), capture=True)
    wall = time.time() - t_start
    metrics = s.stop() or {}
    if r is not None and r.stdout:
        with open(os.path.join(workdir, label + '.out'), 'w', encoding='utf-8') as f:
            f.write(r.stdout)
    log('  wall %.1f s  (rc=%s)' % (wall, r.returncode if r else 'timeout'))
    return {
        'label': label,
        'bench_exit': (r.returncode if r else None),
        'wall': round(wall, 1),
        'started': t0,
        'res': metrics,
        'log': logfile,
    }


def parse_insert_round(entry):
    entry['insert'] = benchlog.parse_insert(benchlog.read_log(entry['log']))
    return entry


def parse_query_round(entry):
    entry['parse'] = benchlog.parse_query(benchlog.read_log(entry['log']))
    return entry


# ------------------------------------------------------------------ main

def main():
    ap = argparse.ArgumentParser(
        description='TDengine TSDB performance suite (writes + queries + report)',
        epilog='example: python3 main.py /path/to/TDengine-server-3.x-Linux-x64.tar.gz')
    ap.add_argument('positional_package', nargs='?', metavar='PACKAGE',
                    help='installer package (same as --package)')
    ap.add_argument('--package', help='installer package (.exe/.deb/.rpm/.tar.gz/.tar.xz)')
    ap.add_argument('--skip-install', action='store_true',
                    help='use the already installed TDengine')
    ap.add_argument('--home', help='TDengine home (default: auto-detect)')
    ap.add_argument('--config-dir', default=os.path.join(ROOT, 'config'))
    ap.add_argument('--report-dir', default=os.path.join(ROOT, 'report'))
    ap.add_argument('--result-dir', default=os.path.join(ROOT, 'results'))
    ap.add_argument('--tag', help='run tag, defaults to <platform>_<timestamp>')
    ap.add_argument('--only', choices=['all', 'writes', 'queries'], default='all')
    ap.add_argument('--skip-cold', action='store_true',
                    help='run only the hot rounds of the write phase')
    ap.add_argument('--write-modes', help='comma separated subset of write modes: taosc,stmt2')
    ap.add_argument('--queries', help='comma separated subset, e.g. Q1,Q5')
    ap.add_argument('--query-rounds', type=int, default=1,
                    help='how many times to repeat the whole query set')
    ap.add_argument('--baseline', help='result.json of the other platform for comparison')
    args = ap.parse_args()
    if not args.package and args.positional_package:
        args.package = args.positional_package

    home, bin_dir, cfg_dir = (args.home, args.home, None) if args.home else installer.detect_home()
    if args.home and not cfg_dir:
        cfg_dir = os.path.join(args.home, 'cfg') if IS_WIN else '/etc/taos'
        bin_dir = args.home if IS_WIN else os.path.join(args.home, 'bin')

    tag = args.tag or '%s_%s' % ('windows' if IS_WIN else 'linux',
                                 datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
    workdir = os.path.join(args.result_dir, tag)
    os.makedirs(workdir, exist_ok=True)
    os.makedirs(args.report_dir, exist_ok=True)

    started = now()
    log('================ TDengine perf suite ================')
    log('platform : %s' % ('Windows' if IS_WIN else 'Linux'))
    log('home     : %s' % home)
    log('tag      : %s' % tag)
    log('workdir  : %s' % workdir)

    if not args.skip_install and args.package:
        if not installer.install(args.package):
            log('ERROR: install failed')
            return 1
        home, bin_dir, cfg_dir = installer.detect_home()
    if not args.skip_install and not args.package:
        log('! no --package given, using the installed TDengine')

    if not installer.start(home):
        log('ERROR: taosd is not running')
        return 1

    env = gather_env(home, bin_dir, cfg_dir)
    env['package'] = os.path.abspath(args.package) if args.package else None
    env['start'] = started
    log('version  : %s   benchmark: %s' % (env.get('version'), env.get('bench_version')))

    result = dict(env)
    result['tag'] = tag
    result['writes'] = []
    result['queries'] = []

    # ---------------- writes
    if args.only in ('all', 'writes'):
        rounds = [
            ('w1_cold', 'insert.json', 'taosc', True),
            ('w1_hot', 'insert.json', 'taosc', False),
            ('w2_cold', 'insert_stmt2.json', 'stmt2', True),
            ('w2_hot', 'insert_stmt2.json', 'stmt2', False),
        ]
        if args.skip_cold:
            rounds = [r for r in rounds if not r[3]]
        if args.write_modes:
            wanted = {m.strip().lower() for m in args.write_modes.split(',') if m.strip()}
            unknown = wanted - {'taosc', 'stmt2'}
            if unknown:
                log('! unknown --write-modes value(s): %s (allowed: taosc,stmt2)'
                    % ','.join(sorted(unknown)))
            rounds = [r for r in rounds if r[2] in wanted]
        if not rounds:
            log('! no write round selected, skipping the write phase')
        for label, cfgname, mode, cold in rounds:
            src = os.path.join(args.config_dir, cfgname)
            dst = materialize(src, os.path.join(workdir, label + '.json'), cfg_dir)
            e = run_bench(dst, label, workdir, home, env['data_dir'],
                          recreate=True, cold=cold)
            e['mode'] = mode
            e['round'] = 'cold' if cold else 'hot'
            result['writes'].append(parse_insert_round(e))
        with open(os.path.join(workdir, 'partial.json'), 'w') as f:
            json.dump(result, f, indent=2)

    # ---------------- queries
    if args.only in ('all', 'queries'):
        wanted = QUERY_ORDER
        if args.queries:
            wanted = [q.strip().upper() for q in args.queries.split(',') if q.strip()]
        for rep in range(args.query_rounds):
            for qname in wanted:
                src = os.path.join(args.config_dir, qname + '.json')
                if not os.path.exists(src):
                    log('! skip %s (config missing)' % qname)
                    continue
                with open(src, encoding='utf-8') as f:
                    qcfg = json.load(f)
                label = qname if args.query_rounds == 1 else '%s_r%d' % (qname, rep + 1)
                dst = materialize(src, os.path.join(workdir, label + '.json'), cfg_dir)
                e = run_bench(dst, label, workdir, home, env['data_dir'])
                e['sql'] = (qcfg.get('specified_table_query', {}).get('sqls') or [{}])[0].get('sql', '')
                e['times'] = qcfg.get('query_times')
                e['threads'] = qcfg.get('specified_table_query', {}).get('threads')
                result['queries'].append(parse_query_round(e))
        with open(os.path.join(workdir, 'partial.json'), 'w') as f:
            json.dump(result, f, indent=2)

    result['end'] = now()
    result_path = os.path.join(workdir, 'result.json')
    with open(result_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2)

    md_path = os.path.join(args.report_dir, tag + '.md')
    baseline = None
    if args.baseline:
        try:
            baseline = json.load(open(args.baseline, encoding='utf-8'))
        except OSError:
            log('! baseline not readable: %s' % args.baseline)
    reportmod.save(result, md_path, baseline)

    log('')
    log('result : %s' % result_path)
    log('report : %s' % md_path)
    log('done at: %s' % result['end'])
    return 0


if __name__ == '__main__':
    sys.exit(main())
