# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Exercise the count-window deployment through independent native CLI sessions."""

import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import tempfile
import time


def create_stream(name):
    return (
        f"create stream testdb.{name} count_window(2,1) from testdb.st "
        "partition by tbname,pile_code,gun_code stream_options(pre_filter(trade_no like 'T_CLEAN_%')) "
        f"into testdb.{name}_out output_subtable(concat('{name}_',%%1)) "
        "as select _twend as ts,last(soc) as soc,first(soc) as prev_soc,last(trade_no) as trade_no "
        "from %%tbname where ts >= _twstart and ts <= _twend and trade_no like 'T_CLEAN_%' "
        "and ts >= (select min(ts) from %%tbname where soc != 0 and ts <= _twstart "
        "and trade_no = (select last(trade_no) from %%tbname where ts >= _twstart and ts <= _twend))"
    )


def wait(description, probe, timeout=80):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = probe()
        if value:
            return value
        time.sleep(0.25)
    raise AssertionError(f"timed out waiting for {description}")


def rows(text):
    return [tuple(c.strip() for c in line.strip().strip('|').split('|'))
            for line in text.splitlines() if line.rstrip().endswith('|')][1:]


class HealthyHeartbeats:
    def __init__(self, stream_id):
        self.trigger = f' {stream_id} Trigger TASK:'
        self.requests = {}

    def completed(self, line):
        fields = line.split()
        if len(fields) < 5:
            return False
        thread = fields[2]
        if 'start to process stream hb req msg' in line:
            self.requests.pop(thread, None)
        elif start := re.search(r'start to process grp (\d+) stream-hb from dnode:1,', line):
            self.requests[thread] = (start.group(1), False)
        elif thread in self.requests:
            group, healthy = self.requests[thread]
            if self.trigger in line and 'task status Running got' in line:
                self.requests[thread] = (group, True)
            elif end := re.search(r'end to process stream hb req msg, gid:(\d+), response code:(-?\d+)\s*$', line):
                del self.requests[thread]
                return healthy and group == end.group(1) and end.group(2) == '0'
        return False


class SnodeHealthScan:
    def __init__(self, earliest):
        self.earliest = earliest
        self.checked_at = None

    def completed(self, line):
        online = re.search(r'snode 1 online, try to check tasks status, currTs:(\d+),', line)
        if online:
            self.checked_at = int(online.group(1))
        end = re.search(r'end health check, soltIdx:1, checkStartTs:(\d+)', line)
        return bool(end and self.checked_at == int(end.group(1)) and self.checked_at >= self.earliest)


def available_ports():
    # The server config accepts ports through 65056. Give the native and MQTT listeners
    # separate ports in the same fixture, including when another taosd is already running.
    for _ in range(128):
        with socket.socket() as rpc, socket.socket() as mqtt:
            rpc.bind(('', 0))
            port = rpc.getsockname()[1]
            if port >= 65056:
                continue
            try:
                mqtt.bind(('', port + 1))
            except OSError:
                continue
            return port, port + 1
    raise RuntimeError('could not reserve two local test ports')


def run_case(root, daemon, shell):
    port, mqtt_port = available_ports()
    for name in ('cfg', 'data', 'log', 'tmp'):
        (root / name).mkdir()
    (root / 'cfg' / 'taos.cfg').write_text(
        f'fqdn localhost\nfirstEp localhost:{port}\nserverPort {port}\nmqttPort {mqtt_port}\n'
        'dataDir ./data\nlogDir ./log\ntempDir ./tmp\nnumOfCores 4\n'
        'numOfMnodeStreamMgmtThreads 2\nstDebugFlag 135\nasyncLog 0\nnumOfLogLines 10000000\n'
        'udf 0\nmonitor 0\ntelemetryReporting 0\n', encoding='utf-8')
    env = dict(os.environ, PATH=str(shell.parent) + os.pathsep + os.environ.get('PATH', ''))
    console = root / 'daemon-console.log'
    with console.open('wb') as output:
        server = subprocess.Popen([str(daemon), '.', 'paused', '-c', 'cfg'],
                                  cwd=root, env=env, stdout=output, stderr=subprocess.STDOUT)
    try:
        def sql(statement, ready=True):
            if server.poll() is not None:
                raise RuntimeError(f'test daemon exited with {server.returncode}')
            command = [str(shell), '-c', 'cfg', '-h', 'localhost', '-P', str(port), '-w', '1024', '-s', statement]
            try:
                cmd = subprocess.run(command, cwd=root, env=env, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, timeout=12)
            except subprocess.TimeoutExpired:
                if ready:
                    raise
                return ''
            text = cmd.stdout.decode('utf-8', errors='replace')
            if cmd.returncode or re.search(r'(?im)^\s*(?:DB error|Error:|failed to connect)', text):
                if ready:
                    raise RuntimeError(f'{statement}\n{text}')
                return ''
            return text

        def task_ids():
            result = rows(sql("select stream_id,type,task_id,serious_id,status from "
                              "information_schema.ins_stream_tasks where stream_name='healthy'"))
            triggers = [row for row in result if row[1] == 'Trigger']
            runners = [row for row in result if row[1] == 'Runner']
            if len(triggers) == 1 and triggers[0][4] == 'Running' and runners:
                if all(row[4] in ('Idle', 'Running') for row in runners):
                    return sorted(row[:4] for row in triggers + runners)
            return None

        def stream_running(name):
            return ('Running',) in rows(sql(
                f"select status from information_schema.ins_streams where stream_name='{name}'"))

        wait('native SQL startup', lambda: ('1',) in rows(sql('select 1', ready=False)))
        # SQL can respond before the first dnode status report enables vnode allocation.
        wait('dnode ready for database creation', lambda: ('ready',) in rows(sql(
            'select status from information_schema.ins_dnodes where id=1 and support_vnodes > 0', ready=False)))
        reported = rows(sql("show dnode 1 variables like 'logDir'"))
        assert len(reported) == 1 and (root / reported[0][2]).samefile(root / 'log'), reported
        if not rows(sql('show snodes')):
            sql('create snode on dnode 1')
        sql('create database testdb vgroups 2')
        sql('create stable testdb.st (ts timestamp,soc int,trade_no varchar(32)) '
            'tags(pile_code varchar(32),gun_code varchar(32))')
        sql("create table testdb.t1 using testdb.st tags('P1','G1')")
        sql(create_stream('healthy'))
        original = wait('healthy trigger and runners', task_ids)
        healthy_id = original[0][0]
        assert stream_running('healthy')
        logfile = next((root / 'log').glob('taosdlog.[01]'))
        with logfile.open(encoding='utf-8', errors='replace') as log:
            log.seek(0, 2)
            for phase in ('first-copy', 'reader-bind'):
                (root / f'{phase}.hold').touch()
            sql(create_stream('paused'))
            for phase in ('first-copy', 'reader-bind'):
                # Read in sequence so earlier heartbeats or health checks cannot satisfy this phase.
                marker = re.compile(rf'stream deploy test entered {phase}, phaseStartTs:(\d+)')
                entered = wait(f'{phase} entered', lambda: next(
                    (int(match.group(1)) for line in log if (match := marker.search(line))), None))
                scan = SnodeHealthScan(entered + (31000 if phase == 'reader-bind' else 0))
                listing = rows(sql('show streams'))
                assert {'healthy', 'paused'} <= {row[0] for row in listing}, listing
                assert rows(sql('select server_version()')), 'fresh native login failed'
                reports = 0
                heartbeats = HealthyHeartbeats(healthy_id)
                checked = False

                def progress():
                    nonlocal reports, checked
                    for line in log:
                        if 'stream deploy test expired' in line or 'stream deploy test released' in line:
                            raise AssertionError(f'gate opened before observation: {line}')
                        if heartbeats.completed(line):
                            reports += 1
                        if scan.completed(line):
                            checked = True
                    return checked and reports >= 2

                wait(f'{phase}: health check and two completed healthy-group heartbeats', progress)
                assert task_ids() == original and stream_running('healthy'), f'healthy tasks changed in {phase}'
                print(f'{phase}: {reports} healthy reports, snode 1 health check, stable tasks {original}', flush=True)
                (root / f'{phase}.hold').unlink()
                wait(f'{phase} released', lambda: any(f'stream deploy test released {phase}' in line for line in log))
        wait('completed deployment', lambda: stream_running('paused'))
        sql("insert into testdb.t1 values ('2026-01-01 00:00:00',10,'T_CLEAN_A') "
            "('2026-01-01 00:00:01',11,'T_CLEAN_A') ('2026-01-01 00:00:02',12,'T_CLEAN_A')")
        wait('two count-window results', lambda: rows(sql(
            'select soc,prev_soc from testdb.paused_out order by ts')) == [('11', '10'), ('12', '11')])
    except BaseException:
        for path in [console, *sorted((root / 'log').glob('taosdlog.[01]'))]:
            with path.open('rb') as stream:
                stream.seek(max(0, path.stat().st_size - 12000))
                print(stream.read().decode(errors='replace'), file=sys.stderr)
        raise
    finally:
        for gate in root.glob('*.hold'):
            gate.unlink()
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=12)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)


if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='stream-deploy-cli-') as tmp:
        run_case(Path(tmp), Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve())
