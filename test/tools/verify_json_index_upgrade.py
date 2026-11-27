# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""Linux integration check requiring old/new taosd binaries and GDB.

Creates a fresh isolated data directory, upgrades real legacy indexes, then
kills rebuilding taosd after its first inserted tag and verifies restart recovery.
The work directory is preserved for inspection.
"""

import ctypes as C, pathlib, subprocess, time, json, shutil, os, sys
import argparse

def main():
    parser = argparse.ArgumentParser(description='Verify JSON UCS-4 index upgrade and interrupted rebuild recovery (Linux).')
    parser.add_argument('--old-taosd', required=True, help='Server built before the UCS-4 index change')
    parser.add_argument('--new-taosd', required=True, help='Debug server built with the UCS-4 index change')
    parser.add_argument('--library', required=True, help='Matching libtaos.so')
    parser.add_argument('--work-dir', required=True, help='New, nonexistent directory for isolated test data')
    parser.add_argument('--port', type=int, default=17338)
    args = parser.parse_args()
    base = pathlib.Path(args.work_dir).resolve()
    assert not base.exists(), 'use a fresh isolated fixture directory'
    for name in ['cfg', 'data', 'log', 'tmp']:
        (base / name).mkdir(parents=True, exist_ok=True)
    port = args.port
    (base / 'cfg/taos.cfg').write_text(f'fqdn localhost\nserverPort {port}\nfirstEp localhost:{port}\nsecondEp localhost:{port}\ndataDir {base}/data\nlogDir {base}/log\ntempDir {base}/tmp\ncharset UTF-8\nmonitor 0\ntelemetryReporting 0\n')
    lib = C.CDLL(args.library)
    lib.taos_options.argtypes = [C.c_int, C.c_void_p]
    lib.taos_options(3, str(base / 'cfg').encode())
    lib.taos_connect.argtypes = [C.c_char_p] * 4 + [C.c_uint16]
    lib.taos_connect.restype = C.c_void_p
    lib.taos_options_connection.argtypes = [C.c_void_p, C.c_int, C.c_char_p]
    lib.taos_query.argtypes = [C.c_void_p, C.c_char_p]
    lib.taos_query.restype = C.c_void_p
    lib.taos_errno.argtypes = [C.c_void_p]
    lib.taos_errno.restype = C.c_int
    lib.taos_errstr.argtypes = [C.c_void_p]
    lib.taos_errstr.restype = C.c_char_p
    lib.taos_fetch_row.argtypes = [C.c_void_p]
    lib.taos_fetch_row.restype = C.POINTER(C.c_void_p)
    lib.taos_free_result.argtypes = [C.c_void_p]
    lib.taos_close.argtypes = [C.c_void_p]
    p = None
    conn = None

    def start(binary):
        nonlocal p, conn
        f = open(base / (pathlib.Path(binary).name + '.stdout'), 'a')
        p = subprocess.Popen([binary, '-c', str(base / 'cfg')], stdout=f, stderr=subprocess.STDOUT)
        f.close()
        for i in range(60):
            if p.poll() is not None:
                raise RuntimeError('server exited ' + str(p.returncode))
            conn = lib.taos_connect(b'localhost', b'root', b'taosdata', None, port)
            if conn:
                time.sleep(3)
                return
            time.sleep(1)
        raise RuntimeError('connect timeout')

    def stop():
        nonlocal p, conn
        if conn:
            lib.taos_close(conn)
            conn = None
        if p:
            p.terminate()
            try:
                p.wait(timeout=30)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
            p = None

    def query(sql, charset='UTF-8', expect=None):
        assert lib.taos_options_connection(conn, 0, charset.encode()) == 0
        r = lib.taos_query(conn, sql.encode(charset))
        code = lib.taos_errno(r)
        err = lib.taos_errstr(r).decode(errors='replace')
        value = None
        if code == 0 and expect is not None:
            row = lib.taos_fetch_row(r)
            value = C.cast(row[0], C.POINTER(C.c_int64))[0] if row and row[0] else 0
        lib.taos_free_result(r)
        print(json.dumps([charset, sql, code, err, value], ensure_ascii=False), flush=True)
        assert code == 0, (sql, err)
        if expect is not None:
            assert value == expect, (sql, value, expect)

    def check():
        for cs in ['UTF-8', 'GBK', 'CP936']:
            query("select count(*) from ji.st where jt->'city'='中文'", cs, 1)
            query("select count(*) from ji.st where jt->'city'='北京'", cs, 1)
            query("select count(*) from ji.st where jt->'city'='missing'", cs, 0)
        query("select count(*) from ji.emoji where jt->'city'='😀'", expect=1)
    try:
        start(args.old_taosd)
        query('create database ji vgroups 1 keep 36500')
        query('create stable ji.st(ts timestamp,v int) tags(jt json)')
        for (i, city) in enumerate(['中文', '北京']):
            query(f'''insert into ji.ct{i} using ji.st tags('{{"city":"{city}"}}') values(1751600030000,1)''')
        query('create stable ji.emoji(ts timestamp,v int) tags(jt json)')
        query('insert into ji.ect using ji.emoji tags(\'{"city":"😀"}\') values(1751600030000,1)')
        check()
        stop()
        legacy = list((base / 'data').rglob('invert/*.tindex'))
        assert legacy, 'old-format index fixture missing'
        print('OLD INDEX FILES', legacy, flush=True)
        shutil.copytree(base / 'data', base / 'old-data-snapshot')
        start(args.new_taosd)
        check()
        query('alter table ji.ct0 set tag jt=\'{"city":"上海"}\' ', 'GBK')
        query("select count(*) from ji.st where jt->'city'='中文'", 'UTF-8', 0)
        query("select count(*) from ji.st where jt->'city'='上海'", 'GBK', 1)
        query('drop table ji.ct1')
        query("select count(*) from ji.st where jt->'city'='北京'", 'UTF-8', 0)
        stop()
        start(args.new_taosd)
        query("select count(*) from ji.st where jt->'city'='上海'", 'GBK', 1)
        query("select count(*) from ji.st where jt->'city'='北京'", 'UTF-8', 0)
        stop()
        (base / 'data').rename(base / 'completed-data')
        shutil.copytree(base / 'old-data-snapshot', base / 'data')
        gdb = subprocess.run(['gdb', '-batch', '-ex', 'set pagination off', '-ex', 'break metaSaveJsonVarToIdx', '-ex', 'ignore 1 1', '-ex', 'run', '-ex', 'python import os, signal; os.kill(gdb.selected_inferior().pid, signal.SIGKILL); print("REBUILD_KILLED")', '--args', args.new_taosd, '-c', str(base / 'cfg')], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=90)
        (base / 'interruption-gdb.log').write_bytes(gdb.stdout)
        assert b'REBUILD_KILLED' in gdb.stdout, gdb.stdout[-2000:]
        assert list((base / 'data').rglob('invert-ucs4-v1.building')), 'no interrupted staging index'
        assert not list((base / 'data').rglob('invert-ucs4-v1')), 'index published before rebuild finished'
        # Fail the merge worker after the metadata scan. The vnode must observe
        # the checked-close error and leave the staging directory unpublished.
        legacy_bytes = {f: f.read_bytes() for f in legacy}
        for function, failure in [('idxCacheIteratorCreate', '(int)-2147483390'),
                                  ('fstLastTransitionCreate', '(FstLastTransition*)0')]:
            result = subprocess.run([
                'gdb', '-batch', '-ex', f'break {function}', '-ex', 'run',
                '-ex', f'return {failure}', '-ex', 'disable 1', '-ex', 'continue',
                '--args', args.new_taosd, '-c', str(base / 'cfg'),
            ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=90)
            (base / f'{function}-oom.log').write_bytes(result.stdout)
            assert any(b'failed to flush staging index' in line and b'Out of Memory' in line
                       for line in result.stdout.splitlines()), result.stdout[-2000:]
            assert not list((base / 'data').rglob('invert-ucs4-v1')), function
            assert legacy_bytes == {f: f.read_bytes() for f in legacy}, function
        start(args.new_taosd)
        check()
        stop()
        assert list((base / 'data').rglob('invert-ucs4-v1')), 'recovery did not publish index'
        # Inject operational failures at FST loading and observe the return from
        # metaOpenJsonIndex, not just a leaf function. No directory may change.
        for label, error in [('io', -2130771963), ('permission', -2130771955), ('oom', -2147483390)]:
            before = {str(f): f.read_bytes() for f in (base / 'data').rglob('invert-ucs4-v1/*.tindex')}
            commands = base / f'error-{label}.gdb'
            commands.write_text(f"""set pagination off
break tfileReaderLoadFst
run
python
class CheckIndexError(gdb.FinishBreakpoint):
    def stop(self):
        actual = int(self.return_value)
        if actual != {error}:
            raise RuntimeError('unexpected index error: ' + str(actual))
        print('EXPECTED_INDEX_ERROR_{label}')
        return True
frame = gdb.newest_frame()
while frame is not None and frame.name() != 'metaOpenJsonIndex':
    frame = frame.older()
if frame is None:
    raise RuntimeError('metaOpenJsonIndex not on stack')
check_return = CheckIndexError(frame, internal=True)
end
return (int){error}
continue
python import os, signal; os.kill(gdb.selected_inferior().pid, signal.SIGKILL)
""")
            result = subprocess.run(['gdb', '-batch', '-x', str(commands), '--args', args.new_taosd,
                                     '-c', str(base / 'cfg')], stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, timeout=90)
            (base / f'error-{label}.log').write_bytes(result.stdout)
            assert f'EXPECTED_INDEX_ERROR_{label}'.encode() in result.stdout, result.stdout[-2000:]
            after = {str(f): f.read_bytes() for f in (base / 'data').rglob('invert-ucs4-v1/*.tindex')}
            assert before == after, 'operational failure changed published index'
            assert not list((base / 'data').rglob('invert-ucs4-v1.building'))
            assert not list((base / 'data').rglob('invert-ucs4-v1.corrupt'))
            print('OPERATIONAL ERROR PRESERVED WITHOUT REBUILD', label, flush=True)

        def damage_index(kind):
            files = list((base / 'data').rglob('invert-ucs4-v1/*.tindex'))
            assert files, 'published index fixture missing'
            target = files[0]
            raw = bytearray(target.read_bytes())
            if kind == 'magic':
                raw[-1] ^= 1
            elif kind == 'truncate':
                raw = raw[:16]
            elif kind == 'fst':
                # The FST checksum immediately precedes the 8-byte file footer.
                raw[-9] ^= 1
            target.write_bytes(raw)
            return target.parent

        for kind in ['magic', 'truncate', 'fst']:
            final = damage_index(kind)
            start(args.new_taosd)
            check()
            stop()
            assert final.with_name(final.name + '.corrupt').is_dir()
            print('CORRUPT INDEX REBUILT', kind, flush=True)

        # Directory sync calls: staging, parent after quarantine, parent after
        # publication. Kill just before the latter two syncs to cover both
        # rename windows, then verify all connection charsets after restart.
        for sync_skip in [1, 2]:
            final = damage_index('magic')
            gdb = subprocess.run([
                'gdb', '-batch', '-ex', 'set pagination off',
                '-ex', 'break metaSyncIndexDirectory',
                '-ex', f'ignore 1 {sync_skip}', '-ex', 'run',
                '-ex', 'python import os, signal; os.kill(gdb.selected_inferior().pid, signal.SIGKILL); print("SWITCH_KILLED")',
                '--args', args.new_taosd, '-c', str(base / 'cfg')],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=90)
            (base / f'switch-{sync_skip}-gdb.log').write_bytes(gdb.stdout)
            assert b'SWITCH_KILLED' in gdb.stdout, gdb.stdout[-2000:]
            assert final.with_name(final.name + '.corrupt').is_dir()
            assert final.exists() == (sync_skip == 2), 'unexpected publication state'
            start(args.new_taosd)
            check()
            stop()
            assert final.is_dir()
            print('INTERRUPTED INDEX SWITCH RECOVERED', sync_skip, flush=True)
        # The repair backup must reflect the original metadata, including the
        # absent derived index. Normal startup then uses the repaired copy.
        vnode = final.parent.parent.name
        assert vnode.startswith('vnode') and vnode[5:].isdigit(), final
        shutil.rmtree(final)
        repair_args = [args.new_taosd, '-c', str(base / 'cfg'), '-r', '--mode', 'force',
                       '--node-type', 'vnode', '--repair-target', f'meta:vnode={vnode[5:]}:strategy=from_redo']
        # Fail the second entry after one has already been replayed into temporary metadata.
        original_meta = final.parent.stat().st_ino
        for operation in ('metaDecodeEntry', 'metaHandleEntry2'):
            failed = subprocess.run([
                'gdb', '-batch', '-ex', 'set pagination off', '-ex', 'break metaForceRepairFromRedo',
                '-ex', 'run', '-ex', f'break {operation}', '-ex', 'ignore 2 1', '-ex', 'continue',
                '-ex', 'return (int) -1', '-ex', 'finish', '-ex', 'continue',
                '--args', *repair_args, '--backup-path', str(base / f'backup-{operation}')],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
            (base / f'repair-{operation}.log').write_bytes(failed.stdout)
            assert b'Breakpoint 2,' in failed.stdout and b'Value returned is $1 = -1' in failed.stdout, failed.stdout[-2000:]
            assert final.parent.stat().st_ino == original_meta and not final.exists(), operation
        backup = base / 'repair-backup'
        result = subprocess.run([*repair_args, '--backup-path', str(backup)],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
        (base / 'repair.log').write_bytes(result.stdout)
        assert result.returncode == 0, result.stdout[-2000:]
        backups = list(backup.glob(f'taos_backup_*/{vnode}/meta'))
        assert len(backups) == 1 and not (backups[0] / final.name).exists(), backups
        assert final.is_dir()
        start(args.new_taosd)
        check()
        stop()
        print('REDO REPAIR WITH MISSING DERIVED INDEX PASSED', flush=True)
        print('OLD INDEX UPGRADE, CORRUPTION REBUILD, INTERRUPTED REBUILD/SWITCH RECOVERY AND CROSS-CHARSET QUERIES PASSED', flush=True)
    finally:
        stop()
if __name__ == '__main__':
    main()
