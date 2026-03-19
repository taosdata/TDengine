#!/usr/bin/env python3
# filepath: hot_update/run/main.py
#
# IMPORTANT import order
# ----------------------
# 1. stdlib only
# 2. Parse --from-dir / --rollupdate early (before any taos import)
# 3. config_lib.prepare_native_lib(lib_dir)  <- sets LD_LIBRARY_PATH
#      rolling upgrade  (--rollupdate): lib_dir = from_dir  (old client, runs during upgrade)
#      cold upgrade     (default):      lib_dir = to_dir    (new client, back-compat with old server)
# 4. Everything else (taos, clusterSetup, ...)

import sys
import os
import socket
import argparse
import time
import multiprocessing

# -- Very early arg parse: need --from-dir before any taos import --------------

_HELP_TEXT = """
Usage: python -m run.main -F <from-dir> -T <to-dir> [OPTIONS]

Run a TDengine rolling-upgrade or cold-upgrade test.

Required arguments:
  -F, --from-dir  DIR   Path to the TDengine installation directory of the BASE
                        (source) version.  Used to start the initial cluster and
                        load the old connector library.
                        Example: /opt/tdengine/TDengine-enterprise-3.3.8.0

  -T, --to-dir    DIR   Path to the TDengine installation directory of the TARGET
                        (destination) version.  Used to upgrade each node and, for
                        cold upgrades, to load the new connector library.
                        Example: /opt/tdengine/TDengine-enterprise-3.4.0.8

Optional arguments:
  -p, --path      DIR   Working directory where cluster data and config files are
                        stored.  Created automatically if it does not exist.
                        Default: ~/td_rolling_upgrade

  -f, --fqdn      HOST  FQDN (hostname) of the server running the TDengine nodes.
                        Default: output of socket.gethostname()

  -q, --quick           Quick mode: use only 100 subtables x 1 000 initial rows
                        and a 30-second verification window instead of the full
                        defaults.  Useful for rapid CI smoke tests.

  -r, --rollupdate      Perform a ROLLING (hot) upgrade: background write / query /
                        subscribe workloads are started BEFORE the upgrade and run
                        continuously throughout.  Without this flag the tool performs
                        a COLD upgrade (cluster is stopped, upgraded, then restarted
                        before workloads begin).

  -S, --check-sysinfo   After the upgrade, compare the INFORMATION_SCHEMA column
                        definitions of both versions and fail the test if any
                        unexpected schema changes are detected.  Use together with
                        --whitelist-dir to allow known differences.

  -G, --gen-whitelist [FILE]
                        Capture the INFORMATION_SCHEMA diff for this upgrade pair
                        and write it to a whitelist YAML file, then exit without
                        running any workloads.  If FILE is omitted the path defaults
                        to <whitelist-dir>/{from_ver}~{to_ver}.yaml

  --whitelist-dir DIR   Directory that contains whitelist .yaml files loaded by
                        --check-sysinfo.  Default: <script_root>/whitelist

  -h, --help            Show this help message and exit.

Examples:
  # Cold upgrade (default)
  python -m run.main -F /opt/td/3.3.8.0 -T /opt/td/3.4.0.8

  # Rolling (hot) upgrade
  python -m run.main -F /opt/td/3.3.8.0 -T /opt/td/3.4.0.8 -r

  # Quick rolling upgrade with schema check
  python -m run.main -F /opt/td/3.3.8.0 -T /opt/td/3.4.0.8 -r -q -S

  # Generate a whitelist for a new version pair, then run with it
  python -m run.main -F /opt/td/3.3.8.0 -T /opt/td/3.4.0.8 -G
  python -m run.main -F /opt/td/3.3.8.0 -T /opt/td/3.4.0.8 -r -S
"""


def _print_help(file=None):
    import sys as _sys
    print(_HELP_TEXT.strip(), file=file or _sys.stdout)


def _early_args():
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--from-dir",   "-F", default=None)
    p.add_argument("--to-dir",     "-T", default=None)
    p.add_argument("--path",       "-p", default=os.path.expanduser("~/td_rolling_upgrade"))
    p.add_argument("--fqdn",       "-f", default="")
    p.add_argument("--quick",      "-q", action="store_true")
    p.add_argument("--rollupdate", "-r", action="store_true")
    p.add_argument("--help",       "-h", action="store_true")
    args, _ = p.parse_known_args()
    return args

_args = _early_args()

# Handle --help or missing required args before any taos import
if _args.help:
    _print_help()
    sys.exit(0)

if not os.environ.get("_TAOS_COLD_PHASE2"):  # subprocess passes sys.argv intact
    _missing = []
    if not _args.from_dir:
        _missing.append("--from-dir / -F")
    if not _args.to_dir:
        _missing.append("--to-dir / -T")
    if _missing:
        for _m in _missing:
            print(f"error: required argument missing: {_m}", file=sys.stderr)
        print(file=sys.stderr)
        _print_help(file=sys.stderr)
        sys.exit(2)

# -- Configure LD_LIBRARY_PATH BEFORE any taos import -------------------------

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config_lib import prepare_native_lib, taosd_path as get_taosd_path

# Sentinel env var that marks the cold-upgrade Phase 4-5 subprocess.
_COLD_PHASE2_ENV = "_TAOS_COLD_PHASE2"

# Phase 1-3 always use the old (from_dir) client so we can connect to the
# base-version server.  After a cold upgrade the main process spawns a fresh
# subprocess that starts with the new (to_dir) library (detected below).
_lib_dir = _args.to_dir if os.environ.get(_COLD_PHASE2_ENV) else _args.from_dir
prepare_native_lib(_lib_dir)

# -- Now safe to import taos-dependent modules ---------------------------------

import config
from server.clusterSetup import ClusterManager, Logger
from server.rollingUpgrade import RollingUpgrader, ColdUpgrader
from resource.resourceManager import ResourceManager
from resource.verifier import Verifier
from resource.userVerifier import UserVerifier
from client.writer import writer_main
from client.querier import querier_main
from client.subscriber import subscriber_main
from run.reporter import Reporter
from resource.sysinfo_checker import (get_server_version, snapshot as sysinfo_snapshot,
                                       compare_snapshots)
from resource.whitelist_loader import (load_whitelists, apply_whitelist,
                                        write_whitelist_yaml, gen_whitelist_filepath)


# Suppress verbose internal logging; only surface errors to stderr
class _SilentLogger(Logger):
    def info(self, msg):    pass
    def debug(self, msg):   pass
    def notice(self, msg):  pass
    def success(self, msg): pass
    def error(self, msg):
        import sys
        print(f"  [internal-error] {msg}", file=sys.stderr)


# ==============================================================================
# Helpers
# ==============================================================================

def _init_metrics(manager):
    m = manager.dict()
    for k in [
        "write_last_latency", "write_max_latency", "write_window_max",
        "query_last_latency", "query_max_latency", "query_window_max",
        "subscribe_last_gap", "subscribe_max_gap", "subscribe_window_max",
        "write_last_current", "write_last_voltage", "write_last_phase",
    ]:
        m[k] = 0.0
    m["write_last_ts"]             = 0
    m["query_last_count"]          = 0
    m["subscribe_recv_total"]      = 0
    m["subscribe_last_batch_rows"] = 0
    m["subscribe_last_recv_time"]  = 0.0   # unix timestamp; 0 = never received
    # Phase-4 success counters (reset at start of rolling upgrade)
    m["write_phase4_success"]     = 0
    m["query_phase4_success"]     = 0
    m["subscribe_phase4_recv"]    = 0
    # Cumulative counters
    m["write_total_rows"]         = 0   # total rows successfully inserted
    m["write_retry_count"]        = 0   # total individual retry attempts (writer)
    m["query_retry_count"]        = 0   # total individual retry attempts (querier)
    # Failure counters: each unit = MAX_CONSECUTIVE_RETRIES consecutive failures
    for k in ["write_error_count", "query_error_count", "subscribe_error_count"]:
        m[k] = 0
    # Handshake flag: subscriber sets True once TMQ subscription is active
    m["subscribe_ready"] = False
    return m


def _start_workers(fqdn, cfg_dir, metrics, stop_event, writer_stop_event):
    procs = []
    for i, fn in enumerate((writer_main, querier_main, subscriber_main)):
        evt = writer_stop_event if i == 0 else stop_event
        p = multiprocessing.Process(
            target=fn, args=(fqdn, cfg_dir, metrics, evt), daemon=True
        )
        p.start()
        procs.append(p)
    return procs


def _write_candidate_whitelist(si_diff, from_ver, to_ver):
    """Write a candidate whitelist YAML to cwd. Returns the written path, or None on error."""
    import re
    def _safe_ver(s):
        m = re.search(r'\d+(?:\.\d+)+', str(s))
        return m.group(0) if m else re.sub(r'[^\w.\-]', '_', str(s))
    fname = f"{_safe_ver(from_ver)}~{_safe_ver(to_ver)}.yaml"
    cand_path = os.path.join(os.getcwd(), fname)
    try:
        write_whitelist_yaml(si_diff, from_ver, to_ver, cand_path)
        return cand_path
    except Exception as _wce:
        print(f"  WARNING: failed to write candidate whitelist: {_wce}", file=sys.stderr)
        return None


def _stop_workers(procs, stop_event):
    stop_event.set()
    for p in procs:
        p.join(timeout=10)
        if p.is_alive():
            p.terminate()


def _workloads_ok(metrics, rp):
    # Workers no longer exit fatally; just surface error counts as warnings.
    # The definitive pass/fail check is done at the end of Phase 5.
    wec = metrics.get("write_error_count", 0)
    qec = metrics.get("query_error_count", 0)
    if wec > 0:
        rp.error(f"Writer has {wec} failure batch(es) so far.")
    if qec > 0:
        rp.error(f"Querier has {qec} failure batch(es) so far.")
    return True  # never abort mid-run


def _version_ge(ver_str: str, min_ver: str) -> bool:
    """Return True if ver_str >= min_ver using dotted 4-part version comparison.

    Extracts the first X.Y.Z.W pattern found in the string, so directory names
    like 'TDengine-enterprise-3.3.8.0' are handled correctly.
    """
    import re

    def _parse(s):
        m = re.search(r'(\d+)\.(\d+)\.(\d+)\.(\d+)', s)
        if m:
            return tuple(int(x) for x in m.groups())
        m = re.search(r'(\d+)\.(\d+)\.(\d+)', s)
        if m:
            return tuple(int(x) for x in m.groups()) + (0,)
        return (0, 0, 0, 0)

    return _parse(ver_str) >= _parse(min_ver)


# ==============================================================================
# Cold-upgrade Phase 4-5  (runs in a fresh subprocess with to_dir library)
# ==============================================================================

def _run_cold_phase2(state_file: str):
    """
    Invoked in the subprocess that starts with the target-version libtaos.so.
    Loads the pre-upgrade state from a pickle file, then runs Phase 4 (start
    background workloads) and Phase 5 (verification + result checks).
    """
    import pickle

    rp = Reporter()
    with open(state_file, "rb") as _f:
        st = pickle.load(_f)

    from_ver         = st["from_ver"]
    to_ver           = st["to_ver"]
    fqdn             = st["fqdn"]
    base_path        = st["base_path"]
    cfg_dir          = st["cfg_dir"]
    checks           = st["checks"]
    test_start       = st["test_start"]
    rsma_supported   = st["rsma_supported"]
    tsma_supported   = st["tsma_supported"]
    stream_supported = st["stream_supported"]
    check_sysinfo    = st.get("check_sysinfo", True)
    gen_whitelist    = st.get("gen_whitelist", None)
    whitelist_dir    = st.get("whitelist_dir", "")
    sysinfo_before   = st.get("sysinfo_before", None)
    from_ver_actual  = st.get("from_ver_actual", None)

    if st.get("quick"):
        config.SUBTABLE_COUNT         = 100
        config.INIT_ROWS_PER_SUBTABLE = 1_000
        config.VERIFY_DURATION_S      = 30

    rm = ResourceManager(fqdn=fqdn, cfg_dir=cfg_dir, logger=_SilentLogger())

    # Wait for all upgraded nodes to become ready before starting workloads.
    # This must happen here (in the subprocess) because the parent process
    # uses the old client library which cannot connect to the new servers.
    rp.info("Waiting for all upgraded nodes to become ready ...")
    import taos as _taos
    deadline_ready = time.time() + config.NODE_READY_TIMEOUT_S
    while time.time() < deadline_ready:
        try:
            _conn = _taos.connect(host=fqdn, config=cfg_dir)
            _cur  = _conn.cursor()
            _cur.execute("SHOW DNODES")
            _rows = _cur.fetchall()
            _cur.close()
            _conn.close()
            if all(str(r[4]) == "ready" for r in _rows):
                rp.info(f"All {len(_rows)} node(s) ready.")
                break
            _not_ready = [r[0] for r in _rows if str(r[4]) != "ready"]
            rp.info(f"Waiting for nodes: {_not_ready} ...")
        except Exception as _e:
            rp.info(f"  connect attempt failed: {_e} ...")
        time.sleep(3)
    else:
        rp.error("Timed out waiting for all nodes to become ready after cold upgrade")
        sys.exit(1)

    # -- INFORMATION_SCHEMA snapshot AFTER upgrade ----------------------------
    _sysinfo_after  = None
    _to_ver_actual  = None
    if (check_sysinfo or gen_whitelist is not None) and sysinfo_before is not None:
        try:
            _to_ver_actual = get_server_version(fqdn, cfg_dir)
            _sysinfo_after = sysinfo_snapshot(fqdn, cfg_dir)
            rp.info(f"SysInfo: server version (to) = {_to_ver_actual}, "
                    f"{len(_sysinfo_after)} tables captured")
        except Exception as _sie:
            rp.error(f"SysInfo snapshot (after) failed: {_sie}")

    # Gen-whitelist mode: write file and exit (no workloads needed)
    if gen_whitelist is not None:
        if sysinfo_before is not None and _sysinfo_after is not None:
            _si_diff = compare_snapshots(sysinfo_before, _sysinfo_after)
            rp.info(f"SysInfo diff:\n{_si_diff.format_report()}")
            _wl_path = gen_whitelist_filepath(
                from_ver_actual or from_ver, _to_ver_actual or to_ver,
                whitelist_dir, gen_whitelist,
            )
            try:
                write_whitelist_yaml(_si_diff, from_ver_actual or from_ver,
                                     _to_ver_actual or to_ver, _wl_path)
                rp.info(f"Whitelist written: {_wl_path}")
            except Exception as _we:
                rp.error(f"Failed to write whitelist: {_we}")
        else:
            rp.error("SysInfo: snapshot unavailable, whitelist not written")
        sys.exit(0)

    manager         = multiprocessing.Manager()
    metrics         = _init_metrics(manager)
    stop_evt        = manager.Event()
    writer_stop_evt = manager.Event()
    worker_procs    = []

    # -- Phase 4: Background workloads (started AFTER cold upgrade) -----------
    rp.step_start("Phase 4  Background workloads (write / query / subscribe)")
    worker_procs = _start_workers(fqdn, cfg_dir, metrics, stop_evt, writer_stop_evt)

    rp.info("Waiting 15s for workloads to warm up ...")
    time.sleep(15)

    if not _workloads_ok(metrics, rp):
        _stop_workers(worker_procs, stop_evt)
        sys.exit(1)

    rp.info(
        f"Workloads live  |  "
        f"write={metrics['write_last_latency']:.3f}s  "
        f"query={metrics['query_last_latency']:.3f}s  "
        f"sub_gap={metrics['subscribe_last_gap']:.3f}s  "
        f"|  errors: w={metrics['write_error_count']} "
        f"q={metrics['query_error_count']} "
        f"s={metrics['subscribe_error_count']}"
    )
    rp.info(
        f"  inserted row : ts={metrics['write_last_ts']}  "
        f"curr={metrics['write_last_current']:.3f}A  "
        f"volt={metrics['write_last_voltage']}V  "
        f"phase={metrics['write_last_phase']:.3f}  "
        f"|  query count={metrics['query_last_count']:,}  "
        f"|  sub recv_total={metrics['subscribe_recv_total']:,}"
    )
    rp.step_done("Phase 4")

    # -- Phase 5: Post-upgrade verification ------------------------------------
    rp.step_start(f"Phase 5  Post-upgrade verification ({config.VERIFY_DURATION_S}s)")

    metrics["write_window_max"]     = 0.0
    metrics["query_window_max"]     = 0.0
    metrics["subscribe_window_max"] = 0.0

    deadline = time.time() + config.VERIFY_DURATION_S
    while time.time() < deadline:
        if not _workloads_ok(metrics, rp):
            break
        remaining = int(deadline - time.time())
        rp.info(
            f"[{remaining:3d}s]  "
            f"write={metrics.get('write_last_latency', 0):.3f}s  "
            f"query={metrics.get('query_last_latency', 0):.3f}s  "
            f"sub_gap={metrics.get('subscribe_last_gap', 0):.3f}s"
        )
        rp.info(
            f"         inserted: "
            f"ts={metrics.get('write_last_ts', 0)}  "
            f"curr={metrics.get('write_last_current', 0):.3f}A  "
            f"volt={metrics.get('write_last_voltage', 0)}V  "
            f"phase={metrics.get('write_last_phase', 0):.3f}  "
            f"|  count={metrics.get('query_last_count', 0):,}  "
            f"|  sub_recv={metrics.get('subscribe_recv_total', 0):,}  "
        )
        time.sleep(5)

    # Stop writer gracefully: signal via its dedicated event so it finishes
    # the current INSERT + metrics update before exiting.  Using terminate()
    # would risk a race where the row is committed to the DB (and consumed by
    # TMQ) but write_total_rows is never incremented, causing received > written.
    writer_proc = worker_procs[0]
    writer_stop_evt.set()
    writer_proc.join(timeout=10)
    if writer_proc.is_alive():
        writer_proc.terminate()
        writer_proc.join(timeout=3)

    final_written = metrics.get("write_total_rows", 0)
    rp.info(
        f"Writer stopped ({final_written:,} rows total). "
        f"Waiting up to {config.SUBSCRIBE_DRAIN_WAIT_S}s for subscriber to drain ..."
    )
    drain_deadline = time.time() + config.SUBSCRIBE_DRAIN_WAIT_S
    while time.time() < drain_deadline:
        time.sleep(2)
        cur_recv = metrics.get("subscribe_recv_total", 0)
        rp.info(
            f"  drain: written={final_written:,}  subscribed={cur_recv:,}  "
            f"gap={final_written - cur_recv:,}"
        )
        if cur_recv >= final_written:
            rp.info("Subscriber has caught up.")
            break
    else:
        rp.info(
            f"Drain wait timed out after {config.SUBSCRIBE_DRAIN_WAIT_S}s "
            f"(written={final_written:,}  subscribed={metrics.get('subscribe_recv_total', 0):,})"
        )

    time.sleep(3)
    _stop_workers(worker_procs[1:], stop_evt)
    rp.step_done("Phase 5")

    # -- Check results -------------------------------------------------------
    write_max = metrics.get("write_window_max", 0.0)
    query_max = metrics.get("query_window_max", 0.0)
    sub_max   = metrics.get("subscribe_window_max", 0.0)

    def _chk(label, ok, detail=""):
        checks.append((label, ok, detail))

    write_fail_cnt = metrics.get("write_error_count", 0)
    _chk("Write: no failure batches", write_fail_cnt == 0,
         f"failures={write_fail_cnt}  total_rows={metrics.get('write_total_rows', 0):,}  "
         f"retries={metrics.get('write_retry_count', 0):,}  max_latency={write_max:.3f}s")

    query_fail_cnt = metrics.get("query_error_count", 0)
    _chk("Query: no failure batches", query_fail_cnt == 0,
         f"failures={query_fail_cnt}  max_latency={query_max:.3f}s")

    last_recv   = metrics.get("subscribe_last_recv_time", 0.0)
    sub_silence = time.time() - last_recv if last_recv > 0 else float("inf")
    _chk(
        f"Subscribe: data received within {config.SUBSCRIBE_NO_DATA_TIMEOUT_S}s",
        sub_silence <= config.SUBSCRIBE_NO_DATA_TIMEOUT_S,
        f"silence={sub_silence:.1f}s  total_recv={metrics.get('subscribe_recv_total', 0):,}",
    )

    total_written = metrics.get("write_total_rows", 0)
    total_recv    = metrics.get("subscribe_recv_total", 0)
    _chk(
        "Subscribe: rows received == rows written",
        total_written == total_recv,
        f"written={total_written:,}  received={total_recv:,}  diff={total_written - total_recv:,}",
    )

    uv = UserVerifier(fqdn=fqdn, cfg_dir=cfg_dir)
    auth_ok, auth_msg = uv.verify_auth()
    _chk("test_user authentication after upgrade", auth_ok, auth_msg)

    _priv_before = st.get("priv_before")
    if _priv_before is not None:
        priv_ok, priv_msg = uv.verify_privileges(_priv_before)
        _chk("test_user privileges unchanged ", priv_ok, priv_msg)
    else:
        _chk("test_user privileges unchanged ", False,
             "baseline snapshot not captured (Phase 2 error)")

    _idx_before = st.get("idx_before")
    if _idx_before is not None:
        idx_ok, idx_msg = rm.verify_tag_indexes(_idx_before)
        _chk("Tag indexes preserved after upgrade", idx_ok, idx_msg)
    else:
        _chk("Tag indexes preserved after upgrade", False,
             "baseline snapshot not captured (Phase 2 error)")

    if tsma_supported:
        _tsma_before = st.get("tsma_before")
        if _tsma_before is not None:
            tsma_ok, tsma_msg = rm.verify_tsma(_tsma_before)
            _chk("TSMA preserved after upgrade", tsma_ok, tsma_msg)
        else:
            _chk("TSMA preserved after upgrade", False,
                 "baseline snapshot not captured (Phase 2 error)")
    else:
        rp.info(f"TSMA check skipped: base version {from_ver} < 3.3.6.0")

    if rsma_supported:
        _rsma_before = st.get("rsma_before")
        if _rsma_before is not None:
            rsma_ok, rsma_msg = rm.verify_rsma(_rsma_before)
            _chk("RSMA preserved after upgrade", rsma_ok, rsma_msg)
        else:
            _chk("RSMA preserved after upgrade", False,
                 "baseline snapshot not captured (Phase 2 error)")
    else:
        rp.info(f"RSMA check skipped: base version {from_ver} < 3.3.8.0")

    if stream_supported:
        _stream_before = st.get("stream_before")
        if _stream_before is not None:
            stream_ok, stream_msg = rm.verify_stream(_stream_before)
            _chk("Stream preserved after upgrade", stream_ok, stream_msg)
        else:
            _chk("Stream preserved after upgrade", False,
                 "baseline snapshot not captured (Phase 2 error)")
    else:
        rp.info(f"Stream check skipped: base version {from_ver} < 3.3.7.0")

    # ------ INFORMATION_SCHEMA check ------
    if check_sysinfo and sysinfo_before is not None and _sysinfo_after is not None:
        _si_diff = compare_snapshots(sysinfo_before, _sysinfo_after)
        _wl = load_whitelists(whitelist_dir, from_ver_actual or from_ver,
                               _to_ver_actual or to_ver)
        if _wl.source_files:
            rp.info(f"SysInfo whitelist files: {_wl.source_files}")
        _fr = apply_whitelist(_si_diff, _wl)
        if _fr.has_unexpected():
            _cand_wl_path = _write_candidate_whitelist(
                _si_diff, from_ver_actual or from_ver, _to_ver_actual or to_ver)
            _detail = _fr.format_unexpected()
            if _cand_wl_path:
                _detail += f"\n  {_cand_wl_path}"
            _chk("INFORMATION_SCHEMA: no unexpected changes", False, _detail)
        else:
            _si_detail = "(no changes)" if _si_diff.is_empty() else "(all changes whitelisted)"
            _chk("INFORMATION_SCHEMA: no unexpected changes", True, _si_detail)
            if _fr.format_expected():
                rp.info(f"  Expected (whitelisted):\n{_fr.format_expected()}")
    elif check_sysinfo and sysinfo_before is not None:
        _chk("INFORMATION_SCHEMA: no unexpected changes", False,
             "after-snapshot not captured")

    all_passed = all(ok for _, ok, _ in checks)
    rp.summary_end(
        passed=all_passed, checks=checks, start_time=test_start,
        write_max=write_max, query_max=query_max, sub_max=sub_max,
    )
    sys.exit(0 if all_passed else 1)

def main():
    # Cold-upgrade Phase 4-5 runs in a re-spawned subprocess with to_dir lib.
    _cold_state_file = os.environ.get(_COLD_PHASE2_ENV)
    if _cold_state_file:
        _run_cold_phase2(_cold_state_file)
        return  # _run_cold_phase2 calls sys.exit; this line is a safety net

    parser = argparse.ArgumentParser(
        description="TDengine rolling/cold upgrade test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_HELP_TEXT,
    )
    parser.add_argument("--from-dir", "-F", required=True,
                        metavar="DIR",
                        help="TDengine installation dir of the BASE (source) version")
    parser.add_argument("--to-dir",   "-T", required=True,
                        metavar="DIR",
                        help="TDengine installation dir of the TARGET (destination) version")
    parser.add_argument("--path",     "-p", default=os.path.expanduser("~/td_rolling_upgrade"))
    parser.add_argument("--fqdn",     "-f", default="")
    parser.add_argument("--quick",      "-q", action="store_true",
                        help="Quick mode: 100 subtables x 1000 rows")
    parser.add_argument("--rollupdate", "-r", action="store_true",
                        help="Rolling (hot) upgrade: background workloads run during upgrade. "
                             "If omitted, performs a cold upgrade (workloads start after upgrade).")
    parser.add_argument("--check-sysinfo", "-S", dest="check_sysinfo", action="store_true",
                        help="Enable INFORMATION_SCHEMA schema-change check (disabled by default)")
    parser.set_defaults(check_sysinfo=False)
    parser.add_argument("--gen-whitelist", "-G", metavar="FILE", nargs="?", const=True, default=None,
                        help="Generate a whitelist from this upgrade's INFORMATION_SCHEMA diff "
                             "and exit (skips all other checks). FILE defaults to "
                             "whitelist/{from_ver}~{to_ver}.yaml")
    parser.add_argument("--whitelist-dir", metavar="DIR", default=None,
                        help="Directory containing whitelist .yaml files "
                             "(default: <script_dir>/whitelist)")
    args = parser.parse_args()

    fqdn      = args.fqdn or socket.gethostname()
    from_dir  = os.path.abspath(args.from_dir)
    to_dir    = os.path.abspath(args.to_dir)
    base_path = os.path.abspath(args.path)
    cfg_dir   = os.path.join(base_path, "dnode1", "cfg")

    if args.quick:
        config.SUBTABLE_COUNT         = 100
        config.INIT_ROWS_PER_SUBTABLE = 1_000
        config.VERIFY_DURATION_S      = 30

    _script_root  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    whitelist_dir = args.whitelist_dir or os.path.join(_script_root, "whitelist")

    from_ver   = os.path.basename(from_dir)
    to_ver     = os.path.basename(to_dir)
    rp         = Reporter()
    # Feature version guards
    rsma_supported   = _version_ge(from_ver, "3.3.8.0")  # RSMA:   >= 3.3.8.0
    tsma_supported   = _version_ge(from_ver, "3.3.6.0")  # TSMA:   >= 3.3.6.0
    stream_supported = _version_ge(from_ver, "3.3.7.0")  # Stream: >= 3.3.7.0
    test_start = time.time()

    rp.summary_start(
        from_ver=from_ver, to_ver=to_ver,
        fqdn=fqdn,
        dnode_count=config.DNODE_COUNT,
        mnode_count=config.MNODE_COUNT,
        subtables=config.SUBTABLE_COUNT,
        rows_per_table=config.INIT_ROWS_PER_SUBTABLE,
        verify_window=config.VERIFY_DURATION_S,
        check_sysinfo=args.check_sysinfo,
        gen_whitelist=args.gen_whitelist,
    )

    checks = []

    # -- Phase 1: Cluster setup ------------------------------------------------
    rp.step_start("Phase 1  Cluster setup (base version)")
    try:
        base_taosd  = get_taosd_path(from_dir)
        rp.info(f"Base taosd : {base_taosd}")
        rp.info(f"Base path  : {base_path}")
        rp.info(f"FQDN       : {fqdn}")
        rp.info(
            f"Topology   : {config.DNODE_COUNT} dnodes / "
            f"{config.MNODE_COUNT} mnodes / replica={config.REPLICA}"
        )

        cluster_mgr = ClusterManager(fqdn=fqdn, base_path=base_path, level=1, disk=1,
                                       logger=_SilentLogger())

        # Point DNodeManager at base-version taosd BEFORE create_cluster() starts
        # processes, so every node runs the correct version from the beginning.
        cluster_mgr.dnode_manager.taosd_path = base_taosd

        rp.info(f"Starting {config.DNODE_COUNT} taosd process(es) ...")
        cluster_mgr.create_cluster(
            dnode_nums=config.DNODE_COUNT,
            mnode_nums=config.MNODE_COUNT,
        )
        rp.info("Connecting to cluster ...")
        cluster_mgr.connect()

        if config.DNODE_COUNT > 1:
            rp.info(f"Adding dnodes 2–{config.DNODE_COUNT} to cluster ...")
            cluster_mgr.create_dnodes_in_cluster(config.DNODE_COUNT)
        rp.info(f"Configuring {config.MNODE_COUNT} mnode(s) ...")
        cluster_mgr.create_mnodes_in_cluster(config.MNODE_COUNT)
        rp.info("Waiting for cluster ready ...")
        cluster_mgr.wait_for_cluster_ready(timeout=60)
        rp.info("Cluster is ready.")
    except Exception as e:
        rp.error(f"Cluster setup failed: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)
    rp.step_done("Phase 1")

    # -- Phase 2: Resource preparation -----------------------------------------
    rp.step_start("Phase 2  Resource preparation")
    _sysinfo_before  = None
    _from_ver_actual = None
    try:
        rm = ResourceManager(fqdn=fqdn, cfg_dir=cfg_dir, logger=_SilentLogger())

        # -- INFORMATION_SCHEMA snapshot BEFORE upgrade -----------------------
        if args.check_sysinfo or args.gen_whitelist is not None:
            try:
                _from_ver_actual = get_server_version(fqdn, cfg_dir)
                _sysinfo_before  = sysinfo_snapshot(fqdn, cfg_dir)
                rp.info(f"SysInfo: server version = {_from_ver_actual}, "
                        f"{len(_sysinfo_before)} tables in INFORMATION_SCHEMA")
            except Exception as _sie:
                rp.error(f"SysInfo snapshot (before) failed: {_sie}")

        if args.gen_whitelist is None:
            # create db/super/child table
            rm.create_database()
            rp.info(f"Database '{config.DB_NAME}' (replica={config.REPLICA}) created")
            rm.create_stable()
            rp.info(f"Stable '{config.STABLE_NAME}' created")
            rm.create_subtables()
            rp.info(f"{config.SUBTABLE_COUNT} subtables created")

            # insert data
            rp.info(f"Insert initial data: {config.SUBTABLE_COUNT} x {config.INIT_ROWS_PER_SUBTABLE:,} rows ...")
            rm.write_initial_data()
            rp.info(
                f"Initial data: {config.SUBTABLE_COUNT} x {config.INIT_ROWS_PER_SUBTABLE:,} rows written"
            )

            # tsma (>= 3.3.6.0 only) — create BEFORE topic to avoid stream metadata conflicts
            if tsma_supported:
                tsma_before = rm.create_tsma()
                rp.info(f"TSMA created: {sorted(tsma_before.keys())}")
            else:
                rp.info(f"TSMA skipped: base version {from_ver} < 3.3.6.0")

            # stream (>= 3.3.7.0 only) — new-style PERIOD stream; Snode created by create_tsma
            if stream_supported:
                stream_before = rm.create_stream()
                rp.info(f"Stream created: {sorted(stream_before.keys())}")
            else:
                rp.info(f"Stream skipped: base version {from_ver} < 3.3.7.0")

            # tag index  (must be before topic creation; 3.3.x rejects schema
            #             changes on stables that already have a topic)
            idx_before = rm.create_tag_indexes()
            rp.info(f"Tag indexes created: {sorted(idx_before.keys())}")

            # topic
            rm.create_topic()
            rp.info(f"Topic '{config.TOPIC_NAME}' created")
            priv_before = rm.create_test_user()
            rp.info(
                f"test_user '{config.TEST_USER_NAME}' created: "
                f"{len(priv_before)} privilege row(s) granted "
                f"(SELECT on {config.TEST_USER_READ_STABLE}, "
                f"INSERT on {config.TEST_USER_WRITE_STABLE})"
            )

            # rsma (>= 3.3.8.0 only)
            if rsma_supported:
                rsma_before = rm.create_rsma()
                rp.info(f"RSMA created: {sorted(rsma_before.keys())}")
            else:
                rp.info(f"RSMA skipped: base version {from_ver} < 3.3.8.0")
        else:
            rp.info("Gen-whitelist mode: skipping resource preparation")
    except Exception as e:
        rp.error(f"Resource preparation failed: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)
    rp.step_done("Phase 2")

    # Shared workload state (needed before Phase 3 in both modes)
    manager         = multiprocessing.Manager()
    metrics         = _init_metrics(manager)
    stop_evt        = manager.Event()
    writer_stop_evt = manager.Event()
    worker_procs    = []

    if args.rollupdate:
        # ══════════════════════════════════════════════════════════════════════
        # ROLLING (HOT) UPGRADE
        #   Phase 3: start background workloads  (BEFORE upgrade)
        #   Phase 4: rolling upgrade             (workloads running throughout)
        # ══════════════════════════════════════════════════════════════════════

        # -- Phase 3: Background workloads (skipped in gen-whitelist mode) -----
        if args.gen_whitelist is None:
            rp.step_start("Phase 3  Background workloads (write / query / subscribe)")
            worker_procs = _start_workers(fqdn, cfg_dir, metrics, stop_evt, writer_stop_evt)

            rp.info("Waiting 15s for workloads to warm up ...")
            time.sleep(15)

            if not _workloads_ok(metrics, rp):
                _stop_workers(worker_procs, stop_evt)
                sys.exit(1)

            rp.info(
                f"Workloads live  |  "
                f"write={metrics['write_last_latency']:.3f}s  "
                f"query={metrics['query_last_latency']:.3f}s  "
                f"sub_gap={metrics['subscribe_last_gap']:.3f}s  "
                f"|  errors: w={metrics['write_error_count']} "
                f"q={metrics['query_error_count']} "
                f"s={metrics['subscribe_error_count']}"
            )
            rp.info(
                f"  inserted row : ts={metrics['write_last_ts']}  "
                f"curr={metrics['write_last_current']:.3f}A  "
                f"volt={metrics['write_last_voltage']}V  "
                f"phase={metrics['write_last_phase']:.3f}  "
                f"|  query count={metrics['query_last_count']:,}  "
                f"|  sub recv_total={metrics['subscribe_recv_total']:,}"
            )
            rp.step_done("Phase 3")

        # -- Phase 4: Rolling upgrade ------------------------------------------
        rp.step_start("Phase 4  Rolling upgrade")
        # Reset phase-4 success counters so we only capture activity during upgrade
        metrics["write_phase4_success"]  = 0
        metrics["query_phase4_success"]  = 0
        metrics["subscribe_phase4_recv"] = 0
        upgrade_ok = False
        try:
            upgrader     = RollingUpgrader(cluster_mgr, rp)
            target_taosd = get_taosd_path(to_dir)
            upgrade_ok   = upgrader.run(
                to_taosd_path=target_taosd,
                node_count=config.DNODE_COUNT,
                metrics=metrics,
            )
        except Exception as e:
            rp.error(f"Rolling upgrade exception: {e}")
            import traceback; traceback.print_exc()

        if not upgrade_ok:
            _stop_workers(worker_procs, stop_evt)
            checks.append(("Rolling upgrade completed", False, ""))
            rp.summary_end(
                passed=False, checks=checks, start_time=test_start,
                write_max=metrics.get("write_window_max", 0),
                query_max=metrics.get("query_window_max", 0),
                sub_max=metrics.get("subscribe_window_max", 0),
            )
            sys.exit(1)

        checks.append(("Rolling upgrade completed", True, "all nodes upgraded"))
        rp.check("Rolling upgrade completed", True, "all nodes upgraded")
        rp.info(
            f"\n        ------------- running information during rolling upgrade -------------\n"
            f"\n             writes     succeeded={metrics.get('write_phase4_success', 0):,} rows  "
            f"retries(total)={metrics.get('write_retry_count', 0):,}"
            f"\n             queries    succeeded={metrics.get('query_phase4_success', 0):,}       "
            f"retries(total)={metrics.get('query_retry_count', 0):,}"
            f"\n             subscribed succeeded={metrics.get('subscribe_phase4_recv', 0):,} rows "
            f"\n        ----------------------------------------------------------------------\n"
        )
        rp.step_done("Phase 4")

        # -- Gen-whitelist mode: capture after-snapshot, write file, exit -----
        if args.gen_whitelist is not None:
            _to_ver_actual = None
            _sysinfo_after = None
            try:
                _to_ver_actual = get_server_version(fqdn, cfg_dir)
                _sysinfo_after = sysinfo_snapshot(fqdn, cfg_dir)
                rp.info(f"SysInfo: server version (to) = {_to_ver_actual}, "
                        f"{len(_sysinfo_after)} tables captured")
            except Exception as _sie:
                rp.error(f"SysInfo snapshot (after) failed: {_sie}")
            if _sysinfo_before is not None and _sysinfo_after is not None:
                _si_diff = compare_snapshots(_sysinfo_before, _sysinfo_after)
                rp.info(f"SysInfo diff:\n{_si_diff.format_report()}")
                _wl_path = gen_whitelist_filepath(
                    _from_ver_actual or from_ver, _to_ver_actual or to_ver,
                    whitelist_dir, args.gen_whitelist,
                )
                try:
                    write_whitelist_yaml(_si_diff, _from_ver_actual or from_ver,
                                         _to_ver_actual or to_ver, _wl_path)
                    rp.info(f"Whitelist written: {_wl_path}")
                except Exception as _we:
                    rp.error(f"Failed to write whitelist: {_we}")
            else:
                rp.error("SysInfo: snapshot unavailable, whitelist not written")
            _stop_workers(worker_procs, stop_evt)
            sys.exit(0)

    else:
        # ══════════════════════════════════════════════════════════════════════
        # COLD UPGRADE
        #   Phase 3: stop all nodes → replace binaries → start all nodes
        #            (NO background workloads during upgrade)
        #   Phase 4: start background workloads  (AFTER upgrade)
        # ══════════════════════════════════════════════════════════════════════

        # -- Phase 3: Cold upgrade ---------------------------------------------
        rp.step_start("Phase 3  Cold upgrade (stop all nodes → upgrade → restart all nodes)")
        upgrade_ok = False
        try:
            upgrader     = ColdUpgrader(cluster_mgr, rp)
            target_taosd = get_taosd_path(to_dir)
            upgrade_ok   = upgrader.run(
                to_taosd_path=target_taosd,
                node_count=config.DNODE_COUNT,
                metrics=metrics,
            )
        except Exception as e:
            rp.error(f"Cold upgrade exception: {e}")
            import traceback; traceback.print_exc()

        if not upgrade_ok:
            checks.append(("Cold upgrade completed", False, ""))
            rp.summary_end(
                passed=False, checks=checks, start_time=test_start,
                write_max=0, query_max=0, sub_max=0,
            )
            sys.exit(1)

        checks.append(("Cold upgrade completed", True, "all nodes upgraded"))
        rp.check("Cold upgrade completed", True, "all nodes upgraded")
        rp.step_done("Phase 3")

        # -- Spawn post-upgrade subprocess with to_dir client library ----------
        # glibc reads LD_LIBRARY_PATH only once at process start, so libtaos.so
        # cannot be switched mid-process.  Spawn a fresh Python process that
        # starts with the target-version library for Phase 4-5.
        import pickle, subprocess as _subp
        _state_file = os.path.join(base_path, "_cold_phase2_state.pkl")
        with open(_state_file, "wb") as _f:
            pickle.dump({
                "from_ver":        from_ver,
                "to_ver":          to_ver,
                "fqdn":            fqdn,
                "base_path":       base_path,
                "cfg_dir":         cfg_dir,
                "checks":          list(checks),
                "test_start":      test_start,
                "rsma_supported":  rsma_supported,
                "tsma_supported":  tsma_supported,
                "stream_supported": stream_supported,
                "quick":           args.quick,
                "priv_before":     locals().get("priv_before"),
                "idx_before":      locals().get("idx_before"),
                "tsma_before":     locals().get("tsma_before"),
                "rsma_before":     locals().get("rsma_before"),
                "stream_before":   locals().get("stream_before"),
                "check_sysinfo":   args.check_sysinfo,
                "gen_whitelist":   args.gen_whitelist,
                "whitelist_dir":   whitelist_dir,
                "sysinfo_before":  _sysinfo_before,
                "from_ver_actual": _from_ver_actual,
            }, _f)
        _env = os.environ.copy()
        _env.pop("_TAOS_LIB_DIR", None)  # let prepare_native_lib re-init for to_dir
        _env[_COLD_PHASE2_ENV] = _state_file
        rp.info("Switching to target-version client library (Phase 4-5) ...")
        result = _subp.run([sys.executable] + sys.argv, env=_env)
        sys.exit(result.returncode)

    # -- Phase 5: Post-upgrade verification ------------------------------------
    rp.step_start(f"Phase 5  Post-upgrade verification ({config.VERIFY_DURATION_S}s)")

    # Wait for any in-flight slow queries that were disrupted by the last node
    # restart to complete and drain out of the workers before we reset the
    # window maximums.  Without this, a 16 s reconnect latency from Phase 4
    # can be written back to query_window_max *after* we zero it here and will
    # then appear as a false failure in the verification window.
    rp.info(f"Waiting {config.VERIFY_DURATION_S}s for cluster connections to stabilize ...")
    time.sleep(20)

    metrics["write_window_max"]     = 0.0
    metrics["query_window_max"]     = 0.0
    metrics["subscribe_window_max"] = 0.0

    deadline = time.time() + config.VERIFY_DURATION_S
    while time.time() < deadline:
        if not _workloads_ok(metrics, rp):
            break
        remaining = int(deadline - time.time())
        rp.info(
            f"[{remaining:3d}s]  "
            f"write={metrics.get('write_last_latency', 0):.3f}s  "
            f"query={metrics.get('query_last_latency', 0):.3f}s  "
            f"sub_gap={metrics.get('subscribe_last_gap', 0):.3f}s"
        )
        rp.info(
            f"         inserted: "
            f"ts={metrics.get('write_last_ts', 0)}  "
            f"curr={metrics.get('write_last_current', 0):.3f}A  "
            f"volt={metrics.get('write_last_voltage', 0)}V  "
            f"phase={metrics.get('write_last_phase', 0):.3f}  "
            f"|  count={metrics.get('query_last_count', 0):,}  "
            f"|  sub_recv={metrics.get('subscribe_recv_total', 0):,}  "
        )
        time.sleep(5)

    # Stop writer gracefully: signal via its dedicated event so it finishes
    # the current INSERT + metrics update before exiting.  Using terminate()
    # would risk a race where the row is committed to the DB (and consumed by
    # TMQ) but write_total_rows is never incremented, causing received > written.
    writer_proc = worker_procs[0]
    writer_stop_evt.set()
    writer_proc.join(timeout=10)
    if writer_proc.is_alive():
        writer_proc.terminate()
        writer_proc.join(timeout=3)

    final_written = metrics.get("write_total_rows", 0)
    rp.info(
        f"Writer stopped ({final_written:,} rows total). "
        f"Waiting up to {config.SUBSCRIBE_DRAIN_WAIT_S}s for subscriber to drain ..."
    )
    drain_deadline = time.time() + config.SUBSCRIBE_DRAIN_WAIT_S
    while time.time() < drain_deadline:
        time.sleep(2)
        cur_recv = metrics.get("subscribe_recv_total", 0)
        rp.info(
            f"  drain: written={final_written:,}  subscribed={cur_recv:,}  "
            f"gap={final_written - cur_recv:,}"
        )
        if cur_recv >= final_written:
            rp.info("Subscriber has caught up.")
            break
    else:
        rp.info(
            f"Drain wait timed out after {config.SUBSCRIBE_DRAIN_WAIT_S}s "
            f"(written={final_written:,}  subscribed={metrics.get('subscribe_recv_total', 0):,})"
        )

    # Grace period: give subscriber one extra poll cycle for any last in-flight message
    time.sleep(3)

    # Now stop querier and subscriber
    _stop_workers(worker_procs[1:], stop_evt)
    rp.step_done("Phase 5")

    # -- Check results ---------------------------------------------------------
    write_max = metrics.get("write_window_max", 0.0)
    query_max = metrics.get("query_window_max", 0.0)
    sub_max   = metrics.get("subscribe_window_max", 0.0)

    def _chk(label, ok, detail=""):
        checks.append((label, ok, detail))

    # Write pass: zero real failures (each failure = MAX_CONSECUTIVE_RETRIES retries all exhausted)
    write_fail_cnt = metrics.get("write_error_count", 0)
    write_ok = write_fail_cnt == 0
    _chk("Write: no failure batches", write_ok,
         f"failures={write_fail_cnt}  total_rows={metrics.get('write_total_rows', 0):,}  "
         f"retries={metrics.get('write_retry_count', 0):,}  max_latency={write_max:.3f}s")

    # Query pass: zero real failures
    query_fail_cnt = metrics.get("query_error_count", 0)
    query_ok = query_fail_cnt == 0
    _chk("Query: no failure batches", query_ok,
         f"failures={query_fail_cnt}  max_latency={query_max:.3f}s")

    # Subscribe pass: data received within SUBSCRIBE_NO_DATA_TIMEOUT_S seconds of test end
    last_recv = metrics.get("subscribe_last_recv_time", 0.0)
    sub_silence = time.time() - last_recv if last_recv > 0 else float("inf")
    sub_ok = sub_silence <= config.SUBSCRIBE_NO_DATA_TIMEOUT_S
    _chk(
        f"Subscribe: data received within {config.SUBSCRIBE_NO_DATA_TIMEOUT_S}s",
        sub_ok,
        f"silence={sub_silence:.1f}s  total_recv={metrics.get('subscribe_recv_total', 0):,}",
    )

    # Written == subscribed: every inserted row must have been consumed
    total_written = metrics.get("write_total_rows", 0)
    total_recv    = metrics.get("subscribe_recv_total", 0)
    rows_match    = total_written == total_recv
    _chk(
        "Subscribe: rows received == rows written",
        rows_match,
        f"written={total_written:,}  received={total_recv:,}  diff={total_written - total_recv:,}",
    )

    _RPC_SIG_MISMATCH_MSG = f"[0x0141/0x0140] client {from_ver} incompatible with server {to_ver}"

    def _is_sig_error(e):
        s = str(e)
        return (
            "0x0141" in s or "0x80000141" in s or "Invalid signature" in s or
            "0x0140" in s or "0x80000140" in s or "Edition not compatible" in s
        )

    def _safe_verify(label, fn, *args, **kwargs):
        """Call fn(*args) and _chk the result; catch [0x0141] as a compat failure."""
        try:
            ok, msg = fn(*args, **kwargs)
            _chk(label, ok, msg)
        except Exception as _e:
            if _is_sig_error(_e):
                _chk("RPC backward compatibility", False, _RPC_SIG_MISMATCH_MSG)
            else:
                raise

    # ------ test_user privilege verification ------
    uv = UserVerifier(fqdn=fqdn, cfg_dir=cfg_dir)
    auth_ok, auth_msg = uv.verify_auth()
    if not auth_ok and _is_sig_error(Exception(auth_msg)):
        auth_msg = _RPC_SIG_MISMATCH_MSG
    _chk("test_user authentication after upgrade", auth_ok, auth_msg)

    _priv_before = locals().get("priv_before", None)
    if _priv_before is not None:
        _safe_verify("test_user privileges unchanged", uv.verify_privileges, _priv_before)
    else:
        _chk("test_user privileges unchanged ", False,
             "baseline snapshot not captured (Phase 2 error)")

    # ------ tag index verification ------
    _idx_before = locals().get("idx_before", None)
    if _idx_before is not None:
        _safe_verify("Tag indexes preserved after upgrade", rm.verify_tag_indexes, _idx_before)
    else:
        _chk("Tag indexes preserved after upgrade", False,
             "baseline snapshot not captured (Phase 2 error)")

    # ------ TSMA verification ------
    if tsma_supported:
        _tsma_before = locals().get("tsma_before", None)
        if _tsma_before is not None:
            _safe_verify("TSMA preserved after upgrade", rm.verify_tsma, _tsma_before)
        else:
            _chk("TSMA preserved after upgrade", False,
                 "baseline snapshot not captured (Phase 2 error)")
    else:
        rp.info(f"TSMA check skipped: base version {from_ver} < 3.3.6.0")

    # ------ RSMA verification ------
    if rsma_supported:
        _rsma_before = locals().get("rsma_before", None)
        if _rsma_before is not None:
            _safe_verify("RSMA preserved after upgrade", rm.verify_rsma, _rsma_before)
        else:
            _chk("RSMA preserved after upgrade", False,
                 "baseline snapshot not captured (Phase 2 error)")
    else:
        rp.info(f"RSMA check skipped: base version {from_ver} < 3.3.8.0")

    # ------ Stream verification ------
    if stream_supported:
        _stream_before = locals().get("stream_before", None)
        if _stream_before is not None:
            _safe_verify("Stream preserved after upgrade", rm.verify_stream, _stream_before)
        else:
            _chk("Stream preserved after upgrade", False,
                 "baseline snapshot not captured (Phase 2 error)")
    else:
        rp.info(f"Stream check skipped: base version {from_ver} < 3.3.7.0")

    # ------ INFORMATION_SCHEMA check ------
    if args.check_sysinfo:
        _to_ver_actual = None
        _sysinfo_after = None
        try:
            _to_ver_actual = get_server_version(fqdn, cfg_dir)
            _sysinfo_after = sysinfo_snapshot(fqdn, cfg_dir)
            rp.info(f"SysInfo: server version (to) = {_to_ver_actual}, "
                    f"{len(_sysinfo_after)} tables captured")
        except Exception as _sie:
            rp.error(f"SysInfo snapshot (after) failed: {_sie}")
        if _sysinfo_before is not None and _sysinfo_after is not None:
            _si_diff = compare_snapshots(_sysinfo_before, _sysinfo_after)
            _wl = load_whitelists(whitelist_dir, _from_ver_actual or from_ver,
                                   _to_ver_actual or to_ver)
            if _wl.source_files:
                rp.info(f"SysInfo whitelist files: {_wl.source_files}")
            _fr = apply_whitelist(_si_diff, _wl)
            if _fr.has_unexpected():
                _cand_wl_path = _write_candidate_whitelist(
                    _si_diff, _from_ver_actual or from_ver, _to_ver_actual or to_ver)
                _detail = _fr.format_unexpected()
                if _cand_wl_path:
                    _detail += f"\n  {_cand_wl_path}"
                _chk("INFORMATION_SCHEMA: no unexpected changes", False, _detail)
            else:
                _si_detail = "(no changes)" if _si_diff.is_empty() else "(all changes whitelisted)"
                _chk("INFORMATION_SCHEMA: no unexpected changes", True, _si_detail)
                if _fr.format_expected():
                    rp.info(f"  Expected (whitelisted):\n{_fr.format_expected()}")
        elif _sysinfo_before is None:
            _chk("INFORMATION_SCHEMA: no unexpected changes", False,
                 "before-snapshot not captured")

    all_passed = all(ok for _, ok, _ in checks)

    rp.summary_end(
        passed=all_passed, checks=checks, start_time=test_start,
        write_max=write_max, query_max=query_max, sub_max=sub_max,
    )
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    # Use 'spawn' so each worker starts as a fresh interpreter.
    # It inherits os.environ (including LD_LIBRARY_PATH set by prepare_native_lib)
    # so `import taos` succeeds in the child without inheriting libtaos's mutable
    # C-level global state from the parent (which happens with 'fork').
    multiprocessing.set_start_method("spawn")
    main()
