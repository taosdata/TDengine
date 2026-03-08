#!/usr/bin/env python3
# filepath: hot_update/run/main.py
#
# IMPORTANT import order
# ----------------------
# 1. stdlib only
# 2. Parse --from-dir early (before any taos import)
# 3. config_lib.prepare_native_lib(from_dir)  <- sets LD_LIBRARY_PATH + symlink
# 4. Everything else (taos, clusterSetup, ...)

import sys
import os
import socket
import argparse
import time
import multiprocessing

# -- Very early arg parse: need --from-dir before any taos import --------------

def _early_args():
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--from-dir", "-F", required=True)
    p.add_argument("--to-dir",   "-T", required=True)
    p.add_argument("--path",     "-p", default=os.path.expanduser("~/td_rolling_upgrade"))
    p.add_argument("--fqdn",     "-f", default="")
    p.add_argument("--quick",    "-q", action="store_true")
    p.add_argument("--help",     "-h", action="store_true")
    args, _ = p.parse_known_args()
    return args

_args = _early_args()

# -- Configure LD_LIBRARY_PATH BEFORE any taos import -------------------------

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config_lib import prepare_native_lib, taosd_path as get_taosd_path

prepare_native_lib(_args.from_dir)

# -- Now safe to import taos-dependent modules ---------------------------------

import config
from server.clusterSetup import ClusterManager, Logger
from server.rollingUpgrade import RollingUpgrader
from resource.resourceManager import ResourceManager
from resource.verifier import Verifier
from client.writer import writer_main
from client.querier import querier_main
from client.subscriber import subscriber_main
from run.reporter import Reporter


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
    m["write_last_ts"]            = 0
    m["query_last_count"]         = 0
    m["subscribe_recv_total"]     = 0
    m["subscribe_last_batch_rows"] = 0
    # Phase-4 success counters (reset at start of rolling upgrade)
    m["write_phase4_success"]     = 0
    m["query_phase4_success"]     = 0
    m["subscribe_phase4_recv"]    = 0
    for k in ["write_failed", "query_failed", "subscribe_failed"]:
        m[k] = False
    for k in ["write_error_count", "query_error_count", "subscribe_error_count"]:
        m[k] = 0
    return m


def _start_workers(fqdn, cfg_dir, metrics, stop_event):
    procs = []
    for fn in (writer_main, querier_main, subscriber_main):
        p = multiprocessing.Process(
            target=fn, args=(fqdn, cfg_dir, metrics, stop_event), daemon=True
        )
        p.start()
        procs.append(p)
    return procs


def _stop_workers(procs, stop_event):
    stop_event.set()
    for p in procs:
        p.join(timeout=10)
        if p.is_alive():
            p.terminate()


def _workloads_ok(metrics, rp):
    if metrics.get("write_failed"):
        rp.error("Writer process reported a fatal failure.")
        return False
    if metrics.get("query_failed"):
        rp.error("Querier process reported a fatal failure.")
        return False
    if metrics.get("subscribe_failed"):
        rp.error("Subscriber process reported a fatal failure.")
        return False
    return True


# ==============================================================================
# Main
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="TDengine rolling upgrade test")
    parser.add_argument("--from-dir", "-F", required=True)
    parser.add_argument("--to-dir",   "-T", required=True)
    parser.add_argument("--path",     "-p", default=os.path.expanduser("~/td_rolling_upgrade"))
    parser.add_argument("--fqdn",     "-f", default="")
    parser.add_argument("--quick",    "-q", action="store_true",
                        help="Quick mode: 100 subtables x 1000 rows")
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

    from_ver   = os.path.basename(from_dir)
    to_ver     = os.path.basename(to_dir)
    rp         = Reporter()
    test_start = time.time()

    rp.summary_start(
        from_ver=from_ver, to_ver=to_ver,
        fqdn=fqdn,
        dnode_count=config.DNODE_COUNT,
        mnode_count=config.MNODE_COUNT,
        subtables=config.SUBTABLE_COUNT,
        rows_per_table=config.INIT_ROWS_PER_SUBTABLE,
        verify_window=config.VERIFY_DURATION_S,
    )

    checks = []

    # -- Phase 1: Cluster setup ------------------------------------------------
    rp.step_start("Phase 1  Cluster setup (base version)")
    try:
        base_taosd  = get_taosd_path(from_dir)
        cluster_mgr = ClusterManager(fqdn=fqdn, base_path=base_path, level=1, disk=1,
                                       logger=_SilentLogger())

        # Point DNodeManager at base-version taosd BEFORE create_cluster() starts
        # processes, so every node runs the correct version from the beginning.
        cluster_mgr.dnode_manager.taosd_path = base_taosd

        cluster_mgr.create_cluster(
            dnode_nums=config.DNODE_COUNT,
            mnode_nums=config.MNODE_COUNT,
        )
        cluster_mgr.connect()

        if config.DNODE_COUNT > 1:
            cluster_mgr.create_dnodes_in_cluster(config.DNODE_COUNT)
        cluster_mgr.create_mnodes_in_cluster(config.MNODE_COUNT)
        cluster_mgr.wait_for_cluster_ready(timeout=60)
    except Exception as e:
        rp.error(f"Cluster setup failed: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)
    rp.step_done("Phase 1")

    # -- Phase 2: Resource preparation -----------------------------------------
    rp.step_start("Phase 2  Resource preparation (DB / tables / data / topic)")
    try:
        rm = ResourceManager(fqdn=fqdn, cfg_dir=cfg_dir, logger=_SilentLogger())
        rm.create_database()
        rp.info(f"Database '{config.DB_NAME}' (replica={config.REPLICA}) created")
        rm.create_stable()
        rp.info(f"Stable '{config.STABLE_NAME}' created")
        rm.create_subtables()
        rp.info(f"{config.SUBTABLE_COUNT} subtables created")
        rm.write_initial_data()
        rp.info(
            f"Initial data: {config.SUBTABLE_COUNT} x {config.INIT_ROWS_PER_SUBTABLE:,} rows written"
        )
        rm.create_topic()
        rp.info(f"Topic '{config.TOPIC_NAME}' created")
    except Exception as e:
        rp.error(f"Resource preparation failed: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)
    rp.step_done("Phase 2")

    # -- Phase 3: Background workloads -----------------------------------------
    rp.step_start("Phase 3  Background workloads (write / query / subscribe)")
    manager      = multiprocessing.Manager()
    metrics      = _init_metrics(manager)
    stop_evt     = manager.Event()
    worker_procs = _start_workers(fqdn, cfg_dir, metrics, stop_evt)

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

    # -- Phase 4: Rolling upgrade -----------------------------------------------
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
        f"\n             writes     succeeded={metrics.get('write_phase4_success', 0):,}  rows"
        f"\n             queries    succeeded={metrics.get('query_phase4_success', 0):,}  "
        f"\n             subscribed succeeded={metrics.get('subscribe_phase4_recv', 0):,} rows"
        f"\n        -------------------------------------------------------------\n"
    )
    rp.step_done("Phase 4")

    # -- Phase 5: Post-upgrade verification ------------------------------------
    rp.step_start(f"Phase 5  Post-upgrade verification ({config.VERIFY_DURATION_S}s)")

    # Wait for any in-flight slow queries that were disrupted by the last node
    # restart to complete and drain out of the workers before we reset the
    # window maximums.  Without this, a 16 s reconnect latency from Phase 4
    # can be written back to query_window_max *after* we zero it here and will
    # then appear as a false failure in the verification window.
    rp.info("Waiting 20s for cluster connections to stabilize ...")
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
            f"(last_batch={metrics.get('subscribe_last_batch_rows', 0)})"
        )
        time.sleep(5)

    _stop_workers(worker_procs, stop_evt)
    rp.step_done("Phase 5")

    # -- Check results ---------------------------------------------------------
    write_max = metrics.get("write_window_max", 0.0)
    query_max = metrics.get("query_window_max", 0.0)
    sub_max   = metrics.get("subscribe_window_max", 0.0)

    def _chk(label, ok, detail=""):
        checks.append((label, ok, detail))
        rp.check(label, ok, detail)

    write_ok = (not metrics.get("write_failed")) and write_max <= config.MAX_WRITE_LATENCY_S
    query_ok = (not metrics.get("query_failed")) and query_max <= config.MAX_QUERY_LATENCY_S
    sub_ok   = (not metrics.get("subscribe_failed")) and sub_max <= config.MAX_SUBSCRIBE_GAP_S

    _chk(f"Write latency <= {config.MAX_WRITE_LATENCY_S}s", write_ok, f"max={write_max:.3f}s")
    _chk(f"Query latency <= {config.MAX_QUERY_LATENCY_S}s", query_ok, f"max={query_max:.3f}s")
    _chk(f"Subscribe gap <= {config.MAX_SUBSCRIBE_GAP_S}s", sub_ok,   f"max={sub_max:.3f}s")

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
