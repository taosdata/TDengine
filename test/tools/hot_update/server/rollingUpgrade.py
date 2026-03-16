#!/usr/bin/env python3
# filepath: hot_update/server/rollingUpgrade.py

import os
import re
import sys
import time
import shutil
import signal
import random
import subprocess
from typing import Optional, List

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config
from server.clusterSetup import ClusterManager


# ==================== helpers ====================

def _get_taosd_version(bin_path: str) -> str:
    """Run `bin_path -V` and return the version string, e.g. '3.4.0.8.enterprise'."""
    try:
        result = subprocess.run(
            [bin_path, "-V"],
            capture_output=True, text=True, timeout=5
        )
        output = result.stdout + result.stderr
        # 3.4+: "taosd version: 3.4.0.8.enterprise"
        # 3.3.0.x: "enterprise version: 3.3.0.0"
        m = re.search(r'(?:taosd|enterprise) version:\s*(\S+)', output)
        return m.group(1) if m else "unknown"
    except Exception:
        return "unknown"

def _find_taosd_pid(cfg_dir: str) -> Optional[int]:
    try:
        result = subprocess.run(
            ["pgrep", "-f", f"taosd.*{cfg_dir}"],
            capture_output=True, text=True
        )
        pids = [int(p) for p in result.stdout.strip().split() if p.isdigit()]
        return pids[0] if pids else None
    except Exception:
        return None


def _wait_port_listen(port: int, timeout: int = 120) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = subprocess.run(
            f"lsof -i tcp:{port} | grep -q LISTEN",
            shell=True, capture_output=True
        )
        if result.returncode == 0:
            return True
        time.sleep(2)
    return False


def _node_bin_dir(base_path: str, index: int) -> str:
    return os.path.join(base_path, f"dnode{index}", "bin")


def _node_bin_path(base_path: str, index: int) -> str:
    return os.path.join(_node_bin_dir(base_path, index), "taosd")


# ==================== RollingUpgrader ====================

class RollingUpgrader:
    """
    Orchestrates rolling upgrade of all DNodes from base to target version.
    Uses per-node private taosd binary copies so upgrading one node does not
    affect others.
    """

    def __init__(self, cluster_mgr: ClusterManager, reporter=None):
        self.cluster_mgr = cluster_mgr
        self.base_path   = cluster_mgr.base_path
        self.fqdn        = cluster_mgr.fqdn
        self.rp          = reporter   # duck-typed: needs .info() .error() .warn()

    def _log(self, msg):
        if self.rp:
            self.rp.info(msg)

    def _err(self, msg):
        if self.rp:
            self.rp.error(msg)

    def _node_versions(self) -> dict:
        """
        Return a dict mapping dnode index -> version string by running
        each node's private taosd binary with -V.
        Falls back to the cluster manager's base taosd for nodes not yet upgraded.
        """
        base_taosd = getattr(self.cluster_mgr.dnode_manager, "taosd_path", None)
        versions = {}
        for dnode in self.cluster_mgr.dnode_manager.dnodes:
            bin_path = _node_bin_path(self.base_path, dnode.index)
            if not os.path.isfile(bin_path):
                bin_path = base_taosd or ""
            versions[dnode.index] = _get_taosd_version(bin_path) if bin_path else "unknown"
        return versions

    def _print_dnodes(self, title: str = ""):
        """Query SHOW DNODES and print a compact status table via the reporter."""
        import taos as _taos

        rows = None
        # Try each dnode in order until one accepts a connection.
        # Pass port explicitly so the client connects to that node's own port
        # rather than falling through to firstEp (dnode1:6030) in every config.
        for dnode in self.cluster_mgr.dnode_manager.dnodes:
            try:
                tmp_conn = _taos.connect(host=self.fqdn, port=dnode.port,
                                         config=dnode.cfg_dir)
                cursor = tmp_conn.cursor()
                cursor.execute("SHOW DNODES")
                rows = cursor.fetchall()
                cursor.close()
                tmp_conn.close()
                break
            except Exception:
                continue

        if rows is None:
            self._log("  [SHOW DNODES unavailable: cluster electing new mnode leader, will recover shortly]")
            return

        self._render_dnodes(rows, title, self._node_versions())

    def _print_dnodes_after_stop(self, stopped_index: int, timeout: int = 30):
        """
        Poll SHOW DNODES until `stopped_index` no longer shows 'ready', then
        print the table.  This ensures the snapshot reflects the actual offline
        state rather than the stale MNode cache from before the heartbeat times out.
        """
        import taos as _taos

        title    = f"after DNode {stopped_index} stopped"
        deadline = time.time() + timeout

        while time.time() < deadline:
            rows = None
            for dnode in self.cluster_mgr.dnode_manager.dnodes:
                if dnode.index == stopped_index:
                    continue   # skip the node we just stopped
                try:
                    tmp_conn = _taos.connect(host=self.fqdn, port=dnode.port,
                                             config=dnode.cfg_dir)
                    cursor = tmp_conn.cursor()
                    cursor.execute("SHOW DNODES")
                    rows = cursor.fetchall()
                    cursor.close()
                    tmp_conn.close()
                    break
                except Exception:
                    continue

            if rows is None:
                # All other nodes unreachable – mnode quorum lost; print note and return
                self._log("  [SHOW DNODES unavailable: cluster electing new mnode leader, will recover shortly]")
                return

            # Check if the stopped node has transitioned out of 'ready'
            for row in rows:
                if row[0] == stopped_index and str(row[4]) != "ready":
                    self._render_dnodes(rows, title, self._node_versions())
                    return

            time.sleep(2)

        # Timed out – print the last snapshot with a note
        if rows is not None:
            self._render_dnodes(rows, title + "  (heartbeat timeout pending)", self._node_versions())

    def _render_dnodes(self, rows, title: str, versions: dict = None):
        """Print a pre-fetched SHOW DNODES result set, with an optional Version column.

        SHOW DNODES column layouts:
          3.3.x (7 cols):  id, endpoint, vnodes, support_vnodes, status,
                           create_time, note
          3.4.x (9 cols):  id, endpoint, vnodes, support_vnodes, status,
                           create_time, reboot_time, note, machine_id
        """
        versions = versions or {}
        header = (
            f"  {'ID':>3}  {'Endpoint':<24}  {'VNodes':>6}  {'Status':<10}  "
            f"{'Version':<28}  {'Reboot':<24}  Note"
        )
        sep = (
            f"  {'-'*3}  {'-'*24}  {'-'*6}  {'-'*10}  "
            f"{'-'*28}  {'-'*24}  {'-'*20}"
        )
        self._log(f"  ── SHOW DNODES  {title}" if title else "  ── SHOW DNODES")
        self._log(header)
        self._log(sep)
        for row in rows:
            dnode_id   = row[0]
            endpoint   = str(row[1])
            vnode_num  = row[2]
            status     = str(row[4])
            # Detect column layout by row width:
            #   len == 7  → 3.3.x:  note at [6], no reboot_time
            #   len >= 9  → 3.4.x:  reboot_time at [6], note at [7]
            if len(row) >= 9:
                reboot_time = str(row[6]) if row[6] else "-"
                note        = str(row[7]) if row[7] else ""
            elif len(row) >= 7:
                reboot_time = "-"
                note        = str(row[6]) if row[6] else ""
            else:
                reboot_time = "-"
                note        = ""
            version    = versions.get(dnode_id, "-")
            status_fmt = "[OFFLINE]" if status != "ready" else "ready   "
            self._log(
                f"  {dnode_id:>3}  {endpoint:<24}  {vnode_num:>6}  {status_fmt:<10}  "
                f"{version:<28}  {reboot_time:<24}  {note}"
            )
        self._log("")

    # ------------------------------------------------------------------
    # Node operations
    # ------------------------------------------------------------------

    def _stop_node(self, index: int) -> bool:
        dnode = next(
            (d for d in self.cluster_mgr.dnode_manager.dnodes if d.index == index), None
        )
        if not dnode:
            self._err(f"DNode {index} config not found")
            return False

        pid = _find_taosd_pid(dnode.cfg_dir)
        if pid is None:
            self._log(f"DNode {index}: no running taosd found (already stopped)")
            return True

        self._log(f"DNode {index}: stopping (pid={pid}) ...")
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return True

        deadline = time.time() + config.GRACEFUL_STOP_TIMEOUT
        while time.time() < deadline:
            try:
                os.kill(pid, 0)
                time.sleep(1)
            except ProcessLookupError:
                self._log(f"DNode {index}: stopped")
                return True

        self._log(f"DNode {index}: graceful stop timed out, SIGKILL ...")
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        time.sleep(2)
        return True

    def _install_binary(self, index: int, taosd_src: str):
        """Copy taosd_src into this node's private bin dir."""
        bin_dir  = _node_bin_dir(self.base_path, index)
        bin_path = _node_bin_path(self.base_path, index)
        os.makedirs(bin_dir, exist_ok=True)
        shutil.copy2(taosd_src, bin_path)
        os.chmod(bin_path, 0o755)

    def _start_node(self, index: int) -> bool:
        dnode = next(
            (d for d in self.cluster_mgr.dnode_manager.dnodes if d.index == index), None
        )
        if not dnode:
            self._err(f"DNode {index} config not found")
            return False

        bin_path = _node_bin_path(self.base_path, index)
        cmd      = f"nohup {bin_path} -c {dnode.cfg_dir} > /dev/null 2>&1 &"
        subprocess.Popen(cmd, shell=True,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(1)

        if _wait_port_listen(dnode.port, timeout=120):
            self._log(f"DNode {index}: port {dnode.port} listening")
            return True
        self._err(f"DNode {index}: port {dnode.port} did not come up within 120s")
        return False

    def _wait_dnode_ready(self, index: int) -> bool:
        self._log(f"DNode {index}: waiting for ready status ...")
        deadline = time.time() + config.NODE_READY_TIMEOUT_S
        while time.time() < deadline:
            try:
                cursor = self.cluster_mgr.conn.cursor()
                cursor.execute("SHOW DNODES")
                rows = cursor.fetchall()
                cursor.close()
                for row in rows:
                    if row[0] == index and row[4] == "ready":
                        return True
            except Exception as e:
                self._log(f"DNode {index}: status check error: {e}")
            time.sleep(3)
        self._err(f"DNode {index}: did not become ready within {config.NODE_READY_TIMEOUT_S}s")
        return False

    # ------------------------------------------------------------------
    # Upgrade single node
    # ------------------------------------------------------------------

    def upgrade_node(self, index: int, to_taosd_path: str, metrics) -> bool:
        if not self._stop_node(index):
            return False

        # Poll until cluster marks the node offline, then print snapshot
        self._print_dnodes_after_stop(stopped_index=index)

        # Simulate the time required to install a new package in production
        self._log(
            f"DNode {index}: waiting {config.NODE_INSTALL_SLEEP_S}s "
            f"(simulating package installation) ..."
        )
        time.sleep(config.NODE_INSTALL_SLEEP_S)

        self._install_binary(index, to_taosd_path)
        self._log(f"DNode {index}: binary replaced")

        if not self._start_node(index):
            return False

        if not self._wait_dnode_ready(index):
            return False

        # Print cluster state after node is back and READY
        self._print_dnodes(title=f"after DNode {index} ready")

        time.sleep(3)

        if hasattr(self.rp, "node_upgrade_done"):
            self.rp.node_upgrade_done(index)
        return True

    # ------------------------------------------------------------------
    # Run all nodes in random order
    # ------------------------------------------------------------------

    def run(self, to_taosd_path: str, node_count: int, metrics) -> bool:
        to_taosd_path = os.path.abspath(to_taosd_path)
        if not os.path.isfile(to_taosd_path):
            self._err(f"Target taosd not found: {to_taosd_path}")
            return False
        os.chmod(to_taosd_path, 0o755)

        order: List[int] = list(range(1, node_count + 1))
        random.shuffle(order)

        if hasattr(self.rp, "node_upgrade"):
            self.rp.info(f"Upgrade order: {order}")

        for i, idx in enumerate(order, 1):
            self._log(f"--- Upgrading DNode {idx}  ({i}/{node_count}) ---")
            if not self.upgrade_node(idx, to_taosd_path, metrics):
                self._err(f"Upgrade failed at DNode {idx}")
                return False

        return True


# ==================== ColdUpgrader ====================

class ColdUpgrader(RollingUpgrader):
    """
    Orchestrates cold (offline) upgrade of all DNodes:
      1. Stop every node
      2. Replace binaries on every node
      3. Start every node
      4. Wait for all nodes to become ready
    Background workloads are NOT running during this process.
    """

    def run(self, to_taosd_path: str, node_count: int, metrics) -> bool:
        to_taosd_path = os.path.abspath(to_taosd_path)
        if not os.path.isfile(to_taosd_path):
            self._err(f"Target taosd not found: {to_taosd_path}")
            return False
        os.chmod(to_taosd_path, 0o755)

        order: List[int] = list(range(1, node_count + 1))

        # Step 1: stop all nodes
        self._log("Cold upgrade: stopping all nodes ...")
        for idx in order:
            self._log(f"  DNode {idx}: stopping ...")
            if not self._stop_node(idx):
                self._err(f"Failed to stop DNode {idx}")
                return False
        self._log("All nodes stopped.")

        # Step 2: replace binaries on all nodes
        for idx in order:
            self._install_binary(idx, to_taosd_path)
            self._log(f"  DNode {idx}: binary replaced")

        # Step 3: start all nodes
        self._log("Cold upgrade: starting all nodes ...")
        for idx in order:
            self._log(f"  DNode {idx}: starting ...")
            if not self._start_node(idx):
                self._err(f"Failed to start DNode {idx}")
                return False

        # Step 4 is intentionally omitted: the parent process still uses the
        # old (from_dir) client library which cannot connect to the upgraded
        # servers.  Connectivity and node-ready checking is performed by the
        # Phase 4-5 subprocess that starts with the new (to_dir) library.
        self._log("All nodes started (port listening confirmed).")
        return True
