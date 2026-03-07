#!/usr/bin/env python3
# filepath: hot_update/server/rollingUpgrade.py

import os
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

        self._install_binary(index, to_taosd_path)
        self._log(f"DNode {index}: binary replaced")

        if not self._start_node(index):
            return False

        if not self._wait_dnode_ready(index):
            return False

        time.sleep(3)

        if metrics.get("write_failed") or metrics.get("query_failed") or metrics.get("subscribe_failed"):
            self._err(f"Workload failure detected after upgrading DNode {index}")
            return False

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
