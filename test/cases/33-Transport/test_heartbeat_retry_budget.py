import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from new_test_framework.utils import tdLog, tdSql, tdDnodes


CONNECT_TIMEOUT_SEC = 60
STORM_WARMUP_SEC = 25
MEASURE_WINDOW_SEC = 10
MAX_RETRY_ATTEMPTS = 120  # bounded retries keep this at a few per second; unbounded chains grow into hundreds


class TestHeartbeatRetryBudget:
    """Verify client heartbeat retry attempts stay bounded when the server is unreachable."""

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    @staticmethod
    def taos_bin():
        path = tdDnodes.dnodes[0].getPath("taos")
        if os.path.exists(path):
            return path
        if sys.platform == "win32" and os.path.exists(path + ".exe"):
            return path + ".exe"
        raise AssertionError(f"taos client not found: {path}")

    def test_heartbeat_retry_budget(self):
        """Heartbeat retry attempts are bounded when the server is unreachable

        Behavioural verification (no log inspection):
        1. Start a taos client connected to the cluster and confirm the server
           reports its connection (performance_schema.perf_connections), which
           proves the client heartbeat is running.
        2. Stop the only dnode and bind a TCP listener on the mnode port. Every
           heartbeat retry from the client (and from the test connection) shows
           up as one accepted connection with an immediate close.
        3. After a warm-up, count accepted connections for a fixed window. With
           bounded heartbeat retries the count stays at a few attempts per second;
           without the fix the retry chains accumulate for retryMaxWaitTime and
           the count grows into hundreds.

        Catalog:
            - Transport:Heartbeat

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-11-23 Created
        """
        tdSql.query("select endpoint from information_schema.ins_mnodes")
        endpoint = tdSql.queryResult[0][0]
        _, port_text = endpoint.rsplit(":", 1)
        port = int(port_text)
        tdLog.info(f"mnode endpoint: {endpoint}")

        cfg_dir = tempfile.mkdtemp(prefix="hb_retry_budget_")
        log_dir = os.path.join(cfg_dir, "log")
        os.makedirs(log_dir, exist_ok=True)
        with open(os.path.join(cfg_dir, "taos.cfg"), "w") as f:
            f.write(f"firstEp {endpoint}\n")
            f.write("fqdn localhost\n")
            f.write("maxRetryWaitTime 300000\n")
            f.write(f"logDir {log_dir}\n")

        cli = subprocess.Popen(
            [self.taos_bin(), "-c", cfg_dir],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        listener = None
        try:
            deadline = time.time() + CONNECT_TIMEOUT_SEC
            connected = False
            while time.time() < deadline:
                if cli.poll() is not None:
                    raise AssertionError(f"taos client exited early, rc={cli.returncode}")
                tdSql.query("select pid from performance_schema.perf_connections")
                if any(row[0] is not None and int(row[0]) == cli.pid for row in tdSql.queryResult):
                    connected = True
                    break
                time.sleep(0.5)
            if not connected:
                raise AssertionError("taos client connection was not reported by the cluster heartbeat")

            tdDnodes.stop(1)

            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            bind_deadline = time.time() + 30
            while True:
                try:
                    listener.bind(("", port))
                    break
                except OSError:
                    if time.time() > bind_deadline:
                        raise
                    time.sleep(0.5)
            listener.listen(128)
            listener.settimeout(0.2)

            counter = [0]
            stop_event = threading.Event()

            def accept_loop():
                while not stop_event.is_set():
                    try:
                        conn, _ = listener.accept()
                    except socket.timeout:
                        continue
                    except OSError:
                        break
                    counter[0] += 1
                    try:
                        conn.close()
                    except OSError:
                        pass

            thread = threading.Thread(target=accept_loop, daemon=True)
            thread.start()

            time.sleep(STORM_WARMUP_SEC)
            counter[0] = 0
            time.sleep(MEASURE_WINDOW_SEC)
            attempts = counter[0]
            stop_event.set()
            listener.close()
            thread.join(timeout=2)
            listener = None
            tdLog.info(f"heartbeat connection attempts in {MEASURE_WINDOW_SEC}s: {attempts}")

            if cli.poll() is not None:
                raise AssertionError("taos client exited while the server was unreachable")
            if attempts == 0:
                raise AssertionError("no heartbeat retry attempt observed; the retry path was not exercised")
            if attempts > MAX_RETRY_ATTEMPTS:
                raise AssertionError(
                    f"heartbeat retries are not bounded: {attempts} attempts in {MEASURE_WINDOW_SEC}s "
                    f"(limit {MAX_RETRY_ATTEMPTS})"
                )
            tdLog.info("heartbeat retry attempts stayed bounded after the endpoint became unreachable")
            print("heartbeat retry budget bounds retry attempts ........ [ passed ]")
        finally:
            if listener is not None:
                try:
                    listener.close()
                except OSError:
                    pass
            try:
                if cli.poll() is None:
                    cli.terminate()
                    cli.wait(timeout=10)
            except Exception:
                cli.kill()
            try:
                if cli.stdin:
                    cli.stdin.close()
            except Exception:
                pass
            shutil.rmtree(cfg_dir, ignore_errors=True)
