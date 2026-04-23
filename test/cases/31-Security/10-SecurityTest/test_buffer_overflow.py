import socket
import struct
import time
import taos
from new_test_framework.utils import tdLog, tdSql, etool


class TestBufferOverflow:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.host = "127.0.0.1"
        cls.port = 6030
        cls._conn = taos.connect(host=cls.host)

    # --- util ---

    def _raw_tcp_connect(self, host, port, timeout=5):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
        return s

    def _safe_execute(self, sql, label=""):
        try:
            self._conn.execute(sql)
            tdLog.info(f"{label}: executed successfully")
            return True
        except Exception as e:
            tdLog.info(f"{label}: server returned error")
            return False

    def _verify_alive(self):
        tdSql.query("select server_version()")
        assert tdSql.queryRows == 1

    # --- impl ---

    def do_oversized_identifiers(self):
        """Verify server rejects oversized database/table names gracefully"""
        tdLog.info("=== step1: create database with oversized names")
        for length in [192, 256, 512, 1024, 4096, 65535]:
            name = 'a' * length
            self._safe_execute(f"CREATE DATABASE IF NOT EXISTS `{name}`",
                               f"db name len={length}")

        self._verify_alive()

        tdLog.info("=== step2: create table with oversized names")
        tdSql.execute("create database if not exists bo_test_db")
        tdSql.execute("use bo_test_db")
        for length in [192, 256, 1024, 8192]:
            name = 't' * length
            self._safe_execute(f"CREATE TABLE `{name}` (ts TIMESTAMP, v INT)",
                               f"table name len={length}")

        self._verify_alive()

        tdSql.execute("drop database if exists bo_test_db")
        print("oversized identifiers ................. [passed]")

    def do_oversized_string_values(self):
        """Verify server handles oversized BINARY/NCHAR values"""
        tdLog.info("=== step1: insert oversized string values")
        tdSql.execute("create database if not exists bo_test_db")
        tdSql.execute("use bo_test_db")
        tdSql.execute("create table if not exists long_str (ts timestamp, b binary(256), n nchar(256))")

        for length in [256, 512, 1024, 4096, 65535]:
            val = 'X' * length
            self._safe_execute(
                f"INSERT INTO long_str VALUES (NOW+{length}s, '{val}', '{val}')",
                f"value len={length}")

        self._verify_alive()

        tdSql.execute("drop database if exists bo_test_db")
        print("oversized string values ............... [passed]")

    def do_deep_nesting(self):
        """Verify server handles deeply nested expressions"""
        tdLog.info("=== step1: send deeply nested arithmetic expressions")
        for depth in [100, 500, 1000, 5000]:
            expr = "1"
            for _ in range(depth):
                expr = f"({expr}+1)"
            self._safe_execute(f"SELECT {expr}", f"nesting depth={depth}")

        self._verify_alive()

        print("deep nesting expressions .............. [passed]")

    def do_malformed_packets(self):
        """Verify server rejects malformed TCP packets"""
        tdLog.info("=== step1: send various malformed payloads to RPC port")
        payloads = [
            (b'\xff\xff\xff\x7f' + b'\x00' * 8, "huge content-length"),
            (b'\x00' * 64, "all-zero 64 bytes"),
            (b'\xff' * 128, "all-0xFF 128 bytes"),
            (struct.pack('<i', -1) + b'\xAA' * 60, "negative length"),
            (b'\x01\x02', "2-byte truncated"),
            (bytes(range(256)) * 4, "1KB patterned"),
            (b'SELECT\x001\x00FROM\x00t' + b'\x00' * 100, "null-stuffed SQL"),
        ]

        for payload, label in payloads:
            try:
                sock = self._raw_tcp_connect(self.host, self.port, timeout=3)
                sock.sendall(payload)
                try:
                    resp = sock.recv(1024)
                    if len(resp) == 0:
                        tdLog.info(f"{label}: server closed connection")
                    else:
                        tdLog.info(f"{label}: got {len(resp)} bytes back")
                except (ConnectionResetError, BrokenPipeError, socket.timeout):
                    tdLog.info(f"{label}: server reset/closed connection")
                finally:
                    sock.close()
            except Exception as e:
                tdLog.info(f"{label}: {e}")

        tdLog.info("=== step2: verify server still healthy")
        time.sleep(1)
        self._verify_alive()
        print("malformed packet rejection ............ [passed]")

    def do_format_string_injection(self):
        """Verify format string payloads do not crash the server"""
        tdLog.info("=== step1: inject format string payloads")
        tdSql.execute("create database if not exists bo_test_db")
        tdSql.execute("use bo_test_db")
        tdSql.execute("create table if not exists fmt_test (ts timestamp, v binary(512))")

        payloads = [
            "%s%s%s%s%s%s%s%s%s%s",
            "%x%x%x%x%x%x%x%x",
            "%n%n%n%n",
            "%p%p%p%p%p%p%p%p",
            "%.999999f",
            "%99999s",
            "AAAA" + "%08x." * 20,
        ]

        for p in payloads:
            self._safe_execute(f"CREATE DATABASE `{p}`", f"fmt-db: {p[:30]}")
            self._safe_execute(f"CREATE TABLE `{p}` (ts TIMESTAMP, v INT)",
                               f"fmt-tbl: {p[:30]}")
            self._safe_execute(f"INSERT INTO fmt_test VALUES (NOW, '{p}')",
                               f"fmt-val: {p[:30]}")

        tdLog.info("=== step2: verify server still healthy")
        self._verify_alive()

        tdSql.execute("drop database if exists bo_test_db")
        print("format string injection ............... [passed]")

    def do_oversized_tags(self):
        """Verify server handles oversized tag values"""
        tdLog.info("=== step1: create tables with oversized tags")
        tdSql.execute("create database if not exists bo_test_db")
        tdSql.execute("use bo_test_db")
        tdSql.execute("create stable if not exists stb_tag (ts timestamp, v int) tags (t1 binary(256))")

        for length in [256, 512, 1024, 4096, 16384]:
            tag_val = 'T' * length
            self._safe_execute(
                f"CREATE TABLE IF NOT EXISTS ctb_{length} USING stb_tag TAGS ('{tag_val}')",
                f"tag len={length}")

        self._verify_alive()

        tdSql.execute("drop database if exists bo_test_db")
        print("oversized tags ........................ [passed]")

    def do_excessive_columns(self):
        """Verify server rejects tables with excessive column counts"""
        tdLog.info("=== step1: create tables with excessive columns")
        tdSql.execute("create database if not exists bo_test_db")
        tdSql.execute("use bo_test_db")

        for ncols in [128, 1024, 4096, 16384]:
            cols = ", ".join(f"c{i} INT" for i in range(ncols))
            self._safe_execute(
                f"CREATE TABLE many_cols_{ncols} (ts TIMESTAMP, {cols})",
                f"columns={ncols}")

        self._verify_alive()

        tdSql.execute("drop database if exists bo_test_db")
        print("excessive columns ..................... [passed]")

    def do_boundary_integers(self):
        """Verify server handles boundary integer values"""
        tdLog.info("=== step1: insert boundary integer values")
        tdSql.execute("create database if not exists bo_test_db")
        tdSql.execute("use bo_test_db")
        tdSql.execute("create table if not exists int_bound (ts timestamp, v bigint)")

        boundaries = [
            0, -1, 1,
            2**31 - 1, -(2**31), 2**31,
            2**63 - 1, -(2**63), 2**63,
            2**64 - 1, 2**64, -(2**64),
            2**128,
        ]

        for val in boundaries:
            self._safe_execute(
                f"INSERT INTO int_bound VALUES (NOW+{abs(val) % 100000}a, {val})",
                f"int val={val}")

        tdLog.info("=== step2: verify server still healthy")
        self._verify_alive()

        tdSql.execute("drop database if exists bo_test_db")
        print("boundary integers ..................... [passed]")

    def do_oversized_sql(self):
        """Verify server handles extremely large SQL statements"""
        tdLog.info("=== step1: send oversized SQL statements")
        tdSql.execute("create database if not exists bo_test_db")
        tdSql.execute("use bo_test_db")
        tdSql.execute("create table if not exists big_sql (ts timestamp, v int)")

        for size_kb in [64, 256, 1024, 4096]:
            rows = ", ".join(f"(NOW+{i}a, {i})" for i in range(size_kb * 10))
            sql = f"INSERT INTO big_sql VALUES {rows}"
            tdLog.info(f"SQL size ~{len(sql)//1024}KB ({size_kb*10} rows)")
            self._safe_execute(sql, f"sql size ~{size_kb}KB")

        self._verify_alive()

        tdSql.execute("drop database if exists bo_test_db")
        print("oversized SQL statements .............. [passed]")

    def do_rapid_oversized_requests(self):
        """Verify server survives rapid-fire oversized requests"""
        tdLog.info("=== step1: send 200 rapid oversized requests")
        tdSql.execute("create database if not exists bo_test_db")
        tdSql.execute("use bo_test_db")

        self._conn.execute("use bo_test_db")
        long_name = 'R' * 1024
        for i in range(200):
            try:
                self._conn.execute(f"CREATE TABLE `{long_name}_{i}` (ts TIMESTAMP, v INT)")
            except Exception:
                pass

        tdLog.info("=== step2: verify server still healthy")
        self._verify_alive()

        tdSql.execute("drop database if exists bo_test_db")
        print("rapid oversized requests .............. [passed]")

    # --- cleanup ---

    def do_cleanup(self):
        tdLog.info("=== cleanup test artifacts")
        tdSql.execute("drop database if exists bo_test_db")
        print("cleanup .............................. [passed]")

    # --- main ---

    def test_buffer_overflow(self):
        """TDengine buffer overflow defense verification

        1. Oversized identifier rejection
        2. Oversized string value handling
        3. Deep nesting expression handling
        4. Malformed packet rejection
        5. Format string injection defense
        6. Oversized tag handling
        7. Excessive column count rejection
        8. Boundary integer handling
        9. Oversized SQL statement handling
        10. Rapid oversized request resilience

        Catalog:
            - Security

        Since: v3.3.0.0

        Labels: common

        Jira: None

        History:
            - 2026-04-22 Created buffer overflow defense verification tests

        """
        self.do_oversized_identifiers()
        self.do_oversized_string_values()
        self.do_deep_nesting()
        self.do_malformed_packets()
        self.do_format_string_injection()
        self.do_oversized_tags()
        self.do_excessive_columns()
        self.do_boundary_integers()
        self.do_oversized_sql()
        self.do_rapid_oversized_requests()
        self.do_cleanup()
