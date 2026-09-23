# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

import base64
import hashlib
import json
import os
import shutil
import socket
import socketserver
import struct
import threading
import time
import urllib.parse

from new_test_framework.utils import tdLog, tdSql, tdStream


CALLER_DIR = os.path.dirname(os.path.realpath(__file__))
NOTIFY_RESULT_DIR = os.path.join(CALLER_DIR, "notify_result_tmp")
NOTIFY_PATH = "external_window_multigroup"
NOTIFY_PORT = 12345


class _NotifyHandler(socketserver.StreamRequestHandler):
    def handle(self):
        request = self.rfile.readline().decode("ascii").strip().split()
        if len(request) != 3:
            return
        headers = {}
        while True:
            line = self.rfile.readline().decode("ascii").strip()
            if not line:
                break
            key, value = line.split(":", 1)
            headers[key.lower()] = value.strip()

        websocket_key = headers.get("sec-websocket-key")
        if websocket_key is None:
            return
        accept = base64.b64encode(
            hashlib.sha1(
                (websocket_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()
            ).digest()
        ).decode()
        self.wfile.write(
            (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
            ).encode("ascii")
        )
        self.wfile.flush()

        path = urllib.parse.urlsplit(request[1]).path.strip("/") or "root"
        log_path = os.path.join(NOTIFY_RESULT_DIR, f"{path}.log")
        while True:
            header = self.rfile.read(2)
            if len(header) != 2:
                return
            opcode = header[0] & 0x0F
            masked = (header[1] & 0x80) != 0
            length = header[1] & 0x7F
            if length == 126:
                length = struct.unpack("!H", self.rfile.read(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", self.rfile.read(8))[0]
            mask = self.rfile.read(4) if masked else b""
            payload = self.rfile.read(length)
            if masked:
                payload = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
            if opcode == 8:
                return
            if opcode == 9:
                self._send_frame(10, payload)
                continue
            if opcode != 1:
                continue
            with self.server.log_lock:
                with open(log_path, "a", encoding="utf-8") as log_file:
                    log_file.write(payload.decode("utf-8") + "\n")

    def _send_frame(self, opcode, payload):
        header = bytes([0x80 | opcode])
        length = len(payload)
        if length < 126:
            header += bytes([length])
        elif length <= 0xFFFF:
            header += bytes([126]) + struct.pack("!H", length)
        else:
            header += bytes([127]) + struct.pack("!Q", length)
        self.wfile.write(header + payload)
        self.wfile.flush()


class _NotifyServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(self):
        self.client_lock = threading.Lock()
        self.clients = set()
        super().__init__(("0.0.0.0", NOTIFY_PORT), _NotifyHandler)
        self.log_lock = threading.Lock()

    def process_request_thread(self, request, client_address):
        with self.client_lock:
            self.clients.add(request)
        try:
            super().process_request_thread(request, client_address)
        finally:
            with self.client_lock:
                self.clients.discard(request)

    def close_clients(self):
        with self.client_lock:
            clients = list(self.clients)
        for client in clients:
            try:
                client.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            client.close()


class TestExternalWindowMultiGroupNotify:
    DB = "ext_mg_notify"
    RETRY = 60

    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.createSnode(1)

    def test_grouped_notifications_keep_group_window_and_table_identity(self):
        """External-window grouped notifications keep exact identities.

        Validate grouped notifications keep group window and table identity behavior.

        Catalog:
            - Streams:05-Notify:ExternalWindow

        Since: v3.4.2.0

        Labels: common,ci,integration,functional

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/7024149350

        History:
            - 2026-08-24 OpenAI Added multi-group notification coverage
        """

        tdStream.dropAllStreamsAndDbs()
        shutil.rmtree(NOTIFY_RESULT_DIR, ignore_errors=True)
        os.makedirs(NOTIFY_RESULT_DIR, exist_ok=True)
        notify_server = _NotifyServer()
        notify_thread = threading.Thread(target=notify_server.serve_forever, daemon=True)
        notify_thread.start()
        time.sleep(1)
        try:
            tables = self._prepare_tables()
            tdSql.execute(
                f"create stream {self.DB}.s_notify interval(1s) sliding(1s) "
                f"from {self.DB}.trig partition by gid "
                "stream_options(event_type(window_close)) "
                f"notify('ws://localhost:{NOTIFY_PORT}/{NOTIFY_PATH}') "
                "on(window_close) "
                f"into {self.DB}.r_notify (ts,total) tags(out_gid int as %%1) "
                f"as select _twstart ts,sum(v) total from {self.DB}.calc "
                "where ts>=_twstart and ts<_twend and gid=%%1"
            )
            tdStream.checkStreamStatus("s_notify")
            self._write_trigger_rows(tables)

            result_sql = (
                f"select ts,out_gid,total from {self.DB}.r_notify order by out_gid,ts"
            )
            self._wait_rows(result_sql, 3 * len(tables))
            result_rows = self._query_rows(result_sql)

            tdSql.query(
                f"select tbname,out_gid from {self.DB}.r_notify "
                "group by tbname,out_gid"
            )
            table_to_gid = {
                tdSql.getData(row, 0): tdSql.getData(row, 1)
                for row in range(tdSql.getRows())
            }
            assert sorted(table_to_gid.values()) == sorted(gid for _, gid in tables)

            events = self._wait_events(3 * len(tables))
            observed = []
            trigger_ids = set()
            for event in events:
                table_name = event.get("tableName")
                assert table_name in table_to_gid, event
                gid = table_to_gid[table_name]
                rows = (event.get("result") or {}).get("data") or []
                assert len(rows) == 1, event
                row = rows[0]
                assert row.get("ts") == event.get("windowStart"), event
                assert event.get("windowEnd") == event.get("windowStart") + 1000, event
                assert event.get("eventType") == "WINDOW_CLOSE", event
                assert event.get("triggerType") == "Interval", event
                trigger_id = event.get("triggerId")
                assert isinstance(trigger_id, str) and trigger_id, event
                assert trigger_id not in trigger_ids, event
                trigger_ids.add(trigger_id)
                observed.append((row.get("ts"), gid, row.get("total")))

            assert sorted(observed, key=lambda row: (row[1], row[0])) == result_rows
        finally:
            tdStream.dropAllStreamsAndDbs()
            notify_server.shutdown()
            notify_server.close_clients()
            notify_server.server_close()
            notify_thread.join(timeout=5)

    def _prepare_tables(self):
        tdSql.execute(f"create database {self.DB} vgroups 2")
        tdSql.execute(
            f"create stable {self.DB}.trig (ts timestamp, v int) tags (gid int)"
        )
        tdSql.execute(
            f"create stable {self.DB}.calc (ts timestamp, v int) tags (gid int)"
        )
        for gid in range(12):
            tdSql.execute(
                f"create table {self.DB}.t{gid} using {self.DB}.trig tags ({gid})"
            )

        tdSql.query(
            "select table_name,vgroup_id from information_schema.ins_tables "
            f"where db_name='{self.DB}' and stable_name='trig' "
            "order by vgroup_id,table_name"
        )
        by_vgroup = {}
        for row in range(tdSql.getRows()):
            by_vgroup.setdefault(tdSql.getData(row, 1), []).append(
                tdSql.getData(row, 0)
            )
        assert len(by_vgroup) >= 2, by_vgroup
        groups = sorted(by_vgroup.items(), key=lambda item: item[0])
        multi_table_group = next(
            (item for item in groups if len(item[1]) >= 2), None
        )
        assert multi_table_group is not None, by_vgroup
        other_group = next(
            (
                item
                for item in groups
                if item[0] != multi_table_group[0] and item[1]
            ),
            None,
        )
        assert other_group is not None, by_vgroup
        selected = [
            multi_table_group[1][0],
            other_group[1][0],
            multi_table_group[1][1],
        ]
        assert len(set(selected)) == 3, selected
        tables = [(table_name, int(table_name[1:])) for table_name in selected]

        for _, gid in tables:
            tdSql.execute(
                f"create table {self.DB}.c{gid} using {self.DB}.calc tags ({gid})"
            )
            tdSql.execute(
                f"insert into {self.DB}.c{gid} values "
                "('2025-08-24 01:00:00.100',1) "
                "('2025-08-24 01:00:00.600',2) "
                "('2025-08-24 01:00:01.200',3) "
                "('2025-08-24 01:00:02.100',4)"
            )
        return tables

    def _write_trigger_rows(self, tables):
        values = (
            "('2025-08-24 01:00:00.000',1) "
            "('2025-08-24 01:00:01.000',2) "
            "('2025-08-24 01:00:02.000',3) "
            "('2025-08-24 01:00:03.000',4)"
        )
        tdSql.execute(
            "insert into "
            + " ".join(
                f"{self.DB}.{table_name} values {values}"
                for table_name, _ in tables
            )
        )

    def _wait_rows(self, sql, expected):
        tdSql.checkResultsByFunc(
            sql=sql,
            func=lambda: tdSql.getRows() == expected,
            retry=self.RETRY,
        )

    def _wait_events(self, expected):
        path = os.path.join(NOTIFY_RESULT_DIR, f"{NOTIFY_PATH}.log")
        latest = []
        stable = 0
        for _ in range(self.RETRY):
            latest = self._load_events(path)
            if len(latest) == expected:
                stable += 1
                if stable >= 2:
                    return latest
            else:
                stable = 0
            time.sleep(1)
        raise AssertionError(f"expected {expected} notifications, got {latest}")

    @staticmethod
    def _load_events(path):
        if not os.path.exists(path):
            return []
        events = []
        with open(path, "r", encoding="utf-8") as log_file:
            for line in log_file:
                payload = json.loads(line)
                for stream in payload.get("streams") or []:
                    if stream.get("streamName") == f"{TestExternalWindowMultiGroupNotify.DB}.s_notify":
                        events.extend(stream.get("events") or [])
        return events

    @staticmethod
    def _query_rows(sql):
        tdSql.query(sql)
        rows = []
        for row in range(tdSql.getRows()):
            values = [tdSql.getData(row, col) for col in range(tdSql.getCols())]
            if hasattr(values[0], "timestamp"):
                values[0] = int(values[0].timestamp() * 1000)
            rows.append(tuple(values))
        return rows
