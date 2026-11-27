# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from contextlib import contextmanager
import ctypes
import hashlib
import importlib.util
import json
import logging
import math
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import traceback


if not __debug__:
    raise RuntimeError("JOIN runtime cases require Python assertions")


TEST_ROOT = Path(__file__).resolve().parents[1]
CASE = TEST_ROOT / "cases/14-JoinQueries/test_join_parallel.py"
HELPER = TEST_ROOT / "new_test_framework/utils/join_runtime_logs.py"


class SqlAdapter:
    def __init__(self, cursor):
        self.cursor = cursor
        self.queryResult = []

    def execute(self, sql):
        return self.cursor.execute(sql)

    def query(self, sql):
        self.cursor.execute(sql)
        self.queryResult = self.cursor.fetchall()
        return len(self.queryResult)


def dump(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def sha(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1048576), b""):
            value.update(block)
    return value.hexdigest()


def runtime_layout(build_dir=None):
    source = build_dir or os.getenv("TAOS_BIN_PATH") or os.getenv("BUILD_DIR")
    if not source:
        # run_case.sh puts the CI build library directory first.
        lib_path = os.getenv("LD_LIBRARY_PATH", "").split(os.pathsep)[0]
        if lib_path:
            source = Path(lib_path).parent
    if not source:
        raise ValueError("Set TAOS_BIN_PATH or BUILD_DIR")
    root = Path(source).expanduser().resolve()
    if root.is_file():
        root = root.parent
    binary = "taosd.exe" if os.name == "nt" else "taosd"
    libraries = (("taos.dll", "taosnative.dll") if os.name == "nt" else
                 ("libtaos.dylib", "libtaosnative.dylib")
                 if sys.platform == "darwin" else
                 ("libtaos.so", "libtaosnative.so"))
    for base in (root, root / "build", root.parent):
        server = base / "bin" / binary
        lib_dir = base / "lib"
        if server.is_file() and all((lib_dir / n).is_file() for n in libraries):
            return server, lib_dir, [lib_dir / n for n in libraries]
    raise ValueError(f"No matching server/client build under {root}")


def verify_loaded_libraries(libraries, output):
    expected = {str(Path(path).resolve()) for path in libraries}
    if sys.platform.startswith("linux"):
        maps = Path("/proc/self/maps").read_text()
        (output / "client-maps.txt").write_text(maps)
        actual = {line.split()[-1] for line in maps.splitlines()
                  if len(line.split()) >= 6 and
                  line.split()[-1].startswith("/")}
    elif sys.platform == "darwin":
        dyld = ctypes.CDLL(None)
        dyld._dyld_image_count.restype = ctypes.c_uint32
        dyld._dyld_get_image_name.argtypes = [ctypes.c_uint32]
        dyld._dyld_get_image_name.restype = ctypes.c_char_p
        actual = {os.fsdecode(dyld._dyld_get_image_name(index))
                  for index in range(dyld._dyld_image_count())}
    else:
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.GetModuleHandleW.argtypes = [ctypes.c_wchar_p]
        kernel.GetModuleHandleW.restype = ctypes.c_void_p
        kernel.GetModuleFileNameW.argtypes = [ctypes.c_void_p,
                                             ctypes.c_wchar_p,
                                             ctypes.c_uint32]
        actual = set()
        for path in libraries:
            handle = kernel.GetModuleHandleW(Path(path).name)
            assert handle, f"Client library not loaded: {path}"
            name = ctypes.create_unicode_buffer(32768)
            assert kernel.GetModuleFileNameW(handle, name, len(name))
            actual.add(name.value)
    actual = {str(Path(path).resolve()) for path in actual}
    assert expected <= actual, f"Client libraries differ from build: {expected}"


def requested_cases(case):
    cases = ("JP-08", "JP-10", "JP-10-inflight", "JP-10-match",
             "JP-10-timeout", "JP-10-failure", "JP-10-retry", "JP-10-idle")
    assert case in (*cases, "all"), case
    return cases if case == "all" else (case,)


def requires_sessions(case):
    return any(name.startswith("JP-10") for name in requested_cases(case))


def require_fetch_gate(server):
    if b"fetch test barrier held" not in server.read_bytes():
        raise ValueError("JP-10-inflight requires a BUILD_TEST=ON server")


def require_match_gate(server):
    if b"temporal match test %s" not in server.read_bytes():
        raise ValueError("JP-10-match requires a BUILD_TEST=ON server")


def require_timeout_gate(server):
    if b"query timeout test %s" not in server.read_bytes():
        raise ValueError("JP-10-timeout requires a BUILD_TEST=ON server")


def require_failure_gate(server):
    if b"join failure test %s" not in server.read_bytes():
        raise ValueError("JP-10-failure requires a BUILD_TEST=ON server")


def require_retry_gate(server):
    if b"fetch retry test held" not in server.read_bytes():
        raise ValueError("JP-10-retry requires a BUILD_TEST=ON server")


def require_idle_checker(server):
    binary = server.read_bytes()
    if any(marker not in binary for marker in (
            b"TD_JOIN_TEST_IDLE_GATE_DIR", b"idle timeout test expired")):
        raise ValueError("JP-10-idle requires a BUILD_TEST=ON server")


def validate_runtime_evidence(evidence, rows):
    assert isinstance(evidence, list) and len(evidence) == 4, evidence
    assert all(isinstance(entry, dict) for entry in evidence), evidence
    assert {entry.get("kind") for entry in evidence} == {
        "hash", "merge", "asof", "window"}, evidence
    for entry in evidence:
        assert entry.get("input_rows") == rows, entry
        expected = rows * (8 if entry["kind"] == "hash" else 1)
        assert entry.get("rows") == expected, entry
        assert isinstance(entry.get("paused"), list) and entry["paused"]
        for phase in ("resumed", "started", "produced", "completed"):
            assert isinstance(entry.get(phase), dict) and entry[phase], phase
        if entry["kind"] == "hash":
            producers = entry.get("producers")
            assert isinstance(producers, list) and len(producers) == 2
            assert all(isinstance(proof, dict) and proof for proof in producers)


def task_key(value):
    assert isinstance(value, (list, tuple)) and len(value) == 5, value
    assert all(type(part) is int for part in value), value
    return tuple(value)


def evidence_event(event, key):
    assert isinstance(event, dict), event
    assert task_key(event.get("key")) == key, event
    assert isinstance(event.get("path"), str) and event["path"], event
    assert type(event.get("offset")) is int and event["offset"] >= 0, event
    assert isinstance(event.get("line"), str), event
    return event


def session_identity(event, text):
    assert text in event["line"], event
    match = re.search(r"\bsession:(0x[\da-fA-F]+),\s+"
                      r"job:(0x[\da-fA-F]+)(?:,|\s|$)", event["line"])
    assert match is not None, event
    identity = tuple(int(value, 16) for value in match.groups())
    assert all(identity), event
    return identity


def lifecycle_field(event, name):
    match = re.search(r"\b" + name + r":(-?\d+)(?:,|\s|$)", event["line"])
    assert match is not None, event
    return int(match.group(1))


def exchange_event(event, key, marker, exchange_id, block_id):
    event = evidence_event(event, key)
    identity = re.search(r"\bQID:(0x[\da-fA-F]+),\s+"
                         r"TID:(0x[\da-fA-F]+)\s+", event["line"])
    assert identity is not None, event
    assert tuple(int(part, 16) for part in identity.groups()) == (
        key[0], key[3]), event
    assert marker + "," in event["line"], event
    assert lifecycle_field(event, "exchangeId") == exchange_id, event
    assert lifecycle_field(event, "blockId") == block_id, event
    return event


def validate_exchange_cleanup(task, root, boundary, exchange_ids):
    key = task_key(task["key"])
    planned = task.get("planned_exchange_blocks")
    assert isinstance(planned, list), task
    assert all(type(block) is int and block >= 0 for block in planned), task
    assert len(set(planned)) == len(planned), task
    exchanges = task.get("exchanges")
    assert isinstance(exchanges, list) and len(exchanges) == len(planned), task
    blocks = set()
    path = task["registered"]["path"]
    for exchange in exchanges:
        assert isinstance(exchange, dict), exchange
        block_id, exchange_id = (exchange.get(name) for name in (
            "block_id", "exchange_id"))
        assert type(block_id) is int and block_id in planned, exchange
        assert block_id not in blocks, exchange
        blocks.add(block_id)
        assert type(exchange_id) is int and exchange_id > 0, exchange
        identity = path, exchange_id
        assert identity not in exchange_ids, exchange
        exchange_ids.add(identity)
        created = exchange_event(exchange.get("created"), key,
                                 "exchange lifecycle created", exchange_id,
                                 block_id)
        finalized = exchange_event(exchange.get("finalized"), key,
                                   "exchange resources released", exchange_id,
                                   block_id)
        assert created["path"] == finalized["path"] == path, exchange
        assert created["offset"] < boundary[path], exchange
        assert created["offset"] < finalized["offset"], exchange
        assert lifecycle_field(finalized, "code") == 0, exchange
        if key == root:
            assert finalized["offset"] >= boundary[path], exchange
        requests = exchange.get("requests")
        assert isinstance(requests, list), exchange
        count = exchange.get("fetch_count")
        assert type(count) is int and count >= 0, exchange
        assert lifecycle_field(finalized, "fetches") == count, exchange
        assert len(requests) == count, exchange
        fetch_ids = set()
        for request in requests:
            assert isinstance(request, dict), request
            fields = {name: request.get(name) for name in (
                "fetch_id", "source_index", "seq_id")}
            assert all(type(value) is int for value in fields.values()), request
            assert fields["fetch_id"] > 0 and fields["source_index"] >= 0
            assert fields["seq_id"] >= 0, request
            assert fields["fetch_id"] not in fetch_ids, request
            fetch_ids.add(fields["fetch_id"])
            events = []
            for name in ("registered", "released"):
                event = exchange_event(request.get(name), key,
                                       "fetch context " + name, exchange_id,
                                       block_id)
                assert event["path"] == path, request
                for field, label in (("fetch_id", "fetchId"),
                                     ("source_index", "sourceIndex"),
                                     ("seq_id", "seqId")):
                    assert lifecycle_field(event, label) == fields[field]
                events.append(event)
            registered, released = events
            assert (created["offset"] < registered["offset"] <
                    finalized["offset"]), request
            # The wrapper may drop its Exchange reference before freeing itself.
            assert registered["offset"] < released["offset"], request
        assert all(fetch_id <= count for fetch_id in fetch_ids), exchange
    assert blocks == set(planned), task


def validate_task_cleanup(entry, session_mode, paused=True):
    planned = entry.get("planned_task_keys")
    assert isinstance(planned, (list, tuple)) and len(planned) >= 3, entry
    planned_keys = {task_key(key) for key in planned}
    assert len(planned_keys) == len(planned), entry
    assert all(key[0] == entry["query_id"] for key in planned_keys), entry
    native_keys = {(key[0], key[3]) for key in planned_keys}
    assert len(native_keys) == len(planned_keys), entry
    root = task_key(entry.get("root_key"))
    assert root in planned_keys, entry
    boundary = entry.get("cancel_boundary")
    assert isinstance(boundary, dict) and boundary, entry
    for path, offset in boundary.items():
        assert isinstance(path, str) and path, entry
        assert type(offset) is int and offset >= 0, entry
    tasks = entry.get("tasks")
    assert isinstance(tasks, list) and len(tasks) == len(planned_keys), entry
    keys, root_events = set(), None
    exchange_ids = set()
    for task in tasks:
        assert isinstance(task, dict), task
        key = task_key(task.get("key"))
        assert key in planned_keys and key not in keys, task
        keys.add(key)
        registered = evidence_event(task.get("registered"), key)
        dropped = evidence_event(task.get("dropped"), key)
        path = registered["path"]
        assert path == dropped["path"] and path in boundary, task
        assert registered["offset"] < dropped["offset"], task
        assert registered["offset"] < boundary[path], task
        assert "task preprocess succeed," in registered["line"], task
        assert dropped["line"].endswith(" task ctx dropped"), task
        if session_mode == "enabled":
            initialized = evidence_event(task.get("session_initialized"), key)
            destroyed = evidence_event(task.get("session"), key)
            assert initialized["path"] == destroyed["path"] == path, task
            assert (initialized["offset"] < destroyed["offset"] <
                    dropped["offset"]), task
            assert registered["offset"] < destroyed["offset"], task
            assert initialized["offset"] < boundary[path], task
            assert session_identity(initialized, "session initialized,") == (
                session_identity(destroyed, "task session destroyed,")), task
        else:
            assert "session_initialized" not in task, task
            assert "session" not in task, task
        validate_exchange_cleanup(task, root, boundary, exchange_ids)
        if key == root:
            root_events = registered, dropped
    assert keys == planned_keys, entry
    registered, dropped = root_events
    path = registered["path"]
    if paused:
        events = entry.get("paused")
        assert isinstance(events, list) and len(events) == 2, entry
        full, post = (evidence_event(event, root) for event in events)
        assert full["path"] == post["path"] == path, entry
        assert "data put into sink," in full["line"], entry
        assert re.search(r"continueExecTask:0(?:,|\s|$)", full["line"]), entry
        assert re.search(r"end to handle event at phase POST_(?:CQUERY|QUERY), "
                         r"code:0\s", post["line"]), entry
        assert (registered["offset"] < full["offset"] < post["offset"] <
                boundary[path] <= dropped["offset"]), entry
    else:
        assert entry.get("paused") == [], entry
        assert registered["offset"] < boundary[path] <= dropped["offset"]
    if session_mode == "disabled":
        assert not entry.get("job_removed"), entry


def validate_lifecycle_evidence(evidence, rows, session_mode):
    assert isinstance(evidence, list) and len(evidence) == 8, evidence
    assert all(isinstance(entry, dict) for entry in evidence), evidence
    expected = {(kind, action)
                for kind in ("hash", "merge", "asof", "window")
                for action in ("stop", "close")}
    assert {(entry.get("kind"), entry.get("action"))
            for entry in evidence} == expected, evidence
    query_ids = set()
    for entry in evidence:
        query_id = entry.get("query_id")
        followup_id = entry.get("followup_query_id")
        assert isinstance(query_id, int) and query_id > 0, entry
        assert isinstance(followup_id, int) and followup_id > 0, entry
        assert query_id != followup_id, entry
        assert not {query_id, followup_id} & query_ids, entry
        query_ids.update((query_id, followup_id))
        assert entry.get("input_rows") == rows, entry
        expected_rows = rows * (8 if entry["kind"] == "hash" else 1)
        assert entry.get("followup_rows") == expected_rows, entry
        if entry["action"] == "stop":
            assert entry.get("cancel_code") in (0x020f, 0x022d), entry
        assert entry.get("session_mode") == session_mode, entry
        validate_task_cleanup(entry, session_mode)


def validate_held_request(entry):
    root = task_key(entry["root_key"])
    held = entry.get("held_request")
    assert isinstance(held, dict) and task_key(held.get("key")) == root, entry
    task, = [task for task in entry["tasks"] if task_key(task["key"]) == root]
    exchange, = [exchange for exchange in task["exchanges"]
                 if exchange["exchange_id"] == held.get("exchange_id") and
                 exchange["block_id"] == held.get("block_id")]
    request, = [request for request in exchange["requests"]
                if request["fetch_id"] == held.get("fetch_id")]
    for name in ("source_index", "seq_id", "registered", "released"):
        assert held.get(name) == request[name], held
    boundary = entry["cancel_boundary"]
    released = entry.get("release_boundary")
    assert isinstance(released, dict) and set(released) == set(boundary)
    for path, offset in released.items():
        assert type(offset) is int and offset >= boundary[path], entry
    events = []
    for name, marker in (("held", "fetch test barrier held"),
                         ("resumed", "fetch test barrier resumed"),
                         ("discarded", "fetch callback discarded")):
        event = exchange_event(held.get(name), root, marker,
                               exchange["exchange_id"], exchange["block_id"])
        assert event["path"] == exchange["created"]["path"], held
        for field, label in (("fetch_id", "fetchId"),
                             ("source_index", "sourceIndex"),
                             ("seq_id", "seqId")):
            assert lifecycle_field(event, label) == request[field], held
        events.append(event)
    paused, resumed, discarded = events
    path = paused["path"]
    assert (request["registered"]["offset"] < paused["offset"] <
            boundary[path] <= exchange["finalized"]["offset"] <
            released[path] <= resumed["offset"] < discarded["offset"] <
            request["released"]["offset"]), held
    assert task["dropped"]["offset"] < released[path], entry
    assert all(ex["finalized"]["offset"] < released[path]
               for ex in task["exchanges"]), entry


def validate_kill_mapping(entry):
    mapping = entry.get("kill_mapping")
    assert isinstance(mapping, dict), entry
    request, connection = mapping.get("request"), mapping.get("connection")
    for event in (request, connection):
        assert isinstance(event, dict), mapping
        assert isinstance(event.get("path"), str) and event["path"], mapping
        assert type(event.get("offset")) is int and event["offset"] >= 0
        assert isinstance(event.get("line"), str), mapping
    assert request["path"] == connection["path"], mapping
    assert connection["offset"] < request["offset"], mapping
    registered = re.search(
        r"\breq:(0x[\da-fA-F]+), create request from conn:"
        r"(0x[\da-fA-F]+),.*\bQID:(0x[\da-fA-F]+)(?:,|\s|$)",
        request["line"])
    opened = re.search(
        r"\bconn:(0x[\da-fA-F]+), connection is opening, connId:(\d+),",
        connection["line"])
    assert registered is not None and opened is not None, mapping
    rid, local, query = (int(value, 16) for value in registered.groups())
    server = int(opened[2])
    assert rid > 0 and server > 0 and local == int(opened[1], 16), mapping
    assert query == entry["query_id"], mapping
    assert mapping.get("kill_id") == entry["kill_id"] == f"{server:x}:{rid:x}"


def validate_inflight_evidence(evidence, rows, session_mode):
    kinds = {"hash", "merge", "asof", "window"}
    assert isinstance(evidence, list) and len(evidence) == len(kinds), evidence
    seen, query_ids = set(), set()
    for entry in evidence:
        assert isinstance(entry, dict), entry
        kind = entry.get("kind")
        assert isinstance(kind, str) and kind in kinds and kind not in seen
        seen.add(kind)
        assert entry.get("action") == "kill", entry
        assert entry.get("cancel_mode") == "inflight", entry
        assert entry.get("result_retained") is True, entry
        assert entry.get("cancel_code") in (0x020f, 0x022d), entry
        assert entry.get("session_mode") == session_mode, entry
        assert re.fullmatch(r"[\da-fA-F]+:[\da-fA-F]+",
                            entry.get("kill_id", "")), entry
        query_id = entry.get("query_id")
        followup = entry.get("followup_query_id")
        assert type(query_id) is int and query_id > 0, entry
        assert type(followup) is int and followup > 0 and followup != query_id
        assert not {query_id, followup} & query_ids, entry
        query_ids.update((query_id, followup))
        assert entry.get("input_rows") == rows, entry
        expected_rows = rows * (8 if kind == "hash" else 1)
        assert entry.get("followup_rows") == expected_rows, entry
        validate_kill_mapping(entry)
        validate_task_cleanup(entry, session_mode, paused=False)
        validate_held_request(entry)
    assert seen == kinds, evidence


def match_event(event, key, kind, block_id, phase):
    event = evidence_event(event, key)
    identity = re.search(r"\bQID:(0x[\da-fA-F]+),\s+"
                         r"TID:(0x[\da-fA-F]+) temporal match test "
                         + re.escape(phase) + r",", event["line"])
    assert identity is not None, event
    assert tuple(int(part, 16) for part in identity.groups()) == (
        key[0], key[3]), event
    assert re.search(r"\bkind:" + re.escape(kind) + r",", event["line"])
    assert lifecycle_field(event, "blockId") == block_id, event
    names = ("asofOp", "rows", "totalRows", "probeBegin", "probeRead",
             "probeEnd", "cacheRows", "cacheOutRows", "cacheGroups",
             "cacheRead", "candidates", "grpRemains", "done")
    return event, {name: lifecycle_field(event, name) for name in names}


def validate_match_state(entry):
    root = task_key(entry["root_key"])
    state = entry.get("match_state")
    assert isinstance(state, dict) and task_key(state.get("key")) == root
    kind, block_id = state.get("kind"), state.get("block_id")
    assert kind == entry["kind"], state
    assert type(block_id) is int and block_id >= 0, state
    task, = [task for task in entry["tasks"] if task_key(task["key"]) == root]
    path = task["registered"]["path"]
    events, fields = {}, {}
    phases = ["held", "resumed", "destroyed"]
    if entry["action"] == "resume":
        phases.append("progress")
    for phase in phases:
        events[phase], fields[phase] = match_event(
            state.get(phase), root, kind, block_id, phase)
        assert events[phase]["path"] == path, state
    held = fields["held"]
    asof_op = ({"equal": 41, "strict-backward": 40, "forward": 43,
                "strict-forward": 42, "null-filter": 41}
               [entry["scenario"]] if kind == "asof" else 0)
    assert all(values["asofOp"] == asof_op for values in fields.values())
    assert held["grpRemains"] == 1 and held["done"] == 0, state
    assert 0 < held["rows"] <= held["totalRows"] < entry["expected_rows"]
    assert (0 <= held["probeBegin"] <= held["probeRead"] <=
            held["probeEnd"] < entry["input_rows"]), state
    assert all(held[name] >= 0 for name in (
        "cacheRows", "cacheOutRows", "cacheGroups", "cacheRead")), state
    assert 0 < held["candidates"] <= entry["input_rows"], state
    if kind == "asof":
        cache = "cacheRows" if asof_op in (42, 43) else "cacheOutRows"
        assert held[cache] >= held["candidates"], state
    else:
        assert held["cacheRows"] >= held["candidates"], state
        assert held["cacheGroups"] > 0, state
    assert fields["resumed"] == held, state
    boundary, released = entry["cancel_boundary"], entry.get("release_boundary")
    assert isinstance(released, dict) and set(released) == set(boundary)
    for name, offset in released.items():
        assert type(offset) is int and offset >= boundary[name], entry
    assert (task["registered"]["offset"] < events["held"]["offset"] <
            boundary[path] <= released[path] <= events["resumed"]["offset"] <
            events["destroyed"]["offset"] < task["dropped"]["offset"]), state
    assert all(exchange["finalized"]["offset"] >= released[path]
               for exchange in task["exchanges"]), entry
    if entry["action"] == "resume":
        assert (events["resumed"]["offset"] < events["progress"]["offset"] <
                events["destroyed"]["offset"]), state
        assert fields["progress"]["rows"] > 0, state
        assert fields["progress"]["totalRows"] > held["totalRows"], state
        assert fields["destroyed"]["totalRows"] == entry["expected_rows"]


def validate_match_evidence(evidence, session_mode):
    scenarios = load_module("join_match_expectations", HELPER).MATCH_SCENARIOS
    expected = {(name, kind, action): rows for name, kind, rows in scenarios
                for action in (("resume", "kill") if name == "equal"
                               else ("resume",))}
    assert isinstance(evidence, list) and len(evidence) == len(expected)
    seen, query_ids = set(), set()
    for entry in evidence:
        assert isinstance(entry, dict), entry
        scenario = entry.get("scenario")
        kind, action = entry.get("kind"), entry.get("action")
        assert isinstance(scenario, str), entry
        assert isinstance(kind, str) and isinstance(action, str), entry
        pair = scenario, kind, action
        assert pair in expected and pair not in seen, entry
        seen.add(pair)
        query, followup = entry.get("query_id"), entry.get("followup_query_id")
        assert type(query) is int and query > 0, entry
        assert type(followup) is int and followup > 0 and followup != query
        assert not {query, followup} & query_ids, entry
        query_ids.update((query, followup))
        assert entry.get("input_rows") == 128, entry
        assert entry.get("expected_rows") == expected[pair], entry
        assert entry.get("followup_rows") == entry["expected_rows"], entry
        assert entry.get("session_mode") == session_mode, entry
        assert entry.get("cancel_mode") == "match-" + action, entry
        if action == "kill":
            assert entry.get("cancel_code") in (0x020f, 0x022d), entry
            assert entry.get("result_retained") is True, entry
            validate_kill_mapping(entry)
        else:
            assert entry.get("rows") == entry["expected_rows"], entry
            assert entry.get("result_freed") is True, entry
        assert len(entry.get("planned_task_keys", [])) == 5, entry
        validate_task_cleanup(entry, session_mode, paused=False)
        assert sum(len(task["exchanges"]) for task in entry["tasks"]) == 4
        validate_match_state(entry)
    assert seen == set(expected), evidence


def rpc_client_event(event):
    assert isinstance(event, dict), event
    assert isinstance(event.get("path"), str) and event["path"], event
    assert type(event.get("offset")) is int and event["offset"] >= 0, event
    assert isinstance(event.get("line"), str), event
    return event


def validate_rpc_timeout_request(entry):
    root = task_key(entry["root_key"])
    request = entry.get("timeout_request")
    assert isinstance(request, dict), entry
    sid, conn = request.get("sid"), request.get("conn")
    assert type(sid) is int and sid > 0, request
    assert type(conn) is int and conn > 0, request
    phases = ("scheduler_sent", "transport_sent", "timer_removed",
              "job_failed", "drop_sent")
    events = [rpc_client_event(request.get(name)) for name in phases]
    sent, transport, timer, failed, dropped = events
    path = sent["path"]
    assert all(event["path"] == path for event in events), request
    assert all(left["offset"] < right["offset"]
               for left, right in zip(events, events[1:])), request
    identity = (r"\bQID:(0x[\da-fA-F]+),.*\bSID:(-?\d+), "
                r"CID:(0x[\da-fA-F]+), TID:(0x[\da-fA-F]+), EID:(\d+),")
    for event in (sent, dropped):
        match = re.search(identity, event["line"])
        assert match is not None, event
        key = (int(match[1], 16), int(match[2]), int(match[3], 16),
               int(match[4], 16), int(match[5]))
        assert key == root, event
    match = re.search(
        r"start to send (query|merge-query|fetch|merge-fetch) msg .*"
        r"\bpHandle:(0x[\da-fA-F]+|[\da-fA-F]+)(?:,|\s|$)", sent["line"])
    assert match is not None and int(match[2], 16) == sid, sent
    message = match[1]
    match = re.search(
        r"\bconn:(0x[\da-fA-F]+|[\da-fA-F]+), " + re.escape(message) +
        r" is sent to .*\bsid:(\d+), QID:(0x[\da-fA-F]+):"
        r"0x[\da-fA-F]+\s*$", transport["line"])
    assert match is not None, transport
    assert (int(match[1], 16), int(match[2]), int(match[3], 16)) == (
        conn, sid, root[0]), transport
    match = re.search(r"\bconn:(0x[\da-fA-F]+|[\da-fA-F]+), "
                      r"remove timeout msg sid:(\d+)\s*$", timer["line"])
    assert match is not None and (int(match[1], 16), int(match[2])) == (
        conn, sid), timer
    match = re.search(r"\bQID:(0x[\da-fA-F]+), SUBID:-1, SID:(\d+), "
                      r"job failed with error Conn read timeout\s*$",
                      failed["line"])
    assert match is not None and (int(match[1], 16), int(match[2])) == (
        root[0], root[1]), failed
    assert dropped["line"].endswith(
        "clear refId before send drop-task msg, code:Conn read timeout")
    boundary = entry.get("client_release_boundary")
    assert isinstance(boundary, dict) and path in boundary, entry
    assert type(boundary[path]) is int and dropped["offset"] < boundary[path]


def validate_timeout_state(entry):
    root = task_key(entry["root_key"])
    state = entry.get("timeout_state")
    assert isinstance(state, dict) and task_key(state.get("key")) == root
    assert state.get("kind") == entry["kind"], state
    assert state.get("read_timeout") == entry["read_timeout"], state
    block = state.get("block_id")
    assert type(block) is int and block >= 0, state
    task, = [task for task in entry["tasks"] if task_key(task["key"]) == root]
    path = task["registered"]["path"]
    events, snapshots = [], []
    for phase in ("held", "resumed"):
        event = evidence_event(state.get(phase), root)
        assert event["path"] == path, event
        identity = re.search(r"\bQID:(0x[\da-fA-F]+), "
                             r"TID:(0x[\da-fA-F]+) query timeout test " +
                             phase + r",", event["line"])
        assert identity is not None, event
        assert tuple(int(p, 16) for p in identity.groups()) == (
            root[0], root[3]), event
        fields = {name: lifecycle_field(event, name)
                  for name in ("blockId", "rows", "readTimeout")}
        assert fields["blockId"] == block and fields["rows"] > 0, state
        assert fields["readTimeout"] == entry["read_timeout"], state
        events.append(event)
        snapshots.append(fields)
    assert snapshots[0] == snapshots[1], state
    boundary, released = entry["cancel_boundary"], entry.get("release_boundary")
    assert isinstance(released, dict) and set(released) == set(boundary)
    for name, offset in released.items():
        assert type(offset) is int and offset >= boundary[name], entry
    assert (task["registered"]["offset"] < events[0]["offset"] <
            boundary[path] <= released[path] <= events[1]["offset"] <
            task["dropped"]["offset"]), state
    assert all(exchange["finalized"]["offset"] >= released[path]
               for exchange in task["exchanges"]), entry


def validate_timeout_evidence(evidence, rows, session_mode):
    kinds = {"hash", "merge", "asof", "window"}
    assert isinstance(evidence, list) and len(evidence) == len(kinds), evidence
    seen, query_ids = set(), set()
    for entry in evidence:
        assert isinstance(entry, dict), entry
        kind = entry.get("kind")
        assert isinstance(kind, str) and kind in kinds and kind not in seen
        seen.add(kind)
        assert entry.get("action") == "timeout", entry
        assert entry.get("cancel_mode") == "rpc-timeout", entry
        assert entry.get("cancel_code") == 0x0019, entry
        assert entry.get("read_timeout") == 64, entry
        elapsed = entry.get("elapsed_seconds")
        assert type(elapsed) in (int, float) and math.isfinite(elapsed)
        assert elapsed >= entry["read_timeout"], entry
        assert entry.get("result_retained_at_timeout") is True, entry
        assert entry.get("result_freed_before_release") is True, entry
        query, followup = entry.get("query_id"), entry.get("followup_query_id")
        assert type(query) is int and query > 0, entry
        assert type(followup) is int and followup > 0 and followup != query
        assert not {query, followup} & query_ids, entry
        query_ids.update((query, followup))
        assert entry.get("input_rows") == rows, entry
        assert entry.get("followup_rows") == rows * (8 if kind == "hash" else 1)
        assert entry.get("session_mode") == session_mode, entry
        assert len(entry.get("planned_task_keys", [])) == 5, entry
        validate_task_cleanup(entry, session_mode, paused=False)
        assert sum(len(task["exchanges"]) for task in entry["tasks"]) == 4
        validate_timeout_state(entry)
        validate_rpc_timeout_request(entry)
    assert seen == kinds, evidence


def failure_task_event(event, key, raw=False):
    event = rpc_client_event(event) if raw else evidence_event(event, key)
    identity = re.search(
        r"\bQID:(0x[\da-fA-F]+),.*?\bSID:(-?\d+),.*?"
        r"CID:(0x[\da-fA-F]+),\s+TID:(0x[\da-fA-F]+),\s+EID:(\d+)",
        event["line"])
    assert identity is not None, event
    actual = (int(identity[1], 16), int(identity[2]), int(identity[3], 16),
              int(identity[4], 16), int(identity[5]))
    assert actual == key, event
    return event


def failure_gate_event(event, key, block, phase):
    event = evidence_event(event, key)
    identity = re.search(
        r"\bQID:(0x[\da-fA-F]+),\s+TID:(0x[\da-fA-F]+) "
        r"join failure test " + re.escape(phase) + r",", event["line"])
    assert identity is not None and tuple(
        int(part, 16) for part in identity.groups()) == (key[0], key[3]), event
    fields = {name: lifecycle_field(event, name)
              for name in ("blockId", "rows", "code")}
    assert fields["blockId"] == block and fields["rows"] > 0, event
    assert fields["code"] == (-2147481806 if phase == "injected" else 0)
    return event, fields


def validate_failure_state(entry):
    root = task_key(entry["root_key"])
    state = entry.get("failure_state")
    assert isinstance(state, dict) and task_key(state.get("key")) == root
    assert (state.get("kind"), state.get("side")) == (
        entry["kind"], entry["side"]), state
    tasks = {task_key(task["key"]): task for task in entry["tasks"]}
    inputs = state.get("inputs")
    assert isinstance(inputs, list) and len(inputs) == 2, state
    input_keys, blocks = [], []
    for item in inputs:
        assert isinstance(item, dict), item
        key = task_key(item.get("key"))
        assert key in tasks and key != root and key not in input_keys, item
        assert len(tasks[key]["exchanges"]) == 1, item
        block, output = item.get("exchange_block"), item.get("producer_block")
        assert type(block) is int and block >= 0 and block not in blocks, item
        assert type(output) is int and output >= 0, item
        input_keys.append(key)
        blocks.append(block)
    assert set(blocks) == set(tasks[root]["planned_exchange_blocks"]), state
    assert len(tasks[root]["exchanges"]) == 2, state
    assert all(not task["exchanges"] for key, task in tasks.items()
               if key not in [root, *input_keys]), state
    side = 0 if entry["side"] == "left" else 1
    target, peer = input_keys[side], input_keys[1 - side]
    assert task_key(state.get("target_key")) == target, state
    assert task_key(state.get("peer_key")) == peer, state
    boundary, released = entry["cancel_boundary"], entry.get("release_boundary")
    assert isinstance(released, dict) and set(released) == set(boundary)
    for path, offset in released.items():
        assert type(offset) is int and offset >= boundary[path], entry
    gates, snapshots = {}, {}
    for name, key, block, phase in (
        ("target_held", target, inputs[side]["producer_block"], "held"),
        ("peer_held", peer, inputs[1 - side]["producer_block"], "held"),
        ("injected", target, inputs[side]["producer_block"], "injected"),
        ("peer_resumed", peer, inputs[1 - side]["producer_block"], "resumed"),
    ):
        gates[name], snapshots[name] = failure_gate_event(
            state.get(name), key, block, phase)
        assert gates[name]["path"] == tasks[key]["registered"]["path"], state
    for first, last in (("target_held", "injected"),
                         ("peer_held", "peer_resumed")):
        assert all(snapshots[first][name] == snapshots[last][name]
                   for name in ("blockId", "rows")), state
    for name, key in (("target_held", target), ("peer_held", peer)):
        event = gates[name]
        assert (tasks[key]["registered"]["offset"] < event["offset"] <
                boundary[event["path"]]), state
    failed = {}
    for name, key in (("target_exec_failed", target),
                       ("root_exec_failed", root)):
        event = failure_task_event(state.get(name), key)
        assert event["path"] == tasks[key]["registered"]["path"], event
        assert re.search(r"qExecTask failed, code:(?:0x)?80000732 - "
                         r"Executor internal error\s*$", event["line"],
                         re.IGNORECASE), event
        assert event["offset"] < tasks[key]["dropped"]["offset"], state
        failed[name] = event
    injected, resumed = gates["injected"], gates["peer_resumed"]
    path = injected["path"]
    assert (boundary[path] <= injected["offset"] <
            failed["target_exec_failed"]["offset"] < released[path]), state
    peer_path = path = resumed["path"]
    assert (released[path] <= resumed["offset"] <
            tasks[peer]["dropped"]["offset"]), state
    for task in tasks.values():
        path = task["registered"]["path"]
        assert task["dropped"]["offset"] >= boundary[path], task
        if entry["session_mode"] == "enabled":
            assert task["session"]["offset"] >= boundary[path], task
        for exchange in task["exchanges"]:
            assert exchange["finalized"]["offset"] >= boundary[path], exchange
    assert all(exchange["finalized"]["offset"] >= released[peer_path]
               for exchange in tasks[peer]["exchanges"]), state
    callback = evidence_event(state.get("root_exchange_error"), root)
    identity = re.search(
        r"\bTID:(0x[\da-fA-F]+)\s+QID:(0x[\da-fA-F]+) "
        r"exchange \S+ (\d+)th source got rsp, code:(-?\d+),",
        callback["line"])
    assert identity is not None and (int(identity[1], 16),
        int(identity[2], 16), int(identity[3]), int(identity[4])) == (
            root[3], root[0], 0, -2147481806), callback
    path = tasks[root]["registered"]["path"]
    assert callback["path"] == path and (
        boundary[path] <= callback["offset"] <
        failed["root_exec_failed"]["offset"]), state
    origin = failed["target_exec_failed"]
    if origin["path"] == path:
        assert origin["offset"] < callback["offset"], state
    job = rpc_client_event(state.get("job_failed"))
    identity = re.search(
        r"\bQID:(0x[\da-fA-F]+), SUBID:-1, SID:(\d+), "
        r"job failed with error Executor internal error\s*$", job["line"])
    assert identity is not None and (int(identity[1], 16),
        int(identity[2])) == (root[0], root[1]), job
    dropped = failure_task_event(state.get("drop_sent"), root, raw=True)
    assert "start to send drop-task msg" in dropped["line"], dropped
    assert job["path"] == dropped["path"] and (
        job["offset"] < dropped["offset"]), state
    clients = entry.get("client_release_boundary")
    assert isinstance(clients, dict) and job["path"] in clients, entry
    for name, offset in clients.items():
        assert isinstance(name, str) and name and type(offset) is int
        assert offset >= 0, entry
    assert clients[job["path"]] <= dropped["offset"], state


def validate_failure_evidence(evidence, rows, session_mode):
    expected = {(kind, side) for kind in ("hash", "merge", "asof", "window")
                for side in ("left", "right")}
    assert isinstance(evidence, list) and len(evidence) == len(expected)
    seen, queries = set(), set()
    for entry in evidence:
        assert isinstance(entry, dict), entry
        kind, side = entry.get("kind"), entry.get("side")
        assert isinstance(kind, str) and isinstance(side, str), entry
        assert (kind, side) in expected and (kind, side) not in seen, entry
        seen.add((kind, side))
        assert entry.get("action") == "failure", entry
        assert entry.get("cancel_mode") == "producer-failure", entry
        assert entry.get("error_code") == 0x0732, entry
        assert entry.get("result_retained_at_failure") is True, entry
        assert entry.get("result_released_normally") is True, entry
        query, followup = entry.get("query_id"), entry.get("followup_query_id")
        assert type(query) is int and query > 0, entry
        assert type(followup) is int and followup > 0 and followup != query
        assert not {query, followup} & queries, entry
        queries.update((query, followup))
        assert entry.get("input_rows") == rows, entry
        assert entry.get("followup_rows") == rows * (8 if kind == "hash" else 1)
        assert entry.get("session_mode") == session_mode, entry
        assert len(entry.get("planned_task_keys", [])) == 5, entry
        validate_task_cleanup(entry, session_mode, paused=False)
        assert sum(len(task["exchanges"]) for task in entry["tasks"]) == 4
        validate_failure_state(entry)
    assert seen == expected, evidence


def validate_retry_attempt(attempt, query, session_mode):
    assert isinstance(attempt, dict) and len(
        attempt.get("planned_task_keys", [])) == 5, attempt
    validate_task_cleanup(dict(attempt, query_id=query), session_mode,
                          paused=False)
    keys = {task_key(task["key"]) for task in attempt["tasks"]}
    root = task_key(attempt["root_key"])
    tasks = {task_key(task["key"]): task for task in attempt["tasks"]}
    assert len(tasks[root]["exchanges"]) == 2
    assert sum(len(task["exchanges"]) for task in tasks.values()) == 4
    bindings = attempt.get("input_bindings")
    assert isinstance(bindings, list) and len(bindings) == 4, attempt
    edges, blocks = {}, set()
    for binding in bindings:
        consumer = task_key(binding.get("consumer"))
        producer = task_key(binding.get("producer"))
        block = binding.get("block_id")
        assert consumer in keys and producer in keys and consumer != producer
        assert type(block) is int and block in tasks[consumer][
            "planned_exchange_blocks"], binding
        assert (consumer, block) not in blocks, binding
        blocks.add((consumer, block))
        edges.setdefault(consumer, []).append(producer)
    children = edges.get(root, [])
    assert len(children) == len(set(children)) == 2, attempt
    leaves = []
    for child in children:
        assert len(edges.get(child, [])) == 1, attempt
        leaves.extend(edges[child])
    assert len(set(leaves)) == 2 and not any(key in edges for key in leaves)
    assert {root, *children, *leaves} == keys, attempt
    return root, tasks


def retry_reply_event(event, root, marker, exchange):
    event = exchange_event(event, root, marker, exchange["exchange_id"],
                           exchange["block_id"])
    assert lifecycle_field(event, "fetchId") > 0, event
    assert lifecycle_field(event, "sourceIndex") == 0, event
    assert lifecycle_field(event, "seqId") == 0, event
    return event


def validate_retry_evidence(evidence, rows, session_mode):
    kinds = {"hash", "merge", "asof", "window"}
    assert isinstance(evidence, list) and len(evidence) == len(kinds)
    seen, queries = set(), set()
    for entry in evidence:
        assert isinstance(entry, dict), entry
        kind = entry.get("kind")
        assert isinstance(kind, str) and kind in kinds and kind not in seen
        seen.add(kind)
        assert entry.get("action") == "retry", entry
        assert entry.get("retry_code") == 0x0914, entry
        assert entry.get("result_retained") is True, entry
        assert entry.get("new_execution_waiting_at_old_release") is True, entry
        assert entry.get("result_released_normally") is True, entry
        query, followup = entry.get("query_id"), entry.get("followup_query_id")
        assert type(query) is int and query > 0, entry
        assert type(followup) is int and followup > 0 and followup != query
        assert not {query, followup} & queries, entry
        queries.update((query, followup))
        assert entry.get("input_rows") == rows
        expected = rows * (8 if kind == "hash" else 1)
        assert entry.get("rows") == entry.get("followup_rows") == expected
        old_root, old = validate_retry_attempt(entry.get("old_execution"),
                                               query, session_mode)
        new_root, new = validate_retry_attempt(entry.get("new_execution"),
                                               query, session_mode)
        by_id = {key[3]: key for key in old}
        assert set(by_id) == {key[3] for key in new}, entry
        for key in new:
            previous = by_id[key[3]]
            assert (key[0], key[2]) == (previous[0], previous[2])
            assert key[1] == previous[1] + 1 and key[4] == previous[4] + 1
        assert new_root[3] == old_root[3]
        instances = [{(item["created"]["path"], item["exchange_id"])
                      for task in tasks.values() for item in task["exchanges"]}
                     for tasks in (old, new)]
        assert not instances[0] & instances[1], "Exchange reused across retry"
        reply = entry.get("reply")
        assert isinstance(reply, dict), entry
        phases = {}
        for names, root, tasks in (
            (("held", "injected", "resumed", "discarded", "released"),
             old_root, old),
            (("next_held", "next_resumed"), new_root, new),
        ):
            initial = reply.get(names[0])
            assert isinstance(initial, dict)
            exchange_id = lifecycle_field(initial, "exchangeId")
            exchange, = [item for item in tasks[root]["exchanges"]
                          if item["exchange_id"] == exchange_id]
            markers = {"held": "fetch retry test held",
                       "injected": "fetch retry test injected",
                       "resumed": "fetch retry test resumed",
                       "discarded": "fetch callback discarded",
                       "released": "fetch context released",
                       "next_held": "fetch retry next held",
                       "next_resumed": "fetch retry next resumed"}
            for name in names:
                phases[name] = retry_reply_event(reply.get(name), root,
                                                 markers[name], exchange)
                assert lifecycle_field(phases[name], "fetchId") == (
                    lifecycle_field(initial, "fetchId"))
            request, = [item for item in exchange["requests"]
                         if item["fetch_id"] == lifecycle_field(
                             initial, "fetchId")]
            assert request["registered"]["offset"] < initial["offset"]
            if root == old_root:
                assert phases["released"] == request["released"]
            else:
                next_registered = retry_reply_event(
                    reply.get("next_registered"), new_root,
                    "fetch context registered", exchange)
                assert all(next_registered[name] == request["registered"][name]
                           for name in ("path", "offset", "line", "key"))
                assert phases["next_resumed"]["offset"] < (
                    exchange["finalized"]["offset"])
        path = phases["held"]["path"]
        assert all(event["path"] == path for event in phases.values())
        old_boundary = entry["old_execution"]["cancel_boundary"][path]
        new_boundary = entry["new_execution"]["cancel_boundary"][path]
        release = entry.get("release_boundary")
        assert isinstance(release, dict) and type(release.get(path)) is int
        registered = next_registered
        assert registered["path"] == path
        next_release = entry.get("next_release_boundary")
        assert isinstance(next_release, dict) and type(
            next_release.get(path)) is int
        assert (phases["held"]["offset"] < old_boundary <=
                phases["injected"]["offset"] < registered["offset"] <
                new_boundary <= release[path] <=
                phases["resumed"]["offset"] < phases["discarded"]["offset"] <
                phases["released"]["offset"] < next_release[path] <=
                phases["next_resumed"]["offset"])
        assert (registered["offset"] < phases["next_held"]["offset"] <
                next_release[path])
        assert all(task["dropped"]["offset"] < release[path]
                   for task in old.values())
        assert all(item["finalized"]["offset"] < release[path]
                   for task in old.values() for item in task["exchanges"])
        failed = failure_task_event(entry.get("root_failed"), old_root)
        assert re.search(r"qExecTask failed, code:80000914 - "
                         r"Sync leader is restoring$",
                         failed["line"]), failed
        callback = evidence_event(entry.get("exchange_error"), old_root)
        identity = re.search(
            r"TID:(0x[\da-fA-F]+) QID:(0x[\da-fA-F]+) exchange \S+ "
            r"0th source got rsp, code:-2147481324,", callback["line"])
        assert identity is not None and (int(identity[1], 16),
            int(identity[2], 16)) == (old_root[3], old_root[0]), callback
        assert callback["path"] == failed["path"] == path
        assert (phases["injected"]["offset"] < callback["offset"] <
                failed["offset"] < old[old_root]["dropped"]["offset"])
        restart = failure_task_event(entry.get("restart"), old_root, raw=True)
        assert "start to restart all tasks by error:" in restart["line"]
        assert "Sync leader is restoring" in (
            restart["line"]), restart
        updated = rpc_client_event(entry.get("series_updated"))
        assert restart["path"] == updated["path"] and (
            restart["offset"] < updated["offset"])
        assert re.search(r"update job sId:" + str(new_root[1]) + r",",
                         updated["line"]), updated
        dropped = failure_task_event(entry.get("drop_sent"), new_root,
                                      raw=True)
        assert "start to send drop-task msg" in dropped["line"], dropped
        free = entry.get("result_free_boundary")
        assert isinstance(free, dict) and type(free.get(dropped["path"])) is int
        assert restart["path"] == dropped["path"]
        assert updated["offset"] < free[dropped["path"]] <= dropped["offset"]
    assert seen == kinds


def validate_idle_evidence(evidence, rows, session_mode):
    kinds = {"hash", "merge", "asof", "window"}
    assert isinstance(evidence, list) and len(evidence) == 4, evidence
    seen, queries = set(), set()
    for entry in evidence:
        kind = entry.get("kind")
        assert isinstance(kind, str) and kind in kinds and kind not in seen
        seen.add(kind)
        assert entry.get("action") == "idle", entry
        assert entry.get("idle_timeout") == 60, entry
        assert entry.get("idle_scan_interval") == 1, entry
        assert entry.get("expiry_code") == 0x073b, entry
        assert entry.get("expired_result_code") == 0x0018, entry
        for name in ("result_retained_at_expiry",
                     "connection_retained_at_expiry",
                     "result_released_after_cleanup", "idle_timeout_restored"):
            assert entry.get(name) is True, entry
        query, followup = entry.get("query_id"), entry.get("followup_query_id")
        assert type(query) is int and query > 0
        assert type(followup) is int and followup > 0 and query != followup
        assert not {query, followup} & queries
        queries.update((query, followup))
        assert entry.get("input_rows") == rows
        assert entry.get("followup_rows") == rows * (8 if kind == "hash" else 1)
        elapsed = entry.get("elapsed_seconds")
        assert type(elapsed) in (int, float) and math.isfinite(elapsed)
        assert elapsed >= 0
        wall_start = entry.get("wall_started_seconds")
        wall_end = entry.get("wall_finished_seconds")
        for value in (wall_start, wall_end):
            assert type(value) in (int, float) and math.isfinite(value)
        assert wall_end - wall_start >= 59, (
            "Second-resolution expiry must use real wall-clock time")
        assert entry.get("session_mode") == session_mode
        assert len(entry.get("planned_task_keys", [])) == 5
        validate_task_cleanup(entry, session_mode)
        tasks = {task_key(task["key"]): task for task in entry["tasks"]}
        assert sum(len(task["exchanges"]) for task in tasks.values()) == 4
        expired = entry.get("idle_expirations")
        assert isinstance(expired, list) and len(expired) == 5
        keys = set()
        for event in expired:
            key = task_key(event.get("key"))
            assert key in tasks and key not in keys
            keys.add(key)
            event = failure_task_event(event, key)
            assert re.search(r"idle timeout test expired, lastAckTs:\d+, "
                             r"nowTs:\d+, timeout:60, code:8000073b - "
                             r"Timeout for long time no fetch$", event["line"])
            acknowledged = lifecycle_field(event, "lastAckTs")
            now = lifecycle_field(event, "nowTs")
            assert acknowledged > 0 and now - acknowledged >= 60
            assert wall_start < acknowledged + 1 and wall_end >= now
            task = tasks[key]
            path = task["registered"]["path"]
            assert event["path"] == path
            assert (entry["cancel_boundary"][path] <= event["offset"] <
                    task["dropped"]["offset"])
        root = task_key(entry["root_key"])
        response = failure_task_event(entry.get("root_query_response"), root)
        assert "query msg rsped," in response["line"]
        assert "code:0 - success" in response["line"]
        assert response["path"] == entry["paused"][1]["path"]
        assert response["offset"] < entry["paused"][1]["offset"]
        quiet = entry.get("quiet_boundary")
        assert isinstance(quiet, dict) and quiet
        assert all(isinstance(path, str) and path and type(value) is int and
                   value >= 0 for path, value in quiet.items())
    assert seen == kinds


def validate_result(result, rows=8192, case="all", session_mode=None):
    assert isinstance(result, dict), result
    assert result.get("status") == "passed", result
    assert result.get("case") == case, result
    assert not result.get("skipped"), "Runtime cases must execute, not skip"
    cases = requested_cases(case)
    if "JP-08" in cases:
        validate_runtime_evidence(result.get("runtime_evidence"), rows)
    if requires_sessions(case):
        mode = result.get("session_mode")
        assert mode in ("enabled", "disabled"), result
        assert session_mode is None or mode == session_mode, result
    if "JP-10" in cases:
        validate_lifecycle_evidence(result.get("lifecycle_evidence"), rows,
                                    mode)
    if "JP-10-inflight" in cases:
        validate_inflight_evidence(result.get("inflight_evidence"), rows, mode)
    if "JP-10-match" in cases:
        validate_match_evidence(result.get("match_evidence"), mode)
    if "JP-10-timeout" in cases:
        validate_timeout_evidence(result.get("timeout_evidence"), rows, mode)
    if "JP-10-failure" in cases:
        validate_failure_evidence(result.get("failure_evidence"), rows, mode)
    if "JP-10-retry" in cases:
        validate_retry_evidence(result.get("retry_evidence"), rows, mode)
    if "JP-10-idle" in cases:
        validate_idle_evidence(result.get("idle_evidence"), rows, mode)


def pool_mode(line):
    if "memory pool initialized," in line:
        return "enabled"
    if re.search(r"memory pool disabled (?:cause of configured disabled|"
                  r"since not enough system (?:total memory|"
                  r"available memory(?: after reserved)?),)", line):
        return "disabled"
    return None


def validate_session_mode(mode, events):
    assert mode in ("enabled", "disabled"), mode
    assert isinstance(events, list) and events, events
    for event in events:
        assert isinstance(event, dict), event
        assert isinstance(event.get("path"), str) and event["path"], event
        assert type(event.get("offset")) is int and event["offset"] >= 0, event
        assert pool_mode(event.get("line", "")) == mode, event
    return mode


def detect_session_mode(paths):
    events = []
    for path in paths:
        with path.open("rb") as stream:
            while True:
                offset = stream.tell()
                raw = stream.readline()
                if not raw:
                    break
                line = raw.decode(errors="replace").rstrip()
                if pool_mode(line) is not None:
                    events.append({"path": str(path), "offset": offset,
                                   "line": line})
    modes = {pool_mode(event["line"]) for event in events}
    assert len(modes) == 1, "Unknown or conflicting server memory-pool state"
    mode = validate_session_mode(modes.pop(), events)
    return mode, events


def stop_owned_process(process, timeout=30):
    forced = False
    if process.poll() is None:
        if os.name == "nt":
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            process.terminate()
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            forced = True
            process.kill()
            process.wait(timeout=5)
    else:
        process.wait()
    return {"pid": process.pid, "exit_code": process.returncode,
            "forced_kill": forced}


def client_command(script, function, *arguments):
    # The native connector initializes process-global configuration once.
    return [sys.executable, "-c",
            "import runpy, sys; from pathlib import Path; "
            "runpy.run_path(sys.argv[1])[sys.argv[2]]("
            "*sys.argv[3:-1], Path(sys.argv[-1]))",
            str(script), function, *arguments]


def run_client(script, directory, env, mode, timeout):
    command = client_command(script, "client", mode, str(directory))
    path = directory / f"client-{mode}.log"
    with path.open("a") as log:
        try:
            return subprocess.run(command, env=env, stdout=log,
                                  stderr=subprocess.STDOUT, timeout=timeout,
                                  check=True)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            if mode == "case":
                print_client_tail(path)
            raise


def print_client_tail(path):
    with path.open("rb") as log:
        log.seek(max(0, path.stat().st_size - 4096))
        print(log.read().decode(errors="replace"), end="", flush=True)


def load_module(name, source):
    spec = importlib.util.spec_from_file_location(name, source)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def client(mode, directory):
    manifest = json.loads((directory / "manifest.json").read_text())
    import taos

    connection = taos.connect(host="localhost", port=manifest["port"],
                              user="root", password="taosdata",
                              config=str(directory / "client-config"))
    cursor = None
    case = None
    selected = manifest["case"]
    result = {"status": "failed", "case": selected}
    try:
        cursor = connection.cursor()
        verify_loaded_libraries(manifest["libraries"], directory)
        if mode == "ready":
            cursor.execute("select server_version()")
            cursor.fetchall()
            return
        session_mode = None
        if requires_sessions(selected):
            session_mode = validate_session_mode(
                manifest.get("session_mode"),
                manifest.get("session_mode_evidence"))
            assert os.getenv("JOIN_PARALLEL_SESSION_MODE") == session_mode
            result["session_mode"] = session_mode
        load_module("new_test_framework.utils.join_runtime_logs",
                    directory / "sources/join_runtime_logs.py")
        module = load_module("join_parallel_case",
                              directory / "sources/test_join_parallel.py")
        case = module.TestJoinParallel()
        case.sql = SqlAdapter(cursor)
        case.log = logging.getLogger("join-parallel-runtime")
        if "JP-10-inflight" in requested_cases(selected):
            gate = Path(manifest["fetch_gate_dir"]).resolve()
            assert gate.parent == Path(manifest["data_root"]).resolve()
            assert gate.is_dir()
            assert os.getenv("JOIN_PARALLEL_FETCH_GATE_DIR") == str(gate)
            case.fetch_gate_dir = gate
        if "JP-10-match" in requested_cases(selected):
            gate = Path(manifest["match_gate_dir"]).resolve()
            assert gate.parent == Path(manifest["data_root"]).resolve()
            assert gate.is_dir()
            assert os.getenv("JOIN_PARALLEL_MATCH_GATE_DIR") == str(gate)
            case.match_gate_dir = gate
        if "JP-10-timeout" in requested_cases(selected):
            gate = Path(manifest["timeout_gate_dir"]).resolve()
            assert gate.parent == Path(manifest["data_root"]).resolve()
            assert gate.is_dir()
            assert os.getenv("JOIN_PARALLEL_TIMEOUT_GATE_DIR") == str(gate)
            assert manifest.get("read_timeout") == 64
            case.timeout_gate_dir = gate
        if "JP-10-failure" in requested_cases(selected):
            gate = Path(manifest["failure_gate_dir"]).resolve()
            assert gate.parent == Path(manifest["data_root"]).resolve()
            assert gate.is_dir()
            assert os.getenv("JOIN_PARALLEL_FAILURE_GATE_DIR") == str(gate)
            case.failure_gate_dir = gate
        if "JP-10-retry" in requested_cases(selected):
            gate = Path(manifest["retry_gate_dir"]).resolve()
            assert gate.parent == Path(manifest["data_root"]).resolve()
            assert gate.is_dir()
            assert os.getenv("JOIN_PARALLEL_RETRY_GATE_DIR") == str(gate)
            case.retry_gate_dir = gate
        if "JP-10-idle" in requested_cases(selected):
            gate = Path(manifest["idle_gate_dir"]).resolve()
            assert gate.parent == Path(manifest["data_root"]).resolve()
            assert gate.is_dir()
            assert os.getenv("JOIN_PARALLEL_IDLE_GATE_DIR") == str(gate)
            case.idle_gate_dir = gate
        for case_id in requested_cases(selected):
            print(f"{case_id}: executing SQL and checking runtime state",
                  flush=True)
            case.do_functional(case_id, "CI runtime validation")
        result.update(status="passed",
                      runtime_evidence=getattr(case, "runtime_evidence", []),
                      lifecycle_evidence=getattr(case, "lifecycle_evidence",
                                                 []),
                      inflight_evidence=getattr(case, "inflight_evidence", []),
                      match_evidence=getattr(case, "match_evidence", []),
                      timeout_evidence=getattr(case, "timeout_evidence", []),
                      failure_evidence=getattr(case, "failure_evidence", []),
                      retry_evidence=getattr(case, "retry_evidence", []),
                      idle_evidence=getattr(case, "idle_evidence", []))
        validate_result(result, case=selected, session_mode=session_mode)
    except BaseException:
        result["status"] = "failed"
        result["error"] = traceback.format_exc()
        raise
    finally:
        if case is not None:
            result["runtime_evidence"] = getattr(case, "runtime_evidence", [])
            result["lifecycle_evidence"] = getattr(case, "lifecycle_evidence",
                                                   [])
            result["inflight_evidence"] = getattr(case, "inflight_evidence", [])
            result["match_evidence"] = getattr(case, "match_evidence", [])
            result["timeout_evidence"] = getattr(case, "timeout_evidence", [])
            result["failure_evidence"] = getattr(case, "failure_evidence", [])
            result["retry_evidence"] = getattr(case, "retry_evidence", [])
            result["idle_evidence"] = getattr(case, "idle_evidence", [])
        try:
            try:
                if cursor is not None:
                    cursor.close()
            finally:
                connection.close()
        except BaseException:
            result["status"] = "failed"
            result["cleanup_error"] = traceback.format_exc()
            raise
        finally:
            if mode == "case":
                dump(directory / "result.json", result)


def configure(directory, port, data_root, rpc_timeout=False,
              producer_failure=False):
    # Some vnode metadata paths still use a 128-byte filename buffer.
    # Leave room for internal suffixes independently of CI artifact paths.
    if any(len(os.fsencode(str(data_root / name))) > 64
           for name in ("data", "tmp")):
        raise ValueError(f"Temporary storage path is too long: {data_root}")
    data_root.mkdir(parents=True, exist_ok=True)
    for name in ("server-config", "client-config", "logs", "client-logs",
                 "sources"):
        (directory / name).mkdir()
    for name in ("data", "tmp"):
        (data_root / name).mkdir()
    common = ["fqdn localhost", f"firstEp localhost:{port}",
              f"serverPort {port}", "numOfCores 2", "queryPolicy 1",
              "monitor 0", "telemetryReporting 0", "timezone UTC",
              "qDebugFlag 135", "asyncLog 0"]
    if rpc_timeout:
        common += ["readTimeout 64", "shareConnLimit 1"]
    query_threads = 8 if rpc_timeout or producer_failure else 2
    server = common + [f"dataDir {data_root / 'data'}",
                       f"logDir {directory / 'logs'}",
                       f"tempDir {data_root / 'tmp'}",
                       "minimalDataDirGB 0.001", "supportVnodes 16",
                       f"numOfVnodeQueryThreads {query_threads}",
                       "numOfVnodeFetchThreads 4"]
    config = common + [f"logDir {directory / 'client-logs'}",
                       "queryPlannerTrace 1", "cDebugFlag 135"]
    if rpc_timeout:
        config += ["rpcDebugFlag 135"]
    (directory / "server-config/taos.cfg").write_text("\n".join(server) + "\n")
    (directory / "client-config/taos.cfg").write_text("\n".join(config) + "\n")
    for source in (CASE, HELPER, Path(__file__).resolve()):
        (directory / "sources" / source.name).write_bytes(source.read_bytes())


def check_ports(port):
    if not 1024 <= port <= 65514:
        raise ValueError("port must leave room for 20 adjacent service ports")
    for number in range(port, port + 21):
        for kind in (socket.SOCK_STREAM, socket.SOCK_DGRAM):
            with socket.socket(socket.AF_INET, kind) as probe:
                if kind == socket.SOCK_STREAM:
                    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(("127.0.0.1", number))


@contextmanager
def _runtime_server(args, directory, server, lib_dir, libraries, data_root):
    inflight = "JP-10-inflight" in requested_cases(args.case)
    if inflight:
        require_fetch_gate(server)
    matching = "JP-10-match" in requested_cases(args.case)
    if matching:
        require_match_gate(server)
    timing_out = "JP-10-timeout" in requested_cases(args.case)
    if timing_out:
        require_timeout_gate(server)
    failing = "JP-10-failure" in requested_cases(args.case)
    if failing:
        require_failure_gate(server)
    retrying = "JP-10-retry" in requested_cases(args.case)
    if retrying:
        require_retry_gate(server)
    idling = "JP-10-idle" in requested_cases(args.case)
    if idling:
        require_idle_checker(server)
    check_ports(args.port)
    configure(directory, args.port, data_root, rpc_timeout=timing_out,
              producer_failure=failing or retrying)
    manifest = {"port": args.port, "server": str(server), "case": args.case,
                "data_root": str(data_root),
                "libraries": [str(path) for path in libraries],
                "components": {str(path): sha(path)
                               for path in (server, *libraries)},
                "sources": {str(path): sha(path)
                            for path in (CASE, HELPER, Path(__file__))}}
    dump(directory / "manifest.json", manifest)
    env = os.environ.copy()
    variable = ("PATH" if os.name == "nt" else "DYLD_LIBRARY_PATH"
                if sys.platform == "darwin" else "LD_LIBRARY_PATH")
    env[variable] = str(lib_dir) + os.pathsep + env.get(variable, "")
    env["TZ"] = "UTC"
    env["JOIN_PARALLEL_BUFFER_ROWS"] = "8192"
    if inflight:
        gate = data_root / "fetch-gate"
        gate.mkdir(mode=0o700)
        manifest["fetch_gate_dir"] = str(gate)
        dump(directory / "manifest.json", manifest)
        env["TD_JOIN_TEST_FETCH_GATE_DIR"] = str(gate)
        env["JOIN_PARALLEL_FETCH_GATE_DIR"] = str(gate)
    if matching:
        gate = data_root / "match-gate"
        gate.mkdir(mode=0o700)
        manifest["match_gate_dir"] = str(gate)
        dump(directory / "manifest.json", manifest)
        env["TD_JOIN_TEST_MATCH_GATE_DIR"] = str(gate)
        env["JOIN_PARALLEL_MATCH_GATE_DIR"] = str(gate)
    if timing_out:
        gate = data_root / "timeout-gate"
        gate.mkdir(mode=0o700)
        manifest.update(timeout_gate_dir=str(gate), read_timeout=64)
        dump(directory / "manifest.json", manifest)
        env["TD_JOIN_TEST_TIMEOUT_GATE_DIR"] = str(gate)
        env["JOIN_PARALLEL_TIMEOUT_GATE_DIR"] = str(gate)
    if failing:
        gate = data_root / "failure-gate"
        gate.mkdir(mode=0o700)
        manifest["failure_gate_dir"] = str(gate)
        dump(directory / "manifest.json", manifest)
        env["TD_JOIN_TEST_FAILURE_GATE_DIR"] = str(gate)
        env["JOIN_PARALLEL_FAILURE_GATE_DIR"] = str(gate)
    if retrying:
        gate = data_root / "retry-gate"
        gate.mkdir(mode=0o700)
        manifest["retry_gate_dir"] = str(gate)
        dump(directory / "manifest.json", manifest)
        env["TD_JOIN_TEST_RETRY_GATE_DIR"] = str(gate)
        env["JOIN_PARALLEL_RETRY_GATE_DIR"] = str(gate)
    if idling:
        gate = data_root / "idle-gate"
        gate.mkdir(mode=0o700)
        manifest.update(idle_gate_dir=str(gate), idle_timeout=60,
                        idle_scan_interval=1)
        dump(directory / "manifest.json", manifest)
        env["TD_JOIN_TEST_IDLE_GATE_DIR"] = str(gate)
        env["JOIN_PARALLEL_IDLE_GATE_DIR"] = str(gate)
    process = None
    label = "/".join(requested_cases(args.case))
    result = {"status": "failed", "output": str(directory), "case": args.case}
    with (directory / "taosd.log").open("w") as output:
        try:
            kwargs = ({"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
                      if os.name == "nt" else {"start_new_session": True})
            process = subprocess.Popen(
                [str(server), "-c", str(directory / "server-config")],
                env=env, stdout=output, stderr=subprocess.STDOUT, **kwargs)
            result["server_pid"] = process.pid
            print(f"{label} isolated server PID: {process.pid}", flush=True)
            deadline = time.monotonic() + args.ready_timeout
            while True:
                assert process.poll() is None, "Isolated taosd exited early"
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Isolated taosd did not become ready")
                try:
                    run_client(Path(__file__), directory, env, "ready",
                               min(12, remaining))
                    print(f"{label} server ready on port {args.port}",
                          flush=True)
                    break
                except (subprocess.CalledProcessError,
                        subprocess.TimeoutExpired):
                    time.sleep(min(0.25, max(0, remaining)))
            for role, folder, pattern in (
                    ("SERVER", "logs", "taosdlog.*"),
                    ("CLIENT", "client-logs", "taoslog[0-9]*.*")):
                paths = sorted((directory / folder).glob(pattern))
                assert paths, f"No {role.lower()} debug logs created"
                env[f"JOIN_PARALLEL_{role}_LOGS"] = json.dumps(
                    [str(path) for path in paths])
            session_mode = None
            if requires_sessions(args.case):
                server_paths = [Path(path) for path in json.loads(
                    env["JOIN_PARALLEL_SERVER_LOGS"])]
                session_mode, mode_events = detect_session_mode(server_paths)
                manifest.update(session_mode=session_mode,
                                session_mode_evidence=mode_events)
                dump(directory / "manifest.json", manifest)
                env["JOIN_PARALLEL_SESSION_MODE"] = session_mode
                print(f"{label} server memory pool: {session_mode}",
                      flush=True)
            yield directory, env, session_mode
            result["status"] = "passed"
        except BaseException:
            result["error"] = traceback.format_exc()
            raise
        finally:
            if process is not None:
                result["service_exit"] = stop_owned_process(process)
                print(f"{label} service exit: {result['service_exit']}",
                      flush=True)
                if (result["service_exit"]["exit_code"] != 0 or
                        result["service_exit"]["forced_kill"]):
                    result["status"] = "failed"
            dump(directory / "controller-result.json", result)
    assert result["status"] == "passed", result


@contextmanager
def runtime_server(args):
    label = "/".join(requested_cases(args.case))
    server, lib_dir, libraries = runtime_layout(args.build_dir)
    print(f"{label} server: {server}", flush=True)
    for library in libraries:
        print(f"{label} client library: {library}", flush=True)
    if args.output:
        directory = args.output.resolve()
        directory.mkdir(parents=True, exist_ok=False)
    else:
        parent = Path(os.getenv("SIM_DIR", tempfile.gettempdir()))
        parent = parent / "join_parallel_runtime"
        parent.mkdir(parents=True, exist_ok=True)
        directory = Path(tempfile.mkdtemp(prefix="run-", dir=parent)).resolve()
    print(f"{label} artifacts: {directory}", flush=True)
    storage_parent = None
    if os.name != "nt" and Path("/tmp").is_dir():
        storage_parent = "/tmp"
    data_root = None
    error = None
    try:
        with tempfile.TemporaryDirectory(prefix="jp08-",
                                          dir=storage_parent) as tmp:
            data_root = Path(tmp).resolve()
            print(f"{label} temporary storage: {data_root}", flush=True)
            with _runtime_server(args, directory, server, lib_dir, libraries,
                                 data_root) as environment:
                yield environment
    except BaseException:
        error = traceback.format_exc()
        raise
    finally:
        path = directory / "controller-result.json"
        result = (json.loads(path.read_text()) if path.is_file()
                  else {"status": "failed", "output": str(directory)})
        result["data_root"] = str(data_root) if data_root is not None else None
        result["data_removed"] = data_root is None or not data_root.exists()
        if error is not None:
            result.update(status="failed", error=error)
        if not result["data_removed"]:
            result["status"] = "failed"
        dump(path, result)
    assert result["status"] == "passed", result


def check_runtime_case(environment, case_id, timeout):
    directory, env, session_mode = environment
    run_client(Path(__file__), directory, env, "case", timeout)
    result = json.loads((directory / "result.json").read_text())
    validate_result(result, case=case_id, session_mode=session_mode)
