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

import copy
import json
import os
from pathlib import Path
import re
import time


MATCH_SCENARIOS = (
    ("equal", "asof", 16384),
    ("equal", "window", 16384),
    ("strict-backward", "asof", 16128),
    ("forward", "asof", 16256),
    ("strict-forward", "asof", 16128),
    ("range", "window", 16128),
    ("null-filter", "asof", 8002),
    ("null-filter", "window", 8002),
)


class JoinRuntimeLogs:
    """Correlate appended runtime logs without mixing tasks or log files."""

    identity = re.compile(
        r"QID:(0x[\da-fA-F]+),.*?\bSID:(-?\d+),.*?"
        r"CID:(0x[\da-fA-F]+),\s+TID:(0x[\da-fA-F]+),\s+EID:(\d+)"
    )
    async_identity = re.compile(
        r"\bQID:(0x[\da-fA-F]+),\s+TID:(0x[\da-fA-F]+)\s+"
        r"(exchange lifecycle created|exchange resources released|"
        r"fetch context registered|fetch context released|"
        r"fetch test barrier held|fetch test barrier resumed|"
        r"fetch test barrier timed out|fetch callback discarded|"
        r"fetch retry test held|fetch retry test injected|"
        r"fetch retry test resumed|fetch retry test timed out|"
        r"fetch retry next held|fetch retry next resumed),"
    )
    match_identity = re.compile(
        r"\bQID:(0x[\da-fA-F]+),\s+TID:(0x[\da-fA-F]+)\s+"
        r"temporal match test (held|resumed|timed out|progress|destroyed),"
    )
    timeout_identity = re.compile(
        r"\bQID:(0x[\da-fA-F]+),\s+TID:(0x[\da-fA-F]+)\s+"
        r"query timeout test (held|resumed|timed out),"
    )
    failure_identity = re.compile(
        r"\bQID:(0x[\da-fA-F]+),\s+TID:(0x[\da-fA-F]+)\s+"
        r"join failure test (held|injected|resumed|timed out),"
    )
    exchange_error_identity = re.compile(
        r"\bTID:(0x[\da-fA-F]+)\s+QID:(0x[\da-fA-F]+) "
        r"exchange \S+ (\d+)th source got rsp, code:(-?\d+),"
    )
    match_fields = ("blockId", "asofOp", "rows", "totalRows", "probeBegin",
                    "probeRead", "probeEnd", "cacheRows", "cacheOutRows",
                    "cacheGroups", "cacheRead", "candidates", "grpRemains",
                    "done")
    client_request = re.compile(
        r"\breq:(0x[\da-fA-F]+), create request from conn:(0x[\da-fA-F]+), "
        r"current:-?\d+, app current:-?\d+, total:-?\d+, "
        r"QID:(0x[\da-fA-F]+)\s*$"
    )
    client_connection = re.compile(
        r"\bconn:(0x[\da-fA-F]+), connection is opening, connId:(\d+), "
        r"dnodeConn:[^,\s]+, QID:(0x[\da-fA-F]+)\s*$"
    )

    def __init__(self, query_id):
        self.query_id = query_id
        self.files = []
        self.events = []
        self.async_events = []
        self.match_events = []
        self.timeout_events = []
        self.timeout_client_events = []
        self.failure_events = []
        self.failure_exchange_events = []
        self.failure_client_events = []
        self.retry_client_events = []
        self.client_requests = []
        self.client_connections = []
        self.plans = {}
        self.roots = {}
        for role in ("SERVER", "CLIENT"):
            variable = f"JOIN_PARALLEL_{role}_LOGS"
            paths = json.loads(os.environ[variable])
            assert isinstance(paths, list) and paths, variable
            for name in paths:
                path = Path(name)
                status = path.stat()
                assert path.is_file(), path
                self.files.append({
                    "path": path, "role": role, "inode": status.st_ino,
                    "offset": status.st_size, "pending": b"",
                })

    @classmethod
    def key(cls, line):
        match = cls.identity.search(line)
        if match is None:
            return None
        qid, sid, cid, tid, eid = match.groups()
        return int(qid, 16), int(sid), int(cid, 16), int(tid, 16), int(eid)

    @staticmethod
    def nodes(node):
        name = node["Name"]
        body = node[name]
        yield name, body
        for child in body.get("Children", []):
            yield from JoinRuntimeLogs.nodes(child)

    def poll(self):
        for state in self.files:
            path = state["path"]
            status = path.stat()
            assert status.st_ino == state["inode"], f"Log rotated: {path}"
            assert status.st_size >= state["offset"], f"Log truncated: {path}"
            with path.open("rb") as stream:
                stream.seek(state["offset"])
                data = state["pending"] + stream.read()
                offset = state["offset"] - len(state["pending"])
                state["offset"] = stream.tell()
            lines = data.splitlines(keepends=True)
            state["pending"] = b""
            for raw in lines:
                if not raw.endswith(b"\n"):
                    state["pending"] = raw
                    break
                line = raw.decode(errors="replace")
                if state["role"] == "CLIENT":
                    if (getattr(self, "trace_migration", False) and
                            any(marker in line for marker in (
                                "start to send ", " is sent to ",
                                "read error:", "handle rsp msg, type:"))):
                        self.migration_client_events.append({
                            "path": str(path), "offset": offset,
                            "line": line.rstrip(),
                        })
                    self._record_client_identity(path, offset, line)
                    retry_qid = re.search(r"\bQID:(0x[\da-fA-F]+),", line)
                    if (retry_qid is not None and
                            int(retry_qid[1], 16) == self.query_id and
                            any(marker in line for marker in (
                                "start to restart all tasks by error:",
                                "update job sId:",
                                "job failed with error "))):
                        self.retry_client_events.append({
                            "path": str(path), "offset": offset,
                            "line": line.rstrip(),
                        })
                    if any(marker in line for marker in (
                            "job failed with error ",
                            "start to send drop-task msg")):
                        self.failure_client_events.append({
                            "path": str(path), "offset": offset,
                            "line": line.rstrip(),
                        })
                    if any(marker in line for marker in (
                            "start to send ", " is sent to ",
                            "remove timeout msg sid:",
                            "job failed with error Conn read timeout",
                            "clear refId before send drop-task msg")):
                        self.timeout_client_events.append({
                            "path": str(path), "offset": offset,
                            "line": line.rstrip(),
                        })
                key = self.key(line)
                asynchronous = self.async_identity.search(line)
                matching = self.match_identity.search(line)
                timing = self.timeout_identity.search(line)
                failing = self.failure_identity.search(line)
                exchange_error = self.exchange_error_identity.search(line)
                if (state["role"] == "SERVER" and failing is not None and
                        int(failing[1], 16) == self.query_id):
                    fields = dict(re.findall(
                        r"\b(blockId|rows|code):(-?\d+)(?=,|\s|$)", line))
                    self.failure_events.append({
                        "path": str(path), "offset": offset,
                        "line": line.rstrip(), "kind": failing[3],
                        "query_id": int(failing[1], 16),
                        "task_id": int(failing[2], 16),
                        "fields": {name: int(value)
                                   for name, value in fields.items()},
                    })
                if (state["role"] == "SERVER" and
                        exchange_error is not None and
                        int(exchange_error[2], 16) == self.query_id and
                        int(exchange_error[4]) != 0):
                    self.failure_exchange_events.append({
                        "path": str(path), "offset": offset,
                        "line": line.rstrip(),
                        "query_id": int(exchange_error[2], 16),
                        "task_id": int(exchange_error[1], 16),
                        "source_index": int(exchange_error[3]),
                        "code": int(exchange_error[4]),
                    })
                if (state["role"] == "SERVER" and timing is not None and
                        int(timing[1], 16) == self.query_id):
                    fields = dict(re.findall(
                        r"\b(blockId|rows|readTimeout):(-?\d+)(?=,|\s|$)",
                        line))
                    self.timeout_events.append({
                        "path": str(path), "offset": offset,
                        "line": line.rstrip(), "kind": timing[3],
                        "query_id": int(timing[1], 16),
                        "task_id": int(timing[2], 16),
                        "fields": {name: int(value)
                                   for name, value in fields.items()},
                    })
                elif (state["role"] == "SERVER" and matching is not None and
                        int(matching[1], 16) == self.query_id):
                    fields = dict(re.findall(
                        r"\b(" + "|".join(self.match_fields) +
                        r"):(-?\d+)(?=,|\s|$)", line))
                    kind = re.search(r"\bkind:(asof|window)(?=,|\s|$)", line)
                    self.match_events.append({
                        "path": str(path), "offset": offset,
                        "line": line.rstrip(), "kind": matching[3],
                        "query_id": int(matching[1], 16),
                        "task_id": int(matching[2], 16),
                        "join_kind": kind[1] if kind is not None else None,
                        "fields": {name: int(value)
                                   for name, value in fields.items()},
                    })
                elif (state["role"] == "SERVER" and asynchronous is not None
                        and int(asynchronous[1], 16) == self.query_id):
                    fields = dict(re.findall(
                        r"\b(exchangeId|blockId|fetchId|fetches|sourceIndex|"
                        r"seqId|code):(-?\d+)(?=,|\s|$)", line))
                    self.async_events.append({
                        "path": str(path), "offset": offset,
                        "line": line.rstrip(), "kind": asynchronous[3],
                        "query_id": int(asynchronous[1], 16),
                        "task_id": int(asynchronous[2], 16),
                        "fields": {name: int(value)
                                   for name, value in fields.items()},
                    })
                elif key is not None and key[0] == self.query_id:
                    event = {"path": str(path), "offset": offset,
                             "key": key, "line": line.rstrip()}
                    if state["role"] == "SERVER":
                        self.events.append(event)
                    elif "physical plan len:" in line:
                        plan = json.loads(line[line.index("{"):])
                        plan = plan["PhysiSubplan"]
                        assert int(plan["Id"]["QueryId"]) == self.query_id
                        self.plans[key] = plan
                        if int(plan["Level"]) == 0:
                            self.roots[key] = plan
                offset += len(raw)

    def _record_client_identity(self, path, offset, line):
        request = self.client_request.search(line)
        connection = self.client_connection.search(line)
        event = {"path": str(path), "offset": offset, "line": line.rstrip()}
        if request is not None:
            event.update(req_rid=int(request[1], 16),
                         local_conn_id=int(request[2], 16),
                         query_id=int(request[3], 16))
            self.client_requests.append(event)
        elif connection is not None:
            event.update(local_conn_id=int(connection[1], 16),
                         server_conn_id=int(connection[2]),
                         query_id=int(connection[3], 16))
            self.client_connections.append(event)

    def wait_kill_id(self, timeout=15):
        """Match one new request to its own connection's server identity."""
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            requests = [event for event in self.client_requests
                        if event["query_id"] == self.query_id]
            assert len(requests) <= 1, "Multiple target client requests"
            if requests:
                request = requests[0]
                assert request["req_rid"] > 0 and request["local_conn_id"] > 0
                path, local = request["path"], request["local_conn_id"]
                same_request = [event for event in self.client_requests
                                if event["path"] == path and
                                event["req_rid"] == request["req_rid"]]
                assert len(same_request) == 1, "Client request identity reused"
                connections = [event for event in self.client_connections
                               if event["path"] == path and
                               event["local_conn_id"] == local]
                assert len(connections) <= 1, (
                    "Client connection identity reused"
                )
                if connections:
                    connection = connections[0]
                    server = connection["server_conn_id"]
                    assert 0 < server <= 0xffffffff, connection
                    assert connection["offset"] < request["offset"], (
                        "Client connection opened after target request"
                    )
                    same_server = [event for event in self.client_connections
                                   if event["path"] == path and
                                   event["server_conn_id"] == server]
                    assert len(same_server) == 1, (
                        "Server connection identity is ambiguous"
                    )
                    return {"kill_id": f"{server:x}:{request['req_rid']:x}",
                            "request": dict(request),
                            "connection": dict(connection)}
            if time.monotonic() >= deadline:
                raise AssertionError("Missing target client KILL identity")
            time.sleep(0.02)

    def root_events(self):
        assert len(self.roots) <= 1, (
            "Root task retried during runtime validation"
        )
        if not self.roots:
            return []
        key, plan = next(iter(self.roots.items()))
        # Encoded multi-block sinks return continue=0 only at a FULL watermark.
        assert plan["ProcessOneBlock"] is False, plan
        assert not plan.get("LocalBlockExchange", False), plan
        sink = plan["DataSink"]
        assert not sink[sink["Name"]].get("LocalBlockExchange", False)
        joins = [body for name, body in self.nodes(plan["RootNode"])
                 if "Join" in name]
        assert len(joins) == 1, "Expected one root JOIN computation"
        assert len(joins[0]["Children"]) == 2
        for child in joins[0]["Children"]:
            exchanges = [body for name, body in self.nodes(child)
                         if name == "PhysiExchange"]
            assert len(exchanges) == 1, "Root inputs must be split JOIN tasks"
            for source in exchanges[0]["SrcEndPoints"]:
                source = source.get("DownstreamSource", source)
                assert not source.get("LocalExec", False), source
        events = [event for event in self.events if event["key"] == key]
        assert len({event["path"] for event in events}) <= 1, (
            "Root task appeared in multiple server logs"
        )
        return events

    @staticmethod
    def pause_candidate(events):
        # A later put in the same result batch can overwrite continue=0.
        # Require the execution phase to return before calling it a pause.
        full = post = None
        for index, event in enumerate(events):
            line = event["line"]
            if any(text in line for text in (
                    "qExecTask done", "qExecTask end with empty",
                    "qExecTask failed", "task ctx dropped")):
                return None
            if "data put into sink," in line:
                full = index if "continueExecTask:0" in line else None
                post = None
            elif "start to execTask," in line:
                full = post = None
            elif any(text in line for text in (
                    "task not end and buf is ", "task fetched blocks ",
                    "there are data in sink,", "fetch rsp send,",
                    "start to handle event at phase PRE_FETCH")):
                full = post = None
            elif (full is not None and "end to handle event at phase "
                  in line and re.search(r"POST_(?:QUERY|CQUERY), code:0\s",
                                       line)):
                post = index
        return None if post is None else (full, post)

    def wait_paused(self, timeout=30):
        deadline = time.monotonic() + timeout
        previous, since = None, time.monotonic()
        while time.monotonic() < deadline:
            self.poll()
            events = self.root_events()
            candidate = self.pause_candidate(events)
            signature = (tuple(events[i]["offset"] for i in candidate)
                         if candidate is not None else None)
            if signature != previous:
                previous, since = signature, time.monotonic()
            if candidate is not None and time.monotonic() - since >= 0.25:
                self.paused = [events[i] for i in candidate]
                return self.paused
            time.sleep(0.02)
        tail = [event["line"] for event in self.root_events()[-3:]]
        raise AssertionError(
            f"No quiescent FULL root sink: qid={self.query_id}, "
            f"root_tasks={len(self.roots)}, tail={tail}"
        )

    def mark_resume(self):
        self.poll()
        self.resume_offset = len(self.root_events())

    def check_resumed(self, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.poll()
            events = self.root_events()[self.resume_offset:]
            resumed = started = produced = completed = None
            for event in events:
                line = event["line"]
                if (resumed is None and re.search(
                        r"task not end and buf is (LOW|EMPTY), "
                        r"need to continue query", line)):
                    resumed = event
                if (resumed and started is None and
                        "start to execTask," in line):
                    started = event
                if (started and produced is None and
                        "data put into sink," in line):
                    produced = event
                if produced and ("task all data fetched and done" in line or
                                 "no more data in sink and query end" in line):
                    completed = event
            if completed is not None:
                return {"paused": self.paused, "resumed": resumed,
                        "started": started, "produced": produced,
                        "completed": completed}
            time.sleep(0.02)
        raise AssertionError("Missing sink consume/resume/completion chain")

    @staticmethod
    def producer_cycle(events):
        full = post = resumed = started = None
        for event in events:
            line = event["line"]
            put = "data put into sink," in line
            if started is not None and put:
                return {"full": full, "paused": post, "resumed": resumed,
                        "started": started, "produced": event}
            if any(text in line for text in (
                    "qExecTask done", "qExecTask end with empty",
                    "qExecTask failed", "task ctx dropped")):
                full = post = resumed = started = None
                continue
            if post is not None:
                if resumed is None and re.search(
                        r"task not end and buf is (LOW|EMPTY), "
                        r"need to continue query", line):
                    resumed = event
                    continue
                if resumed is not None and "start to execTask," in line:
                    started = event
                    continue
                if not put and "start to execTask," not in line:
                    continue
                full = post = resumed = started = None
            if put:
                full = event if "continueExecTask:0" in line else None
            elif any(text in line for text in (
                    "start to execTask,", "task not end and buf is ",
                    "task fetched blocks ", "there are data in sink,",
                    "fetch rsp send,", "phase PRE_FETCH")):
                full = None
            elif (full is not None and "end to handle event at phase "
                  in line and re.search(r"POST_(?:QUERY|CQUERY), code:0\s",
                                       line)):
                post = event
        return None

    def check_producers(self, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.poll()
            producers = [(key, plan) for key, plan in self.plans.items()
                         if int(plan["Level"]) > 0 and any(
                             "Join" in name
                             for name, _ in self.nodes(plan["RootNode"]))]
            assert len(producers) <= 2, "Unexpected JOIN task topology"
            proofs = []
            for key, plan in producers:
                assert plan["ProcessOneBlock"] is False, plan
                assert not plan.get("LocalBlockExchange", False), plan
                sink = plan["DataSink"]
                assert not sink[sink["Name"]].get("LocalBlockExchange", False)
                events = [event for event in self.events if event["key"] == key]
                assert len({event["path"] for event in events}) <= 1
                proof = self.producer_cycle(events)
                if proof is not None:
                    proofs.append(proof)
            if len(proofs) == 2:
                return proofs
            time.sleep(0.02)
        raise AssertionError("Missing FULL/pause/resume in JOIN producers")

    def _cancel_groups(self, planned):
        generations = {(key[0], key[2], key[3]) for key in planned}
        assert len(generations) == len(planned), "Task execution retried"
        groups = {key: {"events": [], "registered": None, "path": None}
                  for key in planned}
        for index, event in enumerate(self.events):
            key = event["key"]
            assert key in groups, f"Unexpected runtime task identity: {key}"
            group = groups[key]
            if group["path"] is None:
                group["path"] = event["path"]
            assert group["path"] == event["path"], (
                f"Task appeared in multiple server logs: {key}"
            )
            group["events"].append((index, event))
            assert not any(message in event["line"] for message in (
                "fail to remove session",
                "taosHashRemove from ctx hash failed",
            )), f"Task cleanup failed: {event['line']}"
            if "task preprocess succeed," in event["line"]:
                assert group["registered"] is None, (
                    f"Task registered more than once: {key}"
                )
                group["registered"] = event
        assert all(group["registered"] is not None
                   for group in groups.values()), (
            "Physical plans and registered tasks differ"
        )
        return groups

    @staticmethod
    def _session_handles(event):
        match = re.search(
            r"\bsession:(0x[\da-fA-F]+),\s+job:(0x[\da-fA-F]+)(?:,|\s|$)",
            event["line"])
        assert match is not None, f"Missing session identity: {event['line']}"
        handles = tuple(int(value, 16) for value in match.groups())
        assert all(handles), f"Null session identity: {event['line']}"
        return handles

    def _session_evidence(self, group, dropped=None):
        initialized = [event for _, event in group["events"]
                       if " session initialized," in event["line"]]
        destroyed = [event for _, event in group["events"]
                     if " task session destroyed," in event["line"]]
        if self.cancel_session_mode == "disabled":
            assert not initialized and not destroyed, (
                "Session evidence contradicts disabled memory pool"
            )
            return None, None
        assert len(initialized) == 1, "Expected one initialized task session"
        assert len(destroyed) <= 1, "Task session destroyed more than once"
        first = initialized[0]
        handles = self._session_handles(first)
        last = destroyed[0] if destroyed else None
        if last is not None:
            assert self._session_handles(last) == handles, (
                "Destroyed session identity differs from initialization"
            )
            assert first["offset"] < last["offset"], (
                "Session destroyed before initialization"
            )
        if dropped is not None:
            assert last is not None, "Task dropped without destroying session"
            assert last["offset"] < dropped["offset"], (
                "Session destroyed after task context drop"
            )
        return first, last

    def _planned_exchange_blocks(self, planned):
        result = {}
        for key in planned:
            blocks = []
            for name, body in self.nodes(self.plans[key]["RootNode"]):
                if name != "PhysiExchange":
                    continue
                assert not body.get("DynamicOp", False), (
                    "Dynamic Exchange lifecycle is outside this proof"
                )
                descriptor = body["OutputDataBlockDesc"]["DataBlockDesc"]
                block = int(descriptor["DataBlockId"])
                assert block >= 0 and block not in blocks, (
                    "Exchange output blocks must have distinct identities"
                )
                blocks.append(block)
            result[key] = sorted(blocks)
        return result

    def _exchange_groups(self, task_groups):
        identities = {(key[0], key[3]): key for key in task_groups}
        assert len(identities) == len(task_groups), (
            "Ambiguous Exchange task execution identity"
        )
        result = {key: {block: {"created": None, "finalized": None,
                                "requests": {}}
                        for block in self.cancel_exchange_blocks[key]}
                  for key in task_groups}
        instances = {}
        for event in self.async_events:
            native = event["query_id"], event["task_id"]
            assert native in identities, "Unexpected Exchange task identity"
            key = identities[native]
            assert event["path"] == task_groups[key]["path"], (
                "Exchange appeared in multiple server logs"
            )
            fields = event["fields"]
            exchange_id = fields.get("exchangeId")
            block = fields.get("blockId")
            assert exchange_id is not None and exchange_id > 0, event
            assert block in result[key], "Unexpected Exchange block identity"
            state = result[key][block]
            proof = {name: event[name] for name in ("path", "offset", "line")}
            proof["key"] = key
            kind = event["kind"]
            if kind == "exchange lifecycle created":
                assert state["created"] is None, "Exchange created twice"
                instance = event["path"], exchange_id
                assert instance not in instances, "Exchange instance reused"
                instances[instance] = key, block
                state.update(exchange_id=exchange_id, created=proof)
                continue
            assert state["created"] is not None, (
                "Exchange event lacks a matching creation"
            )
            assert state["exchange_id"] == exchange_id, (
                "Exchange instance differs from creation"
            )
            assert state["created"]["offset"] < event["offset"], event
            if kind == "exchange resources released":
                assert state["finalized"] is None, "Exchange released twice"
                assert fields.get("code") == 0, "Exchange cleanup failed"
                count = fields.get("fetches")
                assert count is not None and count >= 0, event
                if key == self.cancel_root:
                    assert event["offset"] >= self.cancel_file_offsets[
                        event["path"]], "Root Exchange released before cancel"
                state.update(finalized=proof, fetch_count=count)
                continue
            fetch_id = fields.get("fetchId")
            source = fields.get("sourceIndex")
            sequence = fields.get("seqId")
            assert fetch_id is not None and fetch_id > 0, event
            assert source is not None and source >= 0, event
            assert sequence is not None and sequence >= 0, event
            requests = state["requests"]
            if kind == "fetch context registered":
                assert fetch_id not in requests, (
                    "Fetch context registered twice"
                )
                assert state["finalized"] is None, (
                    "Fetch context registered after Exchange release"
                )
                requests[fetch_id] = {
                    "fetch_id": fetch_id, "source_index": source,
                    "seq_id": sequence, "registered": proof,
                    "released": None,
                }
            else:
                assert fetch_id in requests, "Fetch release lacks registration"
                request = requests[fetch_id]
                assert (request["source_index"], request["seq_id"]) == (
                    source, sequence), "Fetch context identity changed"
                assert request["registered"]["offset"] < event["offset"], event
                assert kind != "fetch test barrier timed out", (
                    "Fetch test barrier timed out"
                )
                if kind == "fetch context released":
                    assert request["released"] is None, (
                        "Fetch context freed twice"
                    )
                    request["released"] = proof
        for blocks in result.values():
            assert all(state["created"] is not None
                       for state in blocks.values()), (
                "Physical Exchanges and created instances differ"
            )
        return result

    def _exchange_evidence(self, groups):
        result, missing = {}, []
        for key, blocks in groups.items():
            result[key] = []
            for block, state in blocks.items():
                if state["finalized"] is None:
                    missing.append((key, block, "Exchange release"))
                else:
                    count = state["fetch_count"]
                    assert len(state["requests"]) == count and (
                        sorted(state["requests"]) == list(range(1, count + 1))
                    ), "Fetch context registrations differ from final count"
                for fetch_id, request in state["requests"].items():
                    if request["released"] is None:
                        missing.append((key, block, fetch_id))
                result[key].append({
                    "block_id": block, "exchange_id": state["exchange_id"],
                    "created": state["created"],
                    "finalized": state["finalized"],
                    "fetch_count": state.get("fetch_count"),
                    "requests": list(state["requests"].values()),
                })
        return result, missing

    def _read_session_mode(self):
        self.cancel_session_mode = os.getenv("JOIN_PARALLEL_SESSION_MODE")
        assert self.cancel_session_mode in ("enabled", "disabled"), (
            "Set JOIN_PARALLEL_SESSION_MODE from actual server pool startup"
        )

    def _freeze_cancel_state(self):
        planned = frozenset(self.plans)
        groups = self._cancel_groups(planned)
        self.cancel_tasks = {key: group["registered"]
                             for key, group in groups.items()}
        self.cancel_sessions = {
            key: self._session_evidence(group)[0]
            for key, group in groups.items()}
        self.cancel_root = next(iter(self.roots))
        self.cancel_event_index = len(self.events)
        self.cancel_file_offsets = {
            str(state["path"]): state["offset"] for state in self.files
            if state["role"] == "SERVER"}
        self.cancel_exchange_blocks = self._planned_exchange_blocks(planned)
        exchanges = self._exchange_groups(groups)
        self.cancel_exchanges = {
            (key, block): state["created"]
            for key, blocks in exchanges.items()
            for block, state in blocks.items()}

    def mark_cancel(self):
        """Freeze registered tasks while the observed root remains paused."""
        self._read_session_mode()
        self.poll()
        events = self.root_events()
        candidate = self.pause_candidate(events)
        assert getattr(self, "paused", None) and candidate is not None, (
            "Cancellation requires an observed FULL root pause"
        )
        assert [events[index] for index in candidate] == self.paused, (
            "Root pause changed before cancellation"
        )
        self._freeze_cancel_state()

    def _cleanup_snapshot(self):
        assert getattr(self, "cancel_tasks", None), "Mark cancellation first"
        planned = frozenset(self.cancel_tasks)
        self.poll()
        assert frozenset(self.plans) == planned, (
            "Physical task identities changed after cancellation"
        )
        groups = self._cancel_groups(planned)
        assert self._planned_exchange_blocks(planned) == (
            self.cancel_exchange_blocks), "Physical Exchange plan changed"
        exchanges = self._exchange_groups(groups)
        for key, blocks in exchanges.items():
            for block, state in blocks.items():
                assert state["created"] == self.cancel_exchanges[key, block]
        exchange_proofs, async_missing = self._exchange_evidence(exchanges)
        tasks, removed, missing = [], [], []
        for key in sorted(planned):
            registered = self.cancel_tasks[key]
            assert groups[key]["registered"] == registered
            dropped = None
            for index, event in groups[key]["events"]:
                line = event["line"]
                if line.endswith(" the whole query job removed"):
                    removed.append(event)
                if not line.endswith(" task ctx dropped"):
                    continue
                assert event["offset"] > registered["offset"], (
                    f"Task dropped before registration: {key}"
                )
                if key == self.cancel_root:
                    assert index >= self.cancel_event_index and (
                        event["offset"] >= self.cancel_file_offsets[
                            event["path"]]), (
                        "Root dropped before the cancellation boundary"
                    )
                assert dropped is None, f"Task dropped twice: {key}"
                dropped = event
            initialized, session = self._session_evidence(groups[key],
                                                          dropped)
            assert initialized == self.cancel_sessions[key], (
                "Task session initialization changed after cancellation"
            )
            if dropped is None:
                missing.append(key)
                continue
            proof = {"key": key, "registered": registered,
                     "dropped": dropped,
                     "planned_exchange_blocks":
                         self.cancel_exchange_blocks[key],
                     "exchanges": exchange_proofs[key]}
            if session is not None:
                proof["session_initialized"] = initialized
                proof["session"] = session
            tasks.append(proof)
        proof = {"paused": self.paused, "tasks": tasks,
                 "job_removed": removed,
                 "planned_task_keys": sorted(planned),
                 "root_key": self.cancel_root,
                 "cancel_boundary": dict(self.cancel_file_offsets),
                 "session_mode": self.cancel_session_mode}
        return proof, missing, async_missing

    def check_cancelled(self, timeout=10):
        """Observe task, Exchange and query-owned fetch context reclamation."""
        deadline = time.monotonic() + timeout
        while True:
            proof, missing, asynchronous = self._cleanup_snapshot()
            if not missing and not asynchronous:
                return proof
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"Missing task context cleanup: {missing}; "
                    f"missing async cleanup: {asynchronous}"
                )
            time.sleep(0.02)

    @staticmethod
    def _async_proof(event, key):
        proof = {name: event[name] for name in ("path", "offset", "line")}
        proof["key"] = key
        return proof

    def _inflight_events(self, ticket):
        selected = {}
        identity = ticket["key"][0], ticket["key"][3]
        fields = {"exchangeId": ticket["exchange_id"],
                  "blockId": ticket["block_id"],
                  "fetchId": ticket["fetch_id"]}
        for event in self.async_events:
            assert event["kind"] != "fetch test barrier timed out", (
                "Fetch test barrier timed out"
            )
            if (event["query_id"], event["task_id"]) != identity:
                continue
            if not all(event["fields"].get(name) == value
                       for name, value in fields.items()):
                continue
            kind = event["kind"]
            if kind not in ("fetch context registered",
                            "fetch test barrier held",
                            "fetch test barrier resumed",
                            "fetch callback discarded",
                            "fetch context released"):
                continue
            assert event["path"] == ticket["held"]["path"], (
                "Held fetch appeared in multiple server logs"
            )
            assert (event["fields"].get("sourceIndex"),
                    event["fields"].get("seqId")) == (
                        ticket["source_index"], ticket["seq_id"]), (
                "Held fetch context identity changed"
            )
            assert kind not in selected, f"Duplicate held fetch event: {kind}"
            selected[kind] = self._async_proof(event, ticket["key"])
        return selected

    def _pending_inflight(self):
        ticket = self.inflight_request
        events = self._inflight_events(ticket)
        assert events.get("fetch context registered") == ticket["registered"]
        assert events.get("fetch test barrier held") == ticket["held"]
        assert not any(kind in events for kind in (
            "fetch test barrier resumed", "fetch callback discarded",
            "fetch context released")), "Held fetch completed before release"
        return events

    def wait_inflight_held(self, timeout=15):
        """Observe a root fetch held before delivery into its Exchange."""
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            assert not any(event["kind"] == "fetch test barrier timed out"
                           for event in self.async_events), (
                "Fetch test barrier timed out"
            )
            held = [event for event in self.async_events
                    if event["kind"] == "fetch test barrier held"]
            assert len(held) <= 1, "Multiple held fetch contexts"
            if self.roots and held:
                self.root_events()
                key = next(iter(self.roots))
                event = held[0]
                native = event["query_id"], event["task_id"]
                assert native == (key[0], key[3]), (
                    "Held fetch does not belong to the root task"
                )
                matches = [planned for planned in self.plans
                           if (planned[0], planned[3]) == native]
                assert matches == [key], "Ambiguous held fetch task identity"
                fields = event["fields"]
                ticket = {"key": key,
                          "exchange_id": fields.get("exchangeId"),
                          "block_id": fields.get("blockId"),
                          "fetch_id": fields.get("fetchId"),
                          "source_index": fields.get("sourceIndex"),
                          "seq_id": fields.get("seqId"),
                          "held": self._async_proof(event, key)}
                assert all(type(ticket[name]) is int for name in (
                    "exchange_id", "block_id", "fetch_id", "source_index",
                    "seq_id")), "Incomplete held fetch identity"
                assert ticket["exchange_id"] > 0 and ticket["fetch_id"] > 0
                assert all(ticket[name] >= 0 for name in (
                    "block_id", "source_index", "seq_id")), ticket
                events = self._inflight_events(ticket)
                registered = events.get("fetch context registered")
                assert registered is not None, "Held fetch lacks registration"
                assert registered["offset"] < event["offset"], (
                    "Held fetch precedes registration"
                )
                ticket["registered"] = registered
                self.inflight_request = ticket
                self._pending_inflight()
                return dict(ticket)
            if time.monotonic() >= deadline:
                raise AssertionError("Missing held root fetch context")
            time.sleep(0.02)

    def mark_inflight_cancel(self):
        """Freeze a launched DAG while its selected root fetch is pending."""
        assert getattr(self, "inflight_request", None), (
            "Call wait_inflight_held first"
        )
        self._read_session_mode()
        self.poll()
        events = self.root_events()
        assert not any("task ctx dropped" in event["line"]
                       for event in events), "Root dropped before cancellation"
        self._pending_inflight()
        self.paused = []
        self._freeze_cancel_state()
        held = self.inflight_request["held"]
        assert held["offset"] < self.cancel_file_offsets[held["path"]], (
            "Held fetch is outside the cancellation boundary"
        )

    def _detached_inflight_root(self, proof):
        root = next((task for task in proof["tasks"]
                     if task["key"] == self.cancel_root), None)
        if root is None:
            return None
        if any(exchange["finalized"] is None
               for exchange in root["exchanges"]):
            return None
        return root

    def wait_inflight_detached(self, timeout=15):
        """Require root cleanup while transport still owns the held context."""
        assert getattr(self, "inflight_request", None), "No held fetch context"
        deadline = time.monotonic() + timeout
        while True:
            proof, _, _ = self._cleanup_snapshot()
            self._pending_inflight()
            root = self._detached_inflight_root(proof)
            if root is not None:
                self.inflight_detached = root
                return root
            if time.monotonic() >= deadline:
                raise AssertionError("Held fetch root did not detach")
            time.sleep(0.02)

    def mark_inflight_release(self):
        """Record a release boundary only after cancellation detached root."""
        assert getattr(self, "inflight_detached", None), (
            "Wait for held fetch root cleanup before release"
        )
        proof, _, _ = self._cleanup_snapshot()
        self._pending_inflight()
        assert self._detached_inflight_root(proof) is not None, (
            "Held fetch root cleanup disappeared"
        )
        self.inflight_release_offsets = {
            str(state["path"]): state["offset"] for state in self.files
            if state["role"] == "SERVER"}

    def check_inflight_cancelled(self, timeout=15):
        """Match a killed query's held response to its eventual discard/free."""
        assert getattr(self, "inflight_release_offsets", None), (
            "Call mark_inflight_release before releasing the barrier"
        )
        proof = self.check_cancelled(timeout)
        ticket = dict(self.inflight_request)
        events = self._inflight_events(ticket)
        for name, kind in (("resumed", "fetch test barrier resumed"),
                           ("discarded", "fetch callback discarded"),
                           ("released", "fetch context released")):
            assert kind in events, f"Missing held fetch event: {kind}"
            ticket[name] = events[kind]
        path = ticket["held"]["path"]
        assert (ticket["registered"]["offset"] < ticket["held"]["offset"] <
                self.cancel_file_offsets[path] <=
                self.inflight_release_offsets[path] <=
                ticket["resumed"]["offset"] <
                ticket["discarded"]["offset"] < ticket["released"]["offset"]
                ), "Invalid held fetch cancellation/release order"
        root = self._detached_inflight_root(proof)
        exchange = next(item for item in root["exchanges"]
                        if item["exchange_id"] == ticket["exchange_id"] and
                        item["block_id"] == ticket["block_id"])
        assert (self.cancel_file_offsets[path] <=
                exchange["finalized"]["offset"] <
                self.inflight_release_offsets[path]), (
            "Held fetch Exchange did not finalize before barrier release"
        )
        proof.update(cancel_mode="inflight", held_request=ticket,
                     release_boundary=dict(self.inflight_release_offsets))
        return proof

    def _match_root(self, kind):
        assert kind in ("asof", "window"), "Unsupported temporal match kind"
        assert len(self.roots) == 1, "Missing or retried temporal root task"
        key, plan = next(iter(self.roots.items()))
        joins = [(name, body) for name, body in self.nodes(plan["RootNode"])
                 if "Join" in name]
        assert len(joins) == 1, "Expected one temporal root JOIN"
        name, join = joins[0]
        assert name == "PhysiMergeJoin" and int(join["SubType"]) == (
            4 if kind == "asof" else 5), "Temporal match plan kind differs"
        descriptor = join["OutputDataBlockDesc"]["DataBlockDesc"]
        block = int(descriptor["DataBlockId"])
        assert block >= 0, "Invalid temporal match output block"
        assert len(self.plans) == 5, "Incomplete temporal match task plan"
        assert [planned for planned in self.plans
                if (planned[0], planned[3]) == (key[0], key[3])] == [key], (
            "Ambiguous temporal match task identity"
        )
        return key, block

    def _match_selected_events(self):
        ticket = self.match_request
        result = {}
        for event in self.match_events:
            assert event["kind"] != "timed out", "Temporal match gate timed out"
            assert (event["query_id"], event["task_id"]) == (
                ticket["key"][0], ticket["key"][3]), (
                "Temporal match state belongs to a different task"
            )
            assert event["join_kind"] == ticket["kind"], (
                "Temporal match event kind changed"
            )
            assert event["fields"].get("blockId") == ticket["block_id"], (
                "Temporal match output block changed"
            )
            assert event["path"] == ticket["held"]["path"], (
                "Temporal match state spans server logs"
            )
            result.setdefault(event["kind"], []).append(event)
        for name, events in result.items():
            assert len(events) == 1 or name == "progress", (
                f"Repeated temporal match event: {name}"
            )
        return result

    def _pending_match(self):
        events = self._match_selected_events()
        assert set(events) == {"held"}, "Temporal match gate already resumed"
        assert not any(event["key"] == self.match_request["key"] and
                       event["line"].endswith(" task ctx dropped")
                       for event in self.events), (
            "Temporal match root dropped before gate release"
        )
        return events["held"][0]

    def wait_match_held(self, kind, timeout=15):
        """Observe a partially emitted temporal group with retained matches."""
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            assert not any(event["kind"] == "timed out"
                           for event in self.match_events), (
                "Temporal match gate timed out"
            )
            held = [event for event in self.match_events
                    if event["kind"] == "held"]
            assert len(held) <= 1, "Multiple held temporal match states"
            if self.roots and held:
                key, block = self._match_root(kind)
                event = held[0]
                state = event["fields"]
                assert set(state) == set(self.match_fields), (
                    "Incomplete temporal match state"
                )
                assert state["rows"] > 0 and state["totalRows"] >= state[
                    "rows"], "Temporal match has not emitted output"
                assert state["grpRemains"] == 1 and state["done"] == 0, (
                    "Temporal match group is complete"
                )
                assert 0 <= state["probeBegin"] <= state["probeRead"] <= (
                    state["probeEnd"]), "Temporal match probe cursor is done"
                assert all(state[name] >= 0 for name in (
                    "cacheRows", "cacheOutRows", "cacheGroups", "cacheRead"
                )), "Invalid temporal match cache state"
                assert 0 < state["candidates"] <= max(state["cacheRows"],
                                                      state["cacheOutRows"]), (
                    "Temporal match lacks retained candidates"
                )
                self.match_request = {
                    "key": key, "kind": kind, "block_id": block,
                    "held": self._async_proof(event, key),
                }
                self.match_planned = frozenset(self.plans)
                self._pending_match()
                return dict(self.match_request)
            if time.monotonic() >= deadline:
                raise AssertionError("Missing held temporal match group")
            time.sleep(0.02)

    def _mark_match_action(self, action):
        assert getattr(self, "match_request", None), (
            "Call wait_match_held first"
        )
        self._read_session_mode()
        self.poll()
        self._pending_match()
        assert frozenset(self.plans) == self.match_planned, (
            "Temporal match physical tasks changed"
        )
        self.paused = []
        self._freeze_cancel_state()
        held = self.match_request["held"]
        assert self.cancel_tasks[self.match_request["key"]]["path"] == (
            held["path"]), "Temporal match task registration spans server logs"
        assert held["offset"] < self.cancel_file_offsets[held["path"]], (
            "Held temporal match is outside the action boundary"
        )
        self.match_action = action

    def mark_match_resume(self):
        """Freeze the live temporal group before allowing normal execution."""
        self._mark_match_action("resume")

    def mark_match_cancel(self):
        """Freeze the running temporal group before issuing cancellation."""
        self._mark_match_action("kill")

    def mark_match_release(self):
        """A running operator keeps its task alive until this gate resumes."""
        assert getattr(self, "match_action", None), "Mark match action first"
        self.poll()
        self._pending_match()
        assert frozenset(self.plans) == self.match_planned, (
            "Temporal match physical tasks changed"
        )
        self.match_release_offsets = {
            str(state["path"]): state["offset"] for state in self.files
            if state["role"] == "SERVER"}

    def _check_match_finished(self, action, timeout):
        assert getattr(self, "match_action", None) == action, (
            "Unexpected temporal match action"
        )
        assert getattr(self, "match_release_offsets", None), (
            "Mark temporal match release before opening the gate"
        )
        proof = self.check_cancelled(timeout)
        events = self._match_selected_events()
        assert {"held", "resumed", "destroyed"} <= set(events), (
            "Incomplete temporal match lifetime"
        )
        held, resumed, destroyed = (events[name][0]
                                   for name in ("held", "resumed", "destroyed"))
        assert held["fields"] == resumed["fields"], (
            "Temporal match state changed while held"
        )
        path = held["path"]
        assert (held["offset"] < self.cancel_file_offsets[path] <=
                self.match_release_offsets[path] <= resumed["offset"] <
                destroyed["offset"]), "Invalid temporal match gate ordering"
        previous = resumed
        for progress in events.get("progress", []):
            assert previous["offset"] < progress["offset"] < (
                destroyed["offset"]), "Invalid temporal match progress order"
            assert progress["fields"].get("rows", 0) > 0 and (
                progress["fields"].get("totalRows", 0) >
                previous["fields"]["totalRows"]), (
                "Temporal match did not produce more output"
            )
            previous = progress
        assert destroyed["fields"].get("totalRows", -1) >= (
            previous["fields"]["totalRows"]), (
            "Temporal match row count regressed"
        )
        if action == "resume":
            assert events.get("progress"), "No temporal match continuation"
        root = next(task for task in proof["tasks"]
                    if task["key"] == self.match_request["key"])
        assert destroyed["offset"] < root["dropped"]["offset"], (
            "Temporal JOIN state destroyed after task drop"
        )
        ticket = dict(self.match_request)
        for name, event in (("resumed", resumed), ("destroyed", destroyed)):
            ticket[name] = self._async_proof(event, ticket["key"])
        if events.get("progress"):
            ticket["progress"] = self._async_proof(events["progress"][0],
                                                   ticket["key"])
        proof.update(cancel_mode="match-" + action, match_state=ticket,
                     release_boundary=dict(self.match_release_offsets))
        return proof

    def check_match_resumed(self, timeout=15):
        """Require more matched output and full cleanup after normal fetch."""
        return self._check_match_finished("resume", timeout)

    def check_match_cancelled(self, timeout=15):
        """Require operator and task cleanup after cancelling held matches."""
        return self._check_match_finished("kill", timeout)

    def _timeout_root(self, kind):
        assert kind in ("hash", "merge", "asof", "window"), kind
        assert len(self.roots) == 1, "Missing or retried timeout root task"
        key, plan = next(iter(self.roots.items()))
        joins = [(name, body) for name, body in self.nodes(plan["RootNode"])
                 if "Join" in name]
        assert len(joins) == 1, "Expected one timeout root JOIN"
        name, join = joins[0]
        if kind == "hash":
            assert name == "PhysiHashJoin", "Timeout plan algorithm differs"
        else:
            assert name == "PhysiMergeJoin", "Timeout plan algorithm differs"
            subtype = int(join.get("SubType", 0))
            if kind in ("asof", "window"):
                assert subtype == (4 if kind == "asof" else 5), (
                    "Timeout temporal plan kind differs"
                )
            else:
                assert subtype not in (4, 5), "Timeout merge plan differs"
        node = plan["RootNode"]
        body = node[node["Name"]]
        block = int(body["OutputDataBlockDesc"]["DataBlockDesc"][
            "DataBlockId"])
        assert block >= 0 and len(self.plans) == 5, (
            "Incomplete timeout task plan"
        )
        assert [planned for planned in self.plans
                if (planned[0], planned[3]) == (key[0], key[3])] == [key], (
            "Ambiguous timeout root identity"
        )
        return key, block

    def _timeout_selected_events(self):
        ticket = self.timeout_request
        events = {}
        for event in self.timeout_events:
            assert event["kind"] != "timed out", "Query timeout gate expired"
            assert (event["query_id"], event["task_id"]) == (
                ticket["key"][0], ticket["key"][3]), (
                "Query timeout event belongs to a different task"
            )
            assert event["path"] == ticket["held"]["path"], (
                "Query timeout gate spans server logs"
            )
            assert event["fields"].get("blockId") == ticket["block_id"], (
                "Query timeout output block changed"
            )
            assert event["kind"] not in events, (
                "Repeated query timeout gate event"
            )
            events[event["kind"]] = event
        return events

    def _pending_timeout(self):
        events = self._timeout_selected_events()
        assert set(events) == {"held"}, "Query timeout gate already resumed"
        assert not any(event["key"] == self.timeout_request["key"] and
                       event["line"].endswith(" task ctx dropped")
                       for event in self.events), (
            "Query timeout root dropped before gate release"
        )

    def wait_timeout_held(self, kind, timeout=15):
        """Observe an executing root whose response remains withheld."""
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            assert not any(event["kind"] == "timed out"
                           for event in self.timeout_events), (
                "Query timeout gate expired"
            )
            held = [event for event in self.timeout_events
                    if event["kind"] == "held"]
            assert len(held) <= 1, "Multiple query timeout gates"
            if self.roots and held:
                key, block = self._timeout_root(kind)
                event = held[0]
                fields = event["fields"]
                assert set(fields) == {"blockId", "rows", "readTimeout"}
                assert fields["rows"] > 0 and fields["readTimeout"] == 64, (
                    "Invalid query timeout gate state"
                )
                self.timeout_request = {
                    "key": key, "kind": kind, "block_id": block,
                    "read_timeout": fields["readTimeout"],
                    "held": self._async_proof(event, key),
                }
                self.timeout_planned = frozenset(self.plans)
                self._pending_timeout()
                return dict(self.timeout_request)
            if time.monotonic() >= deadline:
                raise AssertionError("Missing held query timeout root")
            time.sleep(0.02)

    def mark_timeout(self):
        """Freeze live resources before awaiting the configured RPC timeout."""
        assert getattr(self, "timeout_request", None), (
            "Call wait_timeout_held first"
        )
        self._read_session_mode()
        self.poll()
        self._pending_timeout()
        assert frozenset(self.plans) == self.timeout_planned
        self.paused = []
        self._freeze_cancel_state()
        held = self.timeout_request["held"]
        assert self.cancel_tasks[self.timeout_request["key"]]["path"] == (
            held["path"]), "Timeout task registration spans server logs"
        assert held["offset"] < self.cancel_file_offsets[held["path"]]

    def _rpc_timeout_evidence(self):
        root = self.timeout_request["key"]
        scheduler = []
        for event in self.timeout_client_events:
            if self.key(event["line"]) != root:
                continue
            match = re.search(
                r"\bstart to send (query|merge-query|fetch|merge-fetch) msg "
                r".*\bpHandle:(0x[\da-fA-F]+|[\da-fA-F]+)(?:,|\s|$)",
                event["line"])
            if match is not None:
                scheduler.append((event, match[1], int(match[2], 16)))
        candidates = []
        for sent, message, handle in scheduler:
            if handle <= 0:
                continue
            transport = []
            for event in self.timeout_client_events:
                if event["path"] != sent["path"] or event["offset"] <= (
                        sent["offset"]):
                    continue
                match = re.search(
                    r"\bconn:(0x[\da-fA-F]+|[\da-fA-F]+), "
                    + re.escape(message) + r" is sent to .*\bsid:(\d+), "
                    r"QID:(0x[\da-fA-F]+):0x[\da-fA-F]+\s*$",
                    event["line"])
                if match is not None and int(match[2]) == handle and int(
                        match[3], 16) == self.query_id:
                    transport.append((event, int(match[1], 16)))
            for event, conn in transport:
                timers = []
                for timer in self.timeout_client_events:
                    if timer["path"] != event["path"] or timer["offset"] <= (
                            event["offset"]):
                        continue
                    match = re.search(
                        r"\bconn:(0x[\da-fA-F]+|[\da-fA-F]+), "
                        r"remove timeout msg sid:(\d+)\s*$", timer["line"])
                    if match is not None and int(match[1], 16) == conn and (
                            int(match[2]) == handle):
                        timers.append(timer)
                assert len(timers) <= 1, "RPC timeout handle expired twice"
                if timers:
                    candidates.append((sent, event, timers[0], handle, conn))
        if not candidates:
            return None
        # A persistent root handle can carry QUERY and then FETCH; its last
        # outgoing request is the one still waiting when the timer removes it.
        candidates.sort(key=lambda item: item[1]["offset"])
        sent, transport, timer, sid, conn = candidates[-1]
        assert all(item[3:] == (sid, conn) and item[0]["path"] == sent["path"]
                   for item in candidates), "Ambiguous RPC timeout identity"
        failed = []
        for event in self.timeout_client_events:
            if event["path"] != sent["path"] or event["offset"] <= (
                    timer["offset"]):
                continue
            match = re.search(
                r"\bQID:(0x[\da-fA-F]+), SUBID:-1, SID:(\d+), "
                r"job failed with error Conn read timeout\s*$", event["line"])
            if match is not None and (int(match[1], 16), int(match[2])) == (
                    root[0], root[1]):
                failed.append(event)
        assert len(failed) <= 1, "Repeated timeout job failure"
        if not failed:
            return None
        return {"scheduler_sent": dict(sent), "transport_sent": dict(transport),
                "timer_removed": dict(timer), "job_failed": dict(failed[0]),
                "sid": sid, "conn": conn}

    def wait_rpc_timeout(self, timeout=15):
        """Require the real transport timer and its precise job error."""
        assert getattr(self, "timeout_request", None), "No held timeout root"
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            self._pending_timeout()
            proof = self._rpc_timeout_evidence()
            if proof is not None:
                self.rpc_timeout_request = proof
                return dict(proof)
            if time.monotonic() >= deadline:
                raise AssertionError("Missing real RPC read timeout evidence")
            time.sleep(0.02)

    def wait_rpc_drop(self, timeout=15):
        """Observe normal failed-result disposal sending the existing DROP."""
        assert getattr(self, "rpc_timeout_request", None), (
            "Require the RPC timeout before disposing its result"
        )
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            self._pending_timeout()
            request = self.rpc_timeout_request
            root = self.timeout_request["key"]
            events = [event for event in self.timeout_client_events
                      if self.key(event["line"]) == root and
                      event["path"] == request["job_failed"]["path"] and
                      event["line"].endswith(
                          "clear refId before send drop-task msg, "
                          "code:Conn read timeout")]
            assert len(events) <= 1, "Repeated timeout DROP"
            if events:
                assert request["job_failed"]["offset"] < events[0]["offset"]
                request["drop_sent"] = dict(events[0])
                return dict(request)
            if time.monotonic() >= deadline:
                raise AssertionError("Missing DROP after RPC timeout result")
            time.sleep(0.02)

    def mark_timeout_release(self):
        """Release the live server worker after the original result is freed."""
        assert "drop_sent" in getattr(self, "rpc_timeout_request", {}), (
            "Wait for the failed result's DROP before releasing the worker"
        )
        self.poll()
        self._pending_timeout()
        assert frozenset(self.plans) == self.timeout_planned
        self.timeout_release_offsets = {
            str(state["path"]): state["offset"] for state in self.files
            if state["role"] == "SERVER"}
        self.timeout_client_release_offsets = {
            str(state["path"]): state["offset"] for state in self.files
            if state["role"] == "CLIENT"}
        dropped = self.rpc_timeout_request["drop_sent"]
        assert dropped["offset"] < self.timeout_client_release_offsets[
            dropped["path"]], "Timeout DROP is after the release boundary"

    def check_timeout_cleaned(self, timeout=15):
        """Require worker continuation and complete timeout-result cleanup."""
        assert getattr(self, "timeout_release_offsets", None), (
            "Mark timeout release before opening the gate"
        )
        proof = self.check_cancelled(timeout)
        events = self._timeout_selected_events()
        assert set(events) == {"held", "resumed"}, (
            "Incomplete query timeout gate lifetime"
        )
        held, resumed = events["held"], events["resumed"]
        assert held["fields"] == resumed["fields"], (
            "Query timeout state changed while held"
        )
        path = held["path"]
        assert (held["offset"] < self.cancel_file_offsets[path] <=
                self.timeout_release_offsets[path] <= resumed["offset"]), (
            "Invalid query timeout release order"
        )
        root = next(task for task in proof["tasks"]
                    if task["key"] == self.timeout_request["key"])
        assert resumed["offset"] < root["dropped"]["offset"], (
            "Query timeout task dropped before worker resumed"
        )
        ticket = dict(self.timeout_request)
        ticket["resumed"] = self._async_proof(resumed, ticket["key"])
        proof.update(cancel_mode="rpc-timeout", timeout_state=ticket,
                     timeout_request=dict(self.rpc_timeout_request),
                     release_boundary=dict(self.timeout_release_offsets),
                     client_release_boundary=dict(
                         self.timeout_client_release_offsets))
        return proof

    def _failure_inputs(self, kind):
        root, _ = self._timeout_root(kind)
        joins = [body for name, body in
                 self.nodes(self.plans[root]["RootNode"]) if "Join" in name]
        children = joins[0].get("Children", [])
        assert len(children) == 2, "Producer failure requires two JOIN inputs"
        inputs = []
        for child in children:
            exchanges = [body for name, body in self.nodes(child)
                         if name == "PhysiExchange"]
            assert len(exchanges) == 1, "Ambiguous root input Exchange"
            exchange = exchanges[0]
            sources = exchange.get("SrcEndPoints", [])
            assert len(sources) == 1, "Ambiguous root input producer"
            source = sources[0]["DownstreamSource"]
            assert not source.get("LocalExec", False)
            key = (int(source["ProducerQueryId"]), int(source["SeriousId"]),
                   int(source["ClientId"]) & ((1 << 64) - 1),
                   int(source["TaskId"]), int(source["ExecId"]))
            assert key in self.plans and key != root, (
                "Root input has no matching producer task"
            )
            plan = self.plans[key]
            assert int(plan["Level"]) == 1 and sum(
                "Join" in name for name, _ in
                self.nodes(plan["RootNode"])) == 1, (
                "Root input is not a single producer JOIN"
            )
            node = plan["RootNode"]
            inputs.append({
                "key": key,
                "exchange_block": int(exchange["OutputDataBlockDesc"][
                    "DataBlockDesc"]["DataBlockId"]),
                "producer_block": int(node[node["Name"]][
                    "OutputDataBlockDesc"]["DataBlockDesc"]["DataBlockId"]),
            })
        assert inputs[0]["key"] != inputs[1]["key"], (
            "Both JOIN sides reference the same producer"
        )
        return root, inputs

    def _failure_selected_events(self):
        ticket = self.failure_request
        inputs = {item["key"]: item for item in ticket["inputs"]}
        identities = {(key[0], key[3]): key for key in inputs}
        assert len(identities) == 2, "Ambiguous producer execution identity"
        selected = {key: {} for key in inputs}
        for event in self.failure_events:
            assert event["kind"] != "timed out", "Producer failure gate expired"
            key = identities.get((event["query_id"], event["task_id"]))
            assert key is not None, "Failure gate belongs to another task"
            fields = event["fields"]
            assert set(fields) == {"blockId", "rows", "code"}, event
            assert fields["blockId"] == inputs[key]["producer_block"] and (
                fields["rows"] > 0), "Invalid producer failure output state"
            expected = -2147481806 if event["kind"] == "injected" else 0
            assert fields["code"] == expected, "Wrong producer failure code"
            events = selected[key]
            assert event["kind"] not in events, "Repeated producer gate event"
            held = events.get("held")
            if event["kind"] != "held":
                assert held is not None and held["path"] == event["path"]
                assert held["offset"] < event["offset"]
                assert all(held["fields"][name] == fields[name]
                           for name in ("blockId", "rows")), (
                    "Producer output changed while held"
                )
            events[event["kind"]] = event
        return selected

    def _pending_failure_peer(self):
        ticket = self.failure_request
        assert frozenset(self.plans) == self.failure_planned, (
            "Producer failure task identities changed"
        )
        assert self._failure_inputs(ticket["kind"])[1] == ticket["inputs"], (
            "Producer failure input bindings changed"
        )
        events = self._failure_selected_events()
        peer = ticket["peer_key"]
        assert set(events[peer]) == {"held"}, (
            "Other producer resumed before failure evidence"
        )
        assert not any(event["key"] == peer and any(
            marker in event["line"] for marker in (
                "task ctx dropped", "task session destroyed",
                "qExecTask failed")) for event in self.events), (
            "Other producer stopped before failure evidence"
        )
        assert not any(event["task_id"] == peer[3] and
                       event["kind"] == "exchange resources released"
                       for event in self.async_events), (
            "Other producer released its Exchange before failure evidence"
        )
        return events

    def wait_failure_held(self, kind, side, timeout=15):
        """Bind held JOIN producers to the root's actual left/right inputs."""
        assert side in ("left", "right"), side
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            assert len(self.plans) <= 5 and len(self.roots) <= 1, (
                "Producer failure task topology changed"
            )
            assert not any(event["kind"] == "timed out"
                           for event in self.failure_events), (
                "Producer failure gate expired"
            )
            if self.roots and len(self.plans) == 5:
                root, inputs = self._failure_inputs(kind)
                target = inputs[0 if side == "left" else 1]["key"]
                peer = inputs[1 if side == "left" else 0]["key"]
                self.failure_request = {
                    "key": root, "kind": kind, "side": side,
                    "inputs": inputs, "target_key": target, "peer_key": peer,
                }
                self.failure_planned = frozenset(self.plans)
                selected = self._failure_selected_events()
                if all(set(events) == {"held"}
                       for events in selected.values()):
                    self._pending_failure_peer()
                    registered = [event["key"] for event in self.events
                                  if "task preprocess succeed," in
                                  event["line"]]
                    expected = self._planned_exchange_blocks(self.plans)
                    created = [(event["task_id"], event["fields"].get(
                        "blockId")) for event in self.async_events
                        if event["kind"] == "exchange lifecycle created"]
                    planned = [(key[3], block) for key, blocks in
                               expected.items() for block in blocks]
                    if (sorted(registered) == sorted(self.plans) and
                            sorted(created) == sorted(planned)):
                        assert len(planned) == 4, (
                            "Producer failure requires four Exchanges"
                        )
                        for name, key in (("target_held", target),
                                          ("peer_held", peer)):
                            self.failure_request[name] = self._async_proof(
                                selected[key]["held"], key)
                        return dict(self.failure_request)
                else:
                    assert all(set(events) <= {"held"}
                               for events in selected.values()), (
                        "Producer gate exited before failure snapshot"
                    )
            if time.monotonic() >= deadline:
                raise AssertionError("Missing two held producer JOIN tasks")
            time.sleep(0.02)

    def mark_failure(self):
        """Freeze the complete live DAG before injecting one producer error."""
        assert "target_held" in getattr(self, "failure_request", {}), (
            "Call wait_failure_held first"
        )
        self._read_session_mode()
        self.poll()
        selected = self._pending_failure_peer()
        assert set(selected[self.failure_request["target_key"]]) == {"held"}
        assert not any(any(marker in event["line"] for marker in (
            "task ctx dropped", "task session destroyed"))
            for event in self.events), "Task released before failure snapshot"
        self.paused = []
        self._freeze_cancel_state()
        groups = self._exchange_groups(self._cancel_groups(self.plans))
        assert all(state["finalized"] is None for blocks in groups.values()
                   for state in blocks.values()), (
            "Exchange released before producer failure snapshot"
        )
        for name in ("target_held", "peer_held"):
            held = self.failure_request[name]
            assert held["path"] == self.cancel_tasks[held["key"]]["path"], (
                "Producer gate and task registration span server logs"
            )
            assert held["offset"] < self.cancel_file_offsets[held["path"]]

    def _failure_exec_event(self, key):
        events = [event for event in self.events if event["key"] == key and
                  "qExecTask failed," in event["line"]]
        assert len(events) <= 1, "Repeated failed producer/root execution"
        if not events:
            return None
        assert re.search(r"qExecTask failed, code:(?:0x)?80000732 - "
                         r"Executor internal error\s*$", events[0]["line"],
                         re.IGNORECASE), "Wrong execution failure code"
        return events[0]

    def wait_failure_injected(self, timeout=15):
        """Require a real producer execution error while its peer stays live."""
        assert getattr(self, "cancel_tasks", None), (
            "Mark producer failure first"
        )
        deadline = time.monotonic() + timeout
        target = self.failure_request["target_key"]
        while True:
            self.poll()
            selected = self._pending_failure_peer()[target]
            assert set(selected) <= {"held", "injected"}, (
                "Failed producer resumed normally"
            )
            failed = self._failure_exec_event(target)
            if "injected" in selected and failed is not None:
                injected = selected["injected"]
                assert injected["path"] == failed["path"] and (
                    self.cancel_file_offsets[injected["path"]] <=
                    injected["offset"] < failed["offset"]), (
                    "Producer execution failed before injection"
                )
                self.failure_request.update(
                    injected=self._async_proof(injected, target),
                    target_exec_failed=dict(failed))
                return dict(self.failure_request)
            if time.monotonic() >= deadline:
                raise AssertionError(
                    "Missing injected producer execution error")
            time.sleep(0.02)

    def mark_failure_release(self):
        """Release the surviving producer after recording the real failure."""
        assert "target_exec_failed" in getattr(self, "failure_request", {}), (
            "Observe injected producer failure before releasing its peer"
        )
        self.poll()
        self._pending_failure_peer()
        self.failure_release_offsets = {
            str(state["path"]): state["offset"] for state in self.files
            if state["role"] == "SERVER"}
        self.failure_client_release_offsets = {
            str(state["path"]): state["offset"] for state in self.files
            if state["role"] == "CLIENT"}
        failed = self.failure_request["target_exec_failed"]
        assert failed["offset"] < self.failure_release_offsets[failed["path"]]

    def _failure_propagated(self):
        ticket = self.failure_request
        root, peer = ticket["key"], ticket["peer_key"]
        selected = self._failure_selected_events()
        assert set(selected[ticket["target_key"]]) == {"held", "injected"}
        assert set(selected[peer]) <= {"held", "resumed"}
        resumed = selected[peer].get("resumed")
        if resumed is not None:
            assert self.failure_release_offsets[resumed["path"]] <= (
                resumed["offset"]), "Other producer resumed before release"
        exchange = [event for event in self.failure_exchange_events
                    if (event["query_id"], event["task_id"]) ==
                    (root[0], root[3])]
        assert len(exchange) <= 1, "Repeated root Exchange error"
        if exchange:
            assert exchange[0]["code"] == -2147481806 and (
                exchange[0]["source_index"] == 0), (
                "Root Exchange received a different error"
            )
        failed = self._failure_exec_event(root)
        jobs = []
        for event in self.failure_client_events:
            match = re.search(
                r"QID:(0x[\da-fA-F]+), SUBID:-1, SID:(\d+), "
                r"job failed with error (.+?)\s*$",
                event["line"])
            if match is None or int(match[1], 16) != root[0]:
                continue
            assert int(match[2]) == root[1], "Failure job identity changed"
            assert match[3] == "Executor internal error", (
                "Failure job reported a different error"
            )
            if jobs:
                assert jobs[0]["path"] == event["path"] and (
                    jobs[-1]["offset"] < event["offset"]), (
                    "Failure job spans client logs or regressed"
                )
            jobs.append(event)
        if resumed is None or not exchange or failed is None or not jobs:
            return None
        callback = exchange[0]
        assert callback["path"] == failed["path"] and (
            self.cancel_file_offsets[callback["path"]] <=
            callback["offset"] < failed["offset"]), (
            "Root failed before receiving producer error"
        )
        origin = ticket["target_exec_failed"]
        if origin["path"] == callback["path"]:
            assert origin["offset"] < callback["offset"], (
                "Root received error before producer failed"
            )
        ticket.update(peer_resumed=self._async_proof(resumed, peer),
                      root_exchange_error=self._async_proof(callback, root),
                      root_exec_failed=dict(failed), job_failed=dict(jobs[0]))
        return dict(ticket)

    def wait_failure_propagated(self, timeout=15):
        """Follow the original producer error through root execution to job."""
        assert getattr(self, "failure_release_offsets", None), (
            "Mark surviving producer release first"
        )
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            proof = self._failure_propagated()
            if proof is not None:
                return proof
            if time.monotonic() >= deadline:
                raise AssertionError(
                    "Missing original producer error propagation")
            time.sleep(0.02)

    def wait_failure_drop(self, timeout=15):
        """Observe scheduler DROP after normal disposal of the failed result."""
        assert "job_failed" in getattr(self, "failure_request", {}), (
            "Observe propagated producer error before disposing its result"
        )
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            ticket = self.failure_request
            job = ticket["job_failed"]
            dropped = [event for event in self.failure_client_events
                       if self.key(event["line"]) == ticket["key"] and
                       "start to send drop-task msg" in event["line"]]
            assert len(dropped) <= 1, "Repeated failed result DROP"
            if dropped:
                event = dropped[0]
                assert event["path"] == job["path"] and (
                    job["offset"] < event["offset"]), (
                    "Failed result DROP precedes the original job error"
                )
                ticket["drop_sent"] = dict(event)
                return dict(ticket)
            if time.monotonic() >= deadline:
                raise AssertionError(
                    "Missing DROP after failed result disposal")
            time.sleep(0.02)

    def check_failure_cleaned(self, timeout=15):
        """Require both producer lifetimes and complete old-DAG reclamation."""
        assert "drop_sent" in getattr(self, "failure_request", {}), (
            "Observe normal failed-result disposal before cleanup"
        )
        proof = self.check_cancelled(timeout)
        ticket = self._failure_propagated()
        assert ticket is not None, "Incomplete producer error propagation"
        peer = next(task for task in proof["tasks"]
                    if task["key"] == ticket["peer_key"])
        resumed = ticket["peer_resumed"]
        assert resumed["path"] == peer["dropped"]["path"] and (
            resumed["offset"] < peer["dropped"]["offset"]), (
            "Other producer dropped before resuming"
        )
        proof.update(cancel_mode="producer-failure", failure_state=ticket,
                     release_boundary=dict(self.failure_release_offsets),
                     client_release_boundary=dict(
                         self.failure_client_release_offsets))
        return proof


class JoinRetryLogs(JoinRuntimeLogs):
    """Separate two executions using task keys and Exchange instance IDs."""

    def poll(self):
        super().poll()
        assert len(self.plans) <= 10 and len(self.roots) <= 2, (
            "Expected exactly one whole-job retry"
        )
        assert not any(event["kind"] == "fetch retry test timed out"
                       for event in self.async_events), "Retry gate expired"
        for event in self.retry_client_events:
            if "job failed with error " not in event["line"]:
                continue
            boundary = getattr(self, "result_free_boundary", {})
            assert event["path"] in boundary and (
                event["offset"] >= boundary[event["path"]]), (
                "Retry exposed a terminal error before result disposal"
            )
            assert re.search(
                r"SUBID:-1, SID:" + str(self.new_root[1]) +
                r", job failed with error Query killed\s*$", event["line"]
            ), "Unexpected terminal error after retry"

    def _owners(self):
        registered = [event for event in self.events
                      if "task preprocess succeed," in event["line"]]
        owners = {}
        for event in self.async_events:
            if event["kind"] != "exchange lifecycle created":
                continue
            candidates = [item for item in registered
                          if item["path"] == event["path"] and
                          item["key"][3] == event["task_id"] and
                          item["offset"] < event["offset"]]
            assert candidates, "Exchange has no registered execution"
            owner = max(candidates, key=lambda item: item["offset"])["key"]
            instance = event["path"], event["fields"]["exchangeId"]
            assert instance not in owners, "Exchange instance reused"
            owners[instance] = owner
        return owners

    def _refresh_view(self, view, keys):
        self.poll()
        owners = self._owners()
        view.files = self.files
        view.plans = {key: self.plans[key] for key in keys}
        view.roots = {key: self.roots[key] for key in keys if key in self.roots}
        view.events = [event for event in self.events if event["key"] in keys]
        view.async_events = []
        for event in self.async_events:
            instance = event["path"], event["fields"].get("exchangeId")
            assert instance in owners, "Unowned Exchange or fetch event"
            if owners[instance] in keys:
                view.async_events.append(event)

    def _view(self, keys):
        view = copy.copy(self)
        view.poll = lambda: self._refresh_view(view, keys)
        view.poll()
        return view

    def _gate(self, kind, key=None):
        events = [event for event in self.async_events
                  if event["kind"] == kind]
        assert len(events) <= 1, "Repeated retry gate event"
        if not events:
            return None
        event = events[0]
        owner = self._owners()[event["path"], event["fields"]["exchangeId"]]
        assert key is None or owner == key, "Retry gate execution differs"
        proof = self._async_proof(event, owner)
        proof["fields"] = dict(event["fields"])
        return proof

    @staticmethod
    def _check_links(view, kind):
        root, _ = view._failure_inputs(kind)
        keys = set(view.plans)
        assert len(keys) == 5
        bindings = []
        for consumer, plan in view.plans.items():
            for name, body in view.nodes(plan["RootNode"]):
                if name != "PhysiExchange":
                    continue
                for value in body["SrcEndPoints"]:
                    source = value["DownstreamSource"]
                    key = (int(source["ProducerQueryId"]),
                           int(source["SeriousId"]),
                           int(source["ClientId"]) & ((1 << 64) - 1),
                           int(source["TaskId"]), int(source["ExecId"]))
                    assert key in keys, "Exchange references another execution"
                    bindings.append({
                        "consumer": consumer, "producer": key,
                        "block_id": int(body["OutputDataBlockDesc"][
                            "DataBlockDesc"]["DataBlockId"]),
                    })
        view.retry_bindings = bindings
        return root

    def _freeze(self, view, kind):
        root = self._check_links(view, kind)
        assert not any("task ctx dropped" in event["line"] or
                       "task session destroyed" in event["line"]
                       for event in view.events), "Execution already dropped"
        view.paused = []
        view._read_session_mode()
        view._freeze_cancel_state()
        states = view._exchange_groups(view._cancel_groups(view.plans))
        assert sum(len(blocks) for blocks in states.values()) == 4
        assert all(state["finalized"] is None for state in states[root].values()
                   ), "Root Exchange released before the retry checkpoint"
        return root

    def wait_retry_held(self, kind, timeout=15):
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            held = self._gate("fetch retry test held")
            if held is not None and len(self.plans) == 5:
                self.old_keys = frozenset(self.plans)
                self.old_view = self._view(self.old_keys)
                root = self._freeze(self.old_view, kind)
                assert tuple(held["key"]) == root
                self.kind, self.old_root = kind, root
                self.old_held = held
                return held
            if time.monotonic() >= deadline:
                raise AssertionError("Missing held original execution")
            time.sleep(0.02)

    def wait_retry_started(self, timeout=15):
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            new_keys = frozenset(self.plans) - self.old_keys
            if len(new_keys) == 5:
                candidate = self._view(new_keys)
                roots = list(candidate.roots)
                registered = [event for event in candidate.async_events
                              if event["kind"] == "fetch context registered"
                              and event["task_id"] == roots[0][3]]
                created = [event for event in candidate.async_events
                           if event["kind"] == "exchange lifecycle created"]
                if len(created) != 4 or not registered:
                    assert len(created) <= 4
                    if time.monotonic() >= deadline:
                        raise AssertionError("Missing retried Exchange DAG")
                    time.sleep(0.02)
                    continue
                old = {key[3]: key for key in self.old_keys}
                assert set(old) == {key[3] for key in new_keys}
                for key in new_keys:
                    previous = old[key[3]]
                    assert (key[0], key[2]) == (previous[0], previous[2])
                    assert key[1] == previous[1] + 1, "Series did not advance"
                    assert key[4] == previous[4] + 1, (
                        "Execution did not advance"
                    )
                self.new_keys = new_keys
                self.new_view = self._view(new_keys)
                self.new_root = self._freeze(self.new_view, self.kind)
                self.next_request = self._async_proof(registered[0],
                                                       self.new_root)
                self.next_request["fields"] = dict(registered[0]["fields"])
                injected = self._gate("fetch retry test injected",
                                      self.old_root)
                assert injected is not None
                assert injected["path"] == self.old_held["path"]
                assert (self.old_view.cancel_file_offsets[injected["path"]]
                        <= injected["offset"] < self.next_request["offset"])
                return self.next_request
            if time.monotonic() >= deadline:
                raise AssertionError("Missing complete retried execution")
            time.sleep(0.02)

    def wait_old_detached(self, timeout=15):
        deadline = time.monotonic() + timeout
        while True:
            proof, missing, asynchronous = self.old_view._cleanup_snapshot()
            held = self.old_held
            block = held["fields"]["blockId"]
            fetch = held["fields"]["fetchId"]
            if not missing and asynchronous == [(self.old_root, block, fetch)]:
                self.old_detached = proof
                return proof
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"Old execution not detached: {missing}, {asynchronous}")
            time.sleep(0.02)

    def mark_old_release(self):
        self.wait_old_detached()
        assert self._gate("fetch retry next resumed", self.new_root) is None
        self.release_boundary = {
            str(state["path"]): state["offset"] for state in self.files
            if state["role"] == "SERVER"}

    def check_old_cleaned(self, timeout=15):
        old = self.old_view.check_cancelled(timeout)
        resumed = self._gate("fetch retry test resumed", self.old_root)
        assert resumed is not None
        held = self.old_held
        same = [event for event in self.async_events
                if event["fields"].get("exchangeId") ==
                held["fields"]["exchangeId"] and
                event["fields"].get("fetchId") == held["fields"]["fetchId"]]
        events = {}
        for marker in ("fetch callback discarded", "fetch context released"):
            matches = [event for event in same if event["kind"] == marker]
            assert len(matches) == 1, "Missing old reply discard or release"
            events[marker] = self._async_proof(matches[0], self.old_root)
        discarded, released = events.values()
        path = held["path"]
        assert all(event["path"] == path
                   for event in (resumed, discarded, released))
        assert (self.release_boundary[path] <= resumed["offset"] <
                discarded["offset"] < released["offset"])
        self.old_proof = old
        self.old_reply = {
            "held": held, "injected": self._gate(
                "fetch retry test injected", self.old_root),
            "resumed": resumed, "discarded": discarded, "released": released}
        return old

    def check_retry_cleaned(self, timeout=15):
        new = self.new_view.check_cancelled(timeout)
        self.poll()
        assert frozenset(self.plans) == self.old_keys | self.new_keys
        restart = [event for event in self.retry_client_events
                   if "start to restart all tasks by error:" in event["line"]]
        updated = [event for event in self.retry_client_events
                   if "update job sId:" in event["line"]]
        assert len(restart) == len(updated) == 1, "Missing one job restart"
        assert self.key(restart[0]["line"]) == self.old_root
        assert "Sync leader is restoring" in (
            restart[0]["line"]), "Wrong retry cause"
        assert restart[0]["path"] == updated[0]["path"] and (
            restart[0]["offset"] < updated[0]["offset"])
        assert re.search(r"update job sId:" + str(self.new_root[1]) + r",",
                         updated[0]["line"]), "Wrong restarted series"
        failed = [event for event in self.events if (
            event["key"] == self.old_root and
            "qExecTask failed, code:80000914 - " in event["line"])]
        assert len(failed) == 1, "Original root did not fail with retry cause"
        errors = [event for event in self.failure_exchange_events if (
            event["task_id"] == self.old_root[3] and
            event["code"] == -2147481324)]
        assert len(errors) == 1, "Missing retryable Exchange failure"
        dropped = [event for event in self.failure_client_events if (
            self.key(event["line"]) == self.new_root and
            "start to send drop-task msg" in event["line"])]
        assert len(dropped) == 1, "Missing normal disposal of retried result"
        assert dropped[0]["offset"] >= self.result_free_boundary[
            dropped[0]["path"]], "Retried result dropped before disposal"
        old = self.old_proof
        old["input_bindings"] = self.old_view.retry_bindings
        new["input_bindings"] = self.new_view.retry_bindings
        return {"old_execution": old, "new_execution": new,
                "reply": dict(self.old_reply, next_held=self.next_held,
                              next_registered=self.next_request,
                              next_resumed=self._gate(
                                  "fetch retry next resumed", self.new_root)),
                "restart": restart[0],
                "series_updated": updated[0],
                "root_failed": failed[0],
                "drop_sent": dropped[0],
                "exchange_error": self._async_proof(errors[0], self.old_root),
                "result_free_boundary": self.result_free_boundary,
                "next_release_boundary": self.next_release_boundary,
                "release_boundary": self.release_boundary}

    def wait_next_held(self, timeout=15):
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            held = self._gate("fetch retry next held", self.new_root)
            if held is not None:
                for name in ("exchangeId", "blockId", "fetchId",
                             "sourceIndex", "seqId"):
                    assert held["fields"][name] == (
                        self.next_request["fields"][name])
                self.next_held = held
                return held
            if time.monotonic() >= deadline:
                raise AssertionError("Retried fetch did not enter its gate")
            time.sleep(0.02)

    def mark_next_release(self):
        assert getattr(self, "old_proof", None)
        self.wait_next_held()
        self.next_release_boundary = {
            str(state["path"]): state["offset"] for state in self.files
            if state["role"] == "SERVER"}

    def mark_result_free(self):
        self.poll()
        self.result_free_boundary = {
            str(state["path"]): state["offset"] for state in self.files
            if state["role"] == "CLIENT"}


class JoinIdleLogs(JoinRuntimeLogs):
    """Prove server idle expiry while client result ownership is retained."""

    def mark_idle(self, kind):
        self.mark_cancel()
        self._timeout_root(kind)
        assert len(self.cancel_tasks) == 5
        self.idle_kind = kind
        self._quiet_client()

    def _quiet_client(self):
        assert not any("phase PRE_FETCH" in event["line"]
                       for event in self.root_events()), (
            "Root fetch must not refresh the idle deadline"
        )
        for event in self.failure_client_events:
            key = self.key(event["line"])
            query = re.search(r"\bQID:(0x[\da-fA-F]+),", event["line"])
            if query is not None and int(query[1], 16) == self.query_id:
                assert key not in self.cancel_tasks and (
                    "job failed with error " not in event["line"]), (
                    "Client stopped or dropped the query before idle cleanup"
                )

    def check_idle_cleaned(self, timeout=95):
        deadline = time.monotonic() + timeout
        while True:
            proof, missing, asynchronous = self._cleanup_snapshot()
            self._quiet_client()
            expired = {}
            for event in self.events:
                if "idle timeout test expired," not in event["line"]:
                    continue
                key = event["key"]
                assert key in self.cancel_tasks and key not in expired, (
                    "Idle expiry identity repeated or changed"
                )
                match = re.search(
                    r"idle timeout test expired, lastAckTs:(\d+), "
                    r"nowTs:(\d+), timeout:60, code:8000073b - "
                    r"Timeout for long time no fetch\s*$", event["line"])
                assert match is not None, "Wrong idle timeout cause"
                acknowledged, now = map(int, match.groups())
                assert acknowledged > 0 and now - acknowledged >= 60, event
                registered = self.cancel_tasks[key]
                assert registered["path"] == event["path"] and (
                    registered["offset"] < event["offset"]), event
                assert self.cancel_file_offsets[event["path"]] <= (
                    event["offset"]), "Task expired before the idle boundary"
                expired[key] = event
            if not missing and not asynchronous:
                assert set(expired) == set(self.cancel_tasks), (
                    "All tasks must expire through the idle checker"
                )
                for task in proof["tasks"]:
                    event = expired[task["key"]]
                    assert event["offset"] < task["dropped"]["offset"]
                roots = [event for event in self.root_events()
                         if "query msg rsped," in event["line"]]
                assert len(roots) == 1 and "code:0 - success" in (
                    roots[0]["line"]), "Missing initial root acknowledgement"
                root = expired[self.cancel_root]
                assert (roots[0]["offset"] < proof["paused"][1]["offset"] <
                        self.cancel_file_offsets[root["path"]] <=
                        root["offset"]), "Root idle expiry occurred too early"
                proof.update(idle_expirations=list(expired.values()),
                             root_query_response=roots[0],
                             quiet_boundary={
                                 str(state["path"]): state["offset"]
                                 for state in self.files
                                 if state["role"] == "CLIENT"})
                return proof
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"Missing idle cleanup: {missing}; {asynchronous}")
            time.sleep(0.05)


class JoinMigrationLogs(JoinRetryLogs):
    """Track real disconnects, partial attempts and surviving resources."""

    def __init__(self, query_id):
        super().__init__(query_id)
        self.trace_migration = True
        self.migration_client_events = []

    def poll(self):
        JoinRuntimeLogs.poll(self)
        assert len(self.plans) <= 28, "Migration exceeded four DAG attempts"
        assert not any(event["kind"] == "timed out"
                       for event in self.timeout_events), "Root gate expired"

    def _sent(self, key, message):
        return [event for event in self.migration_client_events
                if self.key(event["line"]) == key and
                f"start to send {message} msg" in event["line"]]

    def wait_migration_held(self, kind, timeout=20):
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            held = [event for event in self.timeout_events
                    if event["kind"] == "held"]
            assert len(held) <= 1, "Migration barrier entered twice"
            if held:
                assert len(self.roots) == 1
                root, plan = next(iter(self.roots.items()))
                joins = [(name, body) for name, body in
                         self.nodes(plan["RootNode"]) if "Join" in name]
                assert len(joins) == 1
                name, body = joins[0]
                assert name == ("PhysiHashJoin" if kind == "hash"
                                else "PhysiMergeJoin")
                if kind in ("asof", "window"):
                    assert int(body["SubType"]) == (
                        4 if kind == "asof" else 5)
                assert held[0]["task_id"] == root[3]
                fields = held[0]["fields"]
                output = plan["RootNode"][plan["RootNode"]["Name"]]
                block = int(output["OutputDataBlockDesc"]["DataBlockDesc"][
                    "DataBlockId"])
                assert fields["blockId"] == block and fields["rows"] > 0
                assert fields["readTimeout"] == 64
                queries = self._sent(root, "merge-query")
                fetches = self._sent(root, "merge-fetch")
                if len(queries) == 1 and len(fetches) == 1:
                    sent = fetches[0]
                    handle = re.search(r"pHandle:(0x[\da-fA-F]+)$",
                                       sent["line"])
                    assert handle is not None and int(handle[1], 16) > 0
                    pending = []
                    for event in self.migration_client_events:
                        match = re.search(
                            r"conn:(0x[\da-fA-F]+), merge-fetch is sent to "
                            r"[^,]+:(\d+), .*\bsid:(\d+), "
                            r"QID:(0x[\da-fA-F]+):", event["line"])
                        if (match and int(match[3]) == int(handle[1], 16) and
                                int(match[4], 16) == self.query_id):
                            pending.append((event, match[1], int(match[2])))
                    if pending:
                        assert len(pending) == 1
                        self._read_session_mode()
                        self.initial_keys = frozenset(self.plans)
                        view = self._view(self.initial_keys)
                        view._read_session_mode()
                        view._freeze_cancel_state()
                        self.initial_root = root
                        self.initial_view = view
                        self.migration_held = self._async_proof(held[0], root)
                        self.initial_query = queries[0]
                        self.pending_fetch = dict(sent)
                        (self.pending_transport, self.socket,
                         self.failed_port) = pending[0]
                        self.socket_handle = int(handle[1], 16)
                        self.crash_client_boundary = self._offsets("CLIENT")
                        return {
                            "root_key": root, "held": self.migration_held,
                            "server_log": view.cancel_tasks[root]["path"],
                            "port": self.failed_port,
                        }
            if time.monotonic() >= deadline:
                raise AssertionError("Missing held root and in-flight fetch")
            time.sleep(0.02)

    def _offsets(self, role):
        return {str(state["path"]): state["offset"] for state in self.files
                if state["role"] == role}

    def mark_migration_result_free(self):
        self.poll()
        self.migration_free_boundary = self._offsets("CLIENT")
        self.migration_server_free = self._offsets("SERVER")

    def _models(self):
        result = []
        for key, plan in sorted(self.plans.items()):
            inputs, joins, scans = [], [], 0
            for name, body in self.nodes(plan["RootNode"]):
                if "Join" in name:
                    joins.append({"name": name,
                                  "subtype": int(body.get("SubType", 0))})
                scans += name == "PhysiTableScan"
                if name != "PhysiExchange":
                    continue
                block = int(body["OutputDataBlockDesc"]["DataBlockDesc"][
                    "DataBlockId"])
                for source in body["SrcEndPoints"]:
                    value = source["DownstreamSource"]
                    producer = (int(value["ProducerQueryId"]),
                                int(value["SeriousId"]),
                                int(value["ClientId"]) & ((1 << 64) - 1),
                                int(value["TaskId"]), int(value["ExecId"]))
                    inputs.append({"producer": producer, "block_id": block})
            output = plan["RootNode"][plan["RootNode"]["Name"]]
            block = int(output["OutputDataBlockDesc"]["DataBlockDesc"][
                "DataBlockId"])
            result.append({"key": key, "level": int(plan["Level"]),
                           "output_block_id": block, "joins": joins,
                           "scans": scans, "inputs": inputs})
        return result

    def _migration_cleanup(self, crash):
        registered = {event["key"]: event for event in self.events
                      if "task preprocess succeed," in event["line"]}
        assert set(registered) <= set(self.plans)
        models = {tuple(value["key"]): value for value in self._models()}
        tasks, missing = [], []
        for series in sorted({key[1] for key in registered}):
            keys = frozenset(key for key in registered if key[1] == series)
            view = self._view(keys)
            view._read_session_mode()
            view.cancel_root = None
            view.cancel_file_offsets = self.migration_server_free
            view.cancel_exchange_blocks = view._planned_exchange_blocks(keys)
            groups = view._cancel_groups(keys)
            exchanges, pending = view._exchange_evidence(
                view._exchange_groups(groups))
            for key, group in groups.items():
                drops = [event for _, event in group["events"]
                         if event["line"].endswith(" task ctx dropped")]
                assert len(drops) <= 1
                dropped = drops[0] if drops else None
                initialized, session = view._session_evidence(group, dropped)
                task = {"key": key, "registered": registered[key],
                        "planned_exchange_blocks":
                            view.cancel_exchange_blocks[key],
                        "exchanges": exchanges[key]}
                if initialized is not None:
                    task["session_initialized"] = initialized
                if session is not None:
                    task["session"] = session
                if dropped is None and group["path"] == crash["server_log"]:
                    assert key in self.initial_keys
                    assert registered[key]["offset"] < crash["log_end"]
                    task["lifecycle"] = "process-exit"
                else:
                    task.update(lifecycle="cleaned", dropped=dropped)
                    if dropped is None or any(item[0] == key for item in
                                               pending):
                        missing.append(key)
                tasks.append(task)
        registered_keys = set(registered)
        for model in models.values():
            if tuple(model["key"]) in registered_keys:
                assert all(tuple(item["producer"]) in registered_keys
                           for item in model["inputs"]), (
                    "Executed Exchange references an unregistered input"
                )
        return tasks, missing

    def check_migration_cleaned(self, crash, timeout=20):
        deadline = time.monotonic() + timeout
        while True:
            self.poll()
            tasks, missing = self._migration_cleanup(crash)
            if not missing:
                break
            if time.monotonic() >= deadline:
                raise AssertionError(f"Surviving tasks not cleaned: {missing}")
            time.sleep(0.02)
        assert Path(crash["server_log"]).stat().st_size == crash["log_end"]
        roots = sorted(self.roots)
        assert roots[0] == self.initial_root and len(roots) >= 2
        final = roots[-1]
        queries = self._sent(final, "merge-query")
        assert len(queries) == 1
        eof = [event for event in self.migration_client_events if re.search(
            r"conn:" + re.escape(self.socket) +
            r", read error:(?:UV_)?(?:EOF|ECONNRESET)(?:,|\s|$)",
            event["line"]) and event["offset"] >=
            self.crash_client_boundary[event["path"]]]
        assert len(eof) == 1, "Missing real socket EOF/reset"
        callbacks = [event for event in self.migration_client_events
                     if re.search(r"QID:" + hex(self.query_id) +
                                  r", handle rsp msg, type:merge-fetch-rsp, "
                                  r"handle:" + hex(self.socket_handle) +
                                  r", code:Conn is broken$", event["line"])]
        assert len(callbacks) == 1
        restarts = [event for event in self.retry_client_events
                    if "start to restart all tasks by error:" in event["line"]]
        updates = [event for event in self.retry_client_events
                   if "update job sId:" in event["line"]]
        assert 1 <= len(restarts) == len(updates) <= 3
        for event in self.retry_client_events:
            if "job failed with error " in event["line"]:
                assert event["offset"] >= self.migration_free_boundary[
                    event["path"]] and event["line"].endswith("Query killed")
        dropped = [event for event in self.failure_client_events
                   if self.key(event["line"]) == final and
                   "start to send drop-task msg" in event["line"]]
        assert len(dropped) == 1
        return {
            "initial_root": self.initial_root, "final_root": final,
            "initial_task_keys": sorted(self.initial_keys),
            "held": self.migration_held, "crash": crash,
            "initial_query": self.initial_query, "final_query": queries[0],
            "pending_fetch": self.pending_fetch,
            "pending_transport": self.pending_transport,
            "socket": self.socket, "socket_handle": self.socket_handle,
            "eof": eof[0], "broken_callback": callbacks[0],
            "restarts": restarts, "series_updates": updates,
            "crash_client_boundary": self.crash_client_boundary,
            "result_free_boundary": self.migration_free_boundary,
            "server_free_boundary": self.migration_server_free,
            "drop_sent": dropped[0], "models": self._models(),
            "tasks": tasks, "session_mode": self.cancel_session_mode,
        }
