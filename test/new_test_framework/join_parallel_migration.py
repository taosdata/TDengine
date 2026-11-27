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

import json
import logging
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid

from new_test_framework import join_parallel_runtime as runtime


KINDS = ("hash", "merge", "asof", "window")


def control_write(path, value):
    temporary = path.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                         0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream)
    assert not path.exists()
    temporary.rename(path)


def endpoint(event, key):
    event = runtime.failure_task_event(event, key, raw=True)
    match = re.search(r"start to send merge-query msg to "
                      r"node\[-?\d+,localhost,(\d+)\],", event["line"])
    assert match is not None
    return int(match[1])


def validate_graph(models, keys, root):
    selected = {runtime.task_key(model["key"]): model for model in models
                if runtime.task_key(model["key"]) in keys}
    assert set(selected) == keys and 5 <= len(keys) <= 7
    assert [key for key, model in selected.items()
            if model["level"] == 0] == [root]
    assert sum(len(model["joins"]) for model in selected.values()) == 3
    assert sum(model["scans"] for model in selected.values()) == 4
    producers = []
    for key, model in selected.items():
        blocks = set()
        for item in model["inputs"]:
            source = runtime.task_key(item["producer"])
            assert source in keys and source != key
            assert source[:3] == key[:3]
            assert selected[source]["level"] == model["level"] + 1
            assert item["block_id"] not in blocks
            blocks.add(item["block_id"])
            producers.append(source)
    assert len(producers) == len(keys) - 1
    assert set(producers) == keys - {root}


def validate_entry(entry, rows=8192):
    assert isinstance(entry, dict) and entry["kind"] in KINDS
    query, followup = entry["query_id"], entry["followup_query_id"]
    assert type(query) is type(followup) is int
    assert query > 0 and followup > 0 and query != followup
    assert entry["input_rows"] == rows
    expected = rows * (8 if entry["kind"] == "hash" else 1)
    assert entry["rows"] == entry["followup_rows"] == expected
    assert entry["result_retained"] is True
    assert entry["result_released_normally"] is True
    assert entry["followup_same_connection"] is True
    assert entry["query_policy"] == 3
    mode = entry["session_mode"]
    assert mode in ("enabled", "disabled")
    old, new = (runtime.task_key(entry[name]) for name in (
        "initial_root", "final_root"))
    assert old[0] == new[0] == query
    assert old[2:4] == new[2:4] and 1 == old[1] < new[1] <= 4
    assert new[4] > old[4] == 0
    initial = {runtime.task_key(key) for key in entry["initial_task_keys"]}
    models = entry["models"]
    model_keys = {runtime.task_key(model["key"]) for model in models}
    assert len(model_keys) == len(models) and all(key[0] == query
                                                for key in model_keys)
    final = {key for key in model_keys if key[1] == new[1]}
    validate_graph(models, initial, old)
    validate_graph(models, final, new)
    crash = entry["crash"]
    assert crash["node"] in (2, 3) and crash["process_exited"] is True
    assert crash["exit_code"] == (-signal.SIGKILL if os.name != "nt" else 1)
    assert type(crash["pid"]) is int and crash["pid"] > 0
    assert crash["query_id"] == query and crash["kind"] == entry["kind"]
    assert runtime.task_key(crash["root_key"]) == old
    assert crash["port"] == endpoint(entry["initial_query"], old)
    assert endpoint(entry["final_query"], new) != crash["port"]
    nodes = {node["number"]: node for node in entry["nodes"]}
    assert set(nodes) == {1, 2, 3}
    assert len({node["port"] for node in nodes.values()}) == 3
    assert crash["port"] == nodes[crash["node"]]["port"]
    assert crash["server_log"] == nodes[crash["node"]]["server_log"]
    assert all(node["support_vnodes"] == 0 for number, node in nodes.items()
               if number != 1)
    assert nodes[1]["support_vnodes"] > 0
    by_key = {tuple(model["key"]): model for model in models}
    for root in (old, new):
        join, = by_key[root]["joins"]
        assert join["name"] == ("PhysiHashJoin" if entry["kind"] == "hash"
                                else "PhysiMergeJoin")
        assert join["subtype"] == {"asof": 4, "window": 5}.get(
            entry["kind"], 0)
    topology = entry["topology"]
    actual_nodes = {node["id"]: node for node in topology["dnodes"]}
    assert set(actual_nodes) == {1, 2, 3}
    assert {node["id"] for node in topology["qnodes"]} == {2, 3}
    for number, node in nodes.items():
        assert actual_nodes[number]["support_vnodes"] == node["support_vnodes"]
        assert actual_nodes[number]["endpoint"] == f"localhost:{node['port']}"
    assert topology["vgroups"]
    for group in topology["vgroups"]:
        fields = [name for name in group if re.fullmatch(r"v\d+_dnode", name)]
        assert fields and {group[name] for name in fields if group[name]} == {1}
    tasks = entry["tasks"]
    task_keys = {runtime.task_key(task["key"]) for task in tasks}
    assert len(task_keys) == len(tasks) and initial | final <= task_keys
    assert task_keys <= model_keys
    restarts, updates = entry["restarts"], entry["series_updates"]
    assert 1 <= len(restarts) == len(updates) <= 3
    missing = model_keys - task_keys
    failures = set()
    for index, (restart, update) in enumerate(zip(restarts, updates), 1):
        match = re.search(r"\bSID:(\d+), CID:(0x[\da-fA-F]+), "
                          r"TID:(0x[\da-fA-F]+), EID:(\d+)",
                          restart["line"])
        assert match is not None
        key = (query, int(match[1]), int(match[2], 16), int(match[3], 16),
               int(match[4]))
        runtime.failure_task_event(restart, key, raw=True)
        assert key[1] == index
        assert any(restart["line"].endswith(value) for value in (
            "Conn is broken from TID:" + hex(key[3]),
            "Unable to establish connection from TID:" + hex(key[3])))
        assert restart["path"] == update["path"]
        assert restart["offset"] < update["offset"]
        assert re.search(r"update job sId:" + str(index + 1) + r",",
                         update["line"])
        if key in missing:
            failures.add(key)
    assert missing == failures, "Unregistered dispatch lacks a retry failure"
    assert new[1] == len(restarts) + 1
    boundary = entry["server_free_boundary"]
    exchange_ids, process_exit = set(), set()
    for task in tasks:
        key = runtime.task_key(task["key"])
        registered = runtime.failure_task_event(task["registered"], key)
        path = registered["path"]
        assert registered["offset"] < boundary[path]
        assert "task preprocess succeed," in registered["line"]
        assert task["planned_exchange_blocks"] == sorted({
            item["block_id"] for item in by_key[key]["inputs"]})
        if mode == "enabled":
            first = runtime.evidence_event(task["session_initialized"], key)
            assert first["path"] == path
            assert first["offset"] < registered["offset"]
            identity = runtime.session_identity(first, "session initialized,")
        else:
            assert "session_initialized" not in task and "session" not in task
        if task["lifecycle"] == "process-exit":
            assert key in initial and path == crash["server_log"]
            assert registered["offset"] < crash["log_end"]
            assert task.get("dropped") is None
            blocks = set()
            for exchange in task["exchanges"]:
                block, instance = exchange["block_id"], exchange["exchange_id"]
                assert block in task["planned_exchange_blocks"]
                assert block not in blocks and (path, instance) not in (
                    exchange_ids)
                blocks.add(block)
                exchange_ids.add((path, instance))
                created = runtime.exchange_event(
                    exchange["created"], key, "exchange lifecycle created",
                    instance, block)
                assert created["path"] == path
                assert registered["offset"] < created["offset"] < (
                    crash["log_end"])
                fetches = set()
                for request in exchange["requests"]:
                    fetch = request["fetch_id"]
                    assert type(fetch) is int and fetch > 0 and fetch not in (
                        fetches)
                    fetches.add(fetch)
                    first = runtime.exchange_event(
                        request["registered"], key, "fetch context registered",
                        instance, block)
                    assert first["path"] == path
                    assert created["offset"] < first["offset"] < (
                        crash["log_end"])
                    for field, label in (("fetch_id", "fetchId"),
                                         ("source_index", "sourceIndex"),
                                         ("seq_id", "seqId")):
                        assert runtime.lifecycle_field(first, label) == (
                            request[field])
            assert blocks == set(task["planned_exchange_blocks"])
            process_exit.add(key)
        else:
            assert task["lifecycle"] == "cleaned"
            dropped = runtime.failure_task_event(task["dropped"], key)
            assert dropped["path"] == path
            assert registered["offset"] < dropped["offset"]
            assert dropped["line"].endswith(" task ctx dropped")
            if mode == "enabled":
                last = runtime.evidence_event(task["session"], key)
                assert last["path"] == path
                assert registered["offset"] < last["offset"] < (
                    dropped["offset"])
                assert runtime.session_identity(
                    last, "task session destroyed,") == identity
            runtime.validate_exchange_cleanup(task, None, boundary,
                                               exchange_ids)
    assert old in process_exit
    final_task, = [task for task in tasks if tuple(task["key"]) == new]
    assert final_task["registered"]["path"] != crash["server_log"]
    assert final_task["dropped"]["offset"] >= boundary[
        final_task["registered"]["path"]]
    for model in models:
        if tuple(model["key"]) not in task_keys:
            continue
        assert all(tuple(item["producer"]) in task_keys
                   for item in model["inputs"])
    held = runtime.evidence_event(entry["held"], old)
    assert held["path"] == crash["server_log"]
    assert "query timeout test held," in held["line"]
    assert runtime.lifecycle_field(held, "blockId") == (
        by_key[old]["output_block_id"])
    assert runtime.lifecycle_field(held, "rows") > 0
    assert runtime.lifecycle_field(held, "readTimeout") == 64
    assert held["offset"] < crash["log_end"]
    sent = runtime.failure_task_event(entry["pending_fetch"], old, raw=True)
    handle = entry["socket_handle"]
    assert re.search(r"pHandle:" + hex(handle) + r"$", sent["line"])
    transport = runtime.rpc_client_event(entry["pending_transport"])
    assert re.search(r"conn:" + re.escape(entry["socket"]) +
                     r", merge-fetch is sent to [^,]+:" +
                     str(crash["port"]) + r", .*\bsid:" + str(handle) +
                     r", QID:" + hex(query) + r":", transport["line"])
    eof = runtime.rpc_client_event(entry["eof"])
    assert re.search(r"conn:" + re.escape(entry["socket"]) +
                     r", read error:(?:UV_)?(?:EOF|ECONNRESET)(?:,|\s|$)",
                     eof["line"])
    callback = runtime.rpc_client_event(entry["broken_callback"])
    assert re.search(r"QID:" + hex(query) +
                     r", handle rsp msg, type:merge-fetch-rsp, handle:" +
                     hex(handle) + r", code:Conn is broken$", callback["line"])
    path = sent["path"]
    assert all(event["path"] == path for event in (
        transport, eof, callback, restarts[0]))
    assert (sent["offset"] < transport["offset"] <
            entry["crash_client_boundary"][path] <= eof["offset"] <
            callback["offset"] < restarts[0]["offset"])
    drop = runtime.failure_task_event(entry["drop_sent"], new, raw=True)
    assert drop["path"] == updates[-1]["path"]
    assert (updates[-1]["offset"] < entry["result_free_boundary"][path] <=
            drop["offset"])
    assert "start to send drop-task msg" in drop["line"]


def validate_result(result, kinds=KINDS):
    assert result["status"] == "passed" and result["data_removed"] is True
    evidence = result["evidence"]
    assert len(evidence) == len(kinds)
    assert {entry["kind"] for entry in evidence} == set(kinds)
    queries = set()
    controllers = {entry["kind"]: entry for entry in result["controllers"]}
    assert len(controllers) == len(kinds) and set(controllers) == set(kinds)
    for entry in evidence:
        validate_entry(entry)
        ids = {entry["query_id"], entry["followup_query_id"]}
        assert len(ids) == 2 and not queries & ids
        queries.update(ids)
        controller = controllers[entry["kind"]]
        assert controller["status"] == "passed"
        assert controller["crash"] == entry["crash"]
        exits = controller["service_exits"]
        assert len(exits) == 2
        assert {value["node"] for value in exits} == (
            {1, 2, 3} - {entry["crash"]["node"]})
        assert all(value["exit_code"] == 0 and not value["forced_kill"]
                   for value in exits)


def records(cursor, sql):
    cursor.execute(sql)
    names = [column[0] for column in cursor.description]
    return [{name: value.isoformat() if hasattr(value, "isoformat")
             else value for name, value in zip(names, row)}
            for row in cursor.fetchall()]


def connect_ready(taos, directory, port, timeout):
    deadline = time.monotonic() + timeout
    while True:
        connection = None
        try:
            connection = taos.connect(host="localhost", port=port, user="root",
                                      password="taosdata",
                                      config=str(directory / "client-config"))
            cursor = connection.cursor()
            cursor.execute("select server_version()")
            cursor.fetchall()
            return connection, cursor
        except Exception:
            if connection is not None:
                connection.close()
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.1)


def client(directory):
    import taos

    manifest = json.loads((directory / "manifest.json").read_text())
    connection, cursor = connect_ready(taos, directory, manifest["port"], 30)
    runtime.verify_loaded_libraries(manifest["libraries"], directory)
    case = None
    worker = None
    state = None
    result = {"status": "failed", "kind": manifest["kind"]}
    try:
        nodes = manifest["nodes"]
        for node in nodes[1:]:
            cursor.execute(f"create dnode localhost port {node['port']}")
        deadline = time.monotonic() + 30
        while True:
            cursor.execute("show dnodes")
            topology = cursor.fetchall()
            if len(topology) == 3 and all(row[4] == "ready"
                                           for row in topology):
                break
            assert time.monotonic() < deadline, topology
            time.sleep(0.1)
        assert all(row[2] == 0 for row in topology if row[0] in (2, 3))
        for number in (2, 3):
            cursor.execute(f"create qnode on dnode {number}")
        qnodes = records(cursor, "show qnodes")
        assert {value["id"] for value in qnodes} == {2, 3}
        topology = {"dnodes": records(cursor, "show dnodes"),
                    "qnodes": qnodes}
        sources = directory / "sources"
        logs = runtime.load_module("new_test_framework.utils.join_runtime_logs",
                                   sources / "join_runtime_logs.py")
        case = runtime.load_module("join_migration_case",
                                   sources / "test_join_parallel.py"
                                   ).TestJoinParallel()
        case.sql = runtime.SqlAdapter(cursor)
        case.log = logging.getLogger("join-migration")
        case.prepare_data(8192, "duplicate", names="abcd")
        topology["vgroups"] = records(cursor,
                                      f"show {case.database}.vgroups")
        runtime.dump(directory / "topology.json", topology)
        kind, query = next(value for value in case.runtime_queries()
                           if value[0] == manifest["kind"])
        case.check_runtime_plan(kind, query)
        query_id = manifest["query_id"]
        gate = Path(manifest["gate_dir"]) / str(query_id)
        gate.mkdir(mode=0o700)
        case.write_retry_marker(gate, "arm", query_id)
        observer = logs.JoinMigrationLogs(query_id)
        state = {"query": query, "query_id": query_id, "kind": kind,
                 "count": 8192, "followup_id": manifest["followup_query_id"],
                 "outcome": {}, "returned": threading.Event(),
                 "allow_free": threading.Event(), "freed": threading.Event(),
                 "allow_followup": threading.Event(),
                 "finished": threading.Event()}
        worker = threading.Thread(target=case.run_retry_worker, args=(state,),
                                  daemon=True)
        worker.start()
        ticket = observer.wait_migration_held(kind)
        assert not state["returned"].is_set(), state["outcome"]
        selected, = [node for node in nodes
                     if node["server_log"] == ticket["server_log"]]
        assert selected["number"] in (2, 3)
        request = {"kind": kind, "query_id": query_id,
                   "node": selected["number"], "port": ticket["port"],
                   "root_key": ticket["root_key"],
                   "server_log": ticket["server_log"]}
        control = Path(manifest["control_dir"])
        control_write(control / "crash-request.json", request)
        deadline = time.monotonic() + 20
        while not (control / "crashed.json").exists():
            assert time.monotonic() < deadline, "Owned qnode did not exit"
            time.sleep(0.02)
        crash = json.loads((control / "crashed.json").read_text())
        assert state["returned"].wait(45), "Migrated query did not complete"
        outcome = state["outcome"]
        assert "query_error" not in outcome, outcome
        assert outcome.get("result_retained") and not outcome.get(
            "result_freed"), outcome
        observer.mark_migration_result_free()
        state["allow_free"].set()
        assert state["freed"].wait(10), "Retried result did not free"
        proof = observer.check_migration_cleaned(crash)
        outcome["run_followup"] = True
        state["allow_followup"].set()
        assert state["finished"].wait(45), "Same-connection follow-up stalled"
        worker.join(timeout=1)
        assert not worker.is_alive() and "worker_error" not in outcome, outcome
        assert "cleanup_errors" not in outcome, outcome
        proof.update(kind=kind, query_id=query_id, input_rows=8192,
                     rows=outcome["rows"], result_retained=True,
                     result_released_normally=True,
                     followup_same_connection=True,
                     followup_query_id=manifest["followup_query_id"],
                     followup_rows=outcome["followup_rows"], query_policy=3,
                     nodes=nodes, topology=topology)
        validate_entry(proof)
        result.update(status="passed", evidence=proof)
    except BaseException:
        result.update(status="failed", error=traceback.format_exc())
        raise
    finally:
        errors = []
        if state is not None:
            state["allow_free"].set()
            state["allow_followup"].set()
        if worker is not None:
            worker.join(timeout=20)
            if worker.is_alive():
                errors.append("Migration worker leaked")
        if case is not None:
            try:
                case.drop_data()
            except BaseException as error:
                errors.append(repr(error))
        for resource in (cursor, connection):
            try:
                resource.close()
            except BaseException as error:
                errors.append(repr(error))
        if errors:
            result.update(status="failed", cleanup_errors=errors)
        runtime.dump(directory / "result.json", result)
        assert not errors, errors


def configure(directory, storage, port, kind, server, libraries, query_id):
    for name in ("sources", "client-config", "client-logs"):
        (directory / name).mkdir()
    gate, control = storage / "gate", storage / "control"
    gate.mkdir(mode=0o700)
    control.mkdir(mode=0o700)
    common = ["fqdn localhost", f"firstEp localhost:{port}", "numOfCores 2",
              "queryPolicy 3", "queryRspPolicy 1", "readTimeout 64",
              "monitor 0", "telemetryReporting 0", "timezone UTC",
              "qDebugFlag 135", "rpcDebugFlag 135", "asyncLog 0"]
    nodes = []
    for number in (1, 2, 3):
        node = directory / f"node{number}"
        data = storage / f"node{number}"
        for name in ("config", "logs"):
            (node / name).mkdir(parents=True)
        for name in ("data", "tmp"):
            (data / name).mkdir(parents=True)
        support = 16 if number == 1 else 0
        node_port = port + 100 * (number - 1)
        lines = common + [f"serverPort {node_port}",
                          f"dataDir {data / 'data'}",
                          f"tempDir {data / 'tmp'}", f"logDir {node / 'logs'}",
                          f"supportVnodes {support}", "minimalDataDirGB 0.001",
                          "numOfVnodeQueryThreads 8",
                          "numOfVnodeFetchThreads 4"]
        (node / "config/taos.cfg").write_text("\n".join(lines) + "\n")
        nodes.append({"number": number, "port": node_port,
                      "support_vnodes": support,
                      "server_log": str(node / "logs/taosdlog.0")})
    lines = common + [f"serverPort {port}", "cDebugFlag 135",
                      "shareConnLimit 1", "queryPlannerTrace 1",
                      f"logDir {directory / 'client-logs'}"]
    (directory / "client-config/taos.cfg").write_text("\n".join(lines) + "\n")
    sources = (runtime.CASE, runtime.HELPER, Path(runtime.__file__),
               Path(__file__).resolve())
    for source in sources:
        (directory / "sources" / source.name).write_bytes(source.read_bytes())
    manifest = {"kind": kind, "port": port, "nodes": nodes,
                "gate_dir": str(gate), "control_dir": str(control),
                "data_root": str(storage), "query_id": query_id,
                "followup_query_id": query_id % 0x7fffffff + 1,
                "libraries": [str(value) for value in libraries],
                "components": {str(value): runtime.sha(value)
                               for value in (server, *libraries)},
                "sources": {str(value): runtime.sha(value)
                            for value in sources}}
    runtime.dump(directory / "manifest.json", manifest)
    return manifest


def crash_owned(request, manifest, processes):
    assert request["kind"] == manifest["kind"]
    assert request["query_id"] == manifest["query_id"]
    number = request["node"]
    assert type(number) is int and number in (2, 3), (
        "Only a dedicated owned compute node may be crashed")
    node = manifest["nodes"][number - 1]
    assert request["port"] == node["port"]
    assert request["server_log"] == node["server_log"]
    assert runtime.task_key(request["root_key"])[0] == manifest["query_id"]
    process = processes[number - 1]
    assert process.poll() is None, "Owned qnode exited before fault injection"
    process.kill()
    process.wait(timeout=10)
    assert process.returncode == (-signal.SIGKILL if os.name != "nt" else 1)
    return dict(request, pid=process.pid, exit_code=process.returncode,
                process_exited=True, log_end=Path(node["server_log"]).stat(
                    ).st_size)


def run_kind(args, directory, storage, server, lib_dir, libraries, kind, query):
    directory.mkdir()
    storage.mkdir()
    manifest = configure(directory, storage, args.port, kind, server,
                          libraries, query)
    env = os.environ.copy()
    variable = ("PATH" if os.name == "nt" else "DYLD_LIBRARY_PATH"
                if sys.platform == "darwin" else "LD_LIBRARY_PATH")
    env[variable] = str(lib_dir) + os.pathsep + env.get(variable, "")
    env["TD_JOIN_TEST_TIMEOUT_GATE_DIR"] = manifest["gate_dir"]
    env["TZ"] = "UTC"
    env["PYTHONPATH"] = (str(runtime.TEST_ROOT) + os.pathsep +
                         env.get("PYTHONPATH", ""))
    processes, outputs = [], []
    child = None
    controller = {"status": "failed", "kind": kind}
    try:
        flags = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP
                 } if os.name == "nt" else {}
        for node in manifest["nodes"]:
            path = directory / f"node{node['number']}"
            output = (path / "process.log").open("w")
            outputs.append(output)
            processes.append(subprocess.Popen(
                [str(server), "-c", str(path / "config")], env=env,
                stdout=output, stderr=subprocess.STDOUT, **flags))
        deadline = time.monotonic() + args.ready_timeout
        paths = [Path(node["server_log"]) for node in manifest["nodes"]]
        while not all(path.is_file() for path in paths):
            assert all(process.poll() is None for process in processes)
            assert time.monotonic() < deadline, "Server logs did not appear"
            time.sleep(0.05)
        mode, evidence = runtime.detect_session_mode(paths)
        env["JOIN_PARALLEL_SESSION_MODE"] = mode
        env["JOIN_PARALLEL_SERVER_LOGS"] = json.dumps([str(p) for p in paths])
        env["JOIN_PARALLEL_CLIENT_LOGS"] = json.dumps([
            str(directory / "client-logs/taoslog0.0")])
        manifest.update(session_mode=mode, session_mode_evidence=evidence)
        runtime.dump(directory / "manifest.json", manifest)
        output = (directory / "client.log").open("w")
        outputs.append(output)
        command = runtime.client_command(Path(__file__), "client",
                                         str(directory))
        child = subprocess.Popen(command, env=env,
                                 stdout=output, stderr=subprocess.STDOUT,
                                 **flags)
        control = Path(manifest["control_dir"])
        deadline = time.monotonic() + args.timeout
        crashed = None
        while child.poll() is None:
            if (control / "crash-request.json").exists() and crashed is None:
                request_path = control / "crash-request.json"
                request = json.loads(request_path.read_text())
                crashed = crash_owned(request, manifest, processes)
                controller["crash"] = crashed
                control_write(control / "crashed.json", crashed)
            for number, process in enumerate(processes, 1):
                if crashed is None or number != crashed["node"]:
                    assert process.poll() is None, (
                        f"Unexpected node{number} exit")
            assert time.monotonic() < deadline, "Migration client timed out"
            time.sleep(0.02)
        if child.returncode != 0:
            runtime.print_client_tail(directory / "client.log")
        assert child.returncode == 0, (
            f"Migration client exited {child.returncode}")
        assert crashed is not None
        result = json.loads((directory / "result.json").read_text())
        assert result["status"] == "passed"
        validate_entry(result["evidence"])
        controller.update(status="passed", crash=crashed)
        print(f"JP-10-migration {kind}: real socket EOF, qnode migration, "
              "surviving-resource cleanup and same-connection results verified",
              flush=True)
        return result["evidence"]
    except BaseException:
        controller["error"] = traceback.format_exc()
        raise
    finally:
        succeeded = controller["status"] == "passed"
        if child is not None and child.poll() is None:
            runtime.stop_owned_process(child)
        exits = []
        for number, process in reversed(list(enumerate(processes, 1))):
            if controller.get("crash", {}).get("node") == number:
                continue
            exit_info = runtime.stop_owned_process(process)
            exit_info["node"] = number
            exits.append(exit_info)
            if exit_info["exit_code"] or exit_info["forced_kill"]:
                controller["status"] = "failed"
        controller["service_exits"] = exits
        for output in outputs:
            output.close()
        runtime.dump(directory / "controller-result.json", controller)
        if succeeded:
            assert controller["status"] == "passed", controller


def run(args):
    server, lib_dir, libraries = runtime.runtime_layout(args.build_dir)
    runtime.require_timeout_gate(server)
    for number in range(3):
        runtime.check_ports(args.port + 100 * number)
    args.output.mkdir(parents=True, exist_ok=False)
    result = {"status": "failed", "evidence": [], "controllers": []}
    try:
        with tempfile.TemporaryDirectory(prefix="jp-move-") as storage:
            result["data_root"] = storage
            first = int(uuid.uuid4().hex[:8], 16) & 0x7fffffff or 1
            kinds = KINDS if args.kind == "all" else (args.kind,)
            for index, kind in enumerate(kinds):
                query = (first + 2 * index - 1) % 0x7fffffff + 1
                result["evidence"].append(run_kind(
                    args, args.output / kind, Path(storage) / kind, server,
                    lib_dir, libraries, kind, query))
                result["controllers"].append(json.loads((
                    args.output / kind / "controller-result.json").read_text()))
        assert not Path(storage).exists()
        result.update(status="passed", data_removed=True)
        validate_result(result, kinds)
        print("JP-10-migration " + "/".join(kinds) +
              " successfully executed", flush=True)
    except BaseException:
        result.update(status="failed", error=traceback.format_exc())
        raise
    finally:
        if "data_root" in result:
            result["data_removed"] = not Path(result["data_root"]).exists()
        runtime.dump(args.output / "result.json", result)
