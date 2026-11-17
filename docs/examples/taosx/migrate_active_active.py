#!/usr/bin/env python3

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

"""Migrate v3.3.6 taosX active-active tasks to SQL-managed XNODE tasks."""

import argparse
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urlsplit, urlunsplit
from urllib.request import Request, urlopen


SYSTEM_PARAMS = {
    "replica", "msg.consume.excluded", "timeout", "prefer", "enable.wal.marker",
    "group.id", "use.topic.name", "client.id",
}
CONNECTION_PARAMS = {
    "token", "bearer_token", "td.connect.token", "tls_mode", "tls_version",
    "tls_ca", "conn_mode",
}
SOURCE_PARAMS = CONNECTION_PARAMS | {
    "auto.offset.reset", "experimental.snapshot.enable", "msg.consume.start.ts",
    "enable.auto.commit", "enable.replay", "msg.consume.rawdata",
    "write_concurrency", "num.of.writers", "commit.chunk.size", "commit.interval.ms",
    "max.polling.timeout", "enable.concurrent.polling", "refresh.progress.interval",
    "auto.commit.interval.ms", "msg.with.table.name", "session.timeout.ms",
    "max.poll.interval.ms", "fetch.max.wait.ms", "min.poll.rows", "msg.enable.batchmeta",
}
DSN = re.compile(
    r"^(tmq|taos)(?:\+(ws|wss|http|https|native))?://([^/?#]+)/([^?#]+)(?:\?(.*))?$"
)
INTEGER = re.compile(r"^[+-]?[0-9]+$")
TASK_ID = re.compile(r"^[1-9][0-9]*$")
RFC3339 = re.compile(
    r"^(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})"
    r"(?:\.(?P<fraction>[0-9]{1,3}))?(?P<zone>Z|[+-][0-9]{2}:[0-9]{2})$"
)
STOPPED = {"stopped", "stop", "inactive"}
RUNNING = {"running"}
SQL_ERROR = re.compile(r"(?im)^\s*(?:DB error:|error:|failed to (?:open|connect|read|fetch|execute)\b|query interrupted\b|taos_fetch_row failed\b)")
SQL_SUCCESS = re.compile(r"(?im)^\s*(?:Query|Create) OK,\s*([0-9]+) row\(s\) (?:in set|affected)\b")
UNSET = object()


class MigrationError(Exception):
    """A user-facing migration error that never contains credentials."""


def unique_object(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("duplicate JSON object key; resolve the ambiguous input")
        result[name] = value
    return result


def read_json(path):
    try:
        with Path(path).open(encoding="utf-8") as source:
            return json.load(source, object_pairs_hook=unique_object)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("unable to read JSON input") from error


def timestamp(value):
    if type(value) is not int or not 0 <= value <= 9223372036854775807:
        raise ValueError("start time must be a nonnegative signed 64-bit Unix millisecond integer")
    return value


def parse_timestamp(value):
    if type(value) is int:
        return timestamp(value)
    if not isinstance(value, str):
        raise ValueError("start time must be Unix milliseconds or RFC 3339")
    if INTEGER.fullmatch(value):
        try:
            return timestamp(int(value, 10))
        except (ValueError, OverflowError):
            raise ValueError("start time is outside the signed 64-bit millisecond range") from None
    matched = RFC3339.fullmatch(value)
    if matched is None:
        raise ValueError("start time must include Z or an explicit +HH:MM/-HH:MM timezone and at most 3 fractional digits")
    fraction = (matched.group("fraction") or "").ljust(3, "0")
    zone = matched.group("zone")
    if zone == "Z":
        offset = timezone.utc
    else:
        sign = 1 if zone[0] == "+" else -1
        hours, minutes = map(int, zone[1:].split(":"))
        if hours > 23 or minutes > 59:
            raise ValueError("invalid RFC 3339 timezone offset")
        offset = timezone(sign * timedelta(hours=hours, minutes=minutes))
    try:
        parsed = datetime.strptime(matched.group("date"), "%Y-%m-%dT%H:%M:%S").replace(
            tzinfo=offset
        )
    except ValueError:
        raise ValueError("invalid RFC 3339 start time") from None
    return timestamp(int(parsed.timestamp()) * 1000 + int(fraction or 0))


def decode_parameter(value):
    if re.search(r"%(?![0-9a-fA-F]{2})", value):
        raise ValueError("invalid percent encoding in DSN parameter")
    try:
        return unquote(value, errors="strict")
    except UnicodeError:
        raise ValueError("invalid UTF-8 in DSN parameter") from None


def convert_dsn(value, driver, start_time=UNSET):
    if not isinstance(value, str) or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("expected a plain DSN string without control characters")
    matched = DSN.fullmatch(value)
    if matched is None or matched[1] != driver:
        raise ValueError("expected a %s DSN with addresses and a database" % driver)
    authority, database, query = matched.group(3, 4, 5)
    addresses = authority.rsplit("@", 1)[-1]
    if not addresses or any(not address for address in addresses.split(",")):
        raise ValueError("DSN addresses must not be empty")
    params = {}
    for pair in query.split("&") if query else []:
        if not pair:
            raise ValueError("empty DSN parameter")
        name, separator, raw = pair.partition("=")
        if not separator:
            raw = ""
        name, raw = decode_parameter(name), decode_parameter(raw)
        if not name or name in params:
            raise ValueError("duplicate or empty DSN parameter")
        params[name] = raw
    params = {name: raw for name, raw in params.items() if name not in SYSTEM_PARAMS}
    allowed = SOURCE_PARAMS if driver == "tmq" else CONNECTION_PARAMS
    unsupported = params.keys() - allowed
    if unsupported:
        names = ", ".join(name if re.fullmatch(r"[a-zA-Z0-9_.-]+", name) else "<invalid name>" for name in sorted(unsupported))
        raise ValueError("unsupported %s DSN parameter: %s" % (driver, names))
    if driver == "tmq":
        if params.get("auto.offset.reset", "earliest") == "latest":
            raise ValueError("migration does not support auto.offset.reset=latest")
        if params.get("auto.offset.reset", "earliest") != "earliest":
            raise ValueError("migration requires auto.offset.reset=earliest to cover retained data")
        if params.get("experimental.snapshot.enable", "true") not in ("true", "false"):
            raise ValueError("invalid experimental.snapshot.enable")
        for name in ("enable.auto.commit", "enable.replay"):
            if name in params and params[name] != "false":
                raise ValueError("%s must be false" % name)
        if start_time is not UNSET:
            start_time = timestamp(start_time)
            params["msg.consume.start.ts"] = str(start_time)
        elif "msg.consume.start.ts" in params:
            inherited = params["msg.consume.start.ts"]
            if not re.fullmatch(r"[0-9]+", inherited):
                raise ValueError("invalid msg.consume.start.ts")
            try:
                start_time = timestamp(int(inherited, 10))
            except ValueError:
                raise ValueError("invalid msg.consume.start.ts") from None
        if "msg.consume.rawdata" in params:
            rawdata = params["msg.consume.rawdata"]
            if not re.fullmatch(r"[+-]?[0-9]+", rawdata) or not -2147483648 <= int(rawdata) <= 2147483647:
                raise ValueError("invalid msg.consume.rawdata")
            if start_time is not UNSET and start_time != 0 and int(rawdata) != 0:
                raise ValueError("nonzero msg.consume.rawdata conflicts with the migration start time")
    base = value.split("?", 1)[0]
    encoded = "&".join(
        quote(name, safe=".-_") + "=" + quote(raw, safe=".-_")
        for name, raw in sorted(params.items())
    )
    identity = (addresses.lower(), database)
    return base + ("?" + encoded if encoded else ""), identity


def is_replica(task):
    labels = task.get("labels", [])
    if isinstance(labels, dict):
        return labels.get("type") == "replica"
    if isinstance(labels, list):
        return "type::replica" in labels
    raise ValueError("expected task labels as an array or object")


def selected_tasks(tasks):
    if not isinstance(tasks, list) or any(not isinstance(task, dict) for task in tasks):
        raise ValueError("expected the JSON array returned by the old taosX /tasks endpoint")
    selected = [task for task in tasks if not task.get("deleted") and is_replica(task)]
    if not selected:
        raise ValueError("no non-deleted replica-labelled tasks found")
    return selected


def task_start_times(values):
    if values is None:
        return {}
    result = {}
    for item in values:
        task_id, separator, value = item.partition("=")
        if not separator or not TASK_ID.fullmatch(task_id):
            raise ValueError("task start time must use TASK_ID=TIME")
        if task_id in result:
            raise ValueError("duplicate task start time")
        result[task_id] = parse_timestamp(value)
    return result


def sql_string(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def validate_task_ids(tasks):
    identifiers = set()
    for task in tasks:
        task_id = task.get("id")
        if type(task_id) is not int or not 0 < task_id <= 9223372036854775807:
            raise ValueError("task id must be a positive signed 64-bit integer")
        if task_id in identifiers:
            raise ValueError("duplicate legacy task id")
        identifiers.add(task_id)
    return identifiers


def render_with_plans(tasks, start_times=UNSET):
    selected = selected_tasks(tasks)
    if start_times is UNSET or start_times is None:
        starts = {}
    elif isinstance(start_times, dict):
        starts = {str(key): parse_timestamp(value) for key, value in start_times.items()}
    else:
        starts = {str(task["id"]): timestamp(start_times) for task in selected}
    identifiers = validate_task_ids(selected)
    mappings = set()
    creates, starts_sql = [], []
    plans = []
    for task in selected:
        task_id = task.get("id")
        if task.get("via") is not None or task.get("parser") not in (None, {}):
            raise ValueError("VIA and custom parsers cannot be migrated to replica tasks")
        if task.get("jobs", 0) not in (None, 0, 1):
            raise ValueError("parallel jobs require manual review before migration")
        task_key = str(task_id)
        start_time = starts.get(task_key, UNSET)
        try:
            source, source_identity = convert_dsn(task.get("from"), "tmq", start_time)
            sink, sink_identity = convert_dsn(task.get("to"), "taos", start_time)
        except (TypeError, ValueError) as error:
            raise ValueError("task %s: %s" % (task_id, error)) from None
        mapping = source_identity, sink_identity
        if mapping in mappings:
            raise ValueError("duplicate mapping; choose one legacy task for each mapping")
        mappings.add(mapping)
        digest = hashlib.sha256(json.dumps(mapping, separators=(",", ":")).encode("utf-8")).hexdigest()[:12]
        name = "aa_336_%s_%s" % (task_id, digest)
        creates.append(
            "CREATE XNODE TASK %s FROM %s TO %s WITH TYPE 'replica';"
            % (sql_string(name), sql_string(source), sql_string(sink))
        )
        starts_sql.append("START XNODE TASK %s;" % sql_string(name))
        plans.append({
            "legacy_id": task_id,
            "name": name,
            "from": source,
            "to": sink,
            "start_time": None if start_time is UNSET else start_time,
        })
    if set(starts) - {str(task_id) for task_id in identifiers}:
        raise ValueError("start-time task id is outside the migration scope")
    return "\n".join(creates) + "\n", "\n".join(starts_sql) + "\n", plans


def render(tasks, start_times=UNSET):
    if isinstance(start_times, dict) and not start_times:
        raise ValueError("start-time mapping must contain at least one task")
    create_sql, start_sql, _ = render_with_plans(tasks, start_times)
    return create_sql, start_sql


def read_api(base_url, timeout=30):
    base_url = base_url.rstrip("/")
    try:
        parts = urlsplit(base_url)
    except ValueError:
        raise ValueError("old taosX URL is invalid") from None
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ValueError("old taosX URL must be an HTTP or HTTPS URL")
    endpoint = urlunsplit((parts.scheme, parts.netloc, parts.path + "/tasks", "labels=type::replica", ""))
    try:
        response = urlopen(endpoint, timeout=timeout)
        with response:
            payload = response.read(16 * 1024 * 1024 + 1)
    except HTTPError as error:
        raise ValueError("old taosX API returned HTTP %s" % error.code) from None
    except (URLError, TimeoutError, OSError, ValueError):
        raise ValueError("unable to contact old taosX API") from None
    if len(payload) > 16 * 1024 * 1024:
        raise ValueError("old taosX API response is too large")
    try:
        parsed = json.loads(payload.decode("utf-8"), object_pairs_hook=unique_object)
    except (UnicodeError, json.JSONDecodeError):
        raise ValueError("old taosX API returned invalid JSON") from None
    if not isinstance(parsed, list):
        raise ValueError("old taosX API returned an unexpected response")
    return parsed


def read_sqlite(path):
    resolved = Path(path).resolve()
    uri = resolved.as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        task_columns = [row[1] for row in connection.execute("PRAGMA table_info(tasks)")]
        if not task_columns:
            raise sqlite3.Error("tasks table is missing")
        fields = ", ".join('"%s"' % column.replace('"', '""') for column in task_columns)
        where = " WHERE deleted = 0" if "deleted" in task_columns else ""
        order = " ORDER BY id" if "id" in task_columns else ""
        rows = [dict(row) for row in connection.execute("SELECT %s FROM tasks%s%s" % (fields, where, order))]
        label_columns = [row[1] for row in connection.execute("PRAGMA table_info(labels)")]
        if label_columns:
            key_column = "key" if "key" in label_columns else "name" if "name" in label_columns else None
            task_column = "id" if "id" in label_columns else "task_id" if "task_id" in label_columns else None
            if key_column and task_column and "value" in label_columns:
                labels = {}
                query = 'SELECT "%s", "%s", "value" FROM labels' % (task_column, key_column)
                for task_id, key, value in connection.execute(query):
                    labels.setdefault(task_id, []).append(key if value is None else "%s::%s" % (key, value))
                for task in rows:
                    task["labels"] = labels.get(task.get("id"), task.get("labels", []))
        for task in rows:
            if isinstance(task.get("parser"), str):
                try:
                    task["parser"] = json.loads(task["parser"], object_pairs_hook=unique_object)
                except (UnicodeError, json.JSONDecodeError):
                    raise ValueError("invalid parser JSON in SQLite backup") from None
        return rows
    finally:
        connection.close()


def snapshot_data(snapshot):
    if isinstance(snapshot, list):
        return snapshot, {}, None
    if not isinstance(snapshot, dict) or not isinstance(snapshot.get("tasks"), list):
        raise ValueError("tasks.json has an invalid snapshot format")
    return snapshot["tasks"], snapshot.get("start_times", {}), snapshot.get("old_taosx_url")


def write_secure(path, content):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as output:
            output.write(content)
    except Exception:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise


def save_generation(output, tasks, start_times, old_url, create_sql, start_sql):
    output = Path(output)
    if output.exists():
        raise ValueError("output directory already exists")
    output.mkdir(mode=0o700)
    snapshot = {
        "version": 1,
        "old_taosx_url": old_url,
        "start_times": {str(key): value for key, value in start_times.items()},
        "tasks": tasks,
    }
    try:
        write_secure(output / "tasks.json", json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n")
        write_secure(output / "create.sql", create_sql)
        write_secure(output / "start.sql", start_sql)
    except Exception:
        shutil.rmtree(output, ignore_errors=True)
        raise


def canonical_task(task):
    labels = task.get("labels", [])
    if isinstance(labels, dict):
        labels = [key if value is None else "%s::%s" % (key, value) for key, value in sorted(labels.items())]
    return {
        "id": task.get("id"),
        "name": task.get("name"),
        "from": task.get("from"),
        "to": task.get("to"),
        "labels": sorted(labels) if isinstance(labels, list) else labels,
        "via": task.get("via"),
        "parser": task.get("parser"),
        "jobs": task.get("jobs"),
    }


def verify_snapshot_live(snapshot_tasks, live_tasks, records=()):
    expected = {task.get("id"): task for task in selected_tasks(snapshot_tasks)}
    if not isinstance(live_tasks, list) or any(not isinstance(task, dict) for task in live_tasks):
        raise ValueError("old taosX API returned an unexpected task list")
    actual = {task.get("id"): task for task in live_tasks if not task.get("deleted")}
    completed = {
        record.get("id") for record in records
        if record.get("action") == "delete-confirmed" or (
            record.get("action") == "delete" and record.get("requested") is True and not record.get("failed")
        )
    }
    if any(task_id not in expected for task_id, task in actual.items() if is_replica(task)):
        raise ValueError("old taosX task set changed; export a new snapshot")
    for task_id, expected_task in expected.items():
        if task_id not in actual:
            if task_id in completed:
                continue
            raise MigrationError("old task %s is missing with an uncertain deletion result; manual review required" % task_id)
        if task_id in completed:
            raise MigrationError("deleted old task %s has reappeared; manual review required" % task_id)
        if canonical_task(expected_task) != canonical_task(actual[task_id]):
            raise ValueError("old taosX task %s changed; export a new snapshot" % task_id)
    return {task_id: task for task_id, task in actual.items() if task_id in expected}


def api_request(base_url, method, path, body=None, timeout=30, allow_not_found=False):
    endpoint = base_url.rstrip("/") + path
    try:
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = Request(endpoint, data=data, method=method)
        if body is not None:
            request.add_header("Content-Type", "application/json")
        response = urlopen(request, timeout=timeout)
        with response:
            payload = response.read(1024 * 1024)
    except HTTPError as error:
        if allow_not_found and error.code == 404:
            error.close()
            return
        raise MigrationError("old taosX API returned HTTP %s for task operation" % error.code) from None
    except (URLError, TimeoutError, OSError, ValueError):
        raise MigrationError("unable to contact old taosX API for task operation") from None
    if payload:
        try:
            result = json.loads(payload.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError):
            result = None
        if isinstance(result, dict) and (
            result.get("error") or result.get("code") not in (None, 0, "0")
        ):
            raise MigrationError("old taosX API rejected the task operation")


def operation_results_path(output):
    return Path(output) / "old-operations.json"


def save_operation_results(output, records):
    path = operation_results_path(output)
    temporary = path.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as result:
        result.write(json.dumps(records, indent=2) + "\n")
        result.flush()
        os.fsync(result.fileno())
    os.replace(temporary, path)
    if os.name == "posix":
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def load_operation_results(output):
    path = operation_results_path(output)
    if not path.exists():
        return []
    records = read_json(path)
    if not isinstance(records, list) or any(not isinstance(record, dict) for record in records):
        raise ValueError("old-operations.json has an invalid format")
    return records


def remaining_timeout(deadline, maximum=30):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise MigrationError("operation timed out; inspect saved results before retrying")
    return min(maximum, remaining)


def replica_ids(tasks):
    identifiers = set()
    for task in selected_tasks(tasks):
        labels = task.get("labels", [])
        values = [labels.get("rid")] if isinstance(labels, dict) else [label[5:] for label in labels if isinstance(label, str) and label.startswith("rid::")]
        if len(values) != 1 or not isinstance(values[0], str) or not values[0]:
            raise ValueError("old task %s has no unambiguous replica monitor id; manual review required" % task["id"])
        identifiers.add(values[0])
    return sorted(identifiers)


def operate_monitors(output, old_url, tasks, records, delete=False, deadline=None):
    for replica_id in replica_ids(tasks):
        if any(record.get("action") == "monitor-delete" and record.get("rid") == replica_id and record.get("completed") for record in records):
            continue
        action = "monitor-delete" if delete else "monitor-stop"
        records.append({"action": action + "-intent", "rid": replica_id})
        save_operation_results(output, records)
        try:
            api_request(old_url, "DELETE" if delete else "POST", "/replicas/" + quote(replica_id, safe=""),
                        body={"action": "delete", "options": {}} if delete else {"action": "stop"},
                        timeout=30 if deadline is None else remaining_timeout(deadline), allow_not_found=delete)
        except MigrationError:
            records.append({"action": action, "rid": replica_id, "failed": True})
            save_operation_results(output, records)
            raise
        records.append({"action": action, "rid": replica_id, "completed": True})
        save_operation_results(output, records)


def report_old_tasks(snapshot_tasks, live, records, action):
    completed = {record.get("id") for record in records if record.get("action") == "delete-confirmed" or (record.get("action") == "delete" and record.get("requested") is True and not record.get("failed"))}
    for task in selected_tasks(snapshot_tasks):
        task_id = task["id"]
        status = str(live.get(task_id, {}).get("status", "unknown")).lower()
        deleted = task_id in completed and task_id not in live
        result = "completed" if deleted or (action == "stop" and status in STOPPED) else "pending"
        if any(record.get("id") == task_id and record.get("failed") for record in records[-1:]):
            result = "failed"
        print("Old task %s: %s %s (status=%s)" % (task_id, action, result, "deleted" if deleted else status))


def stop_old(output, old_url, snapshot_tasks, timeout):
    deadline = time.monotonic() + timeout
    records = load_operation_results(output)
    live = {}
    try:
        live = verify_snapshot_live(snapshot_tasks, read_api(old_url, timeout=remaining_timeout(deadline)), records)
        operate_monitors(output, old_url, snapshot_tasks, records, deadline=deadline)
        live = verify_snapshot_live(snapshot_tasks, read_api(old_url, timeout=remaining_timeout(deadline)), records)
        for task_id, current in live.items():
            status = str(current.get("status", "")).lower()
            if status not in STOPPED:
                try:
                    api_request(old_url, "POST", "/tasks/%s/stop" % task_id, timeout=remaining_timeout(deadline))
                    records.append({"action": "stop", "id": task_id, "requested": True})
                except MigrationError:
                    records.append({"action": "stop", "id": task_id, "failed": True})
                    save_operation_results(output, records)
                    raise
            else:
                records.append({"action": "stop", "id": task_id, "requested": False, "status": status})
            save_operation_results(output, records)
        while True:
            live = verify_snapshot_live(snapshot_tasks, read_api(old_url, timeout=remaining_timeout(deadline)), records)
            statuses = {task_id: str(task.get("status", "")).lower() for task_id, task in live.items()}
            if all(status in STOPPED for status in statuses.values()):
                remaining_timeout(deadline)
                records.append({"action": "stop-confirmed", "statuses": statuses})
                save_operation_results(output, records)
                return list(live.values())
            time.sleep(min(2, remaining_timeout(deadline)))
    finally:
        report_old_tasks(snapshot_tasks, live, records, "stop")


def delete_old(output, old_url, snapshot_tasks):
    records = load_operation_results(output)
    live = {}
    try:
        live = verify_snapshot_live(snapshot_tasks, read_api(old_url), records)
        if any(str(task.get("status", "")).lower() not in STOPPED for task in live.values()):
            raise MigrationError("refusing to delete old tasks that are not stopped")
        operate_monitors(output, old_url, snapshot_tasks, records, delete=True)
        live = verify_snapshot_live(snapshot_tasks, read_api(old_url), records)
        if any(str(task.get("status", "")).lower() not in STOPPED for task in live.values()):
            raise MigrationError("refusing to delete old tasks that are not stopped")
        for task_id in list(live):
            records.append({"action": "delete-intent", "id": task_id})
            save_operation_results(output, records)
            try:
                api_request(old_url, "DELETE", "/tasks/%s" % task_id)
            except MigrationError:
                records.append({"action": "delete", "id": task_id, "failed": True})
                save_operation_results(output, records)
                raise
            records.append({"action": "delete", "id": task_id, "requested": True})
            save_operation_results(output, records)
            live = verify_snapshot_live(snapshot_tasks, read_api(old_url), records)
            if task_id in live:
                raise MigrationError("old task %s deletion is not confirmed; manual review required" % task_id)
            records.append({"action": "delete-confirmed", "id": task_id})
            save_operation_results(output, records)
    finally:
        report_old_tasks(snapshot_tasks, live, records, "delete")


def invoke_taos(dsn, arguments, timeout=300):
    command = ["taos", "-X", dsn] + list(arguments)
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise MigrationError("unable to execute taos client") from None
    output = (result.stdout or "") + "\n" + (result.stderr or "")
    if result.returncode != 0 or SQL_ERROR.search(output) or not SQL_SUCCESS.search(output):
        raise MigrationError("taos reported an SQL execution error")
    return output


def query_rows(dsn, sql, required, timeout=300):
    with tempfile.TemporaryDirectory(prefix="taosx-migration-") as directory:
        result_path = Path(directory) / "result.csv"
        if any(char in str(result_path) for char in ";\r\n"):
            raise MigrationError("temporary directory cannot be used for taos query output")
        write_secure(result_path, "")
        output = invoke_taos(dsn, ["-s", sql.rstrip("; ") + " >> " + str(result_path) + ";"], timeout=timeout)
        try:
            with result_path.open(encoding="utf-8", newline="") as result:
                reader = csv.DictReader(result, strict=True)
                fields = reader.fieldnames
                if fields is None:
                    if SQL_SUCCESS.search(output).group(1) == "0":
                        return []
                    raise MigrationError("taos returned no structured query result")
                if len(set(fields)) != len(fields) or not set(required).issubset(fields):
                    raise MigrationError("taos query result has unexpected columns")
                rows = list(reader)
        except (csv.Error, UnicodeError, OSError):
            raise MigrationError("unable to read structured taos query result") from None
        if any(None in row or any(value is None for value in row.values()) for row in rows):
            raise MigrationError("taos query result has incomplete rows")
        if len(rows) != int(SQL_SUCCESS.search(output).group(1)):
            raise MigrationError("taos query result is incomplete")
        return rows


def preflight_protection(dsn):
    try:
        rows = query_rows(dsn, "SHOW VARIABLES LIKE 'xnodeDsnProtection'", ("name", "value", "reboot"))
    except MigrationError:
        raise MigrationError("unable to confirm xnodeDsnProtection is disabled; automatic execution is unsupported") from None
    if len(rows) != 1 or rows[0]["name"] != "xnodeDsnProtection" or rows[0]["value"] != "0" or rows[0]["reboot"].lower() != "false":
        raise MigrationError("automatic execution requires confirmed disabled xnodeDsnProtection with no pending restart")


def query_xnode_tasks(dsn, plans, timeout=300):
    names = ", ".join(sql_string(plan["name"]) for plan in plans)
    rows = query_rows(dsn, 'SELECT name, `from`, `to`, type, status, parser, via FROM information_schema.ins_xnode_tasks WHERE name IN (%s)' % names,
                      ("name", "from", "to", "type", "status", "parser", "via"), timeout=timeout)
    tasks = {}
    for row in rows:
        if row["name"] in tasks:
            raise MigrationError("target query returned duplicate task names")
        tasks[row["name"]] = row
    return tasks


def verify_xnode_tasks(tasks, plans, require_running=False):
    for plan in plans:
        name = plan["name"]
        task = tasks.get(name)
        if task is None:
            raise MigrationError("target task %s was not found after SQL execution" % name)
        if task["type"] != "replica":
            raise MigrationError("target task %s has the wrong type" % name)
        if task["from"] != plan["from"] or task["to"] != plan["to"] or task["parser"] not in ("NULL", "{}") or task["via"] != "NULL":
            raise MigrationError("target task %s configuration does not match the migration" % name)
        if require_running and task["status"] not in RUNNING:
            raise MigrationError("target task %s did not enter running state" % name)


def sql_statements(path):
    contents = Path(path).read_text(encoding="utf-8")
    statements = [line.strip() for line in contents.splitlines() if line.strip()]
    if any(not statement.endswith(";") for statement in statements):
        raise ValueError("SQL file contains an incomplete statement")
    return statements


def save_new_results(output, results):
    path = Path(output) / "new-operations.json"
    temporary = path.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as result:
        result.write(json.dumps(list(results.values()), indent=2) + "\n")
    os.replace(temporary, path)


def execute_sql(output, dsn, mode, start_timeout=300):
    tasks, saved_times, _ = snapshot_data(read_json(Path(output) / "tasks.json"))
    create_sql, start_sql, plans = render_with_plans(tasks, saved_times)
    create_path, start_path = Path(output) / "create.sql", Path(output) / "start.sql"
    if create_path.read_text(encoding="utf-8") != create_sql or start_path.read_text(encoding="utf-8") != start_sql:
        raise ValueError("migration SQL does not match tasks.json; preserve the directory and operation records, then review the snapshot, SQL and live task state before continuing")
    results = {plan["name"]: {"name": plan["name"], "legacy_id": plan["legacy_id"], "create": "pending", "start": "pending", "status": "unknown"} for plan in plans}
    active = None
    stage = "create" if mode in ("create", "all") else "start"
    deadline = None
    try:
        save_new_results(output, results)
        preflight_protection(dsn)
        before = query_xnode_tasks(dsn, plans)
        for plan in plans:
            if plan["name"] in before:
                active = results[plan["name"]]
                verify_xnode_tasks(before, [plan])
        active = None
        if mode in ("create", "all"):
            for plan, statement in zip(plans, sql_statements(create_path)):
                active = results[plan["name"]]
                if plan["name"] not in before:
                    invoke_taos(dsn, ["-s", statement])
                    verified = query_xnode_tasks(dsn, [plan])
                    verify_xnode_tasks(verified, [plan])
                    before.update(verified)
                active.update(create="completed", status=before[plan["name"]]["status"])
                save_new_results(output, results)
            active = None
        if mode in ("start", "all"):
            stage = "start"
            before = query_xnode_tasks(dsn, plans)
            for plan in plans:
                active = results[plan["name"]]
                verify_xnode_tasks(before, [plan])
                active.update(create="completed", status=before[plan["name"]]["status"])
                if active["status"] in RUNNING:
                    active["start"] = "completed"
            active = None
            deadline = time.monotonic() + start_timeout
            for plan, statement in zip(plans, sql_statements(start_path)):
                active = results[plan["name"]]
                active.update(create="completed", status=before[plan["name"]]["status"])
                if active["status"] not in RUNNING and active["status"] != "queued":
                    invoke_taos(dsn, ["-s", statement], timeout=remaining_timeout(deadline, 300))
                if active["status"] in RUNNING:
                    active["start"] = "completed"
                save_new_results(output, results)
            active = None
            while True:
                verified = query_xnode_tasks(dsn, plans, timeout=remaining_timeout(deadline, 300))
                verify_xnode_tasks(verified, plans)
                for plan in plans:
                    result = results[plan["name"]]
                    result["status"] = verified[plan["name"]]["status"]
                    result["start"] = "completed" if result["status"] in RUNNING else "pending"
                    if result["status"] in ("failed", "deleted", "deleting", "delete_failed"):
                        result["start"] = "failed"
                save_new_results(output, results)
                if any(result["start"] == "failed" for result in results.values()):
                    raise MigrationError("one or more target tasks failed to start; inspect new-operations.json")
                remaining_timeout(deadline)
                if all(result["start"] == "completed" for result in results.values()):
                    return
                time.sleep(min(2, remaining_timeout(deadline)))
    except (MigrationError, ValueError, OSError):
        if active is not None:
            active[stage] = "failed"
        if mode in ("start", "all") and deadline is not None and time.monotonic() < deadline:
            try:
                actual = query_xnode_tasks(dsn, plans, timeout=remaining_timeout(deadline))
                for plan in plans:
                    if plan["name"] in actual:
                        verify_xnode_tasks(actual, [plan])
                        result = results[plan["name"]]
                        result["status"] = actual[plan["name"]]["status"]
                        if result["status"] in RUNNING:
                            result["start"] = "completed"
            except (MigrationError, ValueError, OSError):
                pass
        raise
    finally:
        save_new_results(output, results)
        for result in results.values():
            print("Target task %s: create=%s start=%s status=%s" % (result["name"], result["create"], result["start"], result["status"]))


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter, epilog="""Examples:
  %(prog)s --old-taosx-url http://old:6050 --output-dir migration
  %(prog)s --output-dir migration --stop-old --stop-timeout 300
  %(prog)s --output-dir migration --delete-old
  %(prog)s --output-dir migration --dsn ws://user:password@new:6041 --execute create
  %(prog)s --output-dir migration --dsn ws://user:password@new:6041 --execute start --start-timeout 300

By default only generate files. Old operations cover all tasks in the snapshot
and their associated new-database discovery monitors. Stop the old taosX service
after running any required --stop-old or --delete-old operation and before
upgrading. Generate an online snapshot while the old service is readable, or
generate one from a consistent --sqlite backup after the service is stopped.
After the upgrade, use --execute create, --execute start, or --execute all
against the new management cluster. The --sqlite mode cannot stop or delete
old tasks; perform those operations before stopping the old service or handle
them manually.
Automatic SQL execution requires disabled xnodeDsnProtection; it is rejected
before writes when protection is enabled or its effective state is unknown.
Retry the same operation with the same directory after inspecting old-operations.json
or new-operations.json. Missing tasks with unconfirmed deletions need manual review.
""")
    parser.add_argument("--output-dir", required=True, type=Path, help="migration directory (required): new for generation, existing for later operations")
    parser.add_argument("--old-taosx-url", default=None, help="old HTTP(S) API; generation default: http://127.0.0.1:6050; later operations use the saved URL and reject changes")
    parser.add_argument("--sqlite", type=Path, help="read-only consistent SQLite backup for offline generation; default: online API")
    timing = parser.add_mutually_exclusive_group()
    timing.add_argument("--start-time", help="generation only: override all task start times, Unix milliseconds or RFC 3339 with timezone; omitted inherits each old FROM DSN value, or the new task default if absent; 0 disables time filtering")
    timing.add_argument("--task-start-time", action="append", default=[], metavar="TASK_ID=TIME", help="generation only: repeat for distinct old task IDs; override listed tasks, others inherit old FROM DSN values or the new task default; same units as --start-time; mutually exclusive with it")
    parser.add_argument("--stop-old", action="store_true", help="stop all old replication tasks in the snapshot and wait until stopped; first disable their associated automatic discovery of new databases")
    parser.add_argument("--delete-old", action="store_true", help="delete all old replication tasks in the snapshot and their associated new-database discovery configuration; require all tasks to be stopped")
    parser.add_argument("--stop-delete-old", action="store_true", help="stop all old replication tasks and wait until stopped, then delete them; also disable and remove their associated new-database discovery configuration")
    parser.add_argument("--stop-timeout", type=float, default=None, metavar="SECONDS", help="finite positive total STOP budget, including requests (default: 300 seconds)")
    parser.add_argument("--execute", choices=("create", "start", "all"), help="post-upgrade SQL phase; all verifies every CREATE before START; default: no SQL execution")
    parser.add_argument("--start-timeout", type=float, default=None, metavar="SECONDS", help="finite positive START and running-state wait budget; only with --execute start/all (default: 300 seconds)")
    parser.add_argument("--dsn", help="required with --execute: management DSN passed unchanged to local taos -X")
    return parser


def main(argv=None):
    parser = build_parser()
    arguments = parser.parse_args(argv)
    old_operations = [arguments.stop_old, arguments.delete_old, arguments.stop_delete_old]
    try:
        if sum(old_operations) > 1:
            raise ValueError("--stop-old, --delete-old and --stop-delete-old are mutually exclusive")
        if arguments.stop_timeout is not None and (not math.isfinite(arguments.stop_timeout) or arguments.stop_timeout <= 0):
            raise ValueError("--stop-timeout must be a finite positive number")
        if arguments.start_timeout is not None:
            if not math.isfinite(arguments.start_timeout) or arguments.start_timeout <= 0:
                raise ValueError("--start-timeout must be a finite positive number")
            if arguments.execute not in ("start", "all"):
                raise ValueError("--start-timeout is only valid with --execute start or all")
        if arguments.execute and any(old_operations):
            raise ValueError("--execute cannot be combined with old task operations")
        if arguments.execute:
            if arguments.sqlite or arguments.old_taosx_url or arguments.start_time is not None or arguments.task_start_time or arguments.stop_timeout is not None:
                raise ValueError("--execute accepts only --output-dir, --dsn and --start-timeout for start/all")
            if not arguments.dsn:
                raise ValueError("--dsn is required with --execute")
            if not arguments.output_dir.is_dir():
                raise ValueError("--output-dir must contain an existing migration")
            execute_sql(arguments.output_dir, arguments.dsn, arguments.execute, arguments.start_timeout or 300)
            print("Executed %s successfully" % arguments.execute)
            return 0
        if arguments.dsn:
            raise ValueError("--dsn is only valid with --execute")
        starts = {}
        if arguments.start_time is not None:
            starts = {"__all__": parse_timestamp(arguments.start_time)}
        elif arguments.task_start_time:
            starts = task_start_times(arguments.task_start_time)
        has_old_operation = any(old_operations)
        output_exists = arguments.output_dir.exists()
        if output_exists and not has_old_operation:
            raise ValueError("output directory already exists; choose an operation or a new directory")
        if arguments.stop_timeout is not None and not has_old_operation:
            raise ValueError("--stop-timeout requires --stop-old, --delete-old or --stop-delete-old")
        if arguments.sqlite and (has_old_operation or arguments.old_taosx_url):
            raise ValueError("--sqlite cannot be combined with old task operations or --old-taosx-url")
        old_url = arguments.old_taosx_url or "http://127.0.0.1:6050"
        if has_old_operation and output_exists:
            snapshot = read_json(arguments.output_dir / "tasks.json")
            tasks, saved_times, saved_url = snapshot_data(snapshot)
            if not saved_url:
                raise ValueError("snapshot has no old service address; manual old-task operations are required")
            if saved_url and arguments.old_taosx_url and saved_url.rstrip("/") != old_url.rstrip("/"):
                raise ValueError("old taosX instance does not match the snapshot")
            old_url = saved_url
            if arguments.start_time is not None or arguments.task_start_time:
                raise ValueError("start time options are only valid when generating a migration")
        else:
            if arguments.sqlite:
                tasks = read_sqlite(arguments.sqlite)
                old_url_for_snapshot = None
            else:
                tasks = read_api(old_url)
                old_url_for_snapshot = old_url
            selected = selected_tasks(tasks)
            validate_task_ids(selected)
            # Sort only new generations; existing snapshots retain their SQL order.
            selected.sort(key=lambda task: task["id"])
            explicit_starts = {} if "__all__" not in starts else {
                str(task["id"]): starts["__all__"] for task in selected
            }
            if "__all__" not in starts:
                explicit_starts = starts
            create_sql, start_sql, _ = render_with_plans(selected, explicit_starts)
            save_generation(arguments.output_dir, selected, explicit_starts, old_url_for_snapshot, create_sql, start_sql)
            tasks, saved_times, saved_url = selected, explicit_starts, old_url_for_snapshot
        if has_old_operation:
            if arguments.sqlite:
                raise ValueError("offline SQLite mode cannot operate old tasks")
            if arguments.stop_old or arguments.stop_delete_old:
                stop_old(arguments.output_dir, old_url, tasks, arguments.stop_timeout or 300)
            if arguments.delete_old or arguments.stop_delete_old:
                delete_old(arguments.output_dir, old_url, tasks)
            print("Old task operation completed")
        else:
            print("Generated tasks.json, create.sql and start.sql in %s" % arguments.output_dir)
        return 0
    except (ValueError, OSError, sqlite3.Error, MigrationError) as error:
        print("Migration failed: %s" % error, file=sys.stderr)
        usage_error = any(marker in str(error) for marker in (
            "cannot be combined", "mutually exclusive", "only valid", "requires --",
        ))
        return 2 if usage_error else 1


if __name__ == "__main__":
    sys.exit(main())
