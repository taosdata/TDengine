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
import csv
from contextlib import contextmanager, redirect_stdout
from http.server import BaseHTTPRequestHandler, HTTPServer
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "migrate_active_active.py"


def legacy_task(task_id=7):
    return {
        "id": task_id,
        "name": "replica-power.archive",
        "from": (
            "tmq+ws://root:p%40ss@td1:6041/power?replica=&timeout=never"
            "&prefer=raw&enable.wal.marker=true&group.id=old_group"
            "&use.topic.name=old_topic&client.id=old_client"
            "&msg.consume.excluded=1&commit.interval.ms=2000"
        ),
        "to": "taos+ws://root:secret@td2:6041/archive",
        "labels": ["type::replica", "rid::old_replica", "database::power.archive"],
        "status": "stopped",
    }


@contextmanager
def legacy_api(tasks=None, status=200, body=None):
    requests = []
    payload = json.dumps(tasks).encode("utf-8") if body is None else body

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format, *arguments):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    worker.start()
    try:
        yield "http://127.0.0.1:%s" % server.server_port, requests
    finally:
        server.shutdown()
        worker.join()
        server.server_close()


class MigrationTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("migration", SCRIPT)
        self.migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.migration)

    def test_rebuilds_mapping_with_new_start_without_old_subscription_identity(self):
        create_sql, start_sql = self.migration.render([legacy_task()], 1700000000000)
        self.assertIn(
            "FROM 'tmq+ws://root:p%40ss@td1:6041/power?commit.interval.ms=2000"
            "&msg.consume.start.ts=1700000000000'", create_sql
        )
        self.assertIn("TO 'taos+ws://root:secret@td2:6041/archive'", create_sql)
        self.assertIn("WITH TYPE 'replica';", create_sql)
        for old_value in ("old_group", "old_topic", "old_client", "prefer=", "replica=", "excluded="):
            self.assertNotIn(old_value, create_sql)
        self.assertNotIn("START XNODE", create_sql)
        self.assertIn("START XNODE TASK 'aa_336_7_", start_sql)

    def test_preserves_query_plus_characters_and_encoded_credentials(self):
        task = legacy_task()
        task["from"] += "&token=abc+def%26ghi"
        create_sql, _ = self.migration.render([task], 0)
        self.assertIn("token=abc%2Bdef%26ghi", create_sql)
        self.assertIn("p%40ss@", create_sql)

    def test_quotes_sql_and_preserves_multi_address_native_and_ipv6_endpoints(self):
        task = legacy_task()
        task["from"] = "tmq://user:p'ass@[::1]:6030,td1:6030/power"
        task["to"] = "taos://root:secret@[::2]:6030,td2:6030/archive"
        create_sql, _ = self.migration.render([task], 0)
        self.assertIn("user:p''ass@[::1]:6030,td1:6030/power?msg.consume.start.ts=0", create_sql)

    def test_preserves_backslashes_in_sql_credentials(self):
        task = legacy_task()
        task["from"] = r"tmq://user:p\ass@td1:6030/power"
        create_sql, _ = self.migration.render([task], 0)
        self.assertIn(r"FROM 'tmq://user:p\\ass@td1:6030/power?", create_sql)

    def test_uses_per_task_timestamps_and_retains_subscription_options(self):
        forward, reverse = legacy_task(), legacy_task(8)
        reverse["from"] = "tmq+ws://root:secret@td2:6041/archive?experimental.snapshot.enable=false"
        reverse["to"] = "taos+ws://root:secret@td1:6041/power"
        create_sql, start_sql = self.migration.render(
            [forward, reverse], {"7": 1700000000000, "8": 1700000001000}
        )
        self.assertIn("msg.consume.start.ts=1700000000000", create_sql)
        self.assertIn("experimental.snapshot.enable=false&msg.consume.start.ts=1700000001000", create_sql)
        self.assertEqual(start_sql.count("START XNODE TASK"), 2)

    def test_inherits_valid_source_start_times_without_changing_target(self):
        for value in ("0", "1700000000000", "9223372036854775807", "000123"):
            with self.subTest(value=value):
                task = legacy_task()
                task["from"] += "&msg.consume.start.ts=" + value
                create_sql, _, plans = self.migration.render_with_plans([task])
                self.assertIn("msg.consume.start.ts=" + value, create_sql)
                self.assertEqual(plans[0]["to"], task["to"])
        create_sql, _ = self.migration.render([legacy_task()])
        self.assertNotIn("msg.consume.start.ts", create_sql)

    def test_rejects_invalid_inherited_start_times_without_exposing_values(self):
        for value in ("-1", "-0", "+1", "1.5", "2026-01-01T00:00:00Z", "", "9223372036854775808", "secret"):
            with self.subTest(value=value):
                task = legacy_task()
                task["from"] += "&msg.consume.start.ts=" + value
                with self.assertRaisesRegex(ValueError, "invalid msg.consume.start.ts") as caught:
                    self.migration.render([task])
                self.assertNotIn("secret", str(caught.exception))

    def test_explicit_time_overrides_old_value_before_validation(self):
        for old_value in ("1700000000000", "0", "invalid"):
            for new_value in (0, 1800000000000):
                with self.subTest(old_value=old_value, new_value=new_value):
                    task = legacy_task()
                    task["from"] += "&msg.consume.start.ts=" + old_value
                    create_sql, _ = self.migration.render([task], new_value)
                    self.assertEqual(create_sql.count("msg.consume.start.ts="), 1)
                    self.assertIn("msg.consume.start.ts=%s'" % new_value, create_sql)

    def test_partial_task_times_preserve_other_inherited_or_missing_values(self):
        tasks = [legacy_task(task_id) for task_id in (7, 8, 9, 10)]
        for task in tasks:
            task["to"] = "taos+ws://root:secret@td%s:6041/archive" % task["id"]
        tasks[0]["from"] += "&msg.consume.start.ts=100"
        tasks[1]["from"] += "&msg.consume.start.ts=200"
        tasks[2]["from"] += "&msg.consume.start.ts=0"
        create_sql, _, plans = self.migration.render_with_plans(tasks, {"7": "2026-01-01T00:00:00Z"})
        for plan, expected in zip(plans[:3], ("1767225600000", "200", "0")):
            self.assertIn("msg.consume.start.ts=" + expected, plan["from"])
        self.assertNotIn("msg.consume.start.ts", plans[3]["from"])
        self.assertEqual(create_sql.count("msg.consume.start.ts="), 3)

    def test_rawdata_conflicts_use_effective_start_time(self):
        for rawdata in ("-2147483648", "1", "2147483647"):
            with self.subTest(rawdata=rawdata):
                task = legacy_task()
                task["from"] += "&msg.consume.start.ts=100&msg.consume.rawdata=" + rawdata
                with self.assertRaisesRegex(ValueError, "conflicts"):
                    self.migration.render([task])
                with self.assertRaisesRegex(ValueError, "conflicts"):
                    self.migration.render([task], 200)
                create_sql, _ = self.migration.render([task], 0)
                self.assertIn("msg.consume.start.ts=0", create_sql)
                for source in (task["from"].replace("start.ts=100", "start.ts=0"), task["from"].replace("&msg.consume.start.ts=100", "")):
                    task["from"] = source
                    self.migration.render([task])
        task = legacy_task()
        task["from"] += "&msg.consume.start.ts=100&msg.consume.rawdata=0"
        self.migration.render([task])

    def test_target_start_time_is_still_rejected(self):
        task = legacy_task()
        task["to"] += "?msg.consume.start.ts=0"
        with self.assertRaisesRegex(ValueError, "unsupported taos DSN parameter"):
            self.migration.render([task], 0)

    def test_fan_out_creates_independent_tasks_without_the_old_shared_group(self):
        first, second = legacy_task(), legacy_task(8)
        second["to"] = "taos+ws://root:secret@td3:6041/archive"
        create_sql, start_sql = self.migration.render([first, second], 0)
        self.assertEqual(create_sql.count("CREATE XNODE TASK"), 2)
        self.assertEqual(len(set(start_sql.splitlines())), 2)
        self.assertNotIn("group.id", create_sql)

    def test_rejects_start_time_keys_that_do_not_match_selected_tasks(self):
        with self.assertRaises(ValueError):
            self.migration.render([legacy_task()], {"7": 0, "8": 0})

    def test_ignores_normal_and_deleted_tasks(self):
        normal = legacy_task(9)
        normal["labels"] = ["type::datain"]
        deleted = legacy_task(10)
        deleted["deleted"] = True
        create_sql, _ = self.migration.render([normal, deleted, legacy_task()], 0)
        self.assertEqual(create_sql.count("CREATE XNODE TASK"), 1)

    def test_accepts_object_labels_and_preserves_allowed_target_connection_parameters(self):
        task = legacy_task()
        task["labels"] = {"type": "replica", "rid": "old_replica"}
        task["to"] += "?tls_mode=verify&token=abc+def&replica=1"
        create_sql, _ = self.migration.render([task], 0)
        self.assertIn("archive?tls_mode=verify&token=abc%2Bdef'", create_sql)
        self.assertNotIn("replica=1", create_sql)

    def test_rejects_input_that_cannot_be_migrated_faithfully(self):
        for field, value in (
            ("from", "tmq+ws://root:very_secret@td1:6041/power?unsupported=1"),
            ("from", "tmq+ws://root:very_secret@td1:6041/power?enable.auto.commit=true"),
            ("from", "tmq+ws://root:very_secret@td1:6041/power?msg.consume.rawdata=1"),
            ("from", "tmq+ws://root:very_secret@td1:6041/power?auto.offset.reset=latest"),
            ("from", "tmq+ws://root:very_secret@td1:6041/power?group.id=a&group.id=b"),
            ("from", "tmq+ws://root:very_secret@td1:6041/"),
            ("from", "tmq+ws://root:very_secret@td1:6041/power#fragment"),
            ("from", "tmq+ws://root:very_secret@td1:6041/power?token=%zz"),
            ("from", {"type": "tmq", "data": {}}),
            ("via", 12),
            ("parser", {"model": "custom"}),
            ("jobs", 2),
        ):
            with self.subTest(field=field, value=value):
                task = legacy_task()
                task[field] = value
                with self.assertRaises(ValueError) as caught:
                    self.migration.render([task], 1700000000000)
                self.assertNotIn("very_secret", str(caught.exception))

    def test_rejects_invalid_and_missing_start_times(self):
        for start_time in (-1, True, 1.5, "today", 9223372036854775808, {}, {"8": 0}):
            with self.subTest(start_time=start_time), self.assertRaises(ValueError):
                self.migration.render([legacy_task()], start_time)

    def test_rejects_duplicate_mappings_even_with_different_credentials(self):
        duplicate = legacy_task(8)
        duplicate["from"] = "tmq+ws://other:password@TD1:6041/power"
        with self.assertRaisesRegex(ValueError, "duplicate mapping"):
            self.migration.render([legacy_task(), duplicate], 0)

    def test_rejects_duplicate_ids_and_empty_replica_selection(self):
        for tasks in ([legacy_task(), legacy_task()], []):
            with self.subTest(tasks=tasks), self.assertRaises(ValueError):
                self.migration.render(tasks, 0)

    def test_names_stay_stable_after_credential_rotation(self):
        task = legacy_task()
        _, original = self.migration.render([task], 0)
        rotated = copy.deepcopy(task)
        rotated["from"] = rotated["from"].replace("p%40ss", "new_password")
        _, updated = self.migration.render([rotated], 0)
        self.assertEqual(original, updated)

    def test_reads_original_sqlite_tables_without_modifying_database(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "taosx.db"
            task = legacy_task()
            with sqlite3.connect(database) as connection:
                connection.execute(
                    'CREATE TABLE tasks (id INTEGER, name TEXT, "from" TEXT, "to" TEXT, '
                    'deleted BOOLEAN, via INTEGER, parser TEXT, jobs INTEGER)'
                )
                connection.execute('CREATE TABLE labels (id INTEGER, "key" TEXT, value TEXT)')
                connection.execute(
                    'INSERT INTO tasks VALUES (?, ?, ?, ?, 0, NULL, NULL, 0)',
                    (task["id"], task["name"], task["from"], task["to"]),
                )
                connection.execute("INSERT INTO labels VALUES (7, 'type', 'replica')")
            before = database.read_bytes()
            tasks = self.migration.read_sqlite(database)
            create_sql, _ = self.migration.render(tasks, 0)
            self.assertIn("WITH TYPE 'replica'", create_sql)
            self.assertEqual(before, database.read_bytes())

    def test_missing_sqlite_input_is_not_created(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing.db"
            with self.assertRaises(sqlite3.Error):
                self.migration.read_sqlite(database)
            self.assertFalse(database.exists())

    def test_api_timeout_does_not_expose_connection_details(self):
        with patch.object(self.migration, "urlopen", side_effect=TimeoutError("very_secret")):
            with self.assertRaises(ValueError) as caught:
                self.migration.read_api("http://127.0.0.1:6050")
        self.assertNotIn("very_secret", str(caught.exception))

    def test_uses_local_api_by_default(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "migration"
            response = io.BytesIO(json.dumps([legacy_task()]).encode("utf-8"))
            arguments = [str(SCRIPT), "--output-dir", str(output)]
            with patch.object(self.migration, "urlopen", return_value=response) as request:
                with patch.object(sys, "argv", arguments), redirect_stdout(io.StringIO()):
                    self.assertEqual(self.migration.main(), 0)
            request.assert_called_once_with("http://127.0.0.1:6050/tasks?labels=type::replica", timeout=30)
            self.assertIn("WITH TYPE 'replica'", (output / "create.sql").read_text())
            self.assertNotIn("msg.consume.start.ts", (output / "create.sql").read_text())
            self.assertEqual(json.loads((output / "tasks.json").read_text())["start_times"], {})


class CommandLineTests(unittest.TestCase):
    def invoke(self, *arguments):
        return subprocess.run(
            [sys.executable, str(SCRIPT), *map(str, arguments)],
            capture_output=True, text=True, check=False,
        )

    def test_help_describes_inheritance_and_only_current_options(self):
        for option in ("-h", "--help"):
            with self.subTest(option=option):
                result = self.invoke(option)
                self.assertEqual(result.returncode, 0, result.stderr)
                help_text = " ".join(result.stdout.split())
                self.assertIn("omitted inherits each old FROM DSN value", help_text)
                self.assertIn("0 disables time filtering", help_text)
                self.assertIn("--old-taosx-url", help_text)
                self.assertIn("--task-start-time", help_text)
                self.assertIn("Generate an online snapshot while the old service is readable", help_text)
                self.assertIn("The --sqlite mode cannot stop or delete old tasks", help_text)
                self.assertIn("use --execute create, --execute start, or --execute all", help_text)
                self.assertNotIn("--taosx-url", help_text)
                self.assertNotIn("--start-times", help_text)

    def test_writes_complete_create_statements_on_separate_lines(self):
        first, second = legacy_task(), legacy_task(8)
        second["to"] = "taos+ws://root:secret@td3:6041/archive"
        with tempfile.TemporaryDirectory() as directory, legacy_api([first, second]) as (url, requests):
            output = Path(directory) / "migration"
            result = self.invoke("--old-taosx-url", url, "--start-time", 0, "--output-dir", output)
            self.assertEqual(result.returncode, 0, result.stderr)
            create_sql = (output / "create.sql").read_text(encoding="utf-8")
            self.assertTrue(create_sql.endswith("\n"))
            statements = [line for line in create_sql.splitlines() if line]
            self.assertEqual(len(statements), 2)
            for statement in statements:
                self.assertRegex(
                    statement, r"^CREATE XNODE TASK '[^']+' FROM '.+' TO '.+' WITH TYPE 'replica';$"
                )

    def test_writes_private_separate_sql_files_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as directory, legacy_api([legacy_task()]) as (url, requests):
            output = Path(directory) / "migration"
            result = self.invoke("--old-taosx-url", url, "--start-time", 0, "--output-dir", output)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn("secret", result.stdout + result.stderr)
            for filename in ("tasks.json", "create.sql", "start.sql"):
                artifact = output / filename
                self.assertTrue(artifact.is_file())
                if os.name == "posix":
                    self.assertEqual(artifact.stat().st_mode & 0o777, 0o600)
            original = (output / "create.sql").read_bytes()
            result = self.invoke("--old-taosx-url", url, "--start-time", 1, "--output-dir", output)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(original, (output / "create.sql").read_bytes())

    def test_validates_all_tasks_before_creating_output(self):
        invalid = legacy_task(8)
        invalid["from"] += "&unknown=secret"
        with tempfile.TemporaryDirectory() as directory, legacy_api([legacy_task(), invalid]) as (url, requests):
            output = Path(directory) / "migration"
            result = self.invoke("--old-taosx-url", url, "--start-time", 0, "--output-dir", output)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn("secret", result.stdout + result.stderr)
            self.assertFalse(output.exists())

    def test_removed_options_are_unknown_before_network_or_output(self):
        with tempfile.TemporaryDirectory() as directory, legacy_api([legacy_task()]) as (url, requests):
            for option, value in (("--taosx-url", url), ("--start-times", Path(directory) / "missing.json")):
                with self.subTest(option=option):
                    output = Path(directory) / "migration"
                    result = self.invoke(option, value, "--old-taosx-url", url, "--start-time", 0, "--output-dir", output)
                    self.assertEqual(result.returncode, 2, result.stderr)
                    self.assertIn("unrecognized arguments: " + option, result.stderr)
                    self.assertEqual(requests, [])
                    self.assertFalse(output.exists())
                    output.mkdir()
                    record = output / "old-operations.json"
                    record.write_text("[]\n", encoding="utf-8")
                    for operation in (["--stop-delete-old"], ["--execute", "all", "--dsn", "ws://management"]):
                        result = self.invoke(option, value, "--output-dir", output, *operation)
                        self.assertEqual(result.returncode, 2, result.stderr)
                        self.assertIn("unrecognized arguments: " + option, result.stderr)
                        self.assertEqual(requests, [])
                        self.assertEqual(list(output.iterdir()), [record])
                        self.assertEqual(record.read_text(), "[]\n")
                    record.unlink()
                    output.rmdir()

    def test_rejects_ambiguous_duplicate_task_start_times(self):
        with tempfile.TemporaryDirectory() as directory, legacy_api([legacy_task()]) as (url, requests):
            output = Path(directory) / "migration"
            result = self.invoke("--old-taosx-url", url, "--task-start-time", "7=0", "--task-start-time", "7=1700000000000", "--output-dir", output)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("duplicate task start time", result.stderr)
            self.assertEqual(requests, [])
            self.assertFalse(output.exists())

    def test_fetches_replica_tasks_without_a_json_input_file(self):
        normal, deleted = legacy_task(8), legacy_task(9)
        normal["labels"] = ["type::normal"]
        deleted["deleted"] = True
        with tempfile.TemporaryDirectory() as directory, legacy_api([legacy_task(), normal, deleted]) as (url, requests):
            output = Path(directory) / "migration"
            result = self.invoke("--old-taosx-url", url + "/legacy/", "--start-time", 0, "--output-dir", output)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(requests, ["/legacy/tasks?labels=type::replica"])
            self.assertEqual((output / "create.sql").read_text().count("CREATE XNODE TASK"), 1)
            self.assertEqual({path.name for path in Path(directory).iterdir()}, {"migration"})
            self.assertEqual({path.name for path in output.iterdir()}, {"tasks.json", "create.sql", "start.sql"})

    def test_api_errors_do_not_create_output_or_expose_response_credentials(self):
        for status in (401, 500):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                with legacy_api(status=status, body=b"very_secret") as (url, requests):
                    output = Path(directory) / "migration"
                    result = self.invoke("--old-taosx-url", url, "--start-time", 0, "--output-dir", output)
                    self.assertEqual(result.returncode, 1, result.stderr)
                    self.assertIn("HTTP %s" % status, result.stderr)
                    self.assertNotIn("very_secret", result.stdout + result.stderr)
                    self.assertNotIn("Traceback", result.stderr)
                    self.assertFalse(output.exists())

    def test_rejects_invalid_api_responses_without_partial_output(self):
        for body in (b'{"error":"very_secret"}', b'[{"id":7,"id":8}]', b"very_secret", b"\xff"):
            with self.subTest(body=body), tempfile.TemporaryDirectory() as directory:
                with legacy_api(body=body) as (url, requests):
                    output = Path(directory) / "migration"
                    result = self.invoke("--old-taosx-url", url, "--start-time", 0, "--output-dir", output)
                    self.assertEqual(result.returncode, 1, result.stderr)
                    self.assertNotIn("very_secret", result.stdout + result.stderr)
                    self.assertNotIn("Traceback", result.stderr)
                    self.assertFalse(output.exists())

    def test_api_connection_failure_does_not_create_output(self):
        with legacy_api([]) as (url, requests):
            pass
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "migration"
            result = self.invoke("--old-taosx-url", url, "--start-time", 0, "--output-dir", output)
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertNotIn("Traceback", result.stderr)
            self.assertFalse(output.exists())

    def test_rejects_combined_api_and_sqlite_inputs(self):
        with tempfile.TemporaryDirectory() as directory, legacy_api([legacy_task()]) as (url, requests):
            output = Path(directory) / "migration"
            result = self.invoke(
                "--old-taosx-url", url, "--sqlite", Path(directory) / "taosx.db",
                "--start-time", 0, "--output-dir", output,
            )
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertEqual(requests, [])
            self.assertFalse(output.exists())

    def test_sqlite_mode_generates_sql_without_contacting_the_api(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "taosx.db"
            task = legacy_task()
            with sqlite3.connect(database) as connection:
                connection.execute('CREATE TABLE tasks (id INTEGER, "from" TEXT, "to" TEXT, deleted BOOLEAN)')
                connection.execute('CREATE TABLE labels (id INTEGER, "key" TEXT, value TEXT)')
                connection.execute('INSERT INTO tasks VALUES (7, ?, ?, 0)', (task["from"], task["to"]))
                connection.execute("INSERT INTO labels VALUES (7, 'type', 'replica')")
            original = database.read_bytes()
            output = Path(directory) / "migration"
            result = self.invoke("--sqlite", database, "--start-time", 0, "--output-dir", output)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("WITH TYPE 'replica'", (output / "create.sql").read_text())
            self.assertEqual(database.read_bytes(), original)

    def test_online_and_sqlite_generation_use_the_same_numeric_task_order(self):
        tasks = [legacy_task(10), legacy_task(2)]
        tasks[0]["to"] = "taos+ws://root:secret@td10:6041/archive"
        tasks[0]["from"] += "&msg.consume.start.ts=100"
        tasks[1]["from"] += "&msg.consume.start.ts=0"
        with tempfile.TemporaryDirectory() as directory, legacy_api(tasks) as (url, requests):
            root = Path(directory)
            database = root / "taosx.db"
            with sqlite3.connect(database) as connection:
                connection.execute('CREATE TABLE tasks (id INTEGER, "from" TEXT, "to" TEXT, deleted BOOLEAN)')
                connection.execute('CREATE TABLE labels (id INTEGER, "key" TEXT, value TEXT)')
                for task in tasks:
                    connection.execute('INSERT INTO tasks VALUES (?, ?, ?, 0)', (task["id"], task["from"], task["to"]))
                    connection.execute("INSERT INTO labels VALUES (?, 'type', 'replica')", (task["id"],))
            before = database.read_bytes()
            for mode, timing, expected, explicit in (
                ("inherited", [], ("0", "100"), {}),
                ("zero", ["--start-time", "0"], ("0", "0"), {"2": 0, "10": 0}),
                ("global", ["--start-time", "1700000000000"], ("1700000000000", "1700000000000"), {"2": 1700000000000, "10": 1700000000000}),
                ("rfc3339", ["--start-time", "2023-11-14T22:13:20Z"], ("1700000000000", "1700000000000"), {"2": 1700000000000, "10": 1700000000000}),
                ("per-task", ["--task-start-time", "10=1700000000000", "--task-start-time", "2=0"], ("0", "1700000000000"), {"2": 0, "10": 1700000000000}),
                ("partial", ["--task-start-time", "2=200"], ("200", "100"), {"2": 200}),
            ):
                with self.subTest(mode=mode):
                    online, offline = root / (mode + "-online"), root / (mode + "-offline")
                    for output, source in ((online, ["--old-taosx-url", url]), (offline, ["--sqlite", database])):
                        result = self.invoke(*source, *timing, "--output-dir", output)
                        self.assertEqual(result.returncode, 0, result.stderr)
                    for name in ("create.sql", "start.sql"):
                        self.assertEqual((online / name).read_bytes(), (offline / name).read_bytes())
                        lines = (online / name).read_text().splitlines()
                        self.assertEqual(len(lines), 2)
                        self.assertIn("'aa_336_2_", lines[0])
                        self.assertIn("'aa_336_10_", lines[1])
                    lines = (online / "create.sql").read_text().splitlines()
                    for line, value in zip(lines, expected):
                        self.assertIn("msg.consume.start.ts=%s'" % value, line)
                    for output in (online, offline):
                        snapshot = json.loads((output / "tasks.json").read_text())
                        self.assertEqual([task["id"] for task in snapshot["tasks"]], [2, 10])
                        for saved, original in zip(snapshot["tasks"], sorted(tasks, key=lambda task: task["id"])):
                            self.assertEqual(saved["from"], original["from"])
                            self.assertEqual(saved["to"], original["to"])
                        if output == online:
                            self.assertEqual(snapshot["tasks"], sorted(tasks, key=lambda task: task["id"]))
                        self.assertEqual(snapshot["start_times"], explicit)
            self.assertEqual(database.read_bytes(), before)

    def test_invalid_inherited_times_and_conflicts_leave_no_partial_output(self):
        for parameters in ("msg.consume.start.ts=-1", "msg.consume.start.ts=1.5", "msg.consume.start.ts=2026-01-01T00:00:00Z", "msg.consume.start.ts=", "msg.consume.start.ts=9223372036854775808", "msg.consume.start.ts=100&msg.consume.rawdata=1"):
            with self.subTest(parameters=parameters), tempfile.TemporaryDirectory() as directory:
                invalid = legacy_task(8)
                invalid["to"] = "taos+ws://root:secret@td3:6041/archive"
                invalid["from"] += "&" + parameters
                with legacy_api([legacy_task(), invalid]) as (url, requests):
                    output = Path(directory) / "migration"
                    result = self.invoke("--old-taosx-url", url, "--output-dir", output)
                    self.assertEqual(result.returncode, 1, result.stderr)
                    self.assertFalse(output.exists())

    def test_generation_rejects_invalid_or_duplicate_ids_without_partial_output(self):
        for task_id in (None, True, "8", 0, -1, 9223372036854775808, 7):
            with self.subTest(task_id=task_id), tempfile.TemporaryDirectory() as directory:
                with legacy_api([legacy_task(), legacy_task(task_id)]) as (url, requests):
                    output = Path(directory) / "migration"
                    result = self.invoke("--old-taosx-url", url, "--output-dir", output)
                    self.assertEqual(result.returncode, 1, result.stderr)
                    message = "duplicate legacy task id" if task_id == 7 else "task id must be a positive signed 64-bit integer"
                    self.assertIn(message, result.stderr)
                    self.assertNotIn("Traceback", result.stderr)
                    self.assertFalse(output.exists())


class OldOperationTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("migration", SCRIPT)
        self.migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.migration)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.output = Path(self.directory.name) / "migration"
        self.tasks = [legacy_task(), legacy_task(8)]
        self.tasks[1]["to"] = "taos+ws://root:secret@td3:6041/archive"
        create_sql, start_sql, _ = self.migration.render_with_plans(self.tasks, {})
        self.migration.save_generation(self.output, self.tasks, {}, "http://old-remote:6050", create_sql, start_sql)
        self.live = copy.deepcopy(self.tasks)
        self.requests = []

    @contextmanager
    def service(self, fail_task=None):
        def operate(base_url, method, path, **options):
            self.requests.append((base_url, method, path, options))
            if path.startswith("/tasks/"):
                task_id = int(path.split("/")[2])
                if task_id == fail_task:
                    raise self.migration.MigrationError("simulated operation failure")
                if method == "DELETE":
                    self.live[:] = [task for task in self.live if task["id"] != task_id]
                else:
                    next(task for task in self.live if task["id"] == task_id)["status"] = "stopped"
        with patch.object(self.migration, "read_api", side_effect=lambda *args, **kwargs: copy.deepcopy(self.live)), patch.object(self.migration, "api_request", side_effect=operate):
            yield

    def test_rejects_read_concurrency_before_generating_files(self):
        task = legacy_task()
        task["from"] += "&read_concurrency=2"
        with self.assertRaisesRegex(ValueError, "task 7:.*read_concurrency"):
            self.migration.render_with_plans([task], {})

    def test_stops_deduplicated_monitor_before_stopping_tasks(self):
        for task in self.live:
            task["status"] = "running"
        with self.service():
            self.migration.stop_old(self.output, "http://old-remote:6050", self.tasks, 2)
        self.assertEqual([(method, path) for _, method, path, _ in self.requests], [
            ("POST", "/replicas/old_replica"), ("POST", "/tasks/7/stop"), ("POST", "/tasks/8/stop"),
        ])
        self.assertEqual(self.requests[0][3]["body"], {"action": "stop"})

    def test_partial_delete_can_resume_including_combined_stop_phase(self):
        with self.service(fail_task=8):
            with self.assertRaises(self.migration.MigrationError):
                self.migration.delete_old(self.output, "http://old-remote:6050", self.tasks)
        self.assertEqual([task["id"] for task in self.live], [8])
        self.requests.clear()
        with self.service():
            self.migration.stop_old(self.output, "http://old-remote:6050", self.tasks, 2)
            self.migration.delete_old(self.output, "http://old-remote:6050", self.tasks)
            self.migration.delete_old(self.output, "http://old-remote:6050", self.tasks)
        self.assertEqual(self.live, [])
        self.assertEqual([path for _, method, path, _ in self.requests if method == "DELETE" and path.startswith("/tasks/")], ["/tasks/8"])

    def test_delete_monitor_precedes_task_deletion(self):
        with self.service():
            self.migration.delete_old(self.output, "http://old-remote:6050", self.tasks)
        self.assertEqual(self.requests[0][1:3], ("DELETE", "/replicas/old_replica"))

    def test_delete_monitor_sends_json_action_and_options(self):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_DELETE(self):
                length = int(self.headers.get("Content-Length", "0"))
                payload = self.rfile.read(length)
                requests.append((
                    self.path,
                    self.headers.get("Content-Type"),
                    json.loads(payload) if payload else None,
                ))
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"code":0}')

            def log_message(self, format, *arguments):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        worker.start()
        try:
            self.migration.operate_monitors(
                self.output, "http://127.0.0.1:%s" % server.server_port, self.tasks, [], delete=True
            )
        finally:
            server.shutdown()
            worker.join()
            server.server_close()

        self.assertEqual(requests, [
            ("/replicas/old_replica", "application/json", {"action": "delete", "options": {}}),
        ])

    def test_delete_continues_when_monitor_is_already_missing(self):
        live = copy.deepcopy(self.tasks)

        class Handler(BaseHTTPRequestHandler):
            def reply(self, status, payload):
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                self.reply(200, live)

            def do_DELETE(self):
                length = int(self.headers.get("Content-Length", "0"))
                self.rfile.read(length)
                if self.path == "/replicas/old_replica":
                    self.reply(404, {"code": 65535, "message": "No replica monitor found"})
                    return
                task_id = int(self.path.rsplit("/", 1)[1])
                live[:] = [task for task in live if task["id"] != task_id]
                self.reply(200, {"code": 0})

            def log_message(self, format, *arguments):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        worker.start()
        caught = None
        try:
            self.migration.delete_old(
                self.output, "http://127.0.0.1:%s" % server.server_port, self.tasks
            )
        except self.migration.MigrationError as error:
            caught = error
        finally:
            server.shutdown()
            worker.join()
            server.server_close()

        self.assertIsNone(caught, "delete_old rejected an already missing monitor: %s" % caught)
        self.assertEqual(live, [])
        records = self.migration.load_operation_results(self.output)
        self.assertTrue(any(
            record.get("action") == "monitor-delete" and record.get("completed") for record in records
        ))
        self.assertEqual(
            [record["id"] for record in records if record.get("action") == "delete-confirmed"], [7, 8]
        )

    def test_unconfirmed_missing_task_requires_manual_review(self):
        self.migration.save_operation_results(self.output, [{"action": "delete-intent", "id": 7}])
        self.live.pop(0)
        with self.service(), self.assertRaisesRegex((ValueError, self.migration.MigrationError), "uncertain|manual"):
            self.migration.delete_old(self.output, "http://old-remote:6050", self.tasks)
        self.assertEqual(self.requests, [])

    def test_delete_intent_is_durable_before_request(self):
        def fail_request(base_url, method, path, **options):
            if path.startswith("/replicas/"):
                return
            records = self.migration.load_operation_results(self.output)
            self.assertTrue(any(record.get("action") == "delete-intent" and record.get("id") == 7 for record in records))
            raise self.migration.MigrationError("simulated response loss")
        with patch.object(self.migration, "read_api", return_value=self.live), patch.object(self.migration, "api_request", side_effect=fail_request):
            with self.assertRaises(self.migration.MigrationError):
                self.migration.delete_old(self.output, "http://old-remote:6050", self.tasks)

    def test_saved_remote_url_is_used_by_separate_stop(self):
        with self.service(), redirect_stdout(io.StringIO()):
            result = self.migration.main(["--output-dir", str(self.output), "--stop-old"])
        self.assertEqual(result, 0)
        self.assertTrue(self.requests)
        self.assertEqual({request[0] for request in self.requests}, {"http://old-remote:6050"})

    def test_unknown_origin_and_changed_url_are_rejected_before_contact(self):
        for saved_url, supplied in ((None, []), ("http://old-remote:6050", ["--old-taosx-url", "http://other:6050"])):
            with self.subTest(saved_url=saved_url):
                snapshot = {"tasks": self.tasks, "start_times": {}, "old_taosx_url": saved_url}
                with patch.object(self.migration, "read_json", return_value=snapshot), self.service(), redirect_stdout(io.StringIO()):
                    result = self.migration.main(["--output-dir", str(self.output), "--stop-old"] + supplied)
                self.assertNotEqual(result, 0)
                self.assertEqual(self.requests, [])

    def test_new_task_or_configuration_drift_still_blocks_deletion(self):
        for variant in ("added", "changed", "missing"):
            self.live = copy.deepcopy(self.tasks)
            if variant == "added":
                self.live.append(legacy_task(9))
            elif variant == "changed":
                self.live[0]["to"] += "?token=different"
            else:
                self.live.pop(0)
            with self.subTest(variant=variant), self.service(), self.assertRaises((ValueError, self.migration.MigrationError)):
                self.migration.delete_old(self.output, "http://old-remote:6050", self.tasks)
            self.assertEqual(self.requests, [])

    def test_nonfinite_stop_timeout_rejected_before_contact(self):
        for value in ("nan", "inf", "-inf", "0", "-1"):
            with self.subTest(value=value), self.service():
                result = self.migration.main(["--output-dir", str(self.output), "--stop-old", "--stop-timeout=" + value])
                self.assertNotEqual(result, 0)
                self.assertEqual(self.requests, [])

    def test_stop_requests_share_remaining_time_budget(self):
        for task in self.live:
            task["status"] = "running"
        elapsed = [0.0]
        budgets = []
        def fetch(*args, **kwargs):
            budgets.append(kwargs.get("timeout", 30))
            elapsed[0] += 0.2
            return copy.deepcopy(self.live)
        def operate(*args, **kwargs):
            budgets.append(kwargs.get("timeout", 30))
            elapsed[0] += 0.3
        with patch.object(self.migration, "read_api", side_effect=fetch), patch.object(self.migration, "api_request", side_effect=operate), patch.object(self.migration.time, "monotonic", side_effect=lambda: elapsed[0]), patch.object(self.migration.time, "sleep", side_effect=lambda delay: elapsed.__setitem__(0, elapsed[0] + delay)):
            with self.assertRaises(self.migration.MigrationError):
                self.migration.stop_old(self.output, "http://old-remote:6050", self.tasks, 1)
        self.assertTrue(budgets)
        self.assertTrue(all(0 < budget <= 1 for budget in budgets))
        self.assertLessEqual(elapsed[0], 1.2)


class TargetExecutionTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("migration", SCRIPT)
        self.migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.migration)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.output = Path(self.directory.name) / "migration"
        task = legacy_task()
        task["from"] = "tmq+ws://root:CaseSecret@active-source:6041/power"
        task["to"] = "taos+ws://root:Secret@replica-target:6041/archive"
        create_sql, start_sql, self.plans = self.migration.render_with_plans([task], {})
        self.migration.save_generation(self.output, [task], {}, "http://old-remote:6050", create_sql, start_sql)
        self.rows = [dict(self.plans[0], type="replica", status="stopped", parser="NULL", via="NULL")]
        self.protection = "0"
        self.reboot = "false"
        self.writes = []
        self.events = []
        self.started = {}
        self.polls = 0
        self.elapsed = 0.0
        self.start_statuses = ["running"]
        self.start_statuses_by_name = {}
        self.fail_write = None
        self.sql_budgets = []

    def client(self, command, **options):
        self.sql_budgets.append(options.get("timeout", 300))
        sql = command[command.index("-s") + 1] if "-s" in command else Path(command[command.index("-f") + 1]).read_text()
        query, separator, destination = sql.partition(" >> ")
        if query.startswith("SHOW VARIABLES"):
            rows = [] if self.protection is None else [{"name": "xnodeDsnProtection", "value": self.protection, "scope": "server", "category": "global", "info": "", "reboot": self.reboot}]
            fields = ["name", "value", "scope", "category", "info", "reboot"]
        elif query.startswith(("SELECT", "SHOW XNODE TASKS")):
            names = [plan["name"] for plan in self.plans if "'%s'" % plan["name"] in query]
            rows = [row for row in self.rows if row["name"] in names] if query.startswith("SELECT") else self.rows
            self.events.append(("SELECT", tuple(names)))
            polled = False
            for row in rows:
                name = row["name"]
                if name in self.started:
                    statuses = self.start_statuses_by_name.get(name, self.start_statuses)
                    row["status"] = statuses[min(self.started[name], len(statuses) - 1)]
                    self.started[name] += 1
                    self.polls += 1
                    polled = True
            if polled:
                self.elapsed += 0.1
            fields = ["name", "from", "to", "type", "status", "parser", "via"]
        else:
            # Recognize generated statements without parsing credentials as SQL.
            kind = query.split()[0]
            path = self.output / ("create.sql" if kind == "CREATE" else "start.sql")
            plans = dict(zip(path.read_text().splitlines(), self.plans))
            plan = plans[query]
            self.events.append((kind, plan["name"]))
            self.writes.append(query)
            if self.fail_write and self.fail_write in query:
                return subprocess.CompletedProcess(command, 0, "DB error: simulated [0x80000001] (0.001s)\n", "")
            if query.startswith("START"):
                self.started[plan["name"]] = 0
            if query.startswith("CREATE"):
                self.rows.append(dict(plan, type="replica", status="stopped", parser="NULL", via="NULL"))
                return subprocess.CompletedProcess(command, 0, "Create OK, 0 row(s) affected\n", "")
            return subprocess.CompletedProcess(command, 0, "Query OK, 0 row(s) affected\n", "")
        if separator:
            if rows:
                with Path(destination.rstrip("; ")).open("w", encoding="utf-8", newline="") as result:
                    writer = csv.DictWriter(result, fields, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(rows)
            output = "Query OK, %s row(s) in set (0.001s)\n" % len(rows)
        else:
            output = " | ".join(fields) + "\n" + "\n".join(" | ".join(str(row[field]) for field in fields) for row in rows)
        return subprocess.CompletedProcess(command, 0, output, "")

    def execute(self, mode="start", timeout=None):
        with patch.object(self.migration.subprocess, "run", side_effect=self.client), patch.object(self.migration.time, "monotonic", side_effect=lambda: self.elapsed), patch.object(self.migration.time, "sleep", side_effect=lambda delay: setattr(self, "elapsed", self.elapsed + delay)), redirect_stdout(io.StringIO()):
            if timeout is None:
                return self.migration.execute_sql(self.output, "ws://management", mode)
            return self.migration.execute_sql(self.output, "ws://management", mode, timeout)

    def generate_missing_tasks(self, count=2):
        tasks = [legacy_task(task_id) for task_id in range(7, 7 + count)]
        for task in tasks:
            task["to"] = "taos+ws://root:secret@td%s:6041/archive" % task["id"]
        create_sql, start_sql, self.plans = self.migration.render_with_plans(tasks, {})
        shutil.rmtree(self.output)
        self.migration.save_generation(self.output, tasks, {}, "http://old-remote:6050", create_sql, start_sql)
        self.rows = []
        self.writes = []
        self.events = []

    def operation_records(self):
        return json.loads((self.output / "new-operations.json").read_text())

    def assert_all_running(self):
        records = self.operation_records()
        names = [plan["name"] for plan in self.plans]
        self.assertEqual([record["name"] for record in records], names)
        self.assertEqual([record["create"] for record in records], ["completed"] * len(names))
        self.assertEqual([record["start"] for record in records], ["completed"] * len(names))
        self.assertEqual([record["status"] for record in records], ["running"] * len(names))
        self.assertCountEqual([row["name"] for row in self.rows], names)
        self.assertTrue(all(row["status"] == "running" for row in self.rows))

    def assert_creates_verified_before_start(self, plans):
        first_start = next(index for index, event in enumerate(self.events) if event[0] == "START")
        for plan in plans:
            created = self.events.index(("CREATE", plan["name"]))
            self.assertLess(created, first_start)
            self.assertIn(("SELECT", (plan["name"],)), self.events[created + 1:first_start])

    def test_all_cli_creates_and_verifies_every_task_before_start(self):
        self.generate_missing_tasks()
        with patch.object(self.migration.subprocess, "run", side_effect=self.client), redirect_stdout(io.StringIO()):
            result = self.migration.main(["--output-dir", str(self.output), "--execute", "all", "--dsn", "ws://management"])
        self.assertEqual(result, 0)
        self.assertEqual([event[0] for event in self.events if event[0] != "SELECT"], ["CREATE", "CREATE", "START", "START"])
        self.assert_creates_verified_before_start(self.plans)
        self.assert_all_running()

    def test_all_second_create_failure_blocks_every_start(self):
        self.generate_missing_tasks(3)
        self.fail_write = "CREATE XNODE TASK '%s'" % self.plans[1]["name"]
        with self.assertRaises(self.migration.MigrationError):
            self.execute("all")
        self.assertEqual([event for event in self.events if event[0] != "SELECT"], [("CREATE", plan["name"]) for plan in self.plans[:2]])
        self.assertEqual([row["name"] for row in self.rows], [self.plans[0]["name"]])
        self.assertEqual(self.rows[0]["status"], "stopped")
        records = self.operation_records()
        self.assertEqual([record["create"] for record in records], ["completed", "failed", "pending"])
        self.assertEqual([record["start"] for record in records], ["pending"] * 3)

    def test_all_resumes_after_create_failure_without_recreating_existing_task(self):
        self.generate_missing_tasks(3)
        self.fail_write = "CREATE XNODE TASK '%s'" % self.plans[1]["name"]
        with self.assertRaises(self.migration.MigrationError):
            self.execute("all")
        existing = self.rows[0]
        self.fail_write = None
        self.writes.clear()
        self.events.clear()
        self.execute("all")
        self.assertEqual([event[1] for event in self.events if event[0] == "CREATE"], [plan["name"] for plan in self.plans[1:]])
        self.assertIs(self.rows[0], existing)
        self.assert_creates_verified_before_start(self.plans[1:])
        self.assert_all_running()

    def test_all_retry_does_not_repeat_create_or_start(self):
        self.generate_missing_tasks()
        self.execute("all")
        self.assert_all_running()
        existing = list(self.rows)
        self.writes.clear()
        self.events.clear()
        self.execute("all")
        self.assertEqual(self.writes, [])
        for row, original in zip(self.rows, existing):
            self.assertIs(row, original)
        self.assert_all_running()

    def test_all_creates_only_missing_tasks_and_waits_for_each_start(self):
        self.generate_missing_tasks()
        existing = dict(self.plans[0], type="replica", status="stopped", parser="NULL", via="NULL")
        self.rows = [existing]
        self.start_statuses_by_name = {self.plans[1]["name"]: ["queued", "queued", "running"]}
        self.execute("all")
        self.assertEqual([event[1] for event in self.events if event[0] == "CREATE"], [self.plans[1]["name"]])
        self.assertIs(self.rows[0], existing)
        self.assert_creates_verified_before_start(self.plans[1:])
        self.assert_all_running()

    def test_all_create_verification_failure_blocks_start(self):
        original_client = self.client
        for field, value in ((None, None), ("from", "tmq+ws://wrong:6041/db"), ("to", "taos+ws://wrong:6041/db"), ("type", "normal"), ("parser", '{"script":"custom"}'), ("via", "2")):
            with self.subTest(field=field):
                self.generate_missing_tasks()
                def client(command, **options):
                    result = original_client(command, **options)
                    if self.rows:
                        if field is None:
                            self.rows.clear()
                        else:
                            self.rows[0][field] = value
                    return result
                with patch.object(self, "client", side_effect=client):
                    with self.assertRaises(self.migration.MigrationError):
                        self.execute("all")
                self.assertEqual([event for event in self.events if event[0] != "SELECT"], [("CREATE", self.plans[0]["name"])])
                records = self.operation_records()
                self.assertEqual([record["create"] for record in records], ["failed", "pending"])
                self.assertEqual([record["start"] for record in records], ["pending", "pending"])

    def test_all_resumes_after_partial_start_failure(self):
        self.generate_missing_tasks(3)
        self.fail_write = "START XNODE TASK '%s'" % self.plans[1]["name"]
        with self.assertRaises(self.migration.MigrationError):
            self.execute("all")
        self.assertEqual([record["create"] for record in self.operation_records()], ["completed"] * 3)
        self.assertEqual([record["start"] for record in self.operation_records()], ["completed", "failed", "pending"])
        self.assertEqual([row["status"] for row in self.rows], ["running", "stopped", "stopped"])
        self.fail_write = None
        self.writes.clear()
        self.events.clear()
        self.execute("all")
        self.assertEqual([event for event in self.events if event[0] != "SELECT"], [("START", plan["name"]) for plan in self.plans[1:]])
        self.assert_all_running()

    def test_rejects_normal_task_even_when_hosts_contain_active_and_replica(self):
        self.rows[0]["type"] = "normal"
        with self.assertRaises(self.migration.MigrationError):
            self.execute()
        self.assertEqual(self.writes, [])

    def test_stopped_task_with_active_hostname_is_started(self):
        self.execute()
        self.assertEqual(len(self.writes), 1)
        self.assertTrue(self.writes[0].startswith("START XNODE TASK"))

    def test_credential_case_and_name_suffix_do_not_match(self):
        for field, value in (("from", self.rows[0]["from"].replace("CaseSecret", "casesecret")), ("name", self.rows[0]["name"] + "_other")):
            with self.subTest(field=field):
                original = self.rows[0][field]
                self.rows[0][field] = value
                with self.assertRaises(self.migration.MigrationError):
                    self.execute()
                self.assertEqual(self.writes, [])
                self.rows[0][field] = original

    def test_unrelated_failed_task_is_result_data(self):
        self.rows.append(dict(self.rows[0], name="unrelated", status="failed"))
        self.execute()
        self.assertEqual(len(self.writes), 1)

    def test_protection_enabled_unknown_or_pending_restart_blocks_all_writes(self):
        for value, reboot in (("1", "false"), (None, "false"), ("0", "true")):
            for mode in ("create", "start", "all"):
                with self.subTest(value=value, reboot=reboot, mode=mode):
                    self.protection, self.reboot = value, reboot
                    with self.assertRaisesRegex(self.migration.MigrationError, "protection|Protection"):
                        self.execute(mode)
                    self.assertEqual(self.writes, [])

    def test_start_waits_through_queued_until_running(self):
        self.start_statuses = ["queued", "running"]
        self.execute()
        self.assertEqual(len(self.writes), 1)
        self.assertGreaterEqual(self.polls, 2)
        records = json.loads((self.output / "new-operations.json").read_text())
        self.assertEqual(records[0]["start"], "completed")

    def test_start_timeout_records_pending_and_limits_subprocess_budget(self):
        self.start_statuses = ["queued"]
        with self.assertRaises(self.migration.MigrationError):
            self.execute(timeout=0.5)
        self.assertLessEqual(self.elapsed, 0.6)
        records = json.loads((self.output / "new-operations.json").read_text())
        self.assertEqual(records[0]["start"], "pending")
        self.assertTrue(all(budget <= 0.5 for budget in self.sql_budgets[-2:]))

    def test_start_sql_error_with_zero_exit_is_not_success(self):
        self.fail_write = "START"
        with self.assertRaises(self.migration.MigrationError):
            self.execute()
        records = json.loads((self.output / "new-operations.json").read_text())
        self.assertEqual(records[0]["start"], "failed")

    def test_failed_start_records_status_without_waiting_for_timeout(self):
        self.start_statuses = ["failed"]
        with self.assertRaises(self.migration.MigrationError):
            self.execute()
        records = json.loads((self.output / "new-operations.json").read_text())
        self.assertEqual(records[0]["start"], "failed")

    def test_parser_and_via_are_checked_before_start(self):
        for field, value in (("parser", '{"script":"custom"}'), ("via", "2")):
            with self.subTest(field=field):
                self.rows[0][field] = value
                with self.assertRaises(self.migration.MigrationError):
                    self.execute()
                self.rows[0][field] = "NULL"
                self.assertEqual(self.writes, [])

    def test_csv_preserves_delimiters_quotes_and_trailing_credential_spaces(self):
        task = legacy_task()
        task["from"] = 'tmq+ws://user:p|a,"ss @td1:6041/power'
        create_sql, start_sql, self.plans = self.migration.render_with_plans([task], {})
        shutil.rmtree(self.output)
        self.migration.save_generation(self.output, [task], {}, "http://old-remote:6050", create_sql, start_sql)
        self.rows = [dict(self.plans[0], type="replica", status="running", parser="NULL", via="NULL")]
        self.execute()
        self.assertEqual(self.writes, [])

    def test_create_accepts_real_client_success_marker(self):
        self.rows = []
        self.execute("create")
        self.assertEqual(len(self.writes), 1)
        records = json.loads((self.output / "new-operations.json").read_text())
        self.assertEqual(records[0]["create"], "completed")

    def test_management_dsn_is_forwarded_to_taos_unchanged(self):
        self.rows[0]["status"] = "running"
        for dsn in ("ws://user:p%40ss@", "http://user:p%40ss@management:6041", "wss://user:p%40ss@management:6041?token=a+b"):
            with self.subTest(dsn=dsn), patch.object(self.migration.subprocess, "run", side_effect=self.client) as client, redirect_stdout(io.StringIO()):
                result = self.migration.main(["--output-dir", str(self.output), "--execute", "start", "--dsn", dsn])
                self.assertEqual(result, 0)
                self.assertTrue(client.call_args_list)
                for call in client.call_args_list:
                    self.assertEqual(call.args[0][:3], ["taos", "-X", dsn])
        self.assertEqual(self.writes, [])

    def test_existing_unsorted_snapshot_executes_without_rewriting_generation(self):
        tasks = [legacy_task(10), legacy_task(2)]
        tasks[0]["to"] = "taos+ws://root:secret@td10:6041/archive"
        create_sql, start_sql, self.plans = self.migration.render_with_plans(tasks, {})
        shutil.rmtree(self.output)
        self.migration.save_generation(self.output, tasks, {}, "http://old-remote:6050", create_sql, start_sql)
        self.rows = [dict(plan, type="replica", status="stopped", parser="NULL", via="NULL") for plan in self.plans]
        originals = {name: (self.output / name).read_bytes() for name in ("tasks.json", "create.sql", "start.sql")}
        self.execute("create")
        records = json.loads((self.output / "new-operations.json").read_text())
        self.assertEqual([record["legacy_id"] for record in records], [10, 2])
        self.assertTrue(all(record["create"] == "completed" for record in records))
        self.assertEqual(self.writes, [])
        for name, contents in originals.items():
            self.assertEqual((self.output / name).read_bytes(), contents)

    def test_snapshot_or_sql_mismatch_is_rejected_before_contact(self):
        originals = {name: (self.output / name).read_bytes() for name in ("tasks.json", "create.sql", "start.sql")}
        for name in originals:
            with self.subTest(name=name):
                if name == "tasks.json":
                    snapshot = json.loads(originals[name])
                    snapshot["tasks"][0]["to"] = "taos+ws://root:secret@other:6041/archive"
                    (self.output / name).write_text(json.dumps(snapshot), encoding="utf-8")
                else:
                    (self.output / name).write_bytes(originals[name] + b"\n")
                with patch.object(self.migration.subprocess, "run") as client:
                    with self.assertRaisesRegex(ValueError, "migration SQL does not match tasks.json"):
                        self.migration.execute_sql(self.output, "ws://management", "create")
                    client.assert_not_called()
                self.assertFalse((self.output / "new-operations.json").exists())
                (self.output / name).write_bytes(originals[name])

    def test_inherited_time_survives_generation_and_execution_validation(self):
        snapshot = json.loads((self.output / "tasks.json").read_text())
        task = snapshot["tasks"][0]
        task["from"] += "?msg.consume.start.ts=1700000000000"
        create_sql, start_sql, self.plans = self.migration.render_with_plans([task], {})
        self.assertIn("msg.consume.start.ts=1700000000000", create_sql)
        shutil.rmtree(self.output)
        self.migration.save_generation(self.output, [task], {}, snapshot["old_taosx_url"], create_sql, start_sql)
        self.rows = [dict(self.plans[0], type="replica", status="stopped", parser="NULL", via="NULL")]
        originals = {path.name: path.read_bytes() for path in self.output.iterdir()}
        self.execute("all")
        self.assertEqual(self.writes, start_sql.splitlines())
        for name, contents in originals.items():
            self.assertEqual((self.output / name).read_bytes(), contents)

    def test_prechange_sql_without_inherited_time_is_rejected_and_records_preserved(self):
        snapshot = json.loads((self.output / "tasks.json").read_text())
        snapshot["tasks"][0]["from"] += "?msg.consume.start.ts=1700000000000"
        (self.output / "tasks.json").write_text(json.dumps(snapshot), encoding="utf-8")
        for name in ("old-operations.json", "new-operations.json"):
            (self.output / name).write_text('[{"status": "completed"}]\n', encoding="utf-8")
        originals = {path.name: path.read_bytes() for path in self.output.iterdir()}
        for mode in ("create", "start", "all"):
            with self.subTest(mode=mode), patch.object(self.migration.subprocess, "run") as client:
                with self.assertRaisesRegex(ValueError, "migration SQL does not match tasks.json"):
                    self.migration.execute_sql(self.output, "ws://management", mode)
                client.assert_not_called()
                self.assertEqual({path.name: path.read_bytes() for path in self.output.iterdir()}, originals)

    def test_partial_start_failure_records_completed_failed_and_pending_tasks(self):
        tasks = [legacy_task(task_id) for task_id in (7, 8, 9)]
        for task in tasks:
            task["to"] = "taos+ws://root:secret@td%s:6041/archive" % task["id"]
        create_sql, start_sql, self.plans = self.migration.render_with_plans(tasks, {})
        shutil.rmtree(self.output)
        self.migration.save_generation(self.output, tasks, {}, "http://old-remote:6050", create_sql, start_sql)
        self.rows = [dict(plan, type="replica", status="running" if index == 0 else "stopped", parser="NULL", via="NULL") for index, plan in enumerate(self.plans)]
        self.fail_write = self.plans[1]["name"]
        with self.assertRaises(self.migration.MigrationError):
            self.execute()
        records = json.loads((self.output / "new-operations.json").read_text())
        self.assertEqual([record["start"] for record in records], ["completed", "failed", "pending"])

    def test_partial_start_failure_refreshes_already_started_tasks(self):
        tasks = [legacy_task(task_id) for task_id in (7, 8)]
        tasks[1]["to"] = "taos+ws://root:secret@td3:6041/archive"
        create_sql, start_sql, self.plans = self.migration.render_with_plans(tasks, {})
        shutil.rmtree(self.output)
        self.migration.save_generation(self.output, tasks, {}, "http://old-remote:6050", create_sql, start_sql)
        self.rows = [dict(plan, type="replica", status="stopped", parser="NULL", via="NULL") for plan in self.plans]
        self.fail_write = self.plans[1]["name"]
        with self.assertRaises(self.migration.MigrationError):
            self.execute()
        records = json.loads((self.output / "new-operations.json").read_text())
        self.assertEqual([record["start"] for record in records], ["completed", "failed"])

    def test_start_timeout_cli_validation(self):
        for mode, value in (("start", "nan"), ("start", "inf"), ("start", "0"), ("create", "2"), (None, "2")):
            arguments = ["--output-dir", str(self.output), "--start-timeout=" + value]
            if mode:
                arguments += ["--execute", mode, "--dsn", "ws://management"]
            with self.subTest(mode=mode, value=value), patch.object(self.migration.subprocess, "run", side_effect=self.client):
                self.assertNotEqual(self.migration.main(arguments), 0)
        self.assertEqual(self.writes, [])

    def test_start_timeout_cli_is_forwarded(self):
        self.start_statuses = ["queued"]
        with patch.object(self.migration.subprocess, "run", side_effect=self.client), patch.object(self.migration.time, "monotonic", side_effect=lambda: self.elapsed), patch.object(self.migration.time, "sleep", side_effect=lambda delay: setattr(self, "elapsed", self.elapsed + delay)), redirect_stdout(io.StringIO()):
            result = self.migration.main(["--output-dir", str(self.output), "--execute", "start", "--dsn", "ws://management", "--start-timeout", "0.5"])
        self.assertEqual(result, 1)
        self.assertTrue(self.writes)
        self.assertLessEqual(self.elapsed, 0.6)


if __name__ == "__main__":
    unittest.main()
