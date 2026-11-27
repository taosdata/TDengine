# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""Offline regression checks for the concurrent SQLAlchemy documentation example."""

from contextlib import nullcontext, redirect_stdout
import io
from pathlib import Path
import re
import runpy
import threading
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch


EXAMPLE = Path(__file__).resolve().parents[2] / "docs/examples/python/sqlalchemy_demo.py"


class ExampleEngine:
    def __init__(self, fail_write=False):
        self.fail_write = fail_write
        self.rows = {"d1001": set(), "d1002": set()}
        self.lock = threading.Lock()
        self.disposed = False

    def begin(self):
        return nullcontext(self)

    def execute(self, sql):
        if "INSERT INTO" in sql:
            if self.fail_write:
                raise RuntimeError("injected writer failure")
            # Model a valid reverse scheduling of the old NOW-based writers:
            # the writer with offset 40 executes 40 ms before offset 0.
            if "NOW" in sql:
                offset = int(re.search(r"NOW \+ (\d+)a", sql).group(1)) - 1
                now = 1_700_000_000_000 - offset
                sql = re.sub(r"NOW \+ (\d+)a", lambda m: str(now + int(m.group(1))), sql)
            with self.lock:
                for table, values in re.findall(
                        r"power\.(d100[12]) USING.*?VALUES (.*?)(?=power\.d100|$)",
                        sql, re.S):
                    self.rows[table].update(int(ts) for ts in re.findall(r"\((\d+),", values))
            return SimpleNamespace(cursor=SimpleNamespace(row_count=4))
        if "SELECT count(*)" in sql:
            count = 20 if self.fail_write else sum(map(len, self.rows.values()))
            return SimpleNamespace(fetchall=lambda: [(count,)])
        return SimpleNamespace(fetchall=lambda: [])

    def dispose(self):
        self.disposed = True


class SqlalchemyExampleTest(unittest.TestCase):
    def run_example(self, engine):
        sqlalchemy = ModuleType("sqlalchemy")
        sqlalchemy.create_engine = lambda **kwargs: engine
        sqlalchemy.text = lambda value: value
        modules = {"sqlalchemy": sqlalchemy, "taosws": ModuleType("taosws")}
        with patch.dict("sys.modules", modules), redirect_stdout(io.StringIO()):
            runpy.run_path(str(EXAMPLE), run_name="__main__")

    def test_concurrent_writes_keep_all_twenty_rows(self):
        engine = ExampleEngine()
        self.run_example(engine)
        self.assertEqual({table: len(rows) for table, rows in engine.rows.items()},
                         {"d1001": 15, "d1002": 5})
        self.assertTrue(engine.disposed)

    def test_writer_exception_reaches_main_even_if_count_matches(self):
        engine = ExampleEngine(fail_write=True)
        with self.assertRaisesRegex(RuntimeError, "injected writer failure"):
            self.run_example(engine)
        self.assertTrue(engine.disposed)


if __name__ == "__main__":
    unittest.main()
