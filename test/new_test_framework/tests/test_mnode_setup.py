# Copyright (c) {year} TAOS Data, Inc. <jhtao@taosdata.com>
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

import ast
import types
from pathlib import Path

import pytest


def load_create_mnodes():
    path = Path(__file__).resolve().parents[2] / "conftest.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_create_mnodes")
    clock = types.SimpleNamespace(now=0)

    def sleep(seconds):
        clock.now += seconds

    namespace = {"time": types.SimpleNamespace(monotonic=lambda: clock.now, sleep=sleep)}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
    return namespace["_create_mnodes"], clock


class SqlError(Exception):
    def __init__(self, errno):
        self.errno = errno


class Cursor:
    def __init__(self, conflict_count=0, error=None, ready_after=0):
        self.conflict_count = conflict_count
        self.error = error
        self.ready_after = ready_after
        self.created = [1]
        self.polls = 0
        self.statements = []

    def execute(self, sql):
        self.statements.append(sql)
        if sql.startswith("create mnode"):
            if self.error is not None:
                raise self.error
            if self.conflict_count:
                self.conflict_count -= 1
                raise SqlError(-2147482669)
            self.created.append(int(sql.rsplit(" ", 1)[1]))

    def fetchall(self):
        self.polls += 1
        if self.polls <= self.ready_after:
            return [(1, "leader", "ready"), (2, "learner", "creating")]
        return [(i, "leader" if i == 1 else "follower", "ready") for i in reversed(self.created)]


def test_mnode_setup_waits_for_conflicts_and_readiness():
    create, clock = load_create_mnodes()
    cursor = Cursor(conflict_count=12, ready_after=2)
    create(cursor, 3)
    assert cursor.created == [1, 2, 3]
    assert clock.now == 14
    third = cursor.statements.index("create mnode on dnode 3")
    assert cursor.statements[third - 3:third] == [
        "select `id`, `role`, `status` from information_schema.ins_mnodes"
    ] * 3


def test_mnode_setup_does_not_retry_unrelated_error():
    create, clock = load_create_mnodes()
    error = SqlError(0x03D4)
    cursor = Cursor(error=error)
    with pytest.raises(SqlError) as exc:
        create(cursor, 3)
    assert exc.value is error
    assert clock.now == 0
    assert len(cursor.statements) == 1


def test_mnode_setup_conflict_timeout():
    create, clock = load_create_mnodes()
    with pytest.raises(TimeoutError, match="transaction conflict"):
        create(Cursor(conflict_count=100), 3, timeout=3)
    assert clock.now == 3


def test_mnode_setup_readiness_timeout():
    create, clock = load_create_mnodes()
    cursor = Cursor(ready_after=100)
    with pytest.raises(TimeoutError, match="mnodes not ready"):
        create(cursor, 3, timeout=3)
    assert clock.now == 3
    assert "create mnode on dnode 3" not in cursor.statements
