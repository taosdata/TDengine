# Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import Mock

import pytest


@pytest.fixture
def failover(monkeypatch):
    root = Path(__file__).resolve().parents[2]

    def load(name, path):
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
        return module

    # Load the real helpers and connector exceptions without native libraries or a cluster.
    for name in ("failover_test", "failover_test.server", "failover_test.server.dnodes",
                 "failover_test.common", "failover_test.constant", "failover_test.log",
                 "taos", "taos.constants", "requests", "psutil", "pandas", "tzlocal"):
        module = types.ModuleType(name)
        module.__path__ = []
        monkeypatch.setitem(sys.modules, name, module)
    errors = load("taos.error", root.parents[1] / "taos-connector-python/taos/error.py")
    sys.modules["taos"].error = errors
    sys.modules["taos.constants"].FieldType = types.SimpleNamespace(C_BLOB=20)
    sys.modules["tzlocal"].get_localzone = Mock()
    log = Mock()
    log.exit.side_effect = lambda message: (_ for _ in ()).throw(SystemExit(message))
    sys.modules["failover_test.log"].tdLog = log
    sql = load("failover_test.sql", root / "new_test_framework/utils/sql.py")
    cluster = load("failover_test.cluster", root / "new_test_framework/utils/clusterCommonCheck.py")

    clock = types.SimpleNamespace(now=0.0)

    def sleep(seconds):
        clock.now += seconds

    timer = types.SimpleNamespace(monotonic=lambda: clock.now, sleep=sleep)
    monkeypatch.setattr(sql, "time", timer)
    monkeypatch.setattr(cluster, "time", timer)
    cursor = Mock()
    cursor.description = [("id", 4), ("endpoint", 8), ("role", 8)]
    cursor.fetchall.return_value = rows("leader", "follower", "offline")
    sql.tdSql.init(cursor)
    return types.SimpleNamespace(
        root=root, load=load, sql=sql, cluster=cluster, clock=clock, cursor=cursor,
        log=log, errors=errors, wait=cluster.clusterComCheck.check3mnodeoff,
    )


def rows(*roles):
    return [(i, f"node{i}:6030", role) for i, role in enumerate(roles, 1)]


@pytest.mark.parametrize("asan", ["0", "1"])
@pytest.mark.parametrize("offline", [1, 2, 3])
def test_connection_error_recovers_on_existing_cursor(failover, monkeypatch, asan, offline):
    env = failover
    monkeypatch.setenv("CI_ASAN_BUILD", asan)
    error = env.errors.ProgrammingError("Unable to establish connection", -2147483637)
    env.cursor.execute.side_effect = [error, error, None]
    roles = ["leader", "follower"]
    roles.insert(offline - 1, "offline")
    env.cursor.fetchall.return_value = list(reversed(rows(*roles)))
    assert env.wait(offline) is True
    assert env.clock.now == 2
    assert env.cursor.execute.call_count == 3
    env.cursor.close.assert_not_called()
    assert "error=ProgrammingError" in env.log.info.call_args_list[0].args[0]
    assert "elapsed=2.000s" in env.log.info.call_args_list[-1].args[0]


@pytest.mark.parametrize("kind", ["sql", "network_code", "python", "text_only"])
@pytest.mark.parametrize("asan", ["0", "1"])
def test_unrelated_errors_fail_immediately(failover, monkeypatch, kind, asan):
    env = failover
    monkeypatch.setenv("CI_ASAN_BUILD", asan)
    error = {
        "sql": env.errors.ProgrammingError("cluster not ready", 0x0130),
        "network_code": env.errors.ProgrammingError("broken link", 0x0018),
        "python": RuntimeError("unexpected failure"),
        "text_only": env.errors.ProgrammingError("Unable to establish connection"),
    }[kind]
    env.cursor.execute.side_effect = error
    with pytest.raises(type(error)) as caught:
        env.wait(3)
    assert caught.value is error
    assert env.cursor.execute.call_count == 1
    assert env.clock.now == 0


def test_timeout_keeps_last_state_and_connection_error(failover):
    env = failover
    error = env.errors.ProgrammingError("Unable to establish connection", -2147483637)
    env.cursor.fetchall.return_value = rows("leader", "follower", "follower")
    env.cursor.execute.side_effect = [None, error, error]
    with pytest.raises(TimeoutError) as caught:
        env.wait(3, timeout=2.5)
    assert caught.value.__cause__ is error
    assert "last_state=[(1, 'node1:6030', 'leader')" in str(caught.value)
    assert "Unable to establish connection" in str(caught.value)
    assert "elapsed=2.500s" in str(caught.value)
    assert env.cursor.execute.call_count == 3
    assert env.clock.now == 2.5


def test_persistent_connection_error_exhausts_default_budget(failover):
    env = failover
    error = env.errors.ProgrammingError("Unable to establish connection", -2147483637)
    env.cursor.execute.side_effect = error
    with pytest.raises(TimeoutError, match="timeout=30s") as caught:
        env.wait(3)
    assert caught.value.__cause__ is error
    assert env.clock.now == 30
    assert env.cursor.execute.call_count == 30


def test_roles_can_converge_after_successful_queries(failover):
    failover.cursor.fetchall.side_effect = [
        rows("follower", "follower", "offline"),
        rows("leader", "follower", "offline"),
    ]
    assert failover.wait(3) is True
    assert failover.cursor.execute.call_count == 2
    assert failover.clock.now == 1


@pytest.mark.parametrize("roles", [
    ("leader", "follower", "follower"),
    ("leader", "leader", "offline"),
    ("follower", "follower", "offline"),
    ("leader", "learner", "offline"),
    ("offline", "leader", "follower"),
])
def test_incorrect_roles_never_pass(failover, roles):
    failover.cursor.fetchall.return_value = rows(*roles)
    with pytest.raises(TimeoutError, match="last_error=None"):
        failover.wait(3, timeout=2)
    assert failover.clock.now == 2


@pytest.mark.parametrize("state", [
    [],
    rows("leader", "follower"),
    rows("leader", "follower", "offline") + rows("leader"),
], ids=["empty", "missing", "extra_duplicate"])
def test_mnode_count_can_converge_after_successful_queries(failover, state):
    failover.cursor.fetchall.side_effect = [state, rows("leader", "follower", "offline")]
    assert failover.wait(3) is True
    assert failover.cursor.execute.call_count == 2
    assert failover.clock.now == 1
    assert f"state={state!r}, error=None" in failover.log.info.call_args_list[0].args[0]


@pytest.mark.parametrize("state", [
    [],
    rows("leader", "follower"),
    rows("leader", "follower", "offline") + rows("leader"),
], ids=["empty", "missing", "extra_duplicate"])
def test_incorrect_mnode_count_times_out_with_last_state(failover, state):
    initial_state = rows("leader")
    failover.cursor.fetchall.side_effect = [initial_state, state, state]
    with pytest.raises(TimeoutError) as caught:
        failover.wait(3, timeout=2.5)
    assert caught.value.__cause__ is None
    assert f"last_state={state!r}, last_error=None" in str(caught.value)
    assert "elapsed=2.500s" in str(caught.value)
    assert failover.cursor.execute.call_count == 3
    assert failover.clock.now == 2.5
    messages = [call.args[0] for call in failover.log.info.call_args_list]
    assert len(messages) == 3
    for elapsed, observed in enumerate([initial_state, state, state]):
        assert f"elapsed={elapsed:.3f}s/2.5s, state={observed!r}, error=None" in messages[elapsed]


@pytest.mark.parametrize("outcome", ["ready", "connection_error"])
def test_slow_native_query_cannot_pass_after_deadline(failover, outcome):
    env = failover

    def execute(_):
        env.clock.now += 4
        if outcome == "connection_error":
            raise env.errors.ProgrammingError("Unable to establish connection", -2147483637)

    env.cursor.execute.side_effect = execute
    with pytest.raises(TimeoutError, match="elapsed=4.000s"):
        env.wait(3, timeout=3)
    assert env.cursor.execute.call_count == 1
    assert env.clock.now == 4


def test_asan_still_scales_multi_attempt_queries(failover, monkeypatch):
    monkeypatch.setenv("CI_ASAN_BUILD", "1")
    error = failover.errors.ProgrammingError("cluster not ready", 0x0130)
    assert failover.sql._scaled_query_times(10, error) == 60


@pytest.mark.parametrize("failure", [None, "timeout", "stream", "deploy", "snapshot"])
@pytest.mark.parametrize("recovery", ["connection_error", "mnode_count"])
def test_recovery_continues_all_checks_before_restart(failover, monkeypatch, failure, recovery):
    env = failover
    utils = types.ModuleType("new_test_framework.utils")
    utils.clusterComCheck = env.cluster.clusterComCheck
    utils.sc = Mock()
    utils.tdLog = env.log
    utils.tdSql = env.sql.tdSql
    utils.tdStream = Mock()
    monkeypatch.setitem(sys.modules, "new_test_framework", types.ModuleType("new_test_framework"))
    monkeypatch.setitem(sys.modules, utils.__name__, utils)
    case = env.load("failover_case", env.root / (
        "cases/18-StreamProcessing/01-Snode/test_snode_runner_parallelism_failover.py"
    )).TestSnodeRunnerParallelismFailover()
    error = env.errors.ProgrammingError("Unable to establish connection", -2147483637)
    if failure == "timeout":
        env.cursor.execute.side_effect = error
    elif recovery == "connection_error":
        env.cursor.execute.side_effect = [error, None]
    else:
        env.cursor.fetchall.side_effect = [
            rows("leader", "follower"), rows("leader", "follower", "offline"),
        ]
    events = []

    def check(stage, result=None):
        def run(*args):
            events.append(stage)
            if failure == stage:
                raise AssertionError(stage)
            return result
        return run

    case.getMnodeLeader = lambda: 3
    utils.sc.dnodeForceStop.side_effect = check("stop")
    case.waitForStreamRunning = check("stream")
    case.checkRunnerDeployIds = check("deploy")
    case.getRunnerSnapshot = check("read_snapshot", ((1, 2, 3),) if failure == "snapshot" else ())
    case.restartMnode = check("restart")
    if failure is None:
        case.switchLeaderAndCheck("runner_snapshot", 8, ())
    else:
        with pytest.raises((AssertionError, SystemExit, TimeoutError)):
            case.switchLeaderAndCheck("runner_snapshot", 8, ())
    stages = ["stop", "stream", "deploy", "read_snapshot"]
    if failure == "timeout":
        stages = ["stop"]
    elif failure in ("stream", "deploy"):
        stages = stages[:stages.index(failure) + 1]
    assert events == stages + ["restart"]
    assert env.cursor.execute.call_count == (30 if failure == "timeout" else 2)
