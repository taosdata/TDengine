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

import os
import re
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from new_test_framework.utils import etool, tdLog, tdSql, tdStream


pytestmark = pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="stream timezone persistence requires the POSIX timezone backend",
)


class TestTaosdumpStreamTimeConfig:
    DB = "taosdump_stream_time_config"
    STREAM = "s_export"
    TSMA = "tsma_export"
    TIMEZONE = "Asia/Shanghai"
    FIRST_DAY_OF_WEEK = 0
    CONFIG = "CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 0)"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()

    def _run_tool(self, args, env=None):
        command = [str(arg) for arg in args]
        tool_env = os.environ.copy() if env is None else env.copy()
        tool_env.pop("LD_PRELOAD", None)
        tdLog.info(f"run tool: {shlex.join(command)}")
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=300,
            env=tool_env,
            check=False,
        )
        if result.stdout:
            tdLog.info(result.stdout)
        if result.stderr:
            tdLog.info(result.stderr)
        assert result.returncode == 0, (
            f"command failed with {result.returncode}: {shlex.join(command)}"
        )
        return result

    def _stream_metadata(self):
        tdSql.query(
            "select stream_name, `timezone`, `first_day_of_week`, `sql` "
            "from information_schema.ins_streams "
            f"where db_name='{self.DB}' order by stream_name"
        )
        rows = list(tdSql.queryResult)
        assert len(rows) == 2, rows

        user_rows = [row for row in rows if row[0] == self.STREAM]
        tsma_rows = [
            row for row in rows if row[3].strip().lower().startswith("create tsma")
        ]
        assert len(user_rows) == 1, rows
        assert len(tsma_rows) == 1, rows
        return user_rows[0], tsma_rows[0]

    def _check_boundary_rows(self, expected):
        tdSql.checkResultsByFunc(
            sql=(
                "select cast(wstart as bigint), cnt "
                f"from {self.DB}.{self.STREAM}_out order by wstart"
            ),
            func=lambda: list(tdSql.queryResult) == expected,
            retry=90,
        )

    def _write_restore_config(self, config_dir):
        config_dir.mkdir()
        log_dir = config_dir / "log"
        temp_dir = config_dir / "tmp"
        log_dir.mkdir()
        temp_dir.mkdir()
        (config_dir / "taos.cfg").write_text(
            "firstEp localhost:6030\n"
            f"logDir {log_dir}\n"
            f"tempDir {temp_dir}\n"
            "timezone UTC\n"
            "firstDayOfWeek 6\n",
            encoding="utf-8",
        )

    def _assert_restore_defaults(self, config_dir, env):
        result = self._run_tool(
            [
                etool.taosFile(),
                "-c",
                config_dir,
                "-w",
                "4096",
                "-s",
                "select timezone(), first_day_of_week();",
            ],
            env=env,
        )
        data_lines = [
            line
            for line in result.stdout.splitlines()
            if "UTC" in line and re.search(r"\|\s*6\s*\|", line)
        ]
        assert data_lines, result.stdout

    def test_taosdump_stream_time_config(self):
        """taosdump preserves frozen stream context and excludes TSMA CONFIG.

        1. Create a configured weekly stream, a TSMA, and deterministic data.
        2. Export basic data, then add a TSMA and export extended metadata.
        3. Restore with different client timezone/fdow defaults.
        4. Verify metadata plus pre- and post-restore window boundaries.

        Catalog:
            - Tools:Taosdump

        Since: v3.4.3.0

        Labels: common,ci,stream,integration,functional,timezone,taosdump
        Feishu: None

        History:
            - 2026-09-04 OpenAI GPT-5 Created
        """
        tdSql.prepare(dbname=self.DB, drop=True, vgroups=1)
        tdSql.execute(f"use {self.DB}")
        tdSql.execute("create table src (ts timestamp, v int, `--flag` int)")
        tdSql.execute(
            f"""create stream {self.STREAM}
            -- retain the trigger definition during dump/restore
            interval(1w) from src
            stream_options(config(timezone='Asia/Shanghai', firstDayOfWeek=0))
            into {self.STREAM}_out as
            select _twstart as wstart, count(*) as cnt from src
            where '-- literal marker' = '-- literal marker'
              and `--flag` = 1
              and ts >= _twstart and ts < _twend"""
        )
        tdStream.checkStreamStatus(self.STREAM)

        tdSql.execute(
            "insert into src values "
            "(1745679600000, 1, 1),"
            "(1745686800000, 2, 1),"
            "(1746288000001, 3, 1)"
        )
        original_boundaries = [
            (1745078400000, 1),
            (1745683200000, 1),
        ]
        self._check_boundary_rows(original_boundaries)

        taosdump = etool.taosDumpFile()
        with tempfile.TemporaryDirectory(prefix="taosdump-stream-time-") as tmp:
            tmp_path = Path(tmp)
            basic_dir = tmp_path / "basic"
            ext_meta_dir = tmp_path / "ext-meta"
            restore_config = tmp_path / "restore-config"
            basic_dir.mkdir()
            ext_meta_dir.mkdir()
            self._write_restore_config(restore_config)

            self._run_tool(
                [taosdump, "-D", self.DB, "-o", basic_dir, "--content=basic"]
            )
            tdSql.execute(
                f"create tsma {self.TSMA} on {self.DB}.src "
                "function(avg(v), count(ts)) interval(1m)"
            )
            original_user, original_tsma = self._stream_metadata()
            assert original_user[1] == self.TIMEZONE, original_user
            assert original_user[2] == self.FIRST_DAY_OF_WEEK, original_user
            assert original_user[3].lower().count("config(") == 1, original_user[3]
            assert self.CONFIG in original_user[3], original_user[3]
            assert "-- retain the trigger definition" in original_user[3]
            assert "'-- literal marker' = '-- literal marker'" in original_user[3]
            assert "`--flag` = 1" in original_user[3]
            assert original_tsma[1] is None, original_tsma
            assert original_tsma[2] is None, original_tsma
            assert "config(" not in original_tsma[3].lower(), original_tsma[3]
            self._run_tool(
                [
                    taosdump,
                    "-D",
                    self.DB,
                    "-o",
                    ext_meta_dir,
                    "--content=ext-meta",
                ]
            )

            stream_file = ext_meta_dir / self.DB / "stream.sql"
            assert stream_file.is_file(), stream_file
            stream_lines = [
                line.strip()
                for line in stream_file.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            user_lines = [
                line for line in stream_lines if line.lower().startswith("create stream")
            ]
            tsma_lines = [
                line for line in stream_lines if line.lower().startswith("create tsma")
            ]
            assert len(user_lines) == 1, stream_lines
            assert len(tsma_lines) == 1, stream_lines
            assert user_lines[0].lower().count("config(") == 1, user_lines[0]
            assert self.CONFIG in user_lines[0], user_lines[0]
            assert "'-- literal marker' = '-- literal marker'" in user_lines[0]
            assert "`--flag` = 1" in user_lines[0]
            assert tsma_lines[0] == original_tsma[3], stream_lines
            assert "config(" not in tsma_lines[0].lower(), tsma_lines[0]

            tdSql.execute(f"drop stream {self.DB}.{self.STREAM}")
            tdSql.execute(f"drop tsma {self.DB}.{self.TSMA}")
            tdSql.execute(f"drop database {self.DB}")
            tdSql.execute("set timezone 'UTC'")
            tdSql.execute("set first_day_of_week 6")

            restore_env = os.environ.copy()
            restore_env["TZ"] = "UTC"
            self._assert_restore_defaults(restore_config, restore_env)
            self._run_tool(
                [
                    taosdump,
                    "-c",
                    restore_config,
                    "-i",
                    basic_dir,
                    "--content=basic",
                ],
                env=restore_env,
            )
            self._run_tool(
                [
                    taosdump,
                    "-c",
                    restore_config,
                    "-i",
                    ext_meta_dir,
                    "--content=ext-meta",
                ],
                env=restore_env,
            )

            tdSql.execute(f"use {self.DB}")
            tdStream.checkStreamStatus(self.STREAM)
            restored_user, restored_tsma = self._stream_metadata()
            assert restored_user[1] == self.TIMEZONE, restored_user
            assert restored_user[2] == self.FIRST_DAY_OF_WEEK, restored_user
            assert restored_user[3].lower().count("config(") == 1, restored_user[3]
            assert self.CONFIG in restored_user[3], restored_user[3]
            assert "-- retain the trigger definition" not in restored_user[3]
            assert "'-- literal marker' = '-- literal marker'" in restored_user[3]
            assert "`--flag` = 1" in restored_user[3]
            assert restored_tsma[1] is None, restored_tsma
            assert restored_tsma[2] is None, restored_tsma
            assert restored_tsma[3].lower().startswith("create tsma"), restored_tsma
            assert "config(" not in restored_tsma[3].lower(), restored_tsma[3]
            self._check_boundary_rows(original_boundaries)

            tdSql.execute(
                "insert into src values "
                "(1746896400000, 4, 1),"
                "(1747497600001, 5, 1)"
            )
            tdSql.checkResultsByFunc(
                sql=(
                    "select cast(wstart as bigint), cnt "
                    f"from {self.STREAM}_out "
                    "where cast(wstart as bigint)=1746892800000"
                ),
                func=lambda: list(tdSql.queryResult) == [(1746892800000, 1)],
                retry=90,
            )
