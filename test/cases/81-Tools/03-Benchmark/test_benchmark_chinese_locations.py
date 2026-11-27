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

import subprocess

from new_test_framework.utils import etool, tdLog, tdSql


class TestBenchmarkChineseLocations:
    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")

    def do_verify_chinese_locations(self, tmp_path, interface):
        expected = {
            name: len(name.encode("utf-8"))
            for name in (
                "旧金山", "洛杉矶", "圣地亚哥", "圣何塞", "帕洛阿尔托",
                "坎贝尔", "山景城", "森尼韦尔", "圣克拉拉", "库比蒂诺",
            )
        }
        database = f"bench_chinese_locations_{interface}"
        tdLog.info(f"Verify Chinese demo locations through {interface}")
        tdSql.execute(f"drop database if exists {database}")
        try:
            with etool.benchmark_write_log() as cfg_dir:
                result = subprocess.run(
                    [
                        etool.benchMarkFile(), "-I", interface, "-C", "-y",
                        "-d", database, "-t", "20", "-n", "2", "-T", "1",
                        "-r", "2", "-c", cfg_dir,
                        "-o", str(tmp_path / f"{interface}.txt"),
                    ],
                    cwd=tmp_path, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, timeout=120,
                )
            assert result.returncode == 0, result.stdout.decode("utf-8", errors="replace")
            tdSql.query(f"select count(*) from {database}.meters")
            tdSql.checkData(0, 0, 40)
            tdSql.query(
                f"select distinct tbname, location, length(location), "
                f"cast(location as nchar(24)) from {database}.meters "
                "order by tbname"
            )
            tdSql.checkRows(20)
            for row_index in range(20):
                location = tdSql.getData(row_index, 1)
                assert location in expected, (interface, row_index, location)
                tdSql.checkData(row_index, 2, expected[location])
                tdSql.checkData(row_index, 3, location)
        finally:
            tdSql.execute(f"drop database if exists {database}")
        print(f"{interface} Chinese locations ................. [passed]")

    def test_benchmark_chinese_locations(self, tmp_path):
        """taosBenchmark Chinese demo locations

        1. Write Chinese demo locations through taosc, stmt and stmt2.
        2. Verify row and child-table counts after each write.
        3. Check Chinese names, UTF-8 byte lengths and NCHAR conversion.
        4. Drop the test databases after each interface.

        Catalog:
            - Tools/Benchmark

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-13 Haoran Chen Added shared UTF-8 demo location regression
        """
        self.do_verify_chinese_locations(tmp_path, "taosc")
        self.do_verify_chinese_locations(tmp_path, "stmt")
        self.do_verify_chinese_locations(tmp_path, "stmt2")
