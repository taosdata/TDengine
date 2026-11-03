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

import ast
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock


class TestVisualStudioEnvironment(unittest.TestCase):
    def test_multiline_mr_description_is_preserved_without_creating_variables(self):
        import json
        source = Path(__file__).with_name("run_windows_ci.py")
        tree = ast.parse(source.read_text(encoding="utf-8"))
        function = next(node for node in tree.body
                        if isinstance(node, ast.FunctionDef) and node.name == "visual_studio_environment")
        description = "MR table\n| MATCH '^....$' | MB_CUR_MAX==1 |\nPATH=description only"
        payload = {"Path": "C:\\VS;C:\\Windows", "CI_MERGE_REQUEST_DESCRIPTION": description}
        result = SimpleNamespace(returncode=0,
            stdout="VS startup banner\n__TSDB_VS_ENV__" + json.dumps(payload) + "\n")
        with tempfile.TemporaryDirectory() as directory:
            vcvars = Path(directory) / "vcvarsall.bat"
            vcvars.touch()
            namespace = {"Path": lambda value: vcvars if str(value).startswith("C:\\Program Files") else Path(value), "os": SimpleNamespace(environ={}),
                         "sys": SimpleNamespace(executable="python.exe"), "tempfile": tempfile,
                         "json": json, "subprocess": SimpleNamespace(run=Mock(return_value=result), PIPE=-1, STDOUT=-2),
                         "fail": Mock(side_effect=RuntimeError("missing environment"))}
            exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec"), namespace)
            actual = namespace["visual_studio_environment"]()
        self.assertEqual(actual, {"PATH": payload["Path"], "CI_MERGE_REQUEST_DESCRIPTION": description})
        self.assertNotIn("MB_CUR_MAX", actual)


if __name__ == "__main__":
    unittest.main()
