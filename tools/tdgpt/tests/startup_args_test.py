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

"""Tests for the model servers' startup argument parsing.

The rules live in `startup_args` rather than inside a server script because the
servers import torch, and this is the code that decides which port the service
actually listens on.
"""

import pytest

from taosanalytics.tsfmservice import startup_args


class TestParseModelServerArgs:
    """What a model server makes of the argv the service hands it."""

    DEFAULT = 6061

    def test_no_arguments_keeps_the_default_port(self):
        port, folder, remaining = startup_args.parse_model_server_args(
            ["srv"], self.DEFAULT
        )

        assert (port, folder, remaining) == (self.DEFAULT, None, ["srv"])

    def test_the_service_invocation_is_parsed(self):
        """`taosanode_service._build_model_args` sends exactly this shape."""
        port, folder, remaining = startup_args.parse_model_server_args(
            ["srv", "--model-folder", "/model/tsfm/moment", "--port", "6066"],
            self.DEFAULT,
        )

        assert (port, folder) == (6066, "/model/tsfm/moment")
        assert remaining == ["srv"]

    def test_a_port_without_its_value_is_reported(self):
        """The reported bug: `--port` last was dropped, so the port fell back to
        the default with nothing in the log."""
        with pytest.raises(startup_args.StartupArgsError) as excinfo:
            startup_args.parse_model_server_args(["srv", "--port"], self.DEFAULT)

        assert "--port" in str(excinfo.value)
        assert "value" in str(excinfo.value)

    def test_a_model_folder_without_its_value_is_reported(self):
        with pytest.raises(startup_args.StartupArgsError) as excinfo:
            startup_args.parse_model_server_args(["srv", "--model-folder"], self.DEFAULT)

        assert "--model-folder" in str(excinfo.value)

    def test_a_port_that_is_not_a_number_is_reported(self):
        with pytest.raises(startup_args.StartupArgsError) as excinfo:
            startup_args.parse_model_server_args(["srv", "--port", "http"], self.DEFAULT)

        assert "--port" in str(excinfo.value)

    @pytest.mark.parametrize("port", ["0", "70000", "-1"])
    def test_a_port_out_of_range_is_reported(self, port):
        with pytest.raises(startup_args.StartupArgsError):
            startup_args.parse_model_server_args(["srv", "--port", port], self.DEFAULT)

    def test_another_flag_where_the_value_belongs_is_reported(self):
        with pytest.raises(startup_args.StartupArgsError):
            startup_args.parse_model_server_args(
                ["srv", "--port", "--model-folder", "/model"], self.DEFAULT
            )

    def test_model_folder_starting_with_dash_is_accepted(self):
        port, folder, remaining = startup_args.parse_model_server_args(
            ["srv", "--model-folder", "-myfolder", "--port", "6066"], self.DEFAULT
        )

        assert (port, folder) == (6066, "-myfolder")
        assert remaining == ["srv"]

    def test_an_inline_value_is_accepted(self):
        port, folder, _ = startup_args.parse_model_server_args(
            ["srv", "--port=6066", "--model-folder=/model/tsfm/moment"], self.DEFAULT
        )

        assert (port, folder) == (6066, "/model/tsfm/moment")

    def test_an_empty_inline_value_is_reported(self):
        with pytest.raises(startup_args.StartupArgsError):
            startup_args.parse_model_server_args(["srv", "--port="], self.DEFAULT)

    def test_unknown_flags_are_still_tolerated(self):
        """Another server's flags must not stop this one from starting -- they are
        handed back rather than raising, so the caller can report them."""
        port, folder, remaining = startup_args.parse_model_server_args(
            ["srv", "--model-name", "x", "--port", "6066", "--enable-ep"],
            self.DEFAULT,
        )

        assert (port, folder) == (6066, None)
        assert remaining == ["srv", "--model-name", "x", "--enable-ep"]

    def test_the_last_known_flag_still_wins(self):
        port, folder, _ = startup_args.parse_model_server_args(
            ["srv", "--model-folder", "/model", "--port", "6067"], self.DEFAULT
        )

        assert (port, folder) == (6067, "/model")
