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
# along with this program. If not, see <https://www.gnu.org/licenses/>.

"""Make the packaging checks visible to the CI pytest wrapper.

ci/pytest.sh decides whether a case passed by looking for the
"successfully executed" line the TDengine test framework prints after a
passing case; the packaging checks are a plain pytest module and never run
through that framework.  Emit the same marker only when the session really
passed, and write it to the original stdout so pytest output capturing does
not hide it from the engine log.
"""

import sys


def pytest_sessionfinish(session, exitstatus):
    if exitstatus != 0:
        return
    stream = sys.__stdout__ if sys.__stdout__ is not None else sys.stdout
    stream.write("successfully executed\n")
    stream.flush()
