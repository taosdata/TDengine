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

"""Adapter skeleton: expose a user function as a TDgpt batch envelope plugin.

Use this when the user's function does not follow the batch plugin protocol.
Batch plugins do not inherit a service base class. Keep this adapter class in
the entry file and put the user's code in a helper package next to it, for
example ``my_algo_impl/``. Deploy both with
``--script my_algo.py --asset my_algo_impl``.
"""



class _UserBatchAdapter:
    name = "user_batch"  # TODO: set to the requested algorithm name
    description = "Adapter for a user-provided batch envelope function"

    def build(self, ts, values, windows, params):
        """Call the user's function and return center, lower and upper curves.

        ``ts`` and ``values`` are numpy arrays. ``windows`` is a list of
        ``(start, end)`` pairs. ``params`` is the request ``config`` object.
        The three returned curves must have the same length and must not
        contain NaN or infinite values.
        """
        # TODO: import the user's function and call it.
        # from .my_algo_impl.user_model import build_band
        # center, lower, upper = build_band(ts, values, windows, params)
        raise NotImplementedError("wire the user's function in build()")
