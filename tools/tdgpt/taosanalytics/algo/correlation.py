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

from taosanalytics.service_registry import loader


def do_dtw(input_list1: list, input_list2: list, params):
    s = loader.get_service("dtw")

    s.set_input_list(input_list1, None)
    s.set_second_input_data(input_list2)

    s.set_params(params)
    dist, path = s.execute()
    return dist, path


def do_tlcc(input_list1: list, input_list2: list, params):
    s = loader.get_service("tlcc")

    s.set_input_list(input_list1, None)
    s.set_second_input_data(input_list2)

    s.set_params(params)
    lags, ccf_vals = s.execute()
    return lags, ccf_vals
