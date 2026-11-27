# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-

import time
from datetime import datetime
from .log import tdLog
import calendar

class GetTime:

    def get_ms_timestamp(self,ts_str):
        _ts_str = ts_str
        if time.daylight and time.localtime().tm_isdst:
            offset = time.altzone
        else:
            offset = time.timezone
        if "+" in _ts_str:
            timestamp = datetime.fromisoformat(_ts_str)
            return int((timestamp-datetime.fromtimestamp(0,timestamp.tzinfo)).total_seconds())*1000+int(timestamp.microsecond / 1000)
        if " " in ts_str:
            p = ts_str.split(" ")[1]
            if len(p) > 15 :
                _ts_str = ts_str[:-3]
        if ':' in _ts_str and '.' in _ts_str:
            timestamp = datetime.strptime(_ts_str, "%Y-%m-%d %H:%M:%S.%f")
            date_time = int(int(calendar.timegm(timestamp.timetuple())+offset)*1000 + timestamp.microsecond/1000)
        elif ':' in _ts_str and '.' not in _ts_str:
            timestamp = datetime.strptime(_ts_str, "%Y-%m-%d %H:%M:%S")
            date_time = int(int(calendar.timegm(timestamp.timetuple())+offset)*1000 + timestamp.microsecond/1000)
        else:
            timestamp = datetime.strptime(_ts_str, "%Y-%m-%d")
            date_time = int(int(calendar.timegm(timestamp.timetuple())+offset)*1000 + timestamp.microsecond/1000)
        return date_time
    def get_us_timestamp(self,ts_str):
        _ts = self.get_ms_timestamp(ts_str) * 1000
        if " " in ts_str:
            p = ts_str.split(" ")[1]
            if len(p) > 12:
                us_ts = p[12:15]
                _ts += int(us_ts)
        return _ts
    def get_ns_timestamp(self,ts_str):
        _ts = self.get_us_timestamp(ts_str) *1000
        if " " in ts_str:
            p = ts_str.split(" ")[1]
            if len(p) > 15:
                us_ts = p[15:]
                _ts += int(us_ts)
        return _ts
    def time_transform(self,ts_str,precision):
        date_time = []
        if precision == 'ms':
            for i in ts_str:
                date_time.append(self.get_ms_timestamp(i))
        elif precision == 'us':
            for i in ts_str:
                date_time.append(self.get_us_timestamp(i))
        elif precision == 'ns':
            for i in ts_str:
                date_time.append(self.get_ns_timestamp(i))
        return date_time