/*
 * Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.taosdata.taosdemo.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimeStampUtilTest {

    @Test
    public void datetimeToLong() {
        final String startTime = "2005-01-01 00:00:00.000";
        long start = TimeStampUtil.datetimeToLong(startTime);
        assertEquals(1104508800000l, start);
        String dateTimeStr = TimeStampUtil.longToDatetime(start);
        assertEquals("2005-01-01 00:00:00.000", dateTimeStr);
    }

    @Test
    public void longToDatetime() {
        System.out.println(TimeStampUtil.longToDatetime(1293334499006l));

        String datetime = TimeStampUtil.longToDatetime(1510000000000L);
        assertEquals("2017-11-07 04:26:40.000", datetime);
        long timestamp = TimeStampUtil.datetimeToLong(datetime);
        assertEquals(1510000000000L, timestamp);
    }

    @Test
    public void range() {
        long start = TimeStampUtil.datetimeToLong("2020-10-01 00:00:00.000");
        long timeGap = 1000;
        long numOfRowsPerTable = 1000l * 3600l * 24l * 90l;
        TimeStampUtil.TimeTuple timeTuple = TimeStampUtil.range(start, timeGap, numOfRowsPerTable);
        System.out.println(TimeStampUtil.longToDatetime(timeTuple.start));
        System.out.println(TimeStampUtil.longToDatetime(timeTuple.end));
        System.out.println(timeTuple.timeGap);

    }

}