/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef TDENGINE_TTIME_H
#define TDENGINE_TTIME_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <sys/time.h>
#include <time.h>

//@return timestamp in second
int32_t taosGetTimestampSec();

//@return timestamp in millisecond
int64_t taosGetTimestampMs();

//@return timestamp in microsecond
int64_t taosGetTimestampUs();

/*
 * @return timestamp decided by global conf variable, tsTimePrecision
 * if precision == TSDB_TIME_PRECISION_MICRO, it returns timestamp in microsecond.
 *    precision == TSDB_TIME_PRECISION_MILLI, it returns timestamp in millisecond.
 */
int64_t taosGetTimestamp(int32_t precision);

int32_t getTimestampInUsFromStr(char* token, int32_t tokenlen, int64_t* ts);

int32_t taosParseTime(char* timestr, int64_t* time, int32_t len, int32_t timePrec);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TTIME_H
