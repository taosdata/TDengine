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

#ifndef _TD_COMMON_TIME_H_
#define _TD_COMMON_TIME_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "taosdef.h"
#include "taosmsg.h"

/*
 * @return timestamp decided by global conf variable, tsTimePrecision
 * if precision == TSDB_TIME_PRECISION_MICRO, it returns timestamp in microsecond.
 *    precision == TSDB_TIME_PRECISION_MILLI, it returns timestamp in millisecond.
 */
static FORCE_INLINE int64_t taosGetTimestamp(int32_t precision) {
  if (precision == TSDB_TIME_PRECISION_MICRO) {
    return taosGetTimestampUs();
  } else if (precision == TSDB_TIME_PRECISION_NANO) {
    return taosGetTimestampNs();
  }else {
    return taosGetTimestampMs();
  }
}

int64_t taosTimeAdd(int64_t t, int64_t duration, char unit, int32_t precision);
int64_t taosTimeTruncate(int64_t t, const SInterval* pInterval, int32_t precision);
int32_t taosTimeCountInterval(int64_t skey, int64_t ekey, int64_t interval, char unit, int32_t precision);

int32_t parseAbsoluteDuration(char* token, int32_t tokenlen, int64_t* ts, char* unit, int32_t timePrecision);
int32_t parseNatualDuration(const char* token, int32_t tokenLen, int64_t* duration, char* unit, int32_t timePrecision);

int32_t taosParseTime(char* timestr, int64_t* time, int32_t len, int32_t timePrec, int8_t dayligth);
void    deltaToUtcInitOnce();

int64_t convertTimePrecision(int64_t time, int32_t fromPrecision, int32_t toPrecision);


#ifdef __cplusplus
}
#endif

#endif  /*_TD_COMMON_TIME_H_*/
