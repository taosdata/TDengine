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

#include "taosdef.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TIME_IS_VAR_DURATION(_t) ((_t) == 'n' || (_t) == 'y' || (_t) == 'N' || (_t) == 'Y')

#define TIME_UNIT_NANOSECOND  'b'
#define TIME_UNIT_MICROSECOND 'u'
#define TIME_UNIT_MILLISECOND 'a'
#define TIME_UNIT_SECOND      's'
#define TIME_UNIT_MINUTE      'm'
#define TIME_UNIT_HOUR        'h'
#define TIME_UNIT_DAY         'd'
#define TIME_UNIT_WEEK        'w'
#define TIME_UNIT_MONTH       'n'
#define TIME_UNIT_YEAR        'y'

/*
 * @return timestamp decided by global conf variable, tsTimePrecision
 * if precision == TSDB_TIME_PRECISION_MICRO, it returns timestamp in microsecond.
 *    precision == TSDB_TIME_PRECISION_MILLI, it returns timestamp in millisecond.
 *    precision == TSDB_TIME_PRECISION_NANO,  it returns timestamp in nanosecond.
 */
static FORCE_INLINE int64_t taosGetTimestamp(int32_t precision) {
  if (precision == TSDB_TIME_PRECISION_MICRO) {
    return taosGetTimestampUs();
  } else if (precision == TSDB_TIME_PRECISION_NANO) {
    return taosGetTimestampNs();
  } else {
    return taosGetTimestampMs();
  }
}

/*
 * @return timestamp of today at 00:00:00 in given precision
 * if precision == TSDB_TIME_PRECISION_MICRO, it returns timestamp in microsecond.
 *    precision == TSDB_TIME_PRECISION_MILLI, it returns timestamp in millisecond.
 *    precision == TSDB_TIME_PRECISION_NANO,  it returns timestamp in nanosecond.
 */
static FORCE_INLINE int64_t taosGetTimestampToday(int32_t precision) {
  int64_t   factor = (precision == TSDB_TIME_PRECISION_MILLI)   ? 1000
                     : (precision == TSDB_TIME_PRECISION_MICRO) ? 1000000
                                                                : 1000000000;
  time_t    t = taosTime(NULL);
  struct tm tm;
  taosLocalTime(&t, &tm);
  tm.tm_hour = 0;
  tm.tm_min = 0;
  tm.tm_sec = 0;

  return (int64_t)taosMktime(&tm) * factor;
}

int64_t taosTimeAdd(int64_t t, int64_t duration, char unit, int32_t precision);

int64_t taosTimeTruncate(int64_t t, const SInterval* pInterval, int32_t precision);
int32_t taosTimeCountInterval(int64_t skey, int64_t ekey, int64_t interval, char unit, int32_t precision);

int32_t parseAbsoluteDuration(const char* token, int32_t tokenlen, int64_t* ts, char* unit, int32_t timePrecision);
int32_t parseNatualDuration(const char* token, int32_t tokenLen, int64_t* duration, char* unit, int32_t timePrecision);

int32_t taosParseTime(const char* timestr, int64_t* time, int32_t len, int32_t timePrec, int8_t dayligth);
void    deltaToUtcInitOnce();
char    getPrecisionUnit(int32_t precision);

int64_t convertTimePrecision(int64_t time, int32_t fromPrecision, int32_t toPrecision);
int64_t convertTimeFromPrecisionToUnit(int64_t time, int32_t fromPrecision, char toUnit);
int32_t convertStringToTimestamp(int16_t type, char* inputData, int64_t timePrec, int64_t* timeVal);

void taosFormatUtcTime(char* buf, int32_t bufLen, int64_t time, int32_t precision);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_TIME_H_*/
