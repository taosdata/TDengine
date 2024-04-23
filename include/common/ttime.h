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

#define IS_CALENDAR_TIME_DURATION(_t) ((_t) == 'n' || (_t) == 'y' || (_t) == 'N' || (_t) == 'Y')

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
  taosLocalTime(&t, &tm, NULL);
  tm.tm_hour = 0;
  tm.tm_min = 0;
  tm.tm_sec = 0;

  return (int64_t)taosMktime(&tm) * factor;
}

int64_t taosTimeAdd(int64_t t, int64_t duration, char unit, int32_t precision);

int64_t taosTimeTruncate(int64_t ts, const SInterval* pInterval);
int64_t taosTimeGetIntervalEnd(int64_t ts, const SInterval* pInterval);
int32_t taosTimeCountIntervalForFill(int64_t skey, int64_t ekey, int64_t interval, char unit, int32_t precision, int32_t order);

int32_t parseAbsoluteDuration(const char* token, int32_t tokenlen, int64_t* ts, char* unit, int32_t timePrecision);
int32_t parseNatualDuration(const char* token, int32_t tokenLen, int64_t* duration, char* unit, int32_t timePrecision);

int32_t taosParseTime(const char* timestr, int64_t* pTime, int32_t len, int32_t timePrec, int8_t dayligth);
void    deltaToUtcInitOnce();
char    getPrecisionUnit(int32_t precision);

int64_t convertTimePrecision(int64_t ts, int32_t fromPrecision, int32_t toPrecision);
int64_t convertTimeFromPrecisionToUnit(int64_t ts, int32_t fromPrecision, char toUnit);
int32_t convertStringToTimestamp(int16_t type, char* inputData, int64_t timePrec, int64_t* timeVal);

void taosFormatUtcTime(char* buf, int32_t bufLen, int64_t ts, int32_t precision);

struct STm {
  struct tm tm;
  int64_t   fsec;  // in NANOSECOND
};

int32_t taosTs2Tm(int64_t ts, int32_t precision, struct STm* tm);
int32_t taosTm2Ts(struct STm* tm, int64_t* ts, int32_t precision);

/// @brief convert a timestamp to a formatted string
/// @param format the timestamp format, must null terminated
/// @param [in,out] formats the formats array pointer generated. Shouldn't be NULL.
/// If (*formats == NULL), [format] will be used and [formats] will be updated to the new generated
/// formats array; If not NULL, [formats] will be used instead of [format] to skip parse formats again.
/// @param out output buffer, should be initialized by memset
/// @notes remember to free the generated formats
int32_t taosTs2Char(const char* format, SArray** formats, int64_t ts, int32_t precision, char* out, int32_t outLen);
/// @brief convert a formatted timestamp string to a timestamp
/// @param format must null terminated
/// @param [in, out] formats, see taosTs2Char
/// @param tsStr must null terminated
/// @retval 0 for success, otherwise error occured
/// @notes remember to free the generated formats even when error occured
int32_t taosChar2Ts(const char* format, SArray** formats, const char* tsStr, int64_t* ts, int32_t precision, char* errMsg,
                    int32_t errMsgLen);

void    TEST_ts2char(const char* format, int64_t ts, int32_t precision, char* out, int32_t outLen);
int32_t TEST_char2ts(const char* format, int64_t* ts, int32_t precision, const char* tsStr);

/// @brief get offset seconds from zero timezone to input timezone
///        for +XX timezone, the offset to zero is negative value
/// @param tzStr timezonestr, eg: +0800, -0830, -08
/// @param offset seconds, eg: +08 offset -28800, -01 offset 3600
/// @return 0 success, other fail
int32_t offsetOfTimezone(char* tzStr, int64_t* offset);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_TIME_H_*/
