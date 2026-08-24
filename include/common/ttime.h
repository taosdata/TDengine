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

/*
 * Calendar-duration classification. The two macros below partition the
 * calendar-aware (DST/month-length-sensitive) units into TWO DISJOINT sets.
 * The split reflects how the companion int64 duration field (e.g. SInterval's
 * interval/sliding/offset) is encoded for that unit -- same field, different
 * encoding selected by the unit char (see getDuration/parseNatualDuration):
 *
 *   IS_CALENDAR_TIME_DURATION  -> month/quarter/year (n/q/y): the int64 keeps
 *                                 the raw period COUNT (e.g. 2y -> 2, unit 'y';
 *                                 q is normalized to n, so 2q -> 6, unit 'n').
 *   IS_CALENDAR_DAY_DURATION   -> day/week (d/w): the int64 is pre-multiplied
 *                                 into TICKS of the target precision (e.g. 2d at
 *                                 ms precision -> 172800000).
 *
 * They are intentionally non-overlapping: a unit matches at most one. Code that
 * needs "any calendar-aware unit" must OR them together, e.g.
 *   IS_CALENDAR_TIME_DURATION(u) || IS_CALENDAR_DAY_DURATION(u)
 * (see timewindowoperator.c). Do NOT fold d/w into IS_CALENDAR_TIME_DURATION:
 * many sites use !IS_CALENDAR_TIME_DURATION as the fixed-tick fast path (e.g.
 * ttime.c taosTimeAdd) and rely on d/w being excluded; merging them would both
 * break those paths and double-count where the two macros are ORed.
 */
#define IS_CALENDAR_TIME_DURATION(_t) \
    ((_t) == 'n' || (_t) == 'y' || (_t) == 'N' || (_t) == 'Y' || \
     (_t) == 'q' || (_t) == 'Q')

/* Day/week durations are calendar-aware (DST-sensitive): one local day is 23h/25h
 * across a DST transition, so they must be advanced/counted via local-time math,
 * not fixed-tick arithmetic.  Disjoint from IS_CALENDAR_TIME_DURATION (month
 * /year/quarter): d/w hold a tick count, n/y hold a raw period count (see above). */
#define IS_CALENDAR_DAY_DURATION(_t) ((_t) == 'd' || (_t) == 'w')

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

#define AUTO_DURATION_LITERAL "auto"
#define AUTO_DURATION_VALUE   -1

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
int64_t taosGetTimestampToday(int32_t precision, timezone_t tz);

int64_t taosTimeAdd(int64_t t, int64_t duration, char unit, int32_t precision, timezone_t tz);

TSKEY   getNextTimeWindowStart(const SInterval* pInterval, TSKEY start, int32_t order);
int64_t taosTimeTruncate(int64_t ts, const SInterval* pInterval);
int64_t taosTimeGetIntervalEnd(int64_t ts, const SInterval* pInterval);
int32_t taosTimeCountIntervalForFill(int64_t skey, int64_t ekey, int64_t interval, char unit, int32_t precision, int32_t order, timezone_t tz);
void    calcIntervalAutoOffset(SInterval* interval);

int32_t parseAbsoluteDuration(const char* token, int32_t tokenlen, int64_t* ts, char* unit, int32_t timePrecision);
int32_t parseNatualDuration(const char* token, int32_t tokenLen, int64_t* duration, char* unit, int32_t timePrecision, bool negativeAllow);

int32_t taosParseShortWeekday(const char* str);
int32_t taosParseTime(const char* timestr, int64_t* pTime, int32_t len, int32_t timePrec, timezone_t tz);
char    getPrecisionUnit(int32_t precision);

/*
 * Resolve the UTC offset that is in effect at the specified timestamp.
 *
 * Unlike taosGetTZOffsetSeconds(), this API is target-instant aware and must
 * be used when DST or historical timezone rules can affect the result.
 * Result convention is east-positive (tm_gmtoff style).
 */
int32_t taosGetTimezoneOffsetAtSeconds(time_t timeSec, timezone_t tz, int64_t *pOffsetSeconds);

/* 1970-01-01 is Thursday; used for week-alignment in TIMETRUNCATE */
#define UNIX_EPOCH_WDAY  4

/*
 * DST-aware TIMETRUNCATE for a single timestamp.
 * Truncates to midnight of the appropriate day (or week start per fdow).
 * Returns the truncated timestamp in ticks, or the original ts on error.
 */
int64_t taosTimeTruncateIANA(int64_t ts, int64_t truncateUnit, int8_t fdow,
                             int32_t precision, timezone_t tz);

int64_t convertTimePrecision(int64_t ts, int32_t fromPrecision, int32_t toPrecision);
int32_t convertCalendarTimeFromUnitToPrecision(int64_t time,  char fromUnit, int32_t toPrecision,int64_t* pRes);
int32_t convertTimeFromPrecisionToUnit(int64_t time, int32_t fromPrecision, char toUnit, int64_t* pRes);
int32_t convertStringToTimestamp(int16_t type, char* inputData, int64_t timePrec, int64_t* timeVal, timezone_t tz, void* charsetCxt);
int32_t getDuration(int64_t val, char unit, int64_t* result, int32_t timePrecision);
int64_t alignToNaturalBoundary(int64_t timestamp, char unit, int64_t value, int64_t offset, int32_t precision, timezone_t tz);

int32_t taosFormatUtcTime(char* buf, int32_t bufLen, int64_t ts, int32_t precision);
char*   formatTimestampLocal(char* buf, int32_t cap, int64_t val, int precision);
char*   formatTimestampTz(char* buf, int32_t cap, int64_t val, int precision, timezone_t tz);
struct STm {
  struct tm tm;
  int64_t   fsec;  // in NANOSECOND
};

int32_t taosTs2Tm(int64_t ts, int32_t precision, struct STm* tm, timezone_t tz);
int32_t taosTm2Ts(struct STm* tm, int64_t* ts, int32_t precision, timezone_t tz);

/// @brief convert a timestamp to a formatted string
/// @param format the timestamp format, must null terminated
/// @param [in,out] formats the formats array pointer generated. Shouldn't be NULL.
/// If (*formats == NULL), [format] will be used and [formats] will be updated to the new generated
/// formats array; If not NULL, [formats] will be used instead of [format] to skip parse formats again.
/// @param out output buffer, should be initialized by memset
/// @notes remember to free the generated formats
int32_t taosTs2Char(const char* format, SArray** formats, int64_t ts, int32_t precision, char* out, int32_t outLen, timezone_t tz);
/// @brief convert a formatted timestamp string to a timestamp
/// @param format must null terminated
/// @param [in, out] formats, see taosTs2Char
/// @param tsStr must null terminated
/// @retval 0 for success, otherwise error occured
/// @notes remember to free the generated formats even when error occured
int32_t taosChar2Ts(const char* format, SArray** formats, const char* tsStr, int64_t* ts, int32_t precision, char* errMsg,
                    int32_t errMsgLen, timezone_t tz);

int32_t TEST_ts2char(const char* format, int64_t ts, int32_t precision, char* out, int32_t outLen);
int32_t TEST_char2ts(const char* format, int64_t* ts, int32_t precision, const char* tsStr);

/// @brief get offset seconds from zero timezone to input timezone
///        for +XX timezone, the offset to zero is negative value
/// @param tzStr timezonestr, eg: +0800, -0830, -08
/// @param offset seconds, eg: +08 offset -28800, -01 offset 3600
/// @return 0 success, other fail
int32_t offsetOfTimezone(char* tzStr, int64_t* offset);

/*
 * Validate a timezone string (IANA name or fixed offset) and optionally
 * return a timezone_t handle.  Rejects ambiguous abbreviations (CST, EST).
 * Accepted formats: IANA ("Asia/Shanghai"), "UTC", "Z", "+HH", "+HHMM",
 * "+HH:MM", "UTC+H[:MM]", "UTC-H[:MM]" and the unsigned POSIX form
 * "UTCH[:MM]" ("UTC0", "UTC8", "UTC8:30"), where an omitted sign means '+'.
 * The "UTC" prefix is case-insensitive ("utc8" == "UTC8").
 * If pTz is non-NULL and validation succeeds, *pTz is set
 * to a freshly allocated timezone_t (caller must tzfree).
 */
int32_t taosValidateTimezone(const char *tzStr, timezone_t *pTz);

/*
 * Whether a timezone literal is a zone *name* — an IANA path
 * ("Asia/Shanghai") or a UTC/GMT-prefixed form ("UTC+8", "utc8", "GMT-5") —
 * as opposed to a bare numeric offset ("+0800").  Callers use this to decide
 * whether the literal must go through taosValidateTimezone() instead of the
 * fixed-offset parser.  The UTC/GMT prefixes are matched case-insensitively;
 * keeping that rule in one place is what stops the execution paths from
 * drifting apart from the validator.
 */
bool taosIsNamedTimezoneLiteral(const char *tzStr);

/*
 * Validate and normalize a timezone string for all platforms.
 * If valid, stores the canonical name in normBuf:
 *   - IANA name         -> written as-is ("Asia/Shanghai")
 *   - Windows TZ name   -> mapped to its IANA equivalent ("Asia/Shanghai")
 *   - fixed-offset      -> POSIX UTC±h[:mm] string ("UTC+8", "UTC-5:30")
 *                          sign is preserved (+ = west, - = east)
 *   - UTC±N (POSIX)     -> offset kept as-is ("UTC-8")
 *   - UTC±HH:MM / HHMM  -> simplified to POSIX short form
 *                          ("UTC+08:00" -> "UTC+8")
 *   - UTCN[:MM]         -> unsigned form, '+' restored
 *                          ("UTC0" -> "UTC+0", "UTC8:30" -> "UTC+8:30")
 *   - UTC/Z             -> "UTC"
 *   - GMT/GMT±N         -> rejected (use UTC series)
 * The "UTC" prefix is case-insensitive on input and always uppercase in
 * normBuf ("utc+8" -> "UTC+8").
 * normBuf must be at least TD_TIMEZONE_LEN bytes.
 * *pTz is set (and caller must tzfree) only when pTz != NULL.
 */
int32_t taosValidateAndNormalizeTimezone(const char *tzStr,
                                         char *normBuf, int32_t normBufLen,
                                         timezone_t *pTz);

bool checkRecursiveTsmaInterval(int64_t baseInterval, int8_t baseUnit, int64_t interval, int8_t unit, int8_t precision,
                                bool checkEq);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_TIME_H_*/
