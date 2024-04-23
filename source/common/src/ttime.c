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

#ifdef DARWIN
#define _XOPEN_SOURCE
#else
#define _XOPEN_SOURCE 500
#endif

#define _BSD_SOURCE
#define _DEFAULT_SOURCE
#include "ttime.h"

#include "tlog.h"

// ==== mktime() kernel code =================//
static int64_t m_deltaUtc = 0;

void deltaToUtcInitOnce() {
  struct tm tm = {0};
  (void)taosStrpTime("1970-01-01 00:00:00", (const char*)("%Y-%m-%d %H:%M:%S"), &tm);
  m_deltaUtc = (int64_t)taosMktime(&tm);
  // printf("====delta:%lld\n\n", seconds);
}

static int64_t parseFraction(char* str, char** end, int32_t timePrec);
static int32_t parseTimeWithTz(const char* timestr, int64_t* time, int32_t timePrec, char delim);
static int32_t parseLocaltime(char* timestr, int32_t len, int64_t* utime, int32_t timePrec, char delim);
static int32_t parseLocaltimeDst(char* timestr, int32_t len, int64_t* utime, int32_t timePrec, char delim);
static char*   forwardToTimeStringEnd(char* str);
static bool    checkTzPresent(const char* str, int32_t len);
static int32_t parseTimezone(char* str, int64_t* tzOffset);

static int32_t (*parseLocaltimeFp[])(char* timestr, int32_t len, int64_t* utime, int32_t timePrec, char delim) = {
    parseLocaltime, parseLocaltimeDst};

int32_t taosParseTime(const char* timestr, int64_t* utime, int32_t len, int32_t timePrec, int8_t day_light) {
  /* parse datatime string in with tz */
  if (strnchr(timestr, 'T', len, false) != NULL) {
    if (checkTzPresent(timestr, len)) {
      return parseTimeWithTz(timestr, utime, timePrec, 'T');
    } else {
      return parseLocaltimeDst((char*)timestr, len, utime, timePrec, 'T');
    }
  } else {
    if (checkTzPresent(timestr, len)) {
      return parseTimeWithTz(timestr, utime, timePrec, 0);
    } else {
      return parseLocaltimeDst((char*)timestr, len, utime, timePrec, 0);
    }
  }
}

bool checkTzPresent(const char* str, int32_t len) {
  char*   seg = forwardToTimeStringEnd((char*)str);
  int32_t seg_len = len - (int32_t)(seg - str);

  char* c = &seg[seg_len - 1];
  for (int32_t i = 0; i < seg_len; ++i) {
    if (*c == 'Z' || *c == 'z' || *c == '+' || *c == '-') {
      return true;
    }
    c--;
  }

  return false;
}

char* forwardToTimeStringEnd(char* str) {
  int32_t i = 0;
  int32_t numOfSep = 0;

  while (str[i] != 0 && numOfSep < 2) {
    if (str[i++] == ':') {
      numOfSep++;
    }
  }

  while (str[i] >= '0' && str[i] <= '9') {
    i++;
  }

  return &str[i];
}

int64_t parseFraction(char* str, char** end, int32_t timePrec) {
  int32_t i = 0;
  int64_t fraction = 0;

  const int32_t MILLI_SEC_FRACTION_LEN = 3;
  const int32_t MICRO_SEC_FRACTION_LEN = 6;
  const int32_t NANO_SEC_FRACTION_LEN = 9;

  int32_t factor[9] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000};
  int32_t times = 1;

  while (str[i] >= '0' && str[i] <= '9') {
    i++;
  }

  int32_t totalLen = i;
  if (totalLen <= 0) {
    return -1;
  }

  /* parse the fraction */
  if (timePrec == TSDB_TIME_PRECISION_MILLI) {
    /* only use the initial 3 bits */
    if (i >= MILLI_SEC_FRACTION_LEN) {
      i = MILLI_SEC_FRACTION_LEN;
    }

    times = MILLI_SEC_FRACTION_LEN - i;
  } else if (timePrec == TSDB_TIME_PRECISION_MICRO) {
    if (i >= MICRO_SEC_FRACTION_LEN) {
      i = MICRO_SEC_FRACTION_LEN;
    }
    times = MICRO_SEC_FRACTION_LEN - i;
  } else if (timePrec == TSDB_TIME_PRECISION_NANO) {
    if (i >= NANO_SEC_FRACTION_LEN) {
      i = NANO_SEC_FRACTION_LEN;
    }
    times = NANO_SEC_FRACTION_LEN - i;
  } else {
    return -1;
  }

  fraction = strnatoi(str, i) * factor[times];
  *end = str + totalLen;

  return fraction;
}

int32_t parseTimezone(char* str, int64_t* tzOffset) {
  int64_t hour = 0;

  int32_t i = 0;
  if (str[i] != '+' && str[i] != '-') {
    return -1;
  }

  i++;

  int32_t j = i;
  while (str[j]) {
    if ((str[j] >= '0' && str[j] <= '9') || str[j] == ':') {
      ++j;
      continue;
    }

    return -1;
  }

  char* sep = strchr(&str[i], ':');
  if (sep != NULL) {
    int32_t len = (int32_t)(sep - &str[i]);

    hour = strnatoi(&str[i], len);
    i += len + 1;
  } else {
    hour = strnatoi(&str[i], 2);
    i += 2;
  }

  if (hour > 12 || hour < 0) {
    return -1;
  }

  // return error if there're illegal charaters after min(2 Digits)
  char* minStr = &str[i];
  if (minStr[1] != '\0' && minStr[2] != '\0') {
    return -1;
  }

  int64_t minute = strnatoi(&str[i], 2);
  if (minute > 59 || (hour == 12 && minute > 0)) {
    return -1;
  }

  if (str[0] == '+') {
    *tzOffset = -(hour * 3600 + minute * 60);
  } else {
    *tzOffset = hour * 3600 + minute * 60;
  }

  return 0;
}

int32_t offsetOfTimezone(char* tzStr, int64_t* offset) {
  if (tzStr && (tzStr[0] == 'z' || tzStr[0] == 'Z')) {
    *offset = 0;
    return 0;
  }
  return parseTimezone(tzStr, offset);
}

/*
 * rfc3339 format:
 * 2013-04-12T15:52:01+08:00
 * 2013-04-12T15:52:01.123+08:00
 *
 * 2013-04-12T15:52:01Z
 * 2013-04-12T15:52:01.123Z
 *
 * iso-8601 format:
 * 2013-04-12T15:52:01+0800
 * 2013-04-12T15:52:01.123+0800
 */
int32_t parseTimeWithTz(const char* timestr, int64_t* time, int32_t timePrec, char delim) {
  int64_t factor = TSDB_TICK_PER_SECOND(timePrec);
  int64_t tzOffset = 0;

  struct tm tm = {0};

  char* str;
  if (delim == 'T') {
    str = taosStrpTime(timestr, "%Y-%m-%dT%H:%M:%S", &tm);
  } else if (delim == 0) {
    str = taosStrpTime(timestr, "%Y-%m-%d %H:%M:%S", &tm);
  } else {
    str = NULL;
  }

  if (str == NULL) {
    return -1;
  }

/* mktime will be affected by TZ, set by using taos_options */
#ifdef WINDOWS
  int64_t seconds = user_mktime64(tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, 0);
  // int64_t seconds = gmtime(&tm);
#else
  int64_t seconds = timegm(&tm);
#endif

  int64_t fraction = 0;
  str = forwardToTimeStringEnd((char*)timestr);

  if ((str[0] == 'Z' || str[0] == 'z') && str[1] == '\0') {
    /* utc time, no millisecond, return directly*/
    *time = seconds * factor;
  } else if (str[0] == '.') {
    str += 1;
    if ((fraction = parseFraction(str, &str, timePrec)) < 0) {
      return -1;
    }

    *time = seconds * factor + fraction;

    char seg = str[0];
    if (seg != 'Z' && seg != 'z' && seg != '+' && seg != '-') {
      return -1;
    } else if ((seg == 'Z' || seg == 'z') && str[1] != '\0') {
      return -1;
    } else if (seg == '+' || seg == '-') {
      // parse the timezone
      if (parseTimezone(str, &tzOffset) == -1) {
        return -1;
      }

      *time += tzOffset * factor;
    }

  } else if (str[0] == '+' || str[0] == '-') {
    *time = seconds * factor + fraction;

    // parse the timezone
    if (parseTimezone(str, &tzOffset) == -1) {
      return -1;
    }

    *time += tzOffset * factor;
  } else {
    return -1;
  }

  return 0;
}

static FORCE_INLINE bool validateTm(struct tm* pTm) {
  if (pTm == NULL) {
    return false;
  }

  int32_t dayOfMonth[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  int32_t leapYearMonthDay = 29;
  int32_t year = pTm->tm_year + 1900;
  bool    isLeapYear = ((year % 100) == 0) ? ((year % 400) == 0) : ((year % 4) == 0);

  if (isLeapYear && (pTm->tm_mon == 1)) {
    if (pTm->tm_mday > leapYearMonthDay) {
      return false;
    }
  } else {
    if (pTm->tm_mday > dayOfMonth[pTm->tm_mon]) {
      return false;
    }
  }

  return true;
}

int32_t parseLocaltime(char* timestr, int32_t len, int64_t* utime, int32_t timePrec, char delim) {
  *utime = 0;
  struct tm tm = {0};

  char* str;
  if (delim == 'T') {
    str = taosStrpTime(timestr, "%Y-%m-%dT%H:%M:%S", &tm);
  } else if (delim == 0) {
    str = taosStrpTime(timestr, "%Y-%m-%d %H:%M:%S", &tm);
  } else {
    str = NULL;
  }

  if (str == NULL || (((str - timestr) < len) && (*str != '.')) || !validateTm(&tm)) {
    // if parse failed, try "%Y-%m-%d" format
    str = taosStrpTime(timestr, "%Y-%m-%d", &tm);
    if (str == NULL || (((str - timestr) < len) && (*str != '.')) || !validateTm(&tm)) {
      return -1;
    }
  }

#ifdef _MSC_VER
#if _MSC_VER >= 1900
  int64_t timezone = _timezone;
#endif
#endif

  int64_t seconds =
      user_mktime64(tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, timezone);

  int64_t fraction = 0;

  if (*str == '.') {
    /* parse the second fraction part */
    if ((fraction = parseFraction(str + 1, &str, timePrec)) < 0) {
      return -1;
    }
  }

  *utime = TSDB_TICK_PER_SECOND(timePrec) * seconds + fraction;
  return 0;
}

int32_t parseLocaltimeDst(char* timestr, int32_t len, int64_t* utime, int32_t timePrec, char delim) {
  *utime = 0;
  struct tm tm = {0};
  tm.tm_isdst = -1;

  char* str;
  if (delim == 'T') {
    str = taosStrpTime(timestr, "%Y-%m-%dT%H:%M:%S", &tm);
  } else if (delim == 0) {
    str = taosStrpTime(timestr, "%Y-%m-%d %H:%M:%S", &tm);
  } else {
    str = NULL;
  }

  if (str == NULL || (((str - timestr) < len) && (*str != '.')) || !validateTm(&tm)) {
    // if parse failed, try "%Y-%m-%d" format
    str = taosStrpTime(timestr, "%Y-%m-%d", &tm);
    if (str == NULL || (((str - timestr) < len) && (*str != '.')) || !validateTm(&tm)) {
      return -1;
    }
  }

  /* mktime will be affected by TZ, set by using taos_options */
  int64_t seconds = taosMktime(&tm);

  int64_t fraction = 0;
  if (*str == '.') {
    /* parse the second fraction part */
    if ((fraction = parseFraction(str + 1, &str, timePrec)) < 0) {
      return -1;
    }
  }

  *utime = TSDB_TICK_PER_SECOND(timePrec) * seconds + fraction;
  return 0;
}

char getPrecisionUnit(int32_t precision) {
  static char units[3] = {TIME_UNIT_MILLISECOND, TIME_UNIT_MICROSECOND, TIME_UNIT_NANOSECOND};
  switch (precision) {
    case TSDB_TIME_PRECISION_MILLI:
    case TSDB_TIME_PRECISION_MICRO:
    case TSDB_TIME_PRECISION_NANO:
      return units[precision];
    default:
      return 0;
  }
}

int64_t convertTimePrecision(int64_t utime, int32_t fromPrecision, int32_t toPrecision) {
  ASSERT(fromPrecision == TSDB_TIME_PRECISION_MILLI || fromPrecision == TSDB_TIME_PRECISION_MICRO ||
         fromPrecision == TSDB_TIME_PRECISION_NANO);
  ASSERT(toPrecision == TSDB_TIME_PRECISION_MILLI || toPrecision == TSDB_TIME_PRECISION_MICRO ||
         toPrecision == TSDB_TIME_PRECISION_NANO);

  switch (fromPrecision) {
    case TSDB_TIME_PRECISION_MILLI: {
      switch (toPrecision) {
        case TSDB_TIME_PRECISION_MILLI:
          return utime;
        case TSDB_TIME_PRECISION_MICRO:
          if (utime > INT64_MAX / 1000) {
            return INT64_MAX;
          }
          return utime * 1000;
        case TSDB_TIME_PRECISION_NANO:
          if (utime > INT64_MAX / 1000000) {
            return INT64_MAX;
          }
          return utime * 1000000;
        default:
          ASSERT(0);
          return utime;
      }
    }  // end from milli
    case TSDB_TIME_PRECISION_MICRO: {
      switch (toPrecision) {
        case TSDB_TIME_PRECISION_MILLI:
          return utime / 1000;
        case TSDB_TIME_PRECISION_MICRO:
          return utime;
        case TSDB_TIME_PRECISION_NANO:
          if (utime > INT64_MAX / 1000) {
            return INT64_MAX;
          }
          return utime * 1000;
        default:
          ASSERT(0);
          return utime;
      }
    }  // end from micro
    case TSDB_TIME_PRECISION_NANO: {
      switch (toPrecision) {
        case TSDB_TIME_PRECISION_MILLI:
          return utime / 1000000;
        case TSDB_TIME_PRECISION_MICRO:
          return utime / 1000;
        case TSDB_TIME_PRECISION_NANO:
          return utime;
        default:
          ASSERT(0);
          return utime;
      }
    }  // end from nano
    default: {
      ASSERT(0);
      return utime;  // only to pass windows compilation
    }
  }  // end switch fromPrecision

  return utime;
}

// !!!!notice:there are precision problems, double lose precison if time is too large, for example:
// 1626006833631000000*1.0 = double = 1626006833631000064
// int64_t convertTimePrecision(int64_t time, int32_t fromPrecision, int32_t toPrecision) {
//  assert(fromPrecision == TSDB_TIME_PRECISION_MILLI || fromPrecision == TSDB_TIME_PRECISION_MICRO ||
//         fromPrecision == TSDB_TIME_PRECISION_NANO);
//  assert(toPrecision == TSDB_TIME_PRECISION_MILLI || toPrecision == TSDB_TIME_PRECISION_MICRO ||
//         toPrecision == TSDB_TIME_PRECISION_NANO);
//  static double factors[3][3] = {{1., 1000., 1000000.}, {1.0 / 1000, 1., 1000.}, {1.0 / 1000000, 1.0 / 1000, 1.}};
//  ((double)time * factors[fromPrecision][toPrecision]);
//}

// !!!!notice: double lose precison if time is too large, for example: 1626006833631000000*1.0 = double =
// 1626006833631000064
int64_t convertTimeFromPrecisionToUnit(int64_t time, int32_t fromPrecision, char toUnit) {
  if (fromPrecision != TSDB_TIME_PRECISION_MILLI && fromPrecision != TSDB_TIME_PRECISION_MICRO &&
      fromPrecision != TSDB_TIME_PRECISION_NANO) {
    return -1;
  }

  int64_t factors[3] = {NANOSECOND_PER_MSEC, NANOSECOND_PER_USEC, 1};
  double  tmp = time;
  switch (toUnit) {
    case 's': {
      time /= (NANOSECOND_PER_SEC / factors[fromPrecision]);
      tmp = (double)time;
      break;
    }
    case 'm':
      time /= (NANOSECOND_PER_MINUTE / factors[fromPrecision]);
      tmp = (double)time;
      break;
    case 'h':
      time /= (NANOSECOND_PER_HOUR / factors[fromPrecision]);
      tmp = (double)time;
      break;
    case 'd':
      time /= (NANOSECOND_PER_DAY / factors[fromPrecision]);
      tmp = (double)time;
      break;
    case 'w':
      time /= (NANOSECOND_PER_WEEK / factors[fromPrecision]);
      tmp = (double)time;
      break;
    case 'a':
      time /= (NANOSECOND_PER_MSEC / factors[fromPrecision]);
      tmp = (double)time;
      break;
    case 'u':
      // the result of (NANOSECOND_PER_USEC/(double)factors[fromPrecision]) maybe a double
      switch (fromPrecision) {
        case TSDB_TIME_PRECISION_MILLI: {
          tmp *= 1000;
          time *= 1000;
          break;
        }
        case TSDB_TIME_PRECISION_MICRO: {
          time /= 1;
          tmp = (double)time;
          break;
        }
        case TSDB_TIME_PRECISION_NANO: {
          time /= 1000;
          tmp = (double)time;
          break;
        }
      }
      break;
    case 'b':
      tmp *= factors[fromPrecision];
      time *= factors[fromPrecision];
      break;
    default: {
      return -1;
    }
  }
  if (tmp >= (double)INT64_MAX) return INT64_MAX;
  if (tmp <= (double)INT64_MIN) return INT64_MIN;
  return time;
}

int32_t convertStringToTimestamp(int16_t type, char* inputData, int64_t timePrec, int64_t* timeVal) {
  int32_t charLen = varDataLen(inputData);
  char*   newColData;
  if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_VARBINARY) {
    newColData = taosMemoryCalloc(1, charLen + 1);
    memcpy(newColData, varDataVal(inputData), charLen);
    int32_t ret = taosParseTime(newColData, timeVal, charLen, (int32_t)timePrec, tsDaylight);
    if (ret != TSDB_CODE_SUCCESS) {
      taosMemoryFree(newColData);
      return TSDB_CODE_INVALID_TIMESTAMP;
    }
    taosMemoryFree(newColData);
  } else if (type == TSDB_DATA_TYPE_NCHAR) {
    newColData = taosMemoryCalloc(1, charLen + TSDB_NCHAR_SIZE);
    int len = taosUcs4ToMbs((TdUcs4*)varDataVal(inputData), charLen, newColData);
    if (len < 0) {
      taosMemoryFree(newColData);
      return TSDB_CODE_FAILED;
    }
    newColData[len] = 0;
    int32_t ret = taosParseTime(newColData, timeVal, len, (int32_t)timePrec, tsDaylight);
    if (ret != TSDB_CODE_SUCCESS) {
      taosMemoryFree(newColData);
      return ret;
    }
    taosMemoryFree(newColData);
  } else {
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getDuration(int64_t val, char unit, int64_t* result, int32_t timePrecision) {
  switch (unit) {
    case 's':
      if (val > INT64_MAX / MILLISECOND_PER_SECOND) {
        return -1;
      }
      (*result) = convertTimePrecision(val * MILLISECOND_PER_SECOND, TSDB_TIME_PRECISION_MILLI, timePrecision);
      break;
    case 'm':
      if (val > INT64_MAX / MILLISECOND_PER_MINUTE) {
        return -1;
      }
      (*result) = convertTimePrecision(val * MILLISECOND_PER_MINUTE, TSDB_TIME_PRECISION_MILLI, timePrecision);
      break;
    case 'h':
      if (val > INT64_MAX / MILLISECOND_PER_MINUTE) {
        return -1;
      }
      (*result) = convertTimePrecision(val * MILLISECOND_PER_HOUR, TSDB_TIME_PRECISION_MILLI, timePrecision);
      break;
    case 'd':
      if (val > INT64_MAX / MILLISECOND_PER_DAY) {
        return -1;
      }
      (*result) = convertTimePrecision(val * MILLISECOND_PER_DAY, TSDB_TIME_PRECISION_MILLI, timePrecision);
      break;
    case 'w':
      if (val > INT64_MAX / MILLISECOND_PER_WEEK) {
        return -1;
      }
      (*result) = convertTimePrecision(val * MILLISECOND_PER_WEEK, TSDB_TIME_PRECISION_MILLI, timePrecision);
      break;
    case 'a':
      (*result) = convertTimePrecision(val, TSDB_TIME_PRECISION_MILLI, timePrecision);
      break;
    case 'u':
      (*result) = convertTimePrecision(val, TSDB_TIME_PRECISION_MICRO, timePrecision);
      break;
    case 'b':
      (*result) = convertTimePrecision(val, TSDB_TIME_PRECISION_NANO, timePrecision);
      break;
    default: {
      return -1;
    }
  }
  return 0;
}

/*
 * n - months
 * y - Years
 * is not allowed, since the duration of month or year are both variable.
 *
 * b - nanoseconds;
 * u - microseconds;
 * a - Millionseconds
 * s - Seconds
 * m - Minutes
 * h - Hours
 * d - Days (24 hours)
 * w - Weeks (7 days)
 */
int32_t parseAbsoluteDuration(const char* token, int32_t tokenlen, int64_t* duration, char* unit,
                              int32_t timePrecision) {
  errno = 0;
  char* endPtr = NULL;

  /* get the basic numeric value */
  int64_t timestamp = taosStr2Int64(token, &endPtr, 10);
  if ((timestamp == 0 && token[0] != '0') || errno != 0) {
    return -1;
  }

  /* natual month/year are not allowed in absolute duration */
  *unit = token[tokenlen - 1];
  if (*unit == 'n' || *unit == 'y') {
    return -1;
  }

  return getDuration(timestamp, *unit, duration, timePrecision);
}

int32_t parseNatualDuration(const char* token, int32_t tokenLen, int64_t* duration, char* unit, int32_t timePrecision) {
  errno = 0;

  /* get the basic numeric value */
  *duration = taosStr2Int64(token, NULL, 10);
  if (*duration < 0 || errno != 0) {
    return -1;
  }

  *unit = token[tokenLen - 1];
  if (*unit == 'n' || *unit == 'y') {
    return 0;
  }
  if(isdigit(*unit)) {
    *unit = getPrecisionUnit(timePrecision);
  }

  return getDuration(*duration, *unit, duration, timePrecision);
}

static bool taosIsLeapYear(int32_t year) {
  return (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
}

int64_t taosTimeAdd(int64_t t, int64_t duration, char unit, int32_t precision) {
  if (duration == 0) {
    return t;
  }

  if (!IS_CALENDAR_TIME_DURATION(unit)) {
    return t + duration;
  }

  // The following code handles the y/n time duration
  int64_t numOfMonth = (unit == 'y') ? duration * 12 : duration;
  int64_t fraction = t % TSDB_TICK_PER_SECOND(precision);

  struct tm tm;
  time_t    tt = (time_t)(t / TSDB_TICK_PER_SECOND(precision));
  taosLocalTime(&tt, &tm, NULL);
  int32_t mon = tm.tm_year * 12 + tm.tm_mon + (int32_t)numOfMonth;
  tm.tm_year = mon / 12;
  tm.tm_mon = mon % 12;
  int daysOfMonth[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (taosIsLeapYear(1900 + tm.tm_year)) {
    daysOfMonth[1] = 29;
  }
  if (tm.tm_mday > daysOfMonth[tm.tm_mon]) {
    tm.tm_mday = daysOfMonth[tm.tm_mon];
  }
  return (int64_t)(taosMktime(&tm) * TSDB_TICK_PER_SECOND(precision) + fraction);
}

/**
 * @brief calc how many windows after filling between skey and ekey
 * @notes for asc order
 *     skey      --->       ekey
 *      ^                    ^
 * _____!_____.........._____|_____..
 *      |__1__)
 *            |__2__)...-->|_ret+1_)
 *      skey + ret * interval <= ekey
 *      skey + ret * interval + interval > ekey
 * ======> (ekey - skey - interval) / interval < ret <= (ekey - skey) / interval
 * For keys from blocks which do not need filling, skey + ret * interval == ekey.
 * For keys need filling, skey + ret * interval <= ekey.
 * Total num of windows is ret + 1(the last window)
 *
 *        for desc order
 *     skey       <---      ekey
 *      ^                    ^
 * _____|____..........______!____...
 *                           |_first_)
 *                     |__1__)
 *  |_ret_)<--...|__2__)
 *      skey >= ekey - ret * interval
 *      skey < ekey - ret * interval + interval
 *=======> (ekey - skey) / interval <= ret < (ekey - skey + interval) / interval
 * For keys from blocks which do not need filling, skey == ekey - ret * interval.
 * For keys need filling, skey >= ekey - ret * interval.
 * Total num of windows is ret + 1(the first window)
 */
int32_t taosTimeCountIntervalForFill(int64_t skey, int64_t ekey, int64_t interval, char unit, int32_t precision,
                                     int32_t order) {
  if (ekey < skey) {
    int64_t tmp = ekey;
    ekey = skey;
    skey = tmp;
  }
  int32_t ret;

  if (unit != 'n' && unit != 'y') {
    ret = (int32_t)((ekey - skey) / interval);
    if (order == TSDB_ORDER_DESC && ret * interval < (ekey - skey)) ret += 1;
  } else {
    skey /= (int64_t)(TSDB_TICK_PER_SECOND(precision));
    ekey /= (int64_t)(TSDB_TICK_PER_SECOND(precision));

    struct tm tm;
    time_t    t = (time_t)skey;
    taosLocalTime(&t, &tm, NULL);
    int32_t smon = tm.tm_year * 12 + tm.tm_mon;

    t = (time_t)ekey;
    taosLocalTime(&t, &tm, NULL);
    int32_t emon = tm.tm_year * 12 + tm.tm_mon;

    if (unit == 'y') {
      interval *= 12;
    }
    ret = (emon - smon) / (int32_t)interval;
    if (order == TSDB_ORDER_DESC && ret * interval < (smon - emon)) ret += 1;
  }
  return ret + 1;
}

int64_t taosTimeTruncate(int64_t ts, const SInterval* pInterval) {
  if (pInterval->sliding == 0) {
    ASSERT(pInterval->interval == 0);
    return ts;
  }

  int64_t start = ts;
  int32_t precision = pInterval->precision;

  if (IS_CALENDAR_TIME_DURATION(pInterval->slidingUnit)) {
    start /= (int64_t)(TSDB_TICK_PER_SECOND(precision));
    struct tm tm;
    time_t    tt = (time_t)start;
    taosLocalTime(&tt, &tm, NULL);
    tm.tm_sec = 0;
    tm.tm_min = 0;
    tm.tm_hour = 0;
    tm.tm_mday = 1;

    if (pInterval->slidingUnit == 'y') {
      tm.tm_mon = 0;
      tm.tm_year = (int32_t)(tm.tm_year / pInterval->sliding * pInterval->sliding);
    } else {
      int32_t mon = tm.tm_year * 12 + tm.tm_mon;
      mon = (int32_t)(mon / pInterval->sliding * pInterval->sliding);
      tm.tm_year = mon / 12;
      tm.tm_mon = mon % 12;
    }

    start = (int64_t)(taosMktime(&tm) * TSDB_TICK_PER_SECOND(precision));
  } else {
    if (IS_CALENDAR_TIME_DURATION(pInterval->intervalUnit)) {
      int64_t news = (ts / pInterval->sliding) * pInterval->sliding;
      ASSERT(news <= ts);

      if (news <= ts) {
        int64_t prev = news;
        int64_t newe = taosTimeAdd(news, pInterval->interval, pInterval->intervalUnit, precision) - 1;

        if (newe < ts) {  // move towards the greater endpoint
          while (newe < ts && news < ts) {
            news += pInterval->sliding;
            newe = taosTimeAdd(news, pInterval->interval, pInterval->intervalUnit, precision) - 1;
          }

          prev = news;
        } else {
          while (newe >= ts) {
            prev = news;
            news -= pInterval->sliding;
            newe = taosTimeAdd(news, pInterval->interval, pInterval->intervalUnit, precision) - 1;
          }
        }

        return prev;
      }
    } else {
      int64_t delta = ts - pInterval->interval;
      int32_t factor = (delta >= 0) ? 1 : -1;

      start = (delta / pInterval->sliding + factor) * pInterval->sliding;

      if (pInterval->intervalUnit == 'd' || pInterval->intervalUnit == 'w') {
        /*
         * here we revised the start time of day according to the local time zone,
         * but in case of DST, the start time of one day need to be dynamically decided.
         */
        // todo refactor to extract function that is available for Linux/Windows/Mac platform
#if defined(WINDOWS) && _MSC_VER >= 1900
        // see
        // https://docs.microsoft.com/en-us/cpp/c-runtime-library/daylight-dstbias-timezone-and-tzname?view=vs-2019
        int64_t timezone = _timezone;
        int32_t daylight = _daylight;
        char**  tzname = _tzname;
#endif

        start += (int64_t)(timezone * TSDB_TICK_PER_SECOND(precision));
      }

      int64_t end = 0;

      // not enough time range
      if (start < 0 || INT64_MAX - start > pInterval->interval - 1) {
        end = taosTimeAdd(start, pInterval->interval, pInterval->intervalUnit, precision) - 1;
        while (end < ts) {  // move forward to the correct time window
          start += pInterval->sliding;

          if (start < 0 || INT64_MAX - start > pInterval->interval - 1) {
            end = start + pInterval->interval - 1;
          } else {
            end = INT64_MAX;
            break;
          }
        }
      } else {
        end = INT64_MAX;
      }
    }
  }

  ASSERT(pInterval->offset >= 0);

  if (pInterval->offset > 0) {
    // try to move current window to the left-hande-side, due to the offset effect.
    int64_t newe = taosTimeAdd(start, pInterval->interval, pInterval->intervalUnit, precision) - 1;
    int64_t slidingStart = start;
    while (newe >= ts) {
      start = slidingStart;
      slidingStart = taosTimeAdd(slidingStart, -pInterval->sliding, pInterval->slidingUnit, precision);
      int64_t slidingEnd = taosTimeAdd(slidingStart, pInterval->interval, pInterval->intervalUnit, precision) - 1;
      newe = taosTimeAdd(slidingEnd, pInterval->offset, pInterval->offsetUnit, precision);
    }
    start = taosTimeAdd(start, pInterval->offset, pInterval->offsetUnit, precision);
  }

  return start;
}

// used together with taosTimeTruncate. when offset is great than zero, slide-start/slide-end is the anchor point
int64_t taosTimeGetIntervalEnd(int64_t intervalStart, const SInterval* pInterval) {
  if (pInterval->offset > 0) {
    int64_t slideStart = taosTimeAdd(intervalStart, -1 * pInterval->offset, pInterval->offsetUnit, pInterval->precision);
    int64_t slideEnd = taosTimeAdd(slideStart, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1;
    int64_t result = taosTimeAdd(slideEnd, pInterval->offset, pInterval->offsetUnit, pInterval->precision);
    return result;
  } else {
    int64_t result = taosTimeAdd(intervalStart, pInterval->interval, pInterval->intervalUnit, pInterval->precision) - 1;
    return result;
  }
}
// internal function, when program is paused in debugger,
// one can call this function from debugger to print a
// timestamp as human readable string, for example (gdb):
//     p fmtts(1593769722)
// outputs:
//     2020-07-03 17:48:42
// and the parameter can also be a variable.
const char* fmtts(int64_t ts) {
  static char buf[96] = {0};
  size_t      pos = 0;
  struct tm   tm;

  if (ts > -62135625943 && ts < 32503651200) {
    time_t t = (time_t)ts;
    if (taosLocalTime(&t, &tm, buf) == NULL) {
      return buf;
    }
    pos += strftime(buf + pos, sizeof(buf), "s=%Y-%m-%d %H:%M:%S", &tm);
  }

  if (ts > -62135625943000 && ts < 32503651200000) {
    time_t t = (time_t)(ts / 1000);
    if (taosLocalTime(&t, &tm, buf) == NULL) {
      return buf;
    }
    if (pos > 0) {
      buf[pos++] = ' ';
      buf[pos++] = '|';
      buf[pos++] = ' ';
    }
    pos += strftime(buf + pos, sizeof(buf), "ms=%Y-%m-%d %H:%M:%S", &tm);
    pos += sprintf(buf + pos, ".%03d", (int32_t)(ts % 1000));
  }

  {
    time_t t = (time_t)(ts / 1000000);
    if (taosLocalTime(&t, &tm, buf) == NULL) {
      return buf;
    }
    if (pos > 0) {
      buf[pos++] = ' ';
      buf[pos++] = '|';
      buf[pos++] = ' ';
    }
    pos += strftime(buf + pos, sizeof(buf), "us=%Y-%m-%d %H:%M:%S", &tm);
    pos += sprintf(buf + pos, ".%06d", (int32_t)(ts % 1000000));
  }

  return buf;
}

void taosFormatUtcTime(char* buf, int32_t bufLen, int64_t t, int32_t precision) {
  char      ts[40] = {0};
  struct tm ptm;

  int32_t fractionLen;
  char*   format = NULL;
  time_t  quot = 0;
  long    mod = 0;

  switch (precision) {
    case TSDB_TIME_PRECISION_MILLI: {
      quot = t / 1000;
      fractionLen = 5;
      format = ".%03" PRId64;
      mod = t % 1000;
      break;
    }

    case TSDB_TIME_PRECISION_MICRO: {
      quot = t / 1000000;
      fractionLen = 8;
      format = ".%06" PRId64;
      mod = t % 1000000;
      break;
    }

    case TSDB_TIME_PRECISION_NANO: {
      quot = t / 1000000000;
      fractionLen = 11;
      format = ".%09" PRId64;
      mod = t % 1000000000;
      break;
    }

    default:
      fractionLen = 0;
      return;
  }

  if (taosLocalTime(&quot, &ptm, buf) == NULL) {
    return;
  }
  int32_t length = (int32_t)strftime(ts, 40, "%Y-%m-%dT%H:%M:%S", &ptm);
  length += snprintf(ts + length, fractionLen, format, mod);
  length += (int32_t)strftime(ts + length, 40 - length, "%z", &ptm);

  tstrncpy(buf, ts, bufLen);
}

int32_t taosTs2Tm(int64_t ts, int32_t precision, struct STm* tm) {
  tm->fsec = ts % TICK_PER_SECOND[precision] * (TICK_PER_SECOND[TSDB_TIME_PRECISION_NANO] / TICK_PER_SECOND[precision]);
  time_t t = ts / TICK_PER_SECOND[precision];
  taosLocalTime(&t, &tm->tm, NULL);
  return TSDB_CODE_SUCCESS;
}

int32_t taosTm2Ts(struct STm* tm, int64_t* ts, int32_t precision) {
  *ts = taosMktime(&tm->tm);
  *ts *= TICK_PER_SECOND[precision];
  *ts += tm->fsec / (TICK_PER_SECOND[TSDB_TIME_PRECISION_NANO] / TICK_PER_SECOND[precision]);
  return TSDB_CODE_SUCCESS;
}

typedef struct {
  const char* name;
  int         len;
  int         id;
  bool        isDigit;
} TSFormatKeyWord;

typedef enum {
  // TSFKW_AD,   // BC AD
  // TSFKW_A_D,  // A.D. B.C.
  TSFKW_AM,   // AM, PM
  TSFKW_A_M,  // A.M., P.M.
  // TSFKW_BC,   // BC AD
  // TSFKW_B_C,  // B.C. A.D.
  TSFKW_DAY,  // MONDAY, TUESDAY ...
  TSFKW_DDD,  // Day of year 001-366
  TSFKW_DD,   // Day of month 01-31
  TSFKW_Day,  // Sunday, Monday
  TSFKW_DY,   // MON, TUE
  TSFKW_Dy,   // Mon, Tue
  TSFKW_D,    // 1-7 -> Sunday(1) -> Saturday(7)
  TSFKW_HH24,
  TSFKW_HH12,
  TSFKW_HH,
  TSFKW_MI,  // minute
  TSFKW_MM,
  TSFKW_MONTH,  // JANUARY, FEBRUARY
  TSFKW_MON,
  TSFKW_Month,
  TSFKW_Mon,
  TSFKW_MS,
  TSFKW_NS,
  //TSFKW_OF,
  TSFKW_PM,
  TSFKW_P_M,
  TSFKW_SS,
  TSFKW_TZH,
  // TSFKW_TZM,
  // TSFKW_TZ,
  TSFKW_US,
  TSFKW_YYYY,
  TSFKW_YYY,
  TSFKW_YY,
  TSFKW_Y,
  // TSFKW_a_d,
  // TSFKW_ad,
  TSFKW_am,
  TSFKW_a_m,
  // TSFKW_b_c,
  // TSFKW_bc,
  TSFKW_day,
  TSFKW_ddd,
  TSFKW_dd,
  TSFKW_dy,   // mon, tue
  TSFKW_d,
  TSFKW_hh24,
  TSFKW_hh12,
  TSFKW_hh,
  TSFKW_mi,
  TSFKW_mm,
  TSFKW_month,
  TSFKW_mon,
  TSFKW_ms,
  TSFKW_ns,
  TSFKW_pm,
  TSFKW_p_m,
  TSFKW_ss,
  TSFKW_tzh,
  // TSFKW_tzm,
  // TSFKW_tz,
  TSFKW_us,
  TSFKW_yyyy,
  TSFKW_yyy,
  TSFKW_yy,
  TSFKW_y,
  TSFKW_last_
} TSFormatKeywordId;

// clang-format off
static const TSFormatKeyWord formatKeyWords[] = {
  //{"AD", 2, TSFKW_AD, false},
  //{"A.D.", 4, TSFKW_A_D},
  {"AM", 2, TSFKW_AM, false},
  {"A.M.", 4, TSFKW_A_M, false},
  //{"BC", 2, TSFKW_BC, false},
  //{"B.C.", 4, TSFKW_B_C, false},
  {"DAY", 3, TSFKW_DAY, false},
  {"DDD", 3, TSFKW_DDD, true},
  {"DD", 2, TSFKW_DD, true},
  {"Day", 3, TSFKW_Day, false},
  {"DY", 2, TSFKW_DY, false},
  {"Dy", 2, TSFKW_Dy, false},
  {"D", 1, TSFKW_D, true},
  {"HH24", 4, TSFKW_HH24, true},
  {"HH12", 4, TSFKW_HH12, true},
  {"HH", 2, TSFKW_HH, true},
  {"MI", 2, TSFKW_MI, true},
  {"MM", 2, TSFKW_MM, true},
  {"MONTH", 5, TSFKW_MONTH, false},
  {"MON", 3, TSFKW_MON, false},
  {"Month", 5, TSFKW_Month, false},
  {"Mon", 3, TSFKW_Mon, false},
  {"MS", 2, TSFKW_MS, true},
  {"NS", 2, TSFKW_NS, true},
  //{"OF", 2, TSFKW_OF, false},
  {"PM", 2, TSFKW_PM, false},
  {"P.M.", 4, TSFKW_P_M, false},
  {"SS", 2, TSFKW_SS, true},
  {"TZH", 3, TSFKW_TZH, false},
  //{"TZM", 3, TSFKW_TZM},
  //{"TZ", 2, TSFKW_TZ},
  {"US", 2, TSFKW_US, true},
  {"YYYY", 4, TSFKW_YYYY, true},
  {"YYY", 3, TSFKW_YYY, true},
  {"YY", 2, TSFKW_YY, true},
  {"Y", 1, TSFKW_Y, true},
  //{"a.d.", 4, TSFKW_a_d, false},
  //{"ad", 2, TSFKW_ad, false},
  {"am", 2, TSFKW_am, false},
  {"a.m.", 4, TSFKW_a_m, false},
  //{"b.c.", 4, TSFKW_b_c, false},
  //{"bc", 2, TSFKW_bc, false},
  {"day", 3, TSFKW_day, false},
  {"ddd", 3, TSFKW_DDD, true},
  {"dd", 2, TSFKW_DD, true},
  {"dy", 2, TSFKW_dy, false},
  {"d", 1, TSFKW_D, true},
  {"hh24", 4, TSFKW_HH24, true},
  {"hh12", 4, TSFKW_HH12, true},
  {"hh", 2, TSFKW_HH, true},
  {"mi", 2, TSFKW_MI, true},
  {"mm", 2, TSFKW_MM, true},
  {"month", 5, TSFKW_month, false},
  {"mon", 3, TSFKW_mon, false},
  {"ms", 2, TSFKW_MS, true},
  {"ns", 2, TSFKW_NS, true},
  //{"of", 2, TSFKW_OF, false},
  {"pm", 2, TSFKW_pm, false},
  {"p.m.", 4, TSFKW_p_m, false},
  {"ss", 2, TSFKW_SS, true},
  {"tzh", 3, TSFKW_TZH, false},
  //{"tzm", 3, TSFKW_TZM},
  //{"tz", 2, TSFKW_tz},
  {"us", 2, TSFKW_US, true},
  {"yyyy", 4, TSFKW_YYYY, true},
  {"yyy", 3, TSFKW_YYY, true},
  {"yy", 2, TSFKW_YY, true},
  {"y", 1, TSFKW_Y, true},
  {NULL, 0, 0}
};
// clang-format on

#define TS_FROMAT_KEYWORD_INDEX_SIZE ('z' - 'A' + 1)
static const int TSFormatKeywordIndex[TS_FROMAT_KEYWORD_INDEX_SIZE] = {
    /*A*/ TSFKW_AM,     -1, -1,
    /*D*/ TSFKW_DAY,    -1, -1, -1,
    /*H*/ TSFKW_HH24,   -1, -1, -1, -1,
    /*M*/ TSFKW_MI,
    /*N*/ TSFKW_NS,     -1,
    /*P*/ TSFKW_PM,     -1, -1,
    /*S*/ TSFKW_SS,
    /*T*/ TSFKW_TZH,
    /*U*/ TSFKW_US,     -1, -1, -1,
    /*Y*/ TSFKW_YYYY,   -1,
    /*[ \ ] ^ _ `*/ -1, -1, -1, -1, -1, -1,
    /*a*/ TSFKW_am,     -1, -1,
    /*d*/ TSFKW_day,    -1, -1, -1,
    /*h*/ TSFKW_hh24,   -1, -1, -1, -1,
    /*m*/ TSFKW_mi,
    /*n*/ TSFKW_ns,     -1,
    /*p*/ TSFKW_pm,     -1, -1,
    /*s*/ TSFKW_ss,
    /*t*/ TSFKW_tzh,
    /*u*/ TSFKW_us,     -1, -1, -1,
    /*y*/ TSFKW_yyyy,   -1};

typedef struct {
  uint8_t                type;
  const char*            c;
  int32_t                len;
  const TSFormatKeyWord* key;
} TSFormatNode;

static const char* const weekDays[] = {"Sunday",   "Monday", "Tuesday",  "Wednesday",
                                       "Thursday", "Friday", "Saturday", "NULL"};
static const char* const shortWeekDays[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "NULL"};
static const char* const fullMonths[] = {"January", "February",  "March",   "April",    "May",      "June", "July",
                                         "August",  "September", "October", "November", "December", NULL};
static const char* const months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul",
                                     "Aug", "Sep", "Oct", "Nov", "Dec", NULL};
#define A_M_STR "A.M."
#define a_m_str "a.m."
#define AM_STR  "AM"
#define am_str  "am"
#define P_M_STR "P.M."
#define p_m_str "p.m."
#define PM_STR  "PM"
#define pm_str  "pm"
static const char* const apms[] = {AM_STR, PM_STR, am_str, pm_str, NULL};
static const char* const long_apms[] = {A_M_STR, P_M_STR, a_m_str, p_m_str, NULL};

#define TS_FORMAT_NODE_TYPE_KEYWORD   1
#define TS_FORMAT_NODE_TYPE_SEPARATOR 2
#define TS_FORMAT_NODE_TYPE_CHAR      3

static const TSFormatKeyWord* keywordSearch(const char* str) {
  if (*str < 'A' || *str > 'z' || (*str > 'Z' && *str < 'a')) return NULL;
  int32_t idx = TSFormatKeywordIndex[str[0] - 'A'];
  if (idx < 0) return NULL;
  const TSFormatKeyWord* key = &formatKeyWords[idx++];
  while (key->name && str[0] == key->name[0]) {
    if (0 == strncmp(key->name, str, key->len)) {
      return key;
    }
    key = &formatKeyWords[idx++];
  }
  return NULL;
}

static bool isSeperatorChar(char c) {
  return (c > 0x20 && c < 0x7F && !(c >= 'A' && c <= 'Z') && !(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9'));
}

static void parseTsFormat(const char* formatStr, SArray* formats) {
  TSFormatNode* lastOtherFormat = NULL;
  while (*formatStr) {
    const TSFormatKeyWord* key = keywordSearch(formatStr);
    if (key) {
      TSFormatNode format = {.key = key, .type = TS_FORMAT_NODE_TYPE_KEYWORD};
      taosArrayPush(formats, &format);
      formatStr += key->len;
      lastOtherFormat = NULL;
    } else {
      if (*formatStr == '"') {
        lastOtherFormat = NULL;
        // for double quoted string
        formatStr++;
        TSFormatNode* last = NULL;
        while (*formatStr) {
          if (*formatStr == '"') {
            formatStr++;
            break;
          }
          if (*formatStr == '\\' && *(formatStr + 1)) {
            formatStr++;
            last = NULL; // stop expanding last format, create new format
          }
          if (last) {
            // expand
            assert(last->type == TS_FORMAT_NODE_TYPE_CHAR);
            last->len++;
            formatStr++;
          } else {
            // create new
            TSFormatNode format = {.type = TS_FORMAT_NODE_TYPE_CHAR, .key = NULL};
            format.c = formatStr;
            format.len = 1;
            taosArrayPush(formats, &format);
            formatStr++;
            last = taosArrayGetLast(formats);
          }
        }
      } else {
        // for other strings
        if (*formatStr == '\\' && *(formatStr + 1)) {
          formatStr++;
          lastOtherFormat = NULL; // stop expanding
        } else {
          if (lastOtherFormat && !isSeperatorChar(*formatStr)) {
            // expanding
          } else {
            // create new
            lastOtherFormat = NULL;
          }
        }
        if (lastOtherFormat) {
          assert(lastOtherFormat->type == TS_FORMAT_NODE_TYPE_CHAR);
          lastOtherFormat->len++;
          formatStr++;
        } else {
          TSFormatNode format = {
            .type = isSeperatorChar(*formatStr) ? TS_FORMAT_NODE_TYPE_SEPARATOR : TS_FORMAT_NODE_TYPE_CHAR,
            .key = NULL};
          format.c = formatStr;
          format.len = 1;
          taosArrayPush(formats, &format);
          formatStr++;
          if (format.type == TS_FORMAT_NODE_TYPE_CHAR) lastOtherFormat = taosArrayGetLast(formats);
        }
      }
    }
  }
}

static int32_t tm2char(const SArray* formats, const struct STm* tm, char* s, int32_t outLen) {
  int32_t size = taosArrayGetSize(formats);
  const char* start = s;
  for (int32_t i = 0; i < size; ++i) {
    TSFormatNode* format = taosArrayGet(formats, i);
    if (format->type != TS_FORMAT_NODE_TYPE_KEYWORD) {
      if (s - start + format->len + 1 > outLen) break;
      strncpy(s, format->c, format->len);
      s += format->len;
      continue;
    }
    if (s - start + 16 > outLen) break;

    switch (format->key->id) {
      case TSFKW_AM:
      case TSFKW_PM:
        sprintf(s, tm->tm.tm_hour % 24 >= 12 ? "PM" : "AM");
        s += 2;
        break;
      case TSFKW_A_M:
      case TSFKW_P_M:
        sprintf(s, tm->tm.tm_hour % 24 >= 12 ? "P.M." : "A.M.");
        s += 4;
        break;
      case TSFKW_am:
      case TSFKW_pm:
        sprintf(s, tm->tm.tm_hour % 24 >= 12 ? "pm" : "am");
        s += 2;
        break;
      case TSFKW_a_m:
      case TSFKW_p_m:
        sprintf(s, tm->tm.tm_hour % 24 >= 12 ? "p.m." : "a.m.");
        s += 4;
        break;
      case TSFKW_DDD:
#ifdef WINDOWS
        return TSDB_CODE_FUNC_TO_CHAR_NOT_SUPPORTED;
#endif
        sprintf(s, "%03d", tm->tm.tm_yday + 1);
        s += strlen(s);
        break;
      case TSFKW_DD:
        sprintf(s, "%02d", tm->tm.tm_mday);
        s += 2;
        break;
      case TSFKW_D:
        sprintf(s, "%d", tm->tm.tm_wday + 1);
        s += 1;
        break;
      case TSFKW_DAY: {
        // MONDAY, TUESDAY...
        const char* wd = weekDays[tm->tm.tm_wday];
        char        buf[10] = {0};
        for (int32_t i = 0; i < strlen(wd); ++i) buf[i] = toupper(wd[i]);
        sprintf(s, "%-9s", buf);
        s += strlen(s);
        break;
      }
      case TSFKW_Day:
        // Monday, TuesDay...
        sprintf(s, "%-9s", weekDays[tm->tm.tm_wday]);
        s += strlen(s);
        break;
      case TSFKW_day: {
        const char* wd = weekDays[tm->tm.tm_wday];
        char        buf[10] = {0};
        for (int32_t i = 0; i < strlen(wd); ++i) buf[i] = tolower(wd[i]);
        sprintf(s, "%-9s", buf);
        s += strlen(s);
        break;
      }
      case TSFKW_DY: {
        // MON, TUE
        const char* wd = shortWeekDays[tm->tm.tm_wday];
        char        buf[8] = {0};
        for (int32_t i = 0; i < strlen(wd); ++i) buf[i] = toupper(wd[i]);
        sprintf(s, "%3s", buf);
        s += 3;
        break;
      }
      case TSFKW_Dy:
        // Mon, Tue
        sprintf(s, "%3s", shortWeekDays[tm->tm.tm_wday]);
        s += 3;
        break;
      case TSFKW_dy: {
        // mon, tue
        const char* wd = shortWeekDays[tm->tm.tm_wday];
        char        buf[8] = {0};
        for (int32_t i = 0; i < strlen(wd); ++i) buf[i] = tolower(wd[i]);
        sprintf(s, "%3s", buf);
        s += 3;
        break;
      }
      case TSFKW_HH24:
        sprintf(s, "%02d", tm->tm.tm_hour);
        s += 2;
        break;
      case TSFKW_HH:
      case TSFKW_HH12:
        // 0 or 12 o'clock in 24H coresponds to 12 o'clock (AM/PM) in 12H
        sprintf(s, "%02d", tm->tm.tm_hour % 12 == 0 ? 12 : tm->tm.tm_hour % 12);
        s += 2;
        break;
      case TSFKW_MI:
        sprintf(s, "%02d", tm->tm.tm_min);
        s += 2;
        break;
      case TSFKW_MM:
        sprintf(s, "%02d", tm->tm.tm_mon + 1);
        s += 2;
        break;
      case TSFKW_MONTH: {
        const char* mon = fullMonths[tm->tm.tm_mon];
        char        buf[10] = {0};
        for (int32_t i = 0; i < strlen(mon); ++i) buf[i] = toupper(mon[i]);
        sprintf(s, "%-9s", buf);
        s += strlen(s);
        break;
      }
      case TSFKW_MON: {
        const char* mon = months[tm->tm.tm_mon];
        char        buf[10] = {0};
        for (int32_t i = 0; i < strlen(mon); ++i) buf[i] = toupper(mon[i]);
        sprintf(s, "%s", buf);
        s += strlen(s);
        break;
      }
      case TSFKW_Month:
        sprintf(s, "%-9s", fullMonths[tm->tm.tm_mon]);
        s += strlen(s);
        break;
      case TSFKW_month: {
        const char* mon = fullMonths[tm->tm.tm_mon];
        char        buf[10] = {0};
        for (int32_t i = 0; i < strlen(mon); ++i) buf[i] = tolower(mon[i]);
        sprintf(s, "%-9s", buf);
        s += strlen(s);
        break;
      }
      case TSFKW_Mon:
        sprintf(s, "%s", months[tm->tm.tm_mon]);
        s += strlen(s);
        break;
      case TSFKW_mon: {
        const char* mon = months[tm->tm.tm_mon];
        char        buf[10] = {0};
        for (int32_t i = 0; i < strlen(mon); ++i) buf[i] = tolower(mon[i]);
        sprintf(s, "%s", buf);
        s += strlen(s);
        break;
      }
      case TSFKW_SS:
        sprintf(s, "%02d", tm->tm.tm_sec);
        s += 2;
        break;
      case TSFKW_MS:
        sprintf(s, "%03" PRId64, tm->fsec / 1000000L);
        s += 3;
        break;
      case TSFKW_US:
        sprintf(s, "%06" PRId64, tm->fsec / 1000L);
        s += 6;
        break;
      case TSFKW_NS:
        sprintf(s, "%09" PRId64, tm->fsec);
        s += 9;
        break;
      case TSFKW_TZH:
        sprintf(s, "%s%02d", tsTimezone < 0 ? "-" : "+", tsTimezone);
        s += strlen(s);
        break;
      case TSFKW_YYYY:
        sprintf(s, "%04d", tm->tm.tm_year + 1900);
        s += strlen(s);
        break;
      case TSFKW_YYY:
        sprintf(s, "%03d", (tm->tm.tm_year + 1900) % 1000);
        s += strlen(s);
        break;
      case TSFKW_YY:
        sprintf(s, "%02d", (tm->tm.tm_year + 1900) % 100);
        s += strlen(s);
        break;
      case TSFKW_Y:
        sprintf(s, "%01d", (tm->tm.tm_year + 1900) % 10);
        s += strlen(s);
        break;
      default:
        break;
    }
  }
  return TSDB_CODE_SUCCESS;
}

/// @brief find s in arr case insensitively
/// @retval the index in arr if found, -1 if not found
static int32_t strArrayCaseSearch(const char* const* arr, const char* s) {
  if (!*s) return -1;
  const char* const* fmt = arr;
  for (; *fmt; ++fmt) {
    const char *l, *r;
    for (l = fmt[0], r = s;; l++, r++) {
      if (*l == '\0') return fmt - arr;
      if (*r == '\0' || tolower(*l) != tolower(*r)) break;
    }
  }
  return -1;
}

static const char* tsFormatStr2Int32(int32_t* dest, const char* str, int32_t len, bool needMoreDigit) {
  char*       last;
  int64_t     res;
  const char* s = str;
  if ('\0' == str[0]) return NULL;
  if (len <= 0) {
    res = taosStr2Int64(s, &last, 10);
    s = last;
  } else {
    char buf[16] = {0};
    strncpy(buf, s, len);
    int32_t copiedLen = strlen(buf);
    if (copiedLen < len) {
      if (!needMoreDigit) {
        // digits not enough, that's ok, cause we do not need more digits
        // '2023-1' 'YYYY-MM'
        // '202a' 'YYYY' -> 202
        res = taosStr2Int64(s, &last, 10);
        s += copiedLen;
      } else {
        // bytes not enough, and there are other digit formats to match
        // '2023-1' 'YYYY-MMDD'
        return NULL;
      }
    } else {
      if (needMoreDigit) {
        res = taosStr2Int64(buf, &last, 10);
        // bytes enough, but digits not enough, like '202a12' 'YYYYMM', YYYY needs four digits
        if (last - buf < len) return NULL;
        s += last - buf;
      } else {
        res = taosStr2Int64(s, &last, 10);
        s = last;
      }
    }
  }
  if (s == str) {
    // no integers found
    return NULL;
  }
  if (errno == ERANGE || res > INT32_MAX || res < INT32_MIN) {
    // out of range
    return NULL;
  }
  *dest = res;
  return s;
}

static int32_t adjustYearTo2020(int32_t year) {
  if (year < 70) return year + 2000;    // 2000 - 2069
  if (year < 100) return year + 1900;   // 1970 - 1999
  if (year < 520) return year + 2000;   // 2100 - 2519
  if (year < 1000) return year + 1000;  // 1520 - 1999
  return year;
}

static bool checkTm(const struct tm* tm) {
  if (tm->tm_mon < 0 || tm->tm_mon > 11) return false;
  if (tm->tm_wday < 0 || tm->tm_wday > 6) return false;
  if (tm->tm_yday < 0 || tm->tm_yday > 365) return false;
  if (tm->tm_mday < 0 || tm->tm_mday > 31) return false;
  if (tm->tm_hour < 0 || tm->tm_hour > 23) return false;
  if (tm->tm_min < 0 || tm->tm_min > 59) return false;
  if (tm->tm_sec < 0 || tm->tm_sec > 60) return false;
  return true;
}

static bool needMoreDigits(SArray* formats, int32_t curIdx) {
  if (curIdx == taosArrayGetSize(formats) - 1) return false;
  TSFormatNode* pNextNode = taosArrayGet(formats, curIdx + 1);
  if (pNextNode->type == TS_FORMAT_NODE_TYPE_SEPARATOR) {
    return false;
  } else if (pNextNode->type == TS_FORMAT_NODE_TYPE_KEYWORD) {
    return pNextNode->key->isDigit;
  } else {
    return isdigit(pNextNode->c[0]);
  }
}

/// @brief convert a formatted time str to timestamp
/// @param[in] s the formatted timestamp str
/// @param[in] formats array of TSFormatNode, output of parseTsFormat
/// @param[out] ts output timestamp
/// @param precision the timestamp precision to convert to, sec/milli/micro/nano
/// @param[out] sErrPos if not NULL, when err occured, points to the failed position of s, only set when ret is -1
/// @param[out] fErrIdx if not NULL, when err occured, the idx of the failed format idx, only set when ret is -1
/// @retval 0 for success
/// @retval -1 for format and s mismatch error
/// @retval -2 if datetime err, like 2023-13-32 25:61:69
/// @retval -3 if not supported
static int32_t char2ts(const char* s, SArray* formats, int64_t* ts, int32_t precision, const char** sErrPos,
                       int32_t* fErrIdx) {
  int32_t size = taosArrayGetSize(formats);
  int32_t pm = 0;      // default am
  int32_t hour12 = 0;  // default HH24
  int32_t year = 0, mon = 0, yd = 0, md = 1, wd = 0;
  int32_t hour = 0, min = 0, sec = 0, us = 0, ms = 0, ns = 0;
  int32_t tzSign = 1, tz = tsTimezone;
  int32_t err = 0;
  bool    withYD = false, withMD = false;

  for (int32_t i = 0; i < size && *s != '\0'; ++i) {
    while (isspace(*s) && *s != '\0') {
      s++;
    }
    if (!s) break;
    TSFormatNode* node = taosArrayGet(formats, i);
    if (node->type == TS_FORMAT_NODE_TYPE_SEPARATOR) {
      // separator matches any character
      if (isSeperatorChar(s[0])) s += node->len;
      continue;
    }
    if (node->type == TS_FORMAT_NODE_TYPE_CHAR) {
      int32_t pos = 0;
      // skip leading spaces
      while (isspace(node->c[pos]) && node->len > 0) pos++;
      while (pos < node->len && *s != '\0') {
        if (!isspace(node->c[pos++])) {
          while (isspace(*s) && *s != '\0') s++;
          if (*s != '\0') s++;  // forward together
        }
      }
      continue;
    }
    assert(node->type == TS_FORMAT_NODE_TYPE_KEYWORD);
    switch (node->key->id) {
      case TSFKW_A_M:
      case TSFKW_P_M:
      case TSFKW_a_m:
      case TSFKW_p_m: {
        int32_t idx = strArrayCaseSearch(long_apms, s);
        if (idx >= 0) {
          s += 4;
          pm = idx % 2;
          hour12 = 1;
        } else {
          err = -1;
        }
      } break;
      case TSFKW_AM:
      case TSFKW_PM:
      case TSFKW_am:
      case TSFKW_pm: {
        int32_t idx = strArrayCaseSearch(apms, s);
        if (idx >= 0) {
          s += 2;
          pm = idx % 2;
          hour12 = 1;
        } else {
          err = -1;
        }
      } break;
      case TSFKW_HH:
      case TSFKW_HH12: {
        const char* newPos = tsFormatStr2Int32(&hour, s, 2, needMoreDigits(formats, i));
        if (NULL == newPos || hour > 12 || hour <= 0) {
          err = -1;
        } else {
          hour12 = 1;
          s = newPos;
        }
      } break;
      case TSFKW_HH24: {
        const char* newPos = tsFormatStr2Int32(&hour, s, 2, needMoreDigits(formats, i));
        if (NULL == newPos) {
          err = -1;
        } else {
          hour12 = 0;
          s = newPos;
        }
      } break;
      case TSFKW_MI: {
        const char* newPos = tsFormatStr2Int32(&min, s, 2, needMoreDigits(formats, i));
        if (NULL == newPos) {
          err = -1;
        } else {
          s = newPos;
        }
      } break;
      case TSFKW_SS: {
        const char* newPos = tsFormatStr2Int32(&sec, s, 2, needMoreDigits(formats, i));
        if (NULL == newPos)
          err = -1;
        else
          s = newPos;
      } break;
      case TSFKW_MS: {
        const char* newPos = tsFormatStr2Int32(&ms, s, 3, needMoreDigits(formats, i));
        if (NULL == newPos)
          err = -1;
        else {
          int32_t len = newPos - s;
          ms *= len == 1 ? 100 : len == 2 ? 10 : 1;
          s = newPos;
        }
      } break;
      case TSFKW_US: {
        const char* newPos = tsFormatStr2Int32(&us, s, 6, needMoreDigits(formats, i));
        if (NULL == newPos)
          err = -1;
        else {
          int32_t len = newPos - s;
          us *= len == 1 ? 100000 : len == 2 ? 10000 : len == 3 ? 1000 : len == 4 ? 100 : len == 5 ? 10 : 1;
          s = newPos;
        }
      } break;
      case TSFKW_NS: {
        const char* newPos = tsFormatStr2Int32(&ns, s, 9, needMoreDigits(formats, i));
        if (NULL == newPos)
          err = -1;
        else {
          int32_t len = newPos - s;
          ns *= len == 1   ? 100000000
                : len == 2 ? 10000000
                : len == 3 ? 1000000
                : len == 4 ? 100000
                : len == 5 ? 10000
                : len == 6 ? 1000
                : len == 7 ? 100
                : len == 8 ? 10
                           : 1;
          s = newPos;
        }
      } break;
      case TSFKW_TZH: {
        tzSign = *s == '-' ? -1 : 1;
        const char* newPos = tsFormatStr2Int32(&tz, s, -1, needMoreDigits(formats, i));
        if (NULL == newPos)
          err = -1;
        else {
          s = newPos;
        }
      } break;
      case TSFKW_MONTH:
      case TSFKW_Month:
      case TSFKW_month: {
        int32_t idx = strArrayCaseSearch(fullMonths, s);
        if (idx >= 0) {
          s += strlen(fullMonths[idx]);
          mon = idx;
        } else {
          err = -1;
        }
      } break;
      case TSFKW_MON:
      case TSFKW_Mon:
      case TSFKW_mon: {
        int32_t idx = strArrayCaseSearch(months, s);
        if (idx >= 0) {
          s += strlen(months[idx]);
          mon = idx;
        } else {
          err = -1;
        }
      } break;
      case TSFKW_MM: {
        const char* newPos = tsFormatStr2Int32(&mon, s, 2, needMoreDigits(formats, i));
        if (NULL == newPos) {
          err = -1;
        } else {
          s = newPos;
          mon -= 1;
        }
      } break;
      case TSFKW_DAY:
      case TSFKW_Day:
      case TSFKW_day: {
        int32_t idx = strArrayCaseSearch(weekDays, s);
        if (idx >= 0) {
          s += strlen(weekDays[idx]);
          wd = idx;
        } else {
          err = -1;
        }
      } break;
      case TSFKW_DY:
      case TSFKW_Dy:
      case TSFKW_dy: {
        int32_t idx = strArrayCaseSearch(shortWeekDays, s);
        if (idx >= 0) {
          s += strlen(shortWeekDays[idx]);
          wd = idx;
        } else {
          err = -1;
        }
      } break;
      case TSFKW_DDD: {
        const char* newPos = tsFormatStr2Int32(&yd, s, 3, needMoreDigits(formats, i));
        if (NULL == newPos) {
          err = -1;
        } else {
          s = newPos;
        }
        withYD = true;
      } break;
      case TSFKW_DD: {
        const char* newPos = tsFormatStr2Int32(&md, s, 2, needMoreDigits(formats, i));
        if (NULL == newPos) {
          err = -1;
        } else {
          s = newPos;
        }
        withMD = true;
      } break;
      case TSFKW_D: {
        const char* newPos = tsFormatStr2Int32(&wd, s, 1, needMoreDigits(formats, i));
        if (NULL == newPos) {
          err = -1;
        } else {
          s = newPos;
        }
      } break;
      case TSFKW_YYYY: {
        const char* newPos = tsFormatStr2Int32(&year, s, 4, needMoreDigits(formats, i));
        if (NULL == newPos) {
          err = -1;
        } else {
          s = newPos;
        }
      } break;
      case TSFKW_YYY: {
        const char* newPos = tsFormatStr2Int32(&year, s, 3, needMoreDigits(formats, i));
        if (NULL == newPos) {
          err = -1;
        } else {
          year = adjustYearTo2020(year);
          s = newPos;
        }
      } break;
      case TSFKW_YY: {
        const char* newPos = tsFormatStr2Int32(&year, s, 2, needMoreDigits(formats, i));
        if (NULL == newPos) {
          err = -1;
        } else {
          year = adjustYearTo2020(year);
          s = newPos;
        }
      } break;
      case TSFKW_Y: {
        const char* newPos = tsFormatStr2Int32(&year, s, 1, needMoreDigits(formats, i));
        if (NULL == newPos) {
          err = -1;
        } else {
          year = adjustYearTo2020(year);
          s = newPos;
        }
      } break;
      default:
        break;
    }
    if (err) {
      if (sErrPos) *sErrPos = s;
      if (fErrIdx) *fErrIdx = i;
      return err;
    }
  }
  if (!withMD) {
    // yyyy-mm-DDD, currently, the c api can't convert to correct timestamp, return not supported
    if (withYD) return -3;
  }
  struct STm tm = {0};
  tm.tm.tm_year = year - 1900;
  tm.tm.tm_mon = mon;
  tm.tm.tm_yday = yd;
  tm.tm.tm_mday = md;
  tm.tm.tm_wday = wd;
  if (hour12) {
    if (pm && hour < 12)
      tm.tm.tm_hour = hour + 12;
    else if (!pm && hour == 12)
      tm.tm.tm_hour = 0;
    else
      tm.tm.tm_hour = hour;
  } else {
    tm.tm.tm_hour = hour;
  }
  tm.tm.tm_min = min;
  tm.tm.tm_sec = sec;
  if (!checkTm(&tm.tm)) return -2;
  if (tz < -12 || tz > 12) return -2;
  tm.fsec = ms * 1000000 + us * 1000 + ns;
  int32_t ret = taosTm2Ts(&tm, ts, precision);
  *ts += 60 * 60 * (tsTimezone - tz) * TICK_PER_SECOND[precision];
  return ret;
}

int32_t taosTs2Char(const char* format, SArray** formats, int64_t ts, int32_t precision, char* out, int32_t outLen) {
  if (!*formats) {
    *formats = taosArrayInit(8, sizeof(TSFormatNode));
    parseTsFormat(format, *formats);
  }
  struct STm tm;
  taosTs2Tm(ts, precision, &tm);
  return tm2char(*formats, &tm, out, outLen);
}

int32_t taosChar2Ts(const char* format, SArray** formats, const char* tsStr, int64_t* ts, int32_t precision, char* errMsg,
                    int32_t errMsgLen) {
  const char* sErrPos;
  int32_t     fErrIdx;
  if (!*formats) {
    *formats = taosArrayInit(4, sizeof(TSFormatNode));
    parseTsFormat(format, *formats);
  }
  int32_t code = char2ts(tsStr, *formats, ts, precision, &sErrPos, &fErrIdx);
  if (code == -1) {
    TSFormatNode* fNode = (taosArrayGet(*formats, fErrIdx));
    snprintf(errMsg, errMsgLen, "mismatch format for: %s and %s", sErrPos,
             fErrIdx < taosArrayGetSize(*formats) ? ((TSFormatNode*)taosArrayGet(*formats, fErrIdx))->key->name : "");
    code = TSDB_CODE_FUNC_TO_TIMESTAMP_FAILED_FORMAT_ERR;
  } else if (code == -2) {
    snprintf(errMsg, errMsgLen, "timestamp format error: %s -> %s", tsStr, format);
    code = TSDB_CODE_FUNC_TO_TIMESTAMP_FAILED_TS_ERR;
  } else if (code == -3) {
    snprintf(errMsg, errMsgLen, "timestamp format not supported");
    code = TSDB_CODE_FUNC_TO_TIMESTAMP_FAILED_NOT_SUPPORTED;
  }
  return code;
}

void TEST_ts2char(const char* format, int64_t ts, int32_t precision, char* out, int32_t outLen) {
  SArray* formats = taosArrayInit(4, sizeof(TSFormatNode));
  parseTsFormat(format, formats);
  struct STm tm;
  taosTs2Tm(ts, precision, &tm);
  tm2char(formats, &tm, out, outLen);
  taosArrayDestroy(formats);
}

int32_t TEST_char2ts(const char* format, int64_t* ts, int32_t precision, const char* tsStr) {
  const char* sErrPos;
  int32_t     fErrIdx;
  SArray*     formats = taosArrayInit(4, sizeof(TSFormatNode));
  parseTsFormat(format, formats);
  int32_t code = char2ts(tsStr, formats, ts, precision, &sErrPos, &fErrIdx);
  if (code == -1) {
    printf("failed position: %s\n", sErrPos);
    printf("failed format: %s\n", ((TSFormatNode*)taosArrayGet(formats, fErrIdx))->key->name);
  }
  taosArrayDestroy(formats);
  return code;
}
