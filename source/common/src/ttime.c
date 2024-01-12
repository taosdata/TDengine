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

  // return error if there're illegal charaters after min(2 Digits)
  char* minStr = &str[i];
  if (minStr[1] != '\0' && minStr[2] != '\0') {
    return -1;
  }

  int64_t minute = strnatoi(&str[i], 2);
  if (minute > 59) {
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
  int64_t numOfMonth = (unit == 'y')? duration*12:duration;
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
          while(newe < ts && news < ts) {
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
