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

#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#include "tsdb.h"
#include "ttime.h"
#include "tutil.h"

static int64_t parseFraction(char* str, char** end, int32_t timePrec);
static int32_t parseTimeWithTz(char* timestr, int64_t* time, int32_t timePrec);
static int32_t parseLocaltime(char* timestr, int64_t* time, int32_t timePrec);

int32_t taosGetTimestampSec() { return (int32_t)time(NULL); }

int64_t taosGetTimestampMs() {
  struct timeval systemTime;
  gettimeofday(&systemTime, NULL);
  return (int64_t)systemTime.tv_sec * 1000L + (uint64_t)systemTime.tv_usec / 1000;
}

int64_t taosGetTimestampUs() {
  struct timeval systemTime;
  gettimeofday(&systemTime, NULL);
  return (int64_t)systemTime.tv_sec * 1000000L + (uint64_t)systemTime.tv_usec;
}

/*
 * If tsTimePrecision == 1, taosGetTimestamp will return timestamp in microsecond.
 * Otherwise, it will return timestamp in millisecond.
 */
int64_t taosGetTimestamp(int32_t precision) {
  if (precision == TSDB_TIME_PRECISION_MICRO) {
    return taosGetTimestampUs();
  } else {
    return taosGetTimestampMs();
  }
}

int32_t taosParseTime(char* timestr, int64_t* time, int32_t len, int32_t timePrec) {
  /* parse datatime string in with tz */
  if (strnchr(timestr, 'T', len, false) != NULL) {
    return parseTimeWithTz(timestr, time, timePrec);
  } else {
    return parseLocaltime(timestr, time, timePrec);
  }
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

  int32_t factor[6] = {1, 10, 100, 1000, 10000, 100000};
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
  } else {
    assert(timePrec == TSDB_TIME_PRECISION_MICRO);
    if (i >= MICRO_SEC_FRACTION_LEN) {
      i = MICRO_SEC_FRACTION_LEN;
    }
    times = MICRO_SEC_FRACTION_LEN - i;
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

  char* sep = strchr(&str[i], ':');
  if (sep != NULL) {
    int32_t len = sep - &str[i];

    hour = strnatoi(&str[i], len);
    i += len + 1;
  } else {
    hour = strnatoi(&str[i], 2);
    i += 2;
  }

  if (hour > 12) {
    return -1;
  }

  int64_t sec = strnatoi(&str[i], 2);
  if (sec > 70) {
    return -1;
  }

  sec += (hour * 3600);

  if (str[0] == '+') {
    *tzOffset = -sec;
  } else {
    *tzOffset = sec;
  }

  return 0;
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
int32_t parseTimeWithTz(char* timestr, int64_t* time, int32_t timePrec) {
  int64_t factor = (timePrec == TSDB_TIME_PRECISION_MILLI) ? 1000 : 1000000;
  int64_t tzOffset = 0;

  struct tm tm = {0};
  char*     str = strptime(timestr, "%Y-%m-%dT%H:%M:%S", &tm);
  if (str == NULL) {
    return -1;
  }

/* mktime will be affected by TZ, set by using taos_options */
#ifdef WINDOWS
  int64_t seconds = gmtime(&tm); 
#else
  int64_t seconds = timegm(&tm);
#endif

  int64_t fraction = 0;
  str = forwardToTimeStringEnd(timestr);

  if (str[0] == 'Z' || str[0] == 'z') {
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

int32_t parseLocaltime(char* timestr, int64_t* time, int32_t timePrec) {
  *time = 0;
  struct tm tm = {0};

  char* str = strptime(timestr, "%Y-%m-%d %H:%M:%S", &tm);
  if (str == NULL) {
    return -1;
  }

  /* mktime will be affected by TZ, set by using taos_options */
  int64_t seconds = mktime(&tm);
  int64_t fraction = 0;

  if (*str == '.') {
    /* parse the second fraction part */
    if ((fraction = parseFraction(str + 1, &str, timePrec)) < 0) {
      return -1;
    }
  }

  int64_t factor = (timePrec == TSDB_TIME_PRECISION_MILLI) ? 1000 : 1000000;
  *time = factor * seconds + fraction;

  return 0;
}

static int32_t getTimestampInUsFromStrImpl(int64_t val, char unit, int64_t* result) {
  *result = val;

  switch (unit) {
    case 's':
      (*result) *= MILLISECOND_PER_SECOND;
      break;
    case 'm':
      (*result) *= MILLISECOND_PER_MINUTE;
      break;
    case 'h':
      (*result) *= MILLISECOND_PER_HOUR;
      break;
    case 'd':
      (*result) *= MILLISECOND_PER_DAY;
      break;
    case 'w':
      (*result) *= MILLISECOND_PER_WEEK;
      break;
    case 'n':
      (*result) *= MILLISECOND_PER_MONTH;
      break;
    case 'y':
      (*result) *= MILLISECOND_PER_YEAR;
      break;
    case 'a':
      break;
    default: {
      ;
      return -1;
    }
  }

  /* get the value in microsecond */
  (*result) *= 1000L;
  return 0;
}

/*
 * a - Millionseconds
 * s - Seconds
 * m - Minutes
 * h - Hours
 * d - Days (24 hours)
 * w - Weeks (7 days)
 * n - Months (30 days)
 * y - Years (365 days)
 */
int32_t getTimestampInUsFromStr(char* token, int32_t tokenlen, int64_t* ts) {
  errno = 0;
  char* endPtr = NULL;

  /* get the basic numeric value */
  int64_t timestamp = strtoll(token, &endPtr, 10);
  if (errno != 0) {
    return -1;
  }

  return getTimestampInUsFromStrImpl(timestamp, token[tokenlen - 1], ts);
}
