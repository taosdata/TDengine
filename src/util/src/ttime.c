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

#include "taosdef.h"
#include "ttime.h"
#include "tutil.h"

/*
 * mktime64 - Converts date to seconds.
 * Converts Gregorian date to seconds since 1970-01-01 00:00:00.
 * Assumes input in normal date format, i.e. 1980-12-31 23:59:59
 * => year=1980, mon=12, day=31, hour=23, min=59, sec=59.
 *
 * [For the Julian calendar (which was used in Russia before 1917,
 * Britain & colonies before 1752, anywhere else before 1582,
 * and is still in use by some communities) leave out the
 * -year/100+year/400 terms, and add 10.]
 *
 * This algorithm was first published by Gauss (I think).
 *
 * A leap second can be indicated by calling this function with sec as
 * 60 (allowable under ISO 8601).  The leap second is treated the same
 * as the following second since they don't exist in UNIX time.
 *
 * An encoding of midnight at the end of the day as 24:00:00 - ie. midnight
 * tomorrow - (allowable under ISO 8601) is supported.
 */
int64_t user_mktime64(const unsigned int year0, const unsigned int mon0,
		const unsigned int day, const unsigned int hour,
		const unsigned int min, const unsigned int sec)
{
	unsigned int mon = mon0, year = year0;

	/* 1..12 -> 11,12,1..10 */
	if (0 >= (int) (mon -= 2)) {
		mon += 12;	/* Puts Feb last since it has leap day */
		year -= 1;
	}

  int64_t res = (((((int64_t) (year/4 - year/100 + year/400 + 367*mon/12 + day) +
		  year*365 - 719499)*24 + hour)*60 + min)*60 + sec);

	return (res + timezone);
}
// ==== mktime() kernel code =================//
static int64_t m_deltaUtc = 0;
void deltaToUtcInitOnce() {  
  struct tm tm = {0};
  
  (void)strptime("1970-01-01 00:00:00", (const char *)("%Y-%m-%d %H:%M:%S"), &tm);
  m_deltaUtc = (int64_t)mktime(&tm);
  //printf("====delta:%lld\n\n", seconds);	
  return;
}

int64_t user_mktime(struct tm * tm)
{
#define TAOS_MINUTE   60
#define TAOS_HOUR     (60*TAOS_MINUTE)
#define TAOS_DAY      (24*TAOS_HOUR)
#define TAOS_YEAR     (365*TAOS_DAY)
  
static int month[12] = {
  0,
  TAOS_DAY*(31),
  TAOS_DAY*(31+29),
  TAOS_DAY*(31+29+31),
  TAOS_DAY*(31+29+31+30),
  TAOS_DAY*(31+29+31+30+31),
  TAOS_DAY*(31+29+31+30+31+30),
  TAOS_DAY*(31+29+31+30+31+30+31),
  TAOS_DAY*(31+29+31+30+31+30+31+31),
  TAOS_DAY*(31+29+31+30+31+30+31+31+30),
  TAOS_DAY*(31+29+31+30+31+30+31+31+30+31),
  TAOS_DAY*(31+29+31+30+31+30+31+31+30+31+30)
};

  int64_t res;
  int     year;
  
  year= tm->tm_year - 70; 
  res= TAOS_YEAR*year + TAOS_DAY*((year+1)/4);
  res+= month[tm->tm_mon];
  
  if(tm->tm_mon > 1 && ((year+2)%4)) {  
    res-= TAOS_DAY;
  }
  
  res+= TAOS_DAY*(tm->tm_mday-1);  
  res+= TAOS_HOUR*tm->tm_hour;  
  res+= TAOS_MINUTE*tm->tm_min;  
  res+= tm->tm_sec;
  
  return res + m_deltaUtc;

}


static int64_t parseFraction(char* str, char** end, int32_t timePrec);
static int32_t parseTimeWithTz(char* timestr, int64_t* time, int32_t timePrec);
static int32_t parseLocaltime(char* timestr, int64_t* time, int32_t timePrec);

int32_t taosGetTimestampSec() { return (int32_t)time(NULL); }

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
  //int64_t seconds = mktime(&tm);
  //int64_t seconds = (int64_t)user_mktime(&tm);
  int64_t seconds = user_mktime64(tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
  
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
