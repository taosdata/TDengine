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

#define _BSD_SOURCE
#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE

#include "os.h"
#include "taosdef.h"
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
		const unsigned int min, const unsigned int sec, int64_t timezone)
{
  unsigned int mon = mon0, year = year0;

  /* 1..12 -> 11,12,1..10 */
  if (0 >= (int) (mon -= 2)) {
    mon += 12;	/* Puts Feb last since it has leap day */
    year -= 1;
  }

  //int64_t res = (((((int64_t) (year/4 - year/100 + year/400 + 367*mon/12 + day) +
  //               year*365 - 719499)*24 + hour)*60 + min)*60 + sec);
  int64_t res;
  res  = 367*((int64_t)mon)/12;
  res += year/4 - year/100 + year/400 + day + ((int64_t)year)*365 - 719499;
  res  = res*24;
  res  = ((res + hour) * 60 + min) * 60 + sec;

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

static int64_t parseFraction(char* str, char** end, int32_t timePrec);
static int32_t parseTimeWithTz(char* timestr, int64_t* time, int32_t timePrec);
static int32_t parseLocaltime(char* timestr, int64_t* time, int32_t timePrec);
static int32_t parseLocaltimeWithDst(char* timestr, int64_t* time, int32_t timePrec);

static int32_t (*parseLocaltimeFp[]) (char* timestr, int64_t* time, int32_t timePrec) = {
  parseLocaltime,
  parseLocaltimeWithDst
}; 

int32_t taosGetTimestampSec() { return (int32_t)time(NULL); }

int32_t taosParseTime(char* timestr, int64_t* time, int32_t len, int32_t timePrec, int8_t daylight) {
  /* parse datatime string in with tz */
  if (strnchr(timestr, 'T', len, false) != NULL) {
    return parseTimeWithTz(timestr, time, timePrec);
  } else {
    return (*parseLocaltimeFp[daylight])(timestr, time, timePrec);
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
    int32_t len = (int32_t)(sep - &str[i]);

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
  int64_t seconds = user_mktime64(tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, 0);
  //int64_t seconds = gmtime(&tm); 
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

#ifdef _MSC_VER
#if _MSC_VER >= 1900
  int64_t timezone = _timezone;
#endif
#endif

  int64_t seconds = user_mktime64(tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, timezone);
  
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

int32_t parseLocaltimeWithDst(char* timestr, int64_t* time, int32_t timePrec) {
  *time = 0;
  struct tm tm = {0};
  tm.tm_isdst = -1;

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


static int32_t getDurationInUs(int64_t val, char unit, int64_t* result) {
  *result = val;

  int64_t factor = 1000L;

  switch (unit) {
    case 's':
      (*result) *= MILLISECOND_PER_SECOND*factor;
      break;
    case 'm':
      (*result) *= MILLISECOND_PER_MINUTE*factor;
      break;
    case 'h':
      (*result) *= MILLISECOND_PER_HOUR*factor;
      break;
    case 'd':
      (*result) *= MILLISECOND_PER_DAY*factor;
      break;
    case 'w':
      (*result) *= MILLISECOND_PER_WEEK*factor;
      break;
    case 'a':
      (*result) *= factor;
      break;
    case 'u':
      break;
    default: {
      return -1;
    }
  }

  /* get the value in microsecond */
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
int32_t parseAbsoluteDuration(char* token, int32_t tokenlen, int64_t* duration) {
  errno = 0;
  char* endPtr = NULL;

  /* get the basic numeric value */
  int64_t timestamp = strtoll(token, &endPtr, 10);
  if (errno != 0) {
    return -1;
  }

  /* natual month/year are not allowed in absolute duration */
  char unit = token[tokenlen - 1];
  if (unit == 'n' || unit == 'y') {
    return -1;
  }

  return getDurationInUs(timestamp, unit, duration);
}

int32_t parseNatualDuration(const char* token, int32_t tokenLen, int64_t* duration, char* unit) {
  errno = 0;

  /* get the basic numeric value */
  *duration = strtoll(token, NULL, 10);
  if (errno != 0) {
    return -1;
  }

  *unit = token[tokenLen - 1];
  if (*unit == 'n' || *unit == 'y') {
    return 0;
  }

  return getDurationInUs(*duration, *unit, duration);
}

int64_t taosTimeAdd(int64_t t, int64_t duration, char unit, int32_t precision) {
  if (duration == 0) {
    return t;
  }
  if (unit == 'y') {
    duration *= 12;
  } else if (unit != 'n') {
    return t + duration;
  }

  struct tm tm;
  time_t tt = (time_t)(t / TSDB_TICK_PER_SECOND(precision));
  localtime_r(&tt, &tm);
  int mon = tm.tm_year * 12 + tm.tm_mon + (int)duration;
  tm.tm_year = mon / 12;
  tm.tm_mon = mon % 12;

  return (int64_t)(mktime(&tm) * TSDB_TICK_PER_SECOND(precision));
}

int32_t taosTimeCountInterval(int64_t skey, int64_t ekey, int64_t interval, char unit, int32_t precision) {
  if (ekey < skey) {
    int64_t tmp = ekey;
    ekey = skey;
    skey = tmp;
  }
  if (unit != 'n' && unit != 'y') {
    return (int32_t)((ekey - skey) / interval);
  }

  skey /= (int64_t)(TSDB_TICK_PER_SECOND(precision));
  ekey /= (int64_t)(TSDB_TICK_PER_SECOND(precision));

  struct tm tm;
  time_t t = (time_t)skey;
  localtime_r(&t, &tm);
  int smon = tm.tm_year * 12 + tm.tm_mon;

  t = (time_t)ekey;
  localtime_r(&t, &tm);
  int emon = tm.tm_year * 12 + tm.tm_mon;

  if (unit == 'y') {
    interval *= 12;
  }

  return (emon - smon) / (int32_t)interval;
}

int64_t taosTimeTruncate(int64_t t, const SInterval* pInterval, int32_t precision) {
  if (pInterval->sliding == 0) {
    assert(pInterval->interval == 0);
    return t;
  }

  int64_t start = t;
  if (pInterval->slidingUnit == 'n' || pInterval->slidingUnit == 'y') {
    start /= (int64_t)(TSDB_TICK_PER_SECOND(precision));
    struct tm tm;
    time_t tt = (time_t)start;
    localtime_r(&tt, &tm);
    tm.tm_sec = 0;
    tm.tm_min = 0;
    tm.tm_hour = 0;
    tm.tm_mday = 1;

    if (pInterval->slidingUnit == 'y') {
      tm.tm_mon = 0;
      tm.tm_year = (int)(tm.tm_year / pInterval->sliding * pInterval->sliding);
    } else {
      int mon = tm.tm_year * 12 + tm.tm_mon;
      mon = (int)(mon / pInterval->sliding * pInterval->sliding);
      tm.tm_year = mon / 12;
      tm.tm_mon = mon % 12;
    }

    start = (int64_t)(mktime(&tm) * TSDB_TICK_PER_SECOND(precision));
  } else {
    int64_t delta = t - pInterval->interval;
    int32_t factor = (delta >= 0) ? 1 : -1;

    start = (delta / pInterval->sliding + factor) * pInterval->sliding;

    if (pInterval->intervalUnit == 'd' || pInterval->intervalUnit == 'w') {
     /*
      * here we revised the start time of day according to the local time zone,
      * but in case of DST, the start time of one day need to be dynamically decided.
      */
      // todo refactor to extract function that is available for Linux/Windows/Mac platform
  #if defined(WINDOWS) && _MSC_VER >= 1900
      // see https://docs.microsoft.com/en-us/cpp/c-runtime-library/daylight-dstbias-timezone-and-tzname?view=vs-2019
      int64_t timezone = _timezone;
      int32_t daylight = _daylight;
      char**  tzname = _tzname;
  #endif

      start += (int64_t)(timezone * TSDB_TICK_PER_SECOND(precision));
    }

    int64_t end = 0;

    // not enough time range
    if (start < 0 || INT64_MAX - start > pInterval->interval - 1) {
      end = start + pInterval->interval - 1;

      while(end < t && ((start + pInterval->sliding) <= INT64_MAX)) { // move forward to the correct time window
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

  if (pInterval->offset > 0) {
    start = taosTimeAdd(start, pInterval->offset, pInterval->offsetUnit, precision);
    if (start > t) {
      start = taosTimeAdd(start, -pInterval->interval, pInterval->intervalUnit, precision);
    }
  }
  return start;
}

// internal function, when program is paused in debugger,
// one can call this function from debugger to print a
// timestamp as human readable string, for example (gdb):
//     p fmtts(1593769722)
// outputs:
//     2020-07-03 17:48:42
// and the parameter can also be a variable.
const char* fmtts(int64_t ts) {
  static char buf[96];
  size_t pos = 0;
  struct tm tm;

  if (ts > -62135625943 && ts < 32503651200) {
    time_t t = (time_t)ts;
    localtime_r(&t, &tm);
    pos += strftime(buf + pos, sizeof(buf), "s=%Y-%m-%d %H:%M:%S", &tm);
  }

  if (ts > -62135625943000 && ts < 32503651200000) {
    time_t t = (time_t)(ts / 1000);
    localtime_r(&t, &tm);
    if (pos > 0) {
      buf[pos++] = ' ';
      buf[pos++] = '|';
      buf[pos++] = ' ';
    }
    pos += strftime(buf + pos, sizeof(buf), "ms=%Y-%m-%d %H:%M:%S", &tm);
    pos += sprintf(buf + pos, ".%03d", (int)(ts % 1000));
  }

  {
    time_t t = (time_t)(ts / 1000000);
    localtime_r(&t, &tm);
    if (pos > 0) {
      buf[pos++] = ' ';
      buf[pos++] = '|';
      buf[pos++] = ' ';
    }
    pos += strftime(buf + pos, sizeof(buf), "us=%Y-%m-%d %H:%M:%S", &tm);
    pos += sprintf(buf + pos, ".%06d", (int)(ts % 1000000));
  }

  return buf;
}
