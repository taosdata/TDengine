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

#define ALLOW_FORBID_FUNC
#define _BSD_SOURCE

#ifdef DARWIN
#define _XOPEN_SOURCE
#else
#define _XOPEN_SOURCE 500
#endif

#define _DEFAULT_SOURCE

#include "os.h"

#ifdef WINDOWS

#include <stdlib.h>
#include <string.h>
#include <time.h>
//#define TM_YEAR_BASE 1970 //origin
#define TM_YEAR_BASE 1900  // slguan
/*
 * We do not implement alternate representations. However, we always
 * check whether a given modifier is allowed for a certain conversion.
 */
#define ALT_E 0x01
#define ALT_O 0x02
#define LEGAL_ALT(x)                   \
  {                                    \
    if (alt_format & ~(x)) return (0); \
  }

static int conv_num(const char **buf, int *dest, int llim, int ulim) {
  int result = 0;

  /* The limit also determines the number of valid digits. */
  int rulim = ulim;

  if (**buf < '0' || **buf > '9') return (0);

  do {
    result *= 10;
    result += *(*buf)++ - '0';
    rulim /= 10;
  } while ((result * 10 <= ulim) && rulim && **buf >= '0' && **buf <= '9');

  if (result < llim || result > ulim) return (0);

  *dest = result;
  return (1);
}

static const char *day[7] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
static const char *abday[7] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
static const char *mon[12] = {"January", "February", "March",     "April",   "May",      "June",
                              "July",    "August",   "September", "October", "November", "December"};
static const char *abmon[12] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
static const char *am_pm[2] = {"AM", "PM"};

#else
#include <sys/time.h>
#endif

char *taosStrpTime(const char *buf, const char *fmt, struct tm *tm) {
#ifdef WINDOWS
  char        c;
  const char *bp;
  size_t      len = 0;
  int         alt_format, i, split_year = 0;

  bp = buf;

  while ((c = *fmt) != '\0') {
    /* Clear `alternate' modifier prior to new conversion. */
    alt_format = 0;

    /* Eat up white-space. */
    if (isspace(c)) {
      while (isspace(*bp)) bp++;

      fmt++;
      continue;
    }

    if ((c = *fmt++) != '%') goto literal;

  again:
    switch (c = *fmt++) {
      case '%': /* "%%" is converted to "%". */
      literal:
        if (c != *bp++) return (0);
        break;

        /*
         * "Alternative" modifiers. Just set the appropriate flag
         * and start over again.
         */
      case 'E': /* "%E?" alternative conversion modifier. */
        LEGAL_ALT(0);
        alt_format |= ALT_E;
        goto again;

      case 'O': /* "%O?" alternative conversion modifier. */
        LEGAL_ALT(0);
        alt_format |= ALT_O;
        goto again;

        /*
         * "Complex" conversion rules, implemented through recursion.
         */
      case 'c': /* Date and time, using the locale's format. */
        LEGAL_ALT(ALT_E);
        if (!(bp = taosStrpTime(bp, "%x %X", tm))) return (0);
        break;

      case 'D': /* The date as "%m/%d/%y". */
        LEGAL_ALT(0);
        if (!(bp = taosStrpTime(bp, "%m/%d/%y", tm))) return (0);
        break;

      case 'R': /* The time as "%H:%M". */
        LEGAL_ALT(0);
        if (!(bp = taosStrpTime(bp, "%H:%M", tm))) return (0);
        break;

      case 'r': /* The time in 12-hour clock representation. */
        LEGAL_ALT(0);
        if (!(bp = taosStrpTime(bp, "%I:%M:%S %p", tm))) return (0);
        break;

      case 'T': /* The time as "%H:%M:%S". */
        LEGAL_ALT(0);
        if (!(bp = taosStrpTime(bp, "%H:%M:%S", tm))) return (0);
        break;

      case 'X': /* The time, using the locale's format. */
        LEGAL_ALT(ALT_E);
        if (!(bp = taosStrpTime(bp, "%H:%M:%S", tm))) return (0);
        break;

      case 'x': /* The date, using the locale's format. */
        LEGAL_ALT(ALT_E);
        if (!(bp = taosStrpTime(bp, "%m/%d/%y", tm))) return (0);
        break;

        /*
         * "Elementary" conversion rules.
         */
      case 'A': /* The day of week, using the locale's form. */
      case 'a':
        LEGAL_ALT(0);
        for (i = 0; i < 7; i++) {
          /* Full name. */
          len = strlen(day[i]);
          if (strncmp(day[i], bp, len) == 0) break;

          /* Abbreviated name. */
          len = strlen(abday[i]);
          if (strncmp(abday[i], bp, len) == 0) break;
        }

        /* Nothing matched. */
        if (i == 7) return (0);

        tm->tm_wday = i;
        bp += len;
        break;

      case 'B': /* The month, using the locale's form. */
      case 'b':
      case 'h':
        LEGAL_ALT(0);
        for (i = 0; i < 12; i++) {
          /* Full name. */
          len = strlen(mon[i]);
          if (strncmp(mon[i], bp, len) == 0) break;

          /* Abbreviated name. */
          len = strlen(abmon[i]);
          if (strncmp(abmon[i], bp, len) == 0) break;
        }

        /* Nothing matched. */
        if (i == 12) return (0);

        tm->tm_mon = i;
        bp += len;
        break;

      case 'C': /* The century number. */
        LEGAL_ALT(ALT_E);
        if (!(conv_num(&bp, &i, 0, 99))) return (0);

        if (split_year) {
          tm->tm_year = (tm->tm_year % 100) + (i * 100);
        } else {
          tm->tm_year = i * 100;
          split_year = 1;
        }
        break;

      case 'd': /* The day of month. */
      case 'e':
        LEGAL_ALT(ALT_O);
        if (!(conv_num(&bp, &tm->tm_mday, 1, 31))) return (0);
        break;

      case 'k': /* The hour (24-hour clock representation). */
        LEGAL_ALT(0);
        /* FALLTHROUGH */
      case 'H':
        LEGAL_ALT(ALT_O);
        if (!(conv_num(&bp, &tm->tm_hour, 0, 23))) return (0);
        break;

      case 'l': /* The hour (12-hour clock representation). */
        LEGAL_ALT(0);
        /* FALLTHROUGH */
      case 'I':
        LEGAL_ALT(ALT_O);
        if (!(conv_num(&bp, &tm->tm_hour, 1, 12))) return (0);
        if (tm->tm_hour == 12) tm->tm_hour = 0;
        break;

      case 'j': /* The day of year. */
        LEGAL_ALT(0);
        if (!(conv_num(&bp, &i, 1, 366))) return (0);
        tm->tm_yday = i - 1;
        break;

      case 'M': /* The minute. */
        LEGAL_ALT(ALT_O);
        if (!(conv_num(&bp, &tm->tm_min, 0, 59))) return (0);
        break;

      case 'm': /* The month. */
        LEGAL_ALT(ALT_O);
        if (!(conv_num(&bp, &i, 1, 12))) return (0);
        tm->tm_mon = i - 1;
        break;

      case 'p': /* The locale's equivalent of AM/PM. */
        LEGAL_ALT(0);
        /* AM? */
        if (strcmp(am_pm[0], bp) == 0) {
          if (tm->tm_hour > 11) return (0);

          bp += strlen(am_pm[0]);
          break;
        }
        /* PM? */
        else if (strcmp(am_pm[1], bp) == 0) {
          if (tm->tm_hour > 11) return (0);

          tm->tm_hour += 12;
          bp += strlen(am_pm[1]);
          break;
        }

        /* Nothing matched. */
        return (0);

      case 'S': /* The seconds. */
        LEGAL_ALT(ALT_O);
        if (!(conv_num(&bp, &tm->tm_sec, 0, 61))) return (0);
        break;

      case 'U': /* The week of year, beginning on sunday. */
      case 'W': /* The week of year, beginning on monday. */
        LEGAL_ALT(ALT_O);
        /*
         * XXX This is bogus, as we can not assume any valid
         * information present in the tm structure at this
         * point to calculate a real value, so just check the
         * range for now.
         */
        if (!(conv_num(&bp, &i, 0, 53))) return (0);
        break;

      case 'w': /* The day of week, beginning on sunday. */
        LEGAL_ALT(ALT_O);
        if (!(conv_num(&bp, &tm->tm_wday, 0, 6))) return (0);
        break;

      case 'Y': /* The year. */
        LEGAL_ALT(ALT_E);
        if (!(conv_num(&bp, &i, 0, 9999))) return (0);

        tm->tm_year = i - TM_YEAR_BASE;
        break;

      case 'y': /* The year within 100 years of the epoch. */
        LEGAL_ALT(ALT_E | ALT_O);
        if (!(conv_num(&bp, &i, 0, 99))) return (0);

        if (split_year) {
          tm->tm_year = ((tm->tm_year / 100) * 100) + i;
          break;
        }
        split_year = 1;
        if (i <= 68)
          tm->tm_year = i + 2000 - TM_YEAR_BASE;
        else
          tm->tm_year = i + 1900 - TM_YEAR_BASE;
        break;

        /*
         * Miscellaneous conversions.
         */
      case 'n': /* Any kind of white-space. */
      case 't':
        LEGAL_ALT(0);
        while (isspace(*bp)) bp++;
        break;

      default: /* Unknown/unsupported conversion. */
        return (0);
    }
  }

  /* LINTED functional specification */
  return ((char *)bp);
#else
  return strptime(buf, fmt, tm);
#endif
}

FORCE_INLINE int32_t taosGetTimeOfDay(struct timeval *tv) {
#ifdef WINDOWS
  time_t t;
  t = taosGetTimestampSec();
  SYSTEMTIME st;
  GetLocalTime(&st);

  tv->tv_sec = (long)t;
  tv->tv_usec = st.wMilliseconds * 1000;

  return 0;
#else
  return gettimeofday(tv, NULL);
#endif
}

time_t taosTime(time_t *t) { return time(t); }

time_t taosMktime(struct tm *timep) {
#ifdef WINDOWS
  struct tm tm1 = {0};
  LARGE_INTEGER        t;
  FILETIME             f;
  SYSTEMTIME           s;
  FILETIME             ff;
  SYSTEMTIME           ss;
  LARGE_INTEGER        offset;

  time_t    tt = 0;
  localtime_s(&tm1, &tt);
  ss.wYear = tm1.tm_year + 1900;
  ss.wMonth = tm1.tm_mon + 1;
  ss.wDay = tm1.tm_wday;
  ss.wHour = tm1.tm_hour;
  ss.wMinute = tm1.tm_min;
  ss.wSecond = tm1.tm_sec;
  ss.wMilliseconds = 0;
  SystemTimeToFileTime(&ss, &ff);
  offset.QuadPart = ff.dwHighDateTime;
  offset.QuadPart <<= 32;
  offset.QuadPart |= ff.dwLowDateTime;

  s.wYear = timep->tm_year + 1900;
  s.wMonth = timep->tm_mon + 1;
  s.wDay = timep->tm_wday;
  s.wHour = timep->tm_hour;
  s.wMinute = timep->tm_min;
  s.wSecond = timep->tm_sec;
  s.wMilliseconds = 0;
  SystemTimeToFileTime(&s, &f);
  t.QuadPart = f.dwHighDateTime;
  t.QuadPart <<= 32;
  t.QuadPart |= f.dwLowDateTime;

  t.QuadPart -= offset.QuadPart;
 return (time_t)(t.QuadPart / 10000000);
#else
  return mktime(timep);
#endif
 }

struct tm *taosLocalTime(const time_t *timep, struct tm *result) {
  if (result == NULL) {
    return localtime(timep);
  }
#ifdef WINDOWS
  if (*timep < 0) {
    SYSTEMTIME    ss,s;
    FILETIME      ff,f;
    LARGE_INTEGER offset;
    struct tm     tm1;
    time_t        tt = 0;
    localtime_s(&tm1, &tt);
    ss.wYear = tm1.tm_year + 1900;
    ss.wMonth = tm1.tm_mon + 1;
    ss.wDay = tm1.tm_mday;
    ss.wHour = tm1.tm_hour;
    ss.wMinute = tm1.tm_min;
    ss.wSecond = tm1.tm_sec;
    ss.wMilliseconds = 0;
    SystemTimeToFileTime(&ss, &ff);
    offset.QuadPart = ff.dwHighDateTime;
    offset.QuadPart <<= 32;
    offset.QuadPart |= ff.dwLowDateTime;
    offset.QuadPart += *timep * 10000000;
    f.dwLowDateTime = offset.QuadPart & 0xffffffff;
    f.dwHighDateTime = (offset.QuadPart >> 32) & 0xffffffff;
    FileTimeToSystemTime(&f, &s);
    result->tm_sec = s.wSecond;
    result->tm_min = s.wMinute;
    result->tm_hour = s.wHour;
    result->tm_mday = s.wDay;
    result->tm_mon = s.wMonth-1;
    result->tm_year = s.wYear-1900;
    result->tm_wday = s.wDayOfWeek;
    result->tm_yday = 0;
    result->tm_isdst = 0;
  } else {
    localtime_s(result, timep);
  }
#else
  localtime_r(timep, result);
#endif
  return result;
}

int32_t taosGetTimestampSec() { return (int32_t)time(NULL); }
int32_t taosClockGetTime(int clock_id, struct timespec *pTS) {
#ifdef WINDOWS
  LARGE_INTEGER        t;
  FILETIME             f;
  static FILETIME      ff;
  static SYSTEMTIME    ss;
  static LARGE_INTEGER offset;

  ss.wYear = 1970;
  ss.wMonth = 1;
  ss.wDay = 1;
  ss.wHour = 0;
  ss.wMinute = 0;
  ss.wSecond = 0;
  ss.wMilliseconds = 0;
  SystemTimeToFileTime(&ss, &ff);
  offset.QuadPart = ff.dwHighDateTime;
  offset.QuadPart <<= 32;
  offset.QuadPart |= ff.dwLowDateTime;

  GetSystemTimeAsFileTime(&f);
  t.QuadPart = f.dwHighDateTime;
  t.QuadPart <<= 32;
  t.QuadPart |= f.dwLowDateTime;

  t.QuadPart -= offset.QuadPart;
  pTS->tv_sec = t.QuadPart / 10000000;
  pTS->tv_nsec = (t.QuadPart % 10000000)*100;
  return (0);
#else
  return clock_gettime(clock_id, pTS);
#endif
}