/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#ifdef WINDOWS
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0602  // GetSystemTimePreciseAsFileTime requires Windows 8.
#endif
#endif

#include "os.h"
#include "osInt.h"
#include "taoserror.h"

#ifdef WINDOWS
// Declare timezone variable for Windows (not available in Windows CRT)
long timezone = 0;
#endif

#if defined(WINDOWS) || defined(TD_ASTRA)

#include <stdlib.h>
#include <string.h>
#include <time.h>
// #define TM_YEAR_BASE 1970 //origin
#define TM_YEAR_BASE 1900  // slguan

// This magic number is the number of 100 nanosecond intervals since January 1, 1601 (UTC)
// until 00:00:00 January 1, 1970
static const uint64_t TIMEEPOCH = ((uint64_t)116444736000000000ULL);

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
  if (!buf || !fmt || !tm) return NULL;
#if defined(WINDOWS) || defined(TD_ASTRA) 
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

#ifdef WINDOWS
static bool formatNeedsTimezone(const char *format) {
  for (const char *p = format; *p != '\0'; ++p) {
    if (*p != '%') continue;
    ++p;
    if (*p == 'E' || *p == 'O') ++p;
    if (*p == '\0') break;
    if (*p == 'z' || *p == 'Z' || *p == 's') return true;
  }
  return false;
}
#endif

#ifdef DARWIN
static size_t formatDarwinTime(char *s, size_t size, const char *format,
                               const struct tm *value) {
  // Darwin's UNIX03 strftime reads process-global offsets for %z, ignoring
  // tm_gmtoff. Expand it before libc formats the remaining date fields.
  if (strchr(format, 'z') == NULL) return strftime(s, size, format, value);

  char offsets[3][32] = {{0}};
  if (value->tm_isdst >= 0) {
    long minutes = value->tm_gmtoff / 60;
    char sign = value->tm_gmtoff < 0 ? '-' : '+';
    if (minutes < 0) minutes = -minutes;
    long hhmm = (minutes / 60) * 100 + minutes % 60;
    (void)snprintf(offsets[0], sizeof(offsets[0]), "%c%04ld", sign, hhmm);
    (void)snprintf(offsets[1], sizeof(offsets[1]), "%c%ld", sign, hhmm);
    (void)snprintf(offsets[2], sizeof(offsets[2]), "%c%4ld", sign, hhmm);
  }
  size_t length = strlen(format);
  size_t extra = strlen(offsets[0]);
  extra = extra > 2 ? extra - 2 : 0;
  if (extra != 0 && length / 2 > (SIZE_MAX - length - 1) / extra) return 0;
  size_t capacity = length + 1 + (length / 2) * extra;
  char *expanded = taosMemoryMalloc(capacity);
  if (expanded == NULL) return 0;

  char *out = expanded;
  for (const char *p = format; *p != '\0';) {
    if (*p != '%' || p[1] == '\0') {
      *out++ = *p++;
      continue;
    }
    const char *conversion = p + 1;
    int padding = 0;
    while (*conversion != '\0' && strchr("EO-_0", *conversion) != NULL) {
      if (*conversion == '-') padding = 1;
      if (*conversion == '_') padding = 2;
      if (*conversion == '0') padding = 0;
      ++conversion;
    }
    if (*conversion == 'z') {
      size_t offsetLength = strlen(offsets[padding]);
      memcpy(out, offsets[padding], offsetLength);
      out += offsetLength;
      p = conversion + 1;
    } else {
      // Preserve the entire conversion, including escaped %%z literals.
      size_t count = (size_t)(conversion - p) + (*conversion != '\0');
      memcpy(out, p, count);
      out += count;
      p += count;
    }
  }
  *out = '\0';
  size_t result = strftime(s, size, expanded, value);
  taosMemoryFree(expanded);
  return result;
}
#endif

size_t taosStrfTime(char *s, size_t size, const char *format,
                    const struct tm *value) {
  if (s == NULL || format == NULL || value == NULL) return 0;
#ifdef WINDOWS
  // Plain date formats need no zone lookup and work for UTC callers too.
  if (!formatNeedsTimezone(format)) {
    return taosTzStrftime(s, size, format, value, NULL);
  }
  timezone_t tz = taosAcquireDefaultTimezone();
  size_t result = tz != NULL
      ? taosTzStrftime(s, size, format, value, taosTimezoneRules(tz)) : 0;
  tzfree(tz);
  return result;
#elif defined(DARWIN)
  return formatDarwinTime(s, size, format, value);
#else
  return strftime(s, size, format, value);
#endif
}

size_t taosStrfTimeAt(char *s, size_t size, const char *format,
                      const time_t *stamp, timezone_t tz) {
  if (s == NULL || format == NULL || stamp == NULL) return 0;
#ifdef WINDOWS
  bool owned = tz == NULL;
  if (owned) tz = taosAcquireDefaultTimezone();
  size_t result = tz != NULL
      ? taosTzStrftimeAt(s, size, format, stamp, taosTimezoneRules(tz)) : 0;
  if (owned) tzfree(tz);
  return result;
#else
  struct tm local;
  if (taosLocalTime(stamp, &local, NULL, 0, tz) == NULL) return 0;
#ifdef DARWIN
  return formatDarwinTime(s, size, format, &local);
#else
  return strftime(s, size, format, &local);
#endif
#endif
}

int32_t taosGetTimeOfDay(struct timeval *tv) {
  if (tv == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  struct timespec systemTime = {0};
  int32_t code = taosClockGetTime(CLOCK_REALTIME, &systemTime);
  if (code != 0) {
    return code;
  }
  tv->tv_sec = systemTime.tv_sec;
  tv->tv_usec = systemTime.tv_nsec / NANOSECOND_PER_USEC;
  return 0;
}

// Second-level helpers use time() instead of CLOCK_REALTIME for
// performance. On Linux, time() returns the seconds the kernel cached at
// the last tick through the vDSO without reading the hardware clock, so
// it is about 10x faster. The cached value may lag CLOCK_REALTIME by up
// to one tick, so near a second boundary it can be one second behind.
int32_t taosTime(time_t *t) {
  if (t == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  time_t r = time(t);
  if (r == (time_t)-1) {
    return TAOS_SYSTEM_ERROR(ERRNO);
  }
  return 0;
}

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
int64_t user_mktime64(const uint32_t year, const uint32_t mon, const uint32_t day, const uint32_t hour,
                      const uint32_t min, const uint32_t sec, int64_t time_zone) {
  uint32_t _mon = mon, _year = year;

  /* 1..12 -> 11,12,1..10 */
  if (0 >= (int32_t)(_mon -= 2)) {
    _mon += 12; /* Puts Feb last since it has leap day */
    _year -= 1;
  }

  // int64_t _res = (((((int64_t) (_year/4 - _year/100 + _year/400 + 367*_mon/12 + day) +
  //                _year*365 - 719499)*24 + hour)*60 + min)*60 + sec);
  int64_t _res = 367 * ((int64_t)_mon) / 12;
  _res += _year / 4 - _year / 100 + _year / 400 + day + ((int64_t)_year) * 365 - 719499;
  _res *= 24;
  _res = ((_res + hour) * 60 + min) * 60 + sec;

  return _res + time_zone;
}

time_t taosMktime(struct tm *timep, timezone_t tz) {
  if (timep == NULL) {
    errno = EINVAL;
    return (time_t)-1;
  }
#ifdef WINDOWS
  bool owned = tz == NULL;
  if (owned) tz = taosAcquireDefaultTimezone();
  if (tz == NULL) return (time_t)-1;
  errno = 0;
  long offset = 0;
  time_t result = taosTzMktime(taosTimezoneRules(tz), timep, &offset);
  int savedErrno = errno;
  if (owned) tzfree(tz);
  errno = savedErrno;
  if (result == (time_t)-1 && errno != 0) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
  } else {
    timezone = -offset;
  }
  return result;
#elif defined(TD_ASTRA)
  /*
   * Clear errno first so the caller-side TAOS_MKTIME_FAILED() macro can reliably
   * distinguish a genuine mktime failure from a (time_t)-1 return for a valid
   * time: -1 is also the valid epoch 1969-12-31 23:59:59 UTC, so failure is
   * detected by errno != TSDB_CODE_SUCCESS, not by the return value alone.
   * Do not remove this reset.
   */
  errno = TSDB_CODE_SUCCESS;
  time_t r =  mktime(timep);
  if (r == (time_t)-1 && errno != TSDB_CODE_SUCCESS) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
  }
  return r;
#else
  if (tz == NULL) {
    tz = getGlobalDefaultTZ();
  }
  /*
   * Clear errno first so the caller-side TAOS_MKTIME_FAILED() macro can reliably
   * distinguish a genuine mktime failure from a (time_t)-1 return for a valid
   * time: -1 is also the valid epoch 1969-12-31 23:59:59 UTC, so failure is
   * detected by errno != TSDB_CODE_SUCCESS, not by the return value alone.
   * Do not remove this reset.
   */
  errno = TSDB_CODE_SUCCESS;
  // tzcode can reject normalization without setting errno.
  timep->tm_wday = -1;
  time_t r = (tz != NULL ? mktime_z(tz, timep) : mktime(timep));
  if (r == (time_t)-1 && timep->tm_wday < 0 && errno == 0) {
    errno = EOVERFLOW;
  }
  if (r == (time_t)-1 && errno != TSDB_CODE_SUCCESS) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
  }
  timezone = -timep->tm_gmtoff;
  return r;
#endif
}

#ifdef WINDOWS
/* POSIX mktime_z for Windows: use the same complete rules as taosMktime.
 * The providers
 * (MySQL / PostgreSQL binary temporal values) call mktime_z directly. */
time_t mktime_z(timezone_t tz, struct tm *timep) {
  return taosMktime(timep, tz);
}
#endif

struct tm *taosGmTimeR(const time_t *timep, struct tm *result) {
  if (timep == NULL || result == NULL) {
    return NULL;
  }
#ifdef WINDOWS
  return taosTzGmtime(timep, result);
#else
  return gmtime_r(timep, result);
#endif
}

time_t taosTimeGm(struct tm *tmp) {
  if (tmp == NULL) {
    return -1;
  }
#ifdef WINDOWS
  errno = 0;
  return taosTzTimegm(tmp);
#elif defined(TD_ASTRA)
  time_t    local = mktime(tmp);
  struct tm local_tm = {0};
  taosLocalTime(&local, &local_tm, NULL, 0, NULL);
  struct tm utc_tm = {0};
  taosGmTimeR(&local, &utc_tm);
  time_t    offset = (local_tm.tm_hour - utc_tm.tm_hour) * 3600 + (local_tm.tm_min - utc_tm.tm_min) * 60 +
                  (local_tm.tm_sec - utc_tm.tm_sec);
  return local - offset;
#else
  return timegm(tmp);
#endif
}


struct tm *taosLocalTime(const time_t *timep, struct tm *result, char *buf, int32_t bufSize, timezone_t tz) {
  struct tm *res = NULL;
  if (timep == NULL || result == NULL) {
    return NULL;
  }
#ifdef WINDOWS
  bool owned = tz == NULL;
  if (owned) tz = taosAcquireDefaultTimezone();
  long offset = 0;
  res = tz != NULL
      ? taosTzLocaltime(taosTimezoneRules(tz), timep, result, &offset) : NULL;
  if (owned) tzfree(tz);
  if (res == NULL && buf != NULL) {
    (void)snprintf(buf, bufSize, "NaN");
  }
  if (res != NULL) timezone = -offset;
  return res;
#elif defined(TD_ASTRA)
  res = localtime_r(timep, result);
  if (res == NULL && buf != NULL) {
    (void)snprintf(buf, bufSize, "NaN");
  }
  return res;
#else
  if (tz == NULL) {
    tz = getGlobalDefaultTZ();
  }
  res = (tz != NULL ? localtime_rz(tz, timep, result) : localtime_r(timep, result));
  if (res == NULL && buf != NULL) {
    (void)snprintf(buf, bufSize, "NaN");
  }
  timezone = -result->tm_gmtoff;
  return res;
#endif
}

// Uses time() for performance; see taosTime().
int32_t taosGetTimestampSec() { return (int32_t)time(NULL); }

int32_t taosGetMonotonicClockId(char *clockId, size_t capacity) {
  if (clockId == NULL || capacity == 0) return -1;
  clockId[0] = 0;
#if defined(__linux__)
  char bootId[38] = {0};
  FILE *bootFile = fopen("/proc/sys/kernel/random/boot_id", "r");
  if (bootFile == NULL) return -1;
  bool haveBootId = fgets(bootId, sizeof(bootId), bootFile) != NULL;
  (void)fclose(bootFile);
  if (!haveBootId || strlen(bootId) != 37 || bootId[36] != '\n') return -1;
  bootId[36] = 0;
  for (size_t index = 0; index < 36; ++index) {
    bool separator = index == 8 || index == 13 || index == 18 || index == 23;
    if (separator ? bootId[index] != '-' : !isxdigit((unsigned char)bootId[index])) return -1;
  }

  char namespaceId[32] = {0};
  ssize_t namespaceLength = readlink("/proc/self/ns/time", namespaceId, sizeof(namespaceId) - 1);
  if (namespaceLength < 0) {
    if (errno != ENOENT) return -1;
    strcpy(namespaceId, "initial");
  } else {
    namespaceId[namespaceLength] = 0;
    if (namespaceLength >= (ssize_t)sizeof(namespaceId) - 1 ||
        strncmp(namespaceId, "time:[", 6) != 0 || namespaceId[namespaceLength - 1] != ']') return -1;
  }
  int32_t length = snprintf(clockId, capacity, "linux:%s:%s", bootId, namespaceId);
  if (length > 0 && (size_t)length < capacity) return 0;
  clockId[0] = 0;
#endif
  return -1;
}

int32_t taosClockGetTime(int clock_id, struct timespec *pTS) {
  int32_t code = 0;
#ifdef WINDOWS
  if (clock_id == CLOCK_MONOTONIC) {
    static LARGE_INTEGER freq;
    static BOOL freq_init = FALSE;
    LARGE_INTEGER t;
    if (!freq_init) {
        QueryPerformanceFrequency(&freq);
        freq_init = TRUE;
    }
    QueryPerformanceCounter(&t);
     // Convert to seconds and nanoseconds.
    pTS->tv_sec = (time_t)(t.QuadPart / freq.QuadPart);
    pTS->tv_nsec = (long)(((t.QuadPart % freq.QuadPart) * 1000000000LL) / freq.QuadPart);
    return 0;
  } else {
    LARGE_INTEGER t;
    FILETIME      f;

    // Stream output can use wall-clock nanoseconds as a composite key. The
    // coarse system clock may return the same value for separate windows in
    // one calculation batch, causing the later row to replace the earlier one.
    GetSystemTimePreciseAsFileTime(&f);
    t.QuadPart = f.dwHighDateTime;
    t.QuadPart <<= 32;
    t.QuadPart |= f.dwLowDateTime;

    t.QuadPart -= TIMEEPOCH;
    pTS->tv_sec = t.QuadPart / 10000000;
    pTS->tv_nsec = (t.QuadPart % 10000000) * 100;
    return (0);
  }
#else
  code = clock_gettime(clock_id, pTS);
  return (-1 == code) ? (terrno = TAOS_SYSTEM_ERROR(ERRNO)) : 0;
#endif
}
