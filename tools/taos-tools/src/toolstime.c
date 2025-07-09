/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#define _BSD_SOURCE

#ifdef DARWIN
#define _XOPEN_SOURCE
#else
#define _XOPEN_SOURCE 500
#endif

#define _DEFAULT_SOURCE

#ifdef LINUX
#include <unistd.h>
#include <strings.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <inttypes.h>
#include <stdbool.h>
#include <time.h>


#include "bench.h"
#include "toolsdef.h"

#if defined(WINDOWS)

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <winsock2.h>

// This magic number is the number of 100 nanosecond intervals since January 1, 1601 (UTC)
// until 00:00:00 January 1, 1970
static const uint64_t TIMEEPOCH = ((uint64_t)116444736000000000ULL);

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

static int32_t parseLocaltime(char* timestr, int64_t* time, int32_t timePrec, char delim);
// static int32_t parseLocaltimeWithDst(char* timestr, int64_t* time, int32_t timePrec, char delim);

static int32_t (*parseLocaltimeFp[]) (char* timestr, int64_t* time, int32_t timePrec, char delim) = {
    parseLocaltime,
    // parseLocaltimeWithDst
};

char *tools_strnchr(char *haystack, char needle, int32_t len, bool skipquote) {
    for (int32_t i = 0; i < len; ++i) {

        // skip the needle in quote, jump to the end of quoted string
        if (skipquote && (haystack[i] == '\'' || haystack[i] == '"')) {
            char quote = haystack[i++];
            while(i < len && haystack[i++] != quote);
            if (i >= len) {
                return NULL;
            }
        }

        if (haystack[i] == needle) {
            return &haystack[i];
        }
    }

    return NULL;
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
int64_t tools_strnatoi(char *num, int32_t len) {
    int64_t ret = 0, i, dig, base = 1;

    if (len > (int32_t)strlen(num)) {
        len = (int32_t)strlen(num);
    }

    if ((len > 2) && (num[0] == '0') && ((num[1] == 'x') || (num[1] == 'X'))) {
        for (i = len - 1; i >= 2; --i, base *= 16) {
            if (num[i] >= '0' && num[i] <= '9') {
                dig = (num[i] - '0');
            } else if (num[i] >= 'a' && num[i] <= 'f') {
                dig = num[i] - 'a' + 10;
            } else if (num[i] >= 'A' && num[i] <= 'F') {
                dig = num[i] - 'A' + 10;
            } else {
                return 0;
            }
            ret += dig * base;
        }
    } else {
        for (i = len - 1; i >= 0; --i, base *= 10) {
            if (num[i] >= '0' && num[i] <= '9') {
                dig = (num[i] - '0');
            } else {
                return 0;
            }
            ret += dig * base;
        }
    }

    return ret;
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
    } else {
        assert(timePrec == TSDB_TIME_PRECISION_NANO);
        if (i >= NANO_SEC_FRACTION_LEN) {
            i = NANO_SEC_FRACTION_LEN;
        }
        times = NANO_SEC_FRACTION_LEN - i;
    }

    fraction = tools_strnatoi(str, i) * factor[times];
    *end = str + totalLen;

    return fraction;
}
int64_t tools_user_mktime64(const unsigned int year0, const unsigned int mon0,
                            const unsigned int day, const unsigned int hour,
                            const unsigned int min, const unsigned int sec, int64_t time_zone)
{
    unsigned int mon = mon0, year = year0;

    /* 1..12 -> 11,12,1..10 */
    if (0 >= (int) (mon -= 2)) {
        mon += 12;  /* Puts Feb last since it has leap day */
        year -= 1;
    }

    //int64_t res = (((((int64_t) (year/4 - year/100 + year/400 + 367*mon/12 + day) +
    //               year*365 - 719499)*24 + hour)*60 + min)*60 + sec);
    int64_t res;
    res  = 367*((int64_t)mon)/12;
    res += year/4 - year/100 + year/400 + day + ((int64_t)year)*365 - 719499;
    res  = res*24;
    res  = ((res + hour) * 60 + min) * 60 + sec;

    return (res + time_zone);
}


int32_t toolsParseTimezone(char* str, int64_t* tzOffset) {
    int64_t hour = 0;

    int32_t i = 0;
    if (str[i] != '+' && str[i] != '-') {
        return -1;
    }

    i++;

    char* sep = strchr(&str[i], ':');
    if (sep != NULL) {
        int32_t len = (int32_t)(sep - &str[i]);

        hour = tools_strnatoi(&str[i], len);
        i += len + 1;
    } else {
        hour = tools_strnatoi(&str[i], 2);
        i += 2;
    }

    //return error if there're illegal characters after min(2 Digits)
    char *minStr = &str[i];
    if (minStr[1] != '\0' && minStr[2] != '\0') {
        return -1;
    }


    int64_t minute = tools_strnatoi(&str[i], 2);
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

int32_t toolsClockGetTime(int clock_id, struct timespec *pTS) {
#if defined(WIN32) || defined(WIN64)
    LARGE_INTEGER        t;
    FILETIME             f;

    GetSystemTimeAsFileTime(&f);
    t.QuadPart = f.dwHighDateTime;
    t.QuadPart <<= 32;
    t.QuadPart |= f.dwLowDateTime;

    t.QuadPart -= TIMEEPOCH;
    pTS->tv_sec = t.QuadPart / 10000000;
    pTS->tv_nsec = (t.QuadPart % 10000000)*100;
    return (0);
#else
    return clock_gettime(clock_id, pTS);
#endif
}


char *toolsStrpTime(const char *buf, const char *fmt, struct tm *tm) {
#if defined(WIN32) || defined(WIN64)
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
                if (!(bp = toolsStrpTime(bp, "%x %X", tm))) return (0);
                break;

            case 'D': /* The date as "%m/%d/%y". */
                LEGAL_ALT(0);
                if (!(bp = toolsStrpTime(bp, "%m/%d/%y", tm))) return (0);
                break;

            case 'R': /* The time as "%H:%M". */
                LEGAL_ALT(0);
                if (!(bp = toolsStrpTime(bp, "%H:%M", tm))) return (0);
                break;

            case 'r': /* The time in 12-hour clock representation. */
                LEGAL_ALT(0);
                if (!(bp = toolsStrpTime(bp, "%I:%M:%S %p", tm))) return (0);
                break;

            case 'T': /* The time as "%H:%M:%S". */
                LEGAL_ALT(0);
                if (!(bp = toolsStrpTime(bp, "%H:%M:%S", tm))) return (0);
                break;

            case 'X': /* The time, using the locale's format. */
LEGAL_ALT(ALT_E);
                if (!(bp = toolsStrpTime(bp, "%H:%M:%S", tm))) return (0);
                break;

            case 'x': /* The date, using the locale's format. */
                LEGAL_ALT(ALT_E);
                if (!(bp = toolsStrpTime(bp, "%m/%d/%y", tm))) return (0);
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

int32_t parseTimeWithTz(char* timestr, int64_t* time, int32_t timePrec, char delim) {

    int64_t factor = (timePrec == TSDB_TIME_PRECISION_MILLI) ? 1000 :
        (timePrec == TSDB_TIME_PRECISION_MICRO ? 1000000 : 1000000000);
    int64_t tzOffset = 0;

    struct tm tm = {0};

    char* str;
    if (delim == 'T') {
str = toolsStrpTime(timestr, "%Y-%m-%dT%H:%M:%S", &tm);
    } else if (delim == 0) {
    str = toolsStrpTime(timestr, "%Y-%m-%d %H:%M:%S", &tm);
} else {
str = NULL;
    }

    if (str == NULL) {
        return -1;
    }

    /* mktime will be affected by TZ, set by using taos_options */
#ifdef WINDOWS
    int64_t seconds = tools_user_mktime64(tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, 0);
    //int64_t seconds = gmtime(&tm);
#else
    int64_t seconds = timegm(&tm);
#endif

    int64_t fraction = 0;
    str = forwardToTimeStringEnd(timestr);

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
            if (toolsParseTimezone(str, &tzOffset) == -1) {
                return -1;
}

            *time += tzOffset * factor;
        }

    } else if (str[0] == '+' || str[0] == '-') {
        *time = seconds * factor + fraction;

        // parse the timezone
        if (toolsParseTimezone(str, &tzOffset) == -1) {
            return -1;
        }

        *time += tzOffset * factor;
    } else {
        return -1;
    }

    return 0;
}

bool checkTzPresent(char *str, int32_t len) {
    char *seg = forwardToTimeStringEnd(str);
    int32_t seg_len = len - (int32_t)(seg - str);

    char *c = &seg[seg_len - 1];
    for (int i = 0; i < seg_len; ++i) {
        if (*c == 'Z' || *c  == 'z' || *c == '+' || *c == '-') {
            return true;
        }
        c--;
    }
    return false;
}

int32_t parseLocaltime(char* timestr, int64_t* time, int32_t timePrec, char delim) {
    *time = 0;
    struct tm tm = {0};

    char* str;
    if (delim == 'T') {
        str = toolsStrpTime(timestr, "%Y-%m-%dT%H:%M:%S", &tm);
    } else if (delim == 0) {
        str = toolsStrpTime(timestr, "%Y-%m-%d %H:%M:%S", &tm);
    } else {
        str = NULL;
    }

    if (str == NULL) {
        return -1;
    }

#ifdef _MSC_VER
#if _MSC_VER >= 1900
    int64_t timezone = _timezone;
#endif
#endif

    int64_t seconds = tools_user_mktime64(tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, timezone);

    int64_t fraction = 0;

    if (*str == '.') {
        /* parse the second fraction part */
        if ((fraction = parseFraction(str + 1, &str, timePrec)) < 0) {
            return -1;
        }
    }

    int64_t factor = (timePrec == TSDB_TIME_PRECISION_MILLI) ? 1000 :
        (timePrec == TSDB_TIME_PRECISION_MICRO ? 1000000 : 1000000000);
    *time = factor * seconds + fraction;

    return 0;
}

// int32_t parseLocaltimeWithDst(char* timestr, int64_t* time, int32_t timePrec, char delim) {
//     *time = 0;
//     struct tm tm = {0};
//     tm.tm_isdst = -1;

//     char* str;
// if (delim == 'T') {
//         str = toolsStrpTime(timestr, "%Y-%m-%dT%H:%M:%S", &tm);
//     } else if (delim == 0) {
//         str = toolsStrpTime(timestr, "%Y-%m-%d %H:%M:%S", &tm);
//     } else {
//         str = NULL;
// }

//     if (str == NULL) {
//         return -1;
//     }

//     /* mktime will be affected by TZ, set by using taos_options */
// int64_t seconds = mktime(&tm);

// int64_t fraction = 0;

//     if (*str == '.') {
//         /* parse the second fraction part */
//         if ((fraction = parseFraction(str + 1, &str, timePrec)) < 0) {
// return -1;
// }
// }

//     int64_t factor = (timePrec == TSDB_TIME_PRECISION_MILLI) ? 1000 :
//         (timePrec == TSDB_TIME_PRECISION_MICRO ? 1000000 : 1000000000);
// *time = factor * seconds + fraction;
//     return 0;
// }

int32_t toolsParseTime(char* timestr, int64_t* time, int32_t len, int32_t timePrec, int8_t day_light) {
    /* parse datatime string in with tz */
if (tools_strnchr(timestr, 'T', len, false) != NULL) {
        if (checkTzPresent(timestr, len)) {
            return parseTimeWithTz(timestr, time, timePrec, 'T');
} else {
return (*parseLocaltimeFp[day_light])(timestr, time, timePrec, 'T');
        }
    } else {
if (checkTzPresent(timestr, len)) {
            return parseTimeWithTz(timestr, time, timePrec, 0);
        } else {
            return (*parseLocaltimeFp[day_light])(timestr, time, timePrec, 0);
        }
    }
}

struct tm* toolsLocalTime(const time_t *timep, struct tm *result) {
#if defined(LINUX) || defined(DARWIN)
    localtime_r(timep, result);
#else
    localtime_s(result, timep);
#endif
    return result;
}

FORCE_INLINE int32_t toolsGetTimestampSec() { return (int32_t)time(NULL); }

FORCE_INLINE int32_t toolsGetTimeOfDay(struct timeval *tv) {
#if defined(WIN32) || defined(WIN64)
    LARGE_INTEGER t;
    FILETIME      f;

    GetSystemTimeAsFileTime(&f);
    t.QuadPart = f.dwHighDateTime;
    t.QuadPart <<= 32;
    t.QuadPart |= f.dwLowDateTime;

    t.QuadPart -= TIMEEPOCH;
    tv->tv_sec = t.QuadPart / 10000000;
    tv->tv_usec = (t.QuadPart % 10000000) / 10;
    return (0);
#else
    return gettimeofday(tv, NULL);
#endif
}

FORCE_INLINE int64_t toolsGetTimestampMs() {
    struct timeval systemTime;
    toolsGetTimeOfDay(&systemTime);
    return (int64_t)systemTime.tv_sec * 1000L +
        (int64_t)systemTime.tv_usec / 1000;
}

FORCE_INLINE int64_t toolsGetTimestampUs() {
    struct timeval systemTime;
    toolsGetTimeOfDay(&systemTime);
    return (int64_t)systemTime.tv_sec * 1000000L + (int64_t)systemTime.tv_usec;
}

#if defined(WINDOWS)
    #define CLOCK_REALTIME 0

void usleep(__int64 usec)
{
    HANDLE timer;
    LARGE_INTEGER ft;

    ft.QuadPart = -(10*usec); // Convert to 100 nanosecond interval, negative value indicates relative time

    timer = CreateWaitableTimer(NULL, TRUE, NULL);
    SetWaitableTimer(timer, &ft, 0, NULL, NULL, 0);
    WaitForSingleObject(timer, INFINITE);
    CloseHandle(timer);
}
#endif

FORCE_INLINE int64_t toolsGetTimestampNs() {
    struct timespec systemTime = {0};
    toolsClockGetTime(CLOCK_REALTIME, &systemTime);
    return (int64_t)systemTime.tv_sec * 1000000000L +
        (int64_t)systemTime.tv_nsec;
}

FORCE_INLINE void toolsMsleep(int32_t mseconds) { usleep(mseconds * 1000); }

struct tm *tLocalTime(const time_t *timep, struct tm *result, char *buf) {
  struct tm *res = NULL;
  if (timep == NULL) {
    return NULL;
  }
  if (result == NULL) {
    res = localtime(timep);
    if (res == NULL && buf != NULL) {
      sprintf(buf, "NaN");
    }
    return res;
  }
#ifdef WINDOWS
  if (*timep < -2208988800LL) {
    if (buf != NULL) {
      sprintf(buf, "NaN");
    }
    return NULL;
  }

  SYSTEMTIME    s;
  FILETIME      f;
  LARGE_INTEGER offset;
  struct tm     tm1;
  time_t        tt = 0;
  if (localtime_s(&tm1, &tt) != 0) {
    if (buf != NULL) {
      sprintf(buf, "NaN");
    }
    return NULL;
  }
  offset.QuadPart = ((uint64_t)116445024000000000ULL);
  offset.QuadPart += *timep * 10000000;
  f.dwLowDateTime = offset.QuadPart & 0xffffffff;
  f.dwHighDateTime = (offset.QuadPart >> 32) & 0xffffffff;
  FileTimeToSystemTime(&f, &s);
  result->tm_sec = s.wSecond;
  result->tm_min = s.wMinute;
  result->tm_hour = s.wHour;
  result->tm_mday = s.wDay;
  result->tm_mon = s.wMonth - 1;
  result->tm_year = s.wYear - 1900;
  result->tm_wday = s.wDayOfWeek;
  result->tm_yday = 0;
  result->tm_isdst = 0;
#else
  res = localtime_r(timep, result);
  if (res == NULL && buf != NULL) {
    sprintf(buf, "NaN");
  }
#endif
  return result;
}

char *toolsFormatTimestamp(char *buf, int64_t val, int32_t precision) {
  time_t  tt;
  int32_t ms = 0;
  if (precision == TSDB_TIME_PRECISION_NANO) {
    tt = (time_t)(val / 1000000000);
    ms = val % 1000000000;
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    tt = (time_t)(val / 1000000);
    ms = val % 1000000;
  } else {
    tt = (time_t)(val / 1000);
    ms = val % 1000;
  }

  if (tt <= 0 && ms < 0) {
    tt--;
    if (precision == TSDB_TIME_PRECISION_NANO) {
      ms += 1000000000;
    } else if (precision == TSDB_TIME_PRECISION_MICRO) {
      ms += 1000000;
    } else {
      ms += 1000;
    }
  }

  struct tm ptm = {0};
  if (tLocalTime(&tt, &ptm, buf) == NULL) {
    return buf;
  }
  size_t pos = strftime(buf, 35, "%Y-%m-%d %H:%M:%S", &ptm);

  if (precision == TSDB_TIME_PRECISION_NANO) {
    sprintf(buf + pos, ".%09d", ms);
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    sprintf(buf + pos, ".%06d", ms);
  } else {
    sprintf(buf + pos, ".%03d", ms);
  }

  return buf;
}
