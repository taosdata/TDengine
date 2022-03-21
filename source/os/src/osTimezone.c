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
#define _DEFAULT_SOURCE
#include "os.h"

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#if (_WIN64)
#include <iphlpapi.h>
#include <mswsock.h>
#include <psapi.h>
#include <stdio.h>
#include <windows.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Mswsock.lib ")
#endif
#include <objbase.h>
#pragma warning(push)
#pragma warning(disable : 4091)
#include <DbgHelp.h>
#pragma warning(pop)
#elif defined(_TD_DARWIN_64)
#include <errno.h>
#include <libproc.h>
#else
#include <argp.h>
#include <linux/sysctl.h>
#include <sys/file.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <unistd.h>
#endif

void taosSetSystemTimezone(const char *inTimezone, char *outTimezone, int8_t *outDaylight) {
  if (inTimezone == NULL || inTimezone[0] == 0) return;

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  char winStr[TD_LOCALE_LEN * 2];
  sprintf(winStr, "TZ=%s", inTimezone);
  putenv(winStr);
  tzset();
   * get CURRENT time zone.
   * system current time zone is affected by daylight saving time(DST)
   *
   * e.g., the local time zone of London in DST is GMT+01:00,
   * otherwise is GMT+00:00
   */
#ifdef _MSC_VER
#if _MSC_VER >= 1900
  // see https://docs.microsoft.com/en-us/cpp/c-runtime-library/daylight-dstbias-timezone-and-tzname?view=vs-2019
  int64_t timezone = _timezone;
  int32_t daylight = _daylight;
  char  **tzname = _tzname;
#endif
#endif

  int32_t tz = (int32_t)((-timezone * MILLISECOND_PER_SECOND) / MILLISECOND_PER_HOUR);
  tz += daylight;
  /*
   * format:
   * (CST, +0800)
   * (BST, +0100)
   */
  sprintf(outTimezone, "(%s, %s%02d00)", tzname[daylight], tz >= 0 ? "+" : "-", abs(tz));
  *outDaylight = daylight;

#elif defined(_TD_DARWIN_64)

  setenv("TZ", inTimezone, 1);
  tzset();
  int32_t tz = (int32_t)((-timezone * MILLISECOND_PER_SECOND) / MILLISECOND_PER_HOUR);
  tz += daylight;

  sprintf(outTimezone, "(%s, %s%02d00)", tzname[daylight], tz >= 0 ? "+" : "-", abs(tz));
  *outDaylight = daylight;

#else
  setenv("TZ", inTimezone, 1);
  tzset();
  int32_t tz = (int32_t)((-timezone * MILLISECOND_PER_SECOND) / MILLISECOND_PER_HOUR);
  tz += daylight;
  sprintf(outTimezone, "(%s, %s%02d00)", tzname[daylight], tz >= 0 ? "+" : "-", abs(tz));
  *outDaylight = daylight;

#endif

}

void taosGetSystemTimezone(char *outTimezone) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  char *tz = getenv("TZ");
  if (tz == NULL || strlen(tz) == 0) {
    strcpy(outTimezone, "not configured");
  } else {
    strcpy(outTimezone, tz);
  }

#elif defined(_TD_DARWIN_64)
  char  buf[4096] = {0};
  char *tz = NULL;
  {
    int n = readlink("/etc/localtime", buf, sizeof(buf));
    if (n < 0) {
      printf("read /etc/localtime error, reason:%s", strerror(errno));
      return;
    }
    buf[n] = '\0';
    for (int i = n - 1; i >= 0; --i) {
      if (buf[i] == '/') {
        if (tz) {
          tz = buf + i + 1;
          break;
        }
        tz = buf + i + 1;
      }
    }
    if (!tz || 0 == strchr(tz, '/')) {
      printf("parsing /etc/localtime failed");
      return;
    }

    setenv("TZ", tz, 1);
    tzset();
  }

  /*
   * NOTE: do not remove it.
   * Enforce set the correct daylight saving time(DST) flag according
   * to current time
   */
  time_t    tx1 = taosGetTimestampSec();
  struct tm tm1;
  taosLocalTime(&tx1, &tm1);

  /*
   * format example:
   *
   * Asia/Shanghai   (CST, +0800)
   * Europe/London   (BST, +0100)
   */
  snprintf(outTimezone, TD_TIMEZONE_LEN, "%s (%s, %+03ld00)", tz, tm1.tm_isdst ? tzname[daylight] : tzname[0],
           -timezone / 3600);

#else
  /*
   * NOTE: do not remove it.
   * Enforce set the correct daylight saving time(DST) flag according
   * to current time
   */
  time_t    tx1 = taosGetTimestampSec();
  struct tm tm1;
  taosLocalTime(&tx1, &tm1);

  /* load time zone string from /etc/timezone */
  // FILE *f = fopen("/etc/timezone", "r");
  TdFilePtr pFile = taosOpenFile("/etc/timezone", TD_FILE_READ);
  char  buf[68] = {0};
  if (pFile != NULL) {
    int len = taosReadFile(pFile, buf, 64);
    if (len < 64 && taosGetErrorFile(pFile)) {
      taosCloseFile(&pFile);
      // printf("read /etc/timezone error, reason:%s", strerror(errno));
      return;
    }

    taosCloseFile(&pFile);

    buf[sizeof(buf) - 1] = 0;
    char *lineEnd = strstr(buf, "\n");
    if (lineEnd != NULL) {
      *lineEnd = 0;
    }

    // for CentOS system, /etc/timezone does not exist. Ignore the TZ environment variables
    if (strlen(buf) > 0) {
      setenv("TZ", buf, 1);
    }
  }
  // get and set default timezone
  tzset();

  /*
   * get CURRENT time zone.
   * system current time zone is affected by daylight saving time(DST)
   *
   * e.g., the local time zone of London in DST is GMT+01:00,
   * otherwise is GMT+00:00
   */
  int32_t tz = (-timezone * MILLISECOND_PER_SECOND) / MILLISECOND_PER_HOUR;
  tz += daylight;

  /*
   * format example:
   *
   * Asia/Shanghai   (CST, +0800)
   * Europe/London   (BST, +0100)
   */
  snprintf(outTimezone, TD_TIMEZONE_LEN, "%s (%s, %s%02d00)", buf, tzname[daylight], tz >= 0 ? "+" : "-", abs(tz));

#endif
}
