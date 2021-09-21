/*****************************************************************************\
*                                                                             *
*   Filename:	    clock_gettime.c					      *
*                                                                             *
*   Description:    WIN32 port of standard C library's clock_gettime().	      *
*                                                                             *
*   Notes:	    							      *
*                                                                             *
*   History:								      *
*    2014-06-04 JFL Created this file.                                        *
*									      *
*         Copyright 2016 Hewlett Packard Enterprise Development LP          *
* Licensed under the Apache 2.0 license - www.apache.org/licenses/LICENSE-2.0 *
\*****************************************************************************/

#include "msvclibx.h"

#include <time.h>
#include <errno.h>


#ifdef _MSDOS

/* Check for the definition of _STRUCT_TIMESPEC before using clock_gettime().
   If it's not defined, use time() instead, which is supported by all OSs. */

#endif /* defined(_MSDOS) */


#ifdef _WIN32

#define WIN32_LEAN_AND_MEAN /* Avoid lots of unnecessary inclusions */
#include <windows.h>
#include "msvcTime.h"
#include "sys/msvcStat.h" /* For MsvcLibX's Filetime2Timespec */

#define MS_PER_SEC      1000ULL     // MS = milliseconds
#define US_PER_MS       1000ULL     // US = microseconds
#define HNS_PER_US      10ULL       // HNS = hundred-nanoseconds (e.g., 1 hns = 100 ns)
#define NS_PER_US       1000ULL

#define HNS_PER_SEC     (MS_PER_SEC * US_PER_MS * HNS_PER_US)
#define NS_PER_HNS      (100ULL)    // NS = nanoseconds
#define NS_PER_SEC      (MS_PER_SEC * US_PER_MS * NS_PER_US)

int clock_gettime_monotonic(struct timespec *tv) {
  static LARGE_INTEGER ticksPerSec;
  LARGE_INTEGER ticks;
  double seconds;

  if (!ticksPerSec.QuadPart) {
    QueryPerformanceFrequency(&ticksPerSec);
    if (!ticksPerSec.QuadPart) {
      errno = ENOTSUP;
      return -1;
    }
  }

  QueryPerformanceCounter(&ticks);

  seconds = (double) ticks.QuadPart / (double) ticksPerSec.QuadPart;
  tv->tv_sec = (time_t)seconds;
  tv->tv_nsec = (long)((ULONGLONG)(seconds * NS_PER_SEC) % NS_PER_SEC);

  return 0;
}

int clock_gettime_realtime(struct timespec *pTS) {
  FILETIME ft;

  GetSystemTimeAsFileTime(&ft);
  Filetime2Timespec(&ft, pTS);

  return 0;
}

int clock_gettime(clockid_t clock_id, struct timespec *pTS) {
  if (clock_id == CLOCK_MONOTONIC) {
    return clock_gettime_monotonic(pTS);
  } else if (clock_id == CLOCK_REALTIME) {
    return clock_gettime_realtime(pTS);
  }

  errno = ENOTSUP;

  return -1;
}

#endif /* defined(_WIN32) */
