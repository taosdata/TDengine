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

#include "todbc_log.h"

uint64_t todbc_get_threadid(void) {
  uint64_t tid = 0;
#ifdef __APPLE__
  pthread_threadid_np(NULL, &tid);
#elif defined(__linux__)
  tid = (uint64_t)syscall(__NR_gettid);
#elif defined(_MSC_VER)
  tid = GetCurrentThreadId();
#else
#error you have to check the target API for thread id
#endif
  return tid;
}

#ifdef _MSC_VER
static __declspec(thread) uint64_t thread_id = 0;
#else
static __thread           uint64_t thread_id = 0;
#endif

#ifdef __GNUC__
 __attribute__((format(printf, 6, 7)))
#endif
void todbc_log(const char *file, int line, const char *func, const char tag, int err, const char *fmt, ...) {
  struct tm      stm = {0};
  struct timeval tv;

  if (thread_id==0) {
    thread_id = todbc_get_threadid();
  }

  gettimeofday(&tv, NULL);
  time_t tt = tv.tv_sec;
  localtime_r(&tt, &stm);

  char           buf[4096];
  size_t         bytes = sizeof(buf);

  char          *p = buf;
  int            n = 0;

  n = snprintf(p, bytes, "%C%02d:%02d:%02d.%06d[%" PRIx64 "]%s[%d]%s()",
                tag, stm.tm_hour, stm.tm_min, stm.tm_sec, (int)tv.tv_usec,
                thread_id, basename((char*)file), line, func);
  if (n>0) {
    bytes -= (size_t)n;
    p     += n;
  }

  if (bytes>0) {
    if (tag=='E' && err) {
      n = snprintf(p, bytes, "[%d]%s", err, strerror(err));
      if (n>0) {
        bytes -= (size_t)n;
        p     += n;
      }
    }
  }

  if (bytes>0) {
    n = snprintf(p, bytes, ": ");
    if (n>0) {
      bytes -= (size_t)n;
      p     += n;
    }
  }

  if (bytes>0) {
    va_list        ap;
    va_start(ap, fmt);
    n = vsnprintf(p, bytes, fmt, ap);
    va_end(ap);
  }

  fprintf(stderr, "%s\n", buf);
}

