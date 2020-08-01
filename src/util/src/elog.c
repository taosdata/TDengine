#include "elog.h"

#include <libgen.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#define gettid() syscall(__NR_gettid)

static ELOG_LEVEL         elog_level_base = ELOG_DEBUG;

static __thread long      elog_thread_id;
static __thread char      elog_thread_name[24] = {0};

void elog_set_level(ELOG_LEVEL base) {
  elog_level_base = base;
}

void elog_set_thread_name(const char *name) {
  elog_thread_id = gettid();
  snprintf(elog_thread_name, sizeof(elog_thread_name), "%s", name);
}

void elog(ELOG_LEVEL level, int fd, const char *file, int line, const char *func, const char *fmt, ...)
{
  if (level < elog_level_base) return;
  if (fd == -1) return;

  if (elog_thread_name[0]=='\0') {
    elog_set_thread_name("unknown");
  }

  char     *p;
  int       n;
  size_t    bytes;

  char buf[4096];
  snprintf(buf, sizeof(buf), "%s", file);

  char fn[1024];
  snprintf(fn, sizeof(fn), "%s", basename(buf));

  char      C;
  switch (level) {
    case ELOG_DEBUG:        C = 'D'; break;
    case ELOG_INFO:         C = 'I'; break;
    case ELOG_WARN:         C = 'W'; break;
    case ELOG_ERROR:        C = 'E'; break;
    case ELOG_CRITICAL:     C = 'C'; break;
    case ELOG_VERBOSE:      C = 'V'; break;
    case ELOG_ABORT:        C = 'A'; break;
    default:                return;
  }

  struct tm      t;
  struct timeval tv;

  if (gettimeofday(&tv, NULL)) return;
  if (!localtime_r(&tv.tv_sec, &t)) return;

  p     = buf;
  bytes = sizeof(buf);

  n = snprintf(p, bytes, "%c[%02d/%02d %02d:%02d:%02d.%06ld][%06ld]: ==",
               C,
               t.tm_mon + 1, t.tm_mday,
               t.tm_hour, t.tm_min, t.tm_sec,
               tv.tv_usec,
               elog_thread_id);
  p += n; bytes -= n;

  va_list arg;
  va_start(arg, fmt);
  if (bytes>0) {
    n = vsnprintf(p, bytes, fmt, arg);
    p += n; bytes -= n;
  }
  va_end(arg);

  if (bytes>0) {
    n = snprintf(p, bytes, "== t:%s#%s[%d]#%s()",
                 elog_thread_name, fn, line, func);
  }

  dprintf(fd, "%s\n", buf);

  if (level == ELOG_ABORT) {
    abort();
  }
}

