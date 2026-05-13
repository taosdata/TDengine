/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "logger.h"

#include "helpers.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#ifndef _WIN32           /* { */
#include <syslog.h>
#include <unistd.h>
#include <sys/syscall.h>
#endif                   /* } */

uintptr_t tod_get_current_thread_id(void)
{
#ifdef _WIN32             /* { */
  DWORD tid = GetCurrentThreadId();
  return tid;
#elif defined(__APPLE__)  /* }{ */
  uint64_t tid = (uint64_t)-1;
  pthread_threadid_np(pthread_self(), &tid);
  return tid;
#else                     /* }{ */
  pid_t tid = syscall(__NR_gettid);
  return tid;
#endif                    /* } */
}

uintptr_t tod_get_current_process_id(void)
{
#ifdef _WIN32             /* { */
  DWORD tid = GetProcessId(GetCurrentProcess());
  return tid;
#else                     /* }{ */
  pid_t pid = getpid();
  return pid;
#endif                    /* } */
}

static char logger_level_char(logger_level_t level)
{
  switch (level) {
    case LOGGER_VERBOSE:       return 'V';
    case LOGGER_DEBUG:         return 'D';
    case LOGGER_INFO:          return 'I';
    case LOGGER_WARN:          return 'W';
    case LOGGER_ERROR:         return 'E';
    case LOGGER_FATAL:         return 'F';
    default:                   return 'V';
  }
}

#ifdef _WIN32                /* { */
#define TEMP_MAX_PATH         MAX_PATH
#define DEFAULT_TEMP_PATH     "C:\\Windows\\Temp"
#else                        /* }{ */
#define TEMP_MAX_PATH         PATH_MAX
#define DEFAULT_TEMP_PATH     "/tmp"
#endif                       /* } */

typedef struct logger_temp_s               logger_temp_t;
struct logger_temp_s {
  FILE                  *file;
};

struct logger_s {
  logger_level_t            level;
  void (*logger)(const char *log);
  union {
    logger_temp_t           temp;
  };
};

static logger_t         _system_logger;

static void logger_to_temp(const char *log);

static void _exit_routine(void)
{
  if (_system_logger.logger == logger_to_temp) {
    logger_temp_t *temp = &_system_logger.temp;
    if (temp->file) {
      fclose(temp->file);
      temp->file = NULL;
    }
  }
}

static void _init_system_logger_level(void)
{
  const char *env = getenv("TAOS_ODBC_LOG_LEVEL");
  if (!env) {
    fprintf(stderr,
        "environment variable `TAOS_ODBC_LOG_LEVEL` could be set as `VERBOSE/DEBUG/INFO/WARN/ERROR/FATAL`, but not set. system logger level fall back to `ERROR`\n");
    return;
  }
#define RECORD(x) {#x, LOGGER_##x}
  struct {
    const char             *name;
    int                     level;
  } _levels[] = {
    RECORD(VERBOSE),
    RECORD(DEBUG),
    RECORD(INFO),
    RECORD(WARN),
    RECORD(ERROR),
    RECORD(FATAL),
  };
#undef RECORD
  for (size_t i=0; i<sizeof(_levels)/sizeof(_levels[0]); ++i) {
    if (0 == tod_strcasecmp(env, _levels[i].name)) {
      _system_logger.level = _levels[i].level;
      return;
    }
  }
  fprintf(stderr,
      "environment variable `TAOS_ODBC_LOG_LEVEL` shall be set as `VERBOSE/DEBUG/INFO/WARN/ERROR/FATAL`, but got `%s`. system logger level fall back to `ERROR`\n",
      env);
}

static void logger_to_temp(const char *log)
{
  FILE *file = _system_logger.temp.file;
  if (file) {
    fprintf(file, "%s\n", log);
    fflush(file);
  }
}

#ifdef _WIN32              /* { */
static void logger_to_event(const char *log)
{
  (void)log;
  // TODO:
}
#elif !defined(__APPLE__)  /* }{ */
static void logger_to_syslog(const char *log)
{
  int priority = LOG_SYSLOG | LOG_USER | LOG_INFO;
  syslog(priority, "%s", log);
}
#endif                     /* } */

static void _init_stderr(void)
{
}

static void _init_temp(void)
{
  const char *temp = getenv("TEMP");
  if (!temp) temp = DEFAULT_TEMP_PATH;
  char _temp_file[TEMP_MAX_PATH+1];
  snprintf(_temp_file, sizeof(_temp_file), "%s/taos_odbc.log", temp);
  _system_logger.logger    = logger_to_temp;
  _system_logger.temp.file = fopen(_temp_file, "a");
  // setvbuf(_system_logger.temp.file, NULL, _IOLBF, 0);
}

#ifdef _WIN32              /* { */
static void _init_event(void)
{
  _system_logger.logger   = logger_to_event;
}
#elif defined(__APPLE__)   /* }{ */
#else                      /* }{ */
static void _init_syslog(void)
{
  _system_logger.logger   = logger_to_syslog;
}
#endif                     /* } */

static void _init_system_logger(void)
{
  struct {
    const char *name;
    void  (*init)(void);
  } cfgs[] = {
    { "stderr", _init_stderr },
    { "temp",   _init_temp },
#ifdef _WIN32              /* { */
    { "event",  _init_event },
#elif defined(__APPLE__)   /* }{ */
#else                      /* }{ */
    { "syslog", _init_syslog },
#endif                     /* } */
  };
  size_t nr_cfgs = sizeof(cfgs)/sizeof(cfgs[0]);

  const char *env = getenv("TAOS_ODBC_LOGGER");
  if (!env) {
    fprintf(stderr, "environment variable `TAOS_ODBC_LOGGER` could be set as `");
    for (size_t i=0; i<nr_cfgs; ++i) {
      if (i) fprintf(stderr, "/%s", cfgs[i].name);
      else   fprintf(stderr, "%s", cfgs[i].name);
    }
    fprintf(stderr, "`, but not set. system logger level fall back to `stderr`\n");
    return;
  }

  for (size_t i=0; i<nr_cfgs; ++i) {
    if (tod_strcasecmp(env, cfgs[i].name)) continue;
    cfgs[i].init();
    return;
  }

  fprintf(stderr, "environment variable `TAOS_ODBC_LOGGER` could be set as `");
  for (size_t i=0; i<nr_cfgs; ++i) {
    if (i) fprintf(stderr, "/%s", cfgs[i].name);
    else   fprintf(stderr, "%s", cfgs[i].name);
  }
  fprintf(stderr, "`, but got `%s`. system logger level fall back to `stderr`\n", env);
  return;
}

static void _init_all(void)
{
  atexit(_exit_routine);
  memset(&_system_logger, 0, sizeof(_system_logger));
  _system_logger.level = LOGGER_ERROR;
  _init_system_logger_level();
  _init_system_logger();
}

static void _init_all_once(void)
{
  static int init = 0;
  if (!init) {
    static pthread_once_t once;
    pthread_once(&once, _init_all);
    init = 1;
  }
}

logger_level_t tod_get_system_logger_level(void)
{
  _init_all_once();
  return _system_logger.level;
}

logger_t* tod_get_system_logger(void)
{
  _init_all_once();
  return &_system_logger;
}

void tod_logger_write_impl(logger_t *logger, logger_level_t request, logger_level_t level,
    const char *file, int line, const char *func,
    const char *fmt, ...)
{
  if (request < level) return;

  char filename[1024]; filename[0] = '\0';
  const char *fn = tod_basename(file, filename, sizeof(filename));

  char buf[1024]; buf[0] = '\0';
  char *p = buf;
  size_t l = sizeof(buf);
  int n;

  double tick;

#ifdef _WIN32               /* { */
  LARGE_INTEGER ticks = {0}, freq = {0};
  QueryPerformanceCounter(&ticks);
  QueryPerformanceFrequency(&freq);
  tick = (double)ticks.QuadPart/freq.QuadPart;
#else                       /* }{ */
  struct timespec ts = {0};

  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  tick = ts.tv_sec + (double)ts.tv_nsec / 1000000000;
#endif                      /* } */

  n = snprintf(p, l, "%c:%.3fs:%zx:%s[%d]:%s():", logger_level_char(request), tick, tod_get_current_thread_id(), fn, line, func);
  p += n;
  l -= n;

  va_list ap;
  va_start(ap, fmt);
  vsnprintf(p, l, fmt, ap);
  va_end(ap);

  fprintf(stderr, "%s\n", buf);
  if (logger && logger->logger) {
    logger->logger(buf);
  }
}
