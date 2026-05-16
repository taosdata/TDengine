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

#ifndef _logger_h_
#define _logger_h_

#include "macros.h"

#ifdef _WIN32      /* { */
#include <stdio.h>
#endif             /* } */

typedef enum logger_level_e {
  LOGGER_VERBOSE,
  LOGGER_DEBUG,
  LOGGER_INFO,
  LOGGER_WARN,
  LOGGER_ERROR,
  LOGGER_FATAL,
} logger_level_t;

typedef void (*logger_f)(const char *log, void *ctx);

typedef struct logger_s            logger_t;

#ifdef __cplusplus         /* { */
#define ABORT_OR_THROW throw int(1)
#else                      /* }{ */
#include <stdlib.h>
#define ABORT_OR_THROW abort()
#endif                     /* } */

#define LOG_USE_COLOR           1
static inline const char* color_red(void)
{
  if (!LOG_USE_COLOR) return "";
  return "\033[1;31m";
}

static inline const char* color_green(void)
{
  if (!LOG_USE_COLOR) return "";
  return "\033[1;32m";
}

static inline const char* color_yellow(void)
{
  if (!LOG_USE_COLOR) return "";
  return "\033[1;33m";
}

static inline const char* color_reset(void)
{
  if (!LOG_USE_COLOR) return "";
  return "\033[0m";
}

#define D(_fmt, ...) TOD_LOGD(_fmt, ##__VA_ARGS__)
#define W(_fmt, ...) do {                                     \
  TOD_LOGW("%s" _fmt "%s",                                    \
      color_yellow(), ##__VA_ARGS__, color_reset());          \
} while (0)
#define I(_fmt, ...) TOD_LOGI(_fmt,  ##__VA_ARGS__)
#define E(_fmt, ...) do {                                     \
  TOD_LOGE("%s" _fmt "%s",                                    \
      color_red(), ##__VA_ARGS__, color_reset());             \
} while (0)
#define X(_fmt, ...) do {                                     \
  TOD_LOGI("%s" _fmt "%s",                                    \
      color_green(), ##__VA_ARGS__, color_reset());           \
} while (0)

#define A(_statement, _fmt, ...)                                        \
  do {                                                                  \
    if (!(_statement)) {                                                \
      TOD_LOGF("%sassertion failed%s:[%s]" _fmt "",                     \
          color_red(), color_reset(), #_statement, ##__VA_ARGS__);      \
      ABORT_OR_THROW;                                                   \
    }                                                                   \
  } while (0)

#define CHECK(_statement) do {                                          \
  D("%s ...", #_statement);                                             \
  if (_statement) {                                                     \
    E("%s => %sfailure%s", #_statement, color_red(), color_reset());    \
    return 1;                                                           \
  }                                                                     \
  D("%s => %ssuccess%s", #_statement, color_green(), color_reset());    \
} while (0)

#define LOG_CALL(fmt, ...)        D("" fmt " ...", ##__VA_ARGS__)
#define LOG_FINI(r, fmt, ...) do {                                             \
  if (r) {                                                                     \
    D("" fmt " => %sfailure%s", ##__VA_ARGS__, color_red(), color_reset());    \
  } else {                                                                     \
    D("" fmt " => %ssuccess%s", ##__VA_ARGS__, color_green(), color_reset());  \
  }                                                                            \
} while (0)

EXTERN_C_BEGIN

logger_level_t tod_get_system_logger_level(void) FA_HIDDEN;
logger_t* tod_get_system_logger(void) FA_HIDDEN;

// NOTE: do NOT call `tod_logger_write_impl` directly, call `tod_logger_write` instead!!!
void tod_logger_write_impl(logger_t *logger, logger_level_t request, logger_level_t level,
    const char *file, int line, const char *func,
    const char *fmt, ...) __attribute__ ((format (printf, 7, 8))) FA_HIDDEN;

#ifdef _WIN32               /* { */
#define tod_logger_write(logger, request, level, file, line, func, fmt, ...) \
  (0 ? fprintf(stderr, fmt, ##__VA_ARGS__) : tod_logger_write_impl(logger, request, level, file, line, func, fmt, ##__VA_ARGS__))
#else
#define tod_logger_write tod_logger_write_impl
#endif                      /* } */

#define TOD_LOGV(fmt, ...) do {                                                                   \
  tod_logger_write(tod_get_system_logger(), LOGGER_VERBOSE, tod_get_system_logger_level(),        \
    __FILE__, __LINE__, __func__,                                                                 \
    fmt, ##__VA_ARGS__);                                                                          \
} while (0)

#define TOD_LOGD(fmt, ...) do {                                                                   \
  tod_logger_write(tod_get_system_logger(), LOGGER_DEBUG, tod_get_system_logger_level(),          \
    __FILE__, __LINE__, __func__,                                                                 \
    fmt, ##__VA_ARGS__);                                                                          \
} while (0)

#define TOD_LOGI(fmt, ...) do {                                                                   \
  tod_logger_write(tod_get_system_logger(), LOGGER_INFO, tod_get_system_logger_level(),           \
    __FILE__, __LINE__, __func__,                                                                 \
    fmt, ##__VA_ARGS__);                                                                          \
} while (0)

#define TOD_LOGW(fmt, ...) do {                                                                   \
  tod_logger_write(tod_get_system_logger(), LOGGER_WARN, tod_get_system_logger_level(),           \
    __FILE__, __LINE__, __func__,                                                                 \
    fmt, ##__VA_ARGS__);                                                                          \
} while (0)

#define TOD_LOGE(fmt, ...) do {                                                                   \
  tod_logger_write(tod_get_system_logger(), LOGGER_ERROR, tod_get_system_logger_level(),          \
    __FILE__, __LINE__, __func__,                                                                 \
    fmt, ##__VA_ARGS__);                                                                          \
} while (0)

#define TOD_LOGF(fmt, ...) do {                                                                   \
  tod_logger_write(tod_get_system_logger(), LOGGER_FATAL, tod_get_system_logger_level(),          \
    __FILE__, __LINE__, __func__,                                                                 \
    fmt, ##__VA_ARGS__);                                                                          \
} while (0)

EXTERN_C_END

#endif // _logger_h_

