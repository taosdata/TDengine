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

#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <syslog.h>
#include <time.h>

#if defined(__APPLE__)
#include <sys/time.h>
#endif

#ifdef WITH_DLT
#include <dlt/dlt.h>
#include <sys/stat.h>
#endif

#include "ttqLogging.h"
#include "ttqMemory.h"
#include "ttqMisc.h"
#include "tmqttBrokerInt.h"
#include "ttqUtil.h"

static char log_fptr_buffer[BUFSIZ];

/* Options for logging should be:
 *
 * A combination of:
 * Via syslog
 * To a file
 * To stdout/stderr
 * To topics
 */

/* Give option of logging timestamp.
 * Logging pid.
 */
static unsigned int log_destinations = MQTT3_LOG_STDERR;
static unsigned int log_priorities = TTQ_LOG_ERR | TTQ_LOG_WARNING | TTQ_LOG_NOTICE | TTQ_LOG_INFO;

#ifdef WITH_DLT
static DltContext dltContext;
static bool       dlt_allowed = false;

void dlt_fifo_check(void) {
  struct stat statbuf;
  int         fd;

  /* If we start DLT but the /tmp/dlt fifo doesn't exist, or isn't available
   * for writing then there is a big delay when we try and close the log
   * later, so check for it first. This has the side effect of not letting
   * people using DLT create the fifo after Mosquitto has started, but at the
   * benefit of not having a massive delay for everybody else. */
  memset(&statbuf, 0, sizeof(statbuf));
  if (stat("/tmp/dlt", &statbuf) == 0) {
    if (S_ISFIFO(statbuf.st_mode)) {
      fd = open("/tmp/dlt", O_NONBLOCK | O_WRONLY);
      if (fd != -1) {
        dlt_allowed = true;
        close(fd);
      }
    }
  }
}
#endif

static int get_time(struct tm **ti) {
  time_t s;

  s = db.now_real_s;

  *ti = localtime(&s);
  if (!(*ti)) {
    fprintf(stderr, "Error obtaining system time.\n");
    return 1;
  }

  return 0;
}

int ttqLogInit(struct tmqtt__config *config) {
  int rc = 0;

  log_priorities = config->log_type;
  log_destinations = config->log_dest;

  if (log_destinations & MQTT3_LOG_SYSLOG) {
    openlog("tmqtt", LOG_PID | LOG_CONS, config->log_facility);
  }

  if (log_destinations & MQTT3_LOG_FILE) {
    config->log_fptr = tmqtt__fopen(config->log_file, "at", true);
    if (config->log_fptr) {
      setvbuf(config->log_fptr, log_fptr_buffer, _IOLBF, sizeof(log_fptr_buffer));
    } else {
      log_destinations = MQTT3_LOG_STDERR;
      log_priorities = TTQ_LOG_ERR;
      ttq_log(NULL, TTQ_LOG_ERR, "Error: Unable to open log file %s for writing.", config->log_file);
    }
  }
  if (log_destinations & MQTT3_LOG_STDOUT) {
    setvbuf(stdout, NULL, _IOLBF, 0);
  }
#ifdef WITH_DLT
  if (log_destinations & MQTT3_LOG_DLT) {
    dlt_fifo_check();
    if (dlt_allowed) {
      DLT_REGISTER_APP("MQTT", "tmqtt log");
      dlt_register_context(&dltContext, "MQTT", "tmqtt DLT context");
    }
  }
#endif
  return rc;
}

int ttqLogClose(struct tmqtt__config *config) {
  if (log_destinations & MQTT3_LOG_SYSLOG) {
    closelog();
  }
  if (log_destinations & MQTT3_LOG_FILE) {
    if (config->log_fptr) {
      fclose(config->log_fptr);
      config->log_fptr = NULL;
    }
  }

#ifdef WITH_DLT
  if (dlt_allowed) {
    dlt_unregister_context(&dltContext);
    DLT_UNREGISTER_APP();
  }
#endif
  /* FIXME - do something for all destinations! */
  return TTQ_ERR_SUCCESS;
}

#ifdef WITH_DLT
DltLogLevelType get_dlt_level(unsigned int priority) {
  switch (priority) {
    case TTQ_LOG_ERR:
      return DLT_LOG_ERROR;
    case TTQ_LOG_WARNING:
      return DLT_LOG_WARN;
    case TTQ_LOG_INFO:
      return DLT_LOG_INFO;
    case TTQ_LOG_DEBUG:
      return DLT_LOG_DEBUG;
    case TTQ_LOG_NOTICE:
    case TTQ_LOG_SUBSCRIBE:
    case TTQ_LOG_UNSUBSCRIBE:
      return DLT_LOG_VERBOSE;
    default:
      return DLT_LOG_DEFAULT;
  }
}
#endif

#include "bnode.h"

static ELogLevel get_xnd_level(unsigned int priority) {
  switch (priority) {
    case TTQ_LOG_ERR:
      return DEBUG_ERROR;
    case TTQ_LOG_WARNING:
      return DEBUG_WARN;
    case TTQ_LOG_INFO:
      return DEBUG_INFO;
    case TTQ_LOG_DEBUG:
      return DEBUG_DEBUG;
    case TTQ_LOG_NOTICE:
    case TTQ_LOG_SUBSCRIBE:
    case TTQ_LOG_UNSUBSCRIBE:
      return DEBUG_TRACE;
    default:
      return DEBUG_DEBUG;
  }
}

static int log__vprintf(unsigned int priority, const char *fmt, va_list va) {
  const char *topic;
  int         syslog_priority;
  char        log_line[1000];
  size_t      log_line_pos;
  bool        log_timestamp = true;
  char       *log_timestamp_format = NULL;
  FILE       *log_fptr = NULL;

  if (db.config) {
    log_timestamp = db.config->log_timestamp;
    log_timestamp_format = db.config->log_timestamp_format;
    log_fptr = db.config->log_fptr;
  }

  if ((log_priorities & priority) && log_destinations != MQTT3_LOG_NONE) {
    switch (priority) {
      case TTQ_LOG_SUBSCRIBE:
        topic = "$SYS/broker/log/M/subscribe";
        syslog_priority = LOG_NOTICE;
        break;
      case TTQ_LOG_UNSUBSCRIBE:
        topic = "$SYS/broker/log/M/unsubscribe";
        syslog_priority = LOG_NOTICE;
        break;
      case TTQ_LOG_DEBUG:
        topic = "$SYS/broker/log/D";

        syslog_priority = LOG_DEBUG;
        break;
      case TTQ_LOG_ERR:
        topic = "$SYS/broker/log/E";

        syslog_priority = LOG_ERR;
        break;
      case TTQ_LOG_WARNING:
        topic = "$SYS/broker/log/W";
        syslog_priority = LOG_WARNING;
        break;
      case TTQ_LOG_NOTICE:
        topic = "$SYS/broker/log/N";

        syslog_priority = LOG_NOTICE;
        break;
      case TTQ_LOG_INFO:
        topic = "$SYS/broker/log/I";
        syslog_priority = LOG_INFO;
        break;
#ifdef WITH_WEBSOCKETS
      case TTQ_LOG_WEBSOCKETS:
        topic = "$SYS/broker/log/WS";
        syslog_priority = LOG_DEBUG;
        break;
#endif
      default:
        topic = "$SYS/broker/log/E";
        syslog_priority = LOG_ERR;
    }
    if (log_timestamp) {
      if (log_timestamp_format) {
        struct tm *ti = NULL;
        get_time(&ti);
        log_line_pos = strftime(log_line, sizeof(log_line), log_timestamp_format, ti);
        if (log_line_pos == 0) {
          log_line_pos = (size_t)snprintf(log_line, sizeof(log_line), "Time error");
        }
      } else {
        log_line_pos = (size_t)snprintf(log_line, sizeof(log_line), "%" PRIu64, (uint64_t)db.now_real_s);
      }
      if (log_line_pos < sizeof(log_line) - 3) {
        log_line[log_line_pos] = ':';
        log_line[log_line_pos + 1] = ' ';
        log_line[log_line_pos + 2] = '\0';
        log_line_pos += 2;
      }
    } else {
      log_line_pos = 0;
    }
    vsnprintf(&log_line[log_line_pos], sizeof(log_line) - log_line_pos, fmt, va);
    log_line[sizeof(log_line) - 1] = '\0'; /* Ensure string is null terminated. */

    if (log_destinations & MQTT3_LOG_STDOUT) {
      fprintf(stdout, "%s\n", log_line);
    }
    if (log_destinations & MQTT3_LOG_STDERR) {
      fprintf(stderr, "%s\n", log_line);
    }
    if (log_destinations & MQTT3_LOG_FILE && log_fptr) {
      fprintf(log_fptr, "%s\n", log_line);
    }
    if (log_destinations & MQTT3_LOG_SYSLOG) {
      syslog(syslog_priority, "%s", log_line);
    }
    if (log_destinations & MQTT3_LOG_TOPIC && priority != TTQ_LOG_DEBUG && priority != TTQ_LOG_INTERNAL) {
      // ttqDbMessageEasyQueue(NULL, topic, 2, (uint32_t)strlen(log_line), log_line, 0, 20, NULL);
    }
#ifdef WITH_DLT
    if (log_destinations & MQTT3_LOG_DLT && priority != TTQ_LOG_INTERNAL) {
      DLT_LOG_STRING(dltContext, get_dlt_level(priority), log_line);
    }
#endif
    switch (get_xnd_level(priority)) {
      case DEBUG_ERROR:
        bndError("%s", log_line);
        break;
      case DEBUG_INFO:
        bndInfo("%s", log_line);
        break;
      case DEBUG_TRACE:
        bndTrace("%s", log_line);
        break;
      case DEBUG_DEBUG:
      default:
        bndDebug("%s", log_line);
        break;
    }
  }

  return TTQ_ERR_SUCCESS;
}

int ttq_log(struct tmqtt *ttq, unsigned int priority, const char *fmt, ...) {
  va_list va;
  int     rc;

  UNUSED(ttq);

  va_start(va, fmt);
  rc = log__vprintf(priority, fmt, va);
  va_end(va);

  return rc;
}

void ttqLogInternal(const char *fmt, ...) {
  va_list va;
  char    buf[200];
  int     len;

  va_start(va, fmt);
  len = vsnprintf(buf, 200, fmt, va);
  va_end(va);

  if (len >= 200) {
    ttq_log(NULL, TTQ_LOG_INTERNAL, "Internal log buffer too short (%d)", len);
    return;
  }

  ttq_log(NULL, TTQ_LOG_INTERNAL, "%s%s%s", "\e[32m", buf, "\e[0m");
}

int tmqtt_log_vprintf(int level, const char *fmt, va_list va) { return log__vprintf((unsigned int)level, fmt, va); }

void tmqttLog(int level, const char *fmt, ...) {
  va_list va;

  va_start(va, fmt);
  log__vprintf((unsigned int)level, fmt, va);
  va_end(va);
}
