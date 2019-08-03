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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <stdint.h>
#include <locale.h>

#include "os.h"
#include "tlog.h"
#include "tsdb.h"
#include "tglobalcfg.h"

char configDir[TSDB_FILENAME_LEN] = "C:/TDengine/cfg";
char tsDirectory[TSDB_FILENAME_LEN] = "C:/TDengine/data";
char logDir[TSDB_FILENAME_LEN] = "C:/TDengine/log";
char scriptDir[TSDB_FILENAME_LEN] = "C:/TDengine/script";

bool taosCheckPthreadValid(pthread_t thread) {
  return thread.p != NULL;
}

void taosResetPthread(pthread_t *thread) {
  thread->p = 0;
}

int64_t taosGetPthreadId() {
  pthread_t id = pthread_self();
  return (int64_t)id.p;
}

int taosSetSockOpt(int socketfd, int level, int optname, void *optval, int optlen) {
  if (level == SOL_SOCKET && optname == SO_NO_CHECK) {
    return 0;
  }

  if (level == SOL_TCP && optname == TCP_KEEPCNT) {
    return 0;
  }

  if (level == SOL_TCP && optname == TCP_KEEPIDLE) {
    return 0;
  }

  if (level == SOL_TCP && optname == TCP_KEEPINTVL) {
    return 0;
  }

  return setsockopt(socketfd, level, optname, optval, optlen);
}

int32_t __sync_val_compare_and_swap_32(int32_t *ptr, int32_t oldval, int32_t newval) {
  return InterlockedCompareExchange(ptr, newval, oldval);
}

int32_t __sync_add_and_fetch_32(int32_t *ptr, int32_t val) {
  return InterlockedAdd(ptr, val);
}

int64_t __sync_val_compare_and_swap_64(int64_t *ptr, int64_t oldval, int64_t newval) {
  return InterlockedCompareExchange64(ptr, newval, oldval);
}

int64_t __sync_add_and_fetch_64(int64_t *ptr, int64_t val) {
  return InterlockedAdd64(ptr, val);
}

void tsPrintOsInfo() {}

char *taosCharsetReplace(char *charsetstr) {
  return charsetstr;
}

void taosGetSystemTimezone() {
  // get and set default timezone
  SGlobalConfig *cfg_timezone = tsGetConfigOption("timezone");
  if (cfg_timezone && cfg_timezone->cfgStatus < TSDB_CFG_CSTATUS_DEFAULT) {
    char *tz = getenv("TZ");
    if (tz == NULL || strlen(tz) == 0) {
      strcpy(tsTimezone, "not configured");
    }
    else {
      strcpy(tsTimezone, tz);
    }
    cfg_timezone->cfgStatus = TSDB_CFG_CSTATUS_DEFAULT;
    pPrint("timezone not configured, use default");
  }
}

void taosGetSystemLocale() {
  // get and set default locale
  SGlobalConfig *cfg_locale = tsGetConfigOption("locale");
  if (cfg_locale && cfg_locale->cfgStatus < TSDB_CFG_CSTATUS_DEFAULT) {
    char *locale = setlocale(LC_CTYPE, "chs");
    if (locale != NULL) {
      strncpy(tsLocale, locale, sizeof(tsLocale) / sizeof(tsLocale[0]));
      cfg_locale->cfgStatus = TSDB_CFG_CSTATUS_DEFAULT;
      pPrint("locale not configured, set to default:%s", tsLocale);
    }
  }

  SGlobalConfig *cfg_charset = tsGetConfigOption("charset");
  if (cfg_charset && cfg_charset->cfgStatus < TSDB_CFG_CSTATUS_DEFAULT) {
    strcpy(tsCharset, "cp936");
    cfg_charset->cfgStatus = TSDB_CFG_CSTATUS_DEFAULT;
    pPrint("charset not configured, set to default:%s", tsCharset);
  }
}

void taosGetSystemInfo() {
  taosGetSystemTimezone();
  taosGetSystemLocale();
}

void taosKillSystem() {
  exit(0);
}

/*
 * Get next token from string *stringp, where tokens are possibly-empty
 * strings separated by characters from delim.
 *
 * Writes NULs into the string at *stringp to end tokens.
 * delim need not remain constant from call to call.
 * On return, *stringp points past the last NUL written (if there might
 * be further tokens), or is NULL (if there are definitely no moretokens).
 *
 * If *stringp is NULL, strsep returns NULL.
 */
char *strsep(char **stringp, const char *delim) {
  char *s;
  const char *spanp;
  int c, sc;
  char *tok;
  if ((s = *stringp) == NULL)
    return (NULL);
  for (tok = s;;) {
    c = *s++;
    spanp = delim;
    do {
      if ((sc = *spanp++) == c) {
        if (c == 0)
          s = NULL;
        else
          s[-1] = 0;
        *stringp = s;
        return (tok);
      }
    } while (sc != 0);
  }
  /* NOTREACHED */
}

char *getpass(const char *prefix) {
  static char passwd[TSDB_KEY_LEN] = {0};

  printf("%s", prefix);
  scanf("%s", passwd);

  char n = getchar();
  return passwd;
}

int flock(int fd, int option) {
  return 0;
}

int sigaction(int sig, struct sigaction *d, void *p) {
  return 0;
}

int wordexp(const char *words, wordexp_t *pwordexp, int flags) {
  pwordexp->we_offs = 0;
  pwordexp->we_wordc = 1;
  pwordexp->we_wordv = (char **)(pwordexp->wordPos);
  pwordexp->we_wordv[0] = (char *)words;
  return 0;
}

void wordfree(wordexp_t *pwordexp) {}

