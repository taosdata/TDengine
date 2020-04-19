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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taosdef.h"
#include "tglobal.h"
#include "ttimer.h"
#include "tulog.h"
#include "tutil.h"

bool taosCheckPthreadValid(pthread_t thread) {
  return thread.p != NULL;
}

void taosResetPthread(pthread_t *thread) {
  thread->p = 0;
}

int64_t taosGetPthreadId() {
#ifdef PTW32_VERSION
  return pthread_getw32threadid_np(pthread_self());
#else
  return (int64_t)pthread_self();
#endif
}

int taosSetSockOpt(int socketfd, int level, int optname, void *optval, int optlen) {
  if (level == SOL_SOCKET && optname == TCP_KEEPCNT) {
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

// add
char interlocked_add_fetch_8(char volatile* ptr, char val) {
  #ifdef _TD_GO_DLL_
    return __sync_fetch_and_add(ptr, val) + val;
  #else
    return _InterlockedExchangeAdd8(ptr, val) + val;
  #endif
}

short interlocked_add_fetch_16(short volatile* ptr, short val) {
  #ifdef _TD_GO_DLL_
    return __sync_fetch_and_add(ptr, val) + val;
  #else
    return _InterlockedExchangeAdd16(ptr, val) + val;
  #endif
}

long interlocked_add_fetch_32(long volatile* ptr, long val) {
  return _InterlockedExchangeAdd(ptr, val) + val;
}

__int64 interlocked_add_fetch_64(__int64 volatile* ptr, __int64 val) {
  return _InterlockedExchangeAdd64(ptr, val) + val;
}

// and
#ifndef _TD_GO_DLL_
char interlocked_and_fetch_8(char volatile* ptr, char val) {
  return _InterlockedAnd8(ptr, val) & val;
}

short interlocked_and_fetch_16(short volatile* ptr, short val) {
  return _InterlockedAnd16(ptr, val) & val;
}
#endif

long interlocked_and_fetch_32(long volatile* ptr, long val) {
  return _InterlockedAnd(ptr, val) & val;
}

#ifndef _M_IX86

__int64 interlocked_and_fetch_64(__int64 volatile* ptr, __int64 val) {
  return _InterlockedAnd64(ptr, val) & val;
}

#else

__int64 interlocked_and_fetch_64(__int64 volatile* ptr, __int64 val) {
  __int64 old, res;
  do {
    old = *ptr;
    res = old & val;
  } while(_InterlockedCompareExchange64(ptr, res, old) != old);
  return res;
}

__int64 interlocked_fetch_and_64(__int64 volatile* ptr, __int64 val) {
  __int64 old;
  do {
    old = *ptr;
  } while(_InterlockedCompareExchange64(ptr, old & val, old) != old);
  return old;
}

#endif

// or
#ifndef _TD_GO_DLL_
char interlocked_or_fetch_8(char volatile* ptr, char val) {
  return _InterlockedOr8(ptr, val) | val;
}

short interlocked_or_fetch_16(short volatile* ptr, short val) {
  return _InterlockedOr16(ptr, val) | val;
}
#endif
long interlocked_or_fetch_32(long volatile* ptr, long val) {
  return _InterlockedOr(ptr, val) | val;
}

#ifndef _M_IX86

__int64 interlocked_or_fetch_64(__int64 volatile* ptr, __int64 val) {
  return _InterlockedOr64(ptr, val) & val;
}

#else

__int64 interlocked_or_fetch_64(__int64 volatile* ptr, __int64 val) {
  __int64 old, res;
  do {
    old = *ptr;
    res = old | val;
  } while(_InterlockedCompareExchange64(ptr, res, old) != old);
  return res;
}

__int64 interlocked_fetch_or_64(__int64 volatile* ptr, __int64 val) {
  __int64 old;
  do {
    old = *ptr;
  } while(_InterlockedCompareExchange64(ptr, old | val, old) != old);
  return old;
}

#endif

// xor
#ifndef _TD_GO_DLL_
char interlocked_xor_fetch_8(char volatile* ptr, char val) {
  return _InterlockedXor8(ptr, val) ^ val;
}

short interlocked_xor_fetch_16(short volatile* ptr, short val) {
  return _InterlockedXor16(ptr, val) ^ val;
}
#endif
long interlocked_xor_fetch_32(long volatile* ptr, long val) {
  return _InterlockedXor(ptr, val) ^ val;
}

#ifndef _M_IX86

__int64 interlocked_xor_fetch_64(__int64 volatile* ptr, __int64 val) {
  return _InterlockedXor64(ptr, val) ^ val;
}

#else

__int64 interlocked_xor_fetch_64(__int64 volatile* ptr, __int64 val) {
  __int64 old, res;
  do {
    old = *ptr;
    res = old ^ val;
  } while(_InterlockedCompareExchange64(ptr, res, old) != old);
  return res;
}

__int64 interlocked_fetch_xor_64(__int64 volatile* ptr, __int64 val) {
  __int64 old;
  do {
    old = *ptr;
  } while(_InterlockedCompareExchange64(ptr, old ^ val, old) != old);
  return old;
}

#endif

void taosPrintOsInfo() {}

void taosGetSystemTimezone() {
  // get and set default timezone
  SGlobalCfg *cfg_timezone = taosGetConfigOption("timezone");
  if (cfg_timezone && cfg_timezone->cfgStatus < TAOS_CFG_CSTATUS_DEFAULT) {
    char *tz = getenv("TZ");
    if (tz == NULL || strlen(tz) == 0) {
      strcpy(tsTimezone, "not configured");
    }
    else {
      strcpy(tsTimezone, tz);
    }
    cfg_timezone->cfgStatus = TAOS_CFG_CSTATUS_DEFAULT;
    uPrint("timezone not configured, use default");
  }
}

void taosGetSystemLocale() {
  // get and set default locale
  SGlobalCfg *cfg_locale = taosGetConfigOption("locale");
  if (cfg_locale && cfg_locale->cfgStatus < TAOS_CFG_CSTATUS_DEFAULT) {
    char *locale = setlocale(LC_CTYPE, "chs");
    if (locale != NULL) {
      strncpy(tsLocale, locale, sizeof(tsLocale) / sizeof(tsLocale[0]));
      cfg_locale->cfgStatus = TAOS_CFG_CSTATUS_DEFAULT;
      uPrint("locale not configured, set to default:%s", tsLocale);
    }
  }

  SGlobalCfg *cfg_charset = taosGetConfigOption("charset");
  if (cfg_charset && cfg_charset->cfgStatus < TAOS_CFG_CSTATUS_DEFAULT) {
    strcpy(tsCharset, "cp936");
    cfg_charset->cfgStatus = TAOS_CFG_CSTATUS_DEFAULT;
    uPrint("charset not configured, set to default:%s", tsCharset);
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

int fsync(int filedes) {
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

void taosGetDisk() {}

bool taosSkipSocketCheck() {
  return false;
}

#define _SEND_FILE_STEP_ 1000

int fsendfile(FILE* out_file, FILE* in_file, int64_t* offset, int32_t count) {
  fseek(in_file, (int32_t)(*offset), 0);
  int writeLen = 0;
  uint8_t buffer[_SEND_FILE_STEP_] = { 0 };
  
  for (int len = 0; len < (count - _SEND_FILE_STEP_); len += _SEND_FILE_STEP_) {
    size_t rlen = fread(buffer, 1, _SEND_FILE_STEP_, in_file);
    if (rlen <= 0) {
      return writeLen;
    }
    else if (rlen < _SEND_FILE_STEP_) {
      fwrite(buffer, 1, rlen, out_file);
      return (int)(writeLen + rlen);
    }
    else {
      fwrite(buffer, 1, _SEND_FILE_STEP_, in_file);
      writeLen += _SEND_FILE_STEP_;
    }
  }

  int remain = count - writeLen;
  if (remain > 0) {
    size_t rlen = fread(buffer, 1, remain, in_file);
    if (rlen <= 0) {
      return writeLen;
    }
    else {
      fwrite(buffer, 1, remain, out_file);
      writeLen += remain;
    }
  }

  return writeLen;
}

int32_t BUILDIN_CLZL(uint64_t val) {
  unsigned long r = 0;
  _BitScanReverse64(&r, val);
  return (int)(r >> 3);
}

int32_t BUILDIN_CLZ(uint32_t val) {
  unsigned long r = 0;
  _BitScanReverse(&r, val);
  return (int)(r >> 3);
}

int32_t BUILDIN_CTZL(uint64_t val) {
  unsigned long r = 0;
  _BitScanForward64(&r, val);
  return (int)(r >> 3);
}

int32_t BUILDIN_CTZ(uint32_t val) {
  unsigned long r = 0;
  _BitScanForward(&r, val);
  return (int)(r >> 3);
}

char *strndup(const char *s, size_t n) {
  int len = strlen(s);
  if (len >= n) {
    len = n;
  }

  char *r = calloc(len + 1, 1);
  memcpy(r, s, len);
  r[len] = 0;
  return r;
}

void taosSetCoreDump() {}

#ifdef _TD_GO_DLL_
int64_t str2int64(char *str) {
  char *endptr = NULL;
  return strtoll(str, &endptr, 10);
}

uint64_t htonll(uint64_t val)
{
    return (((uint64_t) htonl(val)) << 32) + htonl(val >> 32);
}
#endif