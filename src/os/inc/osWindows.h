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

#ifndef TDENGINE_OS_WINDOWS_H
#define TDENGINE_OS_WINDOWS_H

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <locale.h>
#include <intrin.h>
#include <io.h>
#include <math.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include "winsock2.h"
#include <WS2tcpip.h>
#include <winbase.h>
#include <Winsock2.h>
#include <time.h>
#include <inttypes.h>
#include <conio.h>
#include <math.h>
#include "msvcProcess.h"
#include "msvcDirect.h"
#include "msvcFcntl.h"
#include "msvcLibgen.h"
#include "msvcStdio.h"
#include "msvcUnistd.h"
#include "msvcLibgen.h"
#include "sys/msvcStat.h"
#include "sys/msvcTypes.h"

#ifdef __cplusplus
extern "C" {
#endif

char *stpcpy (char *dest, const char *src);
char *stpncpy (char *dest, const char *src, size_t n);

// specific
typedef int (*__compar_fn_t)(const void *, const void *);
#define ssize_t int
#define bzero(ptr, size) memset((ptr), 0, (size))
#define strcasecmp  _stricmp
#define strncasecmp _strnicmp
#define wcsncasecmp _wcsnicmp
#define strtok_r strtok_s
#define snprintf _snprintf
#define in_addr_t unsigned long
#define socklen_t int

struct tm *localtime_r(const time_t *timep, struct tm *result);
char *     strptime(const char *buf, const char *fmt, struct tm *tm);
char *     strsep(char **stringp, const char *delim);
char *     getpass(const char *prefix);
int        flock(int fd, int option);
char *     strndup(const char *s, size_t n);
int        gettimeofday(struct timeval *ptv, void *pTimeZone);

// for send function in tsocket.c
#define MSG_NOSIGNAL             0
#define SO_NO_CHECK              0x1234
#define SOL_TCP                  0x1234

#ifndef TCP_KEEPCNT
  #define TCP_KEEPCNT              0x1234
#endif

#ifndef TCP_KEEPIDLE
  #define TCP_KEEPIDLE             0x1234
#endif

#ifndef TCP_KEEPINTVL
  #define TCP_KEEPINTVL            0x1234
#endif

#define SHUT_RDWR                SD_BOTH
#define SHUT_RD                  SD_RECEIVE
#define SHUT_WR                  SD_SEND

#define LOCK_EX 1
#define LOCK_NB 2
#define LOCK_UN 3

#ifndef PATH_MAX
  #define PATH_MAX 256
#endif

typedef struct {
  int    we_wordc;
  char  *we_wordv[1];
  int    we_offs;
  char   wordPos[1025];
} wordexp_t;
int  wordexp(char *words, wordexp_t *pwordexp, int flags);
void wordfree(wordexp_t *pwordexp);

#define openlog(a, b, c)
#define closelog()
#define LOG_ERR 0
#define LOG_INFO 1
void syslog(int unused, const char *format, ...);

#ifdef __cplusplus
}
#endif
#endif
