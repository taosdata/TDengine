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

#ifndef TDENGINE_PLATFORM_WINDOWS_H
#define TDENGINE_PLATFORM_WINDOWS_H

#include <io.h>
#include <stdio.h>
#include <signal.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <direct.h>
#include "winsock2.h"
#include <WS2tcpip.h>

#ifdef __cplusplus
extern "C" {
#endif

// for function open in stat.h 
#define S_IRWXU                  _S_IREAD
#define S_IRWXG                  _S_IWRITE
#define S_IRWXO                  _S_IWRITE

// for access function in io.h
#define F_OK 00  //Existence only
#define W_OK 02  //Write - only
#define R_OK 04  //Read - only
#define X_OK 06  //Read and write

// for send function in tsocket.c
#define MSG_NOSIGNAL             0
#define SO_NO_CHECK              0x1234
#define SOL_TCP                  0x1234
#define TCP_KEEPCNT              0x1234
#define TCP_KEEPIDLE             0x1234
#define TCP_KEEPINTVL            0x1234

#define LOCK_EX 1
#define LOCK_NB 2
#define LOCK_UN 3

#define bzero(ptr, size) memset((ptr), 0, (size))
#define mkdir(pathname, mode) _mkdir(pathname)
#define strcasecmp  _stricmp
#define strncasecmp _strnicmp
#define wcsncasecmp _wcsnicmp
#define strtok_r strtok_s
#define str2int64 _atoi64
#define snprintf _snprintf
#define in_addr_t unsigned long
#define socklen_t int
#define htobe64 htonll

int getline(char **lineptr, size_t *n, FILE *stream);

int taosWinSetTimer(int ms, void(*callback)(int));

int gettimeofday(struct timeval *tv, struct timezone *tz);

struct tm *localtime_r(const time_t *timep, struct tm *result);

char *strptime(const char *buf, const char *fmt, struct tm *tm);

bool taosCheckPthreadValid(pthread_t thread);

void taosResetPthread(pthread_t *thread);

int64_t taosGetPthreadId();

int taosSetNonblocking(int sock, int on);

int taosSetSockOpt(int socketfd, int level, int optname, void *optval, int optlen);

char *taosCharsetReplace(char *charsetstr);

void tsPrintOsInfo();

void taosGetSystemInfo();

void taosKillSystem();

//for signal, not dispose
#define SIGALRM 1234
typedef int sigset_t;

struct sigaction {
  void (*sa_handler)(int);
};

typedef struct {
  int we_wordc;
  char **we_wordv;
  int we_offs;
  char wordPos[20];
} wordexp_t;

int wordexp(const char *words, wordexp_t *pwordexp, int flags);

void wordfree(wordexp_t *pwordexp);

int flock(int fd, int option);

char *getpass(const char *prefix);

char *strsep(char **stringp, const char *delim);

typedef int(*__compar_fn_t)(const void *, const void *);

int sigaction(int, struct sigaction *, void *);

void sleep(int mseconds);

#ifdef __cplusplus
}
#endif
#endif