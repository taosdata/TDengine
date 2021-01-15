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

#ifndef TDENGINE_OS_DARWIN_H
#define TDENGINE_OS_DARWIN_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <assert.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <float.h>
#include <ifaddrs.h>
#include <libgen.h>
#include <limits.h>
#include <locale.h>
#include <math.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <pthread.h>
#include <pwd.h>
#include <regex.h>
#include <semaphore.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <sys/file.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <syslog.h>
#include <termios.h>
#include <unistd.h>
#include <wchar.h>
#include <wordexp.h>
#include <wctype.h>
#include <inttypes.h>
#include <dispatch/dispatch.h>
#include <fcntl.h>
#include <sys/utsname.h>
#include <math.h>

#define TAOS_OS_FUNC_FILE_SENDIFLE

#define TAOS_OS_FUNC_SEMPHONE
  #define tsem_t dispatch_semaphore_t
  int tsem_init(dispatch_semaphore_t *sem, int pshared, unsigned int value);
  int tsem_wait(dispatch_semaphore_t *sem);
  int tsem_post(dispatch_semaphore_t *sem);
  int tsem_destroy(dispatch_semaphore_t *sem);

#define TAOS_OS_FUNC_SOCKET_SETSOCKETOPT
#define TAOS_OS_FUNC_STRING_STR2INT64
#define TAOS_OS_FUNC_SYSINFO
#define TAOS_OS_FUNC_TIMER

// specific
#define htobe64 htonll
typedef int(*__compar_fn_t)(const void *, const void *);

// for send function in tsocket.c
// #define MSG_NOSIGNAL             0
#define SO_NO_CHECK              0x1234
#define SOL_TCP                  0x1234
#define TCP_KEEPIDLE             0x1234

#ifndef PTHREAD_MUTEX_RECURSIVE_NP
  #define  PTHREAD_MUTEX_RECURSIVE_NP PTHREAD_MUTEX_RECURSIVE
#endif

int64_t tsosStr2int64(char *str);

#include "eok.h"






#ifdef __cplusplus
}
#endif

#endif
