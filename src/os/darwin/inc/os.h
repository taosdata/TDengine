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


#ifndef TDENGINE_PLATFORM_DARWIN_H
#define TDENGINE_PLATFORM_DARWIN_H

#include <ifaddrs.h>
#include <netdb.h>
#include <pwd.h>
#include <syslog.h>
#include <termios.h>
#include <wordexp.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <sys/file.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <stdint.h>
#include <pthread.h>
#include <stdbool.h>
#include <limits.h>
#include <locale.h>
#include <dispatch/dispatch.h>

#define htobe64 htonll

#define taosCloseSocket(x) \
  {                        \
    if (VALIDFD(x)) {      \
      close(x);            \
      x = -1;              \
    }                      \
  }
#define taosWriteSocket(fd, buf, len) write(fd, buf, len)
#define taosReadSocket(fd, buf, len) read(fd, buf, len)

#define __sync_val_compare_and_swap_64 __sync_val_compare_and_swap
#define __sync_val_compare_and_swap_32 __sync_val_compare_and_swap
#define __sync_add_and_fetch_64 __sync_add_and_fetch
#define __sync_add_and_fetch_32 __sync_add_and_fetch
int32_t __sync_val_load_32(int32_t *ptr);
void __sync_val_restore_32(int32_t *ptr, int32_t newval);

#define SWAP(a, b, c)      \
  do {                     \
    typeof(a) __tmp = (a); \
    (a) = (b);             \
    (b) = __tmp;           \
  } while (0)

#define MAX(a, b)            \
  ({                         \
    typeof(a) __a = (a);     \
    typeof(b) __b = (b);     \
    (__a > __b) ? __a : __b; \
  })

#define MIN(a, b)            \
  ({                         \
    typeof(a) __a = (a);     \
    typeof(b) __b = (b);     \
    (__a < __b) ? __a : __b; \
  })

#define MILLISECOND_PER_SECOND (1000L)

#define tsem_t dispatch_semaphore_t

int tsem_init(dispatch_semaphore_t *sem, int pshared, unsigned int value);
int tsem_wait(dispatch_semaphore_t *sem);
int tsem_post(dispatch_semaphore_t *sem);
int tsem_destroy(dispatch_semaphore_t *sem);

ssize_t twrite(int fd, void *buf, size_t n);

char *taosCharsetReplace(char *charsetstr);

bool taosCheckPthreadValid(pthread_t thread);

void taosResetPthread(pthread_t *thread);

int64_t taosGetPthreadId();

int taosSetNonblocking(int sock, int on);

int taosSetSockOpt(int socketfd, int level, int optname, void *optval, int optlen);

void tsPrintOsInfo();

char *taosCharsetReplace(char *charsetstr);

void tsPrintOsInfo();

void taosGetSystemInfo();

void taosKillSystem();

bool taosSkipSocketCheck();

bool taosGetDisk();

typedef int(*__compar_fn_t)(const void *, const void *);

// for send function in tsocket.c
#define MSG_NOSIGNAL             0
#define SO_NO_CHECK              0x1234
#define SOL_TCP                  0x1234
#define TCP_KEEPIDLE             0x1234

#ifndef PTHREAD_MUTEX_RECURSIVE_NP
  #define  PTHREAD_MUTEX_RECURSIVE_NP PTHREAD_MUTEX_RECURSIVE
#endif

#endif