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


#ifndef TDENGINE_PLATFORM_LINUX_H
#define TDENGINE_PLATFORM_LINUX_H

#include <endian.h>
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
#include <sys/epoll.h>
#include <sys/file.h>
#include <sys/ioctl.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <stdint.h>
#include <pthread.h>
#include <stdbool.h>
#include <limits.h>
#include <linux/limits.h>
#include <strings.h>
#include <sys/sendfile.h>

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

#define __sync_sub_and_fetch_64 __sync_sub_and_fetch
#define __sync_sub_and_fetch_32 __sync_sub_and_fetch

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

#define tsem_t sem_t
#define tsem_init sem_init
#define tsem_wait sem_wait
#define tsem_post sem_post
#define tsem_destroy sem_destroy

ssize_t tsendfile(int dfd, int sfd, off_t *offset, size_t size);

ssize_t twrite(int fd, void *buf, size_t n);

bool taosCheckPthreadValid(pthread_t thread);

void taosResetPthread(pthread_t *thread);

int64_t taosGetPthreadId();

int taosSetNonblocking(int sock, int on);

int taosSetSockOpt(int socketfd, int level, int optname, void *optval, int optlen);

void tsPrintOsInfo();

char *taosCharsetReplace(char *charsetstr);

void taosGetSystemInfo();

void taosKillSystem();

bool taosSkipSocketCheck();

int64_t str2int64(char *str);

#define BUILDIN_CLZL(val) __builtin_clzl(val)
#define BUILDIN_CLZ(val)  __builtin_clz(val)
#define BUILDIN_CTZL(val) __builtin_ctzl(val)
#define BUILDIN_CTZ(val)  __builtin_ctz(val)

#endif