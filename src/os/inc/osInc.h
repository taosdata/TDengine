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

#ifndef TDENGINE_OS_INC_H
#define TDENGINE_OS_INC_H

#ifdef __cplusplus
extern "C" {
#endif

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <inttypes.h>
#include <locale.h>
#include <math.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_TD_LINUX_64) || defined(_TD_LINUX_32) || defined(_TD_MIPS_64)  || defined(_TD_ARM_32) || defined(_TD_ARM_64)  || defined(_TD_DARWIN_64)
  #include <arpa/inet.h>
  #include <dirent.h>
  #include <fcntl.h>
  #include <inttypes.h>
  #include <libgen.h>
  #include <limits.h>
  #include <netdb.h>
  #include <netinet/in.h>
  #include <netinet/ip.h>
  #include <netinet/tcp.h>
  #include <netinet/udp.h>
  #include <pwd.h>
  #include <regex.h>
  #include <stddef.h>
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
  #include <sys/utsname.h>
  #include <syslog.h>
  #include <termios.h>
  #include <unistd.h>
  #include <wchar.h>
  #include <wordexp.h>
  #include <wctype.h>

  #if defined(_TD_DARWIN_64)
    #include <dispatch/dispatch.h>
    #include "osEok.h"
  #else
    #include <argp.h>
    #include <dlfcn.h>
    #include <endian.h>
    #include <linux/sysctl.h>
    #include <poll.h>
    #include <sys/epoll.h>
    #include <sys/eventfd.h>
    #include <sys/resource.h>
    #include <sys/sendfile.h>
    #include <sys/prctl.h>

    #if !(defined(_ALPINE))
      #include <error.h>
    #endif
  #endif
#endif

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  #include <intrin.h>
  #include <io.h>
  #include "winsock2.h"
  #include <WS2tcpip.h>
  #include <winbase.h>
  #include <Winsock2.h>
  #include <time.h>
  #include <conio.h>
  #include "msvcProcess.h"
  #include "msvcDirect.h"
  #include "msvcFcntl.h"
  #include "msvcLibgen.h"
  #include "msvcStdio.h"
  #include "msvcUnistd.h"
  #include "msvcLibgen.h"
  #include "sys/msvcStat.h"
  #include "sys/msvcTypes.h"
#endif

#ifdef __cplusplus
}
#endif

#endif
