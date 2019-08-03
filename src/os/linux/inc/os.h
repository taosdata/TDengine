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
#include <stdint.h>
#include <pthread.h>
#include <stdbool.h>
#include <limits.h>

bool taosCheckPthreadValid(pthread_t thread);

void taosResetPthread(pthread_t *thread);

int64_t taosGetPthreadId();

int taosSetNonblocking(int sock, int on);

int taosSetSockOpt(int socketfd, int level, int optname, void *optval, int optlen);

void tsPrintOsInfo();

char *taosCharsetReplace(char *charsetstr);

void taosGetSystemInfo();

void taosKillSystem();

#endif