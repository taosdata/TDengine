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
#include "msvcProcess.h"
#include "msvcDirect.h"
#include "msvcFcntl.h"
#include "msvcStdio.h"
#include "sys/msvcStat.h"
#include "sys/msvcTypes.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TAOS_OS_FUNC_LZ4
  int32_t BUILDIN_CLZL(uint64_t val);
  int32_t BUILDIN_CLZ(uint32_t val);
  int32_t BUILDIN_CTZL(uint64_t val);
  int32_t BUILDIN_CTZ(uint32_t val);

#define TAOS_OS_FUNC_FILE
#define TAOS_OS_FUNC_FILE_ISREG
#define TAOS_OS_FUNC_FILE_ISDIR
#define TAOS_OS_FUNC_FILE_ISLNK
#define TAOS_OS_FUNC_FILE_SENDIFLE
#define TAOS_OS_FUNC_FILE_GETTMPFILEPATH
#define TAOS_OS_FUNC_FILE_FTRUNCATE 

#define TAOS_OS_FUNC_MATH
  #define SWAP(a, b, c)      \
    do {                     \
      c __tmp = (c)(a);      \
      (a) = (c)(b);          \
      (b) = __tmp;           \
    } while (0)
  #define MAX(a,b)  (((a)>(b))?(a):(b))
  #define MIN(a,b)  (((a)<(b))?(a):(b))

#define TAOS_OS_FUNC_SEMPHONE_PTHREAD

#define TAOS_OS_FUNC_SOCKET
#define TAOS_OS_FUNC_SOCKET_SETSOCKETOPT
#define TAOS_OS_FUNC_SOCKET_OP
  #define taosSend(sockfd, buf, len, flags) send((SOCKET)sockfd, buf, len, flags)
  #define taosSendto(sockfd, buf, len, flags, dest_addr, addrlen) sendto((SOCKET)sockfd, buf, len, flags, dest_addr, addrlen)
  #define taosWriteSocket(fd, buf, len) send((SOCKET)fd, buf, len, 0)
  #define taosReadSocket(fd, buf, len) recv((SOCKET)fd, buf, len, 0)
  #define taosCloseSocketNoCheck(fd) closesocket((SOCKET)fd)
  #define taosCloseSocket(fd) closesocket((SOCKET)fd)
typedef SOCKET eventfd_t; 
#define eventfd(a, b) -1

#define TAOS_OS_DEF_EPOLL
  #define TAOS_EPOLL_WAIT_TIME 100

#define TAOS_OS_DEF_ZU
  #define PRIzu "ld"  

#define TAOS_OS_FUNC_STRING_WCHAR
  int twcslen(const wchar_t *wcs);
#define TAOS_OS_FUNC_STRING_GETLINE
#define TAOS_OS_FUNC_STRING_STR2INT64
  #ifdef _TD_GO_DLL_
    int64_t tsosStr2int64(char *str);
    uint64_t htonll(uint64_t val);
  #else
    #define tsosStr2int64 _atoi64
  #endif
#define TAOS_OS_FUNC_STRING_STRDUP
  #define taosStrdupImp(str) _strdup(str)
  #define taosStrndupImp(str, size) _strndup(str, size)  

char *stpcpy (char *dest, const char *src);
char *stpncpy (char *dest, const char *src, size_t n);

#define TAOS_OS_FUNC_SYSINFO

#define TAOS_OS_FUNC_TIME_DEF
  #ifdef _TD_GO_DLL_
    #define MILLISECOND_PER_SECOND (1000LL)
  #else
    #define MILLISECOND_PER_SECOND (1000i64)
  #endif

#define TAOS_OS_FUNC_TIMER_SLEEP
#define TAOS_OS_FUNC_TIMER

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
#define htobe64 htonll
#define getpid _getpid

struct tm *localtime_r(const time_t *timep, struct tm *result);
char *     strptime(const char *buf, const char *fmt, struct tm *tm);
char *     strsep(char **stringp, const char *delim);
char *     getpass(const char *prefix);
int        flock(int fd, int option);
int        fsync(int filedes);
char *     strndup(const char *s, size_t n);
char *     dirname(char *pszPathname);
int        gettimeofday(struct timeval *ptv, void *pTimeZone);

// for access function in io.h
#define F_OK 00  //Existence only
#define W_OK 02  //Write - only
#define R_OK 04  //Read - only
#define X_OK 06  //Read and write

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

#ifdef _MSC_VER
//#if _MSC_VER >= 1900
  #define TAOS_OS_FUNC_SOCKET_INET
//#endif
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

//for signal, not dispose
#define SIGALRM 1234
typedef int sigset_t;
struct sigaction {
  void (*sa_handler)(int);
};
int sigaction(int, struct sigaction *, void *);

typedef struct {
  int    we_wordc;
  char **we_wordv;
  int    we_offs;
  char   wordPos[20];
} wordexp_t;
int  wordexp(const char *words, wordexp_t *pwordexp, int flags);
void wordfree(wordexp_t *pwordexp);

#define TAOS_OS_FUNC_ATOMIC
  #define atomic_load_8(ptr) (*(char volatile*)(ptr))
  #define atomic_load_16(ptr) (*(short volatile*)(ptr))
  #define atomic_load_32(ptr) (*(long volatile*)(ptr))
  #define atomic_load_64(ptr) (*(__int64 volatile*)(ptr))
  #define atomic_load_ptr(ptr) (*(void* volatile*)(ptr))

  #define atomic_store_8(ptr, val) ((*(char volatile*)(ptr)) = (char)(val))
  #define atomic_store_16(ptr, val) ((*(short volatile*)(ptr)) = (short)(val))
  #define atomic_store_32(ptr, val) ((*(long volatile*)(ptr)) = (long)(val))
  #define atomic_store_64(ptr, val) ((*(__int64 volatile*)(ptr)) = (__int64)(val))
  #define atomic_store_ptr(ptr, val) ((*(void* volatile*)(ptr)) = (void*)(val))

  #define atomic_exchange_8(ptr, val) _InterlockedExchange8((char volatile*)(ptr), (char)(val))
  #define atomic_exchange_16(ptr, val) _InterlockedExchange16((short volatile*)(ptr), (short)(val))
  #define atomic_exchange_32(ptr, val) _InterlockedExchange((long volatile*)(ptr), (long)(val))
  #define atomic_exchange_64(ptr, val) _InterlockedExchange64((__int64 volatile*)(ptr), (__int64)(val))
  #ifdef _WIN64 
    #define atomic_exchange_ptr(ptr, val) _InterlockedExchangePointer((void* volatile*)(ptr), (void*)(val)) 
  #else
    #define atomic_exchange_ptr(ptr, val) _InlineInterlockedExchangePointer((void* volatile*)(ptr), (void*)(val))
  #endif
  
  #ifdef _TD_GO_DLL_
    #define atomic_val_compare_exchange_8 __sync_val_compare_and_swap
  #else
    #define atomic_val_compare_exchange_8(ptr, oldval, newval) _InterlockedCompareExchange8((char volatile*)(ptr), (char)(newval), (char)(oldval))
  #endif
  #define atomic_val_compare_exchange_16(ptr, oldval, newval) _InterlockedCompareExchange16((short volatile*)(ptr), (short)(newval), (short)(oldval))
  #define atomic_val_compare_exchange_32(ptr, oldval, newval) _InterlockedCompareExchange((long volatile*)(ptr), (long)(newval), (long)(oldval))
  #define atomic_val_compare_exchange_64(ptr, oldval, newval) _InterlockedCompareExchange64((__int64 volatile*)(ptr), (__int64)(newval), (__int64)(oldval))
  #define atomic_val_compare_exchange_ptr(ptr, oldval, newval) _InterlockedCompareExchangePointer((void* volatile*)(ptr), (void*)(newval), (void*)(oldval))

  char    interlocked_add_fetch_8(char volatile *ptr, char val);
  short   interlocked_add_fetch_16(short volatile *ptr, short val);
  long    interlocked_add_fetch_32(long volatile *ptr, long val);
  __int64 interlocked_add_fetch_64(__int64 volatile *ptr, __int64 val);

  char interlocked_and_fetch_8(char volatile* ptr, char val);
  short interlocked_and_fetch_16(short volatile* ptr, short val);
  long interlocked_and_fetch_32(long volatile* ptr, long val);
  __int64 interlocked_and_fetch_64(__int64 volatile* ptr, __int64 val);

  __int64 interlocked_fetch_and_64(__int64 volatile* ptr, __int64 val);

  char interlocked_or_fetch_8(char volatile* ptr, char val);
  short interlocked_or_fetch_16(short volatile* ptr, short val);
  long interlocked_or_fetch_32(long volatile* ptr, long val);
  __int64 interlocked_or_fetch_64(__int64 volatile* ptr, __int64 val);

  char interlocked_xor_fetch_8(char volatile* ptr, char val);
  short interlocked_xor_fetch_16(short volatile* ptr, short val);
  long interlocked_xor_fetch_32(long volatile* ptr, long val);
  __int64 interlocked_xor_fetch_64(__int64 volatile* ptr, __int64 val);

  __int64 interlocked_fetch_xor_64(__int64 volatile* ptr, __int64 val);

  #define atomic_add_fetch_8(ptr, val) interlocked_add_fetch_8((char volatile*)(ptr), (char)(val))
  #define atomic_add_fetch_16(ptr, val) interlocked_add_fetch_16((short volatile*)(ptr), (short)(val))
  #define atomic_add_fetch_32(ptr, val) interlocked_add_fetch_32((long volatile*)(ptr), (long)(val))
  #define atomic_add_fetch_64(ptr, val) interlocked_add_fetch_64((__int64 volatile*)(ptr), (__int64)(val))
  #ifdef _TD_GO_DLL_
    #define atomic_fetch_add_8 __sync_fetch_and_ad
    #define atomic_fetch_add_16 __sync_fetch_and_add
  #else
    #define atomic_fetch_add_8(ptr, val) _InterlockedExchangeAdd8((char volatile*)(ptr), (char)(val))
    #define atomic_fetch_add_16(ptr, val) _InterlockedExchangeAdd16((short volatile*)(ptr), (short)(val))
  #endif  
  #define atomic_fetch_add_8(ptr, val) _InterlockedExchangeAdd8((char volatile*)(ptr), (char)(val))
  #define atomic_fetch_add_16(ptr, val) _InterlockedExchangeAdd16((short volatile*)(ptr), (short)(val))
  #define atomic_fetch_add_32(ptr, val) _InterlockedExchangeAdd((long volatile*)(ptr), (long)(val))
  #define atomic_fetch_add_64(ptr, val) _InterlockedExchangeAdd64((__int64 volatile*)(ptr), (__int64)(val))
  
  #define atomic_sub_fetch_8(ptr, val) interlocked_add_fetch_8((char volatile*)(ptr), -(char)(val))
  #define atomic_sub_fetch_16(ptr, val) interlocked_add_fetch_16((short volatile*)(ptr), -(short)(val))
  #define atomic_sub_fetch_32(ptr, val) interlocked_add_fetch_32((long volatile*)(ptr), -(long)(val))
  #define atomic_sub_fetch_64(ptr, val) interlocked_add_fetch_64((__int64 volatile*)(ptr), -(__int64)(val))

  #define atomic_fetch_sub_8(ptr, val) _InterlockedExchangeAdd8((char volatile*)(ptr), -(char)(val))
  #define atomic_fetch_sub_16(ptr, val) _InterlockedExchangeAdd16((short volatile*)(ptr), -(short)(val))
  #define atomic_fetch_sub_32(ptr, val) _InterlockedExchangeAdd((long volatile*)(ptr), -(long)(val))
  #define atomic_fetch_sub_64(ptr, val) _InterlockedExchangeAdd64((__int64 volatile*)(ptr), -(__int64)(val))

  #define atomic_and_fetch_8(ptr, val) interlocked_and_fetch_8((char volatile*)(ptr), (char)(val))
  #define atomic_and_fetch_16(ptr, val) interlocked_and_fetch_16((short volatile*)(ptr), (short)(val))
  #define atomic_and_fetch_32(ptr, val) interlocked_and_fetch_32((long volatile*)(ptr), (long)(val))
  #define atomic_and_fetch_64(ptr, val) interlocked_and_fetch_64((__int64 volatile*)(ptr), (__int64)(val))

  #define atomic_fetch_and_8(ptr, val) _InterlockedAnd8((char volatile*)(ptr), (char)(val))
  #define atomic_fetch_and_16(ptr, val) _InterlockedAnd16((short volatile*)(ptr), (short)(val))
  #define atomic_fetch_and_32(ptr, val) _InterlockedAnd((long volatile*)(ptr), (long)(val))
  #define atomic_fetch_and_64(ptr, val) interlocked_fetch_and_64((__int64 volatile*)(ptr), (__int64)(val))

  #define atomic_or_fetch_8(ptr, val) interlocked_or_fetch_8((char volatile*)(ptr), (char)(val))
  #define atomic_or_fetch_16(ptr, val) interlocked_or_fetch_16((short volatile*)(ptr), (short)(val))
  #define atomic_or_fetch_32(ptr, val) interlocked_or_fetch_32((long volatile*)(ptr), (long)(val))
  #define atomic_or_fetch_64(ptr, val) interlocked_or_fetch_64((__int64 volatile*)(ptr), (__int64)(val))

  #define atomic_fetch_or_8(ptr, val) _InterlockedOr8((char volatile*)(ptr), (char)(val))
  #define atomic_fetch_or_16(ptr, val) _InterlockedOr16((short volatile*)(ptr), (short)(val))
  #define atomic_fetch_or_32(ptr, val) _InterlockedOr((long volatile*)(ptr), (long)(val))
  #define atomic_fetch_or_64(ptr, val) interlocked_fetch_or_64((__int64 volatile*)(ptr), (__int64)(val))

  #define atomic_xor_fetch_8(ptr, val) interlocked_xor_fetch_8((char volatile*)(ptr), (char)(val))
  #define atomic_xor_fetch_16(ptr, val) interlocked_xor_fetch_16((short volatile*)(ptr), (short)(val))
  #define atomic_xor_fetch_32(ptr, val) interlocked_xor_fetch_32((long volatile*)(ptr), (long)(val))
  #define atomic_xor_fetch_64(ptr, val) interlocked_xor_fetch_64((__int64 volatile*)(ptr), (__int64)(val))

  #define atomic_fetch_xor_8(ptr, val) _InterlockedXor8((char volatile*)(ptr), (char)(val))
  #define atomic_fetch_xor_16(ptr, val) _InterlockedXor16((short volatile*)(ptr), (short)(val))
  #define atomic_fetch_xor_32(ptr, val) _InterlockedXor((long volatile*)(ptr), (long)(val))
  #define atomic_fetch_xor_64(ptr, val) interlocked_fetch_xor_64((__int64 volatile*)(ptr), (__int64)(val))

  #ifdef _WIN64
    #define atomic_add_fetch_ptr atomic_add_fetch_64
    #define atomic_fetch_add_ptr atomic_fetch_add_64
    #define atomic_sub_fetch_ptr atomic_sub_fetch_64
    #define atomic_fetch_sub_ptr atomic_fetch_sub_64
    #define atomic_and_fetch_ptr atomic_and_fetch_64
    #define atomic_fetch_and_ptr atomic_fetch_and_64
    #define atomic_or_fetch_ptr  atomic_or_fetch_64
    #define atomic_fetch_or_ptr  atomic_fetch_or_64
    #define atomic_xor_fetch_ptr atomic_xor_fetch_64
    #define atomic_fetch_xor_ptr atomic_fetch_xor_64
  #else
    #define atomic_add_fetch_ptr atomic_add_fetch_32
    #define atomic_fetch_add_ptr atomic_fetch_add_32
    #define atomic_sub_fetch_ptr atomic_sub_fetch_32
    #define atomic_fetch_sub_ptr atomic_fetch_sub_32
    #define atomic_and_fetch_ptr atomic_and_fetch_32
    #define atomic_fetch_and_ptr atomic_fetch_and_32
    #define atomic_or_fetch_ptr  atomic_or_fetch_32
    #define atomic_fetch_or_ptr  atomic_fetch_or_32
    #define atomic_xor_fetch_ptr atomic_xor_fetch_32
    #define atomic_fetch_xor_ptr atomic_fetch_xor_32
  #endif
#ifdef __cplusplus
}
#endif
#endif
