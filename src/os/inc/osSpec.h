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

#ifndef TDENGINE_OS_SPEC_H
#define TDENGINE_OS_SPEC_H

#ifdef __cplusplus
extern "C" {
#endif

#define tclose(x) taosCloseSocket(x)
#define tfree(x)         \
  do {                   \
    if (x) {             \
      free((void *)(x)); \
      x = 0;             \
    }                    \
  } while (0);

#define tstrncpy(dst, src, size)   \
  do {                             \
    strncpy((dst), (src), (size)); \
    (dst)[(size)-1] = 0;           \
  } while (0);

#ifndef STDERR_FILENO
#define STDERR_FILENO (2)
#endif

#define FD_VALID(x) ((x) > STDERR_FILENO)
#define FD_INITIALIZER  ((int32_t)-1)

#define WCHAR wchar_t

#define POINTER_SHIFT(p, b) ((void *)((char *)(p) + (b)))
#define POINTER_DISTANCE(p1, p2) ((char *)(p1) - (char *)(p2)) 

#ifndef NDEBUG
#define ASSERT(x) assert(x)
#else
#define ASSERT(x)
#endif

#ifdef UNUSED
#undefine UNUSED
#endif
#define UNUSED(x) ((void)(x))

#ifdef UNUSED_FUNC
#undefine UNUSED_FUNC
#endif

#ifdef UNUSED_PARAM
#undef UNUSED_PARAM
#endif

#if defined(__GNUC__)
#define UNUSED_PARAM(x) _UNUSED##x __attribute__((unused))
#define UNUSED_FUNC __attribute__((unused))
#else
#define UNUSED_PARAM(x) x
#define UNUSED_FUNC
#endif

#ifdef tListLen
#undefine tListLen
#endif
#define tListLen(x) (sizeof(x) / sizeof((x)[0]))

#if defined(__GNUC__)
#define FORCE_INLINE inline __attribute__((always_inline))
#else
#define FORCE_INLINE
#endif

#define DEFAULT_UNICODE_ENCODEC "UCS-4LE"
  
#define DEFAULT_COMP(x, y)       \
  do {                           \
    if ((x) == (y)) {            \
      return 0;                  \
    } else {                     \
      return (x) < (y) ? -1 : 1; \
    }                            \
  } while (0)

#define ALIGN_NUM(n, align) (((n) + ((align)-1)) & (~((align)-1)))

// align to 8bytes
#define ALIGN8(n) ALIGN_NUM(n, 8)

#define POW2(x) ((x) * (x))

#ifndef TAOS_OS_FUNC_MATH
  #define SWAP(a, b, c)        \
    do {                       \
        typeof(a) __tmp = (a); \
        (a) = (b);             \
        (b) = __tmp;           \
    } while (0)

  #define MAX(a, b)              \
    ({                           \
        typeof(a) __a = (a);     \
        typeof(b) __b = (b);     \
        (__a > __b) ? __a : __b; \
    })

  #define MIN(a, b)              \
    ({                           \
        typeof(a) __a = (a);     \
        typeof(b) __b = (b);     \
        (__a < __b) ? __a : __b; \
    })
#endif

#ifndef TAOS_OS_DEF_TIME
  #define MILLISECOND_PER_SECOND ((int64_t)1000L)
#endif
#define MILLISECOND_PER_MINUTE (MILLISECOND_PER_SECOND * 60)
#define MILLISECOND_PER_HOUR   (MILLISECOND_PER_MINUTE * 60)
#define MILLISECOND_PER_DAY    (MILLISECOND_PER_HOUR * 24)
#define MILLISECOND_PER_WEEK   (MILLISECOND_PER_DAY * 7)
#define MILLISECOND_PER_MONTH  (MILLISECOND_PER_DAY * 30)
#define MILLISECOND_PER_YEAR   (MILLISECOND_PER_DAY * 365)

#ifndef TAOS_OS_FUNC_SEMPHONE
  #define tsem_t sem_t
  #define tsem_init sem_init
  #define tsem_wait sem_wait
  #define tsem_post sem_post
  #define tsem_destroy sem_destroy
#endif

#ifndef TAOS_OS_FUNC_ATOMIC
  #define atomic_load_8(ptr) __atomic_load_n((ptr), __ATOMIC_SEQ_CST)
  #define atomic_load_16(ptr) __atomic_load_n((ptr), __ATOMIC_SEQ_CST)
  #define atomic_load_32(ptr) __atomic_load_n((ptr), __ATOMIC_SEQ_CST)
  #define atomic_load_64(ptr) __atomic_load_n((ptr), __ATOMIC_SEQ_CST)
  #define atomic_load_ptr(ptr) __atomic_load_n((ptr), __ATOMIC_SEQ_CST)

  #define atomic_store_8(ptr, val) __atomic_store_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_store_16(ptr, val) __atomic_store_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_store_32(ptr, val) __atomic_store_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_store_64(ptr, val) __atomic_store_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_store_ptr(ptr, val) __atomic_store_n((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_exchange_8(ptr, val) __atomic_exchange_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_exchange_16(ptr, val) __atomic_exchange_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_exchange_32(ptr, val) __atomic_exchange_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_exchange_64(ptr, val) __atomic_exchange_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_exchange_ptr(ptr, val) __atomic_exchange_n((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_val_compare_exchange_8 __sync_val_compare_and_swap
  #define atomic_val_compare_exchange_16 __sync_val_compare_and_swap
  #define atomic_val_compare_exchange_32 __sync_val_compare_and_swap
  #define atomic_val_compare_exchange_64 __sync_val_compare_and_swap
  #define atomic_val_compare_exchange_ptr __sync_val_compare_and_swap

  #define atomic_add_fetch_8(ptr, val) __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_add_fetch_16(ptr, val) __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_add_fetch_32(ptr, val) __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_add_fetch_64(ptr, val) __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_add_fetch_ptr(ptr, val) __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_fetch_add_8(ptr, val) __atomic_fetch_add((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_add_16(ptr, val) __atomic_fetch_add((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_add_32(ptr, val) __atomic_fetch_add((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_add_64(ptr, val) __atomic_fetch_add((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_add_ptr(ptr, val) __atomic_fetch_add((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_sub_fetch_8(ptr, val) __atomic_sub_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_sub_fetch_16(ptr, val) __atomic_sub_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_sub_fetch_32(ptr, val) __atomic_sub_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_sub_fetch_64(ptr, val) __atomic_sub_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_sub_fetch_ptr(ptr, val) __atomic_sub_fetch((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_fetch_sub_8(ptr, val) __atomic_fetch_sub((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_sub_16(ptr, val) __atomic_fetch_sub((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_sub_32(ptr, val) __atomic_fetch_sub((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_sub_64(ptr, val) __atomic_fetch_sub((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_sub_ptr(ptr, val) __atomic_fetch_sub((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_and_fetch_8(ptr, val) __atomic_and_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_and_fetch_16(ptr, val) __atomic_and_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_and_fetch_32(ptr, val) __atomic_and_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_and_fetch_64(ptr, val) __atomic_and_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_and_fetch_ptr(ptr, val) __atomic_and_fetch((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_fetch_and_8(ptr, val) __atomic_fetch_and((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_and_16(ptr, val) __atomic_fetch_and((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_and_32(ptr, val) __atomic_fetch_and((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_and_64(ptr, val) __atomic_fetch_and((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_and_ptr(ptr, val) __atomic_fetch_and((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_or_fetch_8(ptr, val) __atomic_or_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_or_fetch_16(ptr, val) __atomic_or_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_or_fetch_32(ptr, val) __atomic_or_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_or_fetch_64(ptr, val) __atomic_or_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_or_fetch_ptr(ptr, val) __atomic_or_fetch((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_fetch_or_8(ptr, val) __atomic_fetch_or((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_or_16(ptr, val) __atomic_fetch_or((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_or_32(ptr, val) __atomic_fetch_or((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_or_64(ptr, val) __atomic_fetch_or((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_or_ptr(ptr, val) __atomic_fetch_or((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_xor_fetch_8(ptr, val) __atomic_xor_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_xor_fetch_16(ptr, val) __atomic_xor_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_xor_fetch_32(ptr, val) __atomic_xor_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_xor_fetch_64(ptr, val) __atomic_xor_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_xor_fetch_ptr(ptr, val) __atomic_xor_fetch((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_fetch_xor_8(ptr, val) __atomic_fetch_xor((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_xor_16(ptr, val) __atomic_fetch_xor((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_xor_32(ptr, val) __atomic_fetch_xor((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_xor_64(ptr, val) __atomic_fetch_xor((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_xor_ptr(ptr, val) __atomic_fetch_xor((ptr), (val), __ATOMIC_SEQ_CST)
#endif

ssize_t taosTReadImp(int fd, void *buf, size_t count);
ssize_t taosTWriteImp(int fd, void *buf, size_t count);
// TAOS_OS_FUNC_FILEOP
ssize_t taosTSendFileImp(int dfd, int sfd, off_t *offset, size_t size);
int     taosFSendFileImp(FILE* out_file, FILE* in_file, int64_t* offset, int32_t count);
#ifndef TAOS_OS_FUNC_FILEOP
  #define taosTSendFile(dfd, sfd, offset, size) taosTSendFileImp(dfd, sfd, offset, size)
  #define taosFSendFile(outfile, infile, offset, count) taosTSendFileImp(fileno(outfile), fileno(infile), offset, size)
#endif

#ifndef TAOS_OS_FUNC_NETWORK
  #define taosSend(sockfd, buf, len, flags) send(sockfd, buf, len, flags)
  #define taosSendto(sockfd, buf, len, flags, dest_addr, addrlen) sendto(sockfd, buf, len, flags, dest_addr, addrlen)
  #define taosReadSocket(fd, buf, len) read(fd, buf, len)
  #define taosWriteSocket(fd, buf, len) write(fd, buf, len)
  #define taosCloseSocket(x) \
    {                        \
      if (FD_VALID(x)) {     \
        close(x);            \
        x = FD_INITIALIZER;  \
      }                      \
    }
#endif

#ifndef TAOS_OS_FUNC_LZ4
  #define BUILDIN_CLZL(val) __builtin_clzl(val)
  #define BUILDIN_CTZL(val) __builtin_ctzl(val)
  #define BUILDIN_CLZ(val) __builtin_clz(val)
  #define BUILDIN_CTZ(val) __builtin_ctz(val)
#endif

#ifndef TAOS_OS_FUNC_WCHAR
  #define twcslen wcslen
#endif  

#undef threadlocal
#ifdef _ISOC11_SOURCE
  #define threadlocal _Thread_local
#elif defined(__APPLE__)
  #define threadlocal
#elif defined(__GNUC__) && !defined(threadlocal)
  #define threadlocal __thread
#else
  #define threadlocal
#endif

void osInit();

// TAOS_OS_FUNC_PTHREAD
bool taosCheckPthreadValid(pthread_t thread);
int64_t taosGetPthreadId();

// TAOS_OS_FUNC_SOCKET
int taosSetNonblocking(int sock, int on);
void taosBlockSIGPIPE();

// TAOS_OS_FUNC_SOCKET_SETSOCKETOPT
int taosSetSockOpt(int socketfd, int level, int optname, void *optval, int optlen);

// TAOS_OS_FUNC_SYSINFO
void taosGetSystemInfo();
bool taosGetProcIO(float *readKB, float *writeKB);
bool taosGetBandSpeed(float *bandSpeedKb);
bool taosGetDisk();
bool taosGetCpuUsage(float *sysCpuUsage, float *procCpuUsage) ;
bool taosGetProcMemory(float *memoryUsedMB) ;
bool taosGetSysMemory(float *memoryUsedMB);
void taosPrintOsInfo();
int  taosSystem(const char * cmd) ;
void taosKillSystem();

// TAOS_OS_FUNC_CORE
void taosSetCoreDump();

// TAOS_OS_FUNC_UTIL
int64_t tsosStr2int64(char *str);

// TAOS_OS_FUNC_TIMER
void taosMsleep(int mseconds);
int taosInitTimer(void (*callback)(int), int ms);
void taosUninitTimer();

// TAOS_OS_FUNC_RAND
uint32_t taosRand(void);
void taosRandStr(char* str, int32_t size);
uint32_t trand(void);

// TAOS_OS_FUNC_FILE
void taosGetTmpfilePath(const char *fileNamePrefix, char *dstPath);
int32_t taosFileRename(char *fullPath, char *suffix, char delimiter, char **dstPath);

// USE_LIBICONV
int32_t taosUcs4ToMbs(void *ucs4, int32_t ucs4_max_len, char *mbs);
bool    taosMbsToUcs4(char *mbs, size_t mbs_len, char *ucs4, int32_t ucs4_max_len, size_t *len);
int     tasoUcs4Compare(void *f1_ucs4, void *f2_ucs4, int bytes);
bool    taosValidateEncodec(const char *encodec);
char *  taosCharsetReplace(char *charsetstr);

// TAOS_OS_FUNC_MALLOC
#define TAOS_ALLOC_MODE_DEFAULT 0
#define TAOS_ALLOC_MODE_RANDOM_FAIL 1
#define TAOS_ALLOC_MODE_DETECT_LEAK 2
void   taosSetAllocMode(int mode, const char *path, bool autoDump);
void   taosDumpMemoryLeak();
void * tmalloc(size_t size);
void * tcalloc(size_t nmemb, size_t size);
size_t tsizeof(void *ptr);
void   tmemset(void *ptr, int c);
void * trealloc(void *ptr, size_t size);
void   tzfree(void *ptr);

// TAOS_OS_FUNC_DIR
void taosRemoveDir(char *rootDir);
int  taosMkDir(const char *pathname, mode_t mode); 
void taosMvDir(char* destDir, char *srcDir);

#ifdef TAOS_RANDOM_FILE_FAIL
  void taosSetRandomFileFailFactor(int factor);
  void taosSetRandomFileFailOutput(const char *path);
  ssize_t taosReadFileRandomFail(int fd, void *buf, size_t count, const char *file, uint32_t line);
  ssize_t taosWriteFileRandomFail(int fd, void *buf, size_t count, const char *file, uint32_t line);
  off_t taosLSeekRandomFail(int fd, off_t offset, int whence, const char *file, uint32_t line);
  #define taosTRead(fd, buf, count) taosReadFileRandomFail(fd, buf, count, __FILE__, __LINE__)
  #define taosTWrite(fd, buf, count) taosWriteFileRandomFail(fd, buf, count, __FILE__, __LINE__)
  #define taosLSeek(fd, offset, whence) taosLSeekRandomFail(fd, offset, whence, __FILE__, __LINE__)
#else 
  #define taosTRead(fd, buf, count) taosTReadImp(fd, buf, count)
  #define taosTWrite(fd, buf, count) taosTWriteImp(fd, buf, count)
  #define taosLSeek(fd, offset, whence) lseek(fd, offset, whence)
#endif

#ifdef TAOS_RANDOM_NETWORK_FAIL
  ssize_t taosSendRandomFail(int sockfd, const void *buf, size_t len, int flags);
  ssize_t taosSendToRandomFail(int sockfd, const void *buf, size_t len, int flags, const struct sockaddr *dest_addr, socklen_t addrlen);
  ssize_t taosReadSocketRandomFail(int fd, void *buf, size_t count);
  ssize_t taosWriteSocketRandomFail(int fd, const void *buf, size_t count);
  #undef taosSend
  #undef taosSendto
  #undef taosReadSocket
  #undef taosWriteSocket
  #define taosSend(sockfd, buf, len, flags) taosSendRandomFail(sockfd, buf, len, flags)
  #define taosSendto(sockfd, buf, len, flags, dest_addr, addrlen) taosSendToRandomFail(sockfd, buf, len, flags, dest_addr, addrlen)
  #define taosReadSocket(fd, buf, len) taosReadSocketRandomFail(fd, buf, len)
  #define taosWriteSocket(fd, buf, len) taosWriteSocketRandomFail(fd, buf, len)
#endif

#ifdef TAOS_MEM_CHECK
  void *  taos_malloc(size_t size, const char *file, uint32_t line);
  void *  taos_calloc(size_t num, size_t size, const char *file, uint32_t line);
  void *  taos_realloc(void *ptr, size_t size, const char *file, uint32_t line);
  void    taos_free(void *ptr, const char *file, uint32_t line);
  char *  taos_strdup(const char *str, const char *file, uint32_t line);
  char *  taos_strndup(const char *str, size_t size, const char *file, uint32_t line);
  ssize_t taos_getline(char **lineptr, size_t *n, FILE *stream, const char *file, uint32_t line);
  #define malloc(size) taos_malloc(size, __FILE__, __LINE__)
  #define calloc(num, size) taos_calloc(num, size, __FILE__, __LINE__)
  #define realloc(ptr, size) taos_realloc(ptr, size, __FILE__, __LINE__)
  #define free(ptr) taos_free(ptr, __FILE__, __LINE__)
  #define strdup(str) taos_strdup(str, __FILE__, __LINE__)
  #define strndup(str, size) taos_strndup(str, size, __FILE__, __LINE__)
  #define getline(lineptr, n, stream) taos_getline(lineptr, n, stream, __FILE__, __LINE__)
#endif  // TAOS_MEM_CHECK

#ifdef __cplusplus
}
#endif

#endif
