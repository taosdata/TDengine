/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __TOOLSDEF_H_
#define __TOOLSDEF_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <time.h>

#define ALLOW_FORBID_FUNC
#include "os.h"
#include "types.h"
#include "taoserror.h"
#include "taosdef.h"

#define TINY_BUFF_LEN                   8
#define SMALL_BUFF_LEN                  20
#define MIDDLE_BUFF_LEN                  64
#define LARGE_BUFF_LEN                   512
// max file name length on Linux is 255
#define MAX_FILE_NAME_LEN               256  // max file name length on linux is 255.

// max path length on Linux is 4095
#define MAX_PATH_LEN                    4096
#define MAX_DIR_LEN                     3808

// max hostname length on Linux is 253
#define MAX_HOSTNAME_LEN                254

// come from tdef.h
#ifndef TOOLS_MAX_ALLOWED_SQL_LEN
#define TOOLS_MAX_ALLOWED_SQL_LEN        (4*1024*1024u) /* sql max length */
#endif

#if _MSC_VER <= 1900
    #define __func__ __FUNCTION__
#endif

#if defined(__GNUC__)
    #define FORCE_INLINE inline __attribute__((always_inline))
#else
    #define FORCE_INLINE
#endif

#ifdef WINDOWS

#ifndef PATH_MAX
    #define PATH_MAX 256
#endif
#ifndef ssize_t
    #define ssize_t int
#endif
#ifndef F_OK
    #define F_OK 0
#endif

    #define strcasecmp       _stricmp
    #define strncasecmp      _strnicmp
#endif

int64_t tools_strnatoi(char *num, int32_t len);
char *  tools_strnchr(char *haystack, char needle, int32_t len, bool skipquote);
int64_t tools_user_mktime64(const unsigned int year0, const unsigned int mon0,
        const unsigned int day, const unsigned int hour,
        const unsigned int min, const unsigned int sec, int64_t time_zone);
int32_t toolsParseTimezone(char* str, int64_t* tzOffset);
int32_t toolsParseTime(char* timestr, int64_t* time, int32_t len, int32_t timePrec, int8_t day_light);
struct tm* toolsLocalTime(const time_t *timep, struct tm *result);
int32_t toolsGetTimeOfDay(struct timeval *tv);
int32_t toolsClockGetTime(int clock_id, struct timespec *pTS);
int64_t toolsGetTimestampMs();
int64_t toolsGetTimestampUs();
int64_t toolsGetTimestampNs();

typedef struct TdDir      *TdDirPtr;
typedef struct TdDirEntry *TdDirEntryPtr;

int32_t toolsExpandDir(const char *dirname, char *outname, int32_t maxlen);

TdDirPtr      toolsOpenDir(const char *dirname);
TdDirEntryPtr toolsReadDir(TdDirPtr pDir);
char         *toolsGetDirEntryName(TdDirEntryPtr pDirEntry);
int32_t       toolsCloseDir(TdDirPtr *ppDir);

#define toolsGetLineFile(__pLine,__pN, __pFp)                           \
    do {                                                                \
        *(__pLine) = calloc(1, 1024);                                   \
        fgets(*(__pLine), 1023, (__pFp));                               \
        (*(__pLine))[1023] = 0;                                         \
        *(__pN)=strlen(*(__pLine));                                     \
    } while(0)

#define TOOLS_STRNCPY(dst, src, size)                                   \
    do {                                                                \
        strncpy((dst), (src), (size)-1);                                \
        (dst)[(size)-1] = 0;                                            \
    } while (0)


#ifdef RELEASE
    #define TOOLS_ASSERT(x)   do { \
        if (!(x)) errorPrint("%s() LN%d, %s\n", \
            __func__, __LINE__, "assertion");} while(0)
#else
    #include <assert.h>
    #define TOOLS_ASSERT(x)   do { assert(x); } while(0)
#endif // RELEASE

#ifdef WINDOWS
    #define SET_THREAD_NAME(name)
#elif defined(DARWIN)
    #define SET_THREAD_NAME(name)
#else
    #define SET_THREAD_NAME(name)  do { prctl(PR_SET_NAME, (name)); } while (0)
#endif

int64_t atomic_add_fetch_64(int64_t volatile* ptr, int64_t val);
int32_t toolsGetNumberOfCores();

int64_t toolsGetTimestamp(int32_t precision);
void    toolsMsleep(int32_t mseconds);
bool    toolsIsStringNumber(char *input);

void errorWrongValue(char *program, char *wrong_arg, char *wrong_value);
void errorPrintReqArg(char *program, char *wrong_arg);
void errorPrintReqArg2(char *program, char *wrong_arg);
void errorPrintReqArg3(char *program, char *wrong_arg);
int setConsoleEcho(bool on);

char *toolsFormatTimestamp(char *buf, int64_t val, int32_t precision);

#ifdef __cplusplus
} /* end extern "C" */
#endif

#endif // __TOOLSDEF_H_
