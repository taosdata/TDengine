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

#include <stdbool.h>
#include <time.h>

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

#define TSDB_CODE_SUCCESS               0
#define TSDB_CODE_FAILED                -1   // unknown or needn't tell detail error

// NULL definition
#define TSDB_DATA_BOOL_NULL             0x02
#define TSDB_DATA_TINYINT_NULL          0x80
#define TSDB_DATA_SMALLINT_NULL         0x8000
#define TSDB_DATA_INT_NULL              0x80000000L
#define TSDB_DATA_BIGINT_NULL           0x8000000000000000L
#define TSDB_DATA_TIMESTAMP_NULL        TSDB_DATA_BIGINT_NULL

#define TSDB_DATA_FLOAT_NULL            0x7FF00000              // it is an NAN
#define TSDB_DATA_DOUBLE_NULL           0x7FFFFF0000000000L     // an NAN
#define TSDB_DATA_NCHAR_NULL            0xFFFFFFFF
#define TSDB_DATA_BINARY_NULL           0xFF
#define TSDB_DATA_JSON_PLACEHOLDER      0x7F
#define TSDB_DATA_JSON_NULL             0xFFFFFFFF
#define TSDB_DATA_JSON_null             0xFFFFFFFE
#define TSDB_DATA_JSON_NOT_NULL         0x01
#define TSDB_DATA_JSON_CAN_NOT_COMPARE  0x7FFFFFFF

#define TSDB_DATA_UTINYINT_NULL         0xFF
#define TSDB_DATA_USMALLINT_NULL        0xFFFF
#define TSDB_DATA_UINT_NULL             0xFFFFFFFF
#define TSDB_DATA_UBIGINT_NULL          0xFFFFFFFFFFFFFFFFL

#define GET_INT8_VAL(x)    (*(int8_t *)(x))
#define GET_INT16_VAL(x)   (*(int16_t *)(x))
#define GET_INT32_VAL(x)   (*(int32_t *)(x))
#define GET_INT64_VAL(x)   (*(int64_t *)(x))
#define GET_UINT8_VAL(x)   (*(uint8_t*) (x))
#define GET_UINT16_VAL(x)  (*(uint16_t *)(x))
#define GET_UINT32_VAL(x)  (*(uint32_t *)(x))
#define GET_UINT64_VAL(x)  (*(uint64_t *)(x))

#define TSDB_DEFAULT_USER               "root"
#define TSDB_DEFAULT_PASS               "taosdata"

#define TSDB_PASS_LEN                   129
#define SHELL_MAX_PASSWORD_LEN          TSDB_PASS_LEN

#define TSDB_TIME_PRECISION_MILLI       0
#define TSDB_TIME_PRECISION_MICRO       1
#define TSDB_TIME_PRECISION_NANO        2

#define TSDB_MAX_COLUMNS                4096
#define TSDB_MIN_COLUMNS                2       //PRIMARY COLUMN(timestamp) + other columns

#define TSDB_TABLE_NAME_LEN             193     // it is a null-terminated string

#define TSDB_DB_NAME_LEN                65

#define TSDB_COL_NAME_LEN               65
#define TSDB_MAX_ALLOWED_SQL_LEN        (1*1024*1024u)          // sql length should be less than 1mb

#define TSDB_MAX_BYTES_PER_ROW          65531
#define TSDB_MAX_TAGS                   128

#define TSDB_DEFAULT_PKT_SIZE           65480  //same as RPC_MAX_UDP_SIZE

#ifdef TSKEY32
#define TSKEY int32_t;
#else
#define TSKEY int64_t
#endif

#define TSDB_KEYSIZE                    sizeof(TSKEY)
#define TSDB_MAX_FIELD_LEN              65519
#define TSDB_MAX_BINARY_LEN             TSDB_MAX_FIELD_LEN
#define TSDB_FILENAME_LEN               128

#define TSDB_PORT_HTTP                  11

#if _MSC_VER <= 1900
    #define __func__ __FUNCTION__
#endif

#if defined(__GNUC__)
    #define FORCE_INLINE inline __attribute__((always_inline))
#else
    #define FORCE_INLINE
#endif

#ifdef _TD_ARM_32
    float  taos_align_get_float(const char* pBuf);
    double taos_align_get_double(const char* pBuf);

    #define GET_FLOAT_VAL(x)        taos_align_get_float(x)
    #define GET_DOUBLE_VAL(x)       taos_align_get_double(x)
    #define SET_FLOAT_VAL(x, y)     { float z = (float)(y);   (*(int32_t*) x = *(int32_t*)(&z)); }
    #define SET_DOUBLE_VAL(x, y)    { double z = (double)(y); (*(int64_t*) x = *(int64_t*)(&z)); }
    #define SET_TIMESTAMP_VAL(x, y) { int64_t z = (int64_t)(y); (*(int64_t*) x = *(int64_t*)(&z)); }
    #define SET_FLOAT_PTR(x, y)     { (*(int32_t*) x = *(int32_t*)y); }
    #define SET_DOUBLE_PTR(x, y)    { (*(int64_t*) x = *(int64_t*)y); }
#else
    #define GET_FLOAT_VAL(x)        (*(float *)(x))
    #define GET_DOUBLE_VAL(x)       (*(double *)(x))
    #define SET_FLOAT_VAL(x, y)     { (*(float *)(x))  = (float)(y);       }
    #define SET_DOUBLE_VAL(x, y)    { (*(double *)(x)) = (double)(y);      }
    #define SET_TIMESTAMP_VAL(x, y) { (*(int64_t *)(x)) = (int64_t)(y);    }
    #define SET_FLOAT_PTR(x, y)     { (*(float *)(x))  = (*(float *)(y));  }
    #define SET_DOUBLE_PTR(x, y)    { (*(double *)(x)) = (*(double *)(y)); }
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

#ifdef WINDOWS
typedef struct {
    int   we_wordc;
    char *we_wordv[1];
    int   we_offs;
    char  wordPos[1025];
} wordexp_t;
int  wordexp(char *words, wordexp_t *pwordexp, int flags);
void wordfree(wordexp_t *pwordexp);

char *strsep(char **stringp, const char *delim);
#endif

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

#define tstrncpy(dst, src, size)       \
    do {                               \
        strncpy((dst), (src), (size)-1); \
        (dst)[(size)-1] = 0;           \
    } while (0)

#ifdef RELEASE
    #define ASSERT(x)   do { \
        if (!(x)) errorPrint("%s() LN%d, %s\n", \
            __func__, __LINE__, "assertion");} while(0)
#else
    #include <assert.h>
    #define ASSERT(x)   do { assert(x); } while(0)
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

#endif // __TOOLSDEF_H_
