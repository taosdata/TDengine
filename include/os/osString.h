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

#ifndef _TD_OS_STRING_H_
#define _TD_OS_STRING_H_

#ifdef __cplusplus
extern "C" {
#endif

#ifdef TD_ASTRA
#define DISALLOW_NCHAR_WITHOUT_ICONV
#endif

typedef wchar_t TdWchar;
typedef int32_t TdUcs4;
#if !defined(DISALLOW_NCHAR_WITHOUT_ICONV)// && defined(DARWIN)
#include "iconv.h"
#else
typedef void *iconv_t;
#endif
typedef enum { M2C = 0, C2M, CM_NUM } ConvType;

typedef struct {
  iconv_t conv;
  int8_t  inUse;
} SConv;

typedef struct {
  SConv  *gConv[CM_NUM];
  int32_t convUsed[CM_NUM];
  int32_t gConvMaxNum[CM_NUM];
  char    charset[TD_CHARSET_LEN];
} SConvInfo;

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following section.
#ifndef ALLOW_FORBID_FUNC
#define iconv_open  ICONV_OPEN_FUNC_TAOS_FORBID
#define iconv_close ICONV_CLOSE_FUNC_TAOS_FORBID
#define iconv       ICONV_FUNC_TAOS_FORBID
#define wcwidth     WCWIDTH_FUNC_TAOS_FORBID
#define wcswidth    WCSWIDTH_FUNC_TAOS_FORBID
#define mbtowc      MBTOWC_FUNC_TAOS_FORBID
#define mbstowcs    MBSTOWCS_FUNC_TAOS_FORBID
#define wctomb      WCTOMB_FUNC_TAOS_FORBID
#define wcstombs    WCSTOMBS_FUNC_TAOS_FORBID
#define wcsncpy     WCSNCPY_FUNC_TAOS_FORBID
#define wchar_t     WCHAR_T_TYPE_TAOS_FORBID
#define strcasestr  STR_CASE_STR_FORBID
#define strtoll     STR_TO_LL_FUNC_TAOS_FORBID
#define strtoull    STR_TO_ULL_FUNC_TAOS_FORBID
#define strtol      STR_TO_L_FUNC_TAOS_FORBID
#define strtoul     STR_TO_UL_FUNC_TAOS_FORBID
#define strtod      STR_TO_LD_FUNC_TAOS_FORBID
#define strtold     STR_TO_D_FUNC_TAOS_FORBID
#define strtof      STR_TO_F_FUNC_TAOS_FORBID

#ifdef strndup
#undef strndup
#endif
#define strndup STR_TO_F_FUNC_TAOS_FORBID

#endif

#define tstrncpy(dst, src, size)         \
  do {                                   \
    (void)strncpy((dst), (src), (size)); \
    (dst)[(size)-1] = 0;                 \
  } while (0)

int64_t tsnprintf(char *dst, int64_t size, const char *format, ...);
#define TAOS_STRCPY(_dst, _src)         ((void)strcpy(_dst, _src))
#define TAOS_STRNCPY(_dst, _src, _size) ((void)strncpy(_dst, _src, _size))
#define TAOS_STRCAT(_dst, _src)         ((void)strcat(_dst, _src))
#define TAOS_STRNCAT(_dst, _src, len)   ((void)strncat(_dst, _src, len))

char   *tstrdup(const char *src);
char   *tstrndup(const char *str, int64_t size);
int32_t taosUcs4len(TdUcs4 *ucs4);
int32_t taosStr2int64(const char *str, int64_t *val);
int32_t taosStr2int32(const char *str, int32_t *val);
int32_t taosStr2int16(const char *str, int16_t *val);
int32_t taosStr2int8(const char *str, int8_t *val);

int32_t taosStr2Uint64(const char *str, uint64_t *val);
int32_t taosStr2Uint32(const char *str, uint32_t *val);
int32_t taosStr2Uint16(const char *str, uint16_t *val);
int32_t taosStr2Uint8(const char *str, uint8_t *val);

iconv_t taosAcquireConv(int32_t *idx, ConvType type, void* charsetCxt);
void    taosReleaseConv(int32_t idx, iconv_t conv, ConvType type, void* charsetCxt);
int32_t taosUcs4ToMbs(TdUcs4 *ucs4, int32_t ucs4_max_len, char *mbs, void* charsetCxt);
int32_t taosUcs4ToMbsEx(TdUcs4 *ucs4, int32_t ucs4_max_len, char *mbs, iconv_t conv);
bool    taosMbsToUcs4(const char *mbs, size_t mbs_len, TdUcs4 *ucs4, int32_t ucs4_max_len, int32_t *len, void* charsetCxt);
int32_t taosUcs4Compare(TdUcs4 *f1_ucs4, TdUcs4 *f2_ucs4, int32_t bytes);
int32_t taosUcs4Copy(TdUcs4 *target_ucs4, TdUcs4 *source_ucs4, int32_t len_ucs4);
bool    taosValidateEncodec(const char *encodec);
int32_t taosHexEncode(const unsigned char *src, char *dst, int32_t len, int32_t bufSize);
int32_t taosHexDecode(const char *src, char *dst, int32_t len);

int32_t taosWcharWidth(TdWchar wchar);
int32_t taosWcharsWidth(TdWchar *pWchar, int32_t size);
int32_t taosMbToWchar(TdWchar *pWchar, const char *pStr, int32_t size);
int32_t taosMbsToWchars(TdWchar *pWchars, const char *pStrs, int32_t size);
int32_t taosWcharToMb(char *pStr, TdWchar wchar);

char *taosStrCaseStr(const char *str, const char *pattern);

int64_t  taosStr2Int64(const char *str, char **pEnd, int32_t radix);
uint64_t taosStr2UInt64(const char *str, char **pEnd, int32_t radix);
int32_t  taosStr2Int32(const char *str, char **pEnd, int32_t radix);
uint32_t taosStr2UInt32(const char *str, char **pEnd, int32_t radix);
int16_t  taosStr2Int16(const char *str, char **pEnd, int32_t radix);
uint16_t taosStr2UInt16(const char *str, char **pEnd, int32_t radix);
int8_t   taosStr2Int8(const char *str, char **pEnd, int32_t radix);
uint8_t  taosStr2UInt8(const char *str, char **pEnd, int32_t radix);
double   taosStr2Double(const char *str, char **pEnd);
float    taosStr2Float(const char *str, char **pEnd);
int32_t  taosHex2Ascii(const char *z, uint32_t n, void **data, uint32_t *size);
int32_t  taosAscii2Hex(const char *z, uint32_t n, void **data, uint32_t *size);
// int32_t  taosBin2Ascii(const char *z, uint32_t n, void** data, uint32_t* size);
bool isHex(const char *z, uint32_t n);
bool isValidateHex(const char *z, uint32_t n);

#ifdef TD_ASTRA
static FORCE_INLINE size_t strnlen(const char *s, size_t maxlen) {
  const char *end = (const char *)memchr(s, '\0', maxlen);
  return end ? (size_t)(end - s) : maxlen;
}
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_STRING_H_*/
