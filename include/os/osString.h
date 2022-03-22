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

typedef wchar_t TdWchar;
typedef int32_t TdUcs4;

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following section.
#ifndef ALLOW_FORBID_FUNC
    #define iconv_open ICONV_OPEN_FUNC_TAOS_FORBID
    #define iconv_close ICONV_CLOSE_FUNC_TAOS_FORBID
    #define iconv ICONV_FUNC_TAOS_FORBID
    #define wcwidth WCWIDTH_FUNC_TAOS_FORBID
    #define wcswidth WCSWIDTH_FUNC_TAOS_FORBID
    #define mbtowc MBTOWC_FUNC_TAOS_FORBID
    #define mbstowcs MBSTOWCS_FUNC_TAOS_FORBID
    #define wctomb WCTOMB_FUNC_TAOS_FORBID
    #define wcstombs WCSTOMBS_FUNC_TAOS_FORBID
    #define wcsncpy WCSNCPY_FUNC_TAOS_FORBID
    #define wchar_t WCHAR_T_TYPE_TAOS_FORBID
#endif

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  #define tstrdup(str) _strdup(str)
#else
  #define tstrdup(str) strdup(str)
#endif

#define tstrncpy(dst, src, size) \
  do {                              \
    strncpy((dst), (src), (size));  \
    (dst)[(size)-1] = 0;            \
  } while (0)

int32_t taosUcs4len(TdUcs4 *ucs4);
int64_t taosStr2int64(const char *str);

int32_t taosUcs4ToMbs(TdUcs4 *ucs4, int32_t ucs4_max_len, char *mbs);
bool    taosMbsToUcs4(const char *mbs, size_t mbs_len, TdUcs4 *ucs4, int32_t ucs4_max_len, int32_t *len);
int32_t tasoUcs4Compare(TdUcs4 *f1_ucs4, TdUcs4 *f2_ucs4, int32_t bytes);
TdUcs4* tasoUcs4Copy(TdUcs4 *target_ucs4, TdUcs4 *source_ucs4, int32_t len_ucs4);
bool    taosValidateEncodec(const char *encodec);

int32_t taosWcharWidth(TdWchar wchar);
int32_t taosWcharsWidth(TdWchar *pWchar, int32_t size);
int32_t taosMbToWchar(TdWchar *pWchar, const char *pStr, int32_t size);
int32_t taosMbsToWchars(TdWchar *pWchars, const char *pStrs, int32_t size);
int32_t taosWcharToMb(char *pStr, TdWchar wchar);
int32_t taosWcharsToMbs(char *pStrs, TdWchar *pWchars, int32_t size);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_STRING_H_*/
