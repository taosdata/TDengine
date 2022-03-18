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

// If the error is in a third-party library, place this header file under the third-party library header file.
#ifndef ALLOW_FORBID_FUNC
    #define iconv_open ICONV_OPEN_FUNC_TAOS_FORBID
    #define iconv_close ICONV_CLOSE_FUNC_TAOS_FORBID
    #define iconv ICONV_FUNC_TAOS_FORBID
#endif

typedef int32_t TdUcs4;

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
bool    taosValidateEncodec(const char *encodec);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_STRING_H_*/
