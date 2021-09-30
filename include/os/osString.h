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

#ifndef TDENGINE_OS_STRING_H
#define TDENGINE_OS_STRING_H

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  #define tstrdup(str) _strdup(str)
  #define tstrndup(str, size) _strndup(str, size)  
  int32_t tgetline(char **lineptr, size_t *n, FILE *stream);
  int32_t twcslen(const wchar_t *wcs);
#else
  #define tstrdup(str) strdup(str)
  #define tstrndup(str, size) strndup(str, size)
  #define tgetline(lineptr, n, stream) getline(lineptr, n, stream)
  #define twcslen wcslen
#endif

#define tstrncpy(dst, src, size) \
  do {                              \
    strncpy((dst), (src), (size));  \
    (dst)[(size)-1] = 0;            \
  } while (0)

int64_t taosStr2int64(char *str);

// USE_LIBICONV
int32_t taosUcs4ToMbs(void *ucs4, int32_t ucs4_max_len, char *mbs);
bool    taosMbsToUcs4(char *mbs, size_t mbs_len, char *ucs4, int32_t ucs4_max_len, int32_t *len);
int32_t tasoUcs4Compare(void *f1_ucs4, void *f2_ucs4, int32_t bytes);
bool    taosValidateEncodec(const char *encodec);
char *  taosCharsetReplace(char *charsetstr);

#ifdef __cplusplus
}
#endif

#endif
