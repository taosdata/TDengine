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

#define ALLOW_FORBID_FUNC
#define _DEFAULT_SOURCE
#include "os.h"

#ifndef DISALLOW_NCHAR_WITHOUT_ICONV
#include "iconv.h"
#endif

extern int wcwidth(wchar_t c);
extern int wcswidth(const wchar_t *s, size_t n);

int64_t taosStr2int64(const char *str) {
  char *endptr = NULL;
  return strtoll(str, &endptr, 10);
}

int32_t tasoUcs4Compare(TdUcs4 *f1_ucs4, TdUcs4 *f2_ucs4, int32_t bytes) {
  for (int32_t i = 0; i < bytes; i += sizeof(TdUcs4)) {
    int32_t f1 = *(int32_t *)((char *)f1_ucs4 + i);
    int32_t f2 = *(int32_t *)((char *)f2_ucs4 + i);

    if ((f1 == 0 && f2 != 0) || (f1 != 0 && f2 == 0)) {
      return f1 - f2;
    } else if (f1 == 0 && f2 == 0) {
      return 0;
    }

    if (f1 != f2) {
      return f1 - f2;
    }
  }

  return 0;

#if 0
  int32_t ucs4_max_len = bytes + 4;
  char *f1_mbs = taosMemoryCalloc(bytes, 1);
  char *f2_mbs = taosMemoryCalloc(bytes, 1);
  if (taosUcs4ToMbs(f1_ucs4, ucs4_max_len, f1_mbs) < 0) {
    return -1;
  }
  if (taosUcs4ToMbs(f2_ucs4, ucs4_max_len, f2_mbs) < 0) {
    return -1;
  }
  int32_t ret = strcmp(f1_mbs, f2_mbs);
  taosMemoryFree(f1_mbs);
  taosMemoryFree(f2_mbs);
  return ret;
#endif
}


TdUcs4* tasoUcs4Copy(TdUcs4 *target_ucs4, TdUcs4 *source_ucs4, int32_t len_ucs4) {
  assert(taosMemorySize(target_ucs4)>=len_ucs4*sizeof(TdUcs4));
  return memcpy(target_ucs4, source_ucs4, len_ucs4*sizeof(TdUcs4));
}

int32_t taosUcs4ToMbs(TdUcs4 *ucs4, int32_t ucs4_max_len, char *mbs) {
#ifdef DISALLOW_NCHAR_WITHOUT_ICONV
  printf("Nchar cannot be read and written without iconv, please install iconv library and recompile TDengine.\n");
  return -1;
#else
  iconv_t cd = iconv_open(tsCharset, DEFAULT_UNICODE_ENCODEC);
  size_t  ucs4_input_len = ucs4_max_len;
  size_t  outLen = ucs4_max_len;
  if (iconv(cd, (char **)&ucs4, &ucs4_input_len, &mbs, &outLen) == -1) {
    iconv_close(cd);
    return -1;
  }

  iconv_close(cd);
  return (int32_t)(ucs4_max_len - outLen);
#endif
}

bool taosMbsToUcs4(const char *mbs, size_t mbsLength, TdUcs4 *ucs4, int32_t ucs4_max_len, int32_t *len) { 
#ifdef DISALLOW_NCHAR_WITHOUT_ICONV
  printf("Nchar cannot be read and written without iconv, please install iconv library and recompile TDengine.\n");
  return -1;
#else
  memset(ucs4, 0, ucs4_max_len);
  iconv_t cd = iconv_open(DEFAULT_UNICODE_ENCODEC, tsCharset);
  size_t  ucs4_input_len = mbsLength;
  size_t  outLeft = ucs4_max_len;
  if (iconv(cd, (char**)&mbs, &ucs4_input_len, (char**)&ucs4, &outLeft) == -1) {
    iconv_close(cd);
    return false;
  }

  iconv_close(cd);
  if (len != NULL) {
    *len = (int32_t)(ucs4_max_len - outLeft);
    if (*len < 0) {
      return false;
    }
  }

  return true;
#endif
}

bool taosValidateEncodec(const char *encodec) {
#ifdef DISALLOW_NCHAR_WITHOUT_ICONV
  printf("Nchar cannot be read and written without iconv, please install iconv library and recompile TDengine.\n");
  return true;
#else
  iconv_t cd = iconv_open(encodec, DEFAULT_UNICODE_ENCODEC);
  if (cd == (iconv_t)(-1)) {
    return false;
  }

  iconv_close(cd);
  return true;
#endif
}

int32_t taosUcs4len(TdUcs4 *ucs4) {
  TdUcs4 *wstr = (TdUcs4 *)ucs4;
  if (NULL == wstr) {
    return 0;
  }

  int32_t n = 0;
  while (1) {
    if (0 == *wstr++) {
      break;
    }
    n++;
  }

  return n;
}

int32_t taosWcharWidth(TdWchar wchar) { return wcwidth(wchar); }

int32_t taosWcharsWidth(TdWchar *pWchar, int32_t size) { return wcswidth(pWchar, size); }

int32_t taosMbToWchar(TdWchar *pWchar, const char *pStr, int32_t size) { return mbtowc(pWchar, pStr, size); }

int32_t taosMbsToWchars(TdWchar *pWchars, const char *pStrs, int32_t size) { return mbstowcs(pWchars, pStrs, size); }

int32_t taosWcharToMb(char *pStr, TdWchar wchar) { return wctomb(pStr, wchar); }

int32_t taosWcharsToMbs(char *pStrs, TdWchar *pWchars, int32_t size) { return wcstombs(pStrs, pWchars, size); }
