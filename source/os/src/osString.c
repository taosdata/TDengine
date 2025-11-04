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

char *tstrdup(const char *str) {
  if (str == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
#ifdef WINDOWS
  return _strdup(str);
#else
  char *p = strdup(str);
  if (NULL == p) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return p;

#endif
}

#ifdef WINDOWS

// No errors are expected to occur
char *strsep(char **stringp, const char *delim) {
  if (stringp == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  char       *s;
  const char *spanp;
  int32_t     c, sc;
  char       *tok;
  if ((s = *stringp) == NULL) return (NULL);
  if (delim == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  for (tok = s;;) {
    c = *s++;
    spanp = delim;
    do {
      if ((sc = *spanp++) == c) {
        if (c == 0)
          s = NULL;
        else
          s[-1] = 0;
        *stringp = s;
        return (tok);
      }
    } while (sc != 0);
  }
  /* NOTREACHED */
}
/* Duplicate a string, up to at most size characters */
char *taosStrndupi(const char *s, int64_t size) {
  if (s == NULL) return NULL;
  size_t l;
  char  *s2;
  l = strlen(s);
  if (l > size) l = size;
  s2 = malloc(l + 1);
  if (s2) {
    tstrncpy(s2, s, l + 1);
    s2[l] = '\0';
  } else {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return s2;
}
/* Copy no more than N characters of SRC to DEST, returning the address of
   the terminating '\0' in DEST, if any, or else DEST + N.  */
char *stpncpy(char *dest, const char *src, int n) {
  if (dest == NULL || src == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  size_t size = strnlen(src, n);
  memcpy(dest, src, size);
  dest += size;
  if (size == n) return dest;
  return memset(dest, '\0', n - size);
}
#elif defined(TD_ASTRA)
/* Copy no more than N characters of SRC to DEST, returning the address of
   the terminating '\0' in DEST, if any, or else DEST + N.  */
char *stpncpy(char *dest, const char *src, int n) {
  if (dest == NULL || src == NULL) { 
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  if (n == 0) {
    return dest;
  }
  char *orig_dest = dest;
  const char *end = (const char *)memchr(src, '\0', n);
  size_t      len = (end != NULL) ? (size_t)(end - src) : n;
  memcpy(dest, src, len);
  if (len < n) {
    memset(dest + len, '\0', n - len);
  }
  return orig_dest + len;
}
char *taosStrndupi(const char *s, int64_t size) {
  if (s == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }

  const char *end = (const char *)memchr(s, '\0', size);
  size_t      actual_len = (end != NULL) ? (size_t)(end - s) : (size_t)size;

  char *p = (char *)malloc(actual_len + 1);
  if (p == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  memcpy(p, s, actual_len);
  p[actual_len] = '\0';

  return p;
}
#else
char *taosStrndupi(const char *s, int64_t size) {
  if (s == NULL) {
    return NULL;
  }
  char *p = strndup(s, size);
  if (NULL == p) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return p;
}
#endif

char *tstrndup(const char *str, int64_t size) {
#if defined(WINDOWS) || defined(TD_ASTRA)
  return taosStrndupi(str, size);
#else
  char *p = strndup(str, size);
  if (str != NULL && NULL == p) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return p;

#endif
}

int32_t taosStr2int64(const char *str, int64_t *val) {
  if (str == NULL || val == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SET_ERRNO(0);
  char   *endptr = NULL;
  int64_t ret = strtoll(str, &endptr, 10);
  if (ERRNO != 0) {
    return TAOS_SYSTEM_ERROR(ERRNO);
  } else {
    if (endptr == str) {
      return TSDB_CODE_INVALID_PARA;
    }
    *val = ret;
    return 0;
  }
}

int32_t taosStr2int32(const char *str, int32_t *val) {
  OS_PARAM_CHECK(str);
  OS_PARAM_CHECK(val);
  int64_t tmp = 0;
  int32_t code = taosStr2int64(str, &tmp);
  if (code) {
    return code;
  } else if (tmp > INT32_MAX || tmp < INT32_MIN) {
    return TAOS_SYSTEM_ERROR(ERANGE);
  } else {
    *val = (int32_t)tmp;
    return 0;
  }
}
int32_t taosStr2int16(const char *str, int16_t *val) {
  OS_PARAM_CHECK(str);
  OS_PARAM_CHECK(val);
  int64_t tmp = 0;
  int32_t code = taosStr2int64(str, &tmp);
  if (code) {
    return code;
  } else if (tmp > INT16_MAX || tmp < INT16_MIN) {
    return TAOS_SYSTEM_ERROR(ERANGE);
  } else {
    *val = (int16_t)tmp;
    return 0;
  }
}

int32_t taosStr2int8(const char *str, int8_t *val) {
  OS_PARAM_CHECK(str);
  OS_PARAM_CHECK(val);
  int64_t tmp = 0;
  int32_t code = taosStr2int64(str, &tmp);
  if (code) {
    return code;
  } else if (tmp > INT8_MAX || tmp < INT8_MIN) {
    return TAOS_SYSTEM_ERROR(ERANGE);
  } else {
    *val = (int8_t)tmp;
    return 0;
  }
}

int32_t taosStr2Uint64(const char *str, uint64_t *val) {
  if (str == NULL || val == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  char *endptr = NULL;
  SET_ERRNO(0);
  uint64_t ret = strtoull(str, &endptr, 10);

  if (ERRNO != 0) {
    return TAOS_SYSTEM_ERROR(ERRNO);
  } else {
    if (endptr == str) {
      return TSDB_CODE_INVALID_PARA;
    }
    *val = ret;
    return 0;
  }
}

int32_t taosStr2Uint32(const char *str, uint32_t *val) {
  OS_PARAM_CHECK(str);
  OS_PARAM_CHECK(val);
  uint64_t tmp = 0;
  int32_t  code = taosStr2Uint64(str, &tmp);
  if (code) {
    return code;
  } else if (tmp > UINT32_MAX) {
    return TAOS_SYSTEM_ERROR(ERANGE);
  } else {
    *val = (int32_t)tmp;
    return 0;
  }
}

int32_t taosStr2Uint16(const char *str, uint16_t *val) {
  OS_PARAM_CHECK(str);
  OS_PARAM_CHECK(val);
  uint64_t tmp = 0;
  int32_t  code = taosStr2Uint64(str, &tmp);
  if (code) {
    return code;
  } else if (tmp > UINT16_MAX) {
    return TAOS_SYSTEM_ERROR(ERANGE);
  } else {
    *val = (int16_t)tmp;
    return 0;
  }
}

int32_t taosStr2Uint8(const char *str, uint8_t *val) {
  OS_PARAM_CHECK(str);
  OS_PARAM_CHECK(val);
  uint64_t tmp = 0;
  int32_t  code = taosStr2Uint64(str, &tmp);
  if (code) {
    return code;
  } else if (tmp > UINT8_MAX) {
    return TAOS_SYSTEM_ERROR(ERANGE);
  } else {
    *val = (int8_t)tmp;
    return 0;
  }
}

int32_t taosUcs4Compare(TdUcs4 *f1_ucs4, TdUcs4 *f2_ucs4, int32_t bytes) {
  if ((f1_ucs4 == NULL || f2_ucs4 == NULL)) {
    return TSDB_CODE_INVALID_PARA;
  }
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

  //#if 0
  //  int32_t ucs4_max_len = bytes + 4;
  //  char *f1_mbs = taosMemoryCalloc(bytes, 1);
  //  char *f2_mbs = taosMemoryCalloc(bytes, 1);
  //  if (taosUcs4ToMbs(f1_ucs4, ucs4_max_len, f1_mbs) < 0) {
  //    return -1;
  //  }
  //  if (taosUcs4ToMbs(f2_ucs4, ucs4_max_len, f2_mbs) < 0) {
  //    return -1;
  //  }
  //  int32_t ret = strcmp(f1_mbs, f2_mbs);
  //  taosMemoryFree(f1_mbs);
  //  taosMemoryFree(f2_mbs);
  //  return ret;
  //#endif
}

#if 0
int32_t tasoUcs4Copy(TdUcs4 *target_ucs4, TdUcs4 *source_ucs4, int32_t len_ucs4) {
  if (target_ucs4 == NULL || source_ucs4 == NULL || len_ucs4 <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }
#ifndef TD_ASTRA
  if (taosMemorySize(target_ucs4) < len_ucs4 * sizeof(TdUcs4)) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  (void)memcpy(target_ucs4, source_ucs4, len_ucs4 * sizeof(TdUcs4));

  return TSDB_CODE_SUCCESS;
#else
  return TSDB_CODE_APP_ERROR;
#endif
}
#endif 

iconv_t taosAcquireConv(int32_t *idx, ConvType type, void* charsetCxt) {
#ifndef DISALLOW_NCHAR_WITHOUT_ICONV
  if(idx == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return (iconv_t)NULL;
  }

  int32_t retryLimit = 100, i = 0;

  if (charsetCxt == NULL){
    charsetCxt = tsCharsetCxt;
  }
  SConvInfo *info = (SConvInfo *)charsetCxt;
  if (info == NULL) {
    *idx = -1;
    if (type == M2C) {
      iconv_t c = iconv_open(DEFAULT_UNICODE_ENCODEC, "UTF-8");
      if ((iconv_t)-1 == c) {
        terrno = TAOS_SYSTEM_ERROR(ERRNO);
      }
      return c;
    } else {
      iconv_t c = iconv_open("UTF-8", DEFAULT_UNICODE_ENCODEC);
      if ((iconv_t)-1 == c) {
        terrno = TAOS_SYSTEM_ERROR(ERRNO);
      }
      return c;
    }
  }

  while (true) {
    if (i++ >= retryLimit) {
      uError("taosAcquireConv retry limit reached");
      return (iconv_t)-1;
    }

    int32_t used = atomic_add_fetch_32(&info->convUsed[type], 1);
    if (used > info->gConvMaxNum[type]) {
      (void)atomic_sub_fetch_32(&info->convUsed[type], 1);
      (void)sched_yield();
      continue;
    }

    break;
  }

  int32_t startId = ((uint32_t)(taosGetSelfPthreadId())) % info->gConvMaxNum[type];
  while (true) {
    if (info->gConv[type][startId].inUse) {
      startId = (startId + 1) % info->gConvMaxNum[type];
      continue;
    }

    int8_t old = atomic_val_compare_exchange_8(&info->gConv[type][startId].inUse, 0, 1);
    if (0 == old) {
      break;
    }
  }

  *idx = startId;
  if ((iconv_t)0 == info->gConv[type][startId].conv) {
    return (iconv_t)-1;
  } else {
    return info->gConv[type][startId].conv;
  }
#else
  terrno = TSDB_CODE_APP_ERROR;
  return (iconv_t)-1;
#endif
}

void taosReleaseConv(int32_t idx, iconv_t conv, ConvType type, void *charsetCxt) {
#ifndef DISALLOW_NCHAR_WITHOUT_ICONV
  if (idx < 0) {
    (void)iconv_close(conv);
    return;
  }

  if (charsetCxt == NULL) {
    charsetCxt = tsCharsetCxt;
  }
  SConvInfo *info = (SConvInfo *)charsetCxt;

  atomic_store_8(&info->gConv[type][idx].inUse, 0);
  (void)atomic_sub_fetch_32(&info->convUsed[type], 1);
#endif
}

bool taosMbsToUcs4(const char *mbs, size_t mbsLength, TdUcs4 *ucs4, int32_t ucs4_max_len, int32_t *len, void* charsetCxt) {
  if (ucs4_max_len == 0) {
    return true;
  }
  if (ucs4_max_len < 0 || mbs == NULL || ucs4 == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return false;
  }
#ifdef DISALLOW_NCHAR_WITHOUT_ICONV
  printf("Nchar cannot be read and written without iconv, please install iconv library and recompile.\n");
  terrno = TSDB_CODE_APP_ERROR;
  return false;
#else
  (void)memset(ucs4, 0, ucs4_max_len);

  int32_t idx = -1;
  iconv_t conv = taosAcquireConv(&idx, M2C, charsetCxt);
  if ((iconv_t)-1 == conv) {
    return false;
  }

  size_t ucs4_input_len = mbsLength;
  size_t outLeft = ucs4_max_len;
  if (iconv(conv, (char **)&mbs, &ucs4_input_len, (char **)&ucs4, &outLeft) == -1) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    taosReleaseConv(idx, conv, M2C, charsetCxt);
    return false;
  }

  taosReleaseConv(idx, conv, M2C, charsetCxt);
  if (len != NULL) {
    *len = (int32_t)(ucs4_max_len - outLeft);
    if (*len < 0) {
      // can not happen
      terrno = TSDB_CODE_APP_ERROR;
      return false;
    }
  }

  return true;
#endif
}

// if success, return the number of bytes written to mbs ( >= 0)
// otherwise return error code ( < 0)
int32_t taosUcs4ToMbs(TdUcs4 *ucs4, int32_t ucs4_max_len, char *mbs, void* charsetCxt) {
  if (ucs4_max_len == 0) {
    return 0;
  }
  if (ucs4_max_len < 0 || ucs4 == NULL || mbs == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
#ifdef DISALLOW_NCHAR_WITHOUT_ICONV
  printf("Nchar cannot be read and written without iconv, please install iconv library and recompile.\n");
  terrno = TSDB_CODE_APP_ERROR;
  return terrno;
#else

  int32_t idx = -1;
  int32_t code = 0;
  iconv_t conv = taosAcquireConv(&idx, C2M, charsetCxt);
  if ((iconv_t)-1 == conv) {
    return terrno;
  }

  size_t ucs4_input_len = ucs4_max_len;
  size_t outLen = ucs4_max_len;
  if (iconv(conv, (char **)&ucs4, &ucs4_input_len, &mbs, &outLen) == -1) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    taosReleaseConv(idx, conv, C2M, charsetCxt);
    terrno = code;
    return code;
  }

  taosReleaseConv(idx, conv, C2M, charsetCxt);

  return (int32_t)(ucs4_max_len - outLen);
#endif
}

// if success, return the number of bytes written to mbs ( >= 0)
// otherwise return error code ( < 0)
int32_t taosUcs4ToMbsEx(TdUcs4 *ucs4, int32_t ucs4_max_len, char *mbs, iconv_t conv) {
  if (ucs4_max_len == 0) {
    return 0;
  }
  if (ucs4_max_len < 0 || ucs4 == NULL || mbs == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
#ifdef DISALLOW_NCHAR_WITHOUT_ICONV
  printf("Nchar cannot be read and written without iconv, please install iconv library and recompile.\n");
  terrno = TSDB_CODE_APP_ERROR;
  return terrno;
#else

  size_t ucs4_input_len = ucs4_max_len;
  size_t outLen = ucs4_max_len;
  if (iconv(conv, (char **)&ucs4, &ucs4_input_len, &mbs, &outLen) == -1) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return terrno;
  }

  return (int32_t)(ucs4_max_len - outLen);
#endif
}

bool taosValidateEncodec(const char *encodec) {
  if (encodec == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return false;
  }
#ifdef DISALLOW_NCHAR_WITHOUT_ICONV
  printf("Nchar cannot be read and written without iconv, please install iconv library and recompile.\n");
  terrno = TSDB_CODE_APP_ERROR;
  return false;
#else
  iconv_t cd = iconv_open(encodec, DEFAULT_UNICODE_ENCODEC);
  if (cd == (iconv_t)(-1)) {
    terrno = TAOS_SYSTEM_ERROR(ERRNO);
    return false;
  }

  (void)iconv_close(cd);
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

// dst buffer size should be at least 2*len + 1
int32_t taosHexEncode(const unsigned char *src, char *dst, int32_t len, int32_t bufSize) {
  if (!dst || !src || bufSize <= 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  for (int32_t i = 0; i < len; ++i) {
    (void)snprintf(dst + i * 2, bufSize - i * 2, "%02x", src[i]);
  }

  return 0;
}

int32_t taosHexDecode(const char *src, char *dst, int32_t len) {
  if (!src || !dst || len <= 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  uint8_t hn, ln, out;
  for (int i = 0, j = 0; i < len * 2; i += 2, ++j) {
    hn = src[i] > '9' ? src[i] - 'a' + 10 : src[i] - '0';
    ln = src[i + 1] > '9' ? src[i + 1] - 'a' + 10 : src[i + 1] - '0';

    out = (hn << 4) | ln;
    (void)memcpy(dst + j, &out, 1);
  }

  return 0;
}

#ifdef TD_ASTRA
#include <wchar.h>
#include <stdbool.h>
#include <stdint.h>

typedef struct {
    uint32_t start;
    uint32_t end;
} SUnicodeRange;

static const SUnicodeRange __eaw_ranges[] = {
    {0x1100, 0x115F},   {0x2329, 0x232A}, {0x2E80, 0x303E}, {0x3040, 0xA4CF},
    {0xAC00, 0xD7AF},   {0xF900, 0xFAFF}, {0xFE10, 0xFE19}, {0x20000, 0x2FFFD},
    {0x30000, 0x3FFFD}, {0xFF00, 0xFFEF}, {0xA000, 0xA48F}, {0x13A0, 0x13FF}
};

static const int __n_eaw_ranges = sizeof(__eaw_ranges) / sizeof(__eaw_ranges[0]);

static bool isEawWideChar(wchar_t code_point) {
    int low = 0;
    int high = __n_eaw_ranges - 1;

    while (low <= high) {
        int mid = (low + high) / 2;
        if (code_point < __eaw_ranges[mid].start) {
            high = mid - 1;
        } else if (code_point > __eaw_ranges[mid].end) {
            low = mid + 1;
        } else {
            return true;
        }
    }
    return false;
}

int wcwidth(wchar_t c) {
    if (c < 0x20 || (c >= 0x7F && c <= 0x9F)) {
        return 0;
    }
    if (isEawWideChar(c)) {
        return 2;
    }
    if (c >= 0x0300 && c <= 0x036F) {
        return 0;
    }
    return 1;
}

#endif

int32_t taosWcharWidth(TdWchar wchar) { return wcwidth(wchar); }

int32_t taosWcharsWidth(TdWchar *pWchar, int32_t size) {
  if (pWchar == NULL || size <= 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
#ifndef TD_ASTRA
  return wcswidth(pWchar, size);
#else
  int32_t width = 0;
  for (int32_t i = 0; i < size; ++i) {
    width += wcwidth(pWchar[i]);
  }
  return width;
#endif
}

int32_t taosMbToWchar(TdWchar *pWchar, const char *pStr, int32_t size) {
  if (pWchar == NULL || pStr == NULL || size <= 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
#ifndef TD_ASTRA
  return mbtowc(pWchar, pStr, size);
#else
  return mbrtowc(pWchar, &pStr, size, NULL);
#endif
}

#ifdef TD_ASTRA
size_t mbstowcs(wchar_t *dest, const char *src, size_t n) {
  if (src == NULL) {
    return 0;
  }

  size_t count = 0;
  int    result;
  while (*src && count < n) {
    result = mbrtowc(dest, src, MB_CUR_MAX, NULL);
    if (result == -1 || result == 0) {
      return -1;
    }
    src += result;
    dest++;
    count++;
  }

  if (count < n) {
    *dest = L'\0';
  }
  return count;
}
#endif

int32_t taosMbsToWchars(TdWchar *pWchars, const char *pStrs, int32_t size) {
  if (pWchars == NULL || pStrs == NULL || size <= 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  return mbstowcs(pWchars, pStrs, size);
}

int32_t taosWcharToMb(char *pStr, TdWchar wchar) {
  OS_PARAM_CHECK(pStr);
#ifndef TD_ASTRA
  return wctomb(pStr, wchar);
#else
  return wcrtomb(pStr, wchar, NULL);
#endif
}

char *taosStrCaseStr(const char *str, const char *pattern) {
  if (str == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return NULL;
  }
  if (!pattern || !*pattern) return (char *)str;

  size_t i;
  for (; *str; str++) {
    if (toupper(*str) == toupper(*pattern)) {
      for (i = 1;; i++) {
        if (!pattern[i]) return (char *)str;
        if (toupper(str[i]) != toupper(pattern[i])) break;
      }
    }
  }
  return NULL;
}

int64_t taosStr2Int64(const char *str, char **pEnd, int32_t radix) {
  if (str == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }
  int64_t tmp = strtoll(str, pEnd, radix);
#if defined(DARWIN) || defined(_ALPINE)
  if (ERRNO == EINVAL) SET_ERRNO(0);
#endif
  return tmp;
}

uint64_t taosStr2UInt64(const char *str, char **pEnd, int32_t radix) {
  if (str == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }
  uint64_t tmp = strtoull(str, pEnd, radix);
#if defined(DARWIN) || defined(_ALPINE)
  if (ERRNO == EINVAL) SET_ERRNO(0);
#endif
  return tmp;
}

int32_t taosStr2Int32(const char *str, char **pEnd, int32_t radix) {
  if (str == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }
  int32_t tmp = strtol(str, pEnd, radix);
#if defined(DARWIN) || defined(_ALPINE)
  if (ERRNO == EINVAL) SET_ERRNO(0);
#endif
  return tmp;
}

uint32_t taosStr2UInt32(const char *str, char **pEnd, int32_t radix) {
  if (str == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }
  uint32_t tmp = strtol(str, pEnd, radix);
#if defined(DARWIN) || defined(_ALPINE)
  if (ERRNO == EINVAL) SET_ERRNO(0);
#endif
  return tmp;
}

int16_t taosStr2Int16(const char *str, char **pEnd, int32_t radix) {
  if (str == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }
  int32_t tmp = strtol(str, pEnd, radix);
#if defined(DARWIN) || defined(_ALPINE)
  if (ERRNO == EINVAL) SET_ERRNO(0);
#endif
  return (int16_t)tmp;
}

uint16_t taosStr2UInt16(const char *str, char **pEnd, int32_t radix) {
  if (str == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }
  uint32_t tmp = strtoul(str, pEnd, radix);
#if defined(DARWIN) || defined(_ALPINE)
  if (ERRNO == EINVAL) SET_ERRNO(0);
#endif
  return (uint16_t)tmp;
}

int8_t taosStr2Int8(const char *str, char **pEnd, int32_t radix) {
  if (str == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }
  int32_t tmp = strtol(str, pEnd, radix);
  return tmp;
}

uint8_t taosStr2UInt8(const char *str, char **pEnd, int32_t radix) {
  if (str == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }
  uint32_t tmp = strtoul(str, pEnd, radix);
#if defined(DARWIN) || defined(_ALPINE)
  if (ERRNO == EINVAL) SET_ERRNO(0);
#endif
  return tmp;
}

double taosStr2Double(const char *str, char **pEnd) {
  if (str == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }
  double tmp = strtod(str, pEnd);
  return tmp;
}

float taosStr2Float(const char *str, char **pEnd) {
  if (str == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }
  float tmp = strtof(str, pEnd);
  return tmp;
}

#define HEX_PREFIX_LEN 2  // \x
bool isHex(const char *z, uint32_t n) {
  if (n < HEX_PREFIX_LEN) return false;
  if (z[0] == '\\' && z[1] == 'x') return true;
  return false;
}

bool isValidateHex(const char *z, uint32_t n) {
  if (!z) {
    terrno = TSDB_CODE_INVALID_PARA;
    return false;
  }
  if ((n & 1) != 0) return false;
  for (size_t i = HEX_PREFIX_LEN; i < n; i++) {
    if (isxdigit(z[i]) == 0) {
      return false;
    }
  }
  return true;
}

int32_t taosHex2Ascii(const char *z, uint32_t n, void **data, uint32_t *size) {
  OS_PARAM_CHECK(z);
  OS_PARAM_CHECK(data);
  OS_PARAM_CHECK(size);
  n -= HEX_PREFIX_LEN;  // remove 0x
  z += HEX_PREFIX_LEN;
  *size = n / HEX_PREFIX_LEN;
  if (*size == 0) {
    if (!(*data = taosStrdup(""))) {
      return terrno;
    }
    return 0;
  }

  uint8_t *tmp = (uint8_t *)taosMemoryCalloc(*size, 1);
  if (tmp == NULL) {
    return terrno;
  }

  int8_t   num = 0;
  uint8_t *byte = tmp + *size - 1;

  for (int i = n - 1; i >= 0; i--) {
    if (z[i] >= 'a') {
      *byte |= ((uint8_t)(10 + (z[i] - 'a')) << (num * 4));
    } else if (z[i] >= 'A') {
      *byte |= ((uint8_t)(10 + (z[i] - 'A')) << (num * 4));
    } else {
      *byte |= ((uint8_t)(z[i] - '0') << (num * 4));
    }
    if (num == 1) {
      byte--;
      num = 0;
    } else {
      num++;
    }
  }
  *data = tmp;

  return 0;
}

// int32_t taosBin2Ascii(const char *z, uint32_t n, void** data, uint32_t* size){
//
//   for (i = 2; isdigit(z[i]) || (z[i] >= 'a' && z[i] <= 'f') || (z[i] >= 'A' && z[i] <= 'F'); ++i) {
//   }
//
//   n -= 2;   // remove 0b
//   z += 2;
//   *size = n%8 == 0 ? n/8 : n/8 + 1;
//   uint8_t* tmp = (uint8_t*)taosMemoryCalloc(*size, 1);
//   if(tmp == NULL) return -1;
//   int8_t   num = 0;
//   uint8_t *byte = tmp + *size - 1;
//
//   for (int i = n - 1; i >= 0; i--) {
//     *byte |= ((uint8_t)(z[i] - '0') << num);
//     if (num == 7) {
//       byte--;
//       num = 0;
//     } else {
//       num++;
//     }
//   }
//   *data = tmp;
//   return 0;
// }

static char valueOf(uint8_t symbol) {
  switch (symbol) {
    case 0:
      return '0';
    case 1:
      return '1';
    case 2:
      return '2';
    case 3:
      return '3';
    case 4:
      return '4';
    case 5:
      return '5';
    case 6:
      return '6';
    case 7:
      return '7';
    case 8:
      return '8';
    case 9:
      return '9';
    case 10:
      return 'A';
    case 11:
      return 'B';
    case 12:
      return 'C';
    case 13:
      return 'D';
    case 14:
      return 'E';
    case 15:
      return 'F';
    default: {
      return -1;
    }
  }
}

int32_t taosAscii2Hex(const char *z, uint32_t n, void **data, uint32_t *size) {
  *size = n * 2 + HEX_PREFIX_LEN;
  uint8_t *tmp = (uint8_t *)taosMemoryCalloc(*size + 1, 1);
  if (tmp == NULL) {
    return terrno;
  }

  *data = tmp;
  *(tmp++) = '\\';
  *(tmp++) = 'x';
  for (int i = 0; i < n; i++) {
    uint8_t val = z[i];
    tmp[i * 2] = valueOf(val >> 4);
    tmp[i * 2 + 1] = valueOf(val & 0x0F);
  }

  return 0;
}

int64_t tsnprintf(char *dst, int64_t size, const char *format, ...) {
  if (dst == NULL || format == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return 0;
  }
  if (size <= 0) return 0;
  if (size == 1) {
    dst[0] = '\0';
    return 0;
  }
  if (size > SIZE_MAX) {
    size = SIZE_MAX;
  }

  int64_t ret;
  va_list args;
  va_start(args, format);
  ret = vsnprintf(dst, size, format, args);
  va_end(args);
  if (ret >= size) {
    return size - 1;
  } else {
    return ret;
  }
}
