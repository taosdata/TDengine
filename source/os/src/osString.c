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
#ifdef WINDOWS
  return _strdup(str);
#else
  return strdup(str);
#endif
}

#ifdef WINDOWS
char *strsep(char **stringp, const char *delim) {
  char       *s;
  const char *spanp;
  int32_t     c, sc;
  char       *tok;
  if ((s = *stringp) == NULL) return (NULL);
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
char *strndup(const char *s, int size) {
  size_t l;
  char  *s2;
  l = strlen(s);
  if (l > size) l = size;
  s2 = malloc(l + 1);
  if (s2) {
    strncpy(s2, s, l);
    s2[l] = '\0';
  }
  return s2;
}
/* Copy no more than N characters of SRC to DEST, returning the address of
   the terminating '\0' in DEST, if any, or else DEST + N.  */
char *stpncpy(char *dest, const char *src, int n) {
  size_t size = strnlen(src, n);
  memcpy(dest, src, size);
  dest += size;
  if (size == n) return dest;
  return memset(dest, '\0', n - size);
}
#endif

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

TdUcs4 *tasoUcs4Copy(TdUcs4 *target_ucs4, TdUcs4 *source_ucs4, int32_t len_ucs4) {
  ASSERT(taosMemorySize(target_ucs4) >= len_ucs4 * sizeof(TdUcs4));
  return memcpy(target_ucs4, source_ucs4, len_ucs4 * sizeof(TdUcs4));
}

typedef struct {
  iconv_t conv;
  int8_t  inUse;
} SConv;

typedef enum { M2C = 0, C2M } ConvType;

// 0: Mbs --> Ucs4
// 1: Ucs4--> Mbs
SConv  *gConv[2] = {NULL, NULL};
int32_t convUsed[2] = {0, 0};
int32_t gConvMaxNum[2] = {0, 0};

int32_t taosConvInit(void) {
  int8_t M2C = 0;
  gConvMaxNum[M2C] = 512;
  gConvMaxNum[1 - M2C] = 512;

  gConv[M2C] = taosMemoryCalloc(gConvMaxNum[M2C], sizeof(SConv));
  gConv[1 - M2C] = taosMemoryCalloc(gConvMaxNum[1 - M2C], sizeof(SConv));

  for (int32_t i = 0; i < gConvMaxNum[M2C]; ++i) {
    gConv[M2C][i].conv = iconv_open(DEFAULT_UNICODE_ENCODEC, tsCharset);
    if ((iconv_t)-1 == gConv[M2C][i].conv || (iconv_t)0 == gConv[M2C][i].conv) {
      return -1;
    }
  }
  for (int32_t i = 0; i < gConvMaxNum[1 - M2C]; ++i) {
    gConv[1 - M2C][i].conv = iconv_open(tsCharset, DEFAULT_UNICODE_ENCODEC);
    if ((iconv_t)-1 == gConv[1 - M2C][i].conv || (iconv_t)0 == gConv[1 - M2C][i].conv) {
      return -1;
    }
  }

  return 0;
}

void taosConvDestroy() {
  int8_t M2C = 0;
  for (int32_t i = 0; i < gConvMaxNum[M2C]; ++i) {
    iconv_close(gConv[M2C][i].conv);
  }
  for (int32_t i = 0; i < gConvMaxNum[1 - M2C]; ++i) {
    iconv_close(gConv[1 - M2C][i].conv);
  }
  taosMemoryFreeClear(gConv[M2C]);
  taosMemoryFreeClear(gConv[1 - M2C]);
  gConvMaxNum[M2C] = -1;
  gConvMaxNum[1 - M2C] = -1;
}

iconv_t taosAcquireConv(int32_t *idx, ConvType type) {
  if (gConvMaxNum[type] <= 0) {
    *idx = -1;
    if (type == M2C) {
      return iconv_open(DEFAULT_UNICODE_ENCODEC, tsCharset);
    } else {
      return iconv_open(tsCharset, DEFAULT_UNICODE_ENCODEC);
    }
  }

  while (true) {
    int32_t used = atomic_add_fetch_32(&convUsed[type], 1);
    if (used > gConvMaxNum[type]) {
      used = atomic_sub_fetch_32(&convUsed[type], 1);
      sched_yield();
      continue;
    }

    break;
  }

  int32_t startId = taosGetSelfPthreadId() % gConvMaxNum[type];
  while (true) {
    if (gConv[type][startId].inUse) {
      startId = (startId + 1) % gConvMaxNum[type];
      continue;
    }

    int8_t old = atomic_val_compare_exchange_8(&gConv[type][startId].inUse, 0, 1);
    if (0 == old) {
      break;
    }
  }

  *idx = startId;
  return gConv[type][startId].conv;
}

void taosReleaseConv(int32_t idx, iconv_t conv, ConvType type) {
  if (idx < 0) {
    iconv_close(conv);
    return;
  }

  atomic_store_8(&gConv[type][idx].inUse, 0);
  atomic_sub_fetch_32(&convUsed[type], 1);
}

bool taosMbsToUcs4(const char *mbs, size_t mbsLength, TdUcs4 *ucs4, int32_t ucs4_max_len, int32_t *len) {
#ifdef DISALLOW_NCHAR_WITHOUT_ICONV
  printf("Nchar cannot be read and written without iconv, please install iconv library and recompile.\n");
  return -1;
#else
  memset(ucs4, 0, ucs4_max_len);

  int32_t idx = -1;
  iconv_t conv = taosAcquireConv(&idx, M2C);
  size_t  ucs4_input_len = mbsLength;
  size_t  outLeft = ucs4_max_len;
  if (iconv(conv, (char **)&mbs, &ucs4_input_len, (char **)&ucs4, &outLeft) == -1) {
    taosReleaseConv(idx, conv, M2C);
    return false;
  }

  taosReleaseConv(idx, conv, M2C);
  if (len != NULL) {
    *len = (int32_t)(ucs4_max_len - outLeft);
    if (*len < 0) {
      return false;
    }
  }

  return true;
#endif
}

int32_t taosUcs4ToMbs(TdUcs4 *ucs4, int32_t ucs4_max_len, char *mbs) {
#ifdef DISALLOW_NCHAR_WITHOUT_ICONV
  printf("Nchar cannot be read and written without iconv, please install iconv library and recompile.\n");
  return -1;
#else

  int32_t idx = -1;
  iconv_t conv = taosAcquireConv(&idx, C2M);
  size_t  ucs4_input_len = ucs4_max_len;
  size_t  outLen = ucs4_max_len;
  if (iconv(conv, (char **)&ucs4, &ucs4_input_len, &mbs, &outLen) == -1) {
    taosReleaseConv(idx, conv, C2M);
    return -1;
  }
  taosReleaseConv(idx, conv, C2M);
  return (int32_t)(ucs4_max_len - outLen);
#endif
}
bool taosValidateEncodec(const char *encodec) {
#ifdef DISALLOW_NCHAR_WITHOUT_ICONV
  printf("Nchar cannot be read and written without iconv, please install iconv library and recompile.\n");
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

// dst buffer size should be at least 2*len + 1
int32_t taosHexEncode(const unsigned char *src, char *dst, int32_t len) {
  if (!dst) {
    return -1;
  }

  for (int32_t i = 0; i < len; ++i) {
    sprintf(dst + i * 2, "%02x", src[i]);
  }

  return 0;
}

int32_t taosHexDecode(const char *src, char *dst, int32_t len) {
  if (!dst) {
    return -1;
  }

  uint8_t hn, ln, out;
  for (int i = 0, j = 0; i < len * 2; i += 2, ++j) {
    hn = src[i] > '9' ? src[i] - 'a' + 10 : src[i] - '0';
    ln = src[i + 1] > '9' ? src[i + 1] - 'a' + 10 : src[i + 1] - '0';

    out = (hn << 4) | ln;
    memcpy(dst + j, &out, 1);
  }

  return 0;
}

int32_t taosWcharWidth(TdWchar wchar) { return wcwidth(wchar); }

int32_t taosWcharsWidth(TdWchar *pWchar, int32_t size) { return wcswidth(pWchar, size); }

int32_t taosMbToWchar(TdWchar *pWchar, const char *pStr, int32_t size) { return mbtowc(pWchar, pStr, size); }

int32_t taosMbsToWchars(TdWchar *pWchars, const char *pStrs, int32_t size) { return mbstowcs(pWchars, pStrs, size); }

int32_t taosWcharToMb(char *pStr, TdWchar wchar) { return wctomb(pStr, wchar); }

char *taosStrCaseStr(const char *str, const char *pattern) {
  size_t i;

  if (!*pattern) return (char *)str;

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
  int64_t tmp = strtoll(str, pEnd, radix);
#if defined(DARWIN) || defined(_ALPINE)
  if (errno == EINVAL) errno = 0;
#endif
#ifdef TD_CHECK_STR_TO_INT_ERROR
  ASSERT(errno != ERANGE);
  ASSERT(errno != EINVAL);
#endif
  return tmp;
}

uint64_t taosStr2UInt64(const char *str, char **pEnd, int32_t radix) {
  uint64_t tmp = strtoull(str, pEnd, radix);
#if defined(DARWIN) || defined(_ALPINE)
  if (errno == EINVAL) errno = 0;
#endif
#ifdef TD_CHECK_STR_TO_INT_ERROR
  ASSERT(errno != ERANGE);
  ASSERT(errno != EINVAL);
#endif
  return tmp;
}

int32_t taosStr2Int32(const char *str, char **pEnd, int32_t radix) {
  int32_t tmp = strtol(str, pEnd, radix);
#if defined(DARWIN) || defined(_ALPINE)
  if (errno == EINVAL) errno = 0;
#endif
#ifdef TD_CHECK_STR_TO_INT_ERROR
  ASSERT(errno != ERANGE);
  ASSERT(errno != EINVAL);
#endif
  return tmp;
}

uint32_t taosStr2UInt32(const char *str, char **pEnd, int32_t radix) {
  uint32_t tmp = strtol(str, pEnd, radix);
#if defined(DARWIN) || defined(_ALPINE)
  if (errno == EINVAL) errno = 0;
#endif
#ifdef TD_CHECK_STR_TO_INT_ERROR
  ASSERT(errno != ERANGE);
  ASSERT(errno != EINVAL);
#endif
  return tmp;
}

int16_t taosStr2Int16(const char *str, char **pEnd, int32_t radix) {
  int32_t tmp = strtol(str, pEnd, radix);
#if defined(DARWIN) || defined(_ALPINE)
  if (errno == EINVAL) errno = 0;
#endif
#ifdef TD_CHECK_STR_TO_INT_ERROR
  ASSERT(errno != ERANGE);
  ASSERT(errno != EINVAL);
  ASSERT(tmp >= SHRT_MIN);
  ASSERT(tmp <= SHRT_MAX);
#endif
  return (int16_t)tmp;
}

uint16_t taosStr2UInt16(const char *str, char **pEnd, int32_t radix) {
  uint32_t tmp = strtoul(str, pEnd, radix);
#if defined(DARWIN) || defined(_ALPINE)
  if (errno == EINVAL) errno = 0;
#endif
#ifdef TD_CHECK_STR_TO_INT_ERROR
  ASSERT(errno != ERANGE);
  ASSERT(errno != EINVAL);
  ASSERT(tmp <= USHRT_MAX);
#endif
  return (uint16_t)tmp;
}

int8_t taosStr2Int8(const char *str, char **pEnd, int32_t radix) {
  int32_t tmp = strtol(str, pEnd, radix);
#ifdef TD_CHECK_STR_TO_INT_ERROR
  ASSERT(errno != ERANGE);
  ASSERT(errno != EINVAL);
  ASSERT(tmp >= SCHAR_MIN);
  ASSERT(tmp <= SCHAR_MAX);
#endif
  return tmp;
}

uint8_t taosStr2UInt8(const char *str, char **pEnd, int32_t radix) {
  uint32_t tmp = strtoul(str, pEnd, radix);
#if defined(DARWIN) || defined(_ALPINE)
  if (errno == EINVAL) errno = 0;
#endif
#ifdef TD_CHECK_STR_TO_INT_ERROR
  ASSERT(errno != ERANGE);
  ASSERT(errno != EINVAL);
  ASSERT(tmp <= UCHAR_MAX);
#endif
  return tmp;
}

double taosStr2Double(const char *str, char **pEnd) {
  double tmp = strtod(str, pEnd);
#ifdef TD_CHECK_STR_TO_INT_ERROR
  ASSERT(errno != ERANGE);
  ASSERT(errno != EINVAL);
  ASSERT(tmp != HUGE_VAL);
#endif
  return tmp;
}

float taosStr2Float(const char *str, char **pEnd) {
  float tmp = strtof(str, pEnd);
#ifdef TD_CHECK_STR_TO_INT_ERROR
  ASSERT(errno != ERANGE);
  ASSERT(errno != EINVAL);
  ASSERT(tmp != HUGE_VALF);
  ASSERT(tmp != NAN);
#endif
  return tmp;
}

#define HEX_PREFIX_LEN 2    // \x
bool isHex(const char* z, uint32_t n){
  if(n < HEX_PREFIX_LEN) return false;
  if(z[0] == '\\' && z[1] == 'x') return true;
  return false;
}

bool isValidateHex(const char* z, uint32_t n){
  if(n % 2 != 0) return false;
  for(size_t i = HEX_PREFIX_LEN; i < n; i++){
    if(isxdigit(z[i]) == 0){
      return false;
    }
  }
  return true;
}

int32_t taosHex2Ascii(const char *z, uint32_t n, void** data, uint32_t* size){
  n -= HEX_PREFIX_LEN;   // remove 0x
  z += HEX_PREFIX_LEN;
  *size = n / HEX_PREFIX_LEN;
  if(*size == 0) return 0;
  uint8_t* tmp = (uint8_t*)taosMemoryCalloc(*size, 1);
  if(tmp == NULL) return -1;
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

//int32_t taosBin2Ascii(const char *z, uint32_t n, void** data, uint32_t* size){
//
//  for (i = 2; isdigit(z[i]) || (z[i] >= 'a' && z[i] <= 'f') || (z[i] >= 'A' && z[i] <= 'F'); ++i) {
//  }
//
//  n -= 2;   // remove 0b
//  z += 2;
//  *size = n%8 == 0 ? n/8 : n/8 + 1;
//  uint8_t* tmp = (uint8_t*)taosMemoryCalloc(*size, 1);
//  if(tmp == NULL) return -1;
//  int8_t   num = 0;
//  uint8_t *byte = tmp + *size - 1;
//
//  for (int i = n - 1; i >= 0; i--) {
//    *byte |= ((uint8_t)(z[i] - '0') << num);
//    if (num == 7) {
//      byte--;
//      num = 0;
//    } else {
//      num++;
//    }
//  }
//  *data = tmp;
//  return 0;
//}

static char valueOf(uint8_t symbol)
{
  switch(symbol)
  {
    case 0: return '0';
    case 1: return '1';
    case 2: return '2';
    case 3: return '3';
    case 4: return '4';
    case 5: return '5';
    case 6: return '6';
    case 7: return '7';
    case 8: return '8';
    case 9: return '9';
    case 10: return 'A';
    case 11: return 'B';
    case 12: return 'C';
    case 13: return 'D';
    case 14: return 'E';
    case 15: return 'F';
    default:
    {
      return -1;
    }
  }
}

int32_t taosAscii2Hex(const char *z, uint32_t n, void** data, uint32_t* size){
  *size = n * 2 + HEX_PREFIX_LEN;
  uint8_t* tmp = (uint8_t*)taosMemoryCalloc(*size + 1, 1);
  if(tmp == NULL) return -1;
  *data = tmp;
  *(tmp++) = '\\';
  *(tmp++) = 'x';
  for(int i = 0; i < n; i ++){
    uint8_t val = z[i];
    tmp[i*2] = valueOf(val >> 4);
    tmp[i*2 + 1] = valueOf(val & 0x0F);
  }
  return 0;
}
