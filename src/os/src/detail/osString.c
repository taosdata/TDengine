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

#define _DEFAULT_SOURCE
#include "os.h"
#include "tglobal.h"

#ifndef TAOS_OS_FUNC_STRING_STR2INT64
int64_t tsosStr2int64(char *str) {
  char *endptr = NULL;
  return strtoll(str, &endptr, 10);
}
#endif

#ifndef TAOS_OS_FUNC_STRING_WCHAR
int tasoUcs4Compare(void *f1_ucs4, void *f2_ucs4, int bytes) {
  return wcsncmp((wchar_t *)f1_ucs4, (wchar_t *)f2_ucs4, bytes / TSDB_NCHAR_SIZE);
}
#endif

#ifdef USE_LIBICONV
#include "iconv.h"

int32_t taosUcs4ToMbs(void *ucs4, int32_t ucs4_max_len, char *mbs) {
  iconv_t cd = iconv_open(tsCharset, DEFAULT_UNICODE_ENCODEC);
  size_t  ucs4_input_len = ucs4_max_len;
  size_t  outLen = ucs4_max_len;
  if (iconv(cd, (char **)&ucs4, &ucs4_input_len, &mbs, &outLen) == -1) {
    iconv_close(cd);
    return -1;
  }

  iconv_close(cd);
  return (int32_t)(ucs4_max_len - outLen);
}

bool taosMbsToUcs4(char *mbs, size_t mbsLength, char *ucs4, int32_t ucs4_max_len, int32_t *len) {
  memset(ucs4, 0, ucs4_max_len);
  iconv_t cd = iconv_open(DEFAULT_UNICODE_ENCODEC, tsCharset);
  size_t  ucs4_input_len = mbsLength;
  size_t  outLeft = ucs4_max_len;
  if (iconv(cd, &mbs, &ucs4_input_len, &ucs4, &outLeft) == -1) {
    iconv_close(cd);
    return false;
  }

  iconv_close(cd);
  if (len != NULL) {
    *len = (int32_t)(ucs4_max_len - outLeft);
  }

  return true;
}

bool taosValidateEncodec(const char *encodec) {
  iconv_t cd = iconv_open(encodec, DEFAULT_UNICODE_ENCODEC);
  if (cd == (iconv_t)(-1)) {
    return false;
  }

  iconv_close(cd);
  return true;
}

#else

int32_t taosUcs4ToMbs(void *ucs4, int32_t ucs4_max_len, char *mbs) {
  mbstate_t state = {0};
  int32_t   len = (int32_t)wcsnrtombs(NULL, (const wchar_t **)&ucs4, ucs4_max_len / 4, 0, &state);
  if (len < 0) {
    return -1;
  }

  memset(&state, 0, sizeof(state));
  len = wcsnrtombs(mbs, (const wchar_t **)&ucs4, ucs4_max_len / 4, (size_t)len, &state);
  if (len < 0) {
    return -1;
  }

  return len;
}

bool taosMbsToUcs4(char *mbs, size_t mbsLength, char *ucs4, int32_t ucs4_max_len, int32_t *len) {
  memset(ucs4, 0, ucs4_max_len);
  mbstate_t state = {0};
  int32_t retlen = mbsnrtowcs((wchar_t *)ucs4, (const char **)&mbs, mbsLength, ucs4_max_len / 4, &state);
  *len = retlen;

  return retlen >= 0;
}

bool taosValidateEncodec(const char *encodec) {
  return true;
}

#endif

typedef struct CharsetPair {
  char *oldCharset;
  char *newCharset;
} CharsetPair;

char *taosCharsetReplace(char *charsetstr) {
  CharsetPair charsetRep[] = {
      { "utf8", "UTF-8" }, { "936", "CP936" },
  };

  for (int32_t i = 0; i < tListLen(charsetRep); ++i) {
    if (strcasecmp(charsetRep[i].oldCharset, charsetstr) == 0) {
      return strdup(charsetRep[i].newCharset);
    }
  }

  return strdup(charsetstr);
}
