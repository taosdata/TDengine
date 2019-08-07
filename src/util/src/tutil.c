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

#include <assert.h>
#include <locale.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

#include "os.h"

#ifdef USE_LIBICONV
#include "iconv.h"
#endif

#include "tcrc32c.h"
#include "tglobalcfg.h"
#include "ttime.h"
#include "ttypes.h"
#include "tutil.h"

int32_t strdequote(char *z) {
  if (z == NULL) {
    return 0;
  }

  int32_t quote = z[0];
  if (quote != '\'' && quote != '"') {
    return (int32_t)strlen(z);
  }

  int32_t i = 1, j = 0;

  while (z[i] != 0) {
    if (z[i] == quote) {
      if (z[i + 1] == quote) {
        z[j++] = (char)quote;
        i++;
      } else {
        z[j++] = 0;
        return (j - 1);
      }
    } else {
      z[j++] = z[i];
    }

    i++;
  }

  return j + 1;  // only one quote, do nothing
}

void strtrim(char *z) {
  int32_t i = 0;
  int32_t j = 0;

  int32_t delta = 0;
  while (z[j] == ' ') {
    ++j;
  }

  if (z[j] == 0) {
    z[0] = 0;
    return;
  }

  delta = j;

  int32_t stop = 0;
  while (z[j] != 0) {
    if (z[j] == ' ' && stop == 0) {
      stop = j;
    } else if (z[j] != ' ' && stop != 0) {
      stop = 0;
    }

    z[i++] = z[j++];
  }

  if (stop > 0) {
    z[stop - delta] = 0;
  } else if (j != i) {
    z[i] = 0;
  }
}

char **strsplit(char *z, const char *delim, int32_t *num) {
  *num = 0;
  int32_t size = 4;

  char **split = malloc(POINTER_BYTES * size);

  for (char *p = strsep(&z, delim); p != NULL; p = strsep(&z, delim)) {
    size_t len = strlen(p);
    if (len == 0) {
      continue;
    }

    split[(*num)++] = p;
    if ((*num) >= size) {
      size = (size << 1);
      split = realloc(split, POINTER_BYTES * size);
    }
  }

  return split;
}

char *strnchr(char *haystack, char needle, int32_t len, bool skipquote) {
  for (int32_t i = 0; i < len; ++i) {

    // skip the needle in quote, jump to the end of quoted string
    if (skipquote && (haystack[i] == '\'' || haystack[i] == '"')) {
      char quote = haystack[i++];
      while(i < len && haystack[i++] != quote);
      if (i >= len) {
        return NULL;
      }
    }

    if (haystack[i] == needle) {
      return &haystack[i];
    }
  }

  return NULL;
}

void strtolower(char *dst, const char *z) {
  int   quote = 0;
  char *str = z;
  if (dst == NULL) {
    return;
  }

  while (*str) {
    if (*str == '\'' || *str == '"') {
      quote = quote ^ 1;
    }

    if ((!quote) && (*str >= 'A' && *str <= 'Z')) {
      *dst++ = *str | 0x20;
    } else {
      *dst++ = *str;
    }

    str++;
  }

  *dst = 0;
}

char *paGetToken(char *string, char **token, int32_t *tokenLen) {
  char quote = 0;

  while (*string != 0) {
    if (*string == ' ' || *string == '\t') {
      ++string;
    } else {
      break;
    }
  }

  if (*string == '@') {
    quote = 1;
    string++;
  }

  *token = string;

  while (*string != 0) {
    if (*string == '@' && quote) {
      //*string = 0;
      ++string;
      break;
    }

    if (*string == '#' || *string == '\n' || *string == '\r') {
      *string = 0;
      break;
    }

    if ((*string == ' ' || *string == '\t') && !quote) {
      break;
    } else {
      ++string;
    }
  }

  *tokenLen = (int32_t)(string - *token);
  if (quote) {
    *tokenLen = *tokenLen - 1;
  }

  return string;
}

int64_t strnatoi(char *num, int32_t len) {
  int64_t ret = 0, i, dig, base = 1;

  if (len > (int32_t)strlen(num)) {
    len = (int32_t)strlen(num);
  }

  if ((len > 2) && (num[0] == '0') && ((num[1] == 'x') || (num[1] == 'X'))) {
    for (i = len - 1; i >= 2; --i, base *= 16) {
      if (num[i] >= '0' && num[i] <= '9') {
        dig = (num[i] - '0');
      } else if (num[i] >= 'a' && num[i] <= 'f') {
        dig = num[i] - 'a' + 10;
      } else if (num[i] >= 'A' && num[i] <= 'F') {
        dig = num[i] - 'A' + 10;
      } else {
        return 0;
      }
      ret = dig * base;
    }
  } else {
    for (i = len - 1; i >= 0; --i, base *= 10) {
      if (num[i] >= '0' && num[i] <= '9') {
        dig = (num[i] - '0');
      } else {
        return 0;
      }
      ret += dig * base;
    }
  }

  return ret;
}

FORCE_INLINE size_t getLen(size_t old, size_t size) {
  if (old == 1) {
    old = 2;
  }

  while (old < size) {
    old = (old * 1.5);
  }

  return old;
}

static char *ensureSpace(char *dest, size_t *curSize, size_t size) {
  if (*curSize < size) {
    *curSize = getLen(*curSize, size);

    char *tmp = realloc(dest, *curSize);
    if (tmp == NULL) {
      free(dest);
      return NULL;
    }

    return tmp;
  }

  return dest;
}

char *strreplace(const char *str, const char *pattern, const char *rep) {
  if (str == NULL || pattern == NULL || rep == NULL) {
    return NULL;
  }

  const char *s = str;

  size_t oldLen = strlen(str);
  size_t newLen = oldLen;

  size_t repLen = strlen(rep);
  size_t patternLen = strlen(pattern);

  char *dest = calloc(1, oldLen + 1);
  if (dest == NULL) {
    return NULL;
  }

  if (patternLen == 0) {
    return strcpy(dest, str);
  }

  int32_t start = 0;

  while (1) {
    char *p = strstr(str, pattern);
    if (p == NULL) {  // remain does not contain pattern
      size_t remain = (oldLen - (str - s));
      size_t size = remain + start + 1;

      dest = ensureSpace(dest, &newLen, size);
      if (dest == NULL) {
        return NULL;
      }

      strcpy(dest + start, str);
      dest[start + remain] = 0;
      break;
    }

    size_t len = p - str;
    size_t size = start + len + repLen + 1;

    dest = ensureSpace(dest, &newLen, size);
    if (dest == NULL) {
      return NULL;
    }

    memcpy(dest + start, str, len);

    str += (len + patternLen);
    start += len;

    memcpy(dest + start, rep, repLen);
    start += repLen;
  }

  return dest;
}

int32_t taosByteArrayToHexStr(char bytes[], int32_t len, char hexstr[]) {
  int32_t i;
  char    hexval[16] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  for (i = 0; i < len; i++) {
    hexstr[i * 2] = hexval[((bytes[i] >> 4) & 0xF)];
    hexstr[(i * 2) + 1] = hexval[(bytes[i]) & 0x0F];
  }

  return 0;
}

int32_t taosHexStrToByteArray(char hexstr[], char bytes[]) {
  int32_t len, i;
  char    ch;
  // char *by;

  len = (int32_t)strlen((char *)hexstr) / 2;

  for (i = 0; i < len; i++) {
    ch = hexstr[i * 2];
    if (ch >= '0' && ch <= '9')
      bytes[i] = (char)(ch - '0');
    else if (ch >= 'A' && ch <= 'F')
      bytes[i] = (char)(ch - 'A' + 10);
    else if (ch >= 'a' && ch <= 'f')
      bytes[i] = (char)(ch - 'a' + 10);
    else
      return -1;

    ch = hexstr[i * 2 + 1];
    if (ch >= '0' && ch <= '9')
      bytes[i] = (char)((bytes[i] << 4) + (ch - '0'));
    else if (ch >= 'A' && ch <= 'F')
      bytes[i] = (char)((bytes[i] << 4) + (ch - 'A' + 10));
    else if (ch >= 'a' && ch <= 'f')
      bytes[i] = (char)((bytes[i] << 4) + (ch - 'a' + 10));
    else
      return -1;
  }

  return 0;
}

// rename file name
int32_t taosFileRename(char *fullPath, char *suffix, char delimiter, char **dstPath) {
  int32_t ts = taosGetTimestampSec();

  char fname[PATH_MAX] = {0};  // max file name length must be less than 255

  char *delimiterPos = strrchr(fullPath, delimiter);
  if (delimiterPos == NULL) return -1;

  int32_t fileNameLen = 0;
  if (suffix)
    fileNameLen = snprintf(fname, PATH_MAX, "%s.%d.%s", delimiterPos + 1, ts, suffix);
  else
    fileNameLen = snprintf(fname, PATH_MAX, "%s.%d", delimiterPos + 1, ts);

  size_t len = (size_t)((delimiterPos - fullPath) + fileNameLen + 1);
  if (*dstPath == NULL) {
    *dstPath = calloc(1, len + 1);
    if (*dstPath == NULL) return -1;
  }

  strncpy(*dstPath, fullPath, (size_t)(delimiterPos - fullPath + 1));
  strncat(*dstPath, fname, (size_t)fileNameLen);
  (*dstPath)[len] = 0;

  return rename(fullPath, *dstPath);
}

bool taosCheckDbName(char *db, char *monitordb) {
  char *pos = strchr(db, '.');
  if (pos == NULL) return false;

  return strncasecmp(pos + 1, monitordb, strlen(monitordb)) == 0;
}

bool taosUcs4ToMbs(void *ucs4, int32_t ucs4_max_len, char *mbs) {
#ifdef USE_LIBICONV
  iconv_t cd = iconv_open(tsCharset, DEFAULT_UNICODE_ENCODEC);
  size_t ucs4_input_len = ucs4_max_len;
  size_t outLen = ucs4_max_len;
  if (iconv(cd, (char **)&ucs4, &ucs4_input_len, &mbs, &outLen) == -1) {
    iconv_close(cd);
    return false;
  }
  iconv_close(cd);
  return true;
#else
  mbstate_t state = {0};
  int32_t len = (int32_t) wcsnrtombs(NULL, (const wchar_t **) &ucs4, ucs4_max_len / 4, 0, &state);
  if (len < 0) {
    return false;
  }
  memset(&state, 0, sizeof(state));
  len = wcsnrtombs(mbs, (const wchar_t **) &ucs4, ucs4_max_len / 4, (size_t) len, &state);
  if (len < 0) {
    return false;
  }
  return true;
#endif
}

bool taosMbsToUcs4(char *mbs, int32_t mbs_len, char *ucs4, int32_t ucs4_max_len) {
  memset(ucs4, 0, ucs4_max_len);
#ifdef USE_LIBICONV
  iconv_t cd = iconv_open(DEFAULT_UNICODE_ENCODEC, tsCharset);
  size_t ucs4_input_len = mbs_len;
  size_t outLen = ucs4_max_len;
  if (iconv(cd, &mbs, &ucs4_input_len, &ucs4, &outLen) == -1) {
    iconv_close(cd);
    return false;
  }
  iconv_close(cd);
  return true;
#else
  mbstate_t state = {0};
  int32_t len = mbsnrtowcs((wchar_t *) ucs4, (const char **) &mbs, mbs_len, ucs4_max_len / 4, &state);
  return len >= 0;
#endif
}

bool taosValidateEncodec(char *encodec) {
#ifdef USE_LIBICONV
  iconv_t cd = iconv_open(encodec, DEFAULT_UNICODE_ENCODEC);
  if (cd == (iconv_t)(-1)) {
    return false;
  }
  iconv_close(cd);
  return true;
#else
  return true;
#endif
}