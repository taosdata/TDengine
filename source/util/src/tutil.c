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
#include "tutil.h"
#include "tlog.h"

void *tmemmem(const char *haystack, int32_t hlen, const char *needle, int32_t nlen) {
  const char *limit;

  if (nlen == 0 || hlen < nlen) {
    return NULL;
  }

  limit = haystack + hlen - nlen + 1;
  while ((haystack = (char *)memchr(haystack, needle[0], limit - haystack)) != NULL) {
    if (memcmp(haystack, needle, nlen) == 0) {
      return (void *)haystack;
    }
    haystack++;
  }
  return NULL;
}

int32_t strdequote(char *z) {
  if (z == NULL) {
    return 0;
  }

  int32_t quote = z[0];
  if (quote != '\'' && quote != '"' && quote != '`') {
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

size_t strtrim(char *z) {
  int32_t i = 0;
  int32_t j = 0;

  int32_t delta = 0;
  while (isspace(z[j])) {
    ++j;
  }

  if (z[j] == 0) {
    z[0] = 0;
    return 0;
  }

  delta = j;

  int32_t stop = 0;
  while (z[j] != 0) {
    if (isspace(z[j]) && stop == 0) {
      stop = j;
    } else if (!isspace(z[j]) && stop != 0) {
      stop = 0;
    }

    z[i++] = z[j++];
  }

  if (stop > 0) {
    z[stop - delta] = 0;
    return (stop - delta);
  } else if (j != i) {
    z[i] = 0;
  }

  return i;
}

char **strsplit(char *z, const char *delim, int32_t *num) {
  *num = 0;
  int32_t size = 4;

  char **split = taosMemoryMalloc(POINTER_BYTES * size);

  for (char *p = strsep(&z, delim); p != NULL; p = strsep(&z, delim)) {
    size_t len = strlen(p);
    if (len == 0) {
      continue;
    }

    split[(*num)++] = p;
    if ((*num) >= size) {
      size = (size << 1);
      split = taosMemoryRealloc(split, POINTER_BYTES * size);
      ASSERTS(NULL != split, "realloc memory failed. size=%d", (int32_t) POINTER_BYTES * size);
    }
  }

  return split;
}

char *strnchr(const char *haystack, char needle, int32_t len, bool skipquote) {
  for (int32_t i = 0; i < len; ++i) {
    // skip the needle in quote, jump to the end of quoted string
    if (skipquote && (haystack[i] == '\'' || haystack[i] == '"')) {
      char quote = haystack[i++];
      while (i < len && haystack[i++] != quote)
        ;
      if (i >= len) {
        return NULL;
      }
    }

    if (haystack[i] == needle) {
      return (char *)&haystack[i];
    }
  }

  return NULL;
}

TdUcs4* wcsnchr(const TdUcs4* haystack, TdUcs4 needle, size_t len) {
  for(int32_t i = 0; i < len; ++i) {
    if (haystack[i] == needle) {
      return (TdUcs4*) &haystack[i];
    }
  }

  return NULL;
}

char *strtolower(char *dst, const char *src) {
  int32_t esc = 0;
  char    quote = 0, *p = dst, c;

  for (c = *src++; c; c = *src++) {
    if (esc) {
      esc = 0;
    } else if (quote) {
      if (c == '\\') {
        esc = 1;
      } else if (c == quote) {
        quote = 0;
      }
    } else if (c >= 'A' && c <= 'Z') {
      c -= 'A' - 'a';
    } else if (c == '\'' || c == '"') {
      quote = c;
    }
    *p++ = c;
  }

  *p = 0;
  return dst;
}

char *strntolower(char *dst, const char *src, int32_t n) {
  int32_t esc = 0;
  char    quote = 0, *p = dst, c;

  if (n == 0) {
    *p = 0;
    return dst;
  }
  for (c = *src++; n-- > 0; c = *src++) {
    if (esc) {
      esc = 0;
    } else if (quote) {
      if (c == '\\') {
        esc = 1;
      } else if (c == quote) {
        quote = 0;
      }
    } else if (c >= 'A' && c <= 'Z') {
      c -= 'A' - 'a';
    } else if (c == '\'' || c == '"' || c == '`') {
      quote = c;
    }
    *p++ = c;
  }

  *p = 0;
  return dst;
}

char *strntolower_s(char *dst, const char *src, int32_t n) {
  char *p = dst, c;
  if (n == 0) {
    return NULL;
  }

  while (n-- > 0) {
    c = *src;
    if (c >= 'A' && c <= 'Z') {
      c -= 'A' - 'a';
    }
    *p++ = c;
    src++;
  }

  return dst;
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
      ret += dig * base;
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

char *strbetween(char *string, char *begin, char *end) {
  char *result = NULL;
  char *_begin = strstr(string, begin);
  if (_begin != NULL) {
    char   *_end = strstr(_begin + strlen(begin), end);
    int32_t size = (int32_t)(_end - _begin);
    if (_end != NULL && size > 0) {
      result = (char *)taosMemoryCalloc(1, size);
      memcpy(result, _begin + strlen(begin), size - +strlen(begin));
    }
  }
  return result;
}

int32_t tintToHex(uint64_t val, char hex[]) {
  const char hexstr[16] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  int32_t j = 0, k = 0;
  if (val == 0)  {
    hex[j++] = hexstr[0];
    return j;
  }

  // ignore the initial 0
  while((val & (((uint64_t)0xfL) << ((15 - k) * 4))) == 0) {
    k += 1;
  }

  for (j = 0; k < 16; ++k, ++j) {
    hex[j] = hexstr[(val & (((uint64_t)0xfL) << ((15 - k) * 4))) >> (15 - k) * 4];
  }

  return j;
}

int32_t titoa(uint64_t val, size_t radix, char str[]) {
  if (radix < 2 || radix > 16) {
    return 0;
  }

  const char* s = "0123456789abcdef";
  char buf[65] = {0};

  int32_t i = 0;
  uint64_t v = val;
  do {
    buf[i++] = s[v % radix];
    v /= radix;
  } while (v > 0);

  // reverse order
  for(int32_t j = 0; j < i; ++j) {
    str[j] = buf[i - j - 1];
  }

  return i;
}

int32_t taosByteArrayToHexStr(char bytes[], int32_t len, char hexstr[]) {
  int32_t i;
  char    hexval[16] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  for (i = 0; i < len; i++) {
    hexstr[i * 2] = hexval[((bytes[i] >> 4u) & 0xF)];
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

char *taosIpStr(uint32_t ipInt) {
  static char    ipStrArray[3][30];
  static int32_t ipStrIndex = 0;

  char *ipStr = ipStrArray[(ipStrIndex++) % 3];
  // sprintf(ipStr, "0x%x:%u.%u.%u.%u", ipInt, ipInt & 0xFF, (ipInt >> 8) & 0xFF, (ipInt >> 16) & 0xFF, (uint8_t)(ipInt
  // >> 24));
  sprintf(ipStr, "%u.%u.%u.%u", ipInt & 0xFF, (ipInt >> 8) & 0xFF, (ipInt >> 16) & 0xFF, (uint8_t)(ipInt >> 24));
  return ipStr;
}

void taosIp2String(uint32_t ip, char *str) {
  sprintf(str, "%u.%u.%u.%u", ip & 0xFF, (ip >> 8) & 0xFF, (ip >> 16) & 0xFF, (uint8_t)(ip >> 24));
}

void taosIpPort2String(uint32_t ip, uint16_t port, char *str) {
  sprintf(str, "%u.%u.%u.%u:%u", ip & 0xFF, (ip >> 8) & 0xFF, (ip >> 16) & 0xFF, (uint8_t)(ip >> 24), port);
}

size_t tstrncspn(const char *str, size_t size, const char *reject, size_t rsize) {
  if (rsize == 0 || rsize == 1) {
    char* p = strnchr(str, reject[0], size, false);
    return (p == NULL)? size:(p-str);
  }

  /* Use multiple small memsets to enable inlining on most targets.  */
  unsigned char  table[256];
  unsigned char *p = memset(table, 0, 64);
  memset(p + 64, 0, 64);
  memset(p + 128, 0, 64);
  memset(p + 192, 0, 64);

  unsigned char *s = (unsigned char *)reject;
  int32_t index = 0;
  do {
    p[s[index++]] = 1;
  } while (index < rsize);

  s = (unsigned char*) str;
  int32_t times = size >> 2;
  if (times == 0) {
    for(int32_t i = 0; i < size; ++i) {
      if (p[s[i]]) {
        return i;
      }
    }

    return size;
  }

  index = 0;
  uint32_t c0, c1, c2, c3;
  for(int32_t i = 0; i < times; ++i, index += 4) {
    int32_t j = index;
    c0 = p[s[j]];
    c1 = p[s[j + 1]];
    c2 = p[s[j + 2]];
    c3 = p[s[j + 3]];

    if ((c0 | c1 | c2 | c3) != 0) {
      size_t count = i * 4;
      return (c0 | c1) != 0 ? count - c0 + 1 : count - c2 + 3;
    }
  }

  int32_t offset = times * 4;
  for(int32_t i = offset; i < size; ++i) {
    if (p[s[i]]) {
      return i;
    }
  }

  return size;
}

size_t twcsncspn(const TdUcs4 *wcs, size_t size, const TdUcs4 *reject, size_t rsize) {
  if (rsize == 0 || rsize == 1) {
    TdUcs4* p = wcsnchr(wcs, reject[0], size);
    return (p == NULL)? size:(p-wcs);
  }

  size_t index = 0;
  while ((index < size) && (wcsnchr(reject, wcs[index], rsize) == NULL)) {
    ++index;
  }

  return index;
}
