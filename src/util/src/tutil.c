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

#include "os.h"
#include "tcrc32c.h"
#include "tglobal.h"
#include "taosdef.h"
#include "tutil.h"
#include "tulog.h"
#include "taoserror.h"

bool isInteger(double x){
  int truncated = (int)x;
  return (x == truncated);
}

int32_t strDealWithEscape(char *z, int32_t len){
  if (z == NULL) {
    return 0;
  }

  int32_t j = 0;
  for (int32_t i = 0; i < len; i++) {
    if (z[i] == '\\') {   // deal with escape character
      if(z[i+1] == 'n'){
        z[j++] = '\n';
      }else if(z[i+1] == 'r'){
        z[j++] = '\r';
      }else if(z[i+1] == 't'){
        z[j++] = '\t';
      }else if(z[i+1] == '\\'){
        z[j++] = '\\';
      }else if(z[i+1] == '\''){
        z[j++] = '\'';
      }else if(z[i+1] == '"'){
        z[j++] = '"';
      }else if(z[i+1] == '%'){
        z[j++] = z[i];
        z[j++] = z[i+1];
      }else if(z[i+1] == '_'){
        z[j++] = z[i];
        z[j++] = z[i+1];
      }else{
        z[j++] = z[i+1];
      }

      i++;
      continue;
    }

    z[j++] = z[i];
  }
  z[j] = 0;
  return j;
}

/*
 * remove the quotation marks at both ends
 * "fsd" => fsd
 * "f""sd" =>f"sd
 *  'fsd' => fsd
 * 'f''sd' =>f'sd
 *  `fsd => fsd
 * `f``sd` =>f`sd
 */
static int32_t strdequote(char *z, int32_t n){
  if(z == NULL || n < 2) return n;
  int32_t quote = z[0];
  z[0] = 0;
  z[n - 1] = 0;
  int32_t i = 1, j = 0;
  while (i < n) {
    if (i < n - 1 && z[i] == quote && z[i + 1] == quote) { // two consecutive quotation marks keep one
      z[j++] = (char)quote;
      i += 2;
    } else {
      z[j++] = z[i++];
    }
  }
  z[j - 1] = 0;
  return j - 1;
}

int32_t stringProcess(char *z, int32_t len) {
  if (z == NULL || len < 2) return len;

  if ((z[0] == '\'' && z[len - 1] == '\'')|| (z[0] == '"' && z[len - 1] == '"')) {
    int32_t n = strdequote(z, len);
    return strDealWithEscape(z, n);
  } else if (z[0] == TS_BACKQUOTE_CHAR && z[len - 1] == TS_BACKQUOTE_CHAR) {
    return strdequote(z, len);
  }

  return len;
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
      assert(NULL != split);
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

char *tstrstr(char *src, char *dst, bool ignoreInEsc) {
  if (!ignoreInEsc) {
    return strstr(src, dst);
  }

  int32_t len = (int32_t)strlen(src);
  bool inEsc = false;
  char escChar = 0;
  char *str = src, *res = NULL;

  for (int32_t i = 0; i < len; ++i) {
    if (src[i] == TS_BACKQUOTE_CHAR || src[i] == '\'' || src[i] == '\"') {
      if (!inEsc) {
        escChar = src[i];
        src[i] = 0;
        res = strstr(str, dst);
        src[i] = escChar;
        if (res) {
          return res;
        }
        str = NULL;
      } else {
        if (src[i] != escChar) {
          continue;
        }

        str = src + i + 1;
      }

      inEsc = !inEsc;
      continue;
    }
  }

  return str ? strstr(str, dst) : NULL;
}

char* strtolower(char *dst, const char *src) {
  int esc = 0;
  char quote = 0, *p = dst, c;

  assert(dst != NULL);

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

char* strntolower(char *dst, const char *src, int32_t n) {
  int esc = 0, inEsc = 0;
  char quote = 0, *p = dst, c;

  assert(dst != NULL);
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
    } else if (inEsc) {
      if (c == '`') {
        inEsc = 0;
      }
    } else if (c >= 'A' && c <= 'Z') {
      c -= 'A' - 'a';
    } else if (inEsc == 0 && (c == '\'' || c == '"')) {
      quote = c;
    } else if (c == '`' && quote == 0) {
      inEsc = 1;
    }
    *p++ = c;
  }

  *p = 0;
  return dst;
}

char* strntolower_s(char *dst, const char *src, int32_t n) {
  char *p = dst, c;

  assert(dst != NULL);
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
    char *_end = strstr(_begin + strlen(begin), end);
    int   size = (int)(_end - _begin);
    if (_end != NULL && size > 0) {
      result = (char *)calloc(1, size);
      memcpy(result, _begin + strlen(begin), size - +strlen(begin));
    }
  }
  return result;
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
  static char ipStrArray[3][30];
  static int ipStrIndex = 0;

  char *ipStr = ipStrArray[(ipStrIndex++) % 3];
  //sprintf(ipStr, "0x%x:%u.%u.%u.%u", ipInt, ipInt & 0xFF, (ipInt >> 8) & 0xFF, (ipInt >> 16) & 0xFF, (uint8_t)(ipInt >> 24));
  sprintf(ipStr, "%u.%u.%u.%u", ipInt & 0xFF, (ipInt >> 8) & 0xFF, (ipInt >> 16) & 0xFF, (uint8_t)(ipInt >> 24));
  return ipStr;
}

void jsonKeyMd5(void *pMsg, int msgLen, void *pKey) {
  T_MD5_CTX context;

  tMD5Init(&context);
  tMD5Update(&context, (uint8_t *)pMsg, msgLen);
  tMD5Final(&context);

  memcpy(pKey, context.digest, sizeof(context.digest));
}

bool isValidateTag(char *input) {
  if (!input) return false;
  for (size_t i = 0; i < strlen(input); ++i) {
    if (isprint(input[i]) == 0) return false;
  }
  return true;
}

FORCE_INLINE float taos_align_get_float(const char* pBuf) {
#if __STDC_VERSION__ >= 201112L
  static_assert(sizeof(float) == sizeof(uint32_t), "sizeof(float) must equal to sizeof(uint32_t)");
#else
  assert(sizeof(float) == sizeof(uint32_t));
#endif
  float fv = 0;
  memcpy(&fv, pBuf, sizeof(fv)); // in ARM, return *((const float*)(pBuf)) may cause problem
  return fv; 
}

FORCE_INLINE double taos_align_get_double(const char* pBuf) {
#if __STDC_VERSION__ >= 201112L
  static_assert(sizeof(double) == sizeof(uint64_t), "sizeof(double) must equal to sizeof(uint64_t)");
#else
  assert(sizeof(double) == sizeof(uint64_t));
#endif
  double dv = 0;
  memcpy(&dv, pBuf, sizeof(dv)); // in ARM, return *((const double*)(pBuf)) may cause problem
  return dv; 
}

//
// TSKEY util
//

// if time area(s1,e1) intersect with time area(s2,e2) then return true else return false
bool timeIntersect(TSKEY s1, TSKEY e1, TSKEY s2, TSKEY e2) {
  // s1,e1 and s2,e2 have 7 scenarios, 5 is intersection, 2 is no intersection, so we pick up 2.
  if(e2 < s1 || s2 > e1)
    return false;
  else
    return true;
}