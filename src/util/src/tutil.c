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

size_t strtrim(char *z) {
  int32_t i = 0;
  int32_t j = 0;

  int32_t delta = 0;
  while (z[j] == ' ') {
    ++j;
  }

  if (z[j] == 0) {
    z[0] = 0;
    return 0;
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
  int esc = 0;
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

// TODO move to comm module
bool taosGetVersionNumber(char *versionStr, int *versionNubmer) {
  if (versionStr == NULL || versionNubmer == NULL) {
    return false;
  }

  int versionNumberPos[5] = {0};
  int len = (int)strlen(versionStr);
  int dot = 0;
  for (int pos = 0; pos < len && dot < 4; ++pos) {
    if (versionStr[pos] == '.') {
      versionStr[pos] = 0;
      versionNumberPos[++dot] = pos + 1;
    }
  }

  if (dot != 3) {
    return false;
  }

  for (int pos = 0; pos < 4; ++pos) {
    versionNubmer[pos] = atoi(versionStr + versionNumberPos[pos]);
  }
  versionStr[versionNumberPos[1] - 1] = '.';
  versionStr[versionNumberPos[2] - 1] = '.';
  versionStr[versionNumberPos[3] - 1] = '.';

  return true;
}

int taosCheckVersion(char *input_client_version, char *input_server_version, int comparedSegments) {
  char client_version[TSDB_VERSION_LEN] = {0};
  char server_version[TSDB_VERSION_LEN] = {0};
  int clientVersionNumber[4] = {0};
  int serverVersionNumber[4] = {0};

  tstrncpy(client_version, input_client_version, sizeof(client_version));
  tstrncpy(server_version, input_server_version, sizeof(server_version));

  if (!taosGetVersionNumber(client_version, clientVersionNumber)) {
    uError("invalid client version:%s", client_version);
    return TSDB_CODE_TSC_INVALID_VERSION;
  }

  if (!taosGetVersionNumber(server_version, serverVersionNumber)) {
    uError("invalid server version:%s", server_version);
    return TSDB_CODE_TSC_INVALID_VERSION;
  }

  for(int32_t i = 0; i < comparedSegments; ++i) {
    if (clientVersionNumber[i] != serverVersionNumber[i]) {
      uError("the %d-th number of server version:%s not matched with client version:%s", i, server_version,
             client_version);
      return TSDB_CODE_TSC_INVALID_VERSION;
    }
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

FORCE_INLINE float taos_align_get_float(const char* pBuf) {
  float fv = 0; 
  *(int32_t*)(&fv) = *(int32_t*)pBuf;
  return fv; 
}

FORCE_INLINE double taos_align_get_double(const char* pBuf) {
  double dv = 0; 
  *(int64_t*)(&dv) = *(int64_t*)pBuf;
  return dv; 
}
