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

#ifdef USE_LIBICONV
#include "iconv.h"
#endif

#include "tcrc32c.h"
#include "tglobalcfg.h"
#include "ttime.h"
#include "taosdef.h"
#include "tutil.h"
#include "tlog.h"
#include "taoserror.h"

int32_t tmpFileSerialNum = 0;

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

void getTmpfilePath(const char *fileNamePrefix, char *dstPath) {
  const char* tdengineTmpFileNamePrefix = "tdengine-";
  
  char tmpPath[PATH_MAX] = {0};

#ifdef WINDOWS
  char *tmpDir = getenv("tmp");
  if (tmpDir == NULL) {
    tmpDir = "";
  }
#else
  char *tmpDir = "/tmp/";
#endif
  
  strcpy(tmpPath, tmpDir);
  strcat(tmpPath, tdengineTmpFileNamePrefix);
  strcat(tmpPath, fileNamePrefix);
  strcat(tmpPath, "-%llu-%u");
  snprintf(dstPath, PATH_MAX, tmpPath, taosGetPthreadId(), atomic_add_fetch_32(&tmpFileSerialNum, 1));
}

int tasoUcs4Compare(void* f1_ucs4, void *f2_ucs4, int bytes) {
#if defined WINDOWS
  for (int i = 0; i < bytes; ++i) {
    int32_t f1 = *(int32_t*)((char*)f1_ucs4 + i * 4);
    int32_t f2 = *(int32_t*)((char*)f2_ucs4 + i * 4);

    if ((f1 == 0 && f2 != 0) || (f1 != 0 && f2 == 0)) {
      return f1 - f2;
    }
    else if (f1 == 0 && f2 == 0) {
      return 0;
    }

    if (f1 != f2) {
      return f1 - f2;
    }
  }
  return 0;

#if 0
  int32_t ucs4_max_len = bytes + 4;
  char *f1_mbs = calloc(bytes, 1);
  char *f2_mbs = calloc(bytes, 1);
  if (!taosUcs4ToMbs(f1_ucs4, ucs4_max_len, f1_mbs)) {
    return -1;
  }
  if (!taosUcs4ToMbs(f2_ucs4, ucs4_max_len, f2_mbs)) {
    return -1;
  }
  int32_t ret = strcmp(f1_mbs, f2_mbs);
  free(f1_mbs);
  free(f2_mbs);
  return ret;
#endif

#else
  return wcsncmp((wchar_t *)f1_ucs4, (wchar_t *)f2_ucs4, bytes / TSDB_NCHAR_SIZE);
#endif
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

bool taosValidateEncodec(const char *encodec) {
#ifdef USE_LIBICONV
  iconv_t cd = iconv_open(encodec, DEFAULT_UNICODE_ENCODEC);
  if (cd == (iconv_t)(-1)) {
    return false;
  }
  iconv_close(cd);
#endif
  return true;
}

bool taosGetVersionNumber(char *versionStr, int *versionNubmer) {
  if (versionStr == NULL || versionNubmer == NULL) {
    return false;
  }

  int versionNumberPos[4] = {0};
  int len = strlen(versionStr);
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
  char client_version[64] = {0};
  char server_version[64] = {0};
  int clientVersionNumber[4] = {0};
  int serverVersionNumber[4] = {0};

  strcpy(client_version, input_client_version);
  strcpy(server_version, input_server_version);

  if (!taosGetVersionNumber(client_version, clientVersionNumber)) {
    pError("invalid client version:%s", client_version);
    return TSDB_CODE_INVALID_CLIENT_VERSION;
  }

  if (!taosGetVersionNumber(server_version, serverVersionNumber)) {
    pError("invalid server version:%s", server_version);
    return TSDB_CODE_INVALID_CLIENT_VERSION;
  }

  for(int32_t i = 0; i < comparedSegments; ++i) {
    if (clientVersionNumber[i] != serverVersionNumber[i]) {
      tscError("the %d-th number of server version:%s not matched with client version:%s", i, server_version, version);
      return TSDB_CODE_INVALID_CLIENT_VERSION;
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
