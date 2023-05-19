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
#include "osLocale.h"

#ifdef WINDOWS
#if (_WIN64)
#include <iphlpapi.h>
#include <mswsock.h>
#include <psapi.h>
#include <stdio.h>
#include <windows.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Mswsock.lib ")
#endif
#include <objbase.h>
#pragma warning(push)
#pragma warning(disable : 4091)
#include <DbgHelp.h>
#pragma warning(pop)
#elif defined(_TD_DARWIN_64)
#include <errno.h>
#include <libproc.h>
#else
#include <argp.h>
#include <linux/sysctl.h>
#include <sys/file.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <unistd.h>
#endif

typedef struct CharsetPair {
  char *oldCharset;
  char *newCharset;
} CharsetPair;

char *taosCharsetReplace(char *charsetstr) {
  CharsetPair charsetRep[] = {
      {"utf8", "UTF-8"},
      {"936", "CP936"},
  };

  for (int32_t i = 0; i < tListLen(charsetRep); ++i) {
    if (strcasecmp(charsetRep[i].oldCharset, charsetstr) == 0) {
      return taosStrdup(charsetRep[i].newCharset);
    }
  }

  return taosStrdup(charsetstr);
}

/**
 * TODO: here we may employ the systemctl API to set/get the correct locale on the Linux. In some cases, the setlocale
 *  seems does not response as expected.
 *
 * In some Linux systems, setLocale(LC_CTYPE, "") may return NULL, in which case the launch of
 * both the Server and the Client may be interrupted.
 *
 * In case that the setLocale failed to be executed, the right charset needs to be set.
 */
void taosSetSystemLocale(const char *inLocale, const char *inCharSet) {
  char *locale = setlocale(LC_CTYPE, inLocale);

  // default locale or user specified locale is not valid, abort launch
  if (inLocale == NULL || strlen(inLocale) == 0) {
    // printf("Invalid locale:%s, please set the valid locale in config file\n", inLocale);
  }

  if (!taosValidateEncodec(inCharSet)) {
    printf("Invalid charset:%s, please set the valid charset in config file\n", inCharSet);
    exit(-1);
  }
}

void taosGetSystemLocale(char *outLocale, char *outCharset) {
#ifdef WINDOWS
  char *locale = setlocale(LC_CTYPE, "en_US.UTF-8");
  if (locale != NULL) {
    tstrncpy(outLocale, locale, TD_LOCALE_LEN);
  }
  strcpy(outCharset, "UTF-8");

#elif defined(_TD_DARWIN_64)
  /*
   * originally from src/os/src/detail/osSysinfo.c
   * POSIX format locale string:
   * (Language Strings)_(Country/Region Strings).(code_page)
   *
   * example: en_US.UTF-8, zh_CN.GB18030, zh_CN.UTF-8,
   *
   * if user does not specify the locale in taos.cfg the program use default LC_CTYPE as system locale.
   *
   * In case of some CentOS systems, their default locale is "en_US.utf8", which is not valid code_page
   * for libiconv that is employed to convert string in this system. This program will automatically use
   * UTF-8 instead as the charset.
   *
   * In case of windows client, the locale string is not valid POSIX format, user needs to set the
   * correct code_page for libiconv. Usually, the code_page of windows system with simple chinese is
   * CP936, CP437 for English charset.
   *
   */

  char  sep = '.';
  char *locale = NULL;

  locale = setlocale(LC_CTYPE, "");
  if (locale == NULL) {
    // printf("can't get locale from system, set it to en_US.UTF-8 since error:%d:%s", errno, strerror(errno));
    strcpy(outLocale, "en_US.UTF-8");
  } else {
    tstrncpy(outLocale, locale, TD_LOCALE_LEN);
    // printf("locale not configured, set to system default:%s", outLocale);
  }

  /* if user does not specify the charset, extract it from locale */
  char *str = strrchr(outLocale, sep);
  if (str != NULL) {
    str++;

    char *revisedCharset = taosCharsetReplace(str);
    tstrncpy(outCharset, revisedCharset, TD_CHARSET_LEN);

    taosMemoryFree(revisedCharset);
    // printf("charset not configured, set to system default:%s", outCharset);
  } else {
    strcpy(outCharset, "UTF-8");
    // printf("can't get locale and charset from system, set it to UTF-8");
  }

#else
  /*
   * POSIX format locale string:
   * (Language Strings)_(Country/Region Strings).(code_page)
   *
   * example: en_US.UTF-8, zh_CN.GB18030, zh_CN.UTF-8,
   *
   * If user does not specify the locale in taos.cfg, the program then uses default LC_CTYPE as system locale.
   *
   * In case of some CentOS systems, their default locale is "en_US.utf8", which is not valid code_page
   * for libiconv that is employed to convert string in this system. This program will automatically use
   * UTF-8 instead as the charset.
   *
   * In case of windows client, the locale string is not valid POSIX format, user needs to set the
   * correct code_page for libiconv. Usually, the code_page of windows system with simple chinese is
   * CP936, CP437 for English charset.
   *
   */
  char  sep = '.';
  char *locale = NULL;

  locale = setlocale(LC_CTYPE, "");
  if (locale == NULL) {
    // printf("can't get locale from system, set it to en_US.UTF-8 since error:%d:%s", errno, strerror(errno));
    strcpy(outLocale, "en_US.UTF-8");
  } else {
    tstrncpy(outLocale, locale, TD_LOCALE_LEN);
    //printf("locale not configured, set to system default:%s\n", outLocale);
  }

  // if user does not specify the charset, extract it from locale
  char *str = strrchr(outLocale, sep);
  if (str != NULL) {
    str++;

    char *revisedCharset = taosCharsetReplace(str);
    tstrncpy(outCharset, revisedCharset, TD_LOCALE_LEN);

    taosMemoryFree(revisedCharset);
    // printf("charset not configured, set to system default:%s", outCharset);
  } else {
    strcpy(outCharset, "UTF-8");
    // printf("can't get locale and charset from system, set it to UTF-8");
  }

#endif
}
