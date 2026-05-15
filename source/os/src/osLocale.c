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
#include "tutil.h"

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
#include <CoreFoundation/CoreFoundation.h>

#else
#include <argp.h>
#ifndef TD_ASTRA
#include <linux/sysctl.h>
#include <sys/file.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#endif
#include <sys/utsname.h>
#include <unistd.h>

#if defined(__GLIBC__)
#include <langinfo.h>
#endif

#endif

typedef struct CharsetPair {
  char *oldCharset;
  char *newCharset;
} CharsetPair;

char *taosCharsetReplace(char *charsetstr) {
  if (charsetstr == NULL) {
    return NULL;
  }
  CharsetPair charsetRep[] = {
      {"utf8", "UTF-8"},
      {"936", "CP936"},
  };

  for (int32_t i = 0; i < tListLen(charsetRep); ++i) {
    if (taosStrcasecmp(charsetRep[i].oldCharset, charsetstr) == 0) {
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
int32_t taosSetSystemLocale(const char *inLocale) {

  char *locale = setlocale(LC_CTYPE, inLocale);
  if (NULL == locale) {
    terrno = TSDB_CODE_INVALID_PARA;
    uError("failed to set locale:%s", inLocale);
    return terrno;
  }

  tstrncpy(tsLocale, locale, TD_LOCALE_LEN);
  return 0;
}

void taosGetSystemLocale(char *outLocale, char *outCharset) {
  if (outLocale == NULL || outCharset == NULL) return;
#ifdef WINDOWS
  char *locale = setlocale(LC_CTYPE, "en_US.UTF-8");
  if (locale != NULL) {
    tstrncpy(outLocale, locale, TD_LOCALE_LEN);
  }
  tstrncpy(outCharset, "UTF-8", TD_CHARSET_LEN);
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
    // printf("can't get locale from system, set it to en_US.UTF-8 since error:%d:%s", ERRNO, strerror(ERRNO));
    tstrncpy(outLocale, "en_US.UTF-8", TD_LOCALE_LEN);
  } else {
    tstrncpy(outLocale, locale, TD_LOCALE_LEN);
    //printf("locale not configured, set to system default:%s\n", outLocale);
  }

  // if user does not specify the charset, extract it from locale
  char *str = strrchr(outLocale, sep);
  if (str != NULL) {
    str++;

    char *revisedCharset = taosCharsetReplace(str);
    if (NULL == revisedCharset) {
      tstrncpy(outCharset, "UTF-8", TD_CHARSET_LEN);
    } else {
      tstrncpy(outCharset, revisedCharset, TD_CHARSET_LEN);

      taosMemoryFree(revisedCharset);
    }
    // printf("charset not configured, set to system default:%s", outCharset);
  } else {
    tstrncpy(outCharset, "UTF-8", TD_CHARSET_LEN);
    // printf("can't get locale and charset from system, set it to UTF-8");
  }

#endif
}

int32_t taosGetOSFirstDayOfWeek(void) {
#ifdef WINDOWS
  /* Windows LOCALE_IFIRSTDAYOFWEEK is 0-6 where 0=Monday, 6=Sunday.
     Normalize to 0-6 where 0=Sunday. */
  WCHAR buffer[4] = {0};

  if (GetLocaleInfoEx(LOCALE_NAME_USER_DEFAULT, LOCALE_IFIRSTDAYOFWEEK,
                      buffer, sizeof(buffer) / sizeof(WCHAR)) > 0) {
    int32_t value = (int32_t)(buffer[0] - L'0');
    if (value >= 0 && value <= 6) {
      return (value + 1) % 7;
    }
  }

  return -1;

#elif defined(_TD_DARWIN_64)
  /* macOS: Try to read AppleFirstWeekday from system preferences first.
     System stores it as a CFDictionary { "gregorian" = N } where N is 1-7
     (CFCalendar convention: 1=Sunday, 2=Monday, ..., 7=Saturday).
     Convert to 0-6 (0=Sunday). */
  CFPropertyListRef prefValue = CFPreferencesCopyAppValue(
      CFSTR("AppleFirstWeekday"),
      kCFPreferencesCurrentApplication
  );

  if (prefValue != NULL) {
    int raw = 0;
    bool gotValue = false;

    if (CFGetTypeID(prefValue) == CFDictionaryGetTypeID()) {
      /* System-stored format: { gregorian = "N" } — value is a CFString */
      CFTypeRef val = CFDictionaryGetValue((CFDictionaryRef)prefValue, CFSTR("gregorian"));
      if (val != NULL) {
        if (CFGetTypeID(val) == CFStringGetTypeID()) {
          raw = (int)CFStringGetIntValue((CFStringRef)val);
          gotValue = true;
        } else if (CFGetTypeID(val) == CFNumberGetTypeID()) {
          gotValue = CFNumberGetValue((CFNumberRef)val, kCFNumberIntType, &raw);
        }
      }
    } else if (CFGetTypeID(prefValue) == CFNumberGetTypeID()) {
      /* Plain integer format */
      gotValue = CFNumberGetValue((CFNumberRef)prefValue, kCFNumberIntType, &raw);
    } else if (CFGetTypeID(prefValue) == CFStringGetTypeID()) {
      /* Plain string format */
      raw = (int)CFStringGetIntValue((CFStringRef)prefValue);
      gotValue = true;
    }

    CFRelease(prefValue);

    if (gotValue && raw >= 1 && raw <= 7) {
      return raw - 1;  /* Convert 1-7 to 0-6 */
    }
  }
  
  /* Fallback: CFCalendarGetFirstWeekday returns 1-7 (1=Sunday, 7=Saturday).
     Convert to 0-6 (0=Sunday). Available since macOS 10.4. */
  CFCalendarRef cal = CFCalendarCopyCurrent();
  if (cal == NULL) {
    return -1;
  }

  CFIndex day = CFCalendarGetFirstWeekday(cal);
  CFRelease(cal);

  if (day < 1 || day > 7) {
    return -1;
  }

  return (int32_t)(day - 1);

#elif defined(__GLIBC__)
  /* Linux glibc: first_weekday is 1-7 (1=Sunday, 2=Monday, ...).
     Normalize to 0-6 where 0=Sunday. */
  const unsigned char *p = (const unsigned char *)nl_langinfo(_NL_TIME_FIRST_WEEKDAY);
  if (p == NULL) {
    return -1;
  }

  int32_t raw = (int32_t)(*p);
  if (raw < 1 || raw > 7) {
    return -1;
  }

  return raw - 1;

#else
  return -1;
#endif
}
