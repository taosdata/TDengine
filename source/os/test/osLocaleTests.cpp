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

#include <gtest/gtest.h>
#include <locale.h>
#include <stdio.h>
#include <string.h>

#define ALLOW_FORBID_FUNC
#include "osLocale.h"

TEST(osLocaleTests, firstDayOfWeek_validRange) {
  int32_t v = taosGetOSFirstDayOfWeek();
  ASSERT_TRUE((v >= 0 && v <= 6) || v == -1);
}

TEST(osLocaleTests, firstDayOfWeek_stableAcrossCalls) {
  int32_t v1 = taosGetOSFirstDayOfWeek();
  int32_t v2 = taosGetOSFirstDayOfWeek();

  ASSERT_TRUE((v1 >= 0 && v1 <= 6) || v1 == -1);
  ASSERT_TRUE((v2 >= 0 && v2 <= 6) || v2 == -1);

  if (v1 != -1 && v2 != -1) {
    ASSERT_EQ(v1, v2);
  }
}

#if defined(__linux__)
static void testLocaleFirstDay(const char *loc, int32_t expected) {
  char *origin = setlocale(LC_TIME, NULL);
  char  saved[128] = {0};
  if (origin != NULL) {
    snprintf(saved, sizeof(saved), "%s", origin);
  }

  /* Try exact locale first, then common variants (e.g., en_US.UTF-8 vs en_US.utf8). */
  const char *variants[] = {loc, NULL, NULL};
  
  if (strcmp(loc, "en_US.UTF-8") == 0) {
    variants[1] = "en_US.utf8";
  } else if (strcmp(loc, "zh_CN.UTF-8") == 0) {
    variants[1] = "zh_CN.utf8";
  } else if (strcmp(loc, "de_DE.UTF-8") == 0) {
    variants[1] = "de_DE.utf8";
  } else if (strcmp(loc, "fr_FR.UTF-8") == 0) {
    variants[1] = "fr_FR.utf8";
  }

  int locale_found = 0;
  for (int i = 0; variants[i] != NULL; i++) {
    char *result = setlocale(LC_TIME, variants[i]);
    if (result != NULL) {
      locale_found = 1;
      break;
    }
  }

  if (!locale_found) {
    /* Restore original locale before skipping. */
    if (saved[0] != '\0') {
      setlocale(LC_TIME, saved);
    }
    GTEST_SKIP() << "locale not installed: " << loc;
  }

  int32_t v = taosGetOSFirstDayOfWeek();

  /* Always restore original locale. */
  if (saved[0] != '\0') {
    setlocale(LC_TIME, saved);
  }

  if (v == -1) {
    GTEST_SKIP() << "OS first day API unavailable on this system";
  }

  ASSERT_EQ(v, expected);
}

TEST(osLocaleTests, firstDayOfWeek_linux_en_US) {
  testLocaleFirstDay("en_US.UTF-8", 0);  /* Sunday */
}
TEST(osLocaleTests, firstDayOfWeek_linux_zh_CN) {
  testLocaleFirstDay("zh_CN.UTF-8", 1);  /* Monday */
}
TEST(osLocaleTests, firstDayOfWeek_linux_de_DE) {
  testLocaleFirstDay("de_DE.UTF-8", 1);  /* Monday */
}
TEST(osLocaleTests, firstDayOfWeek_linux_fr_FR) {
  testLocaleFirstDay("fr_FR.UTF-8", 1);  /* Monday */
}

TEST(osLocaleTests, firstDayOfWeek_linux_C_locale) {
  char *origin = setlocale(LC_TIME, NULL);
  char  saved[128] = {0};
  if (origin != NULL) {
    snprintf(saved, sizeof(saved), "%s", origin);
  }

  ASSERT_NE(setlocale(LC_TIME, "C"), nullptr);

  int32_t v = taosGetOSFirstDayOfWeek();

  if (saved[0] != '\0') {
    setlocale(LC_TIME, saved);
  }

  if (v == -1) {
    GTEST_SKIP() << "OS first day API unavailable on this system";
  }

  /* C locale may vary by system; just verify it returns a valid value. */
  ASSERT_TRUE(v >= 0 && v <= 6);
}
#endif

#if defined(_WIN32) || defined(_WIN64)
TEST(osLocaleTests, firstDayOfWeek_windows_user_locale) {
  int32_t v = taosGetOSFirstDayOfWeek();

  if (v == -1) {
    GTEST_SKIP() << "OS first day API unavailable on Windows";
  }

  /* Windows API returns 0-6 range (0=Monday).
     Expect valid weekday index. */
  ASSERT_TRUE(v >= 0 && v <= 6);
}

TEST(osLocaleTests, firstDayOfWeek_windows_consistency) {
  int32_t v1 = taosGetOSFirstDayOfWeek();
  int32_t v2 = taosGetOSFirstDayOfWeek();

  if (v1 == -1 || v2 == -1) {
    GTEST_SKIP() << "OS first day API unavailable on Windows";
  }

  /* Windows user locale should be stable across calls. */
  ASSERT_EQ(v1, v2);
}
#endif

#if defined(__APPLE__)
#include <CoreFoundation/CoreFoundation.h>

/* Helper: read AppleFirstWeekday from current-application preferences domain.
   Returns 0-6 (0=Sunday) matching the implementation output, or -1 if not set.
   Handles both system dict format { gregorian = N } and plain integer format. */
static int32_t readAppleFirstWeekday(void) {
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
      gotValue = CFNumberGetValue((CFNumberRef)prefValue, kCFNumberIntType, &raw);
    } else if (CFGetTypeID(prefValue) == CFStringGetTypeID()) {
      raw = (int)CFStringGetIntValue((CFStringRef)prefValue);
      gotValue = true;
    }

    CFRelease(prefValue);

    if (gotValue && raw >= 1 && raw <= 7) {
      return raw - 1;  /* Convert 1-7 to 0-6 */
    }
  }

  return -1;
}

/* Helper: write AppleFirstWeekday into the current-application preferences domain.
   day: 1-7 (CFCalendar convention: 1=Sunday, 2=Monday, ..., 7=Saturday), or -1 to remove.
   Writes in system dict format { gregorian = day }. */
static void setAppleFirstWeekday(int32_t day) {
  if (day == -1) {
    CFPreferencesSetAppValue(
        CFSTR("AppleFirstWeekday"),
        NULL,
        kCFPreferencesCurrentApplication
    );
  } else {
    int value = (int)day;
    CFNumberRef num = CFNumberCreate(kCFAllocatorDefault, kCFNumberIntType, &value);
    CFMutableDictionaryRef dict = CFDictionaryCreateMutable(
        kCFAllocatorDefault, 1,
        &kCFTypeDictionaryKeyCallBacks, &kCFTypeDictionaryValueCallBacks);
    CFDictionarySetValue(dict, CFSTR("gregorian"), num);
    CFPreferencesSetAppValue(
        CFSTR("AppleFirstWeekday"),
        dict,
        kCFPreferencesCurrentApplication
    );
    CFRelease(num);
    CFRelease(dict);
  }
  CFPreferencesAppSynchronize(kCFPreferencesCurrentApplication);
}

TEST(osLocaleTests, firstDayOfWeek_macos_user_locale) {
  int32_t v = taosGetOSFirstDayOfWeek();

  if (v == -1) {
    GTEST_SKIP() << "OS first day API unavailable on macOS";
  }

  /* macOS firstWeekday converted to 0-6 (0=Sunday). */
  ASSERT_TRUE(v >= 0 && v <= 6);
}

TEST(osLocaleTests, firstDayOfWeek_macos_consistency) {
  int32_t v1 = taosGetOSFirstDayOfWeek();
  int32_t v2 = taosGetOSFirstDayOfWeek();

  if (v1 == -1 || v2 == -1) {
    GTEST_SKIP() << "OS first day API unavailable on macOS";
  }

  /* macOS firstWeekday should be stable across calls. */
  ASSERT_EQ(v1, v2);
}

TEST(osLocaleTests, firstDayOfWeek_macos_apple_pref_matching) {
  /* Test that implementation correctly reads AppleFirstWeekday preference */
  int32_t sysValue = taosGetOSFirstDayOfWeek();

  if (sysValue == -1) {
    GTEST_SKIP() << "OS first day API unavailable on macOS";
  }

  int32_t prefValue = readAppleFirstWeekday();
  
  if (prefValue != -1) {
    /* If AppleFirstWeekday is available, result should match it */
    EXPECT_EQ(sysValue, prefValue) 
        << "Implementation should prioritize reading AppleFirstWeekday preference. "
        << "Expected: " << prefValue << ", Got: " << sysValue;
  } else {
    /* If AppleFirstWeekday is not set, just verify valid range (fallback behavior) */
    ASSERT_TRUE(sysValue >= 0 && sysValue <= 6);
  }
}

TEST(osLocaleTests, firstDayOfWeek_macos_pref_read_validity) {
  /* Test that AppleFirstWeekday preference, if present, is in valid range */
  int32_t prefValue = readAppleFirstWeekday();
  
  if (prefValue != -1) {
    EXPECT_TRUE(prefValue >= 0 && prefValue <= 6)
        << "AppleFirstWeekday preference should be in range 0-6";
  }
  /* If not set, test is still valid (preference doesn't exist) */
}

TEST(osLocaleTests, firstDayOfWeek_macos_set_and_read) {
  /* Save raw system-format value to restore later.
     AppleFirstWeekday uses 1-7 (CFCalendar: 1=Sunday, 2=Monday, ..., 7=Saturday).
     readAppleFirstWeekday() returns 0-6; we need the raw 1-7 value to restore. */
  CFPropertyListRef origPref = CFPreferencesCopyAppValue(
      CFSTR("AppleFirstWeekday"), kCFPreferencesCurrentApplication);

  /* Set { gregorian = 2 } which means Monday (2 in 1-7 → 1 in 0-6). */
  const int32_t setDay = 2;       /* 2 = Monday in CFCalendar 1-7 */
  const int32_t expectedOut = 1;  /* 0-6 output: 1 = Monday */
  setAppleFirstWeekday(setDay);

  int32_t v = taosGetOSFirstDayOfWeek();

  /* Restore original preference. */
  CFPreferencesSetAppValue(
      CFSTR("AppleFirstWeekday"), origPref, kCFPreferencesCurrentApplication);
  if (origPref != NULL) CFRelease(origPref);
  CFPreferencesAppSynchronize(kCFPreferencesCurrentApplication);

  if (v == -1) {
    GTEST_SKIP() << "OS first day API unavailable on macOS";
  }

  EXPECT_EQ(v, expectedOut)
      << "taosGetOSFirstDayOfWeek should return AppleFirstWeekday value (1-7) converted to 0-6";
}

TEST(osLocaleTests, firstDayOfWeek_macos_set_and_read_all_values) {
  /* Save raw preference to restore later. */
  CFPropertyListRef origPref = CFPreferencesCopyAppValue(
      CFSTR("AppleFirstWeekday"), kCFPreferencesCurrentApplication);

  /* Iterate all CFCalendar days: 1-7 (1=Sunday, ..., 7=Saturday).
     Expected output after conversion: day - 1 (0=Sunday, ..., 6=Saturday). */
  bool apiAvailable = true;
  for (int32_t raw = 1; raw <= 7; ++raw) {
    setAppleFirstWeekday(raw);

    int32_t v = taosGetOSFirstDayOfWeek();

    if (v == -1) {
      apiAvailable = false;
      break;
    }

    EXPECT_EQ(v, raw - 1)
        << "Failed for AppleFirstWeekday gregorian = " << raw
        << " (expected output " << (raw - 1) << ")";
  }

  /* Restore original preference regardless of failures. */
  CFPreferencesSetAppValue(
      CFSTR("AppleFirstWeekday"), origPref, kCFPreferencesCurrentApplication);
  if (origPref != NULL) CFRelease(origPref);
  CFPreferencesAppSynchronize(kCFPreferencesCurrentApplication);

  if (!apiAvailable) {
    GTEST_SKIP() << "OS first day API unavailable on macOS";
  }
}

#endif
