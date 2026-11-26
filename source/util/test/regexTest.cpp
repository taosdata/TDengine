
/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include <chrono>
#include <cstdio>
#include <gtest/gtest.h>
#include <limits.h>
#ifndef WINDOWS
#include <atomic>
#include <locale.h>
#include <setjmp.h>
#include <thread>
#include <vector>
#endif
#include <taosdef.h>
#include "os.h"
#include "tutil.h"
#include "regex.h"
#include "osDef.h"
#include "tcompare.h"
#include "tconv.h"
#include "ttypes.h"

extern "C" {
  typedef struct UsingRegex UsingRegex;
  typedef struct HashRegexPtr HashRegexPtr;
  int32_t getRegComp(const char *pPattern, HashRegexPtr **regexRet);
  int32_t threadGetRegComp(regex_t **regex, const char *pPattern);
}

class regexTest {
 public:
  regexTest() { (void)InitRegexCache(); }
  ~regexTest() { (void)DestroyRegexCache(); }
};
static regexTest test;

static threadlocal regex_t pRegex;
static threadlocal char    *pOldPattern = NULL;

void DestoryThreadLocalRegComp1() {
  if (NULL != pOldPattern) {
    regfree(&pRegex);
    taosMemoryFree(pOldPattern);
    pOldPattern = NULL;
  }
}

static regex_t *threadGetRegComp1(const char *pPattern) {
  if (NULL != pOldPattern) {
    if( strcmp(pOldPattern, pPattern) == 0) {
      return &pRegex;
    } else {
      DestoryThreadLocalRegComp1();
    }
  }
  pOldPattern = (char*)taosMemoryMalloc(strlen(pPattern) + 1);
  if (NULL == pOldPattern) {
    uError("Failed to Malloc when compile regex pattern %s.", pPattern);
    return NULL;
  }
  strcpy(pOldPattern, pPattern);
  int32_t cflags = REG_EXTENDED;
  int32_t ret = regcomp(&pRegex, pPattern, cflags);
  if (ret != 0) {
    char msgbuf[256] = {0};
    regerror(ret, &pRegex, msgbuf, tListLen(msgbuf));
    uError("Failed to compile regex pattern %s. reason %s", pPattern, msgbuf);
    DestoryThreadLocalRegComp1();
    return NULL;
  }
  return &pRegex;
}

TEST(testCase, regexCacheTest1) {
  int times = 100000;
  char    s1[] = "abc";
  auto start = std::chrono::high_resolution_clock::now();

#ifndef WINDOWS
  uint64_t t0 = taosGetTimestampUs();
  for (int i = 0; i < times; i++) {
    HashRegexPtr* ret = NULL;
    int32_t       code = getRegComp(s1, &ret);
    if (code != 0) {
      FAIL() << "Failed to compile regex pattern " << s1;
    }
  }
  uint64_t t1 = taosGetTimestampUs();

  printf("%s regex(current) %d times:%" PRIu64 " us.\n", s1, times, t1 - t0);
 #endif

  uint64_t t2 = taosGetTimestampUs();
  for(int i = 0; i < times; i++) {
    regex_t* rex = threadGetRegComp1(s1);
  }
  uint64_t t3 = taosGetTimestampUs();

  printf("%s regex(before) %d times:%" PRIu64 " us.\n", s1, times, t3 - t2);

  t2 = taosGetTimestampUs();
  for(int i = 0; i < times; i++) {
    regex_t* rex = NULL;
    (void)threadGetRegComp(&rex, s1);
  }
  t3 = taosGetTimestampUs();

  printf("%s regex(new) %d times:%" PRIu64 " us.\n", s1, times, t3 - t2);
}

TEST(testCase, regexCacheTest2) {
  int times = 100000;
  char    s1[] = "abc%*";
  auto start = std::chrono::high_resolution_clock::now();

#ifndef WINDOWS
  uint64_t t0 = taosGetTimestampUs();
  for (int i = 0; i < times; i++) {
    HashRegexPtr* ret = NULL;
    int32_t       code = getRegComp(s1, &ret);
    if (code != 0) {
      FAIL() << "Failed to compile regex pattern " << s1;
    }
  }
  uint64_t t1 = taosGetTimestampUs();

  printf("%s regex(current) %d times:%" PRIu64 " us.\n", s1, times, t1 - t0);
  #endif

  uint64_t t2 = taosGetTimestampUs();
  for(int i = 0; i < times; i++) {
    regex_t* rex = threadGetRegComp1(s1);
  }
  uint64_t t3 = taosGetTimestampUs();

  printf("%s regex(before) %d times:%" PRIu64 " us.\n", s1, times, t3 - t2);

  t2 = taosGetTimestampUs();
  for(int i = 0; i < times; i++) {
    regex_t* rex = NULL;
    (void)threadGetRegComp(&rex, s1);
  }
  t3 = taosGetTimestampUs();

  printf("%s regex(new) %d times:%" PRIu64 " us.\n", s1, times, t3 - t2);
}

TEST(testCase, regexCacheTest3) {
  int times = 100000;
  char    s1[] = "abc%*";
  char    s2[] = "abc";
  auto start = std::chrono::high_resolution_clock::now();

#ifndef WINDOWS
  uint64_t t0 = taosGetTimestampUs();
  for (int i = 0; i < times; i++) {
    HashRegexPtr* ret = NULL;
    int32_t       code = getRegComp(s1, &ret);
    if (code != 0) {
      FAIL() << "Failed to compile regex pattern " << s1;
    }
  }
  uint64_t t1 = taosGetTimestampUs();

  printf("'%s' and '%s' take place by turn regex(current) %d times:%" PRIu64 " us.\n", s1, s2, times, t1 - t0);
#endif

  uint64_t t2 = taosGetTimestampUs();
  for(int i = 0; i < times; i++) {
    regex_t* rex = threadGetRegComp1(s1);
    rex = threadGetRegComp1(s2);
  }
  uint64_t t3 = taosGetTimestampUs();

  printf("'%s' and '%s' take place by turn regex(before) %d times:%" PRIu64 " us.\n", s1, s2, times, t3 - t2);

  t2 = taosGetTimestampUs();
  for(int i = 0; i < times; i++) {
    regex_t* rex = NULL;
    (void)threadGetRegComp(&rex, s1);
    (void)threadGetRegComp(&rex, s2);
  }
  t3 = taosGetTimestampUs();

  printf("'%s' and '%s' take place by turn regex(new) %d times:%" PRIu64 " us.\n", s1, s2, times, t3 - t2);
}

TEST(testCase, regexCacheTest4) {
  int times = 100;
  int count = 1000;
  char    s1[] = "abc%*";
  char    s2[] = "abc";
  auto start = std::chrono::high_resolution_clock::now();

#ifndef WINDOWS
  uint64_t t0 = taosGetTimestampUs();
  for (int i = 0; i < times; i++) {
    for (int j = 0; j < count; ++j) {
      HashRegexPtr* ret = NULL;
      int32_t       code = getRegComp(s1, &ret);
      if (code != 0) {
        FAIL() << "Failed to compile regex pattern " << s1;
      }
    }
    for (int j = 0; j < count; ++j) {
      HashRegexPtr* ret = NULL;
      int32_t       code = getRegComp(s2, &ret);
      if (code != 0) {
        FAIL() << "Failed to compile regex pattern " << s2;
      }
    }
  }
  uint64_t t1 = taosGetTimestampUs();

  printf("'%s' and '%s' take place by turn(per %d count) regex(current) %d times:%" PRIu64 " us.\n", s1, s2, count, times, t1 - t0);
#endif

  uint64_t t2 = taosGetTimestampUs();
  for (int i = 0; i < times; i++) {
    for (int j = 0; j < count; ++j) {
      regex_t* rex = threadGetRegComp1(s1);
    }
    for (int j = 0; j < count; ++j) {
      regex_t* rex = threadGetRegComp1(s2);
    }
  }
  uint64_t t3 = taosGetTimestampUs();

  printf("'%s' and '%s' take place by turn(per %d count) regex(before) %d times:%" PRIu64 " us.\n", s1, s2, count, times, t3 - t2);

  t2 = taosGetTimestampUs();
  for (int i = 0; i < times; i++) {
    for (int j = 0; j < count; ++j) {
      regex_t* rex = NULL;
      (void)threadGetRegComp(&rex, s1);
    }
    for (int j = 0; j < count; ++j) {
      regex_t* rex = NULL;
      (void)threadGetRegComp(&rex, s2);
    }
  }
  t3 = taosGetTimestampUs();

  printf("'%s' and '%s' take place by turn(per %d count) regex(new) %d times:%" PRIu64 " us.\n", s1, s2, count, times, t3 - t2);
}

//  It is not a good idea to test this case, because it will take a long time.
/*
TEST(testCase, regexCacheTest5) {
  int times = 10000;
  int count = 10000;
  char    s1[] = "abc%*";
  char    s2[] = "abc";
  auto start = std::chrono::high_resolution_clock::now();

  uint64_t t0 = taosGetTimestampUs();
  for (int i = 0; i < times; i++) {
    for (int j = 0; j < count; ++j) {
      HashRegexPtr* ret = NULL;
      int32_t       code = getRegComp(s1, &ret);
      if (code != 0) {
        FAIL() << "Failed to compile regex pattern " << s1;
      }
    }
    for (int j = 0; j < count; ++j) {
      HashRegexPtr* ret = NULL;
      int32_t       code = getRegComp(s2, &ret);
      if (code != 0) {
        FAIL() << "Failed to compile regex pattern " << s2;
      }
    }
  }
  uint64_t t1 = taosGetTimestampUs();

  printf("'%s' and '%s' take place by turn(per %d count) regex(current) %d times:%" PRIu64 " us.\n", s1, s2, count, times, t1 - t0);

  uint64_t t2 = taosGetTimestampUs();
  for (int i = 0; i < times; i++) {
    for (int j = 0; j < count; ++j) {
      regex_t* rex = threadGetRegComp1(s1);
    }
    for (int j = 0; j < count; ++j) {
      regex_t* rex = threadGetRegComp1(s2);
    }
  }
  uint64_t t3 = taosGetTimestampUs();

  printf("'%s' and '%s' take place by turn(per %d count) regex(before) %d times:%" PRIu64 " us.\n", s1, s2, count, times, t3 - t2);

  t2 = taosGetTimestampUs();
  for (int i = 0; i < times; i++) {
    for (int j = 0; j < count; ++j) {
      regex_t* rex = NULL;
      (void)threadGetRegComp(&rex, s1);
    }
    for (int j = 0; j < count; ++j) {
      regex_t* rex = NULL;
      (void)threadGetRegComp(&rex, s2);
    }
  }
  t3 = taosGetTimestampUs();

  printf("'%s' and '%s' take place by turn(per %d count) regex(new) %d times:%" PRIu64 " us.\n", s1, s2, count, times, t3 - t2);
}

TEST(testCase, regexCacheTest6) {
  int  times = 10000;
  int  count = 1000;
  char s1[] = "abc%*";
  char s2[] = "abc";
  auto start = std::chrono::high_resolution_clock::now();

  uint64_t t0 = taosGetTimestampUs();
  for (int i = 0; i < times; i++) {
    for (int j = 0; j < count; ++j) {
      HashRegexPtr* ret = NULL;
      int32_t       code = getRegComp(s1, &ret);
      if (code != 0) {
        FAIL() << "Failed to compile regex pattern " << s1;
      }
    }
    for (int j = 0; j < count; ++j) {
      HashRegexPtr* ret = NULL;
      int32_t       code = getRegComp(s2, &ret);
      if (code != 0) {
        FAIL() << "Failed to compile regex pattern " << s2;
      }
    }
  }
  uint64_t t1 = taosGetTimestampUs();

  printf("'%s' and '%s' take place by turn(per %d count) regex(current) %d times:%" PRIu64 " us.\n", s1, s2, count, times, t1 - t0);

  uint64_t t2 = taosGetTimestampUs();
  for (int i = 0; i < times; i++) {
    for (int j = 0; j < count; ++j) {
      regex_t* rex = threadGetRegComp1(s1);
    }
    for (int j = 0; j < count; ++j) {
      regex_t* rex = threadGetRegComp1(s2);
    }
  }
  uint64_t t3 = taosGetTimestampUs();

  printf("'%s' and '%s' take place by turn(per %d count) regex(before) %d times:%" PRIu64 " us.\n", s1, s2, count, times, t3 - t2);

  t2 = taosGetTimestampUs();
  for (int i = 0; i < times; i++) {
    for (int j = 0; j < count; ++j) {
      regex_t* rex = NULL;
      (void)threadGetRegComp(&rex, s1);
    }
    for (int j = 0; j < count; ++j) {
      regex_t* rex = NULL;
      (void)threadGetRegComp(&rex, s2);
    }
  }
  t3 = taosGetTimestampUs();

  printf("'%s' and '%s' take place by turn(per %d count) regex(new) %d times:%" PRIu64 " us.\n", s1, s2, count, times, t3 - t2);
}
*/
#ifndef WINDOWS
class RegexCharsetTest : public testing::Test {
 protected:
  void SetUp() override { taosRegexSetPoolSize(1); }
  void TearDown() override { taosRegexSetPoolSize(1); }
};

TEST_F(RegexCharsetTest, regexClientCharsetPool) {
  taosRegexSetPoolSize(2);
  void *utf8 = taosConvInit("UTF-8");
  ASSERT_NE(utf8, nullptr);
  locale_t original = uselocale((locale_t)0);
  ASSERT_EQ(taosCheckRegexLocale(utf8), TSDB_CODE_SUCCESS);
  EXPECT_EQ(uselocale((locale_t)0), original);
  ASSERT_EQ(taosExecRegexMatchCharset("\xe4\xb8\xad", "^(.)$", utf8), 0);
  EXPECT_EQ(uselocale((locale_t)0), original);
  EXPECT_EQ(taosExecRegexMatchCharset("ab", "^(.)$", utf8), 1);
  EXPECT_EQ(taosExecRegexMatchCharset("ab", "", utf8), 1);
  EXPECT_EQ(taosCheckRegexCharset("(", utf8), TSDB_CODE_PAR_REGULAR_EXPRESSION_ERROR);
  EXPECT_EQ(uselocale((locale_t)0), original);
  int32_t start = -1, len = -1;
  ASSERT_EQ(taosExecRegexGroupsCharset("\xe4\xb8\xad", "(.)", 1, &start, &len, utf8), 0);
  EXPECT_EQ(start, 0);
  EXPECT_EQ(len, 3);
  ASSERT_EQ(taosExecRegexGroupsCharset("ab", "(a|ab)", 1, &start, &len, utf8), 0);
  EXPECT_EQ(len, 2);

  std::atomic<int> failures(0);
  std::vector<std::thread> workers;
  for (int i = 0; i < 8; ++i) {
    workers.emplace_back([&]() {
      locale_t before = uselocale((locale_t)0);
      for (int j = 0; j < 500; ++j) {
        if (taosExecRegexMatchCharset("\xe4\xb8\xad", "^(.)$", utf8) != 0 ||
            uselocale((locale_t)0) != before) ++failures;
      }
    });
  }
  for (auto &worker : workers) worker.join();
  EXPECT_EQ(failures.load(), 0);

  // Executor failures/kill checks may longjmp after scalar evaluation returns.
  // There must be no query-wide locale guard left on that unwinding path.
  jmp_buf exitPoint;
  if (setjmp(exitPoint) == 0) {
    int32_t code = taosCheckRegexCharset("(", utf8);
    if (code != TSDB_CODE_SUCCESS) longjmp(exitPoint, 1);
  }
  EXPECT_EQ(uselocale((locale_t)0), original);
  EXPECT_EQ(taosExecRegexMatchCharset("\xe4\xb8\xad", "^(.)$", utf8), 0);
}

struct RegexCancelTest {
  void *charset;
  std::atomic<bool> started;
  bool restored;
  locale_t original;
};

static void regexCancelCleanup(void *arg) {
  RegexCancelTest *state = static_cast<RegexCancelTest *>(arg);
  state->restored = uselocale((locale_t)0) == state->original;
}

static void *regexCancelableWorker(void *arg) {
  RegexCancelTest *state = static_cast<RegexCancelTest *>(arg);
  state->original = uselocale((locale_t)0);
  taosThreadCleanupPush(regexCancelCleanup, state);
  (void)taosThreadSetCancelType(PTHREAD_CANCEL_ASYNCHRONOUS, nullptr);
  state->started.store(true);
  for (;;) {
    (void)taosExecRegexMatchCharset("\xe4\xb8\xad", "^(.{1})$", state->charset);
    taosThreadTestCancel();
  }
  taosThreadCleanupPop(1);
  return nullptr;
}

TEST_F(RegexCharsetTest, regexPoolRestoresLocaleBeforeThreadCancellation) {
  taosRegexSetPoolSize(1);
  RegexCancelTest state = {};
  state.charset = taosConvInit("UTF-8");
  ASSERT_NE(state.charset, nullptr);
  // Avoid racing first-use locale discovery with the cancellation itself.
  ASSERT_EQ(taosExecRegexMatchCharset("\xe4\xb8\xad", "^(.{1})$", state.charset), 0);
  TdThread worker;
  ASSERT_EQ(taosThreadCreate(&worker, nullptr, regexCancelableWorker, &state), 0);
  while (!state.started.load()) std::this_thread::yield();
  ASSERT_EQ(taosThreadCancel(worker), 0);
  void *result = nullptr;
  ASSERT_EQ(taosThreadJoin(worker, &result), 0);
  EXPECT_TRUE(state.restored);
  // A leaked borrowed slot would hang the sole remaining user here.
  EXPECT_EQ(taosExecRegexMatchCharset("\xe4\xb8\xad", "^(.{1})$", state.charset), 0);
}
#endif
#ifndef WINDOWS
TEST_F(RegexCharsetTest, regexClientCharsetPoolsAreSeparate) {
  void *utf8 = taosConvInit("UTF-8");
  void *gbk = taosConvInit("GBK");
  if (gbk == nullptr) GTEST_SKIP() << "GBK converter unavailable";
  int32_t code = taosCheckRegexLocale(gbk);
  if (code == TSDB_CODE_CHARSET_CONVERT_ERROR) GTEST_SKIP() << "GBK locale unavailable";
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(utf8, nullptr);
  locale_t original = uselocale((locale_t)0);
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(taosExecRegexMatchCharset("\xe4\xb8\xad", "^(.)$", utf8), 0);
    EXPECT_EQ(taosExecRegexMatchCharset("\xd6\xd0", "^(.)$", gbk), 0);
  }
#ifdef __GLIBC__
  void *cp936 = taosConvInit("CP936");
  ASSERT_NE(cp936, nullptr);
  EXPECT_EQ(taosCheckRegexLocale(cp936), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosExecRegexMatchCharset("\xd6\xd0", "^(.)$", cp936), 0);
#endif
  EXPECT_EQ(taosExecRegexMatchCharset("\x81\x5c", "^[\x81\x5c]$", gbk), 0);
  int32_t start = -1, len = -1;
  ASSERT_EQ(taosExecRegexGroupsCharset("\xd6\xd0", "(.)", 1, &start, &len, gbk), 0);
  EXPECT_EQ(start, 0);
  EXPECT_EQ(len, 2);

  char input[VARSTR_HEADER_SIZE + sizeof(TdUcs4)] = {};
  char pattern[VARSTR_HEADER_SIZE + 8] = {};
  TdUcs4 cp = 0x4e2d;
  memcpy(varDataVal(input), &cp, sizeof(cp));
  varDataSetLen(input, sizeof(cp));
  memcpy(varDataVal(pattern), "^.$", 3);
  varDataSetLen(pattern, 3);
  int32_t match = 1;
  ASSERT_EQ(taosCompareRegexCharset(input, TSDB_DATA_TYPE_NCHAR, pattern, TSDB_DATA_TYPE_VARCHAR,
                                  gbk, &match), TSDB_CODE_SUCCESS);
  EXPECT_EQ(match, 0);
  cp = 0x1f600;  // Not representable in GBK: fail conversion, never invert into NMATCH success.
  memcpy(varDataVal(input), &cp, sizeof(cp));
  EXPECT_EQ(taosCompareRegexCharset(input, TSDB_DATA_TYPE_NCHAR, pattern, TSDB_DATA_TYPE_VARCHAR,
                                  gbk, &match), TSDB_CODE_SCALAR_CONVERT_NCHAR_ERROR);
  EXPECT_EQ(uselocale((locale_t)0), original);
}

TEST_F(RegexCharsetTest, regexPoolEvictionUnderConcurrentUse) {
  void *utf8 = taosConvInit("UTF-8");
  ASSERT_NE(utf8, nullptr);
  ASSERT_EQ(taosCheckRegexLocale(utf8), TSDB_CODE_SUCCESS);
  std::atomic<int> failures(0);
  std::atomic<bool> start(false);
  std::vector<std::thread> workers;
  // More than the cache's 20 entries forces eviction and recompilation. Workers
  // also revisit shared patterns while other workers cycle through new keys.
  for (int i = 0; i < 4; ++i) {
    workers.emplace_back([&, i]() {
      locale_t original = uselocale((locale_t)0);
      while (!start.load()) std::this_thread::yield();
      for (int j = 0; j < 80; ++j) {
        char pattern[64];
        snprintf(pattern, sizeof(pattern), "^(a|key%d_%d)$", i, j % 40);
        if (taosExecRegexMatchCharset("a", pattern, utf8) != 0 ||
            taosExecRegexMatchCharset("z", pattern, utf8) != 1 ||
            taosExecRegexMatchCharset("a", "^(a|shared)$", utf8) != 0 ||
            uselocale((locale_t)0) != original) {
          ++failures;
        }
      }
    });
  }
  start.store(true);
  for (auto &worker : workers) worker.join();
  EXPECT_EQ(failures.load(), 0);
  // Changing the configured size must preserve existing cached matchers and
  // allow new pools to be created under the new setting.
  taosRegexSetPoolSize(3);
  EXPECT_EQ(taosExecRegexMatchCharset("a", "^(a|shared)$", utf8), 0);
  EXPECT_EQ(taosExecRegexMatchCharset("a", "^(a|new_pool_size)$", utf8), 0);
}
#endif

TEST(regexTest, connectionCharsetCaptureOffsets) {
  void *gbk = taosConvInit("GBK");
  void *utf8 = taosConvInit("UTF-8");
  ASSERT_NE(gbk, nullptr);
  ASSERT_NE(utf8, nullptr);
#ifndef WINDOWS
  if (taosCheckRegexLocale(gbk) != 0) GTEST_SKIP() << "GBK locale unavailable";
#endif
  regmatch_t matches[3];
  ASSERT_EQ(taosExecRegexCapturesCharset("x\xd6\xd0\xce\xc4", "x(.)(.)", 3, matches, 0, gbk), 0);
  EXPECT_EQ(matches[1].rm_so, 1);
  EXPECT_EQ(matches[1].rm_eo, 3);
  EXPECT_EQ(matches[2].rm_so, 3);
  EXPECT_EQ(matches[2].rm_eo, 5);
  EXPECT_EQ(taosExecRegexMatchCharset("\xe6\x96\x87\xe4\xb8\xad", "^...$", gbk), 0);
  EXPECT_EQ(taosExecRegexMatchCharset("\xe6\x96\x87\xe4\xb8\xad", "^...$", utf8), 1);
  EXPECT_EQ(taosExecRegexCapturesCharset("abc", "^a", 3, matches, REG_NOTBOL, gbk), 1);
  EXPECT_NE(taosCheckRegexCharset("[", gbk), 0);
  EXPECT_EQ(taosExecRegexMatchCharset("\x81\x5c", "^[\x81\x5c]$", gbk), 0);
}

TEST(regexTest, emptyPatternReplacementPreservesMatchSemantics) {
  void *utf8 = taosConvInit("UTF-8");
  ASSERT_NE(utf8, nullptr);
  regmatch_t match;
  ASSERT_EQ(0, taosExecRegexCapturesCharset("ab", "", 1, &match, 0, utf8));
  EXPECT_EQ(0, match.rm_so);
  EXPECT_EQ(0, match.rm_eo);
  ASSERT_EQ(0, taosExecRegexCapturesCharset("b", "", 1, &match, REG_NOTBOL, utf8));
  EXPECT_EQ(0, match.rm_so);
  EXPECT_EQ(0, match.rm_eo);
  EXPECT_EQ(1, taosExecRegexMatchCharset("ab", "", utf8));
  int32_t start = -1, length = 0;
  EXPECT_EQ(1, taosExecRegexGroupsCharset("ab", "", 0, &start, &length, utf8));
}

#ifndef WINDOWS
TEST(regexTest, asciiSyntaxValidationUsesBuiltinLocale) {
  // Exercise both platforms' CODESET names, including the ASCII context that
  // checkRegexPattern creates internally regardless of its input context.
  const char *names[] = {"ASCII", "US-ASCII", "ANSI_X3.4-1968", "CP367", "IBM367", "ISO646-US"};
  for (const char *name : names) {
    SCOPED_TRACE(name);
    void *ascii = taosConvInit(name);
    ASSERT_NE(ascii, nullptr);
    ASSERT_EQ(0, taosCheckRegexLocale(ascii));
    EXPECT_EQ(0, taosExecRegexMatchCharset("abc", "^[a-z]+$", ascii));
    EXPECT_EQ(0, checkRegexPattern("^[a-z]+$", ascii));
    EXPECT_NE(0, checkRegexPattern("[", ascii));
  }
}
#endif
