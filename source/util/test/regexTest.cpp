
#include <chrono>
#include <cstdio>
#include <gtest/gtest.h>
#include <limits.h>
#include <taosdef.h>
#include "os.h"
#include "tutil.h"
#include "regex.h"
#include "osDef.h"
#include "tcompare.h"
#undef strcpy

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
