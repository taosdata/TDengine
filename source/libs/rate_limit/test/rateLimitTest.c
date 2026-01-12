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

#include "../inc/rateLimit.h"
#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include "../inc/rateLimitMgr.h"

#define MILLISECOND_PER_SECOND 1000LL

void testTokenBucket() {
  printf("Testing Token Bucket Algorithm...\n");

  STokenBucket* bucket = rlCreateTokenBucket(10, 5);
  assert(bucket != NULL);

  assert(rlTokenBucketAllow(bucket, 10) == true);
  assert(rlTokenBucketAllow(bucket, 1) == false);

  usleep(200 * 1000);

  assert(rlTokenBucketAllow(bucket, 1) == true);

  rlDestroyTokenBucket(bucket);
  printf("Token Bucket test passed!\n\n");
}

void testLeakyBucket() {
  printf("Testing Leaky Bucket Algorithm...\n");

  SLeakyBucket* bucket = rlCreateLeakyBucket(10, 5);
  assert(bucket != NULL);

  for (int i = 0; i < 10; i++) {
    bool allowed = rlLeakyBucketAllow(bucket, 1);
    if (i < 10) {
      assert(allowed == true);
    }
  }

  bool allowed = rlLeakyBucketAllow(bucket, 1);
  assert(allowed == false);

  usleep(200 * 1000);
  allowed = rlLeakyBucketAllow(bucket, 1);
  assert(allowed == true);

  rlDestroyLeakyBucket(bucket);
  printf("Leaky Bucket test passed!\n\n");
}

void testSlidingWindow() {
  printf("Testing Sliding Window Algorithm...\n");

  SSlidingWindow* window = rlCreateSlidingWindow(1000, 10, 10);
  assert(window != NULL);

  for (int i = 0; i < 10; i++) {
    assert(rlSlidingWindowAllow(window, 1) == true);
  }

  assert(rlSlidingWindowAllow(window, 1) == false);

  usleep(1100 * 1000);
  assert(rlSlidingWindowAllow(window, 1) == true);

  rlDestroySlidingWindow(window);
  printf("Sliding Window test passed!\n\n");
}

void testConnectionLimiting() {
  printf("Testing Connection Limiting...\n");

  assert(rlmInit(100, 100, 100) == 0);

  const char* ip1 = "192.168.1.1";
  const char* ip2 = "192.168.1.2";

  assert(rlmAllowConnection(ip1) == 0);
  assert(rlmAllowConnection(ip2) == 0);

  rlmConnectionClosed(ip1);
  assert(rlmAllowConnection(ip1) == 0);

  rlmConnectionClosed(ip1);
  rlmConnectionClosed(ip2);

  rlmCleanup();
  printf("Connection Limiting test passed!\n\n");
}

void testQueryLimiting() {
  printf("Testing Query Limiting...\n");

  assert(rlmInit(100, 10, 100) == 0);

  const char* user1 = "user1";
  const char* user2 = "user2";
  const char* ip = "192.168.1.1";

  for (int i = 0; i < 10; i++) {
    assert(rlmAllowQuery(user1, NULL, ip) == 0);
  }

  assert(rlmAllowQuery(user1, NULL, ip) != 0);

  assert(rlmAllowQuery(user2, NULL, ip) == 0);

  usleep(60000 * 1000);

  assert(rlmAllowQuery(user1, NULL, ip) == 0);

  rlmCleanup();
  printf("Query Limiting test passed!\n\n");
}

void testStatistics() {
  printf("Testing Statistics...\n");

  assert(rlmInit(100, 10, 100) == 0);

  const char* user = "testuser";

  int64_t allowed, rejected;
  rlmGetStats(RLM_TARGET_USER, user, &allowed, &rejected);
  assert(allowed == 0 && rejected == 0);

  rlmAllowQuery(user, NULL, "192.168.1.1");
  rlmAllowQuery(user, NULL, "192.168.1.1");

  rlmGetStats(RLM_TARGET_USER, user, &allowed, &rejected);
  assert(allowed == 2);

  rlmResetLimiter(RLM_TARGET_USER, user);

  rlmGetStats(RLM_TARGET_USER, user, &allowed, &rejected);
  assert(allowed == 0 && rejected == 0);

  rlmCleanup();
  printf("Statistics test passed!\n\n");
}

void testStress() {
  printf("Testing Stress Conditions...\n");

  assert(rlmInit(1000, 100, 100) == 0);

  int allowedCount = 0;
  int rejectedCount = 0;

  for (int i = 0; i < 10000; i++) {
    const char* ip = "192.168.1.100";
    if (rlmAllowConnection(ip) == 0) {
      allowedCount++;
    } else {
      rejectedCount++;
      rlmConnectionClosed(ip);
    }
  }

  printf("Stress test: %d allowed, %d rejected\n", allowedCount, rejectedCount);

  rlmCleanup();
  printf("Stress test passed!\n\n");
}

int main() {
  printf("TDengine Rate Limiting Module Test Suite\n");
  printf("========================================\n\n");

  testTokenBucket();
  testLeakyBucket();
  testSlidingWindow();
  testConnectionLimiting();
  testQueryLimiting();
  testStatistics();
  testStress();

  printf("========================================\n");
  printf("All tests passed successfully!\n");

  return 0;
}
