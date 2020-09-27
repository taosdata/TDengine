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

#include <stdio.h>

#include "ehash.h"
#include "os.h"
#include "tglobal.h"
#include "tlockfree.h"


#define TEST(test)                                                               \
do {                                                                             \
  DD("start testing: [%s]...", #test);                                           \
  int r = test_##test();                                                         \
  DD("testing: [%s] %s[%d]", #test, r==0?"done":"failed", r);                    \
  if (r) return r;                                                               \
} while (0)

// external test cases
extern int test_ehash(void);
extern int test_hash(void);
extern int test_cache(void);

static int test_lock(void);

static int test(void) {
  TEST(lock);
  TEST(ehash);
  TEST(hash);
  TEST(cache);
  return 0;
}

int main(int argc, char *argv[]) {
  uDebugFlag = 127U;
  taosInitLog("test", 1024, 10);
  int r = test();
  taosCloseLog();
  return r;
}

static int test_lock(void) {
  SRWLatch latch;

  taosInitRWLatch(&latch);

  taosWLockLatch(&latch);
  taosWUnLockLatch(&latch);
  taosRLockLatch(&latch);
  taosRUnLockLatch(&latch);

  // fprintf(stderr, "rlock\n");
  // taosRLockLatch(&latch);
  // fprintf(stderr, "wlock\n");
  // taosWLockLatch(&latch);
  // // fprintf(stderr, "rlock\n");
  // // taosRLockLatch(&latch);
  // // fprintf(stderr, "rulock\n");
  // // taosRUnLockLatch(&latch);
  // fprintf(stderr, "wulock\n");
  // taosWUnLockLatch(&latch);
  // fprintf(stderr, "rulock\n");
  // taosRUnLockLatch(&latch);

  return 0;
}

