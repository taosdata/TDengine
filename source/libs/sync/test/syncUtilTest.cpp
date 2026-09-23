/*
 * Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
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

#include "syncTest.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

void electRandomMSTest() {
  for (int i = 0; i < 10; ++i) {
    int32_t ms = syncUtilElectRandomMS(150, 300);
    printf("syncUtilElectRandomMS: %d \n", ms);
  }
}

// TD-SEC-2026-044 regression test.
// Verifies that the strict identity comparison (used at SYNC ingress authorization)
// never trusts the CID==0 wildcard, while the legacy syncUtilSameId still does.
// Returns the number of failed assertions so the process exit code can flag
// regressions to CI (a security regression test that only prints is useless in CI).
int sameIdStrictTest() {
  SRaftId a = {0};
  SRaftId b = {0};
  int     failures = 0;

#define CHECK(cond)                                   \
  do {                                                \
    if (!(cond)) {                                    \
      ++failures;                                     \
      printf("FAILED: %s\n", #cond);                  \
    }                                                 \
  } while (0)

  // 1) exact match (same cluster id, node id, vgId) must succeed
  a.addr = ((uint64_t)7 << 32) | 42;
  a.vgId = 3;
  b.addr = ((uint64_t)7 << 32) | 42;
  b.vgId = 3;
  CHECK(syncUtilSameIdStrict(&a, &b) == true);

  // 2) the exploitable case: forged CID=0 but valid node id / vgId.
  //    syncUtilSameId (legacy, with CID==0 wildcard) accepts it,
  //    syncUtilSameIdStrict (fix) MUST reject it.
  a.addr = ((uint64_t)7 << 32) | 42;  // genuine member, cluster id 7
  a.vgId = 3;
  b.addr = ((uint64_t)0 << 32) | 42;  // attacker forges CID=0
  b.vgId = 3;
  CHECK(syncUtilSameId(&a, &b) == true);     // legacy behavior, kept on purpose
  CHECK(syncUtilSameIdStrict(&a, &b) == false);  // security fix: must be rejected

  // 3) fully different id must be rejected by both
  a.addr = ((uint64_t)7 << 32) | 42;
  a.vgId = 3;
  b.addr = ((uint64_t)9 << 32) | 43;
  b.vgId = 3;
  CHECK(syncUtilSameIdStrict(&a, &b) == false);
  CHECK(syncUtilSameId(&a, &b) == false);

#undef CHECK

  if (failures == 0) {
    printf("sameIdStrictTest: ALL PASSED\n");
  } else {
    printf("sameIdStrictTest: %d FAILURES\n", failures);
  }
  return failures;
}

int main() {
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;
  logTest();

  int failures = 0;
  electRandomMSTest();
  failures += sameIdStrictTest();

  return failures == 0 ? 0 : 1;
}
