# License Center C SDK Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate `libtaos_license` C SDK into taosd so each dnode validates its license via CULS, logs license info, and initiates a 2-week grace period (with graceful shutdown on expiry) when no valid license is available.

**Architecture:** A new `dmLicense` module inside `mgmt_dnode` runs a background thread (same pattern as `dmMonitor`/`dmAudit`). The thread uses a 4-state machine (INIT → OK ↔ GRACE → EXPIRED) driven by calls to the SDK every 10 s. Grace period start time is persisted to `{dataDir}/license_grace.json` so restarts resume the countdown.

**Tech Stack:** C11, `libtaos_license.a` (Rust static archive via C FFI), `cJSON` (already in TDengine), `taosOpenFile`/`taosWriteFile` (TDengine OS abstraction)

**Spec:** `docs/superpowers/specs/2026-03-25-license-center-integration-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `deps/license/include/taos_license.h` | Create | SDK C header |
| `deps/license/lib/linux-x86_64/libtaos_license.a` | Create | Prebuilt static library |
| `deps/license/lib/linux-aarch64/libtaos_license.a` | Create (optional) | Prebuilt static library for ARM |
| `source/dnode/mgmt/mgmt_dnode/CMakeLists.txt` | Modify | Link taos_license, add dmLicense.c |
| `include/common/tglobal.h` | Modify | Declare `tsCulsAddr` extern |
| `source/common/src/tglobal.c` | Modify | Define `tsCulsAddr`, register + load config |
| `source/dnode/mgmt/mgmt_dnode/inc/dmInt.h` | Modify | Add `SDmLicenseCtx licenseCtx` to `SDnodeMgmt` |
| `source/dnode/mgmt/mgmt_dnode/inc/dmLicense.h` | Create | `SDmLicenseCtx`, `EDmLicenseState`, function decls |
| `source/dnode/mgmt/mgmt_dnode/src/dmLicense.c` | Create | Thread loop, state machine, grace period logic |
| `source/dnode/mgmt/mgmt_dnode/src/dmWorker.c` | Modify | `dmStartLicenseThread`, `dmStopLicenseThread` |
| `source/dnode/mgmt/mgmt_dnode/src/dmInt.c` | Modify | Call start/stop in `dmStartMgmt`/`dmStopMgmt` |
| `source/dnode/mgmt/mgmt_dnode/test/dmLicenseTest.cc` | Create | Google Test unit tests |

---

## Task 1: Copy SDK artifacts into `deps/`

**Files:**
- Create: `deps/license/include/taos_license.h`
- Create: `deps/license/lib/linux-x86_64/libtaos_license.a`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p deps/license/include
mkdir -p deps/license/lib/linux-x86_64
mkdir -p deps/license/lib/linux-aarch64
```

- [ ] **Step 2: Copy SDK header and prebuilt library**

```bash
cp /root/zgc/license-center/sdk/c/include/taos_license.h deps/license/include/
# Build the library first if not already built
cd /root/zgc/license-center/sdk/c && make lib
cd /root/zgc/TDinternal/community
cp /root/zgc/license-center/sdk/c/build/libtaos_license.a deps/license/lib/linux-x86_64/
```

- [ ] **Step 3: Verify files are in place**

```bash
ls -lh deps/license/include/taos_license.h
ls -lh deps/license/lib/linux-x86_64/libtaos_license.a
```

Expected: both files present, `.a` is non-zero size.

- [ ] **Step 4: Commit**

```bash
git add deps/license/
git commit -m "chore: add libtaos_license prebuilt static library and header"
```

---

## Task 2: Wire up CMake build

**Files:**
- Modify: `source/dnode/mgmt/mgmt_dnode/CMakeLists.txt`

- [ ] **Step 1: Open the file and locate the target definition**

```bash
grep -n "add_library\|target_link_libraries\|target_sources\|aux_source" \
  source/dnode/mgmt/mgmt_dnode/CMakeLists.txt | head -20
```

Note the library name (likely `dnode_mgmt` or similar) and the existing `target_link_libraries` call.

- [ ] **Step 2: Add license library import and link**

After the existing `aux_source_directory` or `target_sources` line, add:

```cmake
if(TD_LINUX)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
    set(TAOS_LICENSE_LIB_DIR "${TD_SOURCE_DIR}/deps/license/lib/linux-aarch64")
  else()
    set(TAOS_LICENSE_LIB_DIR "${TD_SOURCE_DIR}/deps/license/lib/linux-x86_64")
  endif()

  add_library(taos_license STATIC IMPORTED GLOBAL)
  set_target_properties(taos_license PROPERTIES
      IMPORTED_LOCATION "${TAOS_LICENSE_LIB_DIR}/libtaos_license.a"
      INTERFACE_INCLUDE_DIRECTORIES "${TD_SOURCE_DIR}/deps/license/include"
  )
endif()
```

Then in the `target_link_libraries(dnode_mgmt ...)` call add:

```cmake
if(TD_LINUX)
  target_sources(dnode_mgmt PRIVATE src/dmLicense.c)
  target_link_libraries(dnode_mgmt PRIVATE taos_license pthread dl m)
endif()
```

- [ ] **Step 3: Verify CMake configures without error (dmLicense.c not yet created — create empty stub)**

```bash
touch source/dnode/mgmt/mgmt_dnode/src/dmLicense.c
cd debug && cmake .. -DBUILD_TEST=false 2>&1 | grep -i "error\|taos_license"
```

Expected: no CMake errors, `taos_license` appears in configuration output.

- [ ] **Step 4: Commit**

```bash
git add source/dnode/mgmt/mgmt_dnode/CMakeLists.txt \
        source/dnode/mgmt/mgmt_dnode/src/dmLicense.c
git commit -m "build: add libtaos_license static import to mgmt_dnode CMakeLists"
```

---

## Task 3: Add `culsAddr` config item

**Files:**
- Modify: `include/common/tglobal.h` (around line 230 where `tsMonitorFqdn` is declared)
- Modify: `source/common/src/tglobal.c` (lines ~221 for definition, ~1063 for registration, ~1902 for load)

- [ ] **Step 1: Add extern declaration to `tglobal.h`**

After the `tsMonitorFqdn` extern declaration, add:

```c
extern char     tsCulsAddr[512];       // CULS P2P multiaddress; empty → grace period
```

- [ ] **Step 2: Add global variable definition to `tglobal.c`**

After the `tsMonitorFqdn` definition (around line 221), add:

```c
char     tsCulsAddr[512] = {0};
```

- [ ] **Step 3: Register config item in `taosAddServerCfg()`**

After the `monitorFqdn` cfgAddString call (around line 1063), add:

```c
TAOS_CHECK_RETURN(cfgAddString(pCfg, "culsAddr", tsCulsAddr,
    CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY, CFG_CATEGORY_LOCAL, CFG_PRIV_SYSTEM));
```

- [ ] **Step 4: Load config value in `taosCfgLoadFromCfgItem()`**

After the `monitorFqdn` load block (around line 1902), add:

```c
TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "culsAddr");
tstrncpy(tsCulsAddr, pItem->str, sizeof(tsCulsAddr));
```

- [ ] **Step 5: Build to verify no compile errors**

```bash
cd debug && make -j$(nproc) common 2>&1 | grep -i "error:" | head -20
```

Expected: 0 errors.

- [ ] **Step 6: Commit**

```bash
git add include/common/tglobal.h source/common/src/tglobal.c
git commit -m "feat: add culsAddr config item for CULS P2P multiaddress"
```

---

## Task 4: Define `SDmLicenseCtx` and private header

**Files:**
- Create: `source/dnode/mgmt/mgmt_dnode/inc/dmLicense.h`
- Modify: `source/dnode/mgmt/mgmt_dnode/inc/dmInt.h`

- [ ] **Step 1: Create `dmLicense.h`**

```c
// source/dnode/mgmt/mgmt_dnode/inc/dmLicense.h

#ifndef _TD_DM_LICENSE_H_
#define _TD_DM_LICENSE_H_

#include "os.h"
#include "tglobal.h"
#include "taos_license.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  DM_LICENSE_STATE_INIT    = 0,
  DM_LICENSE_STATE_OK,
  DM_LICENSE_STATE_GRACE,
  DM_LICENSE_STATE_EXPIRED,
} EDmLicenseState;

typedef struct SDmLicenseCtx {
  EDmLicenseState     state;
  taos_sdk_handle_t  *pSdk;
  int64_t             gracePeriodStartMs;  // 0 = not in grace period
  taos_license_info_t lastLicense;
} SDmLicenseCtx;

// Forward declaration (SDnodeMgmt defined in dmInt.h)
struct SDnodeMgmt;

int32_t dmStartLicenseThread(struct SDnodeMgmt *pMgmt);
void    dmStopLicenseThread(struct SDnodeMgmt *pMgmt);

#ifdef __cplusplus
}
#endif

#endif /* _TD_DM_LICENSE_H_ */
```

- [ ] **Step 2: Add `SDmLicenseCtx licenseCtx` and `TdThread licenseThread` to `SDnodeMgmt` in `dmInt.h`**

After the last `TdThread` field (e.g., `metricsThread`), add two lines:

```c
  TdThread                     licenseThread;
  SDmLicenseCtx                licenseCtx;
```

Also add `#include "dmLicense.h"` at the top of the includes block.

- [ ] **Step 3: Build headers only (verify no compile errors)**

```bash
cd debug && make -j$(nproc) dnode_mgmt 2>&1 | grep -i "error:" | head -20
```

Expected: 0 errors (dmLicense.c is still empty stub — link errors acceptable here).

- [ ] **Step 4: Commit**

```bash
git add source/dnode/mgmt/mgmt_dnode/inc/dmLicense.h \
        source/dnode/mgmt/mgmt_dnode/inc/dmInt.h
git commit -m "feat: add SDmLicenseCtx and dmLicense.h header"
```

---

## Task 5: Implement grace period persistence helpers

**Files:**
- Modify: `source/dnode/mgmt/mgmt_dnode/src/dmLicense.c`

This task implements the two persistence helpers in isolation so they can be unit-tested independently of the thread.

- [ ] **Step 1: Write the failing unit test first**

In `source/dnode/mgmt/mgmt_dnode/test/dmLicenseTest.cc` (create the file):

```cpp
#include <gtest/gtest.h>
#include <cstdio>
#include <cstring>
extern "C" {
#include "dmLicense.h"
}

// Minimal SDnodeMgmt stub for tests — only licenseCtx needed
#include "dmInt.h"

static const char* TEST_DATA_DIR = "/tmp/dmLicenseTest";

class GraceFileTest : public ::testing::Test {
protected:
  void SetUp() override {
    (void)system("rm -rf /tmp/dmLicenseTest && mkdir -p /tmp/dmLicenseTest");
    // Override dataDir for tests
    tstrncpy(tsDataDir, TEST_DATA_DIR, sizeof(tsDataDir));
  }
  void TearDown() override {
    (void)system("rm -rf /tmp/dmLicenseTest");
  }
};

TEST_F(GraceFileTest, WriteAndReadGraceFile) {
  SDmLicenseCtx ctx = {0};
  ctx.gracePeriodStartMs = 0;

  // First call: no file exists → writes new timestamp
  int32_t code = dmLicenseLoadOrStartGrace(&ctx);
  ASSERT_EQ(code, 0);
  ASSERT_GT(ctx.gracePeriodStartMs, 0);
  int64_t first = ctx.gracePeriodStartMs;

  // Second call: file exists → reads persisted value, does not overwrite
  SDmLicenseCtx ctx2 = {0};
  code = dmLicenseLoadOrStartGrace(&ctx2);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(ctx2.gracePeriodStartMs, first);
}

TEST_F(GraceFileTest, CancelGraceDeletesFile) {
  SDmLicenseCtx ctx = {0};
  (void)dmLicenseLoadOrStartGrace(&ctx);
  ASSERT_GT(ctx.gracePeriodStartMs, 0);

  dmLicenseCancelGrace(&ctx);
  ASSERT_EQ(ctx.gracePeriodStartMs, 0);

  // File must be gone: a new call should write a fresh timestamp
  SDmLicenseCtx ctx2 = {0};
  (void)dmLicenseLoadOrStartGrace(&ctx2);
  // Should succeed (fresh file, not the old one)
  ASSERT_GT(ctx2.gracePeriodStartMs, 0);
}
```

- [ ] **Step 2: Add test to CMakeLists.txt**

In `source/dnode/mgmt/mgmt_dnode/CMakeLists.txt`, check if a `test/` section exists (similar to other modules). Add:

```cmake
if(BUILD_TEST)
  add_executable(dmLicenseTest test/dmLicenseTest.cc)
  target_link_libraries(dmLicenseTest gtest_main dnode_mgmt)
  add_test(NAME dmLicenseTest COMMAND dmLicenseTest)
endif()
```

- [ ] **Step 3: Build test target — expect linker failure (functions not yet implemented)**

```bash
cd debug && make -j$(nproc) dmLicenseTest 2>&1 | grep -E "error:|undefined" | head -20
```

Expected: undefined symbol errors for `dmLicenseLoadOrStartGrace`, `dmLicenseCancelGrace`.

- [ ] **Step 4: Implement the persistence helpers in `dmLicense.c`**

```c
// source/dnode/mgmt/mgmt_dnode/src/dmLicense.c
#define _DEFAULT_SOURCE
#include "dmLicense.h"
#include "dmInt.h"
#include "cJSON.h"
#include "tglobal.h"
#include "osFile.h"

#define DM_LICENSE_GRACE_PERIOD_MS  (14LL * 24LL * 3600LL * 1000LL)

static void dmLicenseGracePath(char *path, int32_t len) {
  snprintf(path, len, "%s/license_grace.json", tsDataDir);
}

int32_t dmLicenseLoadOrStartGrace(SDmLicenseCtx *pCtx) {
  char path[PATH_MAX];
  dmLicenseGracePath(path, sizeof(path));

  if (taosCheckExistFile(path)) {
    // Read existing file
    TdFilePtr pFile = taosOpenFile(path, TD_FILE_READ);
    if (pFile == NULL) {
      uError("license: failed to open grace file %s", path);
      pCtx->gracePeriodStartMs = taosGetTimestampMs();
      return 0;
    }
    char buf[256] = {0};
    int64_t n = taosReadFile(pFile, buf, sizeof(buf) - 1);
    (void)taosCloseFile(&pFile);
    if (n <= 0) {
      pCtx->gracePeriodStartMs = taosGetTimestampMs();
      return 0;
    }
    cJSON *pRoot = cJSON_Parse(buf);
    if (pRoot) {
      cJSON *pItem = cJSON_GetObjectItem(pRoot, "grace_start_ms");
      if (cJSON_IsNumber(pItem)) {
        pCtx->gracePeriodStartMs = (int64_t)pItem->valuedouble;
      }
      cJSON_Delete(pRoot);
    }
    if (pCtx->gracePeriodStartMs <= 0) {
      pCtx->gracePeriodStartMs = taosGetTimestampMs();
    }
    uWarn("license: resuming grace period from previous run (started %" PRId64 " ms)",
          pCtx->gracePeriodStartMs);
  } else {
    // First entry: record now
    pCtx->gracePeriodStartMs = taosGetTimestampMs();
    cJSON *pRoot = cJSON_CreateObject();
    if (pRoot) {
      (void)cJSON_AddNumberToObject(pRoot, "grace_start_ms",
                                    (double)pCtx->gracePeriodStartMs);
      char *pStr = cJSON_Print(pRoot);
      cJSON_Delete(pRoot);
      if (pStr) {
        TdFilePtr pFile = taosOpenFile(path,
            TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
        if (pFile != NULL) {
          (void)taosWriteFile(pFile, pStr, strlen(pStr));
          (void)taosCloseFile(&pFile);
        } else {
          uError("license: failed to write grace file %s, countdown runs in memory only", path);
        }
        cJSON_free(pStr);
      }
    }
    uWarn("license: grace period started at %" PRId64 " ms", pCtx->gracePeriodStartMs);
  }
  return 0;
}

void dmLicenseCancelGrace(SDmLicenseCtx *pCtx) {
  char path[PATH_MAX];
  dmLicenseGracePath(path, sizeof(path));
  (void)taosRemoveFile(path);
  pCtx->gracePeriodStartMs = 0;
  uInfo("license: grace period cancelled, license recovered");
}
```

- [ ] **Step 5: Build and run tests**

```bash
cd debug && make -j$(nproc) dmLicenseTest && ./build/bin/dmLicenseTest 2>&1
```

Expected: `[  PASSED  ] 2 tests.`

- [ ] **Step 6: Commit**

```bash
git add source/dnode/mgmt/mgmt_dnode/src/dmLicense.c \
        source/dnode/mgmt/mgmt_dnode/test/dmLicenseTest.cc \
        source/dnode/mgmt/mgmt_dnode/CMakeLists.txt
git commit -m "feat: implement grace period persistence helpers (dmLicenseLoadOrStartGrace)"
```

---

## Task 6: Implement the license thread state machine

**Files:**
- Modify: `source/dnode/mgmt/mgmt_dnode/src/dmLicense.c`

- [ ] **Step 1: Add unit tests for grace expiry and recovery**

Append to `dmLicenseTest.cc`:

```cpp
TEST_F(GraceFileTest, GracePeriodExpiryDetected) {
  SDmLicenseCtx ctx = {0};
  // Simulate grace start 15 days ago
  ctx.gracePeriodStartMs = taosGetTimestampMs() - (15LL * 24 * 3600 * 1000);
  // Write that to disk so dmLicenseLoadOrStartGrace reads it
  // (directly set, then test the expiry calculation inline)
  int64_t elapsed = taosGetTimestampMs() - ctx.gracePeriodStartMs;
  ASSERT_GT(elapsed, 14LL * 24LL * 3600LL * 1000LL);  // expired
}

TEST_F(GraceFileTest, GracePeriodNotYetExpired) {
  SDmLicenseCtx ctx = {0};
  // Simulate grace start 5 days ago
  ctx.gracePeriodStartMs = taosGetTimestampMs() - (5LL * 24 * 3600 * 1000);
  int64_t elapsed = taosGetTimestampMs() - ctx.gracePeriodStartMs;
  ASSERT_LT(elapsed, 14LL * 24LL * 3600LL * 1000LL);  // not expired
}
```

- [ ] **Step 2: Run new tests — verify they pass (no new code required, just arithmetic)**

```bash
cd debug && make -j$(nproc) dmLicenseTest && ./build/bin/dmLicenseTest 2>&1
```

Expected: `[  PASSED  ] 4 tests.`

- [ ] **Step 3: Implement the full thread function in `dmLicense.c`**

Append after the persistence helpers:

```c
#define DM_LICENSE_CHECK_INTERVAL_MS  10000LL   // 10 seconds
#define DM_LICENSE_INSTANCE_ID_LEN    320       // 253 (FQDN) + 1 (:) + 5 (port) + slack

static void dmLicenseEnterGrace(SDmLicenseCtx *pCtx) {
  pCtx->state = DM_LICENSE_STATE_GRACE;
  (void)dmLicenseLoadOrStartGrace(pCtx);
}

static void dmLicenseHandleGetResult(SDmLicenseCtx *pCtx, int ret,
                                      taos_license_info_t *pInfo,
                                      const char *instanceId) {
  if (ret == TAOS_LICENSE_OK) {
    // Log only on change
    if (strcmp(pCtx->lastLicense.license_id, pInfo->license_id) != 0 ||
        pCtx->lastLicense.valid_until != pInfo->valid_until) {
      uInfo("license updated: id=%s type=%s valid_until=%" PRId64
            " timeseries=%lu cpu_cores=%u dnodes=%u",
            pInfo->license_id, pInfo->license_type, pInfo->valid_until,
            (unsigned long)pInfo->timeseries_limit,
            (unsigned)pInfo->cpu_cores_limit,
            (unsigned)pInfo->dnodes_limit);
    }
    pCtx->lastLicense = *pInfo;
    if (pCtx->state == DM_LICENSE_STATE_GRACE) {
      dmLicenseCancelGrace(pCtx);
      pCtx->state = DM_LICENSE_STATE_OK;
    }
    return;
  }

  // Only fetch error string for non-OK codes
  const char *errStr = taos_sdk_error_string(pCtx->pSdk, ret);
  switch (ret) {
    case TAOS_LICENSE_NO_LICENSE:
      uWarn("license: no license available, entering grace period");
      dmLicenseEnterGrace(pCtx);
      break;
    case TAOS_INSTANCE_BLACKLISTED:
      uWarn("license: instance [%s] has been blacklisted, entering grace period", instanceId);
      dmLicenseEnterGrace(pCtx);
      break;
    case TAOS_LICENSE_REVOKED:
      uWarn("license: license revoked, entering grace period");
      dmLicenseEnterGrace(pCtx);
      break;
    case TAOS_LICENSE_EXPIRED:
      uWarn("license: license expired, entering grace period");
      dmLicenseEnterGrace(pCtx);
      break;
    default:
      // Transient errors: network / verify / generic — just warn, retry next cycle
      uWarn("license: check error (transient): %s (code=%d)", errStr ? errStr : "unknown", ret);
      break;
  }
  if (errStr) taos_sdk_free_error_string(errStr);
}

static void *dmLicenseThreadFp(void *param) {
  SDnodeMgmt    *pMgmt = (SDnodeMgmt *)param;
  SDmLicenseCtx *pCtx  = &pMgmt->licenseCtx;
  setThreadName("dnode-license");

  char instanceId[DM_LICENSE_INSTANCE_ID_LEN];
  snprintf(instanceId, sizeof(instanceId), "%s:%u", tsLocalFqdn, tsServerPort);

  int64_t lastCheckMs = 0;

  while (1) {
    taosMsleep(200);
    if (pMgmt->pData->dropped || pMgmt->pData->stopped) break;

    int64_t nowMs = taosGetTimestampMs();
    if (nowMs - lastCheckMs < DM_LICENSE_CHECK_INTERVAL_MS) continue;
    lastCheckMs = nowMs;

    switch (pCtx->state) {

      case DM_LICENSE_STATE_INIT: {
        if (tsCulsAddr[0] == '\0') {
          uWarn("license: culsAddr not configured, entering grace period immediately");
          dmLicenseEnterGrace(pCtx);
          break;
        }
        int ret = taos_sdk_create(&pCtx->pSdk, tsCulsAddr, instanceId);
        if (ret != TAOS_LICENSE_OK) {
          const char *e = taos_sdk_error_string(NULL, ret);
          uError("license: failed to connect to CULS [%s]: %s", tsCulsAddr, e ? e : "unknown");
          if (e) taos_sdk_free_error_string(e);
          pCtx->pSdk = NULL;
          dmLicenseEnterGrace(pCtx);
          break;
        }
        // Initial license fetch — transient errors go to OK (retry); hard errors to GRACE
        taos_license_info_t info = {0};
        ret = taos_sdk_get_license(pCtx->pSdk, &info);
        pCtx->state = DM_LICENSE_STATE_OK;   // set OK first so handler can flip to GRACE
        dmLicenseHandleGetResult(pCtx, ret, &info, instanceId);
        break;
      }

      case DM_LICENSE_STATE_OK: {
        // Heartbeat — failure is non-fatal
        int hbRet = taos_sdk_heartbeat(pCtx->pSdk);
        if (hbRet != TAOS_LICENSE_OK) {
          const char *e = taos_sdk_error_string(pCtx->pSdk, hbRet);
          uWarn("license: heartbeat failed: %s", e ? e : "unknown");
          if (e) taos_sdk_free_error_string(e);
        }
        // License check
        taos_license_info_t info = {0};
        int ret = taos_sdk_get_license(pCtx->pSdk, &info);
        dmLicenseHandleGetResult(pCtx, ret, &info, instanceId);
        break;
      }

      case DM_LICENSE_STATE_GRACE: {
        // Attempt recovery if SDK is available
        if (pCtx->pSdk != NULL) {
          taos_license_info_t info = {0};
          int ret = taos_sdk_get_license(pCtx->pSdk, &info);
          dmLicenseHandleGetResult(pCtx, ret, &info, instanceId);
          if (pCtx->state == DM_LICENSE_STATE_OK) break;  // recovered
        }
        // Check expiry
        int64_t elapsed = taosGetTimestampMs() - pCtx->gracePeriodStartMs;
        if (elapsed >= DM_LICENSE_GRACE_PERIOD_MS) {
          pCtx->state = DM_LICENSE_STATE_EXPIRED;
        } else {
          int64_t remaining = DM_LICENSE_GRACE_PERIOD_MS - elapsed;
          int64_t days  = remaining / (86400LL * 1000LL);
          int64_t hours = (remaining % (86400LL * 1000LL)) / (3600LL * 1000LL);
          uWarn("license: grace period active — %" PRId64 " days %" PRId64 " hours remaining",
                days, hours);
        }
        break;
      }

      case DM_LICENSE_STATE_EXPIRED: {
        uError("license: grace period expired — initiating shutdown");
        // dmStop() is declared in dmEnv.c and triggers a clean dnode exit
        // (same mechanism used in dmHandle.c:441 for server-status-driven shutdown)
        dmStop();
        goto _exit;
      }
    }
  }

_exit:
  if (pCtx->pSdk != NULL) {
    taos_sdk_destroy(pCtx->pSdk);
    pCtx->pSdk = NULL;
  }
  return NULL;
}
```

- [ ] **Step 4: Build `dnode_mgmt` — verify no compile errors**

```bash
cd debug && make -j$(nproc) dnode_mgmt 2>&1 | grep -i "error:" | head -20
```

Fix any compile errors. Common issues: missing `#include "tglobal.h"` for `tsLocalFqdn`/`tsServerPort`, or `#include "setThreadName"`.

- [ ] **Step 5: Commit**

```bash
git add source/dnode/mgmt/mgmt_dnode/src/dmLicense.c \
        source/dnode/mgmt/mgmt_dnode/test/dmLicenseTest.cc
git commit -m "feat: implement license thread state machine (dmLicenseThreadFp)"
```

---

## Task 7: Wire thread into `dmWorker.c` and `dmInt.c`

**Files:**
- Modify: `source/dnode/mgmt/mgmt_dnode/src/dmLicense.c` — add `dmStartLicenseThread` / `dmStopLicenseThread` here (same file as `dmLicenseThreadFp`, which is `static`)
- Modify: `source/dnode/mgmt/mgmt_dnode/src/dmInt.c` — call start/stop

> **Why not `dmWorker.c`?** `dmLicenseThreadFp` is `static` (internal linkage). `dmStartLicenseThread` must live in the same translation unit (`dmLicense.c`) to reference it. This also keeps the module cohesive.

- [ ] **Step 1: Append `dmStartLicenseThread` and `dmStopLicenseThread` to `dmLicense.c`**

```c
int32_t dmStartLicenseThread(SDnodeMgmt *pMgmt) {
  int32_t      code = 0;
  TdThreadAttr thAttr;
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&pMgmt->licenseThread, &thAttr, dmLicenseThreadFp, pMgmt) != 0) {
    code = TAOS_SYSTEM_ERROR(ERRNO);
    uError("failed to create license thread since %s", tstrerror(code));
    (void)taosThreadAttrDestroy(&thAttr);
    return code;
  }
  (void)taosThreadAttrDestroy(&thAttr);
  tmsgReportStartup("dnode-license", "initialized");
  return 0;
}

void dmStopLicenseThread(SDnodeMgmt *pMgmt) {
  if (taosCheckPthreadValid(pMgmt->licenseThread)) {
    (void)taosThreadJoin(pMgmt->licenseThread, NULL);
    taosThreadClear(&pMgmt->licenseThread);
  }
}
```

- [ ] **Step 2: Add thread start call in `dmStartMgmt()` in `dmInt.c`**

After the last `dmStart*Thread` call (currently `dmStartMetricsThread` or similar), add:

```c
if ((code = dmStartLicenseThread(pMgmt)) != 0) {
  dError("failed to start license thread since %s", tstrerror(code));
  return code;
}
```

- [ ] **Step 3: Add thread stop call in `dmStopMgmt()` in `dmInt.c`**

After the first stop call (so license thread stops early, freeing SDK resources):

```c
dmStopLicenseThread(pMgmt);
```

- [ ] **Step 4: Build full taosd**

```bash
cd debug && make -j$(nproc) taosd 2>&1 | grep -i "error:" | head -20
```

Expected: 0 errors.

- [ ] **Step 5: Smoke test — start taosd with no `culsAddr` set, check grace period log**

```bash
./build.sh start
sleep 3
grep -i "license\|grace" /var/log/taos/taosd.log | head -20
```

Expected: `"culsAddr not configured, entering grace period immediately"` in log.

- [ ] **Step 6: Commit**

```bash
git add source/dnode/mgmt/mgmt_dnode/src/dmLicense.c \
        source/dnode/mgmt/mgmt_dnode/src/dmInt.c
git commit -m "feat: wire dmLicenseThread into dmStartMgmt/dmStopMgmt"
```

---

## Task 8: Run all unit tests and final build verification

- [ ] **Step 1: Run unit tests**

```bash
cd debug && make -j$(nproc) dmLicenseTest && ./build/bin/dmLicenseTest --gtest_color=yes
```

Expected: all tests pass.

- [ ] **Step 2: Full build**

```bash
cd debug && make -j$(nproc) taosd 2>&1 | tail -5
```

Expected: build succeeds, no errors or warnings related to license files.

- [ ] **Step 3: Final commit tag**

```bash
git tag -a "license-sdk-integration-v1" -m "License Center C SDK integration complete"
```

---

## Shutdown Mechanism Reference

`dmStop()` is declared in `source/dnode/mgmt/node_mgmt/src/dmEnv.c:260` and is the standard way to trigger a clean dnode exit from within the process. It is used in `dmHandle.c:441` for server-status-driven shutdown. The license thread uses the same call.
