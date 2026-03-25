# License Center C SDK Integration Design

**Date:** 2026-03-25  
**Status:** Approved  
**Scope:** Integrate `libtaos_license` C SDK into taosd for license validation via CULS (Customer License Server)

---

## Problem Statement

taosd currently has no connection to the license center. We need to integrate the License Center C SDK so that each taosd dnode:

1. Connects to a CULS instance at startup
2. Periodically fetches and logs the active license
3. Enters a 2-week grace period when the license is unavailable or the instance is blacklisted
4. Shuts down gracefully when the grace period expires

---

## Architecture

### Component Overview

```
taos.cfg
  culsAddr = /ip4/192.168.1.100/tcp/8087/p2p/12D3KooW...
       │
       ▼
  taosd (dnode)
  ┌─────────────────────────────────────────────┐
  │  dmMgmt layer                               │
  │  ┌─────────────────────────────────────────┐│
  │  │  dmLicenseThread  (new)                 ││
  │  │  ┌──────────────────────────────────┐   ││
  │  │  │  taos_sdk_handle_t  (C SDK)      │   ││
  │  │  │  heartbeat + get_license / 10s   │   ││
  │  │  └──────────────────────────────────┘   ││
  │  │  grace_start_ms → {dataDir}/            ││
  │  │                   license_grace.json    ││
  │  └─────────────────────────────────────────┘│
  └─────────────────────────────────────────────┘
       │  libp2p / TCP
       ▼
  CULS (Customer License Server)
```

### Integration Layer

The license thread lives in `dmMgmt` alongside existing background threads (`dmMonitor`, `dmAudit`, `dmStatus`, etc.). It is started in `dmStartMgmt()` and stopped in `dmStopMgmt()`.

---

## Section 1: Configuration

### New Global Variables

```c
// include/common/tglobal.h
extern char tsCulsAddr[512];   // CULS P2P multiaddress; empty → grace period immediately
```

```c
// source/common/src/tglobal.c
char tsCulsAddr[512] = {0};

// In taosAddServerCfg():
TAOS_CHECK_RETURN(cfgAddString(pCfg, "culsAddr", tsCulsAddr,
    CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,
    CFG_CATEGORY_LOCAL, CFG_PRIV_SYSTEM));

// In taosCfgLoadFromCfgItem():
TAOS_CHECK_GET_CFG_ITEM(pCfg, pItem, "culsAddr");
tstrncpy(tsCulsAddr, pItem->str, sizeof(tsCulsAddr));
```

### taos.cfg Example

```
# CULS P2P multiaddress. Required for license validation.
# If empty, taosd enters grace period immediately on startup.
# culsAddr  /ip4/192.168.1.100/tcp/8087/p2p/12D3KooWxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

---

## Section 2: Thread Architecture & State Machine

### New Files

| File | Purpose |
|------|---------|
| `source/dnode/mgmt/mgmt_dnode/src/dmLicense.c` | Thread loop, state machine, grace period logic |
| `source/dnode/mgmt/mgmt_dnode/inc/dmLicense.h` | Private header: `SDmLicenseCtx`, function declarations |

### State Enum

```c
typedef enum {
  DM_LICENSE_STATE_INIT     = 0,  // before first check
  DM_LICENSE_STATE_OK,            // valid license held
  DM_LICENSE_STATE_GRACE,         // grace period running
  DM_LICENSE_STATE_EXPIRED,       // grace period elapsed → shutdown
} EDmLicenseState;
```

> `taos_sdk_create()` is a synchronous blocking call (10 s timeout per SDK docs). There is no intermediate CONNECTING state; the thread transitions directly from INIT to OK or GRACE based on the return code.

### Context Structure (embedded in SDnodeMgmt)

```c
typedef struct SDmLicenseCtx {
  TdThread            thread;
  EDmLicenseState     state;
  taos_sdk_handle_t  *pSdk;
  int64_t             gracePeriodStartMs;   // 0 = not in grace period
  taos_license_info_t lastLicense;
} SDmLicenseCtx;
```

### Thread Loop

Sleeps 200 ms per iteration (same as monitor thread). Runs the license check every 10 seconds.

```
STATE_INIT:
  if tsCulsAddr == ""
    → uWarn("culsAddr not configured, entering grace period")
    → dmLicenseLoadOrStartGrace()
    → STATE_GRACE  // pSdk remains NULL; no recovery possible until restart with valid culsAddr
  else
    → snprintf(instance_id, sizeof(instance_id), "%s:%u", tsLocalFqdn, tsServerPort)
    → taos_sdk_create(&pSdk, tsCulsAddr, instance_id)
      OK   → taos_sdk_get_license():
               hard failure (NO_LICENSE / BLACKLISTED / REVOKED / EXPIRED)
                 → uWarn(...) → dmLicenseLoadOrStartGrace() → STATE_GRACE
               transient error (NETWORK_ERROR / VERIFY_FAILED / ERROR)
                 → uWarn("Initial license fetch failed: %s, will retry") → STATE_OK
               TAOS_LICENSE_OK → uInfo(...) → STATE_OK
      FAIL → uError("Failed to connect to CULS: %s", errStr)
             dmLicenseLoadOrStartGrace() → STATE_GRACE  // pSdk remains NULL

STATE_OK (every 10s):
  taos_sdk_heartbeat()
    FAIL → uWarn("License heartbeat failed: %s", errStr)  // tolerate, don't change state

  taos_sdk_get_license(&info):
    TAOS_LICENSE_OK           → uInfo("License: id=%s type=%s valid_until=%s "
                                      "timeseries=%lu cpu_cores=%u dnodes=%u",
                                      ...) ; lastLicense = info
    TAOS_LICENSE_NO_LICENSE   → uWarn("No license available, entering grace period")
                                → dmLicenseLoadOrStartGrace() → STATE_GRACE
    TAOS_INSTANCE_BLACKLISTED → uWarn("Instance [%s] has been blacklisted, entering grace period",
                                      instance_id)
                                → dmLicenseLoadOrStartGrace() → STATE_GRACE
    TAOS_LICENSE_REVOKED      → uWarn("License revoked, entering grace period")
                                → dmLicenseLoadOrStartGrace() → STATE_GRACE
    TAOS_LICENSE_EXPIRED      → uWarn("License expired, entering grace period")
                                → dmLicenseLoadOrStartGrace() → STATE_GRACE
    TAOS_LICENSE_NETWORK_ERROR
    TAOS_LICENSE_VERIFY_FAILED
    TAOS_LICENSE_ERROR        → uWarn("License check error: %s", errStr)
                                // network transient: do not trigger grace

STATE_GRACE (every 10s):
  // Try to recover — only possible if pSdk was created successfully
  // (i.e. culsAddr was configured and taos_sdk_create() succeeded).
  // If pSdk == NULL (empty culsAddr or SDK create failure), recovery
  // requires restarting taosd with a valid culsAddr; the grace countdown
  // continues uninterrupted.
  if pSdk != NULL:
    taos_sdk_get_license(&info):
      TAOS_LICENSE_OK → uInfo("License recovered, grace period cancelled")
                        → dmLicenseCancelGrace()  // delete persisted file
                        → STATE_OK

  // Check expiry
  elapsed = now - gracePeriodStartMs
  if elapsed >= 14 days:
    → STATE_EXPIRED
  else:
    → uWarn("License grace period: %lld days %lld hours remaining", days, hours)

STATE_EXPIRED:
  uError("License grace period expired, initiating shutdown")
  dmShutdown()
```

### Thread Lifecycle in dmInt.c

```c
// dmStartMgmt() — append at end:
TAOS_CHECK_RETURN(dmStartLicenseThread(pMgmt));

// dmStopMgmt() — append:
dmStopLicenseThread(pMgmt);  // sets stopped flag, joins thread, calls taos_sdk_destroy()
```

---

## Section 3: Grace Period Persistence

### File

```
{tsDataDir}/license_grace.json
```

Content:
```json
{ "grace_start_ms": 1742900000000 }
```

### Read/Write Logic

Use `cJSON` + `taosOpenFile` / `taosWriteFile` — the same pattern used in `source/dnode/mgmt/node_util/src/dmFile.c`.

```c
// Enter grace period (or resume after restart):
int32_t dmLicenseLoadOrStartGrace(SDmLicenseCtx *pCtx) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/license_grace.json", tsDataDir);

    if (taosCheckExistFile(path)) {
        // Read file, parse with cJSON, extract "grace_start_ms"
        // Example: cJSON *pRoot = cJSON_Parse(buf);
        //          pCtx->gracePeriodStartMs = cJSON_GetObjectItem(pRoot, "grace_start_ms")->valuedouble;
        //          cJSON_Delete(pRoot);
        uWarn("Resuming license grace period from previous run (started %" PRId64 " ms)", pCtx->gracePeriodStartMs);
    } else {
        // First time entering grace — record current time
        pCtx->gracePeriodStartMs = taosGetTimestampMs();
        // Build JSON: cJSON *pRoot = cJSON_CreateObject();
        //             cJSON_AddNumberToObject(pRoot, "grace_start_ms", (double)pCtx->gracePeriodStartMs);
        //             char *pStr = cJSON_Print(pRoot);
        // Write with: TdFilePtr pFile = taosOpenFile(path, TD_FILE_CREATE|TD_FILE_WRITE|TD_FILE_TRUNC|TD_FILE_WRITE_THROUGH);
        //             taosWriteFile(pFile, pStr, strlen(pStr));
        //             taosCloseFile(&pFile);
        //             cJSON_free(pStr); cJSON_Delete(pRoot);
        //             On write failure: log uError and continue (grace countdown still runs in memory)
        uWarn("License grace period started at %" PRId64 " ms", pCtx->gracePeriodStartMs);
    }
    return 0;
}

// Cancel grace period (license recovered):
void dmLicenseCancelGrace(SDmLicenseCtx *pCtx) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/license_grace.json", tsDataDir);
    (void)taosRemoveFile(path);
    pCtx->gracePeriodStartMs = 0;
}
```

### Grace Period Duration

```c
#define DM_LICENSE_GRACE_PERIOD_MS  (14LL * 24LL * 3600LL * 1000LL)  // 2 weeks
```

---

## Section 4: Build System

### Static Library Placement

```
deps/license/
  include/
    taos_license.h
  lib/
    linux-x86_64/
      libtaos_license.a
    linux-aarch64/
      libtaos_license.a
```

The library is a Rust-compiled static archive. Windows is not supported in this iteration.

### CMakeLists.txt Changes

In `source/dnode/mgmt/mgmt_dnode/CMakeLists.txt`:

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

  target_sources(dnode_mgmt PRIVATE src/dmLicense.c)
  target_link_libraries(dnode_mgmt PRIVATE taos_license pthread dl m)
endif()
```

---

## Section 5: Error Handling & Testing

### Error Handling Summary

| Situation | Action |
|-----------|--------|
| `taos_sdk_create` fails | `uError`, enter grace period |
| `TAOS_LICENSE_NETWORK_ERROR` | `uWarn`, retry next cycle (no grace) |
| `TAOS_LICENSE_VERIFY_FAILED` | `uWarn`, retry next cycle (no grace) |
| `TAOS_LICENSE_NO_LICENSE` | `uWarn`, enter grace period |
| `TAOS_LICENSE_REVOKED` | `uWarn`, enter grace period |
| `TAOS_LICENSE_EXPIRED` | `uWarn`, enter grace period |
| `TAOS_INSTANCE_BLACKLISTED` | `uWarn` with instance ID, enter grace period |
| Grace period: license recovered | `uInfo`, cancel grace, back to OK |
| Grace period expired | `uError`, call `dmShutdown()` |
| Error strings from SDK | Always freed with `taos_sdk_free_error_string()` |

### Unit Tests (`mgmt_dnode/test/dmLicenseTest.cc`)

- Grace period file read/write correctness
- Restart resumes countdown from persisted timestamp (mock `taosGetTimestampMs`)
- 2-week expiry triggers shutdown callback
- License recovery in grace period deletes file and returns to OK state
- `culsAddr` empty → immediate grace period, no SDK init

### Integration Test Scenarios

- taosd starts with valid `culsAddr` → license info appears in log
- `culsAddr` empty → grace period warn log from startup
- CULS returns blacklist → instance ID logged, grace period starts
- Grace period recovery → `uInfo("License recovered")` in log

---

## File Change Summary

| File | Change |
|------|--------|
| `include/common/tglobal.h` | Add `extern char tsCulsAddr[512]` |
| `source/common/src/tglobal.c` | Add `tsCulsAddr` definition, `cfgAddString`, load |
| `source/dnode/mgmt/mgmt_dnode/inc/dmInt.h` | Add `SDmLicenseCtx licenseCtx` field to `SDnodeMgmt` |
| `source/dnode/mgmt/mgmt_dnode/inc/dmLicense.h` | New: `SDmLicenseCtx`, function declarations |
| `source/dnode/mgmt/mgmt_dnode/src/dmLicense.c` | New: thread loop, state machine, grace period |
| `source/dnode/mgmt/mgmt_dnode/src/dmInt.c` | Call `dmStartLicenseThread` / `dmStopLicenseThread` |
| `source/dnode/mgmt/mgmt_dnode/CMakeLists.txt` | Link `taos_license`, add `dmLicense.c` |
| `deps/license/include/taos_license.h` | New: copied from SDK |
| `deps/license/lib/linux-x86_64/libtaos_license.a` | New: prebuilt static library |
| `deps/license/lib/linux-aarch64/libtaos_license.a` | New: prebuilt static library (if needed) |
| `mgmt_dnode/test/dmLicenseTest.cc` | New: unit tests |
