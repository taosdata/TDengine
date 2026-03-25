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

#define _DEFAULT_SOURCE
#include "dmLicense.h"
#include "dmInt.h"
#include "taos_license.h"
#include "cJSON.h"
#include "tglobal.h"
#include "osFile.h"

// Grace period duration defined in dmLicense.h (DM_LICENSE_GRACE_PERIOD_MS)

static void dmLicenseGracePath(char *path, int32_t len) {
  snprintf(path, len, "%s/license_grace.json", tsDataDir);
}

int32_t dmLicenseLoadOrStartGrace(SDmLicenseCtx *pCtx) {
  char path[PATH_MAX];
  dmLicenseGracePath(path, sizeof(path));

  if (taosCheckExistFile(path)) {
    TdFilePtr pFile = taosOpenFile(path, TD_FILE_READ);
    if (pFile == NULL) {
      uError("license: failed to open grace file %s", path);
      pCtx->gracePeriodStartMs = taosGetTimestampMs();
      return 0;
    }
    char buf[256] = {0};
    int64_t n = taosReadFile(pFile, buf, sizeof(buf) - 1);
    (void)taosCloseFile(&pFile);
    if (n > 0) {
      cJSON *pRoot = cJSON_Parse(buf);
      if (pRoot) {
        cJSON *pItem = cJSON_GetObjectItem(pRoot, "grace_start_ms");
        if (cJSON_IsNumber(pItem)) {
          pCtx->gracePeriodStartMs = (int64_t)pItem->valuedouble;
        }
        cJSON_Delete(pRoot);
      }
    }
    if (pCtx->gracePeriodStartMs <= 0) {
      pCtx->gracePeriodStartMs = taosGetTimestampMs();
    }
    uWarn("license: resuming grace period from previous run (started %" PRId64 " ms)",
          pCtx->gracePeriodStartMs);
  } else {
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
    if (strcmp(pCtx->lastLicenseId, pInfo->license_id) != 0 ||
        pCtx->lastValidUntil != pInfo->valid_until) {
      uInfo("license updated: id=%s type=%s valid_until=%" PRId64
            " timeseries=%lu cpu_cores=%u dnodes=%u",
            pInfo->license_id, pInfo->license_type, pInfo->valid_until,
            (unsigned long)pInfo->timeseries_limit,
            (unsigned)pInfo->cpu_cores_limit,
            (unsigned)pInfo->dnodes_limit);
    }
    tstrncpy(pCtx->lastLicenseId, pInfo->license_id, sizeof(pCtx->lastLicenseId));
    pCtx->lastValidUntil = pInfo->valid_until;
    if (pCtx->state == DM_LICENSE_STATE_GRACE) {
      dmLicenseCancelGrace(pCtx);
      pCtx->state = DM_LICENSE_STATE_OK;
    }
    return;
  }

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
    default: {
      // Transient errors: network / verify / generic — just warn, retry next cycle
      const char *errStr = taos_sdk_error_string((taos_sdk_handle_t *)pCtx->pSdk, ret);
      uWarn("license: check error (transient): %s (code=%d)", errStr ? errStr : "unknown", ret);
      if (errStr) taos_sdk_free_error_string(errStr);
      break;
    }
  }
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
        int ret = taos_sdk_create((taos_sdk_handle_t **)&pCtx->pSdk, tsCulsAddr, instanceId);
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
        ret = taos_sdk_get_license((taos_sdk_handle_t *)pCtx->pSdk, &info);
        pCtx->state = DM_LICENSE_STATE_OK;   // set OK first so handler can flip to GRACE
        dmLicenseHandleGetResult(pCtx, ret, &info, instanceId);
        break;
      }

      case DM_LICENSE_STATE_OK: {
        // Heartbeat — failure is non-fatal
        int hbRet = taos_sdk_heartbeat((taos_sdk_handle_t *)pCtx->pSdk);
        if (hbRet != TAOS_LICENSE_OK) {
          const char *e = taos_sdk_error_string((taos_sdk_handle_t *)pCtx->pSdk, hbRet);
          uWarn("license: heartbeat failed: %s", e ? e : "unknown");
          if (e) taos_sdk_free_error_string(e);
        }
        // License check
        taos_license_info_t info = {0};
        int ret = taos_sdk_get_license((taos_sdk_handle_t *)pCtx->pSdk, &info);
        dmLicenseHandleGetResult(pCtx, ret, &info, instanceId);
        break;
      }

      case DM_LICENSE_STATE_GRACE: {
        // Attempt recovery if SDK is available
        if (pCtx->pSdk != NULL) {
          taos_license_info_t info = {0};
          int ret = taos_sdk_get_license((taos_sdk_handle_t *)pCtx->pSdk, &info);
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
        // dmStop() triggers a clean dnode exit
        dmStop();
        goto _exit;
      }
    }
  }

_exit:
  if (pCtx->pSdk != NULL) {
    taos_sdk_destroy((taos_sdk_handle_t *)pCtx->pSdk);
    pCtx->pSdk = NULL;
  }
  return NULL;
}

int32_t dmStartLicenseThread(struct SDnodeMgmt *pMgmt) {
  TdThreadAttr thAttr;
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&pMgmt->licenseThread, &thAttr, dmLicenseThreadFp, pMgmt) != 0) {
    (void)taosThreadAttrDestroy(&thAttr);
    uError("failed to create license thread: %s", strerror(errno));
    return -1;
  }
  (void)taosThreadAttrDestroy(&thAttr);
  return 0;
}

void dmStopLicenseThread(struct SDnodeMgmt *pMgmt) {
  pMgmt->pData->stopped = true;
  (void)taosThreadJoin(pMgmt->licenseThread, NULL);
}
