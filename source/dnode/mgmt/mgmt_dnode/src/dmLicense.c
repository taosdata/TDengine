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

int32_t dmStartLicenseThread(struct SDnodeMgmt *pMgmt) {
  return 0;
}

void dmStopLicenseThread(struct SDnodeMgmt *pMgmt) {
}
