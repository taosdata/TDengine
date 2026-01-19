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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "libs/function/function.h"
#include "libs/function/tudf.h"
#include "smInt.h"
#include "stream.h"
#include "tencrypt.h"

extern SSnodeInfo gSnode;

static int32_t smRequire(const SMgmtInputOpt *pInput, bool *required) {
  char path[TSDB_FILENAME_LEN];
  snprintf(path, TSDB_FILENAME_LEN, "%s%ssnode%d", pInput->path, TD_DIRSEP, pInput->dnodeId);
  SJson    *pJson = NULL;

  int32_t code = dmReadFileJson(path, pInput->name, &pJson, required);
  if (code) {
    return code;
  }

  SDCreateSnodeReq req = {0};
  code = smBuildCreateReqFromJson(pJson, &req);
  if (code) {
    if (pJson != NULL) cJSON_Delete(pJson);
    return code;
  }

  smUpdateSnodeInfo(&req);

  if (pJson != NULL) cJSON_Delete(pJson);
  return code;
}

static void smInitOption(SSnodeMgmt *pMgmt, SSnodeOpt *pOption) { pOption->msgCb = pMgmt->msgCb; }

static void smClose(SSnodeMgmt *pMgmt) {
  if (pMgmt->pSnode != NULL) {
    sndClose(pMgmt->pSnode);
    smStopWorker(pMgmt);
    pMgmt->pSnode = NULL;
  }

  taosMemoryFree(pMgmt);
}
int32_t sndOpenWrapper(const char *path, SSnodeOpt *pOption, SSnode **pNode) {
  *pNode = sndOpen(path, pOption);
  if (*pNode == NULL) {
    return terrno;
  }
  return 0;
}

// Check and migrate checkpoint files to encrypted format if needed
int32_t smCheckAndMigrateCheckpoints(const char *path) {
  int32_t code = TSDB_CODE_SUCCESS;

  // Wait for encryption keys to be loaded
  code = taosWaitCfgKeyLoaded();
  if (TSDB_CODE_SUCCESS != code) {
    dError("failed to wait for encryption keys since %s", tstrerror(code));
    return code;
  }

  // Read encrypted flag from global snode info (set by smBuildCreateReqFromJson)
  taosRLockLatch(&gSnode.snodeLock);
  int32_t encrypted = gSnode.encrypted;
  taosRUnLockLatch(&gSnode.snodeLock);

  // If already encrypted or no metaKey, nothing to do
  if (encrypted || tsMetaKey[0] == '\0') {
    return 0;
  }

  dInfo("snode checkpoints not encrypted but tsMetaKey available, starting migration");

  // List all checkpoint files and re-encrypt them
  char checkpointDir[PATH_MAX] = {0};
  snprintf(checkpointDir, sizeof(checkpointDir), "%s%ssnode%scheckpoint", tsDataDir, TD_DIRSEP, TD_DIRSEP);

  TdDirPtr pDir = taosOpenDir(checkpointDir);
  if (pDir == NULL) {
    // Checkpoint directory doesn't exist yet, just mark as encrypted
    dInfo("checkpoint directory doesn't exist, marking as encrypted");
  } else {
    // Iterate through checkpoint files and re-encrypt each one
    TdDirEntryPtr de = NULL;
    while ((de = taosReadDir(pDir)) != NULL) {
      if (taosDirEntryIsDir(de)) {
        continue;
      }

      char *name = taosGetDirEntryName(de);
      if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
        continue;
      }

      // Only process .ck files
      if (strstr(name, ".ck") == NULL) {
        continue;
      }

      // Extract streamId from filename (format: <streamId>.ck)
      int64_t streamId = 0;
      if (sscanf(name, "%" PRIx64 ".ck", &streamId) != 1) {
        dWarn("skip invalid checkpoint file:%s", name);
        continue;
      }

      dInfo("migrating checkpoint file to encrypted format, streamId:%" PRIx64, streamId);

      // Read checkpoint
      void   *data = NULL;
      int64_t dataLen = 0;
      code = streamReadCheckPoint(streamId, &data, &dataLen);
      if (TSDB_CODE_SUCCESS != code) {
        dError("failed to read checkpoint for migration, streamId:%" PRIx64 " since %s", streamId, tstrerror(code));
        return code;
      }

      if (data == NULL || dataLen <= 0) {
        dError("invalid checkpoint data length, streamId:%" PRIx64, streamId);
        taosMemoryFree(data);
        return TSDB_CODE_INVALID_DATA_FMT;
      }

      // Rewrite checkpoint (will be encrypted with tsMetaKey)
      code = streamWriteCheckPoint(streamId, data, dataLen);
      taosMemoryFree(data);
      if (code != TSDB_CODE_SUCCESS) {
        dError("failed to rewrite checkpoint for streamId:%" PRIx64 " since %s", streamId, tstrerror(code));
        taosMemoryFree(data);
        taosCloseDir(&pDir);
        return code;
      }

      dInfo("successfully migrated checkpoint to encrypted format, streamId:%" PRIx64, streamId);
    }

    taosCloseDir(&pDir);
  }

  // Update and persist encrypted flag to snode.json
  // First need to read deployed status
  bool   deployed = false;
  SJson *pJson = NULL;
  code = dmReadFileJson(path, "snode", &pJson, &deployed);
  if (TSDB_CODE_SUCCESS != code) {
    dError("failed to read snode.json for persisting encrypted flag since %s", tstrerror(code));
    return code;
  }
  if (pJson != NULL) cJSON_Delete(pJson);

  // Now write with encrypted flag set to true
  code = dmWriteFileWithEncrypted(path, "snode", deployed, true);
  if (TSDB_CODE_SUCCESS != code) {
    dError("failed to persist encrypted flag to snode.json since %s", tstrerror(code));
    return code;
  }

  // Update global encrypted flag
  taosWLockLatch(&gSnode.snodeLock);
  gSnode.encrypted = 1;
  taosWUnLockLatch(&gSnode.snodeLock);

  dInfo("snode checkpoints migrated to encrypted format and persisted encrypted flag successfully");
  return TSDB_CODE_SUCCESS;
}

int32_t smOpen(SMgmtInputOpt *pInput, SMgmtOutputOpt *pOutput) {
  int32_t     code = 0;
  SSnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SSnodeMgmt));
  if (pMgmt == NULL) {
    code = terrno;
    return code;
  }

  pMgmt->pData = pInput->pData;
  pMgmt->path = pInput->path;
  pMgmt->name = pInput->name;
  pMgmt->msgCb = pInput->msgCb;
  pMgmt->msgCb.mgmt = pMgmt;
  pMgmt->msgCb.putToQueueFp = (PutToQueueFp)smPutMsgToQueue;

  SSnodeOpt option = {0};
  smInitOption(pMgmt, &option);

  code = sndOpenWrapper(pMgmt->path, &option, &pMgmt->pSnode);
  if (code != 0) {
    dError("failed to open snode since %s", tstrerror(code));
    smClose(pMgmt);
    return code;
  }

  tmsgReportStartup("snode-impl", "initialized");

  // Check and migrate checkpoint files to encrypted format if needed
  // The encrypted flag is read from snode.json in smRequire() via smBuildCreateReqFromJson()
  code = smCheckAndMigrateCheckpoints(pMgmt->path);
  if (TSDB_CODE_SUCCESS != code) {
    dError("failed to check and migrate snode checkpoints since %s", tstrerror(code));
    // Don't fail the startup, just log the error
    smClose(pMgmt);
    return code;
  }

  if ((code = smStartWorker(pMgmt)) != 0) {
    dError("failed to start snode worker since %s", tstrerror(code));
    smClose(pMgmt);
    return code;
  }
  tmsgReportStartup("snode-worker", "initialized");

  if ((code = udfcOpen()) != 0) {
    dError("failed to open udfc in snode since:%s", tstrerror(code));
    smClose(pMgmt);
    return code;
  }

  pOutput->pMgmt = pMgmt;
  return 0;
}

static int32_t smStartSnodes(SSnodeMgmt *pMgmt) { 
  return sndInit(pMgmt->pSnode); 
}

SMgmtFunc smGetMgmtFunc() {
  SMgmtFunc mgmtFunc = {0};
  mgmtFunc.openFp = smOpen;
  mgmtFunc.startFp = (NodeStartFp)smStartSnodes;
  mgmtFunc.closeFp = (NodeCloseFp)smClose;
  mgmtFunc.createFp = (NodeCreateFp)smProcessCreateReq;
  mgmtFunc.dropFp = (NodeDropFp)smProcessDropReq;
  mgmtFunc.requiredFp = smRequire;
  mgmtFunc.getHandlesFp = smGetMsgHandles;

  return mgmtFunc;
}
