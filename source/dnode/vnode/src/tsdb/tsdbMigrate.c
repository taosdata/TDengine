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

#include "tss.h"
#include "tsdb.h"
#include "tsdbFS2.h"
#include "tsdbFSet2.h"
#include "vnd.h"
#include "tsdbInt.h"

/* manifest format
{
    "fmtver": 1,
    "vnode": 3,
    "fid": 1736,
    "head": {
        "did.level": 0,
        "did.id": 0,
        "lcn": 0,
        "mcount": 0,
        "fid": 1736,
        "cid": 16,
        "size": 28138,
        "minVer": 4998,
        "maxVer": 6055
    },
    "data": {
        "did.level": 0,
        "did.id": 0,
        "lcn": 4,
        "mcount": 1,
        "fid": 1736,
        "cid": 3,
        "size": 61904100,
        "minVer": 4998,
        "maxVer": 6055
    },
    "sma": {
        "did.level": 0,
        "did.id": 0,
        "lcn": 0,
        "mcount": 1,
        "fid": 1736,
        "cid": 3,
        "size": 245978,
        "minVer": 4998,
        "maxVer": 6055
    },
    "stt lvl": [
        {
            "level": 0,
            "files": [
                {
                    "did.level": 0,
                    "did.id": 0,
                    "lcn": 0,
                    "mcount": 0,
                    "fid": 1736,
                    "cid": 15,
                    "size": 1296689,
                    "minVer": 6057,
                    "maxVer": 6078,
                    "level": 0
                }
            ]
        }
    ],
    "last compact": 0,
    "last commit": 1747209152547
}
*/

extern int32_t tsdbAsyncCompact(STsdb *tsdb, const STimeWindow *tw, bool s3Migrate);


// migrate monitor related functions
typedef struct SS3MigrateMonitor {
  TdThreadCond  stateChanged;
  SVnodeS3MigrateState state;
} SS3MigrateMonitor;


static int32_t getS3MigrateId(STsdb* tsdb) {
  return tsdb->pS3MigrateMonitor->state.vnodeMigrateId;
}


int32_t tsdbOpenS3MigrateMonitor(STsdb *tsdb) {
  SS3MigrateMonitor* pmm = (SS3MigrateMonitor*)taosMemCalloc(1, sizeof(SS3MigrateMonitor));
  if (pmm == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pmm->state.pFileSetStates = taosArrayInit(16, sizeof(SFileSetS3MigrateState));
  if (pmm->state.pFileSetStates == NULL) {
    taosMemoryFree(pmm);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  TAOS_UNUSED(taosThreadCondInit(&pmm->stateChanged, NULL));

  pmm->state.dnodeId = vnodeNodeId(tsdb->pVnode);
  pmm->state.vgId = TD_VID(tsdb->pVnode);

  tsdb->pS3MigrateMonitor = pmm;
  return 0;
}

#if 0
// TODO: remove this block of code
void tsdbCancelS3Migration(STsdb* tsdb) {
  SS3MigrateMonitor* pmm = tsdb->pS3MigrateMonitor;
  if (pmm == NULL) {
    return;
  }

  TAOS_UNUSED(taosThreadMutexLock(&tsdb->mutex));
  pmm->canceled = true;
  TAOS_UNUSED(taosThreadCondBroadcast(&pmm->stateChanged));
  TAOS_UNUSED(taosThreadMutexUnlock(&tsdb->mutex));

  while(1) {
    bool finished = true;
    TAOS_UNUSED(taosThreadMutexLock(&tsdb->mutex));
    for(int32_t i = 0; i < taosArrayGetSize(pmm->state.pFileSetStates); i++) {
      SFileSetS3MigrateState *pState = taosArrayGet(pmm->state.pFileSetStates, i);
      if (pState->state == FILE_SET_MIGRATE_STATE_IN_PROGRESS) {
        finished = false;
        break;
      }
    }
    TAOS_UNUSED(taosThreadMutexUnlock(&tsdb->mutex));
    if (finished) {
      break;
    }
    taosMsleep(100);
  }
}
  #endif

void tsdbCloseS3MigrateMonitor(STsdb *tsdb) {
  SS3MigrateMonitor* pmm = tsdb->pS3MigrateMonitor;
  if (pmm == NULL) {
    return;
  }

  TAOS_UNUSED(taosThreadCondDestroy(&pmm->stateChanged));
  tFreeSVnodeS3MigrateState(&pmm->state);
  taosMemoryFree(tsdb->pS3MigrateMonitor);
  tsdb->pS3MigrateMonitor = NULL;
}

void tsdbStartS3MigrateMonitor(STsdb *tsdb, int32_t s3MigrateId) {
  SS3MigrateMonitor* pmm = tsdb->pS3MigrateMonitor;
  pmm->state.mnodeMigrateId = 0;
  pmm->state.vnodeMigrateId = s3MigrateId;
  pmm->state.startTimeSec = taosGetTimestampSec();
  taosArrayClear(pmm->state.pFileSetStates);
}

void tsdbS3MigrateMonitorAddFileSet(STsdb *tsdb, int32_t fid) {
  // no need to lock mutex here, since the caller should have already locked it
  // TAOS_UNUSED(taosThreadMutexLock(&tsdb->mutex));
  SFileSetS3MigrateState state = { .fid = fid, .state = FILE_SET_MIGRATE_STATE_IN_PROGRESS };
  taosArrayPush(tsdb->pS3MigrateMonitor->state.pFileSetStates, &state);
  // TAOS_UNUSED(taosThreadMutexUnlock(&tsdb->mutex));
}

void tsdbS3MigrateMonitorSetFileSetState(STsdb *tsdb, int32_t fid, int32_t state) {
  SS3MigrateMonitor* pmm = tsdb->pS3MigrateMonitor;

  TAOS_UNUSED(taosThreadMutexLock(&tsdb->mutex));

  for(int32_t i = 0; i < taosArrayGetSize(pmm->state.pFileSetStates); i++) {
    SFileSetS3MigrateState *pState = taosArrayGet(pmm->state.pFileSetStates, i);
    if (pState->fid == fid) {
      pState->state = state;
      break;
    }
  }

  TAOS_UNUSED(taosThreadMutexUnlock(&tsdb->mutex));
}

int32_t tsdbQueryS3MigrateProgress(STsdb *tsdb, int32_t s3MigrateId, int32_t *rspSize, void** ppRsp) {
  SS3MigrateMonitor* pmm = tsdb->pS3MigrateMonitor;
  SVnodeS3MigrateState *pState = &pmm->state;

  TAOS_UNUSED(taosThreadMutexLock(&tsdb->mutex));
  pState->mnodeMigrateId = s3MigrateId;
  *rspSize = tSerializeSQueryS3MigrateProgressRsp(NULL, 0, pState);
  if (*rspSize < 0) {
    TAOS_UNUSED(taosThreadMutexUnlock(&tsdb->mutex));
    return TSDB_CODE_INVALID_MSG;
  }

  *ppRsp = rpcMallocCont(*rspSize);
  if (*ppRsp == NULL) {
    TAOS_UNUSED(taosThreadMutexUnlock(&tsdb->mutex));
    vError("rpcMallocCont %d failed", *rspSize);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  tSerializeSQueryS3MigrateProgressRsp(*ppRsp, *rspSize, pState);
  TAOS_UNUSED(taosThreadMutexUnlock(&tsdb->mutex));

  return 0;
}

int32_t tsdbUpdateS3MigrateState(STsdb* tsdb, SVnodeS3MigrateState* pState) {
  int32_t vid = TD_VID(tsdb->pVnode);

  // the state was generated by this vnode, so no need to process it
  if (pState->dnodeId == vnodeNodeId(tsdb->pVnode)) {
      tsdbDebug(
          "vgId:%d, skip migration state update since it was generated by this vnode", vid);
      return 0;
  }

  SS3MigrateMonitor* pmm = tsdb->pS3MigrateMonitor;
  SVnodeS3MigrateState *pLocalState = &pmm->state;
  if (pLocalState == NULL) {
    tsdbDebug("vgId:%d, skip migration state update since local state not found", vid);
    return 0;
  }

  TAOS_UNUSED(taosThreadMutexLock(&tsdb->mutex));

  bool updated = false;

  for( int32_t i = 0; i < taosArrayGetSize(pLocalState->pFileSetStates); i++) {
    SFileSetS3MigrateState *pLocalFileSet = taosArrayGet(pLocalState->pFileSetStates, i);
    if (pLocalFileSet->state != FILE_SET_MIGRATE_STATE_IN_PROGRESS) {
      continue; // only update the in-progress file sets
    }

    // a wrong case
    if (pState->mnodeMigrateId != pLocalState->vnodeMigrateId) {
      pLocalFileSet->state = FILE_SET_MIGRATE_STATE_FAILED;
      updated = true;
      tsdbError("vgId:%d, fid:%d, set migration state to failed since mnode migrate id mismatch", vid, pLocalFileSet->fid);
      continue;
    }

    // another wrong case
    if (pState->vnodeMigrateId != pLocalState->vnodeMigrateId) {
      pLocalFileSet->state = FILE_SET_MIGRATE_STATE_FAILED;
      updated = true;
      tsdbError("vgId:%d, fid:%d, set migration state to failed since vnode migrate id mismatch", vid, pLocalFileSet->fid);
      continue;
    }

    bool found = false;
    for( int32_t j = 0; j < taosArrayGetSize(pState->pFileSetStates); j++) {
      SFileSetS3MigrateState *pRemoteFileSet = taosArrayGet(pState->pFileSetStates, j);
      if (pLocalFileSet->fid == pRemoteFileSet->fid) {
        found = true;
        if (pRemoteFileSet->state != FILE_SET_MIGRATE_STATE_IN_PROGRESS) {
          pLocalFileSet->state = pRemoteFileSet->state;
          updated = true;
          tsdbDebug("vgId:%d, fid:%d, migration state was updated to %d", vid, pLocalFileSet->fid, pLocalFileSet->state);
        }
        break;
      }
    }

    // the leader vnode has not this file set, so it will never be migrated, mark it as failed
    // to avoid waiting for forever.
    if (!found) {
      pLocalFileSet->state = FILE_SET_MIGRATE_STATE_FAILED;
      updated = true;
      tsdbDebug("vgId:%d, fid:%d, set migration state to failed since remote state not found", vid, pLocalFileSet->fid);
    }
  }

  if (updated) {
    TAOS_UNUSED(taosThreadCondBroadcast(&pmm->stateChanged));
  }
  TAOS_UNUSED(taosThreadMutexUnlock(&tsdb->mutex));
  return 0;
}

// migrate file related functions
int32_t tsdbSsFidLevel(int32_t fid, STsdbKeepCfg *pKeepCfg, int32_t s3KeepLocal, int64_t nowSec) {
  int32_t localFid;
  TSKEY   key;

  if (pKeepCfg->precision == TSDB_TIME_PRECISION_MILLI) {
    nowSec = nowSec * 1000;
  } else if (pKeepCfg->precision == TSDB_TIME_PRECISION_MICRO) {
    nowSec = nowSec * 1000000l;
  } else if (pKeepCfg->precision == TSDB_TIME_PRECISION_NANO) {
    nowSec = nowSec * 1000000000l;
  }

  nowSec = nowSec - pKeepCfg->keepTimeOffset * tsTickPerHour[pKeepCfg->precision];

  key = nowSec - s3KeepLocal * tsTickPerMin[pKeepCfg->precision];
  localFid = tsdbKeyFid(key, pKeepCfg->days, pKeepCfg->precision);

  return fid >= localFid ? 0 : 1;
}


static int32_t downloadManifest(SVnode* pVnode, int32_t fid, STFileSet** ppFileSet) {
  int32_t code = 0, vid = TD_VID(pVnode);

  char path[64];
  snprintf(path, sizeof(path), "vnode%d/f%d/manifests.json", vid, fid);
  int64_t size = 0;
  code = tssGetFileSizeOfDefault(path, &size);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, fid:%d, failed to get manifests size since %s", vid, fid, tstrerror(code));
    return code;
  }

  char* buf = taosMemoryMalloc(size + 1);
  code = tssReadFileFromDefault(path, 0, buf, &size);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, fid:%d, failed to read manifest from shared storage since %s", vid, fid, tstrerror(code));
    taosMemoryFree(buf);
    return code;
  }
  buf[size] = 0;

  cJSON* json = cJSON_Parse(buf);
  taosMemoryFree(buf);
  if (json == NULL) {
    tsdbError("vgId:%d, fid:%d, failed to parse manifest json since %s", vid, fid, tstrerror(code));
    return TSDB_CODE_FILE_CORRUPTED;
  }
  
  code = tsdbJsonToTFileSet(pVnode->pTsdb, json, ppFileSet);
  cJSON_Delete(json);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, fid:%d, failed to parse manifest since %s", vid, fid, tstrerror(code));
    return code;
  }

  STFileSet* fset = *ppFileSet;
  if (fset->fid != fid) {
    tsdbError("vgId:%d, fid:%d, mismatch fid, manifest fid is %d", vid, fid, fset->fid);
    tsdbTFileSetClear(ppFileSet);
    return TSDB_CODE_FILE_CORRUPTED;
  }
  if (fset->farr[TSDB_FTYPE_DATA] == NULL) {
    tsdbError("vgId:%d, fid:%d, data file not found in manifest", vid, fid);
    tsdbTFileSetClear(ppFileSet);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  return code;
}


static int32_t uploadManifest(int32_t dnode, int32_t vnode, STFileSet* fset, int32_t mid) {
  int32_t code = 0;

  cJSON* json = cJSON_CreateObject();
  if (json == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  // update migration id for all files in the file set
  STFileObj* fobj = fset->farr[TSDB_FTYPE_HEAD];
  fobj->f->mid = mid;
  fobj = fset->farr[TSDB_FTYPE_SMA];
  fobj->f->mid = mid;
  fobj = fset->farr[TSDB_FTYPE_DATA];
  fobj->f->mid = mid;
  fobj = fset->farr[TSDB_FTYPE_TOMB];
  if (fobj != NULL) {
    fobj->f->mid = mid;
  }

  SSttLvl* lvl;
  TARRAY2_FOREACH(fset->lvlArr, lvl) {
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      fobj->f->mid = mid;
    }
  }
  
  cJSON_AddNumberToObject(json, "fmtv", 1);
  cJSON_AddNumberToObject(json, "dnode", dnode);
  cJSON_AddNumberToObject(json, "vnode", vnode);

  code = tsdbTFileSetToJson(fset, json);
  if (code != TSDB_CODE_SUCCESS) {
    cJSON_Delete(json);
    return code;
  }
  char* buf = cJSON_PrintUnformatted(json);
  cJSON_Delete(json);

  char path[64];
  snprintf(path, sizeof(path), "vnode%d/f%d/manifests.json", vnode, fset->fid);
  code = tssUploadToDefault(path, buf, strlen(buf));
  taosMemoryFree(buf);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, fid:%d, failed to upload manifest since %s", vnode, fset->fid, tstrerror(code));
    return code;
  }

  return code;
}



static int32_t uploadFile(SRTNer* rtner, STFileObj* fobj) {
  if (fobj == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  const char* ext = strchr(strrchr(fobj->fname, TD_DIRSEP_CHAR), '.');
  int32_t vid = TD_VID(rtner->tsdb->pVnode), mid = getS3MigrateId(rtner->tsdb);
  STFile* f = fobj->f;
  
  char path[TSDB_FILENAME_LEN];
  if (f->type == TSDB_FTYPE_SMA) {
    snprintf(path, sizeof(path), "vnode%d/f%d/v%df%dver%" PRId64 "m%d%s.%d", vid, f->fid, vid, f->fid, f->cid, mid, ext, ++f->mcount);
    STFileOp op = (STFileOp){.optype = TSDB_FOP_MODIFY, .fid = f->fid, .of = *f, .nf = *f};
    op.of.mcount--;
    TARRAY2_APPEND(&rtner->fopArr, op);
  } else {
    snprintf(path, sizeof(path), "vnode%d/f%d/v%df%dver%" PRId64 "m%d%s", vid, f->fid, vid, f->fid, f->cid, mid, ext);
  }

  int code = tssUploadFileToDefault(path, fobj->fname, 0, -1);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, fid:%d, failed to upload file %s since %s", vid, f->fid, fobj->fname, tstrerror(code));
    return code;
  }

  return 0;
}



static int32_t downloadFile(SRTNer* rtner, STFileObj* fobj) {
  if (fobj == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  const char* ext = strchr(strrchr(fobj->fname, TD_DIRSEP_CHAR), '.');
  int32_t vid = TD_VID(rtner->tsdb->pVnode);
  STFile* f = fobj->f;
  
  char path[TSDB_FILENAME_LEN];
  if (f->type == TSDB_FTYPE_SMA) {
    snprintf(path, sizeof(path), "vnode%d/f%d/v%df%dver%" PRId64 "m%d%s.%d", vid, f->fid, vid, f->fid, f->cid, f->mid, ext, f->mcount);
  } else {
    snprintf(path, sizeof(path), "vnode%d/f%d/v%df%dver%" PRId64 "m%d%s", vid, f->fid, vid, f->fid, f->cid, f->mid, ext);
  }

  int code = tssDownloadFileFromDefault(path, fobj->fname, 0, -1);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, fid:%d, failed to download file %s since %s", vid, f->fid, path, tstrerror(code));
    return code;
  }

  return 0;
}



static int32_t downloadDataFileLastChunk(SRTNer* rtner, STFileObj* fobj) {
  int32_t code = 0;
  int32_t vid = TD_VID(rtner->tsdb->pVnode);
  SVnodeCfg *pCfg = &rtner->tsdb->pVnode->config;
  STFile *f = fobj->f;

  char localPath[TSDB_FILENAME_LEN], path[TSDB_FILENAME_LEN];
  tsdbTFileLastChunkName(rtner->tsdb, f, localPath);
  char* fname = strrchr(localPath, TD_DIRSEP_CHAR) + 1;
  sprintf(fname, "v%df%dver%" PRId64 "m%d.%d.data", vid, f->fid, f->cid, f->mid, f->lcn);

  sprintf(path, "vnode%d/f%d/v%df%dver%" PRId64 ".%d.data.%d", vid, f->fid, vid, f->fid, f->cid, f->lcn, f->mcount);

  code = tssDownloadFileFromDefault(path, localPath, 0, -1);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, fid:%d, failed to download data file %s since %s", vid, f->fid, path, tstrerror(code));
    return code;
  }

  return code;
}



static int32_t uploadDataFile(SRTNer* rtner, STFileObj* fobj) {
  int32_t code = 0;
  int32_t vid = TD_VID(rtner->tsdb->pVnode);
  SVnodeCfg *pCfg = &rtner->tsdb->pVnode->config;
  int64_t szFile = 0, szChunk = (int64_t)pCfg->tsdbPageSize * pCfg->s3ChunkSize;
  STFile *f = fobj->f;
  STFileOp op = {.optype = TSDB_FOP_MODIFY, .fid = f->fid, .of = *f};

  char path[TSDB_FILENAME_LEN];
  if (f->lcn <= 1) {
    strcpy(path, fobj->fname);
  } else {
    tsdbTFileLastChunkName(rtner->tsdb, f, path);
  }

  code = taosStatFile(path, &szFile, NULL, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, fid:%d failed to stat file %s since %s", vid, f->fid, path, tstrerror(code));
    return false;
  }

  if (f->lcn > 1) {
    szFile += szChunk * (f->lcn - 1); // add the size of migrated chunks
  }

  int totalChunks = szFile / szChunk;
  if (szFile % szChunk) {
    totalChunks++;
  }

  int lcn = f->lcn < 1 ? 1 : f->lcn;
  if (totalChunks > lcn) { // reset migration counter if there are new chunks
    f->mcount = 0;
  }

  // upload chunks one by one, the first chunk may already been uploaded, but may be
  // modified thereafter, so we need to upload it again
  for (int i = lcn; i <= totalChunks; ++i) {
    int64_t offset = (int64_t)(i - lcn) * szChunk;
    int64_t size = szChunk;
    if (i == totalChunks && szFile % szChunk) {
        size = szFile % szChunk;
    }

    // only include the migration counter in the last chunk filename
    char rpath[TSDB_FILENAME_LEN];
    if (i == totalChunks) {
      sprintf(rpath, "vnode%d/f%d/v%df%dver%" PRId64 ".%d.data.%d", vid, f->fid, vid, f->fid, f->cid, i, ++f->mcount);
    } else {
      sprintf(rpath, "vnode%d/f%d/v%df%dver%" PRId64 ".%d.data", vid, f->fid, vid, f->fid, f->cid, i);
    }

    code = tssUploadFileToDefault(rpath, path, offset, size);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbError("vgId:%d, fid:%d, failed to migrate data file since %s", vid, f->fid, tstrerror(code));
      return code;
    }
  }

  f->lcn = totalChunks;
  op.nf = *f;
  TARRAY2_APPEND(&rtner->fopArr, op);

  // manifest must be uploaded before copy last chunk, otherwise, failed to upload manifest
  // will result in a broken migration
  tsdbInfo("vgId:%d, fid:%d, data file migrated, begin generate & upload manifest file", vid, f->fid);

  // manifest, this also commit the migration
  code = uploadManifest(vnodeNodeId(rtner->tsdb->pVnode), vid, rtner->fset, getS3MigrateId(rtner->tsdb));
  if (code != TSDB_CODE_SUCCESS) {
    tsdbS3MigrateMonitorSetFileSetState(rtner->tsdb, f->fid, FILE_SET_MIGRATE_STATE_FAILED);
    return code;
  }

  tsdbS3MigrateMonitorSetFileSetState(rtner->tsdb, f->fid, FILE_SET_MIGRATE_STATE_SUCCEEDED);
  tsdbInfo("vgId:%d, fid:%d, manifest file uploaded, leader migration succeeded", vid, f->fid);

  // no new chunks generated, no need to copy the last chunk
  if (totalChunks == lcn) {
    return 0;
  }

  // copy the last chunk to the new file
  char newPath[TSDB_FILENAME_LEN];
  tsdbTFileLastChunkName(rtner->tsdb, &op.nf, newPath);

  int64_t offset = (int64_t)(totalChunks - lcn) * szChunk;
  int64_t size = szChunk;
  if (szFile % szChunk) {
    size = szFile % szChunk;
  }

  TdFilePtr fdFrom = taosOpenFile(path, TD_FILE_READ);
  if (fdFrom == NULL) {
    code = terrno;
    tsdbError("vgId:%d, fid:%d, failed to open source file %s since %s", vid, f->fid, path, tstrerror(code));
    return code;
  }

  TdFilePtr fdTo = taosOpenFile(newPath, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (fdTo == NULL) {
    code = terrno;
    tsdbError("vgId:%d, fid:%d, failed to open target file %s since %s", vid, f->fid, newPath, tstrerror(code));
    taosCloseFile(&fdFrom);
    return code;
  }

  int64_t n = taosFSendFile(fdTo, fdFrom, &offset, size);
  if (n < 0) {
    code = terrno;
    tsdbError("vgId:%d, fid:%d, failed to copy file %s to %s since %s", vid, f->fid, path, newPath, tstrerror(code));
  }
  taosCloseFile(&fdFrom);
  taosCloseFile(&fdTo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return 0;
}


static bool shouldMigrate(SRTNer *rtner, int32_t *pCode) {
  int32_t vid = TD_VID(rtner->tsdb->pVnode);
  SVnodeCfg *pCfg = &rtner->tsdb->pVnode->config;
  STFileSet *pLocalFset = rtner->fset;
  STFileObj *flocal = pLocalFset->farr[TSDB_FTYPE_DATA];

  *pCode = 0;
  if (!flocal) {
    return false;
  }

  if (pCfg->s3Compact && flocal->f->lcn < 0) {
    int32_t lcn = flocal->f->lcn;
    STimeWindow win = {0};
    tsdbFidKeyRange(pLocalFset->fid, rtner->tsdb->keepCfg.days, rtner->tsdb->keepCfg.precision, &win.skey, &win.ekey);
    *pCode = tsdbAsyncCompact(rtner->tsdb, &win, true);
    tsdbInfo("vgId:%d, fid:%d, migration cancelled, fileset need compact, lcn: %d", vid, pLocalFset->fid, lcn);
    return false; // compact in progress
  }

  char path[TSDB_FILENAME_LEN];
  if (flocal->f->lcn <= 1) {
    strcpy(path, flocal->fname);
  } else {
    tsdbTFileLastChunkName(rtner->tsdb, flocal->f, path);
  }

  int64_t mtime = 0, size = 0;
  *pCode = taosStatFile(path, &size, &mtime, NULL);
  if (*pCode != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, fid:%d, failed to stat file %s since %s", vid, pLocalFset->fid, path, tstrerror(*pCode));
    return false;
  }

  if (size <= (int64_t)pCfg->tsdbPageSize * pCfg->s3ChunkSize) {
    tsdbInfo("vgId:%d, fid:%d, migration skipped, data file is too small, size: %" PRId64 " bytes", vid, pLocalFset->fid, size);
    return false; // file too small, no need to migrate
  }

  if (mtime >= rtner->now - tsS3UploadDelaySec) {
    tsdbInfo("vgId:%d, fid:%d, migration skipped, data file is active writting, modified at %" PRId64, vid, pLocalFset->fid, mtime);
    return false; // still active writing, postpone migration
  }

  if (tsdbFidLevel(pLocalFset->fid, &rtner->tsdb->keepCfg, rtner->now) < 0) {
    tsdbInfo("vgId:%d, fid:%d, migration skipped, file set is expired", vid, pLocalFset->fid);
    return false; // file set expired
  }

  if (tsdbSsFidLevel(pLocalFset->fid, &rtner->tsdb->keepCfg, pCfg->s3KeepLocal, rtner->now) < 1) {
    tsdbInfo("vgId:%d, fid:%d, migration skipped, keep local file set", vid, pLocalFset->fid);
    return false; // keep on local storage
  }

  // this could be 2 possibilities:
  // 1. this is the first migration, we should do it;
  // 2. there's a compact after the last migration, we should discard the migrated files;
  if (flocal->f->lcn < 1) {
    tsdbInfo("vgId:%d, fid:%d, file set will be migrated", vid, pLocalFset->fid);
    return true;
  }

  // download manifest from shared storage
  STFileSet *pRemoteFset = NULL;
  *pCode = downloadManifest(rtner->tsdb->pVnode, pLocalFset->fid, &pRemoteFset);
  if (*pCode == TSDB_CODE_NOT_FOUND) {
    tsdbError("vgId:%d, fid:%d, remote manifest not found, but local manifest is initialized", vid, pLocalFset->fid);
    tsdbTFileSetClear(&pRemoteFset);
    *pCode = TSDB_CODE_FILE_CORRUPTED;
    return false;
  }

  if (*pCode != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, fid:%d, migration cancelled, failed to download manifest, code: %d", vid, pLocalFset->fid, *pCode);
    return false;
  }
  
  STFileObj *fremote = pRemoteFset->farr[TSDB_FTYPE_DATA];
  if (fremote == NULL) {
    tsdbError("vgId:%d, fid:%d, migration cancelled, cannot find data file information from remote manifest", vid, pLocalFset->fid);
    tsdbTFileSetClear(&pRemoteFset);
    *pCode = TSDB_CODE_FILE_CORRUPTED;
    return false;
  }

  if (fremote->f->lcn != flocal->f->lcn || fremote->f->mcount != flocal->f->mcount) {
    tsdbError("vgId:%d, fid:%d, migration cancelled, remote and local data file information mismatch", vid, pLocalFset->fid);
    tsdbTFileSetClear(&pRemoteFset);
    *pCode = TSDB_CODE_FILE_CORRUPTED;
    return false;
  }
  
  if (fremote->f->maxVer == flocal->f->maxVer) {
    tsdbTFileSetClear(&pRemoteFset);
    tsdbError("vgId:%d, fid:%d, migration skipped, no new data", vid, pLocalFset->fid);
    return false; // no new data
  }

  tsdbTFileSetClear(&pRemoteFset); // we use the local file set information for migration
  tsdbInfo("vgId:%d, fid:%d, file set will be migrated", vid, pLocalFset->fid);
  return true;
}


static int32_t tsdbFollowerDoSsMigrate(SRTNer *rtner) {
  int32_t code = 0, vid = TD_VID(rtner->tsdb->pVnode);
  STFileSet *fset = rtner->fset;
  SS3MigrateMonitor* pmm = rtner->tsdb->pS3MigrateMonitor;
  SFileSetS3MigrateState *pState = NULL;
  int32_t fsIdx = 0;

  tsdbInfo("vgId:%d, fid:%d, vnode is follower, waiting leader migration to be finished", vid, fset->fid);

  TAOS_UNUSED(taosThreadMutexLock(&rtner->tsdb->mutex));
  for (; fsIdx < taosArrayGetSize(pmm->state.pFileSetStates); fsIdx++) {
    pState = taosArrayGet(pmm->state.pFileSetStates, fsIdx);
    if (pState->fid == fset->fid) {
      break;
    }
  }

  while(pState->state == FILE_SET_MIGRATE_STATE_IN_PROGRESS) {
    TAOS_UNUSED(taosThreadCondWait(&pmm->stateChanged, &rtner->tsdb->mutex));
    pState = taosArrayGet(pmm->state.pFileSetStates, fsIdx);
  }

  TAOS_UNUSED(taosThreadMutexUnlock(&rtner->tsdb->mutex));

  if (pState->state != FILE_SET_MIGRATE_STATE_SUCCEEDED) {
    tsdbInfo("vgId:%d, fid:%d, follower migration skipped because leader migration skipped or failed", vid, fset->fid);
    return TSDB_CODE_FAILED;
  }

  tsdbInfo("vgId:%d, fid:%d, follower migration started, begin downloading manifest...", vid, fset->fid);
  STFileSet *pRemoteFset = NULL;
  code = downloadManifest(rtner->tsdb->pVnode, fset->fid, &pRemoteFset);
  if (code == TSDB_CODE_NOT_FOUND) {
    tsdbTFileSetClear(&pRemoteFset);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  tsdbInfo("vgId:%d, fid:%d, manifest downloaded, begin downloading head file", vid, fset->fid);
  code = downloadFile(rtner, pRemoteFset->farr[TSDB_FTYPE_HEAD]);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTFileSetClear(&pRemoteFset);
    return code;
  }
  STFileOp op = {.optype = TSDB_FOP_MODIFY, .fid = fset->fid, .of = *fset->farr[TSDB_FTYPE_HEAD]->f, .nf = *pRemoteFset->farr[TSDB_FTYPE_HEAD]->f};
  TARRAY2_APPEND(&rtner->fopArr, op);

  tsdbInfo("vgId:%d, fid:%d, head file downloaded, begin downloading sma file", vid, fset->fid);
  code = downloadFile(rtner, pRemoteFset->farr[TSDB_FTYPE_SMA]);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTFileSetClear(&pRemoteFset);
    return code;
  }
  op = (STFileOp) {.optype = TSDB_FOP_MODIFY, .fid = fset->fid, .of = *fset->farr[TSDB_FTYPE_SMA]->f, .nf = *pRemoteFset->farr[TSDB_FTYPE_SMA]->f};
  TARRAY2_APPEND(&rtner->fopArr, op);

  tsdbInfo("vgId:%d, fid:%d, sma file downloaded, begin downloading tomb file", vid, fset->fid);
  code = downloadFile(rtner, pRemoteFset->farr[TSDB_FTYPE_TOMB]);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTFileSetClear(&pRemoteFset);
    return code;
  }
  if (fset->farr[TSDB_FTYPE_TOMB] != NULL && pRemoteFset->farr[TSDB_FTYPE_TOMB] != NULL) {
    op = (STFileOp) {.optype = TSDB_FOP_MODIFY, .fid = fset->fid, .of = *fset->farr[TSDB_FTYPE_TOMB]->f, .nf = *pRemoteFset->farr[TSDB_FTYPE_TOMB]->f};
    TARRAY2_APPEND(&rtner->fopArr, op);
  } else if (fset->farr[TSDB_FTYPE_TOMB] != NULL) {
    // the remote tomb file is not found, but local tomb file exists, we should remove it
    op = (STFileOp) {.optype = TSDB_FOP_REMOVE, .fid = fset->fid, .of = *fset->farr[TSDB_FTYPE_TOMB]->f};
    TARRAY2_APPEND(&rtner->fopArr, op);
  } else if (pRemoteFset->farr[TSDB_FTYPE_TOMB] != NULL) {
    op = (STFileOp) {.optype = TSDB_FOP_CREATE, .fid = fset->fid, .nf = *pRemoteFset->farr[TSDB_FTYPE_TOMB]->f};
    TARRAY2_APPEND(&rtner->fopArr, op);
  }

  tsdbInfo("vgId:%d, fid:%d, tomb file downloaded, begin downloading stt files", vid, fset->fid);
  SSttLvl* lvl;
  TARRAY2_FOREACH(fset->lvlArr, lvl) {
    STFileObj* fobj;
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      op = (STFileOp) {.optype = TSDB_FOP_REMOVE, .fid = fset->fid, .of = *fobj->f};
      TARRAY2_APPEND(&rtner->fopArr, op);
    }
  }
  TARRAY2_FOREACH(pRemoteFset->lvlArr, lvl) {
    STFileObj* fobj;
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      code = downloadFile(rtner, fobj);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbTFileSetClear(&pRemoteFset);
        return code;
      }
      op = (STFileOp) {.optype = TSDB_FOP_CREATE, .fid = fset->fid, .nf = *fobj->f};
      TARRAY2_APPEND(&rtner->fopArr, op);
    }
  }

  tsdbInfo("vgId:%d, fid:%d, stt files downloaded, begin downloading data file", vid, fset->fid);
  code = downloadDataFileLastChunk(rtner, pRemoteFset->farr[TSDB_FTYPE_DATA]);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbTFileSetClear(&pRemoteFset);
    return code;
  }
  op = (STFileOp) {.optype = TSDB_FOP_MODIFY, .fid = fset->fid, .of = *fset->farr[TSDB_FTYPE_DATA]->f, .nf = *pRemoteFset->farr[TSDB_FTYPE_DATA]->f};
  TARRAY2_APPEND(&rtner->fopArr, op);

  tsdbInfo("vgId:%d, fid:%d, data file downloaded", vid, fset->fid);
  tsdbTFileSetClear(&pRemoteFset);
  return 0;
}


static int32_t tsdbLeaderDoSsMigrate(SRTNer *rtner) {
  int32_t code = 0, vid = TD_VID(rtner->tsdb->pVnode);
  SVnodeCfg *pCfg = &rtner->tsdb->pVnode->config;
  STFileSet *fset = rtner->fset;

  tsdbInfo("vgId:%d, fid:%d, vnode is leader, migration started", vid, fset->fid);

  if (!shouldMigrate(rtner, &code)) {
    int32_t state = (code == TSDB_CODE_SUCCESS) ? FILE_SET_MIGRATE_STATE_SKIPPED : FILE_SET_MIGRATE_STATE_FAILED;
    tsdbS3MigrateMonitorSetFileSetState(rtner->tsdb, fset->fid, state);
    return code;
  }

  // head file
  tsdbInfo("vgId:%d, fid:%d, begin migrate head file", vid, fset->fid);
  code = uploadFile(rtner, fset->farr[TSDB_FTYPE_HEAD]);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbS3MigrateMonitorSetFileSetState(rtner->tsdb, fset->fid, FILE_SET_MIGRATE_STATE_FAILED);
    return code;
  }

  tsdbInfo("vgId:%d, fid:%d, head file migrated, begin migrate sma file", vid, fset->fid);

  // sma file
  code = uploadFile(rtner, fset->farr[TSDB_FTYPE_SMA]);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbS3MigrateMonitorSetFileSetState(rtner->tsdb, fset->fid, FILE_SET_MIGRATE_STATE_FAILED);
    return code;
  }

  tsdbInfo("vgId:%d, fid:%d, sma file migrated, begin migrate tomb file", vid, fset->fid);

  // tomb file
  code = uploadFile(rtner, fset->farr[TSDB_FTYPE_TOMB]);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbS3MigrateMonitorSetFileSetState(rtner->tsdb, fset->fid, FILE_SET_MIGRATE_STATE_FAILED);
    return code;
  }

  tsdbInfo("vgId:%d, fid:%d, tomb file migrated, begin migrate stt files", vid, fset->fid);

  // stt files
  SSttLvl* lvl;
  TARRAY2_FOREACH(fset->lvlArr, lvl) {
    STFileObj* fobj;
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      code = uploadFile(rtner, fobj);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbS3MigrateMonitorSetFileSetState(rtner->tsdb, fset->fid, FILE_SET_MIGRATE_STATE_FAILED);
        return code;
      }
    }
  }
  
  tsdbInfo("vgId:%d, fid:%d, stt files migrated, begin migrate data file", vid, fset->fid);

  // data file
  code = uploadDataFile(rtner, fset->farr[TSDB_FTYPE_DATA]);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbS3MigrateMonitorSetFileSetState(rtner->tsdb, fset->fid, FILE_SET_MIGRATE_STATE_FAILED);
    return code;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t tsdbDoSsMigrate(SRTNer *rtner) {
  if (rtner->nodeId == vnodeNodeId(rtner->tsdb->pVnode)) {
    return tsdbLeaderDoSsMigrate(rtner);
  }
  return tsdbFollowerDoSsMigrate(rtner);
}
