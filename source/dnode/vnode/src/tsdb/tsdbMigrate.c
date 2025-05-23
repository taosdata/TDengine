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
  int32_t code = 0;

  char path[64];
  snprintf(path, sizeof(path), "vnode%d/f%d/manifests.json", TD_VID(pVnode), fid);
  int64_t size = 0;
  code = tssGetFileSizeOfDefault(path, &size);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, failed to get manifests size since %s", TD_VID(pVnode), tstrerror(code));
    return code;
  }

  char* buf = taosMemoryMalloc(size + 1);
  code = tssReadFileFromDefault(path, 0, buf, &size);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, failed to read manifest from shared storage since %s", TD_VID(pVnode), tstrerror(code));
    taosMemoryFree(buf);
    return code;
  }
  buf[size] = 0;

  cJSON* json = cJSON_Parse(buf);
  taosMemoryFree(buf);
  if (json == NULL) {
    tsdbError("vgId:%d, failed to parse manifest json since %s", TD_VID(pVnode), tstrerror(code));
    return TSDB_CODE_FILE_CORRUPTED;
  }
  
  code = tsdbJsonToTFileSet(pVnode->pTsdb, json, ppFileSet);
  cJSON_Delete(json);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, failed to parse manifest since %s", TD_VID(pVnode), tstrerror(code));
    return code;
  }

  STFileSet* fset = *ppFileSet;
  if (fset->fid != fid) {
    tsdbError("vgId:%d, fid in manifest %d not match with %d", TD_VID(pVnode), fset->fid, fid);
    tsdbTFileSetClear(ppFileSet);
    return TSDB_CODE_FILE_CORRUPTED;
  }
  if (fset->farr[TSDB_FTYPE_DATA] == NULL) {
    tsdbError("vgId:%d, data file not found in manifest", TD_VID(pVnode));
    tsdbTFileSetClear(ppFileSet);
    return TSDB_CODE_FILE_CORRUPTED;
  }

  return code;
}


static int32_t uploadManifest(int32_t dnode, int32_t vnode, STFileSet* fset) {
  int32_t code = 0;

  cJSON* json = cJSON_CreateObject();
  if (json == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
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
    tsdbError("vgId:%d, failed to upload manifest since %s", vnode, tstrerror(code));
    return code;
  }

  return code;
}



static int32_t tsdbMigrateFile(SRTNer* rtner, int32_t vid, STFileObj* fobj) {
  if (fobj == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  const char* fname = strrchr(fobj->fname, TD_DIRSEP_CHAR) + 1;
  STFile* f = fobj->f;
  
  char path[TSDB_FILENAME_LEN];
  if (f->type == TSDB_FTYPE_SMA) {
    snprintf(path, sizeof(path), "vnode%d/f%d/%s.%d", vid, f->fid, fname, ++f->mcount);
    STFileOp op = (STFileOp){.optype = TSDB_FOP_MODIFY, .fid = f->fid, .of = *f, .nf = *f};
    op.of.mcount--;
    TARRAY2_APPEND(&rtner->fopArr, op);
  } else {
    snprintf(path, sizeof(path), "vnode%d/f%d/%s", vid, f->fid, fname);
  }

  int code = tssUploadFileToDefault(path, fobj->fname, 0, -1);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d, failed to migrate file %s since %s", vid, fname, tstrerror(code));
    return code;
  }

  return 0;
}


static int32_t migrateDataFile(SRTNer* rtner, STFileObj* fobj) {
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
    tsdbError("vgId:%d, failed to stat file %s since %s", vid, path, tstrerror(code));
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

    if (!vnodeIsLeader(rtner->tsdb->pVnode)) {
      tsdbError("vgId:%d, abort data migration since not the leader anymore", vid);
      return TSDB_CODE_FAILED;
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
      tsdbError("vgId:%d, failed to migrate data file since %s", vid, tstrerror(code));
      return code;
    }
  }

  f->lcn = totalChunks;
  op.nf = *f;
  TARRAY2_APPEND(&rtner->fopArr, op);

  // no new chunks generated, no need to copy the last chunk
  if (totalChunks == lcn) {
    return 0;
  }

  // copy the last chunk to the new file
  char newPath[TSDB_FILENAME_LEN];
  tsdbTFileLastChunkName(rtner->tsdb, f, newPath);

  int64_t offset = (int64_t)(totalChunks - lcn) * szChunk;
  int64_t size = szChunk;
  if (szFile % szChunk) {
    size = szFile % szChunk;
  }

  TdFilePtr fdFrom = taosOpenFile(path, TD_FILE_READ);
  if (fdFrom == NULL) {
    code = terrno;
    tsdbError("vgId:%d, failed to open file %s since %s", vid, path, tstrerror(code));
    return code;
  }

  TdFilePtr fdTo = taosOpenFile(newPath, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  if (fdTo == NULL) {
    code = terrno;
    tsdbError("vgId:%d, failed to open file %s since %s", vid, newPath, tstrerror(code));
    taosCloseFile(&fdFrom);
    return code;
  }

  int64_t n = taosFSendFile(fdTo, fdFrom, &offset, size);
  if (n < 0) {
    code = terrno;
    tsdbError("vgId:%d, failed to copy file %s to %s since %s", vid, path, newPath, tstrerror(code));
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
    tsdbInfo("vgId:%d, async compact begin lcn: %d.", vid, lcn);
    *pCode = tsdbAsyncCompact(rtner->tsdb, &win, true);
    tsdbInfo("vgId:%d, async compact end lcn: %d.", vid, lcn);
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
    tsdbError("vgId:%d, failed to stat file %s since %s", vid, path, tstrerror(*pCode));
    return false;
  }

  if (size <= (int64_t)pCfg->tsdbPageSize * pCfg->s3ChunkSize) {
    return false; // file too small, no need to migrate
  }

  if (mtime >= rtner->now - tsS3UploadDelaySec) {
    return false; // still active writing, postpone migration
  }

  if (tsdbFidLevel(pLocalFset->fid, &rtner->tsdb->keepCfg, rtner->now) < 0) {
    return false; // file set expired
  }

  if (tsdbSsFidLevel(pLocalFset->fid, &rtner->tsdb->keepCfg, pCfg->s3KeepLocal, rtner->now) < 1) {
    return false; // keep on local storage
  }

  // this could be 2 possibilities:
  // 1. this is the first migration, we should do it;
  // 2. there's a compact after the last migration, we should discard the migrated files;
  if (flocal->f->lcn < 1) {
    return true;
  }

  // download manifest from shared storage
  STFileSet *pRemoteFset = NULL;
  *pCode = downloadManifest(rtner->tsdb->pVnode, pLocalFset->fid, &pRemoteFset);
  if (*pCode == TSDB_CODE_NOT_FOUND) {
    tsdbError("vgId:%d, remote manifest not found, but local manifest is initialized", vid);
    tsdbTFileSetClear(&pRemoteFset);
    *pCode = TSDB_CODE_FILE_CORRUPTED;
    return false;
  }

  if (*pCode != TSDB_CODE_SUCCESS) {
    return false;
  }
  
  STFileObj *fremote = pRemoteFset->farr[TSDB_FTYPE_DATA];
  if (fremote == NULL) {
    tsdbError("vgId:%d, cannot find data file information from remote manifest", vid);
    tsdbTFileSetClear(&pRemoteFset);
    *pCode = TSDB_CODE_FILE_CORRUPTED;
    return false;
  }

  if (fremote->f->lcn != flocal->f->lcn || fremote->f->mcount != flocal->f->mcount) {
    tsdbError("vgId:%d, remote and local data file information mismatch", vid);
    tsdbTFileSetClear(&pRemoteFset);
    *pCode = TSDB_CODE_FILE_CORRUPTED;
    return false;
  }
  
  if (fremote->f->maxVer == flocal->f->maxVer) {
    tsdbTFileSetClear(&pRemoteFset);
    return false; // no new data
  }

  tsdbTFileSetClear(&pRemoteFset); // we use the local file set information for migration
  return true;
}



int32_t tsdbDoSsMigrate(SRTNer *rtner) {
  int32_t code = 0, vid = TD_VID(rtner->tsdb->pVnode);
  SVnodeCfg *pCfg = &rtner->tsdb->pVnode->config;
  STFileSet *fset = rtner->fset;
  
  if (!shouldMigrate(rtner, &code)) {
    return code;
  }
  
  if (!vnodeIsLeader(rtner->tsdb->pVnode)) {
    // TODO:
  }

  // head file
  code = tsdbMigrateFile(rtner, vid, fset->farr[TSDB_FTYPE_HEAD]);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // sma file
  code = tsdbMigrateFile(rtner, vid, fset->farr[TSDB_FTYPE_SMA]);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // tomb file
  code = tsdbMigrateFile(rtner, vid, fset->farr[TSDB_FTYPE_TOMB]);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // stt files
  SSttLvl* lvl;
  TARRAY2_FOREACH(fset->lvlArr, lvl) {
    STFileObj* fobj;
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      code = tsdbMigrateFile(rtner, vid, fobj);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }
  
  // data file
  code = migrateDataFile(rtner, fset->farr[TSDB_FTYPE_DATA]);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // manifest, this also commit the migration
  return uploadManifest(vnodeNodeId(rtner->tsdb->pVnode), vid, fset);
}
