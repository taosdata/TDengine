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

#include "dmRepair.h"
#include "tsdbDataFileRW.h"
#include "tsdbFS2.h"
#include "tsdbSttFileRW.h"

bool tsdbShouldForceRepair(STFileSystem *fs) {
  int32_t vgId = TD_VID(fs->tsdb->pVnode);

  if (!dmRepairFlowEnabled()) {
    return false;
  }

  if (!dmRepairNodeTypeIsVnode()) {
    return false;
  }

  if (!dmRepairModeIsForce()) {
    return false;
  }

  if (!dmRepairNeedTsdbRepair(vgId)) {
    return false;
  }

  return true;
}

static int32_t tsdbForceRepairFileSetBadFiles(STFileSystem *pFS, STFileSet *pFileSet, TFileOpArray *opArr) {
  int32_t code = TSDB_CODE_SUCCESS;

  // TODO: Handle .head .data .sma
  STFileObj *pHead = pFileSet->farr[TSDB_FTYPE_HEAD];
  STFileObj *pData = pFileSet->farr[TSDB_FTYPE_DATA];
  STFileObj *pSMA = pFileSet->farr[TSDB_FTYPE_SMA];
  STFileObj *pTomb = pFileSet->farr[TSDB_FTYPE_TOMB];
  if (pHead) {
    if (!taosCheckExistFile(pHead->fname)) {
      // TODO: append delete head op to opArr
      // TODO: append delete data op to opArr
      // TODO: append delete sma op to opArr
    }
  }

  // TODO: handle .tomb missing
  if (pTomb && !taosCheckExistFile(pTomb->fname)) {
    // TODO: append delete tomb op to opArr
  }

  // Loop to handle stt files
  SSttLvl *sttLevel = NULL;
  TARRAY2_FOREACH(pFileSet->lvlArr, sttLevel) {
    STFileObj *pStt;
    TARRAY2_FOREACH(sttLevel->fobjArr, pStt) {
      if (!taosCheckExistFile(pStt->fname)) {
        // TODO: append delete stt op to op arr
      }
    }
  }
  return code;
}

static int32_t tsdbDeepScanAndFixDataPart() {
  int32_t code = TSDB_CODE_SUCCESS;
  SDataFileReader     *reader = NULL;
  SDataFileWriter     *writer = NULL;
  SBrinBlock           brinBlock = {0};
  const TBrinBlkArray *oldBrinBlkArray = NULL;
  TBrinBlkArray        newBrinBlkArray = {0};

  // Open reader
  code = tsdbDataFileReaderOpen(const char **fname, const SDataFileReaderConfig *config,
                                SDataFileReader **reader) if (code) {
    // TODO
  }

  // Open writer
  code = tsdbDataFileWriterOpen();
  if (code) {
    // TODO
  }

  // Load SBlockBlk
  code = tsdbDataFileReadBrinBlk(reader, &oldBrinBlkArray);
  if (code) {
    // TODO: need to delete all data group
  }

  // Loop to scan each BrinBlock
  for (int32_t i = 0; i < oldBrinBlkArray->size; i++) {
    SBrinBlk *pBrinBlk = TARRAY2_GET_PTR(oldBrinBlkArray, i);

    // the SBrinBlk is bad, just skip this brinblock
    code = tsdbDataFileReadBrinBlock(reader, pBrinBlk, &brinBlock);
    if (code) {
      code = TSDB_CODE_SUCCESS;
      continue;
    }

    // Loop to scan each DataBlock
    for (int32_t iBrinRecord = 0; iBrinRecord < BRIN_BLOCK_SIZE(&brinBlock); iBrinRecord++) {
      // Try to load the Data Block
    }
  }

_exit:
  return code;
}

static int32_t tsdbDeepScanAndFixSttFile(STFileset *pFileSet, STFileObj *pStt) {
  int32_t             code = TSDB_CODE_SUCCESS;
  SSttFileReader     *reader;
  const TSttBlkArray *sttBlkArray = NULL;

  // Open
  SSttFileReaderConfig config = {
      // TODO

  };
  code = tsdbSttFileReaderOpen(pStt->fname, &config, &reader);
  if (code) {
    // TODO: error handle, need to delete this file
  }

  // read the index part
  code = tsdbSttFileReadSttBlk(reader, &sttBlkArray);
  if (code) {
    // TODO: error handle, need to delete this file
  }

  // Loop to read each data part
  for (int32_t i = 0; i < sttBlkArray->size; i++) {
    SSttBlk *pSttBlk = ;
    code = tsdbReadFile(STsdbFD * pFD, int64_t offset, uint8_t *pBuf, int64_t size, int64_t szHint,
                        SEncryptData *encryptData);
    if (code) {
      // TODO: find a bad block, need to eliminate it
    }
  };

  return code;
}

static int32_t tsdbDeepScanAndFixSttPart(STFileSet *pFileSet) {
  int32_t code = TSDB_CODE_SUCCESS;

  SSttLvl *sttLevel = NULL;
  TARRAY2_FOREACH(pFileSet->lvlArr, sttLevel) {
    STFileObj *pStt;
    TARRAY2_FOREACH(sttLevel->fobjArr, pStt) {
      code = tsdbDeepScanAndFixSttFile(pFileSet, pStt);
      if (code) {
        // TODO: tsdbError
        return code
      }
    }
  }

  //
  return code;
}

static int32_t tsdbForceRepairFileSetDeepScanAndFix(STFileSystem *pFS, STFileSet *pFileSet, TFileOpArray *opArr,
                                                    bool *hasChange) {
  int32_t code = TSDB_CODE_SUCCESS;

  code = tsdbDeepScanAndFixDataPart();
  if (code) {
    // TODO: tsdbError
    return code;
  }

  code = tsdbDeepScanAndFixSttPart();
  if (code) {
    // TODO: tsdbError
    return code;
  }

  // TODO
  return code;
}

static int32_t tsdbForceRepairFileSet(STFileSystem *pFS, STFileSet *pFileSet, TFileOpArray *opArr, bool *hasChange) {
  int32_t code = TSDB_CODE_SUCCESS;

  // TODO: if .head or .data is missing, just delete the data
  code = tsdbForceRepairFileSetBadFiles(pFS);
  if (code) {
    // TODO
    return code;
  }

  // TODO: if deep scan and fix the data, do deep scan and fix
  code = tsdbForceRepairFileSetDeepScanAndFix(pFS, pFileSet, opArr, &hasChange);
  if (code) {
    // TODO
    return code;
  }

  return code;
}

static int32_t tsdbForceRepairCommitChange(STFileSystem *pFS, const TFileOpArray *opArr) {
  int32_t code = TSDB_CODE_SUCCESS;
  STsdb  *pTsdb = pFS->tsdb;

  // Begin to commit the change
  code = tsdbFSEditBegin(pFS, opArr, /* TODO: EFEditT etype*/);
  if (code) {
    // TODO: output error message
    return code;
  }

  // Commit the change
  (void)taosThreadMutexLock(&pTsdb->mutex);
  code = tsdbFSEditCommit(pFS);
  if (code) {
    // TODO: output error message
    (void)taosThreadMutexUnlock(&pTsdb->mutex);
    return code;
  }
  (void)taosThreadMutexUnlock(&pTsdb->mutex);
  return code;
}

int32_t tsdbForceRepair(STFileSystem *fs) {
  int32_t code = TSDB_CODE_SUCCESS;

  bool         hasChange = false;
  TFileOpArray opArr = {0};

  // Loop to force repair each file set
  STFileSet *pFileSet = NULL;
  TARRAY2_FOREACH(fs->fSetArr, pFileSet) {
    code = tsdbForceRepairFileSet(fs, pFileSet, &hasChange);
    if (code) {
      tsdbError("vgId:%d %s failed to force repair file set, fid:%d since %s, code:%d", TD_VID(fs->tsdb->pVnode),
                __func__, pFileSet->fid, tstrerror(code), code);
      return code;
    }
  }

  code = tsdbForceRepairCommitChange(fs, &opArr);
  if (code) {
    // TODO: output error log
    return code;
  }

#if 0
  int32_t code = tsdbFSDupState(fs);
  if (code != 0) {
    return code;
  }

  bool             changed = false;
  const STFileSet *srcFset = NULL;
  TARRAY2_FOREACH(fs->fSetArr, srcFset) {
    EDmRepairStrategy repairStrategy = DM_REPAIR_STRATEGY_NONE;
    if (!tsdbRepairMatchTargetForFid(TD_VID(fs->tsdb->pVnode), srcFset->fid, &repairStrategy)) {
      continue;
    }
    TAOS_UNUSED(repairStrategy);

    STsdbRepairPlan plan;
    code = tsdbRepairAnalyzeFileSet(fs, srcFset, &plan);
    if (code != 0) {
      return code;
    }
    if (!plan.affected) {
      continue;
    }

    code = tsdbRepairBackupAffectedFileSet(fs, srcFset, &plan);
    if (code != 0) {
      return code;
    }

    STFileSet *dstFset = tsdbRepairFindTmpFSet(fs, srcFset->fid);
    if (dstFset == NULL) {
      return TSDB_CODE_FAILED;
    }

    if (plan.dropStt) {
      tsdbRepairDropSttOnTmpFSet(dstFset);
      changed = true;
    }

    if (plan.coreAction == TSDB_REPAIR_CORE_DROP) {
      code = tsdbRepairDropCoreOnTmpFSet(fs, dstFset);
      if (code != 0) return code;
      changed = true;
    } else if (plan.coreAction == TSDB_REPAIR_CORE_REBUILD) {
      code = tsdbRepairRebuildCoreOnTmpFSet(fs, srcFset, dstFset, &plan);
      if (code != 0) {
        return code;
      }
      changed = true;
    }
  }

  if (!changed) {
    printf("tsdb force repair dispatch: vnode%d\n", TD_VID(fs->tsdb->pVnode));
    fflush(stdout);
    tsdbMarkForceRepairDone(TD_VID(fs->tsdb->pVnode));
    return 0;
  }

  code = tsdbRepairCommitStagedCurrent(fs);
  if (code != 0) {
    return code;
  }

  printf("tsdb force repair dispatch: vnode%d\n", TD_VID(fs->tsdb->pVnode));
  fflush(stdout);
  tsdbMarkForceRepairDone(TD_VID(fs->tsdb->pVnode));
#endif
  return code;
}

#if 0
static char tsTsdbRepairDoneVnodeId[PATH_MAX] = {0};

typedef enum {
  TSDB_REPAIR_CORE_KEEP = 0,
  TSDB_REPAIR_CORE_DROP,
  TSDB_REPAIR_CORE_REBUILD,
} ECoreRepairAction;

typedef struct {
  int32_t           fid;
  bool              affected;
  bool              dropStt;
  ECoreRepairAction coreAction;
  bool              staged;
  int32_t           totalBlocks;
  int32_t           keptBlocks;
  int32_t           droppedBlocks;
  char              coreReason[64];
} STsdbRepairPlan;

static int32_t save_json(const cJSON *json, const char *fname);

static int32_t tsdbFSDupState(STFileSystem *fs);

static int32_t commit_edit(STFileSystem *fs);

static void tsdbRepairPlanInit(const STFileSet *fset, STsdbRepairPlan *plan) {
  memset(plan, 0, sizeof(*plan));
  plan->fid = fset->fid;
}

static void tsdbRepairPlanSetCore(STsdbRepairPlan *plan, ECoreRepairAction action, const char *reason) {
  if (plan->coreAction == TSDB_REPAIR_CORE_DROP) {
    return;
  }
  if (action == TSDB_REPAIR_CORE_DROP || plan->coreAction == TSDB_REPAIR_CORE_KEEP) {
    plan->coreAction = action;
    if (reason != NULL) {
      tstrncpy(plan->coreReason, reason, sizeof(plan->coreReason));
    }
  }
  plan->affected = plan->dropStt || (plan->coreAction != TSDB_REPAIR_CORE_KEEP);
}

static bool tsdbRepairListContains(const char *csv, int32_t vgId) {
  if (csv == NULL || csv[0] == '\0') {
    return false;
  }

  char buf[PATH_MAX] = {0};
  tstrncpy(buf, csv, sizeof(buf));

  char *savePtr = NULL;
  for (char *token = strtok_r(buf, ",", &savePtr); token != NULL; token = strtok_r(NULL, ",", &savePtr)) {
    if (atoi(token) == vgId) {
      return true;
    }
  }

  return false;
}

static void tsdbMarkForceRepairDone(int32_t vgId) {
  char vnodeText[32] = {0};
  snprintf(vnodeText, sizeof(vnodeText), "%d", vgId);

  if (tsTsdbRepairDoneVnodeId[0] == '\0') {
    tstrncpy(tsTsdbRepairDoneVnodeId, vnodeText, sizeof(tsTsdbRepairDoneVnodeId));
    return;
  }

  if (tsdbRepairListContains(tsTsdbRepairDoneVnodeId, vgId)) {
    return;
  }

  int32_t offset = (int32_t)strlen(tsTsdbRepairDoneVnodeId);
  snprintf(tsTsdbRepairDoneVnodeId + offset, sizeof(tsTsdbRepairDoneVnodeId) - offset, ",%s", vnodeText);
}

static bool tsdbRepairHasTargetForVnode(int32_t vgId) { return dmRepairNeedTsdbRepair(vgId); }

static bool tsdbRepairMatchTargetForFid(int32_t vgId, int32_t fid, EDmRepairStrategy *pStrategy) {
  const SRepairTsdbFileOpt *pOpt = dmRepairGetTsdbFileOpt(vgId, fid);
  if (pOpt == NULL) {
    return false;
  }

  if (pStrategy != NULL) {
    *pStrategy = pOpt->strategy;
  }
  return true;
}


static int32_t tsdbRepairBuildBackupFSetDir(STFileSystem *fs, int32_t fid, char *buf, int32_t bufLen) {
  const char *root = dmRepairHasBackupPath() ? dmRepairBackupPath() : TD_TMP_DIR_PATH;
  const char *sep = root[strlen(root) - 1] == TD_DIRSEP[0] ? "" : TD_DIRSEP;
  time_t      now = (time_t)taosGetTimestampSec();
  struct tm   tmInfo = {0};
  char        dateBuf[16] = {0};

  if (taosLocalTime(&now, &tmInfo, NULL, 0, NULL) == NULL) {
    return TSDB_CODE_FAILED;
  }
  if (taosStrfTime(dateBuf, sizeof(dateBuf), "%Y%m%d", &tmInfo) == 0) {
    return TSDB_CODE_FAILED;
  }

  snprintf(buf, bufLen, "%s%staos_backup_%s%svnode%d%stsdb%sfid_%d", root, sep, dateBuf, TD_DIRSEP,
           TD_VID(fs->tsdb->pVnode), TD_DIRSEP, TD_DIRSEP, fid);
  return 0;
}

static bool tsdbRepairFileMissing(const STFileObj *fobj) {
  if (fobj == NULL) {
    return false;
  }
  return !taosCheckExistFile(fobj->fname);
}

static bool tsdbRepairFileSizeMismatch(const STFileObj *fobj) {
  if (fobj == NULL || !taosCheckExistFile(fobj->fname)) {
    return false;
  }

  int64_t size = 0;
  if (taosStatFile(fobj->fname, &size, NULL, NULL) != 0) {
    return false;
  }

  return size != fobj->f->size;
}

static bool tsdbRepairFileAffected(const STFileObj *fobj) {
  return tsdbRepairFileMissing(fobj) || tsdbRepairFileSizeMismatch(fobj);
}

static int32_t tsdbRepairCopyFileToDir(const char *src, const char *dstDir) {
  if (!taosCheckExistFile(src)) {
    return 0;
  }

  const char *base = strrchr(src, TD_DIRSEP[0]);
  base = (base == NULL) ? src : base + 1;

  char dst[TSDB_FILENAME_LEN] = {0};
  snprintf(dst, sizeof(dst), "%s%s%s", dstDir, TD_DIRSEP, base);
  if (taosCopyFile(src, dst) < 0) {
    return terrno != 0 ? terrno : TSDB_CODE_FAILED;
  }
  return 0;
}

static int32_t tsdbRepairAnalyzeCoreBlocks(STFileSystem *fs, const STFileSet *fset, STsdbRepairPlan *plan) {
  if (fset->farr[TSDB_FTYPE_HEAD] == NULL || fset->farr[TSDB_FTYPE_DATA] == NULL) {
    return 0;
  }

  int32_t               code = 0;
  SDataFileReader      *reader = NULL;
  SBlockData            blockData = {0};
  SDataFileReaderConfig config = {
      .tsdb = fs->tsdb,
      .szPage = fs->tsdb->pVnode->config.tsdbPageSize,
  };

  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset->farr[ftype] == NULL) {
      continue;
    }
    config.files[ftype].exist = true;
    config.files[ftype].file = fset->farr[ftype]->f[0];
  }

  code = tsdbDataFileReaderOpen(NULL, &config, &reader);
  if (code != 0) {
    tsdbRepairPlanSetCore(plan, TSDB_REPAIR_CORE_DROP, "open_core_reader_failed");
    goto _exit;
  }

  const TBrinBlkArray *brinBlkArray = NULL;
  code = tsdbDataFileReadBrinBlk(reader, &brinBlkArray);
  if (code != 0) {
    tsdbRepairPlanSetCore(plan, TSDB_REPAIR_CORE_DROP, "read_head_index_failed");
    goto _exit;
  }

  const SBrinBlk *brinBlk = NULL;
  TARRAY2_FOREACH_PTR(brinBlkArray, brinBlk) {
    SBrinBlock brinBlock = {0};
    code = tBrinBlockInit(&brinBlock);
    if (code != 0) {
      tsdbRepairPlanSetCore(plan, TSDB_REPAIR_CORE_DROP, "init_brin_block_failed");
      goto _exit;
    }

    code = tsdbDataFileReadBrinBlock(reader, brinBlk, &brinBlock);
    if (code != 0) {
      tBrinBlockDestroy(&brinBlock);
      if (plan->keptBlocks > 0) {
        plan->droppedBlocks++;
        tsdbRepairPlanSetCore(plan, TSDB_REPAIR_CORE_REBUILD, "head_index_block_damaged");
      } else {
        tsdbRepairPlanSetCore(plan, TSDB_REPAIR_CORE_DROP, "head_index_block_damaged");
      }
      goto _exit;
    }

    for (int32_t i = 0; i < brinBlock.numOfRecords; ++i) {
      SBrinRecord record = {0};
      plan->totalBlocks++;

      code = tBrinBlockGet(&brinBlock, i, &record);
      if (code != 0) {
        plan->droppedBlocks++;
        tsdbRepairPlanSetCore(plan, plan->keptBlocks > 0 ? TSDB_REPAIR_CORE_REBUILD : TSDB_REPAIR_CORE_DROP,
                              "head_record_damaged");
        break;
      }

      code = tsdbDataFileReadBlockData(reader, &record, &blockData);
      if (code != 0 || blockData.nRow <= 0 || blockData.uid != record.uid || blockData.suid != record.suid) {
        plan->droppedBlocks++;
        tsdbRepairPlanSetCore(plan, TSDB_REPAIR_CORE_REBUILD, "bad_core_block");
      } else {
        plan->keptBlocks++;
      }
      tBlockDataClear(&blockData);
    }

    tBrinBlockDestroy(&brinBlock);
    if (plan->coreAction == TSDB_REPAIR_CORE_DROP) {
      goto _exit;
    }
  }

  if (plan->coreAction == TSDB_REPAIR_CORE_REBUILD && plan->keptBlocks == 0) {
    tsdbRepairPlanSetCore(plan, TSDB_REPAIR_CORE_DROP, "no_valid_core_block");
  }

_exit:
  tBlockDataDestroy(&blockData);
  tsdbDataFileReaderClose(&reader);
  plan->affected = plan->dropStt || (plan->coreAction != TSDB_REPAIR_CORE_KEEP);
  return 0;
}

static int32_t tsdbRepairAnalyzeFileSet(STFileSystem *fs, const STFileSet *fset, STsdbRepairPlan *plan) {
  tsdbRepairPlanInit(fset, plan);

  if (tsdbRepairFileMissing(fset->farr[TSDB_FTYPE_HEAD]) || tsdbRepairFileMissing(fset->farr[TSDB_FTYPE_DATA])) {
    tsdbRepairPlanSetCore(plan, TSDB_REPAIR_CORE_DROP, "missing_core");
  } else if (tsdbRepairFileSizeMismatch(fset->farr[TSDB_FTYPE_HEAD]) ||
             tsdbRepairFileSizeMismatch(fset->farr[TSDB_FTYPE_DATA])) {
    tsdbRepairPlanSetCore(plan, TSDB_REPAIR_CORE_DROP, "size_mismatch_core");
  }

  const SSttLvl *lvl = NULL;
  TARRAY2_FOREACH(fset->lvlArr, lvl) {
    STFileObj *fobj = NULL;
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      if (tsdbRepairFileAffected(fobj)) {
        plan->dropStt = true;
        plan->affected = true;
      }
    }
  }

  if (plan->coreAction == TSDB_REPAIR_CORE_KEEP) {
    return tsdbRepairAnalyzeCoreBlocks(fs, fset, plan);
  }

  return 0;
}

static int32_t tsdbRepairWriteLog(const STFileSet *fset, const STsdbRepairPlan *plan, const char *backupDir) {
  char logPath[TSDB_FILENAME_LEN] = {0};

  snprintf(logPath, sizeof(logPath), "%s%srepair.log", backupDir, TD_DIRSEP);
  char        logBuf[1024] = {0};
  const char *coreAction = "kept";
  if (plan->coreAction == TSDB_REPAIR_CORE_DROP) {
    coreAction = "drop_core_group";
  } else if (plan->coreAction == TSDB_REPAIR_CORE_REBUILD) {
    coreAction = "rebuild_core_group";
  }

  snprintf(logBuf, sizeof(logBuf),
           "fid=%d\nreason=%s\naction=%s\nstt_action=%s\nblock_total=%d\nblock_kept=%d\nblock_dropped=%d\n", fset->fid,
           plan->coreReason[0] != '\0' ? plan->coreReason : (plan->dropStt ? "missing_stt" : "healthy"), coreAction,
           plan->dropStt ? "drop_stt" : "keep_stt", plan->totalBlocks, plan->keptBlocks, plan->droppedBlocks);
  return taosWriteCfgFile(logPath, logBuf, (int32_t)strlen(logBuf));
}

static int32_t tsdbRepairBackupManifest(const STFileSet *fset, const char *backupDir) {
  int32_t code = 0;
  cJSON  *json = cJSON_CreateObject();
  if (json == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  code = tsdbTFileSetToJson(fset, json);
  if (code == 0) {
    char manifest[TSDB_FILENAME_LEN] = {0};
    snprintf(manifest, sizeof(manifest), "%s%smanifest.json", backupDir, TD_DIRSEP);
    code = save_json(json, manifest);
  }

  cJSON_Delete(json);
  return code;
}

static int32_t tsdbRepairBackupAffectedFileSet(STFileSystem *fs, const STFileSet *fset, const STsdbRepairPlan *plan) {
  int32_t code = 0;
  char    backupDir[TSDB_FILENAME_LEN] = {0};
  char    originalDir[TSDB_FILENAME_LEN] = {0};

  code = tsdbRepairBuildBackupFSetDir(fs, fset->fid, backupDir, sizeof(backupDir));
  if (code != 0) {
    return code;
  }

  snprintf(originalDir, sizeof(originalDir), "%s%soriginal", backupDir, TD_DIRSEP);
  code = taosMulMkDir(originalDir);
  if (code != 0) {
    return code;
  }

  code = tsdbRepairBackupManifest(fset, backupDir);
  if (code != 0) {
    return code;
  }

  code = tsdbRepairWriteLog(fset, plan, backupDir);
  if (code != 0) {
    return code;
  }

  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset->farr[ftype] != NULL) {
      code = tsdbRepairCopyFileToDir(fset->farr[ftype]->fname, originalDir);
      if (code != 0) {
        return code;
      }
    }
  }

  const SSttLvl *lvl;
  TARRAY2_FOREACH(fset->lvlArr, lvl) {
    STFileObj *fobj;
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      code = tsdbRepairCopyFileToDir(fobj->fname, originalDir);
      if (code != 0) {
        return code;
      }
    }
  }

  tsdbInfo("vgId:%d fid:%d backed up affected tsdb fileset to %s", TD_VID(fs->tsdb->pVnode), fset->fid, backupDir);
  return 0;
}

static int32_t tsdbRepairCommitStagedCurrent(STFileSystem *fs) {
  char current_t[TSDB_FILENAME_LEN] = {0};

  fs->etype = TSDB_FEDIT_COMMIT;
  current_fname(fs->tsdb, current_t, TSDB_FCURRENT_C);

  int32_t code = save_fs(fs->fSetArrTmp, current_t);
  if (code != 0) {
    return code;
  }

  const char *abortAfterStage = getenv("TAOS_REPAIR_TEST_ABORT_AFTER_STAGE");
  if ((abortAfterStage != NULL && strcmp(abortAfterStage, "1") == 0) ||
      taosCheckExistFile("/tmp/taos_repair_test_abort_after_stage")) {
    tsdbWarn("vgId:%d abort after staging current.c.json for repair test", TD_VID(fs->tsdb->pVnode));
    return TSDB_CODE_FAILED;
  }

  code = commit_edit(fs);
  if (code != 0) {
    return code;
  }

  code = tsdbFSDupState(fs);
  if (code != 0) {
    return code;
  }

  fs->fsstate = TSDB_FS_STATE_NORMAL;
  return 0;
}

static void tsdbRepairRemoveSttFObj(void *data) { TAOS_UNUSED(tsdbTFileObjUnref(*(STFileObj **)data)); }

static STFileSet *tsdbRepairFindTmpFSet(STFileSystem *fs, int32_t fid) {
  STFileSet   key = {.fid = fid};
  STFileSet  *pKey = &key;
  STFileSet **found = TARRAY2_SEARCH(fs->fSetArrTmp, &pKey, tsdbTFileSetCmprFn, TD_EQ);
  return found == NULL ? NULL : *found;
}

static void tsdbRepairDropSttOnTmpFSet(STFileSet *fset) {
  int32_t i = 0;
  while (i < TARRAY2_SIZE(fset->lvlArr)) {
    SSttLvl *lvl = TARRAY2_GET(fset->lvlArr, i);
    int32_t  j = 0;
    while (j < TARRAY2_SIZE(lvl->fobjArr)) {
      STFileObj *fobj = TARRAY2_GET(lvl->fobjArr, j);
      if (tsdbRepairFileAffected(fobj)) {
        TARRAY2_REMOVE(lvl->fobjArr, j, tsdbRepairRemoveSttFObj);
      } else {
        j++;
      }
    }

    if (TARRAY2_SIZE(lvl->fobjArr) == 0) {
      TARRAY2_REMOVE(fset->lvlArr, i, tsdbSttLvlClear);
    } else {
      i++;
    }
  }
}

static int32_t tsdbRepairDropCoreOnTmpFSet(STFileSystem *fs, STFileSet *fset) {
  int32_t      code = 0;
  TFileOpArray fopArr[1];
  TARRAY2_INIT(fopArr);

  for (int32_t ftype = TSDB_FTYPE_HEAD; ftype <= TSDB_FTYPE_SMA; ++ftype) {
    if (fset->farr[ftype] != NULL) {
      STFileOp op = {.optype = TSDB_FOP_REMOVE, .fid = fset->fid, .of = fset->farr[ftype]->f[0]};
      code = TARRAY2_APPEND(fopArr, op);
      if (code != 0) goto _exit;

      TAOS_UNUSED(tsdbTFileObjUnref(fset->farr[ftype]));
      fset->farr[ftype] = NULL;
    }
  }

  const STFileOp *op = NULL;
  TARRAY2_FOREACH_PTR(fopArr, op) {
    code = tsdbTFileSetEdit(fs->tsdb, fset, op);
    if (code != 0) goto _exit;
  }

_exit:
  TARRAY2_DESTROY(fopArr, NULL);
  return code;
}

static int32_t tsdbRepairRebuildCoreOnTmpFSet(STFileSystem *fs, const STFileSet *srcFset, STFileSet *dstFset,
                                              STsdbRepairPlan *plan) {
  int32_t          code = 0;
  SDataFileReader *reader = NULL;
  SDataFileWriter *writer = NULL;
  SBlockData       blockData = {0};
  TFileOpArray     fopArr[1];
  TARRAY2_INIT(fopArr);

  SDataFileReaderConfig readerConfig = {
      .tsdb = fs->tsdb,
      .szPage = fs->tsdb->pVnode->config.tsdbPageSize,
  };
  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (srcFset->farr[ftype] == NULL) {
      continue;
    }
    readerConfig.files[ftype].exist = true;
    readerConfig.files[ftype].file = srcFset->farr[ftype]->f[0];
  }

  code = tsdbDataFileReaderOpen(NULL, &readerConfig, &reader);
  if (code != 0) {
    return code;
  }

  SSkmInfo              skmTb = {0};
  SSkmInfo              skmRow = {0};
  SBuffer               buffers[10] = {0};
  SDataFileWriterConfig writerConfig = {
      .tsdb = fs->tsdb,
      .cmprAlg = fs->tsdb->pVnode->config.tsdbCfg.compression,
      .maxRow = fs->tsdb->pVnode->config.tsdbCfg.maxRows,
      .szPage = fs->tsdb->pVnode->config.tsdbPageSize,
      .fid = srcFset->fid,
      .cid = tsdbFSAllocEid(fs),
      .expLevel = tsdbFidLevel(srcFset->fid, &fs->tsdb->keepCfg, taosGetTimestampSec()),
      .compactVersion = INT64_MAX,
      .lcn = srcFset->farr[TSDB_FTYPE_DATA] ? srcFset->farr[TSDB_FTYPE_DATA]->f->lcn : 0,
      .skmTb = &skmTb,
      .skmRow = &skmRow,
      .buffers = buffers,
  };

  code = tsdbDataFileWriterOpen(&writerConfig, &writer);
  if (code != 0) {
    goto _exit;
  }

  const TBrinBlkArray *brinBlkArray = NULL;
  code = tsdbDataFileReadBrinBlk(reader, &brinBlkArray);
  if (code != 0) {
    goto _exit;
  }

  const SBrinBlk *brinBlk = NULL;
  TARRAY2_FOREACH_PTR(brinBlkArray, brinBlk) {
    SBrinBlock brinBlock = {0};
    code = tBrinBlockInit(&brinBlock);
    if (code != 0) {
      goto _exit;
    }

    code = tsdbDataFileReadBrinBlock(reader, brinBlk, &brinBlock);
    if (code != 0) {
      tBrinBlockDestroy(&brinBlock);
      break;
    }

    for (int32_t i = 0; i < brinBlock.numOfRecords; ++i) {
      SBrinRecord record = {0};
      if (tBrinBlockGet(&brinBlock, i, &record) != 0) {
        continue;
      }
      if (tsdbDataFileReadBlockData(reader, &record, &blockData) != 0 || blockData.nRow <= 0 ||
          blockData.uid != record.uid || blockData.suid != record.suid) {
        tBlockDataClear(&blockData);
        continue;
      }
      code = tsdbDataFileWriteBlockData(writer, &blockData);
      if (code != 0) {
        tBrinBlockDestroy(&brinBlock);
        goto _exit;
      }
      tBlockDataClear(&blockData);
    }

    tBrinBlockDestroy(&brinBlock);
  }

  for (int32_t ftype = TSDB_FTYPE_HEAD; ftype <= TSDB_FTYPE_SMA; ++ftype) {
    if (dstFset->farr[ftype] != NULL) {
      STFileOp op = {.optype = TSDB_FOP_REMOVE, .fid = dstFset->fid, .of = dstFset->farr[ftype]->f[0]};
      code = TARRAY2_APPEND(fopArr, op);
      if (code != 0) {
        goto _exit;
      }
    }
  }

  code = tsdbDataFileWriterClose(&writer, false, fopArr);
  if (code != 0) {
    goto _exit;
  }

  const STFileOp *op = NULL;
  TARRAY2_FOREACH_PTR(fopArr, op) {
    code = tsdbTFileSetEdit(fs->tsdb, dstFset, op);
    if (code != 0) {
      goto _exit;
    }
  }

  plan->staged = true;

_exit:
  TARRAY2_DESTROY(fopArr, NULL);
  tBlockDataDestroy(&blockData);
  tsdbDataFileWriterClose(&writer, code != 0, NULL);
  tsdbDataFileReaderClose(&reader);
  if (code != 0) {
    return code;
  }
  if (dstFset->farr[TSDB_FTYPE_HEAD] == NULL || dstFset->farr[TSDB_FTYPE_DATA] == NULL) {
    code = tsdbRepairDropCoreOnTmpFSet(fs, dstFset);
    if (code != 0) return code;
    plan->coreAction = TSDB_REPAIR_CORE_DROP;
  }
  return 0;
}

#endif
