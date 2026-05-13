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
#include "tsdbDataFileRAW.h"
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

typedef enum {
  TSDB_REPAIR_ACTION_KEEP = 0,
  TSDB_REPAIR_ACTION_DROP = 1,
  TSDB_REPAIR_ACTION_REBUILD = 2,
} ETsdbRepairAction;

typedef enum {
  TSDB_REPAIR_MODE_DROP_INVALID_ONLY = 0,
  TSDB_REPAIR_MODE_HEAD_ONLY_REBUILD = 1,
  TSDB_REPAIR_MODE_FULL_REBUILD = 2,
} ETsdbRepairMode;

typedef TARRAY2(SBrinRecord) TBrinRecordArray;

enum {
  TSDB_REPAIR_HEAD_OP_REMOVE_HEAD = 1 << 0,
  TSDB_REPAIR_HEAD_OP_CREATE_HEAD = 1 << 1,
  TSDB_REPAIR_HEAD_OP_REMOVE_DATA = 1 << 2,
  TSDB_REPAIR_HEAD_OP_CREATE_DATA = 1 << 3,
  TSDB_REPAIR_HEAD_OP_REMOVE_SMA = 1 << 4,
  TSDB_REPAIR_HEAD_OP_CREATE_SMA = 1 << 5,
};

typedef struct {
  ETsdbRepairAction action;
  int32_t           keptBlocks;
  int32_t           droppedBlocks;
  const char       *reason;
} STsdbRepairCoreResult;

typedef struct {
  ETsdbRepairAction action;
  bool              rewriteRequired;
  int32_t           keptDataBlocks;
  int32_t           droppedDataBlocks;
  int32_t           keptTombBlocks;
  int32_t           droppedTombBlocks;
  const char       *reason;
} STsdbRepairSttResult;

const char *tsdbRepairStrategyName(EDmRepairStrategy strategy) {
  switch (strategy) {
    case DM_REPAIR_STRATEGY_TSDB_DROP_INVALID_ONLY:
      return "drop_invalid_only";
    case DM_REPAIR_STRATEGY_TSDB_HEAD_ONLY_REBUILD:
      return "head_only_rebuild";
    case DM_REPAIR_STRATEGY_TSDB_FULL_REBUILD:
      return "full_rebuild";
    default:
      return "unknown";
  }
}

EDmRepairStrategy tsdbRepairNormalizeStrategy(EDmRepairStrategy strategy) {
  return strategy == DM_REPAIR_STRATEGY_NONE ? DM_REPAIR_STRATEGY_TSDB_DROP_INVALID_ONLY : strategy;
}

int32_t tsdbRepairResolveMode(EDmRepairStrategy strategy) {
  switch (tsdbRepairNormalizeStrategy(strategy)) {
    case DM_REPAIR_STRATEGY_TSDB_HEAD_ONLY_REBUILD:
      return TSDB_REPAIR_MODE_HEAD_ONLY_REBUILD;
    case DM_REPAIR_STRATEGY_TSDB_FULL_REBUILD:
      return TSDB_REPAIR_MODE_FULL_REBUILD;
    case DM_REPAIR_STRATEGY_TSDB_DROP_INVALID_ONLY:
    default:
      return TSDB_REPAIR_MODE_DROP_INVALID_ONLY;
  }
}

void tsdbRepairBuildHeadOnlyBrinRecord(const SBrinRecord *src, bool keepSma, SBrinRecord *dst) {
  if (src == NULL || dst == NULL) {
    return;
  }

  *dst = *src;
  if (!keepSma) {
    dst->smaOffset = 0;
    dst->smaSize = 0;
  }
}

int32_t tsdbRepairDescribeHeadOnlyOps(bool hasHead, bool hasSma, bool dropSma) {
  int32_t opMask = TSDB_REPAIR_HEAD_OP_CREATE_HEAD;

  if (hasHead) {
    opMask |= TSDB_REPAIR_HEAD_OP_REMOVE_HEAD;
  }
  if (hasSma && dropSma) {
    opMask |= TSDB_REPAIR_HEAD_OP_REMOVE_SMA;
  }

  return opMask;
}

bool tsdbRepairDataBlockLooksValid(const SBlockData *blockData, const SBrinRecord *record) {
  if (blockData == NULL || record == NULL) {
    return false;
  }

  if (blockData->nRow <= 0) {
    return false;
  }

  if (blockData->suid != record->suid || blockData->uid != record->uid) {
    return false;
  }

  if (blockData->aTSKEY == NULL || blockData->aVersion == NULL) {
    return false;
  }

  return true;
}

bool tsdbRepairSttBlockLooksValid(const SBlockData *blockData) {
  if (blockData == NULL) {
    return false;
  }

  if (blockData->nRow <= 0) {
    return false;
  }

  if (blockData->aTSKEY == NULL || blockData->aVersion == NULL) {
    return false;
  }

  if (blockData->uid == 0 && blockData->aUid == NULL) {
    return false;
  }

  return true;
}

int32_t tsdbRepairResolveCoreAction(int32_t keptBlocks, int32_t droppedBlocks) {
  if (droppedBlocks <= 0) {
    return TSDB_REPAIR_ACTION_KEEP;
  }

  return keptBlocks > 0 ? TSDB_REPAIR_ACTION_REBUILD : TSDB_REPAIR_ACTION_DROP;
}

int32_t tsdbRepairResolveSttAction(int32_t keptDataBlocks, int32_t keptTombBlocks, int32_t droppedDataBlocks,
                                   int32_t droppedTombBlocks) {
  if (droppedDataBlocks <= 0 && droppedTombBlocks <= 0) {
    return TSDB_REPAIR_ACTION_KEEP;
  }

  return (keptDataBlocks > 0 || keptTombBlocks > 0) ? TSDB_REPAIR_ACTION_REBUILD : TSDB_REPAIR_ACTION_DROP;
}

bool tsdbRepairShouldAbortCoreWriterClose(int32_t rebuiltBlocks) { return rebuiltBlocks <= 0; }

bool tsdbRepairShouldAbortSttWriterClose(int32_t rebuiltDataBlocks, int32_t rebuiltTombBlocks) {
  return rebuiltDataBlocks <= 0 && rebuiltTombBlocks <= 0;
}

int32_t tsdbRepairResolveHeadOnlyBrinFlushThreshold(int32_t maxRows) { return maxRows > 0 ? maxRows : 1; }

bool tsdbRepairShouldFlushHeadOnlyBrinBlock(int32_t numOfRecords, int32_t maxRows) {
  return numOfRecords >= tsdbRepairResolveHeadOnlyBrinFlushThreshold(maxRows);
}

bool tsdbRepairShouldRetryHeadOnlyBrinPut(int32_t putCode, int32_t numOfRecords, int32_t maxRows) {
  return putCode == TSDB_CODE_INVALID_PARA && tsdbRepairShouldFlushHeadOnlyBrinBlock(numOfRecords, maxRows);
}

static const char *tsdbRepairActionName(ETsdbRepairAction action) {
  switch (action) {
    case TSDB_REPAIR_ACTION_KEEP:
      return "keep";
    case TSDB_REPAIR_ACTION_DROP:
      return "drop";
    case TSDB_REPAIR_ACTION_REBUILD:
      return "rebuild";
    default:
      return "unknown";
  }
}

static EDmRepairStrategy tsdbRepairGetStrategyForFileSet(int32_t vgId, int32_t fid) {
  const SRepairTsdbFileOpt *pOpt = dmRepairGetTsdbFileOpt(vgId, fid);

  return tsdbRepairNormalizeStrategy(pOpt == NULL ? DM_REPAIR_STRATEGY_NONE : pOpt->strategy);
}

bool tsdbRepairShouldProcessFileSet(int32_t vnodeId, int32_t fid) {
  return dmRepairGetTsdbFileOpt(vnodeId, fid) != NULL;
}

static const char *tsdbRepairModeName(ETsdbRepairMode mode) {
  switch (mode) {
    case TSDB_REPAIR_MODE_DROP_INVALID_ONLY:
      return "drop_invalid_only";
    case TSDB_REPAIR_MODE_HEAD_ONLY_REBUILD:
      return "head_only_rebuild";
    case TSDB_REPAIR_MODE_FULL_REBUILD:
      return "full_rebuild";
    default:
      return "unknown";
  }
}

static void tsdbRepairSetReason(const char **reason, const char *value) {
  if (reason == NULL || *reason != NULL || value == NULL) {
    return;
  }

  *reason = value;
}

static void tsdbRepairCoreResultInit(STsdbRepairCoreResult *result) {
  memset(result, 0, sizeof(*result));
  result->action = TSDB_REPAIR_ACTION_KEEP;
}

static void tsdbRepairSttResultInit(STsdbRepairSttResult *result) {
  memset(result, 0, sizeof(*result));
  result->action = TSDB_REPAIR_ACTION_KEEP;
}

static void tsdbRepairFinalizeCoreResult(STsdbRepairCoreResult *result) {
  if (result->action == TSDB_REPAIR_ACTION_KEEP) {
    result->action = (ETsdbRepairAction)tsdbRepairResolveCoreAction(result->keptBlocks, result->droppedBlocks);
  }

  if (result->reason == NULL && result->action != TSDB_REPAIR_ACTION_KEEP) {
    result->reason = "damaged_core_group";
  }
}

static void tsdbRepairFinalizeSttResult(STsdbRepairSttResult *result) {
  if (result->action == TSDB_REPAIR_ACTION_KEEP) {
    result->action = (ETsdbRepairAction)tsdbRepairResolveSttAction(
        result->keptDataBlocks, result->keptTombBlocks, result->droppedDataBlocks, result->droppedTombBlocks);
  }

  if (result->rewriteRequired && result->action == TSDB_REPAIR_ACTION_KEEP) {
    result->action = (result->keptDataBlocks > 0 || result->keptTombBlocks > 0) ? TSDB_REPAIR_ACTION_REBUILD
                                                                                : TSDB_REPAIR_ACTION_DROP;
  }

  if (result->reason == NULL && result->action != TSDB_REPAIR_ACTION_KEEP) {
    result->reason = "damaged_stt_file";
  }
}

static bool tsdbRepairFileMissing(const STFileObj *fobj) {
  if (fobj == NULL) {
    return false;
  }

  return !taosCheckExistFile(fobj->fname);
}

bool tsdbRepairFileAffected(const STFileObj *fobj) {
  // File length can diverge from current.json across crash windows; let deep scan
  // decide whether the payload is still readable instead of pre-dropping it here.
  return tsdbRepairFileMissing(fobj);
}

const char *tsdbRepairFileIssue(const STFileObj *fobj) {
  if (fobj == NULL) {
    return NULL;
  }

  if (tsdbRepairFileMissing(fobj)) {
    return "missing";
  }

  return NULL;
}

static bool tsdbRepairCoreFilesAffected(const STFileSet *pFileSet) {
  return tsdbRepairFileAffected(pFileSet->farr[TSDB_FTYPE_HEAD]) ||
         tsdbRepairFileAffected(pFileSet->farr[TSDB_FTYPE_DATA]);
}

static int32_t tsdbRepairAppendRemoveFileOp(TFileOpArray *opArr, int32_t fid, const STFile *file) {
  STFileOp op = {
      .optype = TSDB_FOP_REMOVE,
      .fid = fid,
      .of = *file,
  };

  return TARRAY2_APPEND(opArr, op);
}

static int32_t tsdbRepairAppendRemoveFileObjOp(TFileOpArray *opArr, int32_t fid, const STFileObj *fobj) {
  if (fobj == NULL) {
    return 0;
  }

  return tsdbRepairAppendRemoveFileOp(opArr, fid, fobj->f);
}

static int32_t tsdbRepairAppendOps(TFileOpArray *dst, const TFileOpArray *src) {
  const STFileOp *op = NULL;

  TARRAY2_FOREACH_PTR(src, op) {
    int32_t code = TARRAY2_APPEND(dst, *op);
    if (code != 0) {
      return code;
    }
  }

  return 0;
}

static int32_t tsdbRepairAppendRemoveCoreOps(const STFileSet *pFileSet, TFileOpArray *opArr) {
  int32_t code = 0;

  code = tsdbRepairAppendRemoveFileObjOp(opArr, pFileSet->fid, pFileSet->farr[TSDB_FTYPE_HEAD]);
  if (code != 0) {
    return code;
  }

  code = tsdbRepairAppendRemoveFileObjOp(opArr, pFileSet->fid, pFileSet->farr[TSDB_FTYPE_DATA]);
  if (code != 0) {
    return code;
  }

  return tsdbRepairAppendRemoveFileObjOp(opArr, pFileSet->fid, pFileSet->farr[TSDB_FTYPE_SMA]);
}

static int32_t tsdbRepairAppendHeadOnlyRebuildOps(const STFileSet *pFileSet, bool dropSma,
                                                  const TFileOpArray *writerOps, TFileOpArray *opArr) {
  int32_t code = 0;
  int32_t opMask = tsdbRepairDescribeHeadOnlyOps(pFileSet->farr[TSDB_FTYPE_HEAD] != NULL,
                                                 pFileSet->farr[TSDB_FTYPE_SMA] != NULL, dropSma);

  if ((opMask & TSDB_REPAIR_HEAD_OP_REMOVE_HEAD) != 0) {
    code = tsdbRepairAppendRemoveFileObjOp(opArr, pFileSet->fid, pFileSet->farr[TSDB_FTYPE_HEAD]);
    if (code != 0) {
      return code;
    }
  }

  if ((opMask & TSDB_REPAIR_HEAD_OP_REMOVE_SMA) != 0) {
    code = tsdbRepairAppendRemoveFileObjOp(opArr, pFileSet->fid, pFileSet->farr[TSDB_FTYPE_SMA]);
    if (code != 0) {
      return code;
    }
  }

  return tsdbRepairAppendOps(opArr, writerOps);
}

static int32_t tsdbForceRepairFileSetBadFiles(STFileSystem *pFS, const STFileSet *pFileSet, TFileOpArray *opArr) {
  int32_t     code = 0;
  int32_t     vgId = TD_VID(pFS->tsdb->pVnode);
  STFileObj  *pSma = pFileSet->farr[TSDB_FTYPE_SMA];
  STFileObj  *pTomb = pFileSet->farr[TSDB_FTYPE_TOMB];
  const char *headIssue = tsdbRepairFileIssue(pFileSet->farr[TSDB_FTYPE_HEAD]);
  const char *dataIssue = tsdbRepairFileIssue(pFileSet->farr[TSDB_FTYPE_DATA]);
  const char *smaIssue = tsdbRepairFileIssue(pSma);
  SSttLvl    *sttLevel = NULL;

  if (headIssue != NULL || dataIssue != NULL) {
    tsdbWarn("vgId:%d fid:%d drop core group before deep scan, head:%s data:%s", vgId, pFileSet->fid,
             headIssue == NULL ? "ok" : headIssue, dataIssue == NULL ? "ok" : dataIssue);
    if (smaIssue != NULL) {
      tsdbTrace("vgId:%d fid:%d affected sma file is covered by core-group drop, reason:%s", vgId, pFileSet->fid,
                smaIssue);
    }

    code = tsdbRepairAppendRemoveCoreOps(pFileSet, opArr);
    if (code != 0) {
      tsdbError("vgId:%d fid:%d failed to append remove ops for affected core group since %s, code:%d", vgId,
                pFileSet->fid, tstrerror(code), code);
      return code;
    }
  } else if (smaIssue != NULL) {
    tsdbWarn("vgId:%d fid:%d remove affected sma file before deep scan, reason:%s", vgId, pFileSet->fid, smaIssue);
    code = tsdbRepairAppendRemoveFileObjOp(opArr, pFileSet->fid, pSma);
    if (code != 0) {
      tsdbError("vgId:%d fid:%d failed to append remove op for affected sma file since %s, code:%d", vgId,
                pFileSet->fid, tstrerror(code), code);
      return code;
    }
  }

  if (pTomb != NULL) {
    const char *tombIssue = tsdbRepairFileIssue(pTomb);

    if (tombIssue != NULL) {
      tsdbWarn("vgId:%d fid:%d remove affected tomb file before deep scan, reason:%s", vgId, pFileSet->fid, tombIssue);
      code = tsdbRepairAppendRemoveFileObjOp(opArr, pFileSet->fid, pTomb);
      if (code != 0) {
        tsdbError("vgId:%d fid:%d failed to append remove op for affected tomb file since %s, code:%d", vgId,
                  pFileSet->fid, tstrerror(code), code);
        return code;
      }
    }
  }

  TARRAY2_FOREACH(pFileSet->lvlArr, sttLevel) {
    STFileObj *pStt = NULL;

    TARRAY2_FOREACH(sttLevel->fobjArr, pStt) {
      const char *sttIssue = tsdbRepairFileIssue(pStt);

      if (sttIssue == NULL) {
        continue;
      }

      tsdbWarn("vgId:%d fid:%d level:%d remove affected stt file before deep scan, reason:%s file:%s", vgId,
               pFileSet->fid, sttLevel->level, sttIssue, pStt->fname);
      code = tsdbRepairAppendRemoveFileObjOp(opArr, pFileSet->fid, pStt);
      if (code != 0) {
        tsdbError("vgId:%d fid:%d level:%d failed to append remove op for affected stt file:%s since %s, code:%d", vgId,
                  pFileSet->fid, sttLevel->level, pStt->fname, tstrerror(code), code);
        return code;
      }
    }
  }

  return 0;
}

static int32_t tsdbRepairAnalyzeCore(STFileSystem *pFS, const STFileSet *pFileSet, STsdbRepairCoreResult *result) {
  int32_t               code = 0;
  int32_t               vgId = TD_VID(pFS->tsdb->pVnode);
  SDataFileReader      *reader = NULL;
  SBlockData            blockData = {0};
  const TBrinBlkArray  *brinBlkArray = NULL;
  SDataFileReaderConfig readerConfig = {
      .tsdb = pFS->tsdb,
      .szPage = pFS->tsdb->pVnode->config.tsdbPageSize,
  };

  if (pFileSet->farr[TSDB_FTYPE_HEAD] == NULL || pFileSet->farr[TSDB_FTYPE_DATA] == NULL ||
      tsdbRepairCoreFilesAffected(pFileSet)) {
    tsdbTrace("vgId:%d fid:%d skip core deep scan because core files are already affected or absent", vgId,
              pFileSet->fid);
    return 0;
  }

  tsdbDebug("vgId:%d fid:%d start analyzing core group", vgId, pFileSet->fid);

  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (pFileSet->farr[ftype] == NULL || tsdbRepairFileAffected(pFileSet->farr[ftype])) {
      continue;
    }

    readerConfig.files[ftype].exist = true;
    readerConfig.files[ftype].file = pFileSet->farr[ftype]->f[0];
  }

  code = tsdbDataFileReaderOpen(NULL, &readerConfig, &reader);
  if (code != 0) {
    result->action = TSDB_REPAIR_ACTION_DROP;
    tsdbRepairSetReason(&result->reason, "open_core_reader_failed");
    tsdbWarn("vgId:%d fid:%d failed to open core reader, downgrade action to %s since %s", vgId, pFileSet->fid,
             tsdbRepairActionName(result->action), tstrerror(code));
    code = 0;
    goto _exit;
  }

  code = tsdbDataFileReadBrinBlk(reader, &brinBlkArray);
  if (code != 0) {
    result->action = TSDB_REPAIR_ACTION_DROP;
    tsdbRepairSetReason(&result->reason, "read_brin_index_failed");
    tsdbWarn("vgId:%d fid:%d failed to read brin index, downgrade action to %s since %s", vgId, pFileSet->fid,
             tsdbRepairActionName(result->action), tstrerror(code));
    code = 0;
    goto _exit;
  }

  for (int32_t i = 0; i < TARRAY2_SIZE(brinBlkArray); ++i) {
    const SBrinBlk *pBrinBlk = TARRAY2_GET_PTR(brinBlkArray, i);
    SBrinBlock      brinBlock = {0};

    code = tBrinBlockInit(&brinBlock);
    if (code != 0) {
      tsdbError("vgId:%d fid:%d failed to init brin block during core analysis since %s, code:%d", vgId, pFileSet->fid,
                tstrerror(code), code);
      goto _exit;
    }

    code = tsdbDataFileReadBrinBlock(reader, pBrinBlk, &brinBlock);
    if (code != 0) {
      result->droppedBlocks++;
      tsdbRepairSetReason(&result->reason, "damaged_brin_block");
      tsdbWarn("vgId:%d fid:%d skip damaged brin block #%d since %s", vgId, pFileSet->fid, i, tstrerror(code));
      code = 0;
      tBrinBlockDestroy(&brinBlock);
      continue;
    }

    for (int32_t iRecord = 0; iRecord < BRIN_BLOCK_SIZE(&brinBlock); ++iRecord) {
      SBrinRecord record = {0};

      code = tBrinBlockGet(&brinBlock, iRecord, &record);
      if (code != 0) {
        result->droppedBlocks++;
        tsdbRepairSetReason(&result->reason, "damaged_brin_record");
        tsdbWarn("vgId:%d fid:%d stop scanning damaged brin record in block #%d record #%d since %s", vgId,
                 pFileSet->fid, i, iRecord, tstrerror(code));
        code = 0;
        break;
      }

      tBlockDataClear(&blockData);
      code = tsdbDataFileReadBlockData(reader, &record, &blockData);
      if (code != 0 || !tsdbRepairDataBlockLooksValid(&blockData, &record)) {
        result->droppedBlocks++;
        tsdbRepairSetReason(&result->reason, code != 0 ? "read_data_block_failed" : "invalid_data_block");
        tsdbWarn("vgId:%d fid:%d drop damaged core block, blockOffset:%" PRId64 " blockSize:%d uid:%" PRId64
                 " suid:%" PRId64 " reason:%s",
                 vgId, pFileSet->fid, record.blockOffset, record.blockSize, record.uid, record.suid,
                 code != 0 ? tstrerror(code) : "invalid_block_payload");
        code = 0;
        tBlockDataClear(&blockData);
        continue;
      }

      result->keptBlocks++;
      tsdbTrace("vgId:%d fid:%d keep core block, blockOffset:%" PRId64 " blockSize:%d uid:%" PRId64 " suid:%" PRId64,
                vgId, pFileSet->fid, record.blockOffset, record.blockSize, record.uid, record.suid);
      tBlockDataClear(&blockData);
    }

    tBrinBlockDestroy(&brinBlock);
    if (code != 0) {
      goto _exit;
    }
  }

_exit:
  if (code == 0) {
    tsdbRepairFinalizeCoreResult(result);
    tsdbDebug("vgId:%d fid:%d core analysis finished, action:%s keptBlocks:%d droppedBlocks:%d reason:%s", vgId,
              pFileSet->fid, tsdbRepairActionName(result->action), result->keptBlocks, result->droppedBlocks,
              result->reason == NULL ? "healthy" : result->reason);
  }

  tBlockDataDestroy(&blockData);
  tsdbDataFileReaderClose(&reader);
  return code;
}

static int32_t tsdbRepairRebuildCore(STFileSystem *pFS, const STFileSet *pFileSet, TFileOpArray *writerOps,
                                     int32_t *rebuiltBlocks) {
  int32_t               code = 0;
  int32_t               vgId = TD_VID(pFS->tsdb->pVnode);
  SDataFileReader      *reader = NULL;
  SDataFileWriter      *writer = NULL;
  SBlockData            blockData = {0};
  const TBrinBlkArray  *brinBlkArray = NULL;
  SDataFileReaderConfig readerConfig = {
      .tsdb = pFS->tsdb,
      .szPage = pFS->tsdb->pVnode->config.tsdbPageSize,
  };
  SDataFileWriterConfig writerConfig = {
      .tsdb = pFS->tsdb,
      .cmprAlg = pFS->tsdb->pVnode->config.tsdbCfg.compression,
      .maxRow = pFS->tsdb->pVnode->config.tsdbCfg.maxRows,
      .szPage = pFS->tsdb->pVnode->config.tsdbPageSize,
      .fid = pFileSet->fid,
      .cid = tsdbFSAllocEid(pFS),
      .expLevel = tsdbFidLevel(pFileSet->fid, &pFS->tsdb->keepCfg, taosGetTimestampSec()),
      .compactVersion = INT64_MAX,
      .lcn = pFileSet->farr[TSDB_FTYPE_DATA] ? pFileSet->farr[TSDB_FTYPE_DATA]->f->lcn : 0,
  };

  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (pFileSet->farr[ftype] == NULL || tsdbRepairFileAffected(pFileSet->farr[ftype])) {
      continue;
    }

    readerConfig.files[ftype].exist = true;
    readerConfig.files[ftype].file = pFileSet->farr[ftype]->f[0];
  }

  code = tsdbDataFileReaderOpen(NULL, &readerConfig, &reader);
  if (code != 0) {
    tsdbWarn("vgId:%d fid:%d failed to reopen core reader for rebuild since %s, fallback to drop", vgId, pFileSet->fid,
             tstrerror(code));
    code = 0;
    goto _exit;
  }

  code = tsdbDataFileReadBrinBlk(reader, &brinBlkArray);
  if (code != 0) {
    tsdbWarn("vgId:%d fid:%d failed to reread brin index for rebuild since %s, fallback to drop", vgId, pFileSet->fid,
             tstrerror(code));
    code = 0;
    goto _exit;
  }

  code = tsdbDataFileWriterOpen(&writerConfig, &writer);
  if (code != 0) {
    tsdbError("vgId:%d fid:%d failed to open core writer since %s, code:%d", vgId, pFileSet->fid, tstrerror(code),
              code);
    goto _exit;
  }

  for (int32_t i = 0; i < TARRAY2_SIZE(brinBlkArray); ++i) {
    const SBrinBlk *pBrinBlk = TARRAY2_GET_PTR(brinBlkArray, i);
    SBrinBlock      brinBlock = {0};

    code = tBrinBlockInit(&brinBlock);
    if (code != 0) {
      tsdbError("vgId:%d fid:%d failed to init brin block during core rebuild since %s, code:%d", vgId, pFileSet->fid,
                tstrerror(code), code);
      goto _exit;
    }

    code = tsdbDataFileReadBrinBlock(reader, pBrinBlk, &brinBlock);
    if (code != 0) {
      tsdbWarn("vgId:%d fid:%d skip damaged brin block #%d during core rebuild since %s", vgId, pFileSet->fid, i,
               tstrerror(code));
      code = 0;
      tBrinBlockDestroy(&brinBlock);
      continue;
    }

    for (int32_t iRecord = 0; iRecord < BRIN_BLOCK_SIZE(&brinBlock); ++iRecord) {
      SBrinRecord record = {0};

      code = tBrinBlockGet(&brinBlock, iRecord, &record);
      if (code != 0) {
        tsdbWarn("vgId:%d fid:%d stop scanning damaged brin record in block #%d during rebuild since %s", vgId,
                 pFileSet->fid, i, tstrerror(code));
        code = 0;
        break;
      }

      tBlockDataClear(&blockData);
      code = tsdbDataFileReadBlockData(reader, &record, &blockData);
      if (code != 0 || !tsdbRepairDataBlockLooksValid(&blockData, &record)) {
        tsdbTrace("vgId:%d fid:%d skip invalid core block during rebuild, blockOffset:%" PRId64 " reason:%s", vgId,
                  pFileSet->fid, record.blockOffset, code != 0 ? tstrerror(code) : "invalid_block_payload");
        code = 0;
        tBlockDataClear(&blockData);
        continue;
      }

      code = tsdbDataFileWriteBlockData(writer, &blockData);
      tBlockDataClear(&blockData);
      if (code != 0) {
        tsdbError("vgId:%d fid:%d failed to write rebuilt core block since %s, code:%d", vgId, pFileSet->fid,
                  tstrerror(code), code);
        tBrinBlockDestroy(&brinBlock);
        goto _exit;
      }

      (*rebuiltBlocks)++;
      tsdbTrace("vgId:%d fid:%d rebuilt core block, blockOffset:%" PRId64 " total:%d", vgId, pFileSet->fid,
                record.blockOffset, *rebuiltBlocks);
    }

    tBrinBlockDestroy(&brinBlock);
    if (code != 0) {
      goto _exit;
    }
  }

_exit:
  tBlockDataDestroy(&blockData);
  if (writer != NULL) {
    bool    abort = tsdbRepairShouldAbortCoreWriterClose(*rebuiltBlocks);
    int32_t closeCode = tsdbDataFileWriterClose(&writer, abort, abort ? NULL : writerOps);
    if (closeCode != 0) {
      tsdbError("vgId:%d fid:%d failed to close rebuilt core writer since %s, code:%d", vgId, pFileSet->fid,
                tstrerror(closeCode), closeCode);
      code = code == 0 ? closeCode : code;
    }
  }
  // tsdbDataFileWriterClose(&writer, true, NULL);
  tsdbDataFileReaderClose(&reader);
  return code;
}

static int32_t tsdbRepairCollectHeadOnlyRecords(STFileSystem *pFS, const STFileSet *pFileSet,
                                                TBrinRecordArray *recordArr, bool *dropSma, int32_t *rebuiltBlocks) {
  int32_t              code = 0;
  int32_t              vgId = TD_VID(pFS->tsdb->pVnode);
  SDataFileReader     *reader = NULL;
  SBlockData           blockData = {0};
  const TBrinBlkArray *brinBlkArray = NULL;
  bool                 hasReadableSma =
      pFileSet->farr[TSDB_FTYPE_SMA] != NULL && !tsdbRepairFileAffected(pFileSet->farr[TSDB_FTYPE_SMA]);
  bool                  keepSma = hasReadableSma;
  TColumnDataAggArray   columnDataAggArray[1];
  SDataFileReaderConfig readerConfig = {
      .tsdb = pFS->tsdb,
      .szPage = pFS->tsdb->pVnode->config.tsdbPageSize,
  };

  TARRAY2_INIT(columnDataAggArray);

  tsdbInfo("vgId:%d fid:%d start collecting head-only rebuild records, keepSma:%s", vgId, pFileSet->fid,
           hasReadableSma ? "yes" : "no");

  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (pFileSet->farr[ftype] == NULL || tsdbRepairFileAffected(pFileSet->farr[ftype])) {
      continue;
    }

    readerConfig.files[ftype].exist = true;
    readerConfig.files[ftype].file = pFileSet->farr[ftype]->f[0];
  }

  code = tsdbDataFileReaderOpen(NULL, &readerConfig, &reader);
  if (code != 0) {
    tsdbWarn("vgId:%d fid:%d failed to reopen core reader for head-only rebuild since %s, fallback to drop", vgId,
             pFileSet->fid, tstrerror(code));
    code = 0;
    goto _exit;
  }

  code = tsdbDataFileReadBrinBlk(reader, &brinBlkArray);
  if (code != 0) {
    tsdbWarn("vgId:%d fid:%d failed to reread brin index for head-only rebuild since %s, fallback to drop", vgId,
             pFileSet->fid, tstrerror(code));
    code = 0;
    goto _exit;
  }

  for (int32_t i = 0; i < TARRAY2_SIZE(brinBlkArray); ++i) {
    const SBrinBlk *pBrinBlk = TARRAY2_GET_PTR(brinBlkArray, i);
    SBrinBlock      brinBlock = {0};

    code = tBrinBlockInit(&brinBlock);
    if (code != 0) {
      tsdbError("vgId:%d fid:%d failed to init brin block during head-only rebuild since %s, code:%d", vgId,
                pFileSet->fid, tstrerror(code), code);
      goto _exit;
    }

    code = tsdbDataFileReadBrinBlock(reader, pBrinBlk, &brinBlock);
    if (code != 0) {
      tsdbWarn("vgId:%d fid:%d skip damaged brin block #%d during head-only rebuild since %s", vgId, pFileSet->fid, i,
               tstrerror(code));
      code = 0;
      tBrinBlockDestroy(&brinBlock);
      continue;
    }

    for (int32_t iRecord = 0; iRecord < BRIN_BLOCK_SIZE(&brinBlock); ++iRecord) {
      SBrinRecord record = {0};

      code = tBrinBlockGet(&brinBlock, iRecord, &record);
      if (code != 0) {
        tsdbWarn("vgId:%d fid:%d stop scanning damaged brin record in block #%d during head-only rebuild since %s",
                 vgId, pFileSet->fid, i, tstrerror(code));
        code = 0;
        break;
      }

      tBlockDataClear(&blockData);
      code = tsdbDataFileReadBlockData(reader, &record, &blockData);
      if (code != 0 || !tsdbRepairDataBlockLooksValid(&blockData, &record)) {
        tsdbTrace("vgId:%d fid:%d skip invalid core block during head-only rebuild, blockOffset:%" PRId64 " reason:%s",
                  vgId, pFileSet->fid, record.blockOffset, code != 0 ? tstrerror(code) : "invalid_block_payload");
        code = 0;
        tBlockDataClear(&blockData);
        continue;
      }

      if (keepSma && record.smaSize > 0) {
        code = tsdbDataFileReadBlockSma(reader, &record, columnDataAggArray);
        if (code != 0) {
          keepSma = false;
          tsdbWarn("vgId:%d fid:%d disable sma references during head-only rebuild after blockOffset:%" PRId64
                   " since %s",
                   vgId, pFileSet->fid, record.blockOffset, tstrerror(code));
          code = 0;
        }
      }

      code = TARRAY2_APPEND(recordArr, record);
      tBlockDataClear(&blockData);
      if (code != 0) {
        tBrinBlockDestroy(&brinBlock);
        goto _exit;
      }

      (*rebuiltBlocks)++;
    }

    tBrinBlockDestroy(&brinBlock);
    if (code != 0) {
      goto _exit;
    }
  }

  if (!keepSma) {
    for (int32_t i = 0; i < TARRAY2_SIZE(recordArr); ++i) {
      SBrinRecord *record = TARRAY2_GET_PTR(recordArr, i);
      SBrinRecord  adjusted = {0};

      tsdbRepairBuildHeadOnlyBrinRecord(record, false, &adjusted);
      *record = adjusted;
    }
  }

  *dropSma = hasReadableSma && !keepSma;

_exit:
  if (code == 0) {
    tsdbInfo("vgId:%d fid:%d collected %d head-only rebuild records, dropSma:%s", vgId, pFileSet->fid,
             *rebuiltBlocks, *dropSma ? "yes" : "no");
  } else {
    tsdbError("vgId:%d fid:%d failed to collect head-only rebuild records since %s, code:%d", vgId, pFileSet->fid,
              tstrerror(code), code);
  }
  TARRAY2_DESTROY(columnDataAggArray, NULL);
  tBlockDataDestroy(&blockData);
  tsdbDataFileReaderClose(&reader);
  return code;
}

static int32_t tsdbRepairWriteHeadOnlyCore(STFileSystem *pFS, const STFileSet *pFileSet,
                                           const TBrinRecordArray *recordArr, TFileOpArray *writerOps) {
  int32_t                  code = 0;
  int32_t                  vgId = TD_VID(pFS->tsdb->pVnode);
  int64_t                  headFileSize = 0;
  SDiskID                  diskId = {0};
  SDataFileRAWWriter      *writer = NULL;
  SBrinBlock               brinBlock = {0};
  TBrinBlkArray            brinBlkArray[1];
  SBuffer                  buffers[3];
  SHeadFooter              headFooter = {0};
  SVersionRange            range = {.minVer = VERSION_MAX, .maxVer = VERSION_MIN};
  SEncryptData            *pEncryptData = &(pFS->tsdb->pVnode->config.tsdbCfg.encryptData);
  SDataFileRAWWriterConfig writerConfig = {
      .tsdb = pFS->tsdb,
      .szPage = pFS->tsdb->pVnode->config.tsdbPageSize,
      .fid = pFileSet->fid,
      .cid = tsdbFSAllocEid(pFS),
  };

  tsdbInfo("vgId:%d fid:%d start writing head-only core, records:%d", vgId, pFileSet->fid, TARRAY2_SIZE(recordArr));

  code = tsdbAllocateDisk(pFS->tsdb, tsdbFTypeLabel(TSDB_FTYPE_HEAD),
                          tsdbFidLevel(pFileSet->fid, &pFS->tsdb->keepCfg, taosGetTimestampSec()), &diskId);
  if (code != 0) {
    tsdbError("vgId:%d fid:%d failed to allocate disk for head-only rebuild since %s, code:%d", vgId, pFileSet->fid,
              tstrerror(code), code);
    return code;
  }

  writerConfig.file = (STFile){
      .type = TSDB_FTYPE_HEAD,
      .did = diskId,
      .fid = pFileSet->fid,
      .cid = writerConfig.cid,
      .size = 0,
      .minVer = VERSION_MAX,
      .maxVer = VERSION_MIN,
  };

  TARRAY2_INIT(brinBlkArray);
  for (int32_t i = 0; i < 3; ++i) {
    tBufferInit(&buffers[i]);
  }

  code = tBrinBlockInit(&brinBlock);
  if (code != 0) {
    goto _exit;
  }

  code = tsdbDataFileRAWWriterOpen(&writerConfig, &writer);
  if (code != 0) {
    tsdbError("vgId:%d fid:%d failed to open head-only writer since %s, code:%d", vgId, pFileSet->fid, tstrerror(code),
              code);
    goto _exit;
  }

  int32_t flushThreshold = tsdbRepairResolveHeadOnlyBrinFlushThreshold(pFS->tsdb->pVnode->config.tsdbCfg.maxRows);
  for (int32_t i = 0; i < TARRAY2_SIZE(recordArr); ++i) {
    const SBrinRecord *record = TARRAY2_GET_PTR(recordArr, i);

    if (tsdbRepairShouldFlushHeadOnlyBrinBlock(brinBlock.numOfRecords, flushThreshold)) {
      code = tsdbFileWriteBrinBlock(writer->fd, &brinBlock, pFS->tsdb->pVnode->config.tsdbCfg.compression,
                                    &writer->file.size, brinBlkArray, buffers, &range, pEncryptData);
      if (code != 0) {
        goto _exit;
      }
    }

    code = tBrinBlockPut(&brinBlock, record);
    if (tsdbRepairShouldRetryHeadOnlyBrinPut(code, brinBlock.numOfRecords, flushThreshold)) {
      code = tsdbFileWriteBrinBlock(writer->fd, &brinBlock, pFS->tsdb->pVnode->config.tsdbCfg.compression,
                                    &writer->file.size, brinBlkArray, buffers, &range, pEncryptData);
      if (code != 0) {
        goto _exit;
      }

      code = tBrinBlockPut(&brinBlock, record);
    }

    if (code != 0) {
      goto _exit;
    }
  }

  code = tsdbFileWriteBrinBlock(writer->fd, &brinBlock, pFS->tsdb->pVnode->config.tsdbCfg.compression,
                                &writer->file.size, brinBlkArray, buffers, &range, pEncryptData);
  if (code != 0) {
    goto _exit;
  }

  code = tsdbFileWriteBrinBlk(writer->fd, brinBlkArray, headFooter.brinBlkPtr, &writer->file.size, pEncryptData);
  if (code != 0) {
    goto _exit;
  }

  code = tsdbFileWriteHeadFooter(writer->fd, &writer->file.size, &headFooter, pEncryptData);
  if (code != 0) {
    goto _exit;
  }

  if (range.minVer <= range.maxVer) {
    writer->file.minVer = range.minVer;
    writer->file.maxVer = range.maxVer;
  }

_exit:
  if (writer != NULL) {
    headFileSize = writer->file.size;
  }
  if (writer != NULL) {
    int32_t closeCode = tsdbDataFileRAWWriterClose(&writer, code != 0, code == 0 ? writerOps : NULL);
    if (closeCode != 0 && code == 0) {
      code = closeCode;
    }
  }
  if (code == 0) {
    tsdbInfo("vgId:%d fid:%d finished writing head-only core, brinBlocks:%d fileSize:%" PRId64 " writerOps:%d", vgId,
             pFileSet->fid, TARRAY2_SIZE(brinBlkArray), headFileSize, TARRAY2_SIZE(writerOps));
  } else {
    tsdbError("vgId:%d fid:%d failed to write head-only core since %s, code:%d", vgId, pFileSet->fid, tstrerror(code),
              code);
  }
  tBrinBlockDestroy(&brinBlock);
  TARRAY2_DESTROY(brinBlkArray, NULL);
  for (int32_t i = 0; i < 3; ++i) {
    tBufferDestroy(&buffers[i]);
  }
  return code;
}

static int32_t tsdbRepairRebuildCoreHeadOnly(STFileSystem *pFS, const STFileSet *pFileSet, TFileOpArray *writerOps,
                                             bool *dropSma, int32_t *rebuiltBlocks) {
  int32_t          code = 0;
  int32_t          vgId = TD_VID(pFS->tsdb->pVnode);
  TBrinRecordArray recordArr[1];

  TARRAY2_INIT(recordArr);

  code = tsdbRepairCollectHeadOnlyRecords(pFS, pFileSet, recordArr, dropSma, rebuiltBlocks);
  if (code != 0 || *rebuiltBlocks == 0) {
    if (code == 0) {
      tsdbWarn("vgId:%d fid:%d head-only rebuild found no valid records to preserve", vgId, pFileSet->fid);
    }
    goto _exit;
  }

  code = tsdbRepairWriteHeadOnlyCore(pFS, pFileSet, recordArr, writerOps);

_exit:
  if (code == 0 && *rebuiltBlocks > 0) {
    tsdbInfo("vgId:%d fid:%d head-only rebuild prepared successfully, rebuiltBlocks:%d dropSma:%s writerOps:%d", vgId,
             pFileSet->fid, *rebuiltBlocks, *dropSma ? "yes" : "no", TARRAY2_SIZE(writerOps));
  }
  TARRAY2_DESTROY(recordArr, NULL);
  return code;
}

static int32_t tsdbRepairApplyCoreResult(STFileSystem *pFS, const STFileSet *pFileSet, ETsdbRepairMode mode,
                                         const STsdbRepairCoreResult *result, TFileOpArray *opArr) {
  int32_t      code = 0;
  int32_t      vgId = TD_VID(pFS->tsdb->pVnode);
  int32_t      rebuiltBlocks = 0;
  bool         dropSma = false;
  TFileOpArray writerOps[1];

  TARRAY2_INIT(writerOps);

  if (result->action == TSDB_REPAIR_ACTION_KEEP) {
    tsdbDebug("vgId:%d fid:%d keep core group unchanged", vgId, pFileSet->fid);
    goto _exit;
  }

  if (result->action == TSDB_REPAIR_ACTION_DROP) {
    tsdbInfo("vgId:%d fid:%d drop core group, reason:%s", vgId, pFileSet->fid,
             result->reason == NULL ? "damaged_core_group" : result->reason);
    code = tsdbRepairAppendRemoveCoreOps(pFileSet, opArr);
    if (code != 0) {
      tsdbError("vgId:%d fid:%d failed to append remove ops for core group since %s, code:%d", vgId, pFileSet->fid,
                tstrerror(code), code);
    }
    goto _exit;
  }

  tsdbInfo("vgId:%d fid:%d rebuild core group, mode:%s keptBlocks:%d droppedBlocks:%d reason:%s", vgId, pFileSet->fid,
           tsdbRepairModeName(mode), result->keptBlocks, result->droppedBlocks,
           result->reason == NULL ? "damaged_core_group" : result->reason);

  if (mode == TSDB_REPAIR_MODE_HEAD_ONLY_REBUILD) {
    code = tsdbRepairRebuildCoreHeadOnly(pFS, pFileSet, writerOps, &dropSma, &rebuiltBlocks);
  } else {
    code = tsdbRepairRebuildCore(pFS, pFileSet, writerOps, &rebuiltBlocks);
  }
  if (code != 0) {
    goto _exit;
  }

  if (rebuiltBlocks == 0) {
    tsdbWarn("vgId:%d fid:%d rebuilt core group has no valid blocks left, fallback to drop", vgId, pFileSet->fid);
    code = tsdbRepairAppendRemoveCoreOps(pFileSet, opArr);
    if (code != 0) {
      tsdbError("vgId:%d fid:%d failed to append fallback remove ops for core group since %s, code:%d", vgId,
                pFileSet->fid, tstrerror(code), code);
    }
    goto _exit;
  }

  if (mode == TSDB_REPAIR_MODE_HEAD_ONLY_REBUILD) {
    code = tsdbRepairAppendHeadOnlyRebuildOps(pFileSet, dropSma, writerOps, opArr);
    if (code == 0) {
      tsdbInfo("vgId:%d fid:%d prepared head-only rebuild ops, rebuiltBlocks:%d dropSma:%s appendedWriterOps:%d", vgId,
               pFileSet->fid, rebuiltBlocks, dropSma ? "yes" : "no", TARRAY2_SIZE(writerOps));
    }
  } else {
    code = tsdbRepairAppendRemoveCoreOps(pFileSet, opArr);
    if (code != 0) {
      tsdbError("vgId:%d fid:%d failed to append remove ops before core rebuild commit since %s, code:%d", vgId,
                pFileSet->fid, tstrerror(code), code);
      goto _exit;
    }

    code = tsdbRepairAppendOps(opArr, writerOps);
  }
  if (code != 0) {
    tsdbError("vgId:%d fid:%d failed to append rebuilt core ops since %s, code:%d", vgId, pFileSet->fid,
              tstrerror(code), code);
    goto _exit;
  }

  tsdbDebug("vgId:%d fid:%d appended %d rebuilt core ops", vgId, pFileSet->fid, TARRAY2_SIZE(writerOps));

_exit:
  TARRAY2_DESTROY(writerOps, NULL);
  return code;
}

static int32_t tsdbDeepScanAndFixDataPart(STFileSystem *pFS, const STFileSet *pFileSet, ETsdbRepairMode mode,
                                          TFileOpArray *opArr) {
  int32_t               code = 0;
  STsdbRepairCoreResult result;

  tsdbRepairCoreResultInit(&result);

  code = tsdbRepairAnalyzeCore(pFS, pFileSet, &result);
  if (code != 0) {
    tsdbError("vgId:%d fid:%d failed to analyze core group since %s, code:%d", TD_VID(pFS->tsdb->pVnode), pFileSet->fid,
              tstrerror(code), code);
    return code;
  }

  return tsdbRepairApplyCoreResult(pFS, pFileSet, mode, &result, opArr);
}

static int32_t tsdbRepairAnalyzeSttFile(STFileSystem *pFS, const STFileSet *pFileSet, const STFileObj *pStt,
                                        STsdbRepairSttResult *result) {
  int32_t                code = 0;
  int32_t                vgId = TD_VID(pFS->tsdb->pVnode);
  SSttFileReader        *reader = NULL;
  SBlockData             blockData = {0};
  STombBlock             tombBlock = {0};
  bool                   tombBlockInit = false;
  const TSttBlkArray    *sttBlkArray = NULL;
  const TStatisBlkArray *statisBlkArray = NULL;
  const TTombBlkArray   *tombBlkArray = NULL;
  SSttFileReaderConfig   readerConfig = {
        .tsdb = pFS->tsdb,
        .szPage = pFS->tsdb->pVnode->config.tsdbPageSize,
        .file[0] = pStt->f[0],
  };

  if (pStt == NULL || tsdbRepairFileAffected(pStt)) {
    tsdbTrace("vgId:%d fid:%d skip stt deep scan because file is already affected or absent", vgId, pFileSet->fid);
    return 0;
  }

  tsdbDebug("vgId:%d fid:%d level:%d start analyzing stt file:%s", vgId, pFileSet->fid, pStt->f->stt->level,
            pStt->fname);

  code = tsdbSttFileReaderOpen(pStt->fname, &readerConfig, &reader);
  if (code != 0) {
    result->action = TSDB_REPAIR_ACTION_DROP;
    tsdbRepairSetReason(&result->reason, "open_stt_reader_failed");
    tsdbWarn("vgId:%d fid:%d level:%d failed to open stt file:%s, downgrade action to %s since %s", vgId, pFileSet->fid,
             pStt->f->stt->level, pStt->fname, tsdbRepairActionName(result->action), tstrerror(code));
    code = 0;
    goto _exit;
  }

  code = tsdbSttFileReadSttBlk(reader, &sttBlkArray);
  if (code != 0) {
    result->action = TSDB_REPAIR_ACTION_DROP;
    tsdbRepairSetReason(&result->reason, "read_stt_index_failed");
    tsdbWarn("vgId:%d fid:%d level:%d failed to read stt index from file:%s, downgrade action to %s since %s", vgId,
             pFileSet->fid, pStt->f->stt->level, pStt->fname, tsdbRepairActionName(result->action), tstrerror(code));
    code = 0;
    goto _exit;
  }

  code = tsdbSttFileReadStatisBlk(reader, &statisBlkArray);
  if (code != 0) {
    result->rewriteRequired = true;
    tsdbRepairSetReason(&result->reason, "read_statis_index_failed");
    tsdbWarn("vgId:%d fid:%d level:%d failed to read statis index from file:%s since %s, mark file for rebuild", vgId,
             pFileSet->fid, pStt->f->stt->level, pStt->fname, tstrerror(code));
    code = 0;
  }
  TAOS_UNUSED(statisBlkArray);

  for (int32_t i = 0; i < TARRAY2_SIZE(sttBlkArray); ++i) {
    const SSttBlk *pSttBlk = TARRAY2_GET_PTR(sttBlkArray, i);

    tBlockDataClear(&blockData);
    code = tsdbSttFileReadBlockData(reader, pSttBlk, &blockData);
    if (code != 0 || !tsdbRepairSttBlockLooksValid(&blockData)) {
      result->rewriteRequired = true;
      result->droppedDataBlocks++;
      tsdbRepairSetReason(&result->reason, code != 0 ? "read_stt_block_failed" : "invalid_stt_block");
      tsdbWarn("vgId:%d fid:%d level:%d drop damaged stt block from file:%s blockOffset:%" PRId64 " reason:%s", vgId,
               pFileSet->fid, pStt->f->stt->level, pStt->fname, pSttBlk->bInfo.offset,
               code != 0 ? tstrerror(code) : "invalid_block_payload");
      code = 0;
      tBlockDataClear(&blockData);
      continue;
    }

    result->keptDataBlocks++;
    tsdbTrace("vgId:%d fid:%d level:%d keep stt data block from file:%s blockOffset:%" PRId64, vgId, pFileSet->fid,
              pStt->f->stt->level, pStt->fname, pSttBlk->bInfo.offset);
    tBlockDataClear(&blockData);
  }

  code = tsdbSttFileReadTombBlk(reader, &tombBlkArray);
  if (code != 0) {
    result->rewriteRequired = true;
    result->droppedTombBlocks++;
    tsdbRepairSetReason(&result->reason, "read_tomb_index_failed");
    tsdbWarn("vgId:%d fid:%d level:%d failed to read tomb index from stt file:%s since %s, continue without tomb data",
             vgId, pFileSet->fid, pStt->f->stt->level, pStt->fname, tstrerror(code));
    code = 0;
    goto _exit;
  }

  tTombBlockInit(&tombBlock);
  tombBlockInit = true;

  for (int32_t i = 0; i < TARRAY2_SIZE(tombBlkArray); ++i) {
    const STombBlk *pTombBlk = TARRAY2_GET_PTR(tombBlkArray, i);

    code = tsdbSttFileReadTombBlock(reader, pTombBlk, &tombBlock);
    if (code != 0) {
      result->rewriteRequired = true;
      result->droppedTombBlocks++;
      tsdbRepairSetReason(&result->reason, "read_tomb_block_failed");
      tsdbWarn("vgId:%d fid:%d level:%d drop damaged tomb block from stt file:%s since %s", vgId, pFileSet->fid,
               pStt->f->stt->level, pStt->fname, tstrerror(code));
      code = 0;
      continue;
    }

    if (TOMB_BLOCK_SIZE(&tombBlock) > 0) {
      result->keptTombBlocks++;
      tsdbTrace("vgId:%d fid:%d level:%d keep tomb block from stt file:%s numRecords:%d", vgId, pFileSet->fid,
                pStt->f->stt->level, pStt->fname, TOMB_BLOCK_SIZE(&tombBlock));
    }
  }

_exit:
  if (code == 0) {
    tsdbRepairFinalizeSttResult(result);
    tsdbDebug(
        "vgId:%d fid:%d level:%d stt analysis finished, file:%s action:%s keptData:%d droppedData:%d "
        "keptTomb:%d droppedTomb:%d reason:%s",
        vgId, pFileSet->fid, pStt->f->stt->level, pStt->fname, tsdbRepairActionName(result->action),
        result->keptDataBlocks, result->droppedDataBlocks, result->keptTombBlocks, result->droppedTombBlocks,
        result->reason == NULL ? "healthy" : result->reason);
  }

  tBlockDataDestroy(&blockData);
  if (tombBlockInit) {
    tTombBlockDestroy(&tombBlock);
  }
  tsdbSttFileReaderClose(&reader);
  return code;
}

static int32_t tsdbRepairRebuildSttFile(STFileSystem *pFS, const STFileSet *pFileSet, const STFileObj *pStt,
                                        TFileOpArray *writerOps, int32_t *rebuiltDataBlocks,
                                        int32_t *rebuiltTombBlocks) {
  int32_t              code = 0;
  int32_t              vgId = TD_VID(pFS->tsdb->pVnode);
  SSttFileReader      *reader = NULL;
  SSttFileWriter      *writer = NULL;
  SBlockData           blockData = {0};
  STombBlock           tombBlock = {0};
  bool                 tombBlockInit = false;
  const TSttBlkArray  *sttBlkArray = NULL;
  const TTombBlkArray *tombBlkArray = NULL;
  SSttFileReaderConfig readerConfig = {
      .tsdb = pFS->tsdb,
      .szPage = pFS->tsdb->pVnode->config.tsdbPageSize,
      .file[0] = pStt->f[0],
  };
  SSttFileWriterConfig writerConfig = {
      .tsdb = pFS->tsdb,
      .maxRow = pFS->tsdb->pVnode->config.tsdbCfg.maxRows,
      .szPage = pFS->tsdb->pVnode->config.tsdbPageSize,
      .cmprAlg = pFS->tsdb->pVnode->config.tsdbCfg.compression,
      .compactVersion = INT64_MAX,
      .expLevel = tsdbFidLevel(pFileSet->fid, &pFS->tsdb->keepCfg, taosGetTimestampSec()),
      .fid = pFileSet->fid,
      .cid = tsdbFSAllocEid(pFS),
      .level = pStt->f->stt->level,
  };

  code = tsdbSttFileReaderOpen(pStt->fname, &readerConfig, &reader);
  if (code != 0) {
    tsdbWarn("vgId:%d fid:%d level:%d failed to reopen stt file:%s for rebuild since %s, fallback to drop", vgId,
             pFileSet->fid, pStt->f->stt->level, pStt->fname, tstrerror(code));
    code = 0;
    goto _exit;
  }

  code = tsdbSttFileReadSttBlk(reader, &sttBlkArray);
  if (code != 0) {
    tsdbWarn("vgId:%d fid:%d level:%d failed to reread stt index from file:%s for rebuild since %s, fallback to drop",
             vgId, pFileSet->fid, pStt->f->stt->level, pStt->fname, tstrerror(code));
    code = 0;
    goto _exit;
  }

  code = tsdbSttFileWriterOpen(&writerConfig, &writer);
  if (code != 0) {
    tsdbError("vgId:%d fid:%d level:%d failed to open stt writer for file:%s since %s, code:%d", vgId, pFileSet->fid,
              pStt->f->stt->level, pStt->fname, tstrerror(code), code);
    goto _exit;
  }

  for (int32_t i = 0; i < TARRAY2_SIZE(sttBlkArray); ++i) {
    const SSttBlk *pSttBlk = TARRAY2_GET_PTR(sttBlkArray, i);

    tBlockDataClear(&blockData);
    code = tsdbSttFileReadBlockData(reader, pSttBlk, &blockData);
    if (code != 0 || !tsdbRepairSttBlockLooksValid(&blockData)) {
      tsdbTrace("vgId:%d fid:%d level:%d skip invalid stt data block during rebuild, file:%s blockOffset:%" PRId64
                " reason:%s",
                vgId, pFileSet->fid, pStt->f->stt->level, pStt->fname, pSttBlk->bInfo.offset,
                code != 0 ? tstrerror(code) : "invalid_block_payload");
      code = 0;
      tBlockDataClear(&blockData);
      continue;
    }

    code = tsdbSttFileWriteBlockData(writer, &blockData);
    tBlockDataClear(&blockData);
    if (code != 0) {
      tsdbError("vgId:%d fid:%d level:%d failed to write rebuilt stt data block for file:%s since %s, code:%d", vgId,
                pFileSet->fid, pStt->f->stt->level, pStt->fname, tstrerror(code), code);
      goto _exit;
    }

    (*rebuiltDataBlocks)++;
    tsdbTrace("vgId:%d fid:%d level:%d rebuilt stt data block for file:%s total:%d", vgId, pFileSet->fid,
              pStt->f->stt->level, pStt->fname, *rebuiltDataBlocks);
  }

  code = tsdbSttFileReadTombBlk(reader, &tombBlkArray);
  if (code != 0) {
    tsdbWarn(
        "vgId:%d fid:%d level:%d failed to reread tomb index from file:%s during rebuild since %s, continue "
        "without tomb data",
        vgId, pFileSet->fid, pStt->f->stt->level, pStt->fname, tstrerror(code));
    code = 0;
    goto _exit;
  }

  tTombBlockInit(&tombBlock);
  tombBlockInit = true;

  for (int32_t i = 0; i < TARRAY2_SIZE(tombBlkArray); ++i) {
    const STombBlk *pTombBlk = TARRAY2_GET_PTR(tombBlkArray, i);

    code = tsdbSttFileReadTombBlock(reader, pTombBlk, &tombBlock);
    if (code != 0) {
      tsdbTrace("vgId:%d fid:%d level:%d skip invalid tomb block during rebuild, file:%s reason:%s", vgId,
                pFileSet->fid, pStt->f->stt->level, pStt->fname, tstrerror(code));
      code = 0;
      continue;
    }

    for (int32_t j = 0; j < TOMB_BLOCK_SIZE(&tombBlock); ++j) {
      STombRecord record = {0};

      code = tTombBlockGet(&tombBlock, j, &record);
      if (code != 0) {
        tsdbTrace("vgId:%d fid:%d level:%d stop scanning invalid tomb record during rebuild for file:%s since %s", vgId,
                  pFileSet->fid, pStt->f->stt->level, pStt->fname, tstrerror(code));
        code = 0;
        break;
      }

      code = tsdbSttFileWriteTombRecord(writer, &record);
      if (code != 0) {
        tsdbError("vgId:%d fid:%d level:%d failed to write rebuilt tomb record for file:%s since %s, code:%d", vgId,
                  pFileSet->fid, pStt->f->stt->level, pStt->fname, tstrerror(code), code);
        goto _exit;
      }

      if (j == 0) {
        (*rebuiltTombBlocks)++;
      }
    }
  }

_exit:
  tBlockDataDestroy(&blockData);
  if (writer != NULL) {
    int8_t  abort = tsdbRepairShouldAbortSttWriterClose(*rebuiltDataBlocks, *rebuiltTombBlocks);
    int32_t closeCode = tsdbSttFileWriterClose(&writer, abort, abort ? NULL : writerOps);
    if (closeCode != 0) {
      tsdbError("vgId:%d fid:%d level:%d failed to close rebuilt stt writer for file:%s since %s, code:%d", vgId,
                pFileSet->fid, pStt->f->stt->level, pStt->fname, tstrerror(closeCode), closeCode);
      code = code == 0 ? closeCode : code;
    }
  }
  // tsdbSttFileWriterClose(&writer, true, NULL);
  if (tombBlockInit) {
    tTombBlockDestroy(&tombBlock);
  }
  tsdbSttFileReaderClose(&reader);
  return code;
}

static int32_t tsdbRepairApplySttResult(STFileSystem *pFS, const STFileSet *pFileSet, const STFileObj *pStt,
                                        const STsdbRepairSttResult *result, TFileOpArray *opArr) {
  int32_t      code = 0;
  int32_t      vgId = TD_VID(pFS->tsdb->pVnode);
  int32_t      rebuiltDataBlocks = 0;
  int32_t      rebuiltTombBlocks = 0;
  TFileOpArray writerOps[1];

  TARRAY2_INIT(writerOps);

  if (result->action == TSDB_REPAIR_ACTION_KEEP) {
    tsdbDebug("vgId:%d fid:%d level:%d keep stt file unchanged:%s", vgId, pFileSet->fid, pStt->f->stt->level,
              pStt->fname);
    goto _exit;
  }

  if (result->action == TSDB_REPAIR_ACTION_DROP) {
    tsdbInfo("vgId:%d fid:%d level:%d drop stt file:%s reason:%s", vgId, pFileSet->fid, pStt->f->stt->level,
             pStt->fname, result->reason == NULL ? "damaged_stt_file" : result->reason);
    code = tsdbRepairAppendRemoveFileObjOp(opArr, pFileSet->fid, pStt);
    if (code != 0) {
      tsdbError("vgId:%d fid:%d level:%d failed to append remove op for stt file:%s since %s, code:%d", vgId,
                pFileSet->fid, pStt->f->stt->level, pStt->fname, tstrerror(code), code);
    }
    goto _exit;
  }

  tsdbInfo(
      "vgId:%d fid:%d level:%d rebuild stt file:%s keptData:%d droppedData:%d keptTomb:%d droppedTomb:%d "
      "reason:%s",
      vgId, pFileSet->fid, pStt->f->stt->level, pStt->fname, result->keptDataBlocks, result->droppedDataBlocks,
      result->keptTombBlocks, result->droppedTombBlocks, result->reason == NULL ? "damaged_stt_file" : result->reason);

  code = tsdbRepairRebuildSttFile(pFS, pFileSet, pStt, writerOps, &rebuiltDataBlocks, &rebuiltTombBlocks);
  if (code != 0) {
    goto _exit;
  }

  if (rebuiltDataBlocks == 0 && rebuiltTombBlocks == 0) {
    tsdbWarn("vgId:%d fid:%d level:%d rebuilt stt file:%s has no valid content left, fallback to drop", vgId,
             pFileSet->fid, pStt->f->stt->level, pStt->fname);
    code = tsdbRepairAppendRemoveFileObjOp(opArr, pFileSet->fid, pStt);
    if (code != 0) {
      tsdbError("vgId:%d fid:%d level:%d failed to append fallback remove op for stt file:%s since %s, code:%d", vgId,
                pFileSet->fid, pStt->f->stt->level, pStt->fname, tstrerror(code), code);
    }
    goto _exit;
  }

  code = tsdbRepairAppendRemoveFileObjOp(opArr, pFileSet->fid, pStt);
  if (code != 0) {
    tsdbError(
        "vgId:%d fid:%d level:%d failed to append remove op before stt rebuild commit for file:%s since %s, "
        "code:%d",
        vgId, pFileSet->fid, pStt->f->stt->level, pStt->fname, tstrerror(code), code);
    goto _exit;
  }

  code = tsdbRepairAppendOps(opArr, writerOps);
  if (code != 0) {
    tsdbError("vgId:%d fid:%d level:%d failed to append rebuilt stt ops for file:%s since %s, code:%d", vgId,
              pFileSet->fid, pStt->f->stt->level, pStt->fname, tstrerror(code), code);
    goto _exit;
  }

  tsdbDebug("vgId:%d fid:%d level:%d appended %d rebuilt stt ops for file:%s", vgId, pFileSet->fid, pStt->f->stt->level,
            TARRAY2_SIZE(writerOps), pStt->fname);

_exit:
  TARRAY2_DESTROY(writerOps, NULL);
  return code;
}

static int32_t tsdbDeepScanAndFixSttPart(STFileSystem *pFS, const STFileSet *pFileSet, TFileOpArray *opArr) {
  int32_t  code = 0;
  SSttLvl *sttLevel = NULL;

  TARRAY2_FOREACH(pFileSet->lvlArr, sttLevel) {
    STFileObj *pStt = NULL;

    TARRAY2_FOREACH(sttLevel->fobjArr, pStt) {
      STsdbRepairSttResult result;

      tsdbRepairSttResultInit(&result);

      code = tsdbRepairAnalyzeSttFile(pFS, pFileSet, pStt, &result);
      if (code != 0) {
        tsdbError("vgId:%d fid:%d level:%d failed to analyze stt file:%s since %s, code:%d", TD_VID(pFS->tsdb->pVnode),
                  pFileSet->fid, pStt->f->stt->level, pStt->fname, tstrerror(code), code);
        return code;
      }

      code = tsdbRepairApplySttResult(pFS, pFileSet, pStt, &result, opArr);
      if (code != 0) {
        return code;
      }
    }
  }

  return 0;
}

static int32_t tsdbForceRepairFileSetDeepScanAndFix(STFileSystem *pFS, const STFileSet *pFileSet, ETsdbRepairMode mode,
                                                    TFileOpArray *opArr) {
  int32_t code = 0;

  code = tsdbDeepScanAndFixDataPart(pFS, pFileSet, mode, opArr);
  if (code != 0) {
    return code;
  }

  return tsdbDeepScanAndFixSttPart(pFS, pFileSet, opArr);
}

static int32_t tsdbForceRepairFileSet(STFileSystem *pFS, const STFileSet *pFileSet, TFileOpArray *opArr,
                                      bool *hasChange) {
  int32_t           code = 0;
  int32_t           vgId = TD_VID(pFS->tsdb->pVnode);
  EDmRepairStrategy strategy = tsdbRepairGetStrategyForFileSet(vgId, pFileSet->fid);
  ETsdbRepairMode   mode = (ETsdbRepairMode)tsdbRepairResolveMode(strategy);
  TFileOpArray      fileSetOps[1];

  TARRAY2_INIT(fileSetOps);

  tsdbInfo("vgId:%d fid:%d start force repair for fileset, strategy:%s mode:%s", vgId, pFileSet->fid,
           tsdbRepairStrategyName(strategy), tsdbRepairModeName(mode));

  code = tsdbForceRepairFileSetBadFiles(pFS, pFileSet, fileSetOps);
  if (code != 0) {
    tsdbError("vgId:%d fid:%d failed during bad-file detection since %s, code:%d", vgId, pFileSet->fid, tstrerror(code),
              code);
    goto _exit;
  }

  if (mode != TSDB_REPAIR_MODE_DROP_INVALID_ONLY) {
    code = tsdbForceRepairFileSetDeepScanAndFix(pFS, pFileSet, mode, fileSetOps);
    if (code != 0) {
      tsdbError("vgId:%d fid:%d failed during deep scan/apply since %s, code:%d", vgId, pFileSet->fid, tstrerror(code),
                code);
      goto _exit;
    }
  } else {
    tsdbDebug("vgId:%d fid:%d skip deep scan because strategy:%s only drops invalid files", vgId, pFileSet->fid,
              tsdbRepairStrategyName(strategy));
  }

  if (TARRAY2_SIZE(fileSetOps) == 0) {
    tsdbInfo("vgId:%d fid:%d fileset repair finished with no change", vgId, pFileSet->fid);
    goto _exit;
  }

  code = tsdbRepairAppendOps(opArr, fileSetOps);
  if (code != 0) {
    tsdbError("vgId:%d fid:%d failed to append fileset ops since %s, code:%d", vgId, pFileSet->fid, tstrerror(code),
              code);
    goto _exit;
  }

  *hasChange = true;
  tsdbInfo("vgId:%d fid:%d fileset repair appended %d ops", vgId, pFileSet->fid, TARRAY2_SIZE(fileSetOps));

_exit:
  TARRAY2_DESTROY(fileSetOps, NULL);
  return code;
}

static int32_t tsdbForceRepairCommitChange(STFileSystem *pFS, const TFileOpArray *opArr) {
  int32_t code = 0;
  STsdb  *pTsdb = pFS->tsdb;

  if (TARRAY2_SIZE(opArr) == 0) {
    tsdbInfo("vgId:%d skip repair commit because there are no file ops", TD_VID(pTsdb->pVnode));
    return 0;
  }

  tsdbInfo("vgId:%d commit %d repair file ops", TD_VID(pTsdb->pVnode), TARRAY2_SIZE(opArr));

  code = tsdbFSEditBegin(pFS, opArr, TSDB_FEDIT_FORCE_REPAIR);
  if (code != 0) {
    tsdbError("vgId:%d failed to begin repair fs edit since %s, code:%d", TD_VID(pTsdb->pVnode), tstrerror(code), code);
    return code;
  }

  (void)taosThreadMutexLock(&pTsdb->mutex);
  code = tsdbFSEditCommit(pFS);
  if (code != 0) {
    int32_t abortCode = 0;

    (void)taosThreadMutexUnlock(&pTsdb->mutex);
    tsdbError("vgId:%d failed to commit repair fs edit since %s, code:%d", TD_VID(pTsdb->pVnode), tstrerror(code),
              code);

    abortCode = tsdbFSEditAbort(pFS);
    if (abortCode != 0) {
      tsdbError("vgId:%d failed to abort repair fs edit after commit failure since %s, code:%d", TD_VID(pTsdb->pVnode),
                tstrerror(abortCode), abortCode);
    } else {
      tsdbWarn("vgId:%d aborted repair fs edit after commit failure", TD_VID(pTsdb->pVnode));
    }
    return code;
  }
  (void)taosThreadMutexUnlock(&pTsdb->mutex);

  tsdbInfo("vgId:%d committed repair fs edit successfully", TD_VID(pTsdb->pVnode));
  return 0;
}

int32_t tsdbForceRepair(STFileSystem *fs) {
  int32_t      code = 0;
  int32_t      vgId = TD_VID(fs->tsdb->pVnode);
  bool         hasChange = false;
  TFileOpArray opArr[1];

  TARRAY2_INIT(opArr);

  tsdbInfo("vgId:%d start force repair for %d filesets", vgId, TARRAY2_SIZE(fs->fSetArr));

  STFileSet *pFileSet = NULL;
  TARRAY2_FOREACH(fs->fSetArr, pFileSet) {
    if (!tsdbRepairShouldProcessFileSet(vgId, pFileSet->fid)) {
      tsdbDebug("vgId:%d fid:%d skip fileset because it is not part of the explicit repair target set", vgId,
                pFileSet->fid);
      continue;
    }

    code = tsdbForceRepairFileSet(fs, pFileSet, opArr, &hasChange);
    if (code != 0) {
      tsdbError("vgId:%d failed to force repair fileset, fid:%d since %s, code:%d", vgId, pFileSet->fid,
                tstrerror(code), code);
      goto _exit;
    }
  }

  if (hasChange) {
    code = tsdbForceRepairCommitChange(fs, opArr);
    if (code != 0) {
      goto _exit;
    }
    tsdbInfo("vgId:%d force repair completed with %d file ops", vgId, TARRAY2_SIZE(opArr));
  } else {
    tsdbInfo("vgId:%d force repair completed with no file changes", vgId);
  }

_exit:
  TARRAY2_DESTROY(opArr, NULL);
  if (code != 0) {
    return code;
  }
  return 0;
}
