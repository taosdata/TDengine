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

#include "tsdb.h"

extern int32_t tsdbUpdateTableSchema(SMeta *pMeta, int64_t suid, int64_t uid, SSkmInfo *pSkmInfo);
extern int32_t tsdbWriteDataBlock(SDataFWriter *pWriter, SBlockData *pBlockData, SMapData *mDataBlk, int8_t cmprAlg);
extern int32_t tsdbWriteSttBlock(SDataFWriter *pWriter, SBlockData *pBlockData, SArray *aSttBlk, int8_t cmprAlg);

typedef struct {
  STsdb  *pTsdb;
  int64_t commitID;
  int8_t  cmprAlg;
  int32_t maxRows;
  int32_t minRows;

  STsdbFS fs;

  int32_t  fid;
  TABLEID  tbid;
  SSkmInfo tbSkm;

  // Tombstone
  SDelFReader *pDelFReader;
  SArray      *aDelIdx;   // SArray<SDelIdx>
  SArray      *aDelData;  // SArray<SDelData>
  SArray      *aSkyLine;  // SArray<TSDBKEY>
  int32_t      iDelIdx;
  int32_t      iSkyLine;
  TSDBKEY     *pDKey;
  TSDBKEY      dKey;

  // Reader
  SDataFReader   *pReader;
  STsdbDataIter2 *iterList;  // list of iterators
  STsdbDataIter2 *pIter;
  SRBTree         rbt;

  // Writer
  SDataFWriter *pWriter;
  SArray       *aBlockIdx;  // SArray<SBlockIdx>
  SMapData      mDataBlk;   // SMapData<SDataBlk>
  SArray       *aSttBlk;    // SArray<SSttBlk>
  SBlockData    bData;
  SBlockData    sData;
} STsdbMerger;

static int32_t tsdbCommitMerge(STsdbMerger *pMerger) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pMerger->pTsdb;

  code = tsdbFSPrepareCommit(pTsdb, &pMerger->fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  taosThreadRwlockWrlock(&pTsdb->rwLock);

  code = tsdbFSCommit(pTsdb, pMerger->fs.type);
  if (code) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  taosThreadRwlockUnlock(&pTsdb->rwLock);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbAbortMerge(STsdbMerger *pMerger) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pMerger->pTsdb;
  code = tsdbFSRollback(pTsdb, VND_TASK_MERGE);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbMergeWriteTableDataStart(STsdbMerger *pMerger, TABLEID *pId) {
  int32_t code = 0;
  int32_t lino = 0;

  pMerger->tbid = *pId;

  // tombstone
  for (;;) {
    if (pMerger->iDelIdx >= taosArrayGetSize(pMerger->aDelIdx)) {
      pMerger->pDKey = NULL;
      break;
    }

    SDelIdx *pDelIdx = (SDelIdx *)taosArrayGet(pMerger->aDelIdx, pMerger->iDelIdx);
    int32_t  c = tTABLEIDCmprFn(pDelIdx, &pMerger->tbid);
    if (c < 0) {
      pMerger->iDelIdx++;
    } else if (c == 0) {
      pMerger->iDelIdx++;

      code = tsdbReadDelData(pMerger->pDelFReader, pDelIdx, pMerger->aDelData);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbBuildDeleteSkyline(pMerger->aDelData, 0, taosArrayGetSize(pMerger->aDelData) - 1,
                                    pMerger->aSkyLine);
      TSDB_CHECK_CODE(code, lino, _exit);

      pMerger->iSkyLine = 0;
      if (pMerger->iSkyLine < taosArrayGetSize(pMerger->aSkyLine)) {
        TSDBKEY *pKey = (TSDBKEY *)taosArrayGet(pMerger->aSkyLine, pMerger->iSkyLine);

        pMerger->dKey.version = 0;
        pMerger->dKey.ts = pKey->ts;
        pMerger->pDKey = &pMerger->dKey;
      } else {
        pMerger->pDKey = NULL;
      }
      break;
    } else {
      pMerger->pDKey = NULL;
      break;
    }
  }

  // writer
  code = tsdbUpdateTableSchema(pMerger->pTsdb->pVnode->pMeta, pId->suid, pId->uid, &pMerger->tbSkm);
  TSDB_CHECK_CODE(code, lino, _exit);

  tMapDataReset(&pMerger->mDataBlk);

  code = tBlockDataInit(&pMerger->bData, pId, pMerger->tbSkm.pTSchema, NULL, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (!TABLE_SAME_SCHEMA(pMerger->sData.suid, pMerger->sData.uid, pId->suid, pId->uid)) {
    if (pMerger->sData.nRow > 0) {
      code = tsdbWriteSttBlock(pMerger->pWriter, &pMerger->sData, pMerger->aSttBlk, pMerger->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    TABLEID tbid = {.suid = pId->suid, .uid = pId->suid ? 0 : pId->uid};
    code = tBlockDataInit(&pMerger->sData, &tbid, pMerger->tbSkm.pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pMerger->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, suid:%" PRId64 " uid:%" PRId64, TD_VID(pMerger->pTsdb->pVnode), __func__, pId->suid,
              pId->uid);
  }
  return code;
}

static int32_t tsdbMergeWriteTableDataEnd(STsdbMerger *pMerger) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pMerger->bData.nRow > 0) {
    if (pMerger->bData.nRow < pMerger->minRows) {
      for (int32_t iRow = 0; iRow < pMerger->bData.nRow; iRow++) {
        code = tBlockDataAppendRow(&pMerger->sData, &tsdbRowFromBlockData(&pMerger->bData, iRow), NULL,
                                   pMerger->tbid.uid);
        TSDB_CHECK_CODE(code, lino, _exit);

        if (pMerger->sData.nRow >= pMerger->maxRows) {
          code = tsdbWriteSttBlock(pMerger->pWriter, &pMerger->sData, pMerger->aSttBlk, pMerger->cmprAlg);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }
      tBlockDataClear(&pMerger->bData);
    } else {
      code = tsdbWriteDataBlock(pMerger->pWriter, &pMerger->bData, &pMerger->mDataBlk, pMerger->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (pMerger->mDataBlk.nItem > 0) {
    SBlockIdx *pBlockIdx = (SBlockIdx *)taosArrayReserve(pMerger->aBlockIdx, 1);
    if (pBlockIdx == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    pBlockIdx->suid = pMerger->tbid.suid;
    pBlockIdx->uid = pMerger->tbid.uid;

    code = tsdbWriteDataBlk(pMerger->pWriter, &pMerger->mDataBlk, pBlockIdx);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pMerger->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, suid:%" PRId64 " uid:%" PRId64, TD_VID(pMerger->pTsdb->pVnode), __func__,
              pMerger->tbid.suid, pMerger->tbid.uid);
  }
  return code;
}

static bool tsdbMergeRowIsDeleted(STsdbMerger *pMerger, TSDBROW *pRow) {
  TSDBKEY  tKey = TSDBROW_KEY(pRow);
  TSDBKEY *aKey = (TSDBKEY *)TARRAY_DATA(pMerger->aSkyLine);
  int32_t  nKey = TARRAY_SIZE(pMerger->aSkyLine);

  if (tKey.ts > pMerger->pDKey->ts) {
    do {
      pMerger->pDKey->version = aKey[pMerger->iSkyLine].version;
      pMerger->iSkyLine++;
      if (pMerger->iSkyLine < nKey) {
        pMerger->dKey.ts = aKey[pMerger->iSkyLine].ts;
      } else {
        if (pMerger->pDKey->version == 0) {
          pMerger->pDKey = NULL;
          return false;
        } else {
          pMerger->pDKey->ts = INT64_MAX;
        }
      }
    } while (tKey.ts > pMerger->pDKey->ts);
  }

  if (tKey.ts < pMerger->pDKey->ts) {
    if (tKey.version > pMerger->pDKey->version) {
      return false;
    } else {
      return true;
    }
  } else if (tKey.ts == pMerger->pDKey->ts) {
    ASSERT(pMerger->iSkyLine < nKey);
    if (tKey.version > TMAX(pMerger->pDKey->version, aKey[pMerger->iSkyLine].version)) {
      return false;
    } else {
      return true;
    }
  }

  return false;
}

static int32_t tsdbMergeWriteTableData(STsdbMerger *pMerger, SRowInfo *pRowInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  // start a new table data write if need
  if (pRowInfo == NULL || pRowInfo->uid != pMerger->tbid.uid) {
    if (pMerger->tbid.uid) {
      code = tsdbMergeWriteTableDataEnd(pMerger);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (pRowInfo == NULL) {
      if (pMerger->sData.nRow > 0) {
        code = tsdbWriteSttBlock(pMerger->pWriter, &pMerger->sData, pMerger->aSttBlk, pMerger->cmprAlg);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      return code;
    }

    code = tsdbMergeWriteTableDataStart(pMerger, (TABLEID *)pRowInfo);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // check if row is deleted
  if (pMerger->pDKey && tsdbMergeRowIsDeleted(pMerger, &pRowInfo->row)) goto _exit;

  if (tBlockDataTryUpsertRow(&pMerger->bData, &pRowInfo->row, pRowInfo->uid) > pMerger->maxRows) {
    code = tsdbWriteDataBlock(pMerger->pWriter, &pMerger->bData, &pMerger->mDataBlk, pMerger->cmprAlg);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataUpsertRow(&pMerger->bData, &pRowInfo->row, NULL, pRowInfo->uid);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pMerger->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  } else if (pRowInfo) {
    tsdbTrace("vgId:%d %s done, suid:%" PRId64 " uid:%" PRId64 " ts:%" PRId64 " version:%" PRId64,
              TD_VID(pMerger->pTsdb->pVnode), __func__, pRowInfo->suid, pRowInfo->uid, TSDBROW_TS(&pRowInfo->row),
              TSDBROW_VERSION(&pRowInfo->row));
  }
  return code;
}

static bool tsdbMergeTableIsDropped(STsdbMerger *pMerger) {
  SMetaInfo info;

  if (pMerger->pIter->rowInfo.uid == pMerger->tbid.uid) return false;
  if (metaGetInfo(pMerger->pTsdb->pVnode->pMeta, pMerger->pIter->rowInfo.uid, &info, NULL)) {
    return true;
  }
  return false;
}
static int32_t tsdbMergeNextRow(STsdbMerger *pMerger, SRowInfo **ppRowInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  for (;;) {
    if (pMerger->pIter) {
      code = tsdbDataIterNext2(pMerger->pIter, NULL);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (pMerger->pIter->rowInfo.suid == 0 && pMerger->pIter->rowInfo.uid == 0) {
        pMerger->pIter = NULL;
      } else {
        SRBTreeNode *pNode = tRBTreeMin(&pMerger->rbt);
        if (pNode) {
          int32_t c = tsdbDataIterCmprFn(&pMerger->pIter->rbtn, pNode);
          if (c > 0) {
            tRBTreePut(&pMerger->rbt, &pMerger->pIter->rbtn);
            pMerger->pIter = NULL;
          } else if (c == 0) {
            ASSERT(0);
          }
        }
      }
    }

    if (pMerger->pIter == NULL) {
      SRBTreeNode *pNode = tRBTreeDropMin(&pMerger->rbt);
      if (pNode) {
        pMerger->pIter = TSDB_RBTN_TO_DATA_ITER(pNode);
      }
    }

    if (pMerger->pIter) {
      if (tsdbMergeTableIsDropped(pMerger)) {
        TABLEID tbid = {.suid = pMerger->pIter->rowInfo.suid, .uid = pMerger->pIter->rowInfo.uid};
        tRBTreeClear(&pMerger->rbt);
        for (pMerger->pIter = pMerger->iterList; pMerger->pIter; pMerger->pIter = pMerger->pIter->next) {
          code = tsdbDataIterNext2(pMerger->pIter,
                                   &(STsdbFilterInfo){.flag = TSDB_FILTER_FLAG_BY_TABLEID, .tbid = tbid});
          TSDB_CHECK_CODE(code, lino, _exit);

          if (pMerger->pIter->rowInfo.suid || pMerger->pIter->rowInfo.uid) {
            tRBTreePut(&pMerger->rbt, &pMerger->pIter->rbtn);
          }
        }
      } else {
        *ppRowInfo = &pMerger->pIter->rowInfo;
        break;
      }
    } else {
      *ppRowInfo = NULL;
      break;
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pMerger->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbMergeFileSetStart(STsdbMerger *pMerger, SDFileSet *pSet) {
  int32_t code = 0;
  int32_t lino = 0;

  pMerger->fid = pSet->fid;
  pMerger->tbid = (TABLEID){0};

  /* tombstone */
  pMerger->iDelIdx = 0;

  /* reader */
  code = tsdbDataFReaderOpen(&pMerger->pReader, pMerger->pTsdb, pSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbOpenDataFileDataIter(pMerger->pReader, &pMerger->pIter);
  TSDB_CHECK_CODE(code, lino, _exit);

  tRBTreeCreate(&pMerger->rbt, tsdbDataIterCmprFn);
  if (pMerger->pIter) {
    pMerger->pIter->next = pMerger->iterList;
    pMerger->iterList = pMerger->pIter;

    code = tsdbDataIterNext2(pMerger->pIter, NULL);
    TSDB_CHECK_CODE(code, lino, _exit);

    ASSERT(pMerger->pIter->rowInfo.suid || pMerger->pIter->rowInfo.uid);
    tRBTreePut(&pMerger->rbt, &pMerger->pIter->rbtn);
  }

  for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
    code = tsdbOpenSttFileDataIter(pMerger->pReader, iStt, &pMerger->pIter);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pMerger->pIter) {
      pMerger->pIter->next = pMerger->iterList;
      pMerger->iterList = pMerger->pIter;

      code = tsdbDataIterNext2(pMerger->pIter, NULL);
      TSDB_CHECK_CODE(code, lino, _exit);

      ASSERT(pMerger->pIter->rowInfo.suid || pMerger->pIter->rowInfo.uid);
      tRBTreePut(&pMerger->rbt, &pMerger->pIter->rbtn);
    }
  }
  pMerger->pIter = NULL;

  /* writer */
  code = tsdbDataFWriterOpen(&pMerger->pWriter, pMerger->pTsdb,
                             &(SDFileSet){.fid = pMerger->fid,
                                          .diskId = pSet->diskId,
                                          .pHeadF = &(SHeadFile){.commitID = pMerger->commitID},
                                          .pDataF = &(SDataFile){.commitID = pMerger->commitID},
                                          .pSmaF = &(SSmaFile){.commitID = pMerger->commitID},
                                          .nSttF = 1,
                                          .aSttF = {&(SSttFile){.commitID = pMerger->commitID}}},
                             false);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pMerger->aBlockIdx) {
    taosArrayClear(pMerger->aBlockIdx);
  } else if ((pMerger->aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tMapDataReset(&pMerger->mDataBlk);

  if (pMerger->aSttBlk) {
    taosArrayClear(pMerger->aSttBlk);
  } else if ((pMerger->aSttBlk = taosArrayInit(0, sizeof(SSttBlk))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tBlockDataReset(&pMerger->bData);
  tBlockDataReset(&pMerger->sData);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d", TD_VID(pMerger->pTsdb->pVnode), __func__, lino,
              tstrerror(code), pMerger->fid);
  } else {
    tsdbInfo("vgId:%d %s done, fid:%d", TD_VID(pMerger->pTsdb->pVnode), __func__, pMerger->fid);
  }
  return code;
}

static int32_t tsdbMergeFileSetEnd(STsdbMerger *pMerger) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(pMerger->bData.nRow == 0);
  ASSERT(pMerger->sData.nRow == 0);

  /* update files */
  code = tsdbWriteSttBlk(pMerger->pWriter, pMerger->aSttBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbWriteBlockIdx(pMerger->pWriter, pMerger->aBlockIdx);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbUpdateDFileSetHeader(pMerger->pWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFSUpsertFSet(&pMerger->fs, &pMerger->pWriter->wSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDataFWriterClose(&pMerger->pWriter, 1);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDataFReaderClose(&pMerger->pReader);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* do clear */
  while ((pMerger->pIter = pMerger->iterList) != NULL) {
    pMerger->iterList = pMerger->pIter->next;
    tsdbCloseDataIter2(pMerger->pIter);
  }

  tBlockDataReset(&pMerger->bData);
  tBlockDataReset(&pMerger->sData);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d", TD_VID(pMerger->pTsdb->pVnode), __func__, lino,
              tstrerror(code), pMerger->fid);
  } else {
    tsdbInfo("vgId:%d %s done, fid:%d", TD_VID(pMerger->pTsdb->pVnode), __func__, pMerger->fid);
  }
  return code;
}

static int32_t tsdbMergeFileSet(STsdbMerger *pMerger, SDFileSet *pSet) {
  int32_t code = 0;
  int32_t lino = 0;

  // start merge
  code = tsdbMergeFileSetStart(pMerger, pSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  // do merge, end with a NULL row
  SRowInfo *pRowInfo;
  do {
    code = tsdbMergeNextRow(pMerger, &pRowInfo);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbMergeWriteTableData(pMerger, pRowInfo);
    TSDB_CHECK_CODE(code, lino, _exit);
  } while (pRowInfo);

  // end merge
  code = tsdbMergeFileSetEnd(pMerger);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d", TD_VID(pMerger->pTsdb->pVnode), __func__, lino,
              tstrerror(code), pMerger->fid);
    if (pMerger->pWriter) tsdbDataFWriterClose(&pMerger->pWriter, 0);
    while ((pMerger->pIter = pMerger->iterList)) {
      pMerger->iterList = pMerger->pIter->next;
      tsdbCloseDataIter2(pMerger->pIter);
    }
    if (pMerger->pReader) tsdbDataFReaderClose(&pMerger->pReader);
  }
  return code;
}

static void tsdbEndMerge(STsdbMerger *pMerger) {
  // writer
  tBlockDataDestroy(&pMerger->sData);
  tBlockDataDestroy(&pMerger->bData);
  taosArrayDestroy(pMerger->aSttBlk);
  tMapDataClear(&pMerger->mDataBlk);
  taosArrayDestroy(pMerger->aBlockIdx);

  // reader

  // tombstone
  taosArrayDestroy(pMerger->aSkyLine);
  taosArrayDestroy(pMerger->aDelData);
  taosArrayDestroy(pMerger->aDelIdx);

  // others
  tDestroyTSchema(pMerger->tbSkm.pTSchema);
  tsdbFSDestroy(&pMerger->fs);

  tsdbInfo("vgId:%d %s done, commit ID:%" PRId64, TD_VID(pMerger->pTsdb->pVnode), __func__, pMerger->commitID);
}

static int32_t tsdbBeginMerge(STsdb *pTsdb, SMergeInfo *pInfo, STsdbMerger *pMerger) {
  int32_t code = 0;
  int32_t lino = 0;

  pMerger->pTsdb = pTsdb;
  pMerger->commitID = pInfo->commitID;
  pMerger->cmprAlg = pTsdb->pVnode->config.tsdbCfg.compression;
  pMerger->maxRows = pTsdb->pVnode->config.tsdbCfg.maxRows;
  pMerger->minRows = pTsdb->pVnode->config.tsdbCfg.minRows;
  pMerger->fid = INT32_MIN;

  code = tsdbFSCopy(pTsdb, &pMerger->fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* tombstone */
  if (pMerger->fs.pDelFile) {
    code = tsdbDelFReaderOpen(&pMerger->pDelFReader, pMerger->fs.pDelFile, pTsdb);
    TSDB_CHECK_CODE(code, lino, _exit);

    if ((pMerger->aDelIdx = taosArrayInit(0, sizeof(SDelIdx))) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if ((pMerger->aDelData = taosArrayInit(0, sizeof(SDelData))) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if ((pMerger->aSkyLine = taosArrayInit(0, sizeof(TSDBKEY))) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbReadDelIdx(pMerger->pDelFReader, pMerger->aDelIdx);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  /* reader */

  /* writer */
  code = tBlockDataCreate(&pMerger->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tBlockDataCreate(&pMerger->sData);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, commit ID:%" PRId64, TD_VID(pTsdb->pVnode), __func__, lino,
              tstrerror(code), pMerger->commitID);
    tBlockDataDestroy(&pMerger->sData);
    tBlockDataDestroy(&pMerger->bData);
    if (pMerger->fs.pDelFile) {
      taosArrayDestroy(pMerger->aSkyLine);
      taosArrayDestroy(pMerger->aDelData);
      taosArrayDestroy(pMerger->aDelIdx);
      if (pMerger->pDelFReader) tsdbDelFReaderClose(&pMerger->pDelFReader);
    }
    tsdbFSDestroy(&pMerger->fs);
  } else {
    tsdbInfo("vgId:%d %s done, commit ID:%" PRId64, TD_VID(pTsdb->pVnode), __func__, pMerger->commitID);
  }
  return code;
}

int32_t tsdbMerge(STsdb *pTsdb, void *pInfo, int64_t varg) {
  int32_t code = 0;

  STsdbMerger *pMerger = &(STsdbMerger){0};

  if ((code = tsdbBeginMerge(pTsdb, pInfo, pMerger))) return code;

  for (;;) {
    SDFileSet *pSet = (SDFileSet *)taosArraySearch(pMerger->fs.aDFileSet, &(SDFileSet){.fid = pMerger->fid},
                                                   tDFileSetCmprFn, TD_GT);
    if (pSet == NULL) {
      pMerger->fid = INT32_MAX;
      break;
    }

    if ((code = tsdbMergeFileSet(pMerger, pSet))) goto _exit;
  }

  if ((code = tsdbFSUpsertDelFile(&pMerger->fs, NULL))) goto _exit;

_exit:
  if (code) {
    tsdbAbortMerge(pMerger);
  } else {
    tsdbCommitMerge(pMerger);
  }
  tsdbEndMerge(pMerger);

  return code;
}