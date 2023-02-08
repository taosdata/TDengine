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
} STsdbCompactor;

static int32_t tsdbCommitCompact(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pCompactor->pTsdb;

  code = tsdbFSPrepareCommit(pTsdb, &pCompactor->fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  taosThreadRwlockWrlock(&pTsdb->rwLock);

  code = tsdbFSCommit(pTsdb);
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

static int32_t tsdbAbortCompact(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pCompactor->pTsdb;

  // TODO
  ASSERT(0);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbCompactWriteTableDataStart(STsdbCompactor *pCompactor, TABLEID *pId) {
  int32_t code = 0;
  int32_t lino = 0;

  pCompactor->tbid = *pId;

  // tombstone
  for (;;) {
    if (pCompactor->iDelIdx >= taosArrayGetSize(pCompactor->aDelIdx)) {
      pCompactor->pDKey = NULL;
      break;
    }

    SDelIdx *pDelIdx = (SDelIdx *)taosArrayGet(pCompactor->aDelIdx, pCompactor->iDelIdx);
    int32_t  c = tTABLEIDCmprFn(pDelIdx, &pCompactor->tbid);
    if (c < 0) {
      pCompactor->iDelIdx++;
    } else if (c == 0) {
      pCompactor->iDelIdx++;

      code = tsdbReadDelData(pCompactor->pDelFReader, pDelIdx, pCompactor->aDelData);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbBuildDeleteSkyline(pCompactor->aDelData, 0, taosArrayGetSize(pCompactor->aDelData) - 1,
                                    pCompactor->aSkyLine);
      TSDB_CHECK_CODE(code, lino, _exit);

      pCompactor->iSkyLine = 0;
      if (pCompactor->iSkyLine < taosArrayGetSize(pCompactor->aSkyLine)) {
        TSDBKEY *pKey = (TSDBKEY *)taosArrayGet(pCompactor->aSkyLine, pCompactor->iSkyLine);

        pCompactor->dKey.version = 0;
        pCompactor->dKey.ts = pKey->ts;
        pCompactor->pDKey = &pCompactor->dKey;
      } else {
        pCompactor->pDKey = NULL;
      }
      break;
    } else {
      pCompactor->pDKey = NULL;
      break;
    }
  }

  // reader and write (TODO)
  code = tsdbUpdateTableSchema(pCompactor->pTsdb->pVnode->pMeta, pId->suid, pId->uid, &pCompactor->tbSkm);
  TSDB_CHECK_CODE(code, lino, _exit);

  tMapDataReset(&pCompactor->mDataBlk);

  code = tBlockDataInit(&pCompactor->bData, pId, pCompactor->tbSkm.pTSchema, NULL, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (!TABLE_SAME_SCHEMA(pCompactor->tbid.suid, pCompactor->tbid.uid, pId->suid, pId->uid)) {
    if (pCompactor->sData.nRow > 0) {
      code = tsdbWriteSttBlock(pCompactor->pWriter, &pCompactor->sData, pCompactor->aSttBlk, pCompactor->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tBlockDataInit(&pCompactor->sData, pId /* TODO */, pCompactor->tbSkm.pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pCompactor->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, suid:%" PRId64 " uid:%" PRId64, TD_VID(pCompactor->pTsdb->pVnode), __func__, pId->suid,
              pId->uid);
  }
  return code;
}

static int32_t tsdbCompactWriteTableDataEnd(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pCompactor->bData.nRow > 0) {
    if (pCompactor->bData.nRow < pCompactor->minRows) {
      for (int32_t iRow = 0; iRow < pCompactor->bData.nRow; iRow++) {
        code = tBlockDataAppendRow(&pCompactor->sData, &tsdbRowFromBlockData(&pCompactor->bData, iRow), NULL,
                                   pCompactor->tbid.uid);
        TSDB_CHECK_CODE(code, lino, _exit);

        if (pCompactor->sData.nRow >= pCompactor->maxRows) {
          code = tsdbWriteSttBlock(pCompactor->pWriter, &pCompactor->sData, pCompactor->aSttBlk, pCompactor->cmprAlg);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }

      tBlockDataClear(&pCompactor->bData);
    } else {
      code = tsdbWriteDataBlock(pCompactor->pWriter, &pCompactor->bData, &pCompactor->mDataBlk, pCompactor->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (pCompactor->mDataBlk.nItem) {
    SBlockIdx *pBlockIdx = (SBlockIdx *)taosArrayReserve(pCompactor->aBlockIdx, 1);
    if (pBlockIdx == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    pBlockIdx->suid = pCompactor->tbid.suid;
    pBlockIdx->uid = pCompactor->tbid.uid;

    code = tsdbWriteDataBlk(pCompactor->pWriter, &pCompactor->mDataBlk, pBlockIdx);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pCompactor->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, suid:%" PRId64 " uid:%" PRId64, TD_VID(pCompactor->pTsdb->pVnode), __func__,
              pCompactor->tbid.suid, pCompactor->tbid.uid);
  }
  return code;
}

static bool tsdbCompactRowIsDeleted(STsdbCompactor *pCompactor, TSDBROW *pRow) {
  TSDBKEY  tKey = TSDBROW_KEY(pRow);
  TSDBKEY *aKey = (TSDBKEY *)TARRAY_DATA(pCompactor->aSkyLine);
  int32_t  nKey = TARRAY_SIZE(pCompactor->aSkyLine);

  if (tKey.ts > pCompactor->pDKey->ts) {
    do {
      pCompactor->pDKey->version = aKey[pCompactor->iSkyLine].version;
      pCompactor->iSkyLine++;
      if (pCompactor->iSkyLine < nKey) {
        pCompactor->dKey.ts = aKey[pCompactor->iSkyLine].ts;
      } else {
        if (pCompactor->pDKey->version == 0) {
          pCompactor->pDKey = NULL;
          return false;
        } else {
          pCompactor->pDKey->ts = INT64_MAX;
        }
      }
    } while (tKey.ts > pCompactor->pDKey->ts);
  }

  if (tKey.ts < pCompactor->pDKey->ts) {
    if (tKey.version > pCompactor->pDKey->version) {
      return false;
    } else {
      return true;
    }
  } else if (tKey.ts == pCompactor->pDKey->ts) {
    ASSERT(pCompactor->iSkyLine < nKey);
    if (tKey.version > TMAX(pCompactor->pDKey->version, aKey[pCompactor->iSkyLine].version)) {
      return false;
    } else {
      return true;
    }
  }

  return false;
}

static int32_t tsdbCompactWriteTableData(STsdbCompactor *pCompactor, SRowInfo *pRowInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  SRowInfo rInfo;
  if (pRowInfo == NULL) {
    rInfo.suid = INT64_MAX;
    rInfo.uid = INT64_MAX;
    // rInfo.row = TSDBORW_V;
    pRowInfo = &rInfo;
  }

  // start a new table data write if need
  if (pRowInfo->uid != pCompactor->tbid.uid) {
    if (pCompactor->tbid.uid) {
      code = tsdbCompactWriteTableDataEnd(pCompactor);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbCompactWriteTableDataStart(pCompactor, (TABLEID *)pRowInfo);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // check if row is deleted
  if (pCompactor->pDKey && tsdbCompactRowIsDeleted(pCompactor, &pRowInfo->row)) goto _exit;

  code = tBlockDataUpsertRow(&pCompactor->bData, &pRowInfo->row, NULL, pRowInfo->uid);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pCompactor->bData.nRow >= pCompactor->maxRows) {
    code = tsdbWriteDataBlock(pCompactor->pWriter, &pCompactor->bData, &pCompactor->mDataBlk, pCompactor->cmprAlg);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pCompactor->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  } else if (pRowInfo) {
    tsdbTrace("vgId:%d %s done, suid:%" PRId64 " uid:%" PRId64 " ts:%" PRId64 " version:%" PRId64,
              TD_VID(pCompactor->pTsdb->pVnode), __func__, pRowInfo->suid, pRowInfo->uid, TSDBROW_TS(&pRowInfo->row),
              TSDBROW_VERSION(&pRowInfo->row));
  }
  return code;
}

static int32_t tsdbCompactNextRow(STsdbCompactor *pCompactor, SRowInfo **ppRowInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pCompactor->pIter) {
    code = tsdbDataIterNext2(pCompactor->pIter, NULL /* TODO */);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pCompactor->pIter->rowInfo.suid == 0 && pCompactor->pIter->rowInfo.uid == 0) {
      pCompactor->pIter = NULL;
    } else {
      SRBTreeNode *pNode = tRBTreeMin(&pCompactor->rbt);
      if (pNode) {
        int32_t c = tsdbDataIterCmprFn(&pCompactor->pIter->rbtn, pNode);
        if (c > 0) {
          tRBTreePut(&pCompactor->rbt, &pCompactor->pIter->rbtn);
          pCompactor->pIter = NULL;
        } else if (c == 0) {
          ASSERT(0);
        }
      }
    }
  }

  if (pCompactor->pIter == NULL) {
    SRBTreeNode *pNode = tRBTreeMin(&pCompactor->rbt);
    if (pNode) {
      tRBTreeDrop(&pCompactor->rbt, pNode);
      pCompactor->pIter = TSDB_RBTN_TO_DATA_ITER(pNode);
    }
  }

  if (ppRowInfo) {
    if (pCompactor->pIter) {
      *ppRowInfo = &pCompactor->pIter->rowInfo;
    } else {
      *ppRowInfo = NULL;
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pCompactor->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbCompactFileSetStart(STsdbCompactor *pCompactor, SDFileSet *pSet) {
  int32_t code = 0;
  int32_t lino = 0;

  pCompactor->fid = pSet->fid;
  pCompactor->tbid = (TABLEID){0};

  /* tombstone */
  pCompactor->iDelIdx = 0;
  pCompactor->iSkyLine = 0;

  /* reader */
  code = tsdbDataFReaderOpen(&pCompactor->pReader, pCompactor->pTsdb, pSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbOpenDataFileDataIter(pCompactor->pReader, &pCompactor->pIter);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pCompactor->pIter) {
    pCompactor->pIter->next = pCompactor->iterList;
    pCompactor->iterList = pCompactor->pIter;
  }

  for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
    code = tsdbOpenSttFileDataIter(pCompactor->pReader, iStt, &pCompactor->pIter);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pCompactor->pIter) {
      pCompactor->pIter->next = pCompactor->iterList;
      pCompactor->iterList = pCompactor->pIter;
    }
  }

  pCompactor->pIter = NULL;
  tRBTreeCreate(&pCompactor->rbt, tsdbDataIterCmprFn);

  /* writer */
  code = tsdbDataFWriterOpen(&pCompactor->pWriter, pCompactor->pTsdb,
                             &(SDFileSet){.fid = pCompactor->fid,
                                          .diskId = pSet->diskId,
                                          .pHeadF = &(SHeadFile){.commitID = pCompactor->commitID},
                                          .pDataF = &(SDataFile){.commitID = pCompactor->commitID},
                                          .pSmaF = &(SSmaFile){.commitID = pCompactor->commitID},
                                          .nSttF = 1,
                                          .aSttF = {&(SSttFile){.commitID = pCompactor->commitID}}});
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pCompactor->aBlockIdx) {
    taosArrayClear(pCompactor->aBlockIdx);
  } else if ((pCompactor->aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tMapDataReset(&pCompactor->mDataBlk);

  if (pCompactor->aSttBlk) {
    taosArrayClear(pCompactor->aSttBlk);
  } else if ((pCompactor->aSttBlk = taosArrayInit(0, sizeof(SSttBlk))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tBlockDataReset(&pCompactor->bData);
  tBlockDataReset(&pCompactor->sData);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d", TD_VID(pCompactor->pTsdb->pVnode), __func__, lino,
              tstrerror(code), pCompactor->fid);
  } else {
    tsdbInfo("vgId:%d %s done, fid:%d", TD_VID(pCompactor->pTsdb->pVnode), __func__, pCompactor->fid);
  }
  return code;
}

static int32_t tsdbCompactFileSetEnd(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  /* finish remaining data (TODO) */

  /* update files */
  code = tsdbWriteSttBlk(pCompactor->pWriter, pCompactor->aSttBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbWriteBlockIdx(pCompactor->pWriter, pCompactor->aBlockIdx);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbUpdateDFileSetHeader(pCompactor->pWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFSUpsertFSet(&pCompactor->fs, &pCompactor->pWriter->wSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDataFWriterClose(&pCompactor->pWriter, 1);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDataFReaderClose(&pCompactor->pReader);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* do clear */
  while ((pCompactor->pIter = pCompactor->iterList) != NULL) {
    pCompactor->iterList = pCompactor->pIter->next;
    tsdbCloseDataIter2(pCompactor->pIter);
  }

  tBlockDataReset(&pCompactor->bData);
  tBlockDataReset(&pCompactor->sData);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d", TD_VID(pCompactor->pTsdb->pVnode), __func__, lino,
              tstrerror(code), pCompactor->fid);
  } else {
    tsdbInfo("vgId:%d %s done, fid:%d", TD_VID(pCompactor->pTsdb->pVnode), __func__, pCompactor->fid);
  }
  return code;
}

static int32_t tsdbCompactFileSet(STsdbCompactor *pCompactor, SDFileSet *pSet) {
  int32_t code = 0;
  int32_t lino = 0;

  // start compact
  code = tsdbCompactFileSetStart(pCompactor, pSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  // do compact
  SRowInfo *pRowInfo;
  for (;;) {
    code = tsdbCompactNextRow(pCompactor, &pRowInfo);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbCompactWriteTableData(pCompactor, pRowInfo);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pRowInfo == NULL) break;
  }

  // end compact
  code = tsdbCompactFileSetEnd(pCompactor);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  return code;
}

static void tsdbEndCompact(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  // writer
  tBlockDataDestroy(&pCompactor->sData);
  tBlockDataDestroy(&pCompactor->bData);
  taosArrayDestroy(pCompactor->aSttBlk);
  tMapDataClear(&pCompactor->mDataBlk);
  taosArrayDestroy(pCompactor->aBlockIdx);

  // reader

  // tombstone
  taosArrayDestroy(pCompactor->aSkyLine);
  taosArrayDestroy(pCompactor->aDelData);
  taosArrayDestroy(pCompactor->aDelIdx);

  // others
  tDestroyTSchema(pCompactor->tbSkm.pTSchema);
  tsdbFSDestroy(&pCompactor->fs);

  tsdbInfo("vgId:%d %s done, commit ID:%" PRId64, TD_VID(pCompactor->pTsdb->pVnode), __func__, pCompactor->commitID);
}

static int32_t tsdbBeginCompact(STsdb *pTsdb, STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  pCompactor->pTsdb = pTsdb;
  pCompactor->commitID = 0;  // TODO
  pCompactor->cmprAlg = pTsdb->pVnode->config.tsdbCfg.compression;
  pCompactor->maxRows = pTsdb->pVnode->config.tsdbCfg.maxRows;
  pCompactor->minRows = pTsdb->pVnode->config.tsdbCfg.minRows;
  pCompactor->fid = INT32_MIN;

  code = tsdbFSCopy(pTsdb, &pCompactor->fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  /* tombstone */
  if (pCompactor->fs.pDelFile) {
    code = tsdbDelFReaderOpen(&pCompactor->pDelFReader, pCompactor->fs.pDelFile, pTsdb);
    TSDB_CHECK_CODE(code, lino, _exit);

    if ((pCompactor->aDelIdx = taosArrayInit(0, sizeof(SDelIdx))) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if ((pCompactor->aDelData = taosArrayInit(0, sizeof(SDelData))) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if ((pCompactor->aSkyLine = taosArrayInit(0, sizeof(TSDBKEY))) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbReadDelIdx(pCompactor->pDelFReader, pCompactor->aDelIdx);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  /* reader */

  /* writer */
  code = tBlockDataCreate(&pCompactor->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tBlockDataCreate(&pCompactor->sData);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, commit ID:%" PRId64, TD_VID(pTsdb->pVnode), __func__, lino,
              tstrerror(code), pCompactor->commitID);
  } else {
    tsdbInfo("vgId:%d %s done, commit ID:%" PRId64, TD_VID(pTsdb->pVnode), __func__, pCompactor->commitID);
  }
  return code;
}

int32_t tsdbCompact(STsdb *pTsdb, int32_t flag) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbCompactor *pCompactor = &(STsdbCompactor){0};

  // begin compact
  code = tsdbBeginCompact(pTsdb, pCompactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  // loop to compact each file set
  while (true) {
    SDFileSet *pSet = (SDFileSet *)taosArraySearch(pCompactor->fs.aDFileSet, &(SDFileSet){.fid = pCompactor->fid},
                                                   tDFileSetCmprFn, TD_GT);
    if (pSet == NULL) {
      pCompactor->fid = INT32_MAX;
      break;
    }

    code = tsdbCompactFileSet(pCompactor, pSet);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbFSUpsertDelFile(&pCompactor->fs, NULL);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  // commit/abort compact
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, commit ID:%" PRId64, TD_VID(pTsdb->pVnode), __func__, lino,
              tstrerror(code), pCompactor->commitID);
    tsdbAbortCompact(pCompactor);
  } else {
    tsdbCommitCompact(pCompactor);
  }
  tsdbEndCompact(pCompactor);
  return code;
}
