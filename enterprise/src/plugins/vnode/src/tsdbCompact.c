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

#include "../../../../../source/dnode/vnode/src/tsdb/tsdbDataFileRW.h"
#include "../../../../../source/dnode/vnode/src/tsdb/tsdbFS2.h"
#include "../../../../../source/dnode/vnode/src/tsdb/tsdbFSetRW.h"
#include "../../../../../source/dnode/vnode/src/tsdb/tsdbIter.h"
#include "../../../../../source/dnode/vnode/src/tsdb/tsdbSttFileRW.h"
#include "tsdb.h"

extern int32_t tsdbUpdateTableSchema(SMeta *pMeta, int64_t suid, int64_t uid, SSkmInfo *pSkmInfo);
extern int32_t tsdbWriteDataBlock(SDataFWriter *pWriter, SBlockData *pBlockData, SMapData *mDataBlk, int8_t cmprAlg);
extern int32_t tsdbWriteSttBlock(SDataFWriter *pWriter, SBlockData *pBlockData, SArray *aSttBlk, int8_t cmprAlg);

#if 0
typedef struct {
  STsdb  *pTsdb;
  int64_t commitID;
  int8_t  cmprAlg;
  int32_t maxRows;
  int32_t minRows;

  STsdbFS fs;

  int32_t  maxFid;
  int32_t  minFid;
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

static bool tsdbNeedCompactFileSet(STsdbCompactor *pCompactor, SDFileSet *pSet) {
  // TODO
  return true;
}

static int32_t tsdbAbortCompact(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pCompactor->pTsdb;
  code = tsdbFSRollback(pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
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

  // writer
  code = tsdbUpdateTableSchema(pCompactor->pTsdb->pVnode->pMeta, pId->suid, pId->uid, &pCompactor->tbSkm);
  TSDB_CHECK_CODE(code, lino, _exit);

  tMapDataReset(&pCompactor->mDataBlk);

  code = tBlockDataInit(&pCompactor->bData, pId, pCompactor->tbSkm.pTSchema, NULL, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (!TABLE_SAME_SCHEMA(pCompactor->sData.suid, pCompactor->sData.uid, pId->suid, pId->uid)) {
    if (pCompactor->sData.nRow > 0) {
      code = tsdbWriteSttBlock(pCompactor->pWriter, &pCompactor->sData, pCompactor->aSttBlk, pCompactor->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    TABLEID tbid = {.suid = pId->suid, .uid = pId->suid ? 0 : pId->uid};
    code = tBlockDataInit(&pCompactor->sData, &tbid, pCompactor->tbSkm.pTSchema, NULL, 0);
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

  if (pCompactor->mDataBlk.nItem > 0) {
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

  // start a new table data write if need
  if (pRowInfo == NULL || pRowInfo->uid != pCompactor->tbid.uid) {
    if (pCompactor->tbid.uid) {
      code = tsdbCompactWriteTableDataEnd(pCompactor);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (pRowInfo == NULL) {
      if (pCompactor->sData.nRow > 0) {
        code = tsdbWriteSttBlock(pCompactor->pWriter, &pCompactor->sData, pCompactor->aSttBlk, pCompactor->cmprAlg);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
      return code;
    }

    code = tsdbCompactWriteTableDataStart(pCompactor, (TABLEID *)pRowInfo);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // check if row is deleted
  if (pCompactor->pDKey && tsdbCompactRowIsDeleted(pCompactor, &pRowInfo->row)) goto _exit;

  if (tBlockDataTryUpsertRow(&pCompactor->bData, &pRowInfo->row, pRowInfo->uid) > pCompactor->maxRows) {
    code = tsdbWriteDataBlock(pCompactor->pWriter, &pCompactor->bData, &pCompactor->mDataBlk, pCompactor->cmprAlg);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataUpsertRow(&pCompactor->bData, &pRowInfo->row, NULL, pRowInfo->uid);
  TSDB_CHECK_CODE(code, lino, _exit);

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

static bool tsdbCompactTableIsDropped(STsdbCompactor *pCompactor) {
  SMetaInfo info;

  if (pCompactor->pIter->rowInfo.uid == pCompactor->tbid.uid) return false;
  if (metaGetInfo(pCompactor->pTsdb->pVnode->pMeta, pCompactor->pIter->rowInfo.uid, &info, NULL)) {
    return true;
  }
  return false;
}
static int32_t tsdbCompactNextRow(STsdbCompactor *pCompactor, SRowInfo **ppRowInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  for (;;) {
    if (pCompactor->pIter) {
      code = tsdbDataIterNext2(pCompactor->pIter, NULL);
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
      SRBTreeNode *pNode = tRBTreeDropMin(&pCompactor->rbt);
      if (pNode) {
        pCompactor->pIter = TSDB_RBTN_TO_DATA_ITER(pNode);
      }
    }

    if (pCompactor->pIter) {
      if (tsdbCompactTableIsDropped(pCompactor)) {
        TABLEID tbid = {.suid = pCompactor->pIter->rowInfo.suid, .uid = pCompactor->pIter->rowInfo.uid};
        tRBTreeClear(&pCompactor->rbt);
        for (pCompactor->pIter = pCompactor->iterList; pCompactor->pIter; pCompactor->pIter = pCompactor->pIter->next) {
          code = tsdbDataIterNext2(pCompactor->pIter,
                                   &(STsdbFilterInfo){.flag = TSDB_FILTER_FLAG_BY_TABLEID, .tbid = tbid});
          TSDB_CHECK_CODE(code, lino, _exit);

          if (pCompactor->pIter->rowInfo.suid || pCompactor->pIter->rowInfo.uid) {
            tRBTreePut(&pCompactor->rbt, &pCompactor->pIter->rbtn);
          }
        }
      } else {
        *ppRowInfo = &pCompactor->pIter->rowInfo;
        break;
      }
    } else {
      *ppRowInfo = NULL;
      break;
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

  /* reader */
  code = tsdbDataFReaderOpen(&pCompactor->pReader, pCompactor->pTsdb, pSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbOpenDataFileDataIter(pCompactor->pReader, &pCompactor->pIter);
  TSDB_CHECK_CODE(code, lino, _exit);

  tRBTreeCreate(&pCompactor->rbt, tsdbDataIterCmprFn);
  if (pCompactor->pIter) {
    pCompactor->pIter->next = pCompactor->iterList;
    pCompactor->iterList = pCompactor->pIter;

    code = tsdbDataIterNext2(pCompactor->pIter, NULL);
    TSDB_CHECK_CODE(code, lino, _exit);

    ASSERT(pCompactor->pIter->rowInfo.suid || pCompactor->pIter->rowInfo.uid);
    tRBTreePut(&pCompactor->rbt, &pCompactor->pIter->rbtn);
  }

  for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
    code = tsdbOpenSttFileDataIter(pCompactor->pReader, iStt, &pCompactor->pIter);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pCompactor->pIter) {
      pCompactor->pIter->next = pCompactor->iterList;
      pCompactor->iterList = pCompactor->pIter;

      code = tsdbDataIterNext2(pCompactor->pIter, NULL);
      TSDB_CHECK_CODE(code, lino, _exit);

      ASSERT(pCompactor->pIter->rowInfo.suid || pCompactor->pIter->rowInfo.uid);
      tRBTreePut(&pCompactor->rbt, &pCompactor->pIter->rbtn);
    }
  }
  pCompactor->pIter = NULL;

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

  ASSERT(pCompactor->bData.nRow == 0);
  ASSERT(pCompactor->sData.nRow == 0);

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

  // do compact, end with a NULL row
  SRowInfo *pRowInfo;
  do {
    code = tsdbCompactNextRow(pCompactor, &pRowInfo);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbCompactWriteTableData(pCompactor, pRowInfo);
    TSDB_CHECK_CODE(code, lino, _exit);
  } while (pRowInfo);

  // end compact
  code = tsdbCompactFileSetEnd(pCompactor);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d", TD_VID(pCompactor->pTsdb->pVnode), __func__, lino,
              tstrerror(code), pCompactor->fid);
    if (pCompactor->pWriter) tsdbDataFWriterClose(&pCompactor->pWriter, 0);
    while ((pCompactor->pIter = pCompactor->iterList)) {
      pCompactor->iterList = pCompactor->pIter->next;
      tsdbCloseDataIter2(pCompactor->pIter);
    }
    if (pCompactor->pReader) tsdbDataFReaderClose(&pCompactor->pReader);
  }
  return code;
}

static void tsdbEndCompact(STsdbCompactor *pCompactor) {
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

static int32_t tsdbBeginCompact(STsdb *pTsdb, SCompactInfo *pInfo, STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  pCompactor->pTsdb = pTsdb;
  pCompactor->commitID = pInfo->commitID;
  pCompactor->cmprAlg = pTsdb->pVnode->config.tsdbCfg.compression;
  pCompactor->maxRows = pTsdb->pVnode->config.tsdbCfg.maxRows;
  pCompactor->minRows = pTsdb->pVnode->config.tsdbCfg.minRows;
  pCompactor->minFid = tsdbKeyFid(pInfo->tw.skey, pTsdb->keepCfg.days, pTsdb->keepCfg.precision);
  pCompactor->maxFid = tsdbKeyFid(pInfo->tw.ekey, pTsdb->keepCfg.days, pTsdb->keepCfg.precision);
  pCompactor->fid = pCompactor->minFid - 1;

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
    tBlockDataDestroy(&pCompactor->sData);
    tBlockDataDestroy(&pCompactor->bData);
    if (pCompactor->fs.pDelFile) {
      taosArrayDestroy(pCompactor->aSkyLine);
      taosArrayDestroy(pCompactor->aDelData);
      taosArrayDestroy(pCompactor->aDelIdx);
      if (pCompactor->pDelFReader) tsdbDelFReaderClose(&pCompactor->pDelFReader);
    }
    tsdbFSDestroy(&pCompactor->fs);
  } else {
    tsdbInfo("vgId:%d %s done, commit ID:%" PRId64, TD_VID(pTsdb->pVnode), __func__, pCompactor->commitID);
  }
  return code;
}

int32_t tsdbCompact(STsdb *pTsdb, SCompactInfo *pInfo) {
  int32_t code = 0;

  STsdbCompactor *pCompactor = &(STsdbCompactor){0};

  if ((code = tsdbBeginCompact(pTsdb, pInfo, pCompactor))) return code;

  for (;;) {
    SDFileSet *pSet = (SDFileSet *)taosArraySearch(pCompactor->fs.aDFileSet, &(SDFileSet){.fid = pCompactor->fid},
                                                   tDFileSetCmprFn, TD_GT);
    if (pSet == NULL || pSet->fid > pCompactor->maxFid) {
      pCompactor->fid = INT32_MAX;
      break;
    }

    if (!tsdbNeedCompactFileSet(pCompactor, pSet)) continue;

    if ((code = tsdbCompactFileSet(pCompactor, pSet))) goto _exit;
  }

_exit:
  if (code) {
    tsdbAbortCompact(pCompactor);
  } else {
    tsdbFSPrepareCommit(pTsdb, &pCompactor->fs);
  }
  tsdbEndCompact(pCompactor);
  return code;
}

int32_t tsdbCommitCompact(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

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
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}
#endif

// new code ====================================================================================
typedef struct {
  STsdb  *tsdb;
  int32_t szPage;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  int64_t cid;
  int64_t compactVersion;

  int32_t        minFid;
  int32_t        maxFid;
  TFileSetArray *fsetArr;
  TFileOpArray   fopArr[1];

  struct {
    STFileSet *fset;
    SDiskID    did;

    // reader
    SDataFileReader    *dataReader;
    TSttFileReaderArray sttReaderArr[1];

    // iter & merger
    TTsdbIterArray dataIterArr[1];
    SIterMerger   *dataIterMerger;
    TTsdbIterArray tombIterArr[1];
    SIterMerger   *tombIterMerger;

    // writer
    SFSetWriter *writer;

    TABLEID tbid[1];

    // skyline
    SArray  *aSkyLine;
    int32_t  iSkyLine;
    TSDBKEY *pDKey;
    TSDBKEY  dKey;
  } ctx[1];
} SCompactor2;

typedef struct {
  STsdb      *tsdb;
  bool        sync;
  STimeWindow tw;
} SCompactArg;

static int32_t tsdbCompactBegin(SCompactArg *arg, SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *tsdb = arg->tsdb;

  compactor->tsdb = tsdb;
  compactor->szPage = tsdb->pVnode->config.tsdbPageSize;
  compactor->minRow = tsdb->pVnode->config.tsdbCfg.minRows;
  compactor->maxRow = tsdb->pVnode->config.tsdbCfg.maxRows;
  compactor->cmprAlg = tsdb->pVnode->config.tsdbCfg.compression;
  compactor->cid = tsdbFSAllocEid(tsdb->pFS);
  compactor->compactVersion = INT64_MAX;
  compactor->minFid = tsdbKeyFid(arg->tw.skey, tsdb->keepCfg.days, tsdb->keepCfg.precision);
  compactor->maxFid = tsdbKeyFid(arg->tw.ekey, tsdb->keepCfg.days, tsdb->keepCfg.precision);

  code = tsdbFSCreateCopySnapshot(tsdb->pFS, &compactor->fsetArr);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCompactEnd(SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFSEditBegin(compactor->tsdb->pFS, compactor->fopArr, TSDB_FEDIT_MERGE);
  TSDB_CHECK_CODE(code, lino, _exit);

  taosThreadRwlockWrlock(&compactor->tsdb->rwLock);
  code = tsdbFSEditCommit(compactor->tsdb->pFS);
  if (code) {
    taosThreadRwlockUnlock(&compactor->tsdb->rwLock);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  taosThreadRwlockUnlock(&compactor->tsdb->rwLock);

  taosArrayDestroy(compactor->ctx->aSkyLine);

  TARRAY2_DESTROY(compactor->ctx->tombIterArr, NULL);
  TARRAY2_DESTROY(compactor->ctx->dataIterArr, NULL);
  TARRAY2_DESTROY(compactor->ctx->sttReaderArr, NULL);
  TARRAY2_DESTROY(compactor->fopArr, NULL);

  tsdbFSDestroyCopySnapshot(&compactor->fsetArr);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCompactFSetOpenReader(SCompactor2 *compactor) {
  int32_t    code = 0;
  int32_t    lino = 0;
  STFileObj *fobj;

  ASSERT(compactor->ctx->dataReader == NULL);
  ASSERT(TARRAY2_SIZE(compactor->ctx->sttReaderArr) == 0);

  // data
  SDataFileReaderConfig dataFileReaderConfig = {
      .tsdb = compactor->tsdb,
      .szPage = compactor->szPage,
  };
  for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX && (fobj = compactor->ctx->fset->farr[ftype], 1); ftype++) {
    if (fobj == NULL) continue;
    dataFileReaderConfig.files[ftype].exist = true;
    dataFileReaderConfig.files[ftype].file = fobj->f[0];

    STFileOp op = {
        .optype = TSDB_FOP_REMOVE,
        .fid = compactor->ctx->fset->fid,
        .of = fobj->f[0],
    };

    code = TARRAY2_APPEND(compactor->fopArr, op);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  code = tsdbDataFileReaderOpen(NULL, &dataFileReaderConfig, &compactor->ctx->dataReader);
  TSDB_CHECK_CODE(code, lino, _exit);

  // stt
  SSttLvl *lvl;
  TARRAY2_FOREACH(compactor->ctx->fset->lvlArr, lvl) {
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      SSttFileReader      *sttReader;
      SSttFileReaderConfig sttFileReaderConfig = {
          .tsdb = compactor->tsdb,
          .szPage = compactor->szPage,
          .file = fobj->f[0],
      };

      code = tsdbSttFileReaderOpen(fobj->fname, &sttFileReaderConfig, &sttReader);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = TARRAY2_APPEND(compactor->ctx->sttReaderArr, sttReader);
      TSDB_CHECK_CODE(code, lino, _exit);

      STFileOp op = {
          .optype = TSDB_FOP_REMOVE,
          .fid = compactor->ctx->fset->fid,
          .of = fobj->f[0],
      };

      code = TARRAY2_APPEND(compactor->fopArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCompactFSetCloseReader(SCompactor2 *compactor) {
  TARRAY2_CLEAR(compactor->ctx->sttReaderArr, tsdbSttFileReaderClose);
  tsdbDataFileReaderClose(&compactor->ctx->dataReader);
  return 0;
}

static int32_t tsdbCompactFSetOpenIter(SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbIter      *iter;
  STsdbIterConfig iterConfig = {0};

  ASSERT(compactor->ctx->dataIterMerger == NULL);
  ASSERT(compactor->ctx->tombIterMerger == NULL);
  ASSERT(TARRAY2_SIZE(compactor->ctx->dataIterArr) == 0);
  ASSERT(TARRAY2_SIZE(compactor->ctx->tombIterArr) == 0);

  // data
  if (compactor->ctx->dataReader != NULL) {
    // data
    iterConfig.type = TSDB_ITER_TYPE_DATA;
    iterConfig.dataReader = compactor->ctx->dataReader;

    code = tsdbIterOpen(&iterConfig, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(compactor->ctx->dataIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    // tomb
    iterConfig.type = TSDB_ITER_TYPE_DATA_TOMB;
    iterConfig.dataReader = compactor->ctx->dataReader;

    code = tsdbIterOpen(&iterConfig, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(compactor->ctx->tombIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // stt
  SSttFileReader *sttReader;
  TARRAY2_FOREACH(compactor->ctx->sttReaderArr, sttReader) {
    // data
    iterConfig.type = TSDB_ITER_TYPE_STT;
    iterConfig.sttReader = sttReader;

    code = tsdbIterOpen(&iterConfig, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(compactor->ctx->dataIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    // tomb
    iterConfig.type = TSDB_ITER_TYPE_STT_TOMB;
    iterConfig.sttReader = sttReader;

    code = tsdbIterOpen(&iterConfig, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(compactor->ctx->tombIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // merger
  code = tsdbIterMergerOpen(compactor->ctx->dataIterArr, &compactor->ctx->dataIterMerger, false);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbIterMergerOpen(compactor->ctx->tombIterArr, &compactor->ctx->tombIterMerger, true);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCompactFSetCloseIter(SCompactor2 *compactor) {
  tsdbIterMergerClose(&compactor->ctx->dataIterMerger);
  tsdbIterMergerClose(&compactor->ctx->tombIterMerger);
  TARRAY2_CLEAR(compactor->ctx->tombIterArr, tsdbIterClose);
  TARRAY2_CLEAR(compactor->ctx->dataIterArr, tsdbIterClose);
  return 0;
}

static int32_t tsdbCompactFSetOpenWriter(SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(compactor->ctx->writer == NULL);
  SFSetWriterConfig config = {
      .tsdb = compactor->tsdb,
      .toSttOnly = false,
      .compactVersion = compactor->compactVersion,
      .minRow = compactor->minRow,
      .maxRow = compactor->maxRow,
      .szPage = compactor->szPage,
      .cmprAlg = compactor->cmprAlg,
      .fid = compactor->ctx->fset->fid,
      .cid = compactor->cid,
      .did = compactor->ctx->did,
      .level = 0,
  };

  code = tsdbFSetWriterOpen(&config, &compactor->ctx->writer);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCompactFSetCloseWriter(SCompactor2 *compactor) {
  return tsdbFSetWriterClose(&compactor->ctx->writer, 0, compactor->fopArr);
}

static int32_t tsdbCompactFSetBegin(SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t expLevel = 0;
  tsdbFidLevel(compactor->ctx->fset->fid, &compactor->tsdb->keepCfg, taosGetTimestampSec());
  code = tfsAllocDisk(compactor->tsdb->pVnode->pTfs, expLevel, &compactor->ctx->did);
  if (code) {
    code = TAOS_SYSTEM_ERROR(code);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbCompactFSetOpenReader(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCompactFSetOpenIter(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCompactFSetOpenWriter(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  compactor->ctx->tbid->suid = 0;
  compactor->ctx->tbid->uid = 0;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCompactFSetEnd(SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbCompactFSetCloseWriter(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCompactFSetCloseIter(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCompactFSetCloseReader(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbCompactFSetTableDataEnd(SCompactor2 *compactor) {
  // do nothing
  return 0;
}

static int32_t tsdbCompactFSetTableDataBegin(SCompactor2 *compactor, const TABLEID *tbid) {
  int32_t code = 0;
  int32_t lino = 0;

  compactor->ctx->tbid->suid = tbid->suid;
  compactor->ctx->tbid->uid = tbid->uid;

  SArray *delDataArr = NULL;

  for (STombRecord *record; (record = tsdbIterMergerGetTombRecord(compactor->ctx->tombIterMerger)) != NULL;) {
    if (record->suid > tbid->suid || (record->suid == tbid->suid && record->uid > tbid->uid)) {
      break;
    } else {
      if (record->uid == tbid->uid) {
        SDelData delData = {
            .version = record->version,
            .sKey = record->skey,
            .eKey = record->ekey,
        };

        if (delDataArr == NULL && (delDataArr = taosArrayInit(0, sizeof(SDelData))) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          TSDB_CHECK_CODE(code, lino, _exit);
        }

        if (taosArrayPush(delDataArr, &delData) == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }

      code = tsdbIterMergerNext(compactor->ctx->tombIterMerger);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (delDataArr) {
    if (compactor->ctx->aSkyLine == NULL && (compactor->ctx->aSkyLine = taosArrayInit(0, sizeof(TSDBKEY))) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbBuildDeleteSkyline(delDataArr, 0, taosArrayGetSize(delDataArr) - 1, compactor->ctx->aSkyLine);
    TSDB_CHECK_CODE(code, lino, _exit);

    compactor->ctx->iSkyLine = 0;
    TSDBKEY *pKey = (TSDBKEY *)taosArrayGet(compactor->ctx->aSkyLine, compactor->ctx->iSkyLine);
    compactor->ctx->dKey.version = 0;
    compactor->ctx->dKey.ts = pKey->ts;
    compactor->ctx->pDKey = &compactor->ctx->dKey;

    taosArrayDestroy(delDataArr);
  } else {
    compactor->ctx->pDKey = NULL;
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  }
  return code;
}

static bool tsdbRowIsDeleted(SCompactor2 *compactor, TSDBROW *row) {
  TSDBKEY  tKey = TSDBROW_KEY(row);
  TSDBKEY *aKey = (TSDBKEY *)TARRAY_DATA(compactor->ctx->aSkyLine);
  int32_t  nKey = TARRAY_SIZE(compactor->ctx->aSkyLine);

  if (tKey.ts > compactor->ctx->pDKey->ts) {
    do {
      compactor->ctx->pDKey->version = aKey[compactor->ctx->iSkyLine].version;
      compactor->ctx->iSkyLine++;
      if (compactor->ctx->iSkyLine < nKey) {
        compactor->ctx->dKey.ts = aKey[compactor->ctx->iSkyLine].ts;
      } else {
        if (compactor->ctx->pDKey->version == 0) {
          compactor->ctx->pDKey = NULL;
          return false;
        } else {
          compactor->ctx->pDKey->ts = INT64_MAX;
        }
      }
    } while (tKey.ts > compactor->ctx->pDKey->ts);
  }

  if (tKey.ts < compactor->ctx->pDKey->ts) {
    if (tKey.version > compactor->ctx->pDKey->version) {
      return false;
    } else {
      return true;
    }
  } else if (tKey.ts == compactor->ctx->pDKey->ts) {
    ASSERT(compactor->ctx->iSkyLine < nKey);
    if (tKey.version > TMAX(compactor->ctx->pDKey->version, aKey[compactor->ctx->iSkyLine].version)) {
      return false;
    } else {
      return true;
    }
  }

  return false;
}

static int32_t tsdbCompactFSet(SCompactor2 *compactor) {
  int32_t code = 0;
  int32_t lino = 0;

  SMetaInfo info;
  int64_t   numOfRow = 0;
  for (SRowInfo *row; (row = tsdbIterMergerGetData(compactor->ctx->dataIterMerger)) != NULL;) {
    if (row->uid != compactor->ctx->tbid->uid) {
      code = tsdbCompactFSetTableDataEnd(compactor);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (metaGetInfo(compactor->tsdb->pVnode->pMeta, row->uid, &info, NULL) != 0) {
        code = tsdbIterMergerSkipTableData(compactor->ctx->dataIterMerger, (TABLEID *)row);
        TSDB_CHECK_CODE(code, lino, _exit);
        continue;
      }

      code = tsdbCompactFSetTableDataBegin(compactor, (TABLEID *)row);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (compactor->ctx->pDKey == NULL || !tsdbRowIsDeleted(compactor, &row->row)) {
      code = tsdbFSetWriteRow(compactor->ctx->writer, row);
      TSDB_CHECK_CODE(code, lino, _exit);
      numOfRow++;
    }
    code = tsdbIterMergerNext(compactor->ctx->dataIterMerger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactor->tsdb->pVnode), lino, code);
  } else {
    tsdbInfo("vgId:%d fid:%d compact %" PRId64 " rows", TD_VID(compactor->tsdb->pVnode), numOfRow);
  }
  return code;
}

int32_t tsdbDoCompact(void *arg) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SCompactArg *compactArg = (SCompactArg *)arg;

  SCompactor2 compactor[1] = {0};

  code = tsdbCompactBegin(arg, compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  TARRAY2_FOREACH(compactor->fsetArr, compactor->ctx->fset) {
    if (compactor->ctx->fset->fid < compactor->minFid || compactor->ctx->fset->fid > compactor->maxFid) {
      continue;
    }

    // check if the file set should be compacted
    code = tsdbCompactFSetBegin(compactor);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbCompactFSet(compactor);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbCompactFSetEnd(compactor);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbCompactEnd(compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(compactArg->tsdb->pVnode), lino, code);
  }
  if (compactArg->sync) {
    tsem_post(&compactArg->tsdb->pVnode->canCommit);
  }
  taosMemoryFree(compactArg);
  return code;
}

int32_t tsdbAsyncCompact(STsdb *tsdb, const STimeWindow *tw, bool sync) {
  int64_t taskid;

  if (sync) {
    tsem_wait(&tsdb->pVnode->canCommit);
  }

  SCompactArg *arg = (SCompactArg *)taosMemoryCalloc(1, sizeof(*arg));
  if (arg == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  arg->tsdb = tsdb;
  arg->sync = sync;
  arg->tw = *tw;

  int32_t code = tsdbFSScheduleBgTask(tsdb->pFS, TSDB_BG_TASK_COMPACT, tsdbDoCompact, arg, &taskid);
  if (code) {
    taosMemoryFree(arg);
    if (sync) {
      tsem_post(&tsdb->pVnode->canCommit);
    }
  }
  return code;
}