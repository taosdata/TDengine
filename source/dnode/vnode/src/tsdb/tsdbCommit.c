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

typedef enum { MEMORY_DATA_ITER = 0, STT_DATA_ITER } EDataIterT;

#define USE_STREAM_COMPRESSION 0

typedef struct {
  SRBTreeNode n;
  SRowInfo    r;
  EDataIterT  type;
  union {
    struct {
      int32_t     iTbDataP;
      STbDataIter iter;
    };  // memory data iter
    struct {
      int32_t    iStt;
      SArray    *aSttBlk;
      int32_t    iSttBlk;
      SBlockData bData;
      int32_t    iRow;
    };  // stt file data iter
  };
} SDataIter;

typedef struct {
  STsdb *pTsdb;
  /* commit data */
  int64_t commitID;
  int32_t minutes;
  int8_t  precision;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  int8_t  sttTrigger;
  SArray *aTbDataP;  // memory
  STsdbFS fs;        // disk
  // --------------
  TSKEY   nextKey;  // reset by each table commit
  int32_t commitFid;
  int32_t expLevel;
  TSKEY   minKey;
  TSKEY   maxKey;
  // commit file data
  struct {
    SDataFReader *pReader;
    SArray       *aBlockIdx;  // SArray<SBlockIdx>
    int32_t       iBlockIdx;
    SBlockIdx    *pBlockIdx;
    SMapData      mBlock;  // SMapData<SDataBlk>
    SBlockData    bData;
  } dReader;
  struct {
    SDataIter *pIter;
    SRBTree    rbt;
    SDataIter  dataIter;
    SDataIter  aDataIter[TSDB_MAX_STT_TRIGGER];
    int8_t     toLastOnly;
  };
  struct {
    SDataFWriter *pWriter;
    SArray       *aBlockIdx;  // SArray<SBlockIdx>
    SArray       *aSttBlk;    // SArray<SSttBlk>
    SMapData      mBlock;     // SMapData<SDataBlk>
    SBlockData    bData;
#if USE_STREAM_COMPRESSION
    SDiskDataBuilder *pBuilder;
#else
    SBlockData bDatal;
#endif
  } dWriter;
  SSkmInfo skmTable;
  SSkmInfo skmRow;
  /* commit del */
  SDelFReader *pDelFReader;
  SDelFWriter *pDelFWriter;
  SArray      *aDelIdx;   // SArray<SDelIdx>
  SArray      *aDelIdxN;  // SArray<SDelIdx>
  SArray      *aDelData;  // SArray<SDelData>
} SCommitter;

static int32_t tsdbStartCommit(STsdb *pTsdb, SCommitter *pCommitter, SCommitInfo *pInfo);
static int32_t tsdbCommitData(SCommitter *pCommitter);
static int32_t tsdbCommitDel(SCommitter *pCommitter);
static int32_t tsdbCommitCache(SCommitter *pCommitter);
static int32_t tsdbEndCommit(SCommitter *pCommitter, int32_t eno);
static int32_t tsdbNextCommitRow(SCommitter *pCommitter);

int32_t tRowInfoCmprFn(const void *p1, const void *p2) {
  SRowInfo *pInfo1 = (SRowInfo *)p1;
  SRowInfo *pInfo2 = (SRowInfo *)p2;

  if (pInfo1->suid < pInfo2->suid) {
    return -1;
  } else if (pInfo1->suid > pInfo2->suid) {
    return 1;
  }

  if (pInfo1->uid < pInfo2->uid) {
    return -1;
  } else if (pInfo1->uid > pInfo2->uid) {
    return 1;
  }

  return tsdbRowCmprFn(&pInfo1->row, &pInfo2->row);
}

int32_t tsdbBegin(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!pTsdb) return code;

  SMemTable *pMemTable;
  code = tsdbMemTableCreate(pTsdb, &pMemTable);
  TSDB_CHECK_CODE(code, lino, _exit);

  // lock
  if ((code = taosThreadRwlockWrlock(&pTsdb->rwLock))) {
    code = TAOS_SYSTEM_ERROR(code);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pTsdb->mem = pMemTable;

  // unlock
  if ((code = taosThreadRwlockUnlock(&pTsdb->rwLock))) {
    code = TAOS_SYSTEM_ERROR(code);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbPrepareCommit(STsdb *pTsdb) {
  taosThreadRwlockWrlock(&pTsdb->rwLock);
  ASSERT(pTsdb->imem == NULL);
  pTsdb->imem = pTsdb->mem;
  pTsdb->mem = NULL;
  taosThreadRwlockUnlock(&pTsdb->rwLock);

  return 0;
}

int32_t tsdbCommit(STsdb *pTsdb, SCommitInfo *pInfo) {
  if (!pTsdb) return 0;

  int32_t    code = 0;
  int32_t    lino = 0;
  SCommitter commith;
  SMemTable *pMemTable = pTsdb->imem;

  // check
  if (pMemTable->nRow == 0 && pMemTable->nDel == 0) {
    taosThreadRwlockWrlock(&pTsdb->rwLock);
    pTsdb->imem = NULL;
    taosThreadRwlockUnlock(&pTsdb->rwLock);

    tsdbUnrefMemTable(pMemTable, NULL, true);
    goto _exit;
  }

  // start commit
  code = tsdbStartCommit(pTsdb, &commith, pInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

  // commit impl
  code = tsdbCommitData(&commith);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCommitDel(&commith);
  TSDB_CHECK_CODE(code, lino, _exit);

  // end commit
  code = tsdbEndCommit(&commith, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbEndCommit(&commith, code);
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitDelStart(SCommitter *pCommitter) {
  int32_t    code = 0;
  int32_t    lino = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  if ((pCommitter->aDelIdx = taosArrayInit(0, sizeof(SDelIdx))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if ((pCommitter->aDelData = taosArrayInit(0, sizeof(SDelData))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if ((pCommitter->aDelIdxN = taosArrayInit(0, sizeof(SDelIdx))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  SDelFile *pDelFileR = pCommitter->fs.pDelFile;
  if (pDelFileR) {
    code = tsdbDelFReaderOpen(&pCommitter->pDelFReader, pDelFileR, pTsdb);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbReadDelIdx(pCommitter->pDelFReader, pCommitter->aDelIdx);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // prepare new
  SDelFile wDelFile = {.commitID = pCommitter->commitID, .size = 0, .offset = 0};
  code = tsdbDelFWriterOpen(&pCommitter->pDelFWriter, &wDelFile, pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d, commit del start", TD_VID(pTsdb->pVnode));
  }
  return code;
}

static int32_t tsdbCommitTableDel(SCommitter *pCommitter, STbData *pTbData, SDelIdx *pDelIdx) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SDelData *pDelData;
  tb_uid_t  suid;
  tb_uid_t  uid;

  if (pTbData) {
    suid = pTbData->suid;
    uid = pTbData->uid;

    if (pTbData->pHead == NULL) {
      pTbData = NULL;
    }
  }

  if (pDelIdx) {
    suid = pDelIdx->suid;
    uid = pDelIdx->uid;

    code = tsdbReadDelDatav1(pCommitter->pDelFReader, pDelIdx, pCommitter->aDelData, INT64_MAX);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    taosArrayClear(pCommitter->aDelData);
  }

  if (pTbData == NULL && pDelIdx == NULL) goto _exit;

  SDelIdx delIdx = {.suid = suid, .uid = uid};

  // memory
  pDelData = pTbData ? pTbData->pHead : NULL;
  for (; pDelData; pDelData = pDelData->pNext) {
    if (taosArrayPush(pCommitter->aDelData, pDelData) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // write
  code = tsdbWriteDelData(pCommitter->pDelFWriter, pCommitter->aDelData, &delIdx);
  TSDB_CHECK_CODE(code, lino, _exit);

  // put delIdx
  if (taosArrayPush(pCommitter->aDelIdxN, &delIdx) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitDelEnd(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pCommitter->pTsdb;

  code = tsdbWriteDelIdx(pCommitter->pDelFWriter, pCommitter->aDelIdxN);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbUpdateDelFileHdr(pCommitter->pDelFWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFSUpsertDelFile(&pCommitter->fs, &pCommitter->pDelFWriter->fDel);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDelFWriterClose(&pCommitter->pDelFWriter, 1);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pCommitter->pDelFReader) {
    code = tsdbDelFReaderClose(&pCommitter->pDelFReader);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  taosArrayDestroy(pCommitter->aDelIdx);
  taosArrayDestroy(pCommitter->aDelData);
  taosArrayDestroy(pCommitter->aDelIdxN);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbUpdateTableSchema(SMeta *pMeta, int64_t suid, int64_t uid, SSkmInfo *pSkmInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  if (suid) {
    if (pSkmInfo->suid == suid) {
      pSkmInfo->uid = uid;
      goto _exit;
    }
  } else {
    if (pSkmInfo->uid == uid) goto _exit;
  }

  pSkmInfo->suid = suid;
  pSkmInfo->uid = uid;
  tDestroyTSchema(pSkmInfo->pTSchema);
  code = metaGetTbTSchemaEx(pMeta, suid, uid, -1, &pSkmInfo->pTSchema);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  return code;
}

static int32_t tsdbCommitterUpdateRowSchema(SCommitter *pCommitter, int64_t suid, int64_t uid, int32_t sver) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pCommitter->skmRow.pTSchema) {
    if (pCommitter->skmRow.suid == suid) {
      if (suid == 0) {
        if (pCommitter->skmRow.uid == uid && sver == pCommitter->skmRow.pTSchema->version) goto _exit;
      } else {
        if (sver == pCommitter->skmRow.pTSchema->version) goto _exit;
      }
    }
  }

  pCommitter->skmRow.suid = suid;
  pCommitter->skmRow.uid = uid;
  tDestroyTSchema(pCommitter->skmRow.pTSchema);
  code = metaGetTbTSchemaEx(pCommitter->pTsdb->pVnode->pMeta, suid, uid, sver, &pCommitter->skmRow.pTSchema);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  return code;
}

static int32_t tsdbCommitterNextTableData(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(pCommitter->dReader.pBlockIdx);

  pCommitter->dReader.iBlockIdx++;
  if (pCommitter->dReader.iBlockIdx < taosArrayGetSize(pCommitter->dReader.aBlockIdx)) {
    pCommitter->dReader.pBlockIdx =
        (SBlockIdx *)taosArrayGet(pCommitter->dReader.aBlockIdx, pCommitter->dReader.iBlockIdx);

    code = tsdbReadDataBlk(pCommitter->dReader.pReader, pCommitter->dReader.pBlockIdx, &pCommitter->dReader.mBlock);
    TSDB_CHECK_CODE(code, lino, _exit);

    ASSERT(pCommitter->dReader.mBlock.nItem > 0);
  } else {
    pCommitter->dReader.pBlockIdx = NULL;
  }

_exit:
  return code;
}

static int32_t tDataIterCmprFn(const SRBTreeNode *n1, const SRBTreeNode *n2) {
  SDataIter *pIter1 = (SDataIter *)((uint8_t *)n1 - offsetof(SDataIter, n));
  SDataIter *pIter2 = (SDataIter *)((uint8_t *)n2 - offsetof(SDataIter, n));

  return tRowInfoCmprFn(&pIter1->r, &pIter2->r);
}

static int32_t tsdbOpenCommitIter(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  pCommitter->pIter = NULL;
  tRBTreeCreate(&pCommitter->rbt, tDataIterCmprFn);

  // memory
  TSDBKEY    tKey = {.ts = pCommitter->minKey, .version = VERSION_MIN};
  SDataIter *pIter = &pCommitter->dataIter;
  pIter->type = MEMORY_DATA_ITER;
  pIter->iTbDataP = 0;
  for (; pIter->iTbDataP < taosArrayGetSize(pCommitter->aTbDataP); pIter->iTbDataP++) {
    STbData *pTbData = (STbData *)taosArrayGetP(pCommitter->aTbDataP, pIter->iTbDataP);
    tsdbTbDataIterOpen(pTbData, &tKey, 0, &pIter->iter);
    TSDBROW *pRow = tsdbTbDataIterGet(&pIter->iter);
    if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) {
      pCommitter->nextKey = TMIN(pCommitter->nextKey, TSDBROW_TS(pRow));
      pRow = NULL;
    }

    if (pRow == NULL) continue;

    pIter->r.suid = pTbData->suid;
    pIter->r.uid = pTbData->uid;
    pIter->r.row = *pRow;
    break;
  }
  ASSERT(pIter->iTbDataP < taosArrayGetSize(pCommitter->aTbDataP));
  tRBTreePut(&pCommitter->rbt, (SRBTreeNode *)pIter);

  // disk
  pCommitter->toLastOnly = 0;
  SDataFReader *pReader = pCommitter->dReader.pReader;
  if (pReader) {
    if (pReader->pSet->nSttF >= pCommitter->sttTrigger) {
      int8_t iIter = 0;
      for (int32_t iStt = 0; iStt < pReader->pSet->nSttF; iStt++) {
        pIter = &pCommitter->aDataIter[iIter];
        pIter->type = STT_DATA_ITER;
        pIter->iStt = iStt;

        code = tsdbReadSttBlk(pCommitter->dReader.pReader, iStt, pIter->aSttBlk);
        TSDB_CHECK_CODE(code, lino, _exit);

        if (taosArrayGetSize(pIter->aSttBlk) == 0) continue;

        pIter->iSttBlk = 0;
        SSttBlk *pSttBlk = (SSttBlk *)taosArrayGet(pIter->aSttBlk, 0);
        code = tsdbReadSttBlockEx(pCommitter->dReader.pReader, iStt, pSttBlk, &pIter->bData);
        TSDB_CHECK_CODE(code, lino, _exit);

        pIter->iRow = 0;
        pIter->r.suid = pIter->bData.suid;
        pIter->r.uid = pIter->bData.uid ? pIter->bData.uid : pIter->bData.aUid[0];
        pIter->r.row = tsdbRowFromBlockData(&pIter->bData, 0);

        tRBTreePut(&pCommitter->rbt, (SRBTreeNode *)pIter);
        iIter++;
      }
    } else {
      for (int32_t iStt = 0; iStt < pReader->pSet->nSttF; iStt++) {
        SSttFile *pSttFile = pReader->pSet->aSttF[iStt];
        if (pSttFile->size > pSttFile->offset) {
          pCommitter->toLastOnly = 1;
          break;
        }
      }
    }
  }

  code = tsdbNextCommitRow(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitFileDataStart(SCommitter *pCommitter) {
  int32_t    code = 0;
  int32_t    lino = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SDFileSet *pRSet = NULL;

  // memory
  pCommitter->commitFid = tsdbKeyFid(pCommitter->nextKey, pCommitter->minutes, pCommitter->precision);
  pCommitter->expLevel = tsdbFidLevel(pCommitter->commitFid, &pCommitter->pTsdb->keepCfg, taosGetTimestampSec());
  tsdbFidKeyRange(pCommitter->commitFid, pCommitter->minutes, pCommitter->precision, &pCommitter->minKey,
                  &pCommitter->maxKey);
#if 0
  ASSERT(pCommitter->minKey <= pCommitter->nextKey && pCommitter->maxKey >= pCommitter->nextKey);
#endif

  pCommitter->nextKey = TSKEY_MAX;

  // Reader
  SDFileSet tDFileSet = {.fid = pCommitter->commitFid};
  pRSet = (SDFileSet *)taosArraySearch(pCommitter->fs.aDFileSet, &tDFileSet, tDFileSetCmprFn, TD_EQ);
  if (pRSet) {
    code = tsdbDataFReaderOpen(&pCommitter->dReader.pReader, pTsdb, pRSet);
    TSDB_CHECK_CODE(code, lino, _exit);

    // data
    code = tsdbReadBlockIdx(pCommitter->dReader.pReader, pCommitter->dReader.aBlockIdx);
    TSDB_CHECK_CODE(code, lino, _exit);

    pCommitter->dReader.iBlockIdx = 0;
    if (taosArrayGetSize(pCommitter->dReader.aBlockIdx) > 0) {
      pCommitter->dReader.pBlockIdx = (SBlockIdx *)taosArrayGet(pCommitter->dReader.aBlockIdx, 0);
      code = tsdbReadDataBlk(pCommitter->dReader.pReader, pCommitter->dReader.pBlockIdx, &pCommitter->dReader.mBlock);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      pCommitter->dReader.pBlockIdx = NULL;
    }
    tBlockDataReset(&pCommitter->dReader.bData);
  } else {
    pCommitter->dReader.pBlockIdx = NULL;
  }

  // Writer
  SHeadFile fHead = {.commitID = pCommitter->commitID};
  SDataFile fData = {.commitID = pCommitter->commitID};
  SSmaFile  fSma = {.commitID = pCommitter->commitID};
  SSttFile  fStt = {.commitID = pCommitter->commitID};
  SDFileSet wSet = {.fid = pCommitter->commitFid, .pHeadF = &fHead, .pDataF = &fData, .pSmaF = &fSma};
  if (pRSet) {
    ASSERT(pRSet->nSttF <= pCommitter->sttTrigger);
    fData = *pRSet->pDataF;
    fSma = *pRSet->pSmaF;
    wSet.diskId = pRSet->diskId;
    if (pRSet->nSttF < pCommitter->sttTrigger) {
      for (int32_t iStt = 0; iStt < pRSet->nSttF; iStt++) {
        wSet.aSttF[iStt] = pRSet->aSttF[iStt];
      }
      wSet.nSttF = pRSet->nSttF + 1;
    } else {
      wSet.nSttF = 1;
    }
  } else {
    SDiskID did = {0};
    if (tfsAllocDisk(pTsdb->pVnode->pTfs, pCommitter->expLevel, &did) < 0) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    tfsMkdirRecurAt(pTsdb->pVnode->pTfs, pTsdb->path, did);
    wSet.diskId = did;
    wSet.nSttF = 1;
  }
  wSet.aSttF[wSet.nSttF - 1] = &fStt;
  code = tsdbDataFWriterOpen(&pCommitter->dWriter.pWriter, pTsdb, &wSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  taosArrayClear(pCommitter->dWriter.aBlockIdx);
  taosArrayClear(pCommitter->dWriter.aSttBlk);
  tMapDataReset(&pCommitter->dWriter.mBlock);
  tBlockDataReset(&pCommitter->dWriter.bData);
#if USE_STREAM_COMPRESSION
  tDiskDataBuilderClear(pCommitter->dWriter.pBuilder);
#else
  tBlockDataReset(&pCommitter->dWriter.bDatal);
#endif

  // open iter
  code = tsdbOpenCommitIter(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbWriteDataBlock(SDataFWriter *pWriter, SBlockData *pBlockData, SMapData *mDataBlk, int8_t cmprAlg) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pBlockData->nRow == 0) return code;

  SDataBlk dataBlk;
  tDataBlkReset(&dataBlk);

  // info
  dataBlk.nRow += pBlockData->nRow;
  for (int32_t iRow = 0; iRow < pBlockData->nRow; iRow++) {
    TSDBKEY key = {.ts = pBlockData->aTSKEY[iRow], .version = pBlockData->aVersion[iRow]};

    if (iRow == 0) {
      if (tsdbKeyCmprFn(&dataBlk.minKey, &key) > 0) {
        dataBlk.minKey = key;
      }
    } else {
      if (pBlockData->aTSKEY[iRow] == pBlockData->aTSKEY[iRow - 1]) {
        dataBlk.hasDup = 1;
      }
    }

    if (iRow == pBlockData->nRow - 1 && tsdbKeyCmprFn(&dataBlk.maxKey, &key) < 0) {
      dataBlk.maxKey = key;
    }

    dataBlk.minVer = TMIN(dataBlk.minVer, key.version);
    dataBlk.maxVer = TMAX(dataBlk.maxVer, key.version);
  }

  // write
  dataBlk.nSubBlock++;
  code = tsdbWriteBlockData(pWriter, pBlockData, &dataBlk.aSubBlock[dataBlk.nSubBlock - 1],
                            ((dataBlk.nSubBlock == 1) && !dataBlk.hasDup) ? &dataBlk.smaInfo : NULL, cmprAlg, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // put SDataBlk
  code = tMapDataPutItem(mDataBlk, &dataBlk, tPutDataBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  // clear
  tBlockDataClear(pBlockData);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbWriteSttBlock(SDataFWriter *pWriter, SBlockData *pBlockData, SArray *aSttBlk, int8_t cmprAlg) {
  int32_t code = 0;
  int32_t lino = 0;
  SSttBlk sstBlk;

  if (pBlockData->nRow == 0) return code;

  // info
  sstBlk.suid = pBlockData->suid;
  sstBlk.nRow = pBlockData->nRow;
  sstBlk.minKey = TSKEY_MAX;
  sstBlk.maxKey = TSKEY_MIN;
  sstBlk.minVer = VERSION_MAX;
  sstBlk.maxVer = VERSION_MIN;
  for (int32_t iRow = 0; iRow < pBlockData->nRow; iRow++) {
    sstBlk.minKey = TMIN(sstBlk.minKey, pBlockData->aTSKEY[iRow]);
    sstBlk.maxKey = TMAX(sstBlk.maxKey, pBlockData->aTSKEY[iRow]);
    sstBlk.minVer = TMIN(sstBlk.minVer, pBlockData->aVersion[iRow]);
    sstBlk.maxVer = TMAX(sstBlk.maxVer, pBlockData->aVersion[iRow]);
  }
  sstBlk.minUid = pBlockData->uid ? pBlockData->uid : pBlockData->aUid[0];
  sstBlk.maxUid = pBlockData->uid ? pBlockData->uid : pBlockData->aUid[pBlockData->nRow - 1];

  // write
  code = tsdbWriteBlockData(pWriter, pBlockData, &sstBlk.bInfo, NULL, cmprAlg, 1);
  TSDB_CHECK_CODE(code, lino, _exit);

  // push SSttBlk
  if (taosArrayPush(aSttBlk, &sstBlk) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // clear
  tBlockDataClear(pBlockData);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitSttBlk(SDataFWriter *pWriter, SDiskDataBuilder *pBuilder, SArray *aSttBlk) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pBuilder->nRow == 0) return code;

  // gnrt
  const SDiskData *pDiskData;
  const SBlkInfo  *pBlkInfo;
  code = tGnrtDiskData(pBuilder, &pDiskData, &pBlkInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

  SSttBlk sttBlk = {.suid = pBuilder->suid,
                    .minUid = pBlkInfo->minUid,
                    .maxUid = pBlkInfo->maxUid,
                    .minKey = pBlkInfo->minKey,
                    .maxKey = pBlkInfo->maxKey,
                    .minVer = pBlkInfo->minVer,
                    .maxVer = pBlkInfo->maxVer,
                    .nRow = pBuilder->nRow};
  // write
  code = tsdbWriteDiskData(pWriter, pDiskData, &sttBlk.bInfo, NULL);
  TSDB_CHECK_CODE(code, lino, _exit);

  // push
  if (taosArrayPush(aSttBlk, &sttBlk) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // clear
  tDiskDataBuilderClear(pBuilder);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitFileDataEnd(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  // write aBlockIdx
  code = tsdbWriteBlockIdx(pCommitter->dWriter.pWriter, pCommitter->dWriter.aBlockIdx);
  TSDB_CHECK_CODE(code, lino, _exit);

  // write aSttBlk
  code = tsdbWriteSttBlk(pCommitter->dWriter.pWriter, pCommitter->dWriter.aSttBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  // update file header
  code = tsdbUpdateDFileSetHeader(pCommitter->dWriter.pWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // upsert SDFileSet
  code = tsdbFSUpsertFSet(&pCommitter->fs, &pCommitter->dWriter.pWriter->wSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  // close and sync
  code = tsdbDataFWriterClose(&pCommitter->dWriter.pWriter, 1);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pCommitter->dReader.pReader) {
    code = tsdbDataFReaderClose(&pCommitter->dReader.pReader);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbMoveCommitData(SCommitter *pCommitter, TABLEID toTable) {
  int32_t code = 0;
  int32_t lino = 0;

  while (pCommitter->dReader.pBlockIdx && tTABLEIDCmprFn(pCommitter->dReader.pBlockIdx, &toTable) < 0) {
    SBlockIdx blockIdx = *pCommitter->dReader.pBlockIdx;
    code = tsdbWriteDataBlk(pCommitter->dWriter.pWriter, &pCommitter->dReader.mBlock, &blockIdx);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (taosArrayPush(pCommitter->dWriter.aBlockIdx, &blockIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbCommitterNextTableData(pCommitter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitFileDataImpl(SCommitter *pCommitter);
static int32_t tsdbCommitFileData(SCommitter *pCommitter) {
  int32_t    code = 0;
  int32_t    lino = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  // commit file data start
  code = tsdbCommitFileDataStart(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // impl
  code = tsdbCommitFileDataImpl(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // commit file data end
  code = tsdbCommitFileDataEnd(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    tsdbDataFReaderClose(&pCommitter->dReader.pReader);
    tsdbDataFWriterClose(&pCommitter->dWriter.pWriter, 0);
  }
  return code;
}

// ----------------------------------------------------------------------------
static int32_t tsdbStartCommit(STsdb *pTsdb, SCommitter *pCommitter, SCommitInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  memset(pCommitter, 0, sizeof(*pCommitter));
  ASSERT(pTsdb->imem && "last tsdb commit incomplete");

  pCommitter->pTsdb = pTsdb;
  pCommitter->commitID = pInfo->info.state.commitID;
  pCommitter->minutes = pTsdb->keepCfg.days;
  pCommitter->precision = pTsdb->keepCfg.precision;
  pCommitter->minRow = pInfo->info.config.tsdbCfg.minRows;
  pCommitter->maxRow = pInfo->info.config.tsdbCfg.maxRows;
  pCommitter->cmprAlg = pInfo->info.config.tsdbCfg.compression;
  pCommitter->sttTrigger = pInfo->info.config.sttTrigger;
  pCommitter->aTbDataP = tsdbMemTableGetTbDataArray(pTsdb->imem);
  if (pCommitter->aTbDataP == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  code = tsdbFSCopy(pTsdb, &pCommitter->fs);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitDataStart(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  // reader
  pCommitter->dReader.aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pCommitter->dReader.aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataCreate(&pCommitter->dReader.bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  // merger
  for (int32_t iStt = 0; iStt < TSDB_MAX_STT_TRIGGER; iStt++) {
    SDataIter *pIter = &pCommitter->aDataIter[iStt];
    pIter->aSttBlk = taosArrayInit(0, sizeof(SSttBlk));
    if (pIter->aSttBlk == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tBlockDataCreate(&pIter->bData);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // writer
  pCommitter->dWriter.aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pCommitter->dWriter.aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pCommitter->dWriter.aSttBlk = taosArrayInit(0, sizeof(SSttBlk));
  if (pCommitter->dWriter.aSttBlk == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataCreate(&pCommitter->dWriter.bData);
  TSDB_CHECK_CODE(code, lino, _exit);

#if USE_STREAM_COMPRESSION
  code = tDiskDataBuilderCreate(&pCommitter->dWriter.pBuilder);
#else
  code = tBlockDataCreate(&pCommitter->dWriter.bDatal);
#endif
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static void tsdbCommitDataEnd(SCommitter *pCommitter) {
  // reader
  taosArrayDestroy(pCommitter->dReader.aBlockIdx);
  tMapDataClear(&pCommitter->dReader.mBlock);
  tBlockDataDestroy(&pCommitter->dReader.bData);

  // merger
  for (int32_t iStt = 0; iStt < TSDB_MAX_STT_TRIGGER; iStt++) {
    SDataIter *pIter = &pCommitter->aDataIter[iStt];
    taosArrayDestroy(pIter->aSttBlk);
    tBlockDataDestroy(&pIter->bData);
  }

  // writer
  taosArrayDestroy(pCommitter->dWriter.aBlockIdx);
  taosArrayDestroy(pCommitter->dWriter.aSttBlk);
  tMapDataClear(&pCommitter->dWriter.mBlock);
  tBlockDataDestroy(&pCommitter->dWriter.bData);
#if USE_STREAM_COMPRESSION
  tDiskDataBuilderDestroy(pCommitter->dWriter.pBuilder);
#else
  tBlockDataDestroy(&pCommitter->dWriter.bDatal);
#endif
  tDestroyTSchema(pCommitter->skmTable.pTSchema);
  tDestroyTSchema(pCommitter->skmRow.pTSchema);
}

static int32_t tsdbCommitData(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  // check
  if (pMemTable->nRow == 0) goto _exit;

  // start ====================
  code = tsdbCommitDataStart(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

  // impl ====================
  pCommitter->nextKey = pMemTable->minKey;
  while (pCommitter->nextKey < TSKEY_MAX) {
    code = tsdbCommitFileData(pCommitter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // end ====================
  tsdbCommitDataEnd(pCommitter);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitDel(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  if (pMemTable->nDel == 0) {
    goto _exit;
  }

  // start
  code = tsdbCommitDelStart(pCommitter);
  if (code) {
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // impl
  int32_t  iDelIdx = 0;
  int32_t  nDelIdx = taosArrayGetSize(pCommitter->aDelIdx);
  int32_t  iTbData = 0;
  int32_t  nTbData = taosArrayGetSize(pCommitter->aTbDataP);
  STbData *pTbData;
  SDelIdx *pDelIdx;

  ASSERT(nTbData > 0);

  pTbData = (STbData *)taosArrayGetP(pCommitter->aTbDataP, iTbData);
  pDelIdx = (iDelIdx < nDelIdx) ? (SDelIdx *)taosArrayGet(pCommitter->aDelIdx, iDelIdx) : NULL;
  while (true) {
    if (pTbData == NULL && pDelIdx == NULL) break;

    if (pTbData && pDelIdx) {
      int32_t c = tTABLEIDCmprFn(pTbData, pDelIdx);

      if (c == 0) {
        goto _commit_mem_and_disk_del;
      } else if (c < 0) {
        goto _commit_mem_del;
      } else {
        goto _commit_disk_del;
      }
    } else if (pTbData) {
      goto _commit_mem_del;
    } else {
      goto _commit_disk_del;
    }

  _commit_mem_del:
    code = tsdbCommitTableDel(pCommitter, pTbData, NULL);
    TSDB_CHECK_CODE(code, lino, _exit);

    iTbData++;
    pTbData = (iTbData < nTbData) ? (STbData *)taosArrayGetP(pCommitter->aTbDataP, iTbData) : NULL;
    continue;

  _commit_disk_del:
    code = tsdbCommitTableDel(pCommitter, NULL, pDelIdx);
    TSDB_CHECK_CODE(code, lino, _exit);

    iDelIdx++;
    pDelIdx = (iDelIdx < nDelIdx) ? (SDelIdx *)taosArrayGet(pCommitter->aDelIdx, iDelIdx) : NULL;
    continue;

  _commit_mem_and_disk_del:
    code = tsdbCommitTableDel(pCommitter, pTbData, pDelIdx);
    TSDB_CHECK_CODE(code, lino, _exit);

    iTbData++;
    pTbData = (iTbData < nTbData) ? (STbData *)taosArrayGetP(pCommitter->aTbDataP, iTbData) : NULL;
    iDelIdx++;
    pDelIdx = (iDelIdx < nDelIdx) ? (SDelIdx *)taosArrayGet(pCommitter->aDelIdx, iDelIdx) : NULL;
    continue;
  }

  // end
  code = tsdbCommitDelEnd(pCommitter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d, commit del done, nDel:%" PRId64, TD_VID(pTsdb->pVnode), pMemTable->nDel);
  }
  return code;
}

static int32_t tsdbEndCommit(SCommitter *pCommitter, int32_t eno) {
  int32_t code = 0;
  int32_t lino = 0;
  STsdb  *pTsdb = pCommitter->pTsdb;

  if (eno) {
    code = eno;
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    code = tsdbFSPrepareCommit(pCommitter->pTsdb, &pCommitter->fs);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  tsdbFSDestroy(&pCommitter->fs);
  taosArrayDestroy(pCommitter->aTbDataP);
  pCommitter->aTbDataP = NULL;
  if (code || eno) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d, tsdb end commit", TD_VID(pTsdb->pVnode));
  }
  return code;
}

// ================================================================================

static FORCE_INLINE SRowInfo *tsdbGetCommitRow(SCommitter *pCommitter) {
  return (pCommitter->pIter) ? &pCommitter->pIter->r : NULL;
}

static int32_t tsdbNextCommitRow(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pCommitter->pIter) {
    SDataIter *pIter = pCommitter->pIter;
    if (pCommitter->pIter->type == MEMORY_DATA_ITER) {  // memory
      tsdbTbDataIterNext(&pIter->iter);
      TSDBROW *pRow = tsdbTbDataIterGet(&pIter->iter);
      while (true) {
        if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) {
          pCommitter->nextKey = TMIN(pCommitter->nextKey, TSDBROW_TS(pRow));
          pRow = NULL;
        }

        if (pRow) {
          pIter->r.suid = pIter->iter.pTbData->suid;
          pIter->r.uid = pIter->iter.pTbData->uid;
          pIter->r.row = *pRow;
          break;
        }

        pIter->iTbDataP++;
        if (pIter->iTbDataP < taosArrayGetSize(pCommitter->aTbDataP)) {
          STbData *pTbData = (STbData *)taosArrayGetP(pCommitter->aTbDataP, pIter->iTbDataP);
          TSDBKEY  keyFrom = {.ts = pCommitter->minKey, .version = VERSION_MIN};
          tsdbTbDataIterOpen(pTbData, &keyFrom, 0, &pIter->iter);
          pRow = tsdbTbDataIterGet(&pIter->iter);
          continue;
        } else {
          pCommitter->pIter = NULL;
          break;
        }
      }
    } else if (pCommitter->pIter->type == STT_DATA_ITER) {  // last file
      pIter->iRow++;
      if (pIter->iRow < pIter->bData.nRow) {
        pIter->r.uid = pIter->bData.uid ? pIter->bData.uid : pIter->bData.aUid[pIter->iRow];
        pIter->r.row = tsdbRowFromBlockData(&pIter->bData, pIter->iRow);
      } else {
        pIter->iSttBlk++;
        if (pIter->iSttBlk < taosArrayGetSize(pIter->aSttBlk)) {
          SSttBlk *pSttBlk = (SSttBlk *)taosArrayGet(pIter->aSttBlk, pIter->iSttBlk);

          code = tsdbReadSttBlockEx(pCommitter->dReader.pReader, pIter->iStt, pSttBlk, &pIter->bData);
          if (code) goto _exit;

          pIter->iRow = 0;
          pIter->r.suid = pIter->bData.suid;
          pIter->r.uid = pIter->bData.uid ? pIter->bData.uid : pIter->bData.aUid[0];
          pIter->r.row = tsdbRowFromBlockData(&pIter->bData, 0);
        } else {
          pCommitter->pIter = NULL;
        }
      }
    } else {
      ASSERT(0);
    }

    // compare with min in RB Tree
    pIter = (SDataIter *)tRBTreeMin(&pCommitter->rbt);
    if (pCommitter->pIter && pIter) {
      int32_t c = tRowInfoCmprFn(&pCommitter->pIter->r, &pIter->r);
      if (c > 0) {
        tRBTreePut(&pCommitter->rbt, (SRBTreeNode *)pCommitter->pIter);
        pCommitter->pIter = NULL;
      } else {
        ASSERT(c);
      }
    }
  }

  if (pCommitter->pIter == NULL) {
    pCommitter->pIter = (SDataIter *)tRBTreeMin(&pCommitter->rbt);
    if (pCommitter->pIter) {
      tRBTreeDrop(&pCommitter->rbt, (SRBTreeNode *)pCommitter->pIter);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitAheadBlock(SCommitter *pCommitter, SDataBlk *pDataBlk) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlockData *pBlockData = &pCommitter->dWriter.bData;
  SRowInfo   *pRowInfo = tsdbGetCommitRow(pCommitter);
  TABLEID     id = {.suid = pRowInfo->suid, .uid = pRowInfo->uid};

  tBlockDataClear(pBlockData);
  while (pRowInfo) {
    code = tsdbCommitterUpdateRowSchema(pCommitter, id.suid, id.uid, TSDBROW_SVERSION(&pRowInfo->row));
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tBlockDataAppendRow(pBlockData, &pRowInfo->row, pCommitter->skmRow.pTSchema, id.uid);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbNextCommitRow(pCommitter);
    TSDB_CHECK_CODE(code, lino, _exit);

    pRowInfo = tsdbGetCommitRow(pCommitter);
    if (pRowInfo) {
      if (pRowInfo->suid != id.suid || pRowInfo->uid != id.uid) {
        pRowInfo = NULL;
      } else {
        TSDBKEY tKey = TSDBROW_KEY(&pRowInfo->row);
        if (tsdbKeyCmprFn(&tKey, &pDataBlk->minKey) >= 0) pRowInfo = NULL;
      }
    }

    if (pBlockData->nRow >= pCommitter->maxRow) {
      code =
          tsdbWriteDataBlock(pCommitter->dWriter.pWriter, pBlockData, &pCommitter->dWriter.mBlock, pCommitter->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  code = tsdbWriteDataBlock(pCommitter->dWriter.pWriter, pBlockData, &pCommitter->dWriter.mBlock, pCommitter->cmprAlg);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitMergeBlock(SCommitter *pCommitter, SDataBlk *pDataBlk) {
  int32_t code = 0;
  int32_t lino = 0;

  SRowInfo   *pRowInfo = tsdbGetCommitRow(pCommitter);
  TABLEID     id = {.suid = pRowInfo->suid, .uid = pRowInfo->uid};
  SBlockData *pBDataR = &pCommitter->dReader.bData;
  SBlockData *pBDataW = &pCommitter->dWriter.bData;

  code = tsdbReadDataBlock(pCommitter->dReader.pReader, pDataBlk, pBDataR);
  TSDB_CHECK_CODE(code, lino, _exit);

  tBlockDataClear(pBDataW);
  int32_t  iRow = 0;
  TSDBROW  row = tsdbRowFromBlockData(pBDataR, 0);
  TSDBROW *pRow = &row;

  while (pRow && pRowInfo) {
    int32_t c = tsdbRowCmprFn(pRow, &pRowInfo->row);
    if (c < 0) {
      code = tBlockDataAppendRow(pBDataW, pRow, NULL, id.uid);
      TSDB_CHECK_CODE(code, lino, _exit);

      iRow++;
      if (iRow < pBDataR->nRow) {
        row = tsdbRowFromBlockData(pBDataR, iRow);
      } else {
        pRow = NULL;
      }
    } else if (c > 0) {
      code = tsdbCommitterUpdateRowSchema(pCommitter, id.suid, id.uid, TSDBROW_SVERSION(&pRowInfo->row));
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tBlockDataAppendRow(pBDataW, &pRowInfo->row, pCommitter->skmRow.pTSchema, id.uid);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbNextCommitRow(pCommitter);
      TSDB_CHECK_CODE(code, lino, _exit);

      pRowInfo = tsdbGetCommitRow(pCommitter);
      if (pRowInfo) {
        if (pRowInfo->suid != id.suid || pRowInfo->uid != id.uid) {
          pRowInfo = NULL;
        } else {
          TSDBKEY tKey = TSDBROW_KEY(&pRowInfo->row);
          if (tsdbKeyCmprFn(&tKey, &pDataBlk->maxKey) > 0) pRowInfo = NULL;
        }
      }
    } else {
      ASSERT(0 && "dup rows not allowed");
    }

    if (pBDataW->nRow >= pCommitter->maxRow) {
      code = tsdbWriteDataBlock(pCommitter->dWriter.pWriter, pBDataW, &pCommitter->dWriter.mBlock, pCommitter->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  while (pRow) {
    code = tBlockDataAppendRow(pBDataW, pRow, NULL, id.uid);
    TSDB_CHECK_CODE(code, lino, _exit);

    iRow++;
    if (iRow < pBDataR->nRow) {
      row = tsdbRowFromBlockData(pBDataR, iRow);
    } else {
      pRow = NULL;
    }

    if (pBDataW->nRow >= pCommitter->maxRow) {
      code = tsdbWriteDataBlock(pCommitter->dWriter.pWriter, pBDataW, &pCommitter->dWriter.mBlock, pCommitter->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  code = tsdbWriteDataBlock(pCommitter->dWriter.pWriter, pBDataW, &pCommitter->dWriter.mBlock, pCommitter->cmprAlg);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbMergeTableData(SCommitter *pCommitter, TABLEID id) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlockIdx *pBlockIdx = pCommitter->dReader.pBlockIdx;

  ASSERT(pBlockIdx == NULL || tTABLEIDCmprFn(pBlockIdx, &id) >= 0);
  if (pBlockIdx && pBlockIdx->suid == id.suid && pBlockIdx->uid == id.uid) {
    int32_t   iBlock = 0;
    SDataBlk  block;
    SDataBlk *pDataBlk = &block;
    SRowInfo *pRowInfo = tsdbGetCommitRow(pCommitter);

    ASSERT(pRowInfo->suid == id.suid && pRowInfo->uid == id.uid);

    tMapDataGetItemByIdx(&pCommitter->dReader.mBlock, iBlock, pDataBlk, tGetDataBlk);
    while (pDataBlk && pRowInfo) {
      SDataBlk tBlock = {.minKey = TSDBROW_KEY(&pRowInfo->row), .maxKey = TSDBROW_KEY(&pRowInfo->row)};
      int32_t  c = tDataBlkCmprFn(pDataBlk, &tBlock);

      if (c < 0) {
        code = tMapDataPutItem(&pCommitter->dWriter.mBlock, pDataBlk, tPutDataBlk);
        TSDB_CHECK_CODE(code, lino, _exit);

        iBlock++;
        if (iBlock < pCommitter->dReader.mBlock.nItem) {
          tMapDataGetItemByIdx(&pCommitter->dReader.mBlock, iBlock, pDataBlk, tGetDataBlk);
        } else {
          pDataBlk = NULL;
        }
      } else if (c > 0) {
        code = tsdbCommitAheadBlock(pCommitter, pDataBlk);
        TSDB_CHECK_CODE(code, lino, _exit);

        pRowInfo = tsdbGetCommitRow(pCommitter);
        if (pRowInfo && (pRowInfo->suid != id.suid || pRowInfo->uid != id.uid)) pRowInfo = NULL;
      } else {
        code = tsdbCommitMergeBlock(pCommitter, pDataBlk);
        TSDB_CHECK_CODE(code, lino, _exit);

        iBlock++;
        if (iBlock < pCommitter->dReader.mBlock.nItem) {
          tMapDataGetItemByIdx(&pCommitter->dReader.mBlock, iBlock, pDataBlk, tGetDataBlk);
        } else {
          pDataBlk = NULL;
        }
        pRowInfo = tsdbGetCommitRow(pCommitter);
        if (pRowInfo && (pRowInfo->suid != id.suid || pRowInfo->uid != id.uid)) pRowInfo = NULL;
      }
    }

    while (pDataBlk) {
      code = tMapDataPutItem(&pCommitter->dWriter.mBlock, pDataBlk, tPutDataBlk);
      TSDB_CHECK_CODE(code, lino, _exit);

      iBlock++;
      if (iBlock < pCommitter->dReader.mBlock.nItem) {
        tMapDataGetItemByIdx(&pCommitter->dReader.mBlock, iBlock, pDataBlk, tGetDataBlk);
      } else {
        pDataBlk = NULL;
      }
    }

    code = tsdbCommitterNextTableData(pCommitter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbInitSttBlockBuilderIfNeed(SCommitter *pCommitter, TABLEID id) {
  int32_t code = 0;
  int32_t lino = 0;

#if USE_STREAM_COMPRESSION
  SDiskDataBuilder *pBuilder = pCommitter->dWriter.pBuilder;
  if (pBuilder->suid || pBuilder->uid) {
    if (!TABLE_SAME_SCHEMA(pBuilder->suid, pBuilder->uid, id.suid, id.uid)) {
      code = tsdbCommitSttBlk(pCommitter->dWriter.pWriter, pBuilder, pCommitter->dWriter.aSttBlk);
      TSDB_CHECK_CODE(code, lino, _exit);

      tDiskDataBuilderClear(pBuilder);
    }
  }

  if (!pBuilder->suid && !pBuilder->uid) {
    ASSERT(pCommitter->skmTable.suid == id.suid);
    ASSERT(pCommitter->skmTable.uid == id.uid);
    code = tDiskDataBuilderInit(pBuilder, pCommitter->skmTable.pTSchema, &id, pCommitter->cmprAlg, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
#else
  SBlockData *pBData = &pCommitter->dWriter.bDatal;
  if (pBData->suid || pBData->uid) {
    if (!TABLE_SAME_SCHEMA(pBData->suid, pBData->uid, id.suid, id.uid)) {
      code = tsdbWriteSttBlock(pCommitter->dWriter.pWriter, pBData, pCommitter->dWriter.aSttBlk, pCommitter->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);

      tBlockDataReset(pBData);
    }
  }

  if (!pBData->suid && !pBData->uid) {
    ASSERT(pCommitter->skmTable.suid == id.suid);
    ASSERT(pCommitter->skmTable.uid == id.uid);
    TABLEID tid = {.suid = id.suid, .uid = id.suid ? 0 : id.uid};
    code = tBlockDataInit(pBData, &tid, pCommitter->skmTable.pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
#endif

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbAppendLastBlock(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlockData *pBData = &pCommitter->dWriter.bData;
  TABLEID     id = {.suid = pBData->suid, .uid = pBData->uid};

  code = tsdbInitSttBlockBuilderIfNeed(pCommitter, id);
  TSDB_CHECK_CODE(code, lino, _exit);

  for (int32_t iRow = 0; iRow < pBData->nRow; iRow++) {
    TSDBROW row = tsdbRowFromBlockData(pBData, iRow);

#if USE_STREAM_COMPRESSION
    code = tDiskDataAddRow(pCommitter->dWriter.pBuilder, &row, NULL, &id);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pCommitter->dWriter.pBuilder->nRow >= pCommitter->maxRow) {
      code = tsdbCommitSttBlk(pCommitter->dWriter.pWriter, pCommitter->dWriter.pBuilder, pCommitter->dWriter.aSttBlk);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbInitSttBlockBuilderIfNeed(pCommitter, id);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
#else
    code = tBlockDataAppendRow(&pCommitter->dWriter.bDatal, &row, NULL, id.uid);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pCommitter->dWriter.bDatal.nRow >= pCommitter->maxRow) {
      code = tsdbWriteSttBlock(pCommitter->dWriter.pWriter, &pCommitter->dWriter.bDatal, pCommitter->dWriter.aSttBlk,
                               pCommitter->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
#endif
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitTableData(SCommitter *pCommitter, TABLEID id) {
  int32_t code = 0;
  int32_t lino = 0;

  SRowInfo *pRowInfo = tsdbGetCommitRow(pCommitter);
  if (pRowInfo && (pRowInfo->suid != id.suid || pRowInfo->uid != id.uid)) {
    pRowInfo = NULL;
  }

  if (pRowInfo == NULL) goto _exit;

  if (pCommitter->toLastOnly) {
    code = tsdbInitSttBlockBuilderIfNeed(pCommitter, id);
    TSDB_CHECK_CODE(code, lino, _exit);

    while (pRowInfo) {
      STSchema *pTSchema = NULL;
      if (pRowInfo->row.type == TSDBROW_ROW_FMT) {
        code = tsdbCommitterUpdateRowSchema(pCommitter, id.suid, id.uid, TSDBROW_SVERSION(&pRowInfo->row));
        TSDB_CHECK_CODE(code, lino, _exit);
        pTSchema = pCommitter->skmRow.pTSchema;
      }

#if USE_STREAM_COMPRESSION
      code = tDiskDataAddRow(pCommitter->dWriter.pBuilder, &pRowInfo->row, pTSchema, &id);
#else
      code = tBlockDataAppendRow(&pCommitter->dWriter.bDatal, &pRowInfo->row, pTSchema, id.uid);
#endif
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbNextCommitRow(pCommitter);
      TSDB_CHECK_CODE(code, lino, _exit);

      pRowInfo = tsdbGetCommitRow(pCommitter);
      if (pRowInfo && (pRowInfo->suid != id.suid || pRowInfo->uid != id.uid)) {
        pRowInfo = NULL;
      }

#if USE_STREAM_COMPRESSION
      if (pCommitter->dWriter.pBuilder->nRow >= pCommitter->maxRow) {
        code = tsdbCommitSttBlk(pCommitter->dWriter.pWriter, pCommitter->dWriter.pBuilder, pCommitter->dWriter.aSttBlk);
        TSDB_CHECK_CODE(code, lino, _exit);

        code = tsdbInitSttBlockBuilderIfNeed(pCommitter, id);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
#else
      if (pCommitter->dWriter.bDatal.nRow >= pCommitter->maxRow) {
        code = tsdbWriteSttBlock(pCommitter->dWriter.pWriter, &pCommitter->dWriter.bDatal, pCommitter->dWriter.aSttBlk,
                                 pCommitter->cmprAlg);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
#endif
    }
  } else {
    SBlockData *pBData = &pCommitter->dWriter.bData;
    ASSERT(pBData->nRow == 0);

    while (pRowInfo) {
      STSchema *pTSchema = NULL;
      if (pRowInfo->row.type == TSDBROW_ROW_FMT) {
        code = tsdbCommitterUpdateRowSchema(pCommitter, id.suid, id.uid, TSDBROW_SVERSION(&pRowInfo->row));
        TSDB_CHECK_CODE(code, lino, _exit);
        pTSchema = pCommitter->skmRow.pTSchema;
      }

      code = tBlockDataAppendRow(pBData, &pRowInfo->row, pTSchema, id.uid);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbNextCommitRow(pCommitter);
      TSDB_CHECK_CODE(code, lino, _exit);

      pRowInfo = tsdbGetCommitRow(pCommitter);
      if (pRowInfo && (pRowInfo->suid != id.suid || pRowInfo->uid != id.uid)) {
        pRowInfo = NULL;
      }

      if (pBData->nRow >= pCommitter->maxRow) {
        code =
            tsdbWriteDataBlock(pCommitter->dWriter.pWriter, pBData, &pCommitter->dWriter.mBlock, pCommitter->cmprAlg);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }

    if (pBData->nRow) {
      if (pBData->nRow > pCommitter->minRow) {
        code =
            tsdbWriteDataBlock(pCommitter->dWriter.pWriter, pBData, &pCommitter->dWriter.mBlock, pCommitter->cmprAlg);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else {
        code = tsdbAppendLastBlock(pCommitter);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitFileDataImpl(SCommitter *pCommitter) {
  int32_t code = 0;
  int32_t lino = 0;

  SRowInfo *pRowInfo;
  TABLEID   id = {0};
  while ((pRowInfo = tsdbGetCommitRow(pCommitter)) != NULL) {
    ASSERT(pRowInfo->suid != id.suid || pRowInfo->uid != id.uid);
    id.suid = pRowInfo->suid;
    id.uid = pRowInfo->uid;

    code = tsdbMoveCommitData(pCommitter, id);
    TSDB_CHECK_CODE(code, lino, _exit);

    // start
    tMapDataReset(&pCommitter->dWriter.mBlock);

    // impl
    code = tsdbUpdateTableSchema(pCommitter->pTsdb->pVnode->pMeta, id.suid, id.uid, &pCommitter->skmTable);
    TSDB_CHECK_CODE(code, lino, _exit);
    code = tBlockDataInit(&pCommitter->dReader.bData, &id, pCommitter->skmTable.pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
    code = tBlockDataInit(&pCommitter->dWriter.bData, &id, pCommitter->skmTable.pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);

    /* merge with data in .data file */
    code = tsdbMergeTableData(pCommitter, id);
    TSDB_CHECK_CODE(code, lino, _exit);

    /* handle remain table data */
    code = tsdbCommitTableData(pCommitter, id);
    TSDB_CHECK_CODE(code, lino, _exit);

    // end
    if (pCommitter->dWriter.mBlock.nItem > 0) {
      SBlockIdx blockIdx = {.suid = id.suid, .uid = id.uid};
      code = tsdbWriteDataBlk(pCommitter->dWriter.pWriter, &pCommitter->dWriter.mBlock, &blockIdx);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (taosArrayPush(pCommitter->dWriter.aBlockIdx, &blockIdx) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  }

  id.suid = INT64_MAX;
  id.uid = INT64_MAX;
  code = tsdbMoveCommitData(pCommitter, id);
  TSDB_CHECK_CODE(code, lino, _exit);

#if USE_STREAM_COMPRESSION
  code = tsdbCommitSttBlk(pCommitter->dWriter.pWriter, pCommitter->dWriter.pBuilder, pCommitter->dWriter.aSttBlk);
#else
  code = tsdbWriteSttBlock(pCommitter->dWriter.pWriter, &pCommitter->dWriter.bDatal, pCommitter->dWriter.aSttBlk,
                           pCommitter->cmprAlg);
#endif
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pCommitter->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbFinishCommit(STsdb *pTsdb) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SMemTable *pMemTable = pTsdb->imem;

  // lock
  taosThreadRwlockWrlock(&pTsdb->rwLock);

  code = tsdbFSCommit(pTsdb);
  if (code) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pTsdb->imem = NULL;

  // unlock
  taosThreadRwlockUnlock(&pTsdb->rwLock);
  if (pMemTable) {
    tsdbUnrefMemTable(pMemTable, NULL, true);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d, tsdb finish commit", TD_VID(pTsdb->pVnode));
  }
  return code;
}

int32_t tsdbRollbackCommit(STsdb *pTsdb) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFSRollback(pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d, tsdb rollback commit", TD_VID(pTsdb->pVnode));
  }
  return code;
}
