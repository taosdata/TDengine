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
typedef struct {
  int64_t   suid;
  int64_t   uid;
  STSchema *pTSchema;
} SSkmInfo;

typedef enum { MEMORY_DATA_ITER = 0, LAST_DATA_ITER } EDataIterT;

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
      int32_t    iSst;
      SArray    *aSstBlk;
      int32_t    iSstBlk;
      SBlockData bData;
      int32_t    iRow;
    };  // sst file data iter
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
  int8_t  maxLast;
  SArray *aTbDataP;  // memory
  STsdbFS fs;        // disk
  // --------------
  TSKEY   nextKey;  // reset by each table commit
  int32_t commitFid;
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
    SDataIter  aDataIter[TSDB_MAX_SST_FILE];
    int8_t     toLastOnly;
  };
  struct {
    SDataFWriter *pWriter;
    SArray       *aBlockIdx;  // SArray<SBlockIdx>
    SArray       *aSstBlk;    // SArray<SSstBlk>
    SMapData      mBlock;     // SMapData<SDataBlk>
    SBlockData    bData;
    SBlockData    bDatal;
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

static int32_t tsdbStartCommit(STsdb *pTsdb, SCommitter *pCommitter);
static int32_t tsdbCommitData(SCommitter *pCommitter);
static int32_t tsdbCommitDel(SCommitter *pCommitter);
static int32_t tsdbCommitCache(SCommitter *pCommitter);
static int32_t tsdbEndCommit(SCommitter *pCommitter, int32_t eno);
static int32_t tsdbNextCommitRow(SCommitter *pCommitter);

static int32_t tRowInfoCmprFn(const void *p1, const void *p2) {
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

  if (!pTsdb) return code;

  SMemTable *pMemTable;
  code = tsdbMemTableCreate(pTsdb, &pMemTable);
  if (code) goto _err;

  // lock
  code = taosThreadRwlockWrlock(&pTsdb->rwLock);
  if (code) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  pTsdb->mem = pMemTable;

  // unlock
  code = taosThreadRwlockUnlock(&pTsdb->rwLock);
  if (code) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d, tsdb begin failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbCommit(STsdb *pTsdb) {
  if (!pTsdb) return 0;

  int32_t    code = 0;
  SCommitter commith;
  SMemTable *pMemTable = pTsdb->mem;

  // check
  if (pMemTable->nRow == 0 && pMemTable->nDel == 0) {
    taosThreadRwlockWrlock(&pTsdb->rwLock);
    pTsdb->mem = NULL;
    taosThreadRwlockUnlock(&pTsdb->rwLock);

    tsdbUnrefMemTable(pMemTable);
    goto _exit;
  }

  // start commit
  code = tsdbStartCommit(pTsdb, &commith);
  if (code) goto _err;

  // commit impl
  code = tsdbCommitData(&commith);
  if (code) goto _err;

  code = tsdbCommitDel(&commith);
  if (code) goto _err;

  // end commit
  code = tsdbEndCommit(&commith, 0);
  if (code) goto _err;

_exit:
  return code;

_err:
  tsdbEndCommit(&commith, code);
  tsdbError("vgId:%d, failed to commit since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitDelStart(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  pCommitter->aDelIdx = taosArrayInit(0, sizeof(SDelIdx));
  if (pCommitter->aDelIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  pCommitter->aDelData = taosArrayInit(0, sizeof(SDelData));
  if (pCommitter->aDelData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  pCommitter->aDelIdxN = taosArrayInit(0, sizeof(SDelIdx));
  if (pCommitter->aDelIdxN == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  SDelFile *pDelFileR = pCommitter->fs.pDelFile;
  if (pDelFileR) {
    code = tsdbDelFReaderOpen(&pCommitter->pDelFReader, pDelFileR, pTsdb);
    if (code) goto _err;

    code = tsdbReadDelIdx(pCommitter->pDelFReader, pCommitter->aDelIdx);
    if (code) goto _err;
  }

  // prepare new
  SDelFile wDelFile = {.commitID = pCommitter->commitID, .size = 0, .offset = 0};
  code = tsdbDelFWriterOpen(&pCommitter->pDelFWriter, &wDelFile, pTsdb);
  if (code) goto _err;

_exit:
  tsdbDebug("vgId:%d, commit del start", TD_VID(pTsdb->pVnode));
  return code;

_err:
  tsdbError("vgId:%d, commit del start failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableDel(SCommitter *pCommitter, STbData *pTbData, SDelIdx *pDelIdx) {
  int32_t   code = 0;
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

    code = tsdbReadDelData(pCommitter->pDelFReader, pDelIdx, pCommitter->aDelData);
    if (code) goto _err;
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
      goto _err;
    }
  }

  // write
  code = tsdbWriteDelData(pCommitter->pDelFWriter, pCommitter->aDelData, &delIdx);
  if (code) goto _err;

  // put delIdx
  if (taosArrayPush(pCommitter->aDelIdxN, &delIdx) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

_exit:
  return code;

_err:
  tsdbError("vgId:%d, commit table del failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitDelEnd(SCommitter *pCommitter) {
  int32_t code = 0;
  STsdb  *pTsdb = pCommitter->pTsdb;

  code = tsdbWriteDelIdx(pCommitter->pDelFWriter, pCommitter->aDelIdxN);
  if (code) goto _err;

  code = tsdbUpdateDelFileHdr(pCommitter->pDelFWriter);
  if (code) goto _err;

  code = tsdbFSUpsertDelFile(&pCommitter->fs, &pCommitter->pDelFWriter->fDel);
  if (code) goto _err;

  code = tsdbDelFWriterClose(&pCommitter->pDelFWriter, 1);
  if (code) goto _err;

  if (pCommitter->pDelFReader) {
    code = tsdbDelFReaderClose(&pCommitter->pDelFReader);
    if (code) goto _err;
  }

  taosArrayDestroy(pCommitter->aDelIdx);
  taosArrayDestroy(pCommitter->aDelData);
  taosArrayDestroy(pCommitter->aDelIdxN);

  return code;

_err:
  tsdbError("vgId:%d, commit del end failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitterUpdateTableSchema(SCommitter *pCommitter, int64_t suid, int64_t uid) {
  int32_t code = 0;

  if (suid) {
    if (pCommitter->skmTable.suid == suid) {
      pCommitter->skmTable.uid = uid;
      goto _exit;
    }
  } else {
    if (pCommitter->skmTable.uid == uid) goto _exit;
  }

  pCommitter->skmTable.suid = suid;
  pCommitter->skmTable.uid = uid;
  tTSchemaDestroy(pCommitter->skmTable.pTSchema);
  code = metaGetTbTSchemaEx(pCommitter->pTsdb->pVnode->pMeta, suid, uid, -1, &pCommitter->skmTable.pTSchema);
  if (code) goto _exit;

_exit:
  return code;
}

static int32_t tsdbCommitterUpdateRowSchema(SCommitter *pCommitter, int64_t suid, int64_t uid, int32_t sver) {
  int32_t code = 0;

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
  tTSchemaDestroy(pCommitter->skmRow.pTSchema);
  code = metaGetTbTSchemaEx(pCommitter->pTsdb->pVnode->pMeta, suid, uid, sver, &pCommitter->skmRow.pTSchema);
  if (code) {
    goto _exit;
  }

_exit:
  return code;
}

static int32_t tsdbCommitterNextTableData(SCommitter *pCommitter) {
  int32_t code = 0;

  ASSERT(pCommitter->dReader.pBlockIdx);

  pCommitter->dReader.iBlockIdx++;
  if (pCommitter->dReader.iBlockIdx < taosArrayGetSize(pCommitter->dReader.aBlockIdx)) {
    pCommitter->dReader.pBlockIdx =
        (SBlockIdx *)taosArrayGet(pCommitter->dReader.aBlockIdx, pCommitter->dReader.iBlockIdx);

    code = tsdbReadBlock(pCommitter->dReader.pReader, pCommitter->dReader.pBlockIdx, &pCommitter->dReader.mBlock);
    if (code) goto _exit;

    ASSERT(pCommitter->dReader.mBlock.nItem > 0);
  } else {
    pCommitter->dReader.pBlockIdx = NULL;
  }

_exit:
  return code;
}

static int32_t tsdbOpenCommitIter(SCommitter *pCommitter) {
  int32_t code = 0;

  pCommitter->pIter = NULL;
  tRBTreeCreate(&pCommitter->rbt, tRowInfoCmprFn);

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
    if (pReader->pSet->nSstF >= pCommitter->maxLast) {
      int8_t iIter = 0;
      for (int32_t iSst = 0; iSst < pReader->pSet->nSstF; iSst++) {
        pIter = &pCommitter->aDataIter[iIter];
        pIter->type = LAST_DATA_ITER;
        pIter->iSst = iSst;

        code = tsdbReadSstBlk(pCommitter->dReader.pReader, iSst, pIter->aSstBlk);
        if (code) goto _err;

        if (taosArrayGetSize(pIter->aSstBlk) == 0) continue;

        pIter->iSstBlk = 0;
        SSstBlk *pSstBlk = (SSstBlk *)taosArrayGet(pIter->aSstBlk, 0);
        code = tsdbReadSstBlockEx(pCommitter->dReader.pReader, iSst, pSstBlk, &pIter->bData);
        if (code) goto _err;

        pIter->iRow = 0;
        pIter->r.suid = pIter->bData.suid;
        pIter->r.uid = pIter->bData.uid ? pIter->bData.uid : pIter->bData.aUid[0];
        pIter->r.row = tsdbRowFromBlockData(&pIter->bData, 0);

        tRBTreePut(&pCommitter->rbt, (SRBTreeNode *)pIter);
        iIter++;
      }
    } else {
      for (int32_t iSst = 0; iSst < pReader->pSet->nSstF; iSst++) {
        SSstFile *pSstFile = pReader->pSet->aSstF[iSst];
        if (pSstFile->size > pSstFile->offset) {
          pCommitter->toLastOnly = 1;
          break;
        }
      }
    }
  }

  code = tsdbNextCommitRow(pCommitter);
  if (code) goto _err;

  return code;

_err:
  return code;
}

static int32_t tsdbCommitFileDataStart(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SDFileSet *pRSet = NULL;

  // memory
  pCommitter->commitFid = tsdbKeyFid(pCommitter->nextKey, pCommitter->minutes, pCommitter->precision);
  tsdbFidKeyRange(pCommitter->commitFid, pCommitter->minutes, pCommitter->precision, &pCommitter->minKey,
                  &pCommitter->maxKey);
  pCommitter->nextKey = TSKEY_MAX;

  // Reader
  SDFileSet tDFileSet = {.fid = pCommitter->commitFid};
  pRSet = (SDFileSet *)taosArraySearch(pCommitter->fs.aDFileSet, &tDFileSet, tDFileSetCmprFn, TD_EQ);
  if (pRSet) {
    code = tsdbDataFReaderOpen(&pCommitter->dReader.pReader, pTsdb, pRSet);
    if (code) goto _err;

    // data
    code = tsdbReadBlockIdx(pCommitter->dReader.pReader, pCommitter->dReader.aBlockIdx);
    if (code) goto _err;

    pCommitter->dReader.iBlockIdx = 0;
    if (taosArrayGetSize(pCommitter->dReader.aBlockIdx) > 0) {
      pCommitter->dReader.pBlockIdx = (SBlockIdx *)taosArrayGet(pCommitter->dReader.aBlockIdx, 0);
      code = tsdbReadBlock(pCommitter->dReader.pReader, pCommitter->dReader.pBlockIdx, &pCommitter->dReader.mBlock);
      if (code) goto _err;
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
  SSstFile  fSst = {.commitID = pCommitter->commitID};
  SDFileSet wSet = {.fid = pCommitter->commitFid, .pHeadF = &fHead, .pDataF = &fData, .pSmaF = &fSma};
  if (pRSet) {
    ASSERT(pRSet->nSstF <= pCommitter->maxLast);
    fData = *pRSet->pDataF;
    fSma = *pRSet->pSmaF;
    wSet.diskId = pRSet->diskId;
    if (pRSet->nSstF < pCommitter->maxLast) {
      for (int32_t iSst = 0; iSst < pRSet->nSstF; iSst++) {
        wSet.aSstF[iSst] = pRSet->aSstF[iSst];
      }
      wSet.nSstF = pRSet->nSstF + 1;
    } else {
      wSet.nSstF = 1;
    }
  } else {
    SDiskID did = {0};
    tfsAllocDisk(pTsdb->pVnode->pTfs, 0, &did);
    tfsMkdirRecurAt(pTsdb->pVnode->pTfs, pTsdb->path, did);
    wSet.diskId = did;
    wSet.nSstF = 1;
  }
  wSet.aSstF[wSet.nSstF - 1] = &fSst;
  code = tsdbDataFWriterOpen(&pCommitter->dWriter.pWriter, pTsdb, &wSet);
  if (code) goto _err;

  taosArrayClear(pCommitter->dWriter.aBlockIdx);
  taosArrayClear(pCommitter->dWriter.aSstBlk);
  tMapDataReset(&pCommitter->dWriter.mBlock);
  tBlockDataReset(&pCommitter->dWriter.bData);
  tBlockDataReset(&pCommitter->dWriter.bDatal);

  // open iter
  code = tsdbOpenCommitIter(pCommitter);
  if (code) goto _err;

_exit:
  return code;

_err:
  tsdbError("vgId:%d, commit file data start failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitDataBlock(SCommitter *pCommitter) {
  int32_t     code = 0;
  SBlockData *pBlockData = &pCommitter->dWriter.bData;
  SDataBlk    block;

  ASSERT(pBlockData->nRow > 0);

  tDataBlkReset(&block);

  // info
  block.nRow += pBlockData->nRow;
  for (int32_t iRow = 0; iRow < pBlockData->nRow; iRow++) {
    TSDBKEY key = {.ts = pBlockData->aTSKEY[iRow], .version = pBlockData->aVersion[iRow]};

    if (iRow == 0) {
      if (tsdbKeyCmprFn(&block.minKey, &key) > 0) {
        block.minKey = key;
      }
    } else {
      if (pBlockData->aTSKEY[iRow] == pBlockData->aTSKEY[iRow - 1]) {
        block.hasDup = 1;
      }
    }

    if (iRow == pBlockData->nRow - 1 && tsdbKeyCmprFn(&block.maxKey, &key) < 0) {
      block.maxKey = key;
    }

    block.minVer = TMIN(block.minVer, key.version);
    block.maxVer = TMAX(block.maxVer, key.version);
  }

  // write
  block.nSubBlock++;
  code = tsdbWriteBlockData(pCommitter->dWriter.pWriter, pBlockData, &block.aSubBlock[block.nSubBlock - 1],
                            ((block.nSubBlock == 1) && !block.hasDup) ? &block.smaInfo : NULL, pCommitter->cmprAlg, 0);
  if (code) goto _err;

  // put SDataBlk
  code = tMapDataPutItem(&pCommitter->dWriter.mBlock, &block, tPutDataBlk);
  if (code) goto _err;

  // clear
  tBlockDataClear(pBlockData);

  return code;

_err:
  tsdbError("vgId:%d tsdb commit data block failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitLastBlock(SCommitter *pCommitter) {
  int32_t     code = 0;
  SSstBlk     blockL;
  SBlockData *pBlockData = &pCommitter->dWriter.bDatal;

  ASSERT(pBlockData->nRow > 0);

  // info
  blockL.suid = pBlockData->suid;
  blockL.nRow = pBlockData->nRow;
  blockL.minKey = TSKEY_MAX;
  blockL.maxKey = TSKEY_MIN;
  blockL.minVer = VERSION_MAX;
  blockL.maxVer = VERSION_MIN;
  for (int32_t iRow = 0; iRow < pBlockData->nRow; iRow++) {
    blockL.minKey = TMIN(blockL.minKey, pBlockData->aTSKEY[iRow]);
    blockL.maxKey = TMAX(blockL.maxKey, pBlockData->aTSKEY[iRow]);
    blockL.minVer = TMIN(blockL.minVer, pBlockData->aVersion[iRow]);
    blockL.maxVer = TMAX(blockL.maxVer, pBlockData->aVersion[iRow]);
  }
  blockL.minUid = pBlockData->uid ? pBlockData->uid : pBlockData->aUid[0];
  blockL.maxUid = pBlockData->uid ? pBlockData->uid : pBlockData->aUid[pBlockData->nRow - 1];

  // write
  code = tsdbWriteBlockData(pCommitter->dWriter.pWriter, pBlockData, &blockL.bInfo, NULL, pCommitter->cmprAlg, 1);
  if (code) goto _err;

  // push SSstBlk
  if (taosArrayPush(pCommitter->dWriter.aSstBlk, &blockL) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  // clear
  tBlockDataClear(pBlockData);

  return code;

_err:
  tsdbError("vgId:%d tsdb commit last block failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitFileDataEnd(SCommitter *pCommitter) {
  int32_t code = 0;

  // write aBlockIdx
  code = tsdbWriteBlockIdx(pCommitter->dWriter.pWriter, pCommitter->dWriter.aBlockIdx);
  if (code) goto _err;

  // write aSstBlk
  code = tsdbWriteSstBlk(pCommitter->dWriter.pWriter, pCommitter->dWriter.aSstBlk);
  if (code) goto _err;

  // update file header
  code = tsdbUpdateDFileSetHeader(pCommitter->dWriter.pWriter);
  if (code) goto _err;

  // upsert SDFileSet
  code = tsdbFSUpsertFSet(&pCommitter->fs, &pCommitter->dWriter.pWriter->wSet);
  if (code) goto _err;

  // close and sync
  code = tsdbDataFWriterClose(&pCommitter->dWriter.pWriter, 1);
  if (code) goto _err;

  if (pCommitter->dReader.pReader) {
    code = tsdbDataFReaderClose(&pCommitter->dReader.pReader);
    if (code) goto _err;
  }

_exit:
  return code;

_err:
  tsdbError("vgId:%d, commit file data end failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbMoveCommitData(SCommitter *pCommitter, TABLEID toTable) {
  int32_t code = 0;

  while (pCommitter->dReader.pBlockIdx && tTABLEIDCmprFn(pCommitter->dReader.pBlockIdx, &toTable) < 0) {
    SBlockIdx blockIdx = *pCommitter->dReader.pBlockIdx;
    code = tsdbWriteBlock(pCommitter->dWriter.pWriter, &pCommitter->dReader.mBlock, &blockIdx);
    if (code) goto _err;

    if (taosArrayPush(pCommitter->dWriter.aBlockIdx, &blockIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }

    code = tsdbCommitterNextTableData(pCommitter);
    if (code) goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb move commit data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitFileDataImpl(SCommitter *pCommitter);
static int32_t tsdbCommitFileData(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  // commit file data start
  code = tsdbCommitFileDataStart(pCommitter);
  if (code) goto _err;

  // impl
  code = tsdbCommitFileDataImpl(pCommitter);
  if (code) goto _err;

  // commit file data end
  code = tsdbCommitFileDataEnd(pCommitter);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d, commit file data failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  tsdbDataFReaderClose(&pCommitter->dReader.pReader);
  tsdbDataFWriterClose(&pCommitter->dWriter.pWriter, 0);
  return code;
}

// ----------------------------------------------------------------------------
static int32_t tsdbStartCommit(STsdb *pTsdb, SCommitter *pCommitter) {
  int32_t code = 0;

  memset(pCommitter, 0, sizeof(*pCommitter));
  ASSERT(pTsdb->mem && pTsdb->imem == NULL);

  taosThreadRwlockWrlock(&pTsdb->rwLock);
  pTsdb->imem = pTsdb->mem;
  pTsdb->mem = NULL;
  taosThreadRwlockUnlock(&pTsdb->rwLock);

  pCommitter->pTsdb = pTsdb;
  pCommitter->commitID = pTsdb->pVnode->state.commitID;
  pCommitter->minutes = pTsdb->keepCfg.days;
  pCommitter->precision = pTsdb->keepCfg.precision;
  pCommitter->minRow = pTsdb->pVnode->config.tsdbCfg.minRows;
  pCommitter->maxRow = pTsdb->pVnode->config.tsdbCfg.maxRows;
  pCommitter->cmprAlg = pTsdb->pVnode->config.tsdbCfg.compression;
  pCommitter->maxLast = TSDB_DEFAULT_SST_FILE;  // TODO: make it as a config
  pCommitter->aTbDataP = tsdbMemTableGetTbDataArray(pTsdb->imem);
  if (pCommitter->aTbDataP == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  code = tsdbFSCopy(pTsdb, &pCommitter->fs);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d, tsdb start commit failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitDataStart(SCommitter *pCommitter) {
  int32_t code = 0;

  // reader
  pCommitter->dReader.aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pCommitter->dReader.aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  code = tBlockDataCreate(&pCommitter->dReader.bData);
  if (code) goto _exit;

  // merger
  for (int32_t iSst = 0; iSst < TSDB_MAX_SST_FILE; iSst++) {
    SDataIter *pIter = &pCommitter->aDataIter[iSst];
    pIter->aSstBlk = taosArrayInit(0, sizeof(SSstBlk));
    if (pIter->aSstBlk == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    code = tBlockDataCreate(&pIter->bData);
    if (code) goto _exit;
  }

  // writer
  pCommitter->dWriter.aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pCommitter->dWriter.aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  pCommitter->dWriter.aSstBlk = taosArrayInit(0, sizeof(SSstBlk));
  if (pCommitter->dWriter.aSstBlk == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  code = tBlockDataCreate(&pCommitter->dWriter.bData);
  if (code) goto _exit;

  code = tBlockDataCreate(&pCommitter->dWriter.bDatal);
  if (code) goto _exit;

_exit:
  return code;
}

static void tsdbCommitDataEnd(SCommitter *pCommitter) {
  // reader
  taosArrayDestroy(pCommitter->dReader.aBlockIdx);
  tMapDataClear(&pCommitter->dReader.mBlock);
  tBlockDataDestroy(&pCommitter->dReader.bData, 1);

  // merger
  for (int32_t iSst = 0; iSst < TSDB_MAX_SST_FILE; iSst++) {
    SDataIter *pIter = &pCommitter->aDataIter[iSst];
    taosArrayDestroy(pIter->aSstBlk);
    tBlockDataDestroy(&pIter->bData, 1);
  }

  // writer
  taosArrayDestroy(pCommitter->dWriter.aBlockIdx);
  taosArrayDestroy(pCommitter->dWriter.aSstBlk);
  tMapDataClear(&pCommitter->dWriter.mBlock);
  tBlockDataDestroy(&pCommitter->dWriter.bData, 1);
  tBlockDataDestroy(&pCommitter->dWriter.bDatal, 1);
  tTSchemaDestroy(pCommitter->skmTable.pTSchema);
  tTSchemaDestroy(pCommitter->skmRow.pTSchema);
}

static int32_t tsdbCommitData(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  // check
  if (pMemTable->nRow == 0) goto _exit;

  // start ====================
  code = tsdbCommitDataStart(pCommitter);
  if (code) goto _err;

  // impl ====================
  pCommitter->nextKey = pMemTable->minKey;
  while (pCommitter->nextKey < TSKEY_MAX) {
    code = tsdbCommitFileData(pCommitter);
    if (code) goto _err;
  }

  // end ====================
  tsdbCommitDataEnd(pCommitter);

_exit:
  tsdbInfo("vgId:%d, commit data done, nRow:%" PRId64, TD_VID(pTsdb->pVnode), pMemTable->nRow);
  return code;

_err:
  tsdbCommitDataEnd(pCommitter);
  tsdbError("vgId:%d, commit data failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitDel(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  if (pMemTable->nDel == 0) {
    goto _exit;
  }

  // start
  code = tsdbCommitDelStart(pCommitter);
  if (code) {
    goto _err;
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
    if (code) goto _err;

    iTbData++;
    pTbData = (iTbData < nTbData) ? (STbData *)taosArrayGetP(pCommitter->aTbDataP, iTbData) : NULL;
    continue;

  _commit_disk_del:
    code = tsdbCommitTableDel(pCommitter, NULL, pDelIdx);
    if (code) goto _err;

    iDelIdx++;
    pDelIdx = (iDelIdx < nDelIdx) ? (SDelIdx *)taosArrayGet(pCommitter->aDelIdx, iDelIdx) : NULL;
    continue;

  _commit_mem_and_disk_del:
    code = tsdbCommitTableDel(pCommitter, pTbData, pDelIdx);
    if (code) goto _err;

    iTbData++;
    pTbData = (iTbData < nTbData) ? (STbData *)taosArrayGetP(pCommitter->aTbDataP, iTbData) : NULL;
    iDelIdx++;
    pDelIdx = (iDelIdx < nDelIdx) ? (SDelIdx *)taosArrayGet(pCommitter->aDelIdx, iDelIdx) : NULL;
    continue;
  }

  // end
  code = tsdbCommitDelEnd(pCommitter);
  if (code) {
    goto _err;
  }

_exit:
  tsdbDebug("vgId:%d, commit del done, nDel:%" PRId64, TD_VID(pTsdb->pVnode), pMemTable->nDel);
  return code;

_err:
  tsdbError("vgId:%d, commit del failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbEndCommit(SCommitter *pCommitter, int32_t eno) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  ASSERT(eno == 0);

  code = tsdbFSCommit1(pTsdb, &pCommitter->fs);
  if (code) goto _err;

  // lock
  taosThreadRwlockWrlock(&pTsdb->rwLock);

  // commit or rollback
  code = tsdbFSCommit2(pTsdb, &pCommitter->fs);
  if (code) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    goto _err;
  }

  pTsdb->imem = NULL;

  // unlock
  taosThreadRwlockUnlock(&pTsdb->rwLock);

  tsdbUnrefMemTable(pMemTable);
  tsdbFSDestroy(&pCommitter->fs);
  taosArrayDestroy(pCommitter->aTbDataP);

  // if (pCommitter->toMerge) {
  //   code = tsdbMerge(pTsdb);
  //   if (code) goto _err;
  // }

  tsdbInfo("vgId:%d, tsdb end commit", TD_VID(pTsdb->pVnode));
  return code;

_err:
  tsdbError("vgId:%d, tsdb end commit failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

// ================================================================================

static FORCE_INLINE SRowInfo *tsdbGetCommitRow(SCommitter *pCommitter) {
  return (pCommitter->pIter) ? &pCommitter->pIter->r : NULL;
}

static int32_t tsdbNextCommitRow(SCommitter *pCommitter) {
  int32_t code = 0;

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
    } else if (pCommitter->pIter->type == LAST_DATA_ITER) {  // last file
      pIter->iRow++;
      if (pIter->iRow < pIter->bData.nRow) {
        pIter->r.uid = pIter->bData.uid ? pIter->bData.uid : pIter->bData.aUid[pIter->iRow];
        pIter->r.row = tsdbRowFromBlockData(&pIter->bData, pIter->iRow);
      } else {
        pIter->iSstBlk++;
        if (pIter->iSstBlk < taosArrayGetSize(pIter->aSstBlk)) {
          SSstBlk *pSstBlk = (SSstBlk *)taosArrayGet(pIter->aSstBlk, pIter->iSstBlk);

          code = tsdbReadSstBlockEx(pCommitter->dReader.pReader, pIter->iSst, pSstBlk, &pIter->bData);
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
  return code;
}

static int32_t tsdbCommitAheadBlock(SCommitter *pCommitter, SDataBlk *pDataBlk) {
  int32_t     code = 0;
  SBlockData *pBlockData = &pCommitter->dWriter.bData;
  SRowInfo   *pRowInfo = tsdbGetCommitRow(pCommitter);
  TABLEID     id = {.suid = pRowInfo->suid, .uid = pRowInfo->uid};

  tBlockDataClear(pBlockData);
  while (pRowInfo) {
    ASSERT(pRowInfo->row.type == 0);
    code = tsdbCommitterUpdateRowSchema(pCommitter, id.suid, id.uid, TSDBROW_SVERSION(&pRowInfo->row));
    if (code) goto _err;

    code = tBlockDataAppendRow(pBlockData, &pRowInfo->row, pCommitter->skmRow.pTSchema, id.uid);
    if (code) goto _err;

    code = tsdbNextCommitRow(pCommitter);
    if (code) goto _err;

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
      code = tsdbCommitDataBlock(pCommitter);
      if (code) goto _err;
    }
  }

  if (pBlockData->nRow) {
    code = tsdbCommitDataBlock(pCommitter);
    if (code) goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d, tsdb commit ahead block failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitMergeBlock(SCommitter *pCommitter, SDataBlk *pDataBlk) {
  int32_t     code = 0;
  SRowInfo   *pRowInfo = tsdbGetCommitRow(pCommitter);
  TABLEID     id = {.suid = pRowInfo->suid, .uid = pRowInfo->uid};
  SBlockData *pBDataR = &pCommitter->dReader.bData;
  SBlockData *pBDataW = &pCommitter->dWriter.bData;

  code = tsdbReadDataBlock(pCommitter->dReader.pReader, pDataBlk, pBDataR);
  if (code) goto _err;

  tBlockDataClear(pBDataW);
  int32_t  iRow = 0;
  TSDBROW  row = tsdbRowFromBlockData(pBDataR, 0);
  TSDBROW *pRow = &row;

  while (pRow && pRowInfo) {
    int32_t c = tsdbRowCmprFn(pRow, &pRowInfo->row);
    if (c < 0) {
      code = tBlockDataAppendRow(pBDataW, pRow, NULL, id.uid);
      if (code) goto _err;

      iRow++;
      if (iRow < pBDataR->nRow) {
        row = tsdbRowFromBlockData(pBDataR, iRow);
      } else {
        pRow = NULL;
      }
    } else if (c > 0) {
      ASSERT(pRowInfo->row.type == 0);
      code = tsdbCommitterUpdateRowSchema(pCommitter, id.suid, id.uid, TSDBROW_SVERSION(&pRowInfo->row));
      if (code) goto _err;

      code = tBlockDataAppendRow(pBDataW, &pRowInfo->row, pCommitter->skmRow.pTSchema, id.uid);
      if (code) goto _err;

      code = tsdbNextCommitRow(pCommitter);
      if (code) goto _err;

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
      ASSERT(0);
    }

    if (pBDataW->nRow >= pCommitter->maxRow) {
      code = tsdbCommitDataBlock(pCommitter);
      if (code) goto _err;
    }
  }

  while (pRow) {
    code = tBlockDataAppendRow(pBDataW, pRow, NULL, id.uid);
    if (code) goto _err;

    iRow++;
    if (iRow < pBDataR->nRow) {
      row = tsdbRowFromBlockData(pBDataR, iRow);
    } else {
      pRow = NULL;
    }

    if (pBDataW->nRow >= pCommitter->maxRow) {
      code = tsdbCommitDataBlock(pCommitter);
      if (code) goto _err;
    }
  }

  if (pBDataW->nRow) {
    code = tsdbCommitDataBlock(pCommitter);
    if (code) goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d, tsdb commit merge block failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbMergeTableData(SCommitter *pCommitter, TABLEID id) {
  int32_t    code = 0;
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
        if (code) goto _err;

        iBlock++;
        if (iBlock < pCommitter->dReader.mBlock.nItem) {
          tMapDataGetItemByIdx(&pCommitter->dReader.mBlock, iBlock, pDataBlk, tGetDataBlk);
        } else {
          pDataBlk = NULL;
        }
      } else if (c > 0) {
        code = tsdbCommitAheadBlock(pCommitter, pDataBlk);
        if (code) goto _err;

        pRowInfo = tsdbGetCommitRow(pCommitter);
        if (pRowInfo && (pRowInfo->suid != id.suid || pRowInfo->uid != id.uid)) pRowInfo = NULL;
      } else {
        code = tsdbCommitMergeBlock(pCommitter, pDataBlk);
        if (code) goto _err;

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
      if (code) goto _err;

      iBlock++;
      if (iBlock < pCommitter->dReader.mBlock.nItem) {
        tMapDataGetItemByIdx(&pCommitter->dReader.mBlock, iBlock, pDataBlk, tGetDataBlk);
      } else {
        pDataBlk = NULL;
      }
    }

    code = tsdbCommitterNextTableData(pCommitter);
    if (code) goto _err;
  }

_exit:
  return code;

_err:
  tsdbError("vgId:%d tsdb merge table data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbInitLastBlockIfNeed(SCommitter *pCommitter, TABLEID id) {
  int32_t code = 0;

  SBlockData *pBDatal = &pCommitter->dWriter.bDatal;
  if (pBDatal->suid || pBDatal->uid) {
    if ((pBDatal->suid != id.suid) || (id.suid == 0)) {
      if (pBDatal->nRow) {
        code = tsdbCommitLastBlock(pCommitter);
        if (code) goto _exit;
      }
      tBlockDataReset(pBDatal);
    }
  }

  if (!pBDatal->suid && !pBDatal->uid) {
    ASSERT(pCommitter->skmTable.suid == id.suid);
    ASSERT(pCommitter->skmTable.uid == id.uid);
    code = tBlockDataInit(pBDatal, id.suid, id.suid ? 0 : id.uid, pCommitter->skmTable.pTSchema);
    if (code) goto _exit;
  }

_exit:
  return code;
}

static int32_t tsdbAppendLastBlock(SCommitter *pCommitter) {
  int32_t code = 0;

  SBlockData *pBData = &pCommitter->dWriter.bData;
  SBlockData *pBDatal = &pCommitter->dWriter.bDatal;

  TABLEID id = {.suid = pBData->suid, .uid = pBData->uid};
  code = tsdbInitLastBlockIfNeed(pCommitter, id);
  if (code) goto _err;

  for (int32_t iRow = 0; iRow < pBData->nRow; iRow++) {
    TSDBROW row = tsdbRowFromBlockData(pBData, iRow);
    code = tBlockDataAppendRow(pBDatal, &row, NULL, pBData->uid);
    if (code) goto _err;

    if (pBDatal->nRow >= pCommitter->maxRow) {
      code = tsdbCommitLastBlock(pCommitter);
      if (code) goto _err;
    }
  }

  return code;

_err:
  return code;
}

static int32_t tsdbCommitTableData(SCommitter *pCommitter, TABLEID id) {
  int32_t code = 0;

  SRowInfo *pRowInfo = tsdbGetCommitRow(pCommitter);
  if (pRowInfo && (pRowInfo->suid != id.suid || pRowInfo->uid != id.uid)) {
    pRowInfo = NULL;
  }

  if (pRowInfo == NULL) goto _exit;

  SBlockData *pBData;
  if (pCommitter->toLastOnly) {
    pBData = &pCommitter->dWriter.bDatal;
    code = tsdbInitLastBlockIfNeed(pCommitter, id);
    if (code) goto _err;
  } else {
    pBData = &pCommitter->dWriter.bData;
    ASSERT(pBData->nRow == 0);
  }

  while (pRowInfo) {
    STSchema *pTSchema = NULL;
    if (pRowInfo->row.type == 0) {
      code = tsdbCommitterUpdateRowSchema(pCommitter, id.suid, id.uid, TSDBROW_SVERSION(&pRowInfo->row));
      if (code) goto _err;
      pTSchema = pCommitter->skmRow.pTSchema;
    }

    code = tBlockDataAppendRow(pBData, &pRowInfo->row, pTSchema, id.uid);
    if (code) goto _err;

    code = tsdbNextCommitRow(pCommitter);
    if (code) goto _err;

    pRowInfo = tsdbGetCommitRow(pCommitter);
    if (pRowInfo && (pRowInfo->suid != id.suid || pRowInfo->uid != id.uid)) {
      pRowInfo = NULL;
    }

    if (pBData->nRow >= pCommitter->maxRow) {
      if (pCommitter->toLastOnly) {
        code = tsdbCommitLastBlock(pCommitter);
        if (code) goto _err;
      } else {
        code = tsdbCommitDataBlock(pCommitter);
        if (code) goto _err;
      }
    }
  }

  if (!pCommitter->toLastOnly && pBData->nRow) {
    if (pBData->nRow > pCommitter->minRow) {
      code = tsdbCommitDataBlock(pCommitter);
      if (code) goto _err;
    } else {
      code = tsdbAppendLastBlock(pCommitter);
      if (code) goto _err;
    }
  }

_exit:
  return code;

_err:
  tsdbError("vgId:%d tsdb commit table data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitFileDataImpl(SCommitter *pCommitter) {
  int32_t code = 0;

  SRowInfo *pRowInfo;
  TABLEID   id = {0};
  while ((pRowInfo = tsdbGetCommitRow(pCommitter)) != NULL) {
    ASSERT(pRowInfo->suid != id.suid || pRowInfo->uid != id.uid);
    id.suid = pRowInfo->suid;
    id.uid = pRowInfo->uid;

    code = tsdbMoveCommitData(pCommitter, id);
    if (code) goto _err;

    // start
    tMapDataReset(&pCommitter->dWriter.mBlock);

    // impl
    code = tsdbCommitterUpdateTableSchema(pCommitter, id.suid, id.uid);
    if (code) goto _err;
    code = tBlockDataInit(&pCommitter->dReader.bData, id.suid, id.uid, pCommitter->skmTable.pTSchema);
    if (code) goto _err;
    code = tBlockDataInit(&pCommitter->dWriter.bData, id.suid, id.uid, pCommitter->skmTable.pTSchema);
    if (code) goto _err;

    /* merge with data in .data file */
    code = tsdbMergeTableData(pCommitter, id);
    if (code) goto _err;

    /* handle remain table data */
    code = tsdbCommitTableData(pCommitter, id);
    if (code) goto _err;

    // end
    if (pCommitter->dWriter.mBlock.nItem > 0) {
      SBlockIdx blockIdx = {.suid = id.suid, .uid = id.uid};
      code = tsdbWriteBlock(pCommitter->dWriter.pWriter, &pCommitter->dWriter.mBlock, &blockIdx);
      if (code) goto _err;

      if (taosArrayPush(pCommitter->dWriter.aBlockIdx, &blockIdx) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _err;
      }
    }
  }

  id.suid = INT64_MAX;
  id.uid = INT64_MAX;
  code = tsdbMoveCommitData(pCommitter, id);
  if (code) goto _err;

  if (pCommitter->dWriter.bDatal.nRow > 0) {
    code = tsdbCommitLastBlock(pCommitter);
    if (code) goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb commit file data impl failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}
