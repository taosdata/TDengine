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

typedef struct {
  STsdb *pTsdb;
  int8_t toMerge;
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
    SMapData      mBlock;  // SMapData<SBlock>
    SBlockData    bData;
  } dReader;
  struct {
    SDataFWriter *pWriter;
    SArray       *aBlockIdx;  // SArray<SBlockIdx>
    SArray       *aBlockL;    // SArray<SBlockL>
    SMapData      mBlock;     // SMapData<SBlock>
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
    if (pCommitter->skmTable.suid == suid) goto _exit;
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
  pRSet = (SDFileSet *)taosArraySearch(pCommitter->fs.aDFileSet, &(SDFileSet){.fid = pCommitter->commitFid},
                                       tDFileSetCmprFn, TD_EQ);
  if (pRSet) {
    code = tsdbDataFReaderOpen(&pCommitter->dReader.pReader, pTsdb, pRSet);
    if (code) goto _err;

    // data
    code = tsdbReadBlockIdx(pCommitter->dReader.pReader, pCommitter->dReader.aBlockIdx);
    if (code) goto _err;

    pCommitter->dReader.iBlockIdx = 0;
    if (pCommitter->dReader.iBlockIdx < taosArrayGetSize(pCommitter->dReader.aBlockIdx)) {
      pCommitter->dReader.pBlockIdx =
          (SBlockIdx *)taosArrayGet(pCommitter->dReader.aBlockIdx, pCommitter->dReader.iBlockIdx);

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
  SHeadFile fHead;
  SDataFile fData;
  SSmaFile  fSma;
  SLastFile fLast;
  SDFileSet wSet = {0};
  if (pRSet) {
    ASSERT(pCommitter->maxLast == 1 || pRSet->nLastF < pCommitter->maxLast);

    fHead = (SHeadFile){.commitID = pCommitter->commitID};
    fData = *pRSet->pDataF;
    fSma = *pRSet->pSmaF;
    fLast = (SLastFile){.commitID = pCommitter->commitID};

    wSet.diskId = pRSet->diskId;
    wSet.fid = pCommitter->commitFid;
    wSet.pHeadF = &fHead;
    wSet.pDataF = &fData;
    wSet.pSmaF = &fSma;
    for (int8_t iLast = 0; iLast < pRSet->nLastF; iLast++) {
      wSet.aLastF[iLast] = pRSet->aLastF[iLast];
    }
    wSet.nLastF = pRSet->nLastF + 1;
    wSet.aLastF[wSet.nLastF - 1] = &fLast;  // todo
  } else {
    fHead = (SHeadFile){.commitID = pCommitter->commitID};
    fData = (SDataFile){.commitID = pCommitter->commitID};
    fSma = (SSmaFile){.commitID = pCommitter->commitID};
    fLast = (SLastFile){.commitID = pCommitter->commitID};

    SDiskID did = {0};
    tfsAllocDisk(pTsdb->pVnode->pTfs, 0, &did);
    tfsMkdirRecurAt(pTsdb->pVnode->pTfs, pTsdb->path, did);

    wSet.diskId = did;
    wSet.fid = pCommitter->commitFid;
    wSet.pHeadF = &fHead;
    wSet.pDataF = &fData;
    wSet.pSmaF = &fSma;
    wSet.nLastF = 1;
    wSet.aLastF[0] = &fLast;
  }
  if (wSet.nLastF == pCommitter->maxLast) {
    pCommitter->toMerge = 1;
  }
  code = tsdbDataFWriterOpen(&pCommitter->dWriter.pWriter, pTsdb, &wSet);
  if (code) goto _err;

  taosArrayClear(pCommitter->dWriter.aBlockIdx);
  taosArrayClear(pCommitter->dWriter.aBlockL);
  tMapDataReset(&pCommitter->dWriter.mBlock);
  tBlockDataReset(&pCommitter->dWriter.bData);
  tBlockDataReset(&pCommitter->dWriter.bDatal);

_exit:
  return code;

_err:
  tsdbError("vgId:%d, commit file data start failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitDataBlock(SCommitter *pCommitter, SBlock *pBlock) {
  int32_t     code = 0;
  SBlockData *pBlockData = &pCommitter->dWriter.bData;
  SBlock      block;

  ASSERT(pBlockData->nRow > 0);

  if (pBlock) {
    block = *pBlock;  // as a subblock
  } else {
    tBlockReset(&block);  // as a new block
  }

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

  // put SBlock
  code = tMapDataPutItem(&pCommitter->dWriter.mBlock, &block, tPutBlock);
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
  SBlockL     blockL;
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

  // push SBlockL
  if (taosArrayPush(pCommitter->dWriter.aBlockL, &blockL) == NULL) {
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

static int32_t tsdbMergeCommitDataBlock(SCommitter *pCommitter, STbDataIter *pIter, SBlock *pBlock) {
  int32_t     code = 0;
  STbData    *pTbData = pIter->pTbData;
  SBlockData *pBlockDataR = &pCommitter->dReader.bData;
  SBlockData *pBlockDataW = &pCommitter->dWriter.bData;

  code = tsdbReadDataBlock(pCommitter->dReader.pReader, pBlock, pBlockDataR);
  if (code) goto _err;

  tBlockDataClear(pBlockDataW);
  int32_t  iRow = 0;
  TSDBROW  row;
  TSDBROW *pRow1 = tsdbTbDataIterGet(pIter);
  TSDBROW *pRow2 = &row;
  *pRow2 = tsdbRowFromBlockData(pBlockDataR, iRow);
  while (pRow1 && pRow2) {
    int32_t c = tsdbRowCmprFn(pRow1, pRow2);

    if (c < 0) {
      code = tsdbCommitterUpdateRowSchema(pCommitter, pTbData->suid, pTbData->uid, TSDBROW_SVERSION(pRow1));
      if (code) goto _err;

      code = tBlockDataAppendRow(pBlockDataW, pRow1, pCommitter->skmRow.pTSchema, pTbData->uid);
      if (code) goto _err;

      // next
      tsdbTbDataIterNext(pIter);
      pRow1 = tsdbTbDataIterGet(pIter);
    } else if (c > 0) {
      code = tBlockDataAppendRow(pBlockDataW, pRow2, NULL, pTbData->uid);
      if (code) goto _err;

      iRow++;
      if (iRow < pBlockDataR->nRow) {
        *pRow2 = tsdbRowFromBlockData(pBlockDataR, iRow);
      } else {
        pRow2 = NULL;
      }
    } else {
      ASSERT(0);
    }

    // check
    if (pBlockDataW->nRow >= pCommitter->maxRow * 4 / 5) {
      code = tsdbCommitDataBlock(pCommitter, NULL);
      if (code) goto _err;
    }
  }

  while (pRow2) {
    code = tBlockDataAppendRow(pBlockDataW, pRow2, NULL, pTbData->uid);
    if (code) goto _err;

    iRow++;
    if (iRow < pBlockDataR->nRow) {
      *pRow2 = tsdbRowFromBlockData(pBlockDataR, iRow);
    } else {
      pRow2 = NULL;
    }

    // check
    if (pBlockDataW->nRow >= pCommitter->maxRow * 4 / 5) {
      code = tsdbCommitDataBlock(pCommitter, NULL);
      if (code) goto _err;
    }
  }

  // check
  if (pBlockDataW->nRow > 0) {
    code = tsdbCommitDataBlock(pCommitter, NULL);
    if (code) goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d, tsdb merge commit data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableMemData(SCommitter *pCommitter, STbDataIter *pIter, TSDBKEY toKey) {
  int32_t     code = 0;
  STbData    *pTbData = pIter->pTbData;
  SBlockData *pBlockData = &pCommitter->dWriter.bData;

  tBlockDataClear(pBlockData);
  TSDBROW *pRow = tsdbTbDataIterGet(pIter);
  while (true) {
    if (pRow == NULL) {
      if (pBlockData->nRow > 0) {
        goto _write_block;
      } else {
        break;
      }
    }

    // update schema
    code = tsdbCommitterUpdateRowSchema(pCommitter, pTbData->suid, pTbData->uid, TSDBROW_SVERSION(pRow));
    if (code) goto _err;

    // append
    code = tBlockDataAppendRow(pBlockData, pRow, pCommitter->skmRow.pTSchema, pTbData->uid);
    if (code) goto _err;

    tsdbTbDataIterNext(pIter);
    pRow = tsdbTbDataIterGet(pIter);
    if (pRow) {
      TSDBKEY rowKey = TSDBROW_KEY(pRow);
      if (tsdbKeyCmprFn(&rowKey, &toKey) >= 0) {
        pRow = NULL;
      }
    }

    if (pBlockData->nRow >= pCommitter->maxRow * 4 / 5) {
    _write_block:
      code = tsdbCommitDataBlock(pCommitter, NULL);
      if (code) goto _err;
    }
  }

  return code;

_err:
  tsdbError("vgId:%d, tsdb commit table mem data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbGetNumOfRowsLessThan(STbDataIter *pIter, TSDBKEY key) {
  int32_t nRow = 0;

  STbDataIter iter = *pIter;
  while (true) {
    TSDBROW *pRow = tsdbTbDataIterGet(&iter);
    if (pRow == NULL) break;

    int32_t c = tsdbKeyCmprFn(&TSDBROW_KEY(pRow), &key);
    if (c < 0) {
      nRow++;
      tsdbTbDataIterNext(&iter);
    } else if (c > 0) {
      break;
    } else {
      ASSERT(0);
    }
  }

  return nRow;
}

static int32_t tsdbMergeAsSubBlock(SCommitter *pCommitter, STbDataIter *pIter, SBlock *pBlock) {
  int32_t     code = 0;
  STbData    *pTbData = pIter->pTbData;
  SBlockData *pBlockData = &pCommitter->dWriter.bData;

  tBlockDataClear(pBlockData);
  TSDBROW *pRow = tsdbTbDataIterGet(pIter);
  while (true) {
    if (pRow == NULL) break;

    code = tsdbCommitterUpdateRowSchema(pCommitter, pTbData->suid, pTbData->uid, TSDBROW_SVERSION(pRow));
    if (code) goto _err;

    code = tBlockDataAppendRow(pBlockData, pRow, pCommitter->skmRow.pTSchema, pTbData->uid);
    if (code) goto _err;

    tsdbTbDataIterNext(pIter);
    pRow = tsdbTbDataIterGet(pIter);
    if (pRow) {
      TSDBKEY rowKey = TSDBROW_KEY(pRow);
      if (tsdbKeyCmprFn(&rowKey, &pBlock->maxKey) > 0) {
        pRow = NULL;
      }
    }
  }

  ASSERT(pBlockData->nRow > 0 && pBlock->nRow + pBlockData->nRow <= pCommitter->maxRow);

  code = tsdbCommitDataBlock(pCommitter, pBlock);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d, tsdb merge as subblock failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitLastFile(SCommitter *pCommitter, STbDataIter *pIter) {
  int32_t     code = 0;
  STbData    *pTbData = pIter->pTbData;
  SBlockData *pBlockData = &pCommitter->dWriter.bDatal;
  TSDBROW    *pRow = tsdbTbDataIterGet(pIter);

  if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) {
    pRow = NULL;
  }

  if (pRow == NULL) goto _exit;

  if (pBlockData->suid || pBlockData->uid) {
    if (pBlockData->suid != pTbData->suid || pBlockData->suid == 0) {
      if (pBlockData->nRow > 0) {
        code = tsdbCommitLastBlock(pCommitter);
        if (code) goto _err;
      }

      tBlockDataReset(pBlockData);
    }
  }

  if (!pBlockData->suid && !pBlockData->uid) {
    code = tBlockDataInit(pBlockData, pTbData->suid, 0, pCommitter->skmTable.pTSchema);
    if (code) goto _err;
  }

  while (pRow) {
    code = tsdbCommitterUpdateRowSchema(pCommitter, pTbData->suid, pTbData->uid, TSDBROW_SVERSION(pRow));
    if (code) goto _err;

    code = tBlockDataAppendRow(pBlockData, pRow, pCommitter->skmRow.pTSchema, pTbData->uid);
    if (code) goto _err;

    tsdbTbDataIterNext(pIter);
    pRow = tsdbTbDataIterGet(pIter);
    if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) {
      pRow = NULL;
    }

    if (pBlockData->nRow >= pCommitter->maxRow) {
      code = tsdbCommitLastBlock(pCommitter);
      if (code) goto _err;
    }
  }

_exit:
  return code;

_err:
  tsdbError("vgId:%d tsdb merge commit last failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbMergeCommitData(SCommitter *pCommitter, STbDataIter *pIter) {
  int32_t  code = 0;
  STbData *pTbData = pIter->pTbData;
  int32_t  iBlock = 0;
  SBlock   block;
  SBlock  *pBlock = &block;
  TSDBROW *pRow = tsdbTbDataIterGet(pIter);

  if (pCommitter->dReader.pBlockIdx && tTABLEIDCmprFn(pTbData, pCommitter->dReader.pBlockIdx) == 0) {
    tMapDataGetItemByIdx(&pCommitter->dReader.mBlock, iBlock, pBlock, tGetBlock);
  } else {
    pBlock = NULL;
  }

  while (pBlock && pRow) {
    SBlock  tBlock = {.minKey = TSDBROW_KEY(pRow), .maxKey = TSDBROW_KEY(pRow)};
    int32_t c = tBlockCmprFn(pBlock, &tBlock);

    if (c < 0) {
      code = tMapDataPutItem(&pCommitter->dWriter.mBlock, pBlock, tPutBlock);
      if (code) goto _err;

      iBlock++;
      if (iBlock < pCommitter->dReader.mBlock.nItem) {
        tMapDataGetItemByIdx(&pCommitter->dReader.mBlock, iBlock, pBlock, tGetBlock);
      } else {
        pBlock = NULL;
      }
    } else if (c > 0) {
      code = tsdbCommitTableMemData(pCommitter, pIter, pBlock->minKey);
      if (code) goto _err;

      pRow = tsdbTbDataIterGet(pIter);
      if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) {
        pRow = NULL;
      }
    } else {
      int32_t nOvlp = tsdbGetNumOfRowsLessThan(pIter, pBlock->maxKey);

      ASSERT(nOvlp > 0);

      if (pBlock->nRow + nOvlp <= pCommitter->maxRow && pBlock->nSubBlock < TSDB_MAX_SUBBLOCKS) {
        code = tsdbMergeAsSubBlock(pCommitter, pIter, pBlock);
        if (code) goto _err;
      } else {
        code = tsdbMergeCommitDataBlock(pCommitter, pIter, pBlock);
        if (code) goto _err;
      }

      // next
      pRow = tsdbTbDataIterGet(pIter);
      if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) {
        pRow = NULL;
      }
      iBlock++;
      if (iBlock < pCommitter->dReader.mBlock.nItem) {
        tMapDataGetItemByIdx(&pCommitter->dReader.mBlock, iBlock, pBlock, tGetBlock);
      } else {
        pBlock = NULL;
      }
    }
  }

  while (pBlock) {
    code = tMapDataPutItem(&pCommitter->dWriter.mBlock, pBlock, tPutBlock);
    if (code) goto _err;

    iBlock++;
    if (iBlock < pCommitter->dReader.mBlock.nItem) {
      tMapDataGetItemByIdx(&pCommitter->dReader.mBlock, iBlock, pBlock, tGetBlock);
    } else {
      pBlock = NULL;
    }
  }

_exit:
  return code;

_err:
  return code;
}

static int32_t tsdbCommitTableData(SCommitter *pCommitter, STbData *pTbData) {
  int32_t code = 0;

  ASSERT(pCommitter->dReader.pBlockIdx == NULL || tTABLEIDCmprFn(pCommitter->dReader.pBlockIdx, pTbData) >= 0);

  // merge commit table data
  STbDataIter iter = {0};
  TSDBROW    *pRow;

  tMapDataReset(&pCommitter->dWriter.mBlock);

  tsdbTbDataIterOpen(pTbData, &(TSDBKEY){.ts = pCommitter->minKey, .version = VERSION_MIN}, 0, &iter);
  pRow = tsdbTbDataIterGet(&iter);
  if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) {
    pRow = NULL;
  }
  if (pRow == NULL) {
    if (pCommitter->dReader.pBlockIdx && tTABLEIDCmprFn(pCommitter->dReader.pBlockIdx, pTbData) == 0) {
      code = tMapDataCopy(&pCommitter->dReader.mBlock, &pCommitter->dWriter.mBlock);
      if (code) goto _err;
    }
    goto _exit;
  }

  code = tsdbCommitterUpdateTableSchema(pCommitter, pTbData->suid, pTbData->uid);
  if (code) goto _err;
  code = tBlockDataInit(&pCommitter->dReader.bData, pTbData->suid, pTbData->uid, pCommitter->skmTable.pTSchema);
  if (code) goto _err;
  code = tBlockDataInit(&pCommitter->dWriter.bData, pTbData->suid, pTbData->uid, pCommitter->skmTable.pTSchema);
  if (code) goto _err;

  // commit data
  code = tsdbMergeCommitData(pCommitter, &iter);
  if (code) goto _err;

  // commit last
  code = tsdbCommitLastFile(pCommitter, &iter);
  if (code) goto _err;

_exit:
  if (pCommitter->dWriter.mBlock.nItem > 0) {
    SBlockIdx blockIdx = {.suid = pTbData->suid, .uid = pTbData->uid};
    code = tsdbWriteBlock(pCommitter->dWriter.pWriter, &pCommitter->dWriter.mBlock, &blockIdx);
    if (code) goto _err;

    if (taosArrayPush(pCommitter->dWriter.aBlockIdx, &blockIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }
  pRow = tsdbTbDataIterGet(&iter);
  if (pRow) {
    pCommitter->nextKey = TMIN(pCommitter->nextKey, TSDBROW_TS(pRow));
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb commit table data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitFileDataEnd(SCommitter *pCommitter) {
  int32_t code = 0;

  // write aBlockIdx
  code = tsdbWriteBlockIdx(pCommitter->dWriter.pWriter, pCommitter->dWriter.aBlockIdx);
  if (code) goto _err;

  // write aBlockL
  code = tsdbWriteBlockL(pCommitter->dWriter.pWriter, pCommitter->dWriter.aBlockL);
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

  // .data
  while (true) {
    if (pCommitter->dReader.pBlockIdx == NULL || tTABLEIDCmprFn(pCommitter->dReader.pBlockIdx, &toTable) >= 0) break;

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

static int32_t tsdbCommitFileData(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  // commit file data start
  code = tsdbCommitFileDataStart(pCommitter);
  if (code) goto _err;

  // commit file data impl
  for (int32_t iTbData = 0; iTbData < taosArrayGetSize(pCommitter->aTbDataP); iTbData++) {
    STbData *pTbData = (STbData *)taosArrayGetP(pCommitter->aTbDataP, iTbData);

    // move commit until current (suid, uid)
    code = tsdbMoveCommitData(pCommitter, *(TABLEID *)pTbData);
    if (code) goto _err;

    // commit current table data
    code = tsdbCommitTableData(pCommitter, pTbData);
    if (code) goto _err;

    // move next reader table data if need
    if (pCommitter->dReader.pBlockIdx && tTABLEIDCmprFn(pTbData, pCommitter->dReader.pBlockIdx) == 0) {
      code = tsdbCommitterNextTableData(pCommitter);
      if (code) goto _err;
    }
  }

  code = tsdbMoveCommitData(pCommitter, (TABLEID){.suid = INT64_MAX, .uid = INT64_MAX});
  if (code) goto _err;

  if (pCommitter->dWriter.bDatal.nRow > 0) {
    code = tsdbCommitLastBlock(pCommitter);
    if (code) goto _err;
  }

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
  pCommitter->maxLast = TSDB_DEFAULT_LAST_FILE;  // TODO: make it as a config
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

  // Reader
  pCommitter->dReader.aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pCommitter->dReader.aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  code = tBlockDataCreate(&pCommitter->dReader.bData);
  if (code) goto _exit;

  // Writer
  pCommitter->dWriter.aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pCommitter->dWriter.aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  pCommitter->dWriter.aBlockL = taosArrayInit(0, sizeof(SBlockL));
  if (pCommitter->dWriter.aBlockL == NULL) {
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
  // Reader
  taosArrayDestroy(pCommitter->dReader.aBlockIdx);
  tMapDataClear(&pCommitter->dReader.mBlock);
  tBlockDataDestroy(&pCommitter->dReader.bData, 1);

  // Writer
  taosArrayDestroy(pCommitter->dWriter.aBlockIdx);
  taosArrayDestroy(pCommitter->dWriter.aBlockL);
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

  if (pCommitter->toMerge) {
    code = tsdbMerge(pTsdb);
    if (code) goto _err;
  }

  tsdbInfo("vgId:%d, tsdb end commit", TD_VID(pTsdb->pVnode));
  return code;

_err:
  tsdbError("vgId:%d, tsdb end commit failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

// Merger =====================================================================
typedef struct {
  int64_t suid;
  int64_t uid;
  TSDBROW row;
} SRowInfo;

typedef struct {
  SRowInfo      rowInfo;
  SDataFReader *pReader;
  int32_t       iLast;
  SArray       *aBlockL;  // SArray<SBlockL>
  int32_t       iBlockL;
  SBlockData    bData;
  int32_t       iRow;
} SLDataIter;

typedef struct {
  SRBTreeNode *pNode;
  SRBTree      rbt;
} SDataMerger;

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

static void tDataMergerInit(SDataMerger *pMerger, SArray *aNodeP) {
  pMerger->pNode = NULL;
  tRBTreeCreate(&pMerger->rbt, tRowInfoCmprFn);
  for (int32_t iNode = 0; iNode < taosArrayGetSize(aNodeP); iNode++) {
    SRBTreeNode *pNode = (SRBTreeNode *)taosArrayGetP(aNodeP, iNode);

    pNode = tRBTreePut(&pMerger->rbt, pNode);
    ASSERT(pNode);
  }
}

extern int32_t tsdbReadLastBlockEx(SDataFReader *pReader, int32_t iLast, SBlockL *pBlockL,
                                   SBlockData *pBlockData);  // todo

static int32_t tDataMergeNext(SDataMerger *pMerger, SRowInfo **ppInfo) {
  int32_t code = 0;

  if (pMerger->pNode) {
    // next current iter
    SLDataIter *pIter = (SLDataIter *)pMerger->pNode->payload;

    pIter->iRow++;
    if (pIter->iRow < pIter->bData.nRow) {
      pIter->rowInfo.uid = pIter->bData.uid ? pIter->bData.uid : pIter->bData.aUid[pIter->iRow];
      pIter->rowInfo.row = tsdbRowFromBlockData(&pIter->bData, pIter->iRow);
    } else {
      pIter->iBlockL++;
      if (pIter->iBlockL < taosArrayGetSize(pIter->aBlockL)) {
        SBlockL *pBlockL = (SBlockL *)taosArrayGet(pIter->aBlockL, pIter->iBlockL);
        code = tsdbReadLastBlockEx(pIter->pReader, pIter->iLast, pBlockL, &pIter->bData);
        if (code) goto _exit;

        pIter->iRow = 0;
        pIter->rowInfo.suid = pIter->bData.suid;
        pIter->rowInfo.uid = pIter->bData.uid ? pIter->bData.uid : pIter->bData.aUid[0];
        pIter->rowInfo.row = tsdbRowFromBlockData(&pIter->bData, 0);
      } else {
        pMerger->pNode = NULL;
      }
    }

    SRBTreeNode *pMinNode = tRBTreeMin(&pMerger->rbt);
    if (pMerger->pNode && pMinNode) {
      int32_t c = tRowInfoCmprFn(pMerger->pNode->payload, pMinNode->payload);
      if (c > 0) {
        pMerger->pNode = tRBTreePut(&pMerger->rbt, pMerger->pNode);
        ASSERT(pMerger->pNode);
        pMerger->pNode = NULL;
      } else {
        ASSERT(c);
      }
    }
  }

  if (pMerger->pNode == NULL) {
    pMerger->pNode = tRBTreeMin(&pMerger->rbt);
    if (pMerger->pNode) {
      tRBTreeDrop(&pMerger->rbt, pMerger->pNode);
    }
  }

  if (pMerger->pNode) {
    *ppInfo = &((SLDataIter *)pMerger->pNode->payload)[0].rowInfo;
  } else {
    *ppInfo = NULL;
  }

_exit:
  return code;
}

// ================================================================================
typedef struct {
  STsdb  *pTsdb;
  int8_t  maxLast;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  int64_t commitID;
  STsdbFS fs;
  struct {
    SDataFReader *pReader;
    SArray       *aBlockIdx;
    SLDataIter   *aLDataiter[TSDB_MAX_LAST_FILE];
    SDataMerger   merger;
  } dReader;
  struct {
    SDataFWriter *pWriter;
    SArray       *aBlockIdx;
    SArray       *aBlockL;
    SBlockData    bData;
    SBlockData    bDatal;
  } dWriter;
} STsdbMerger;

static int32_t tsdbMergeFileDataStart(STsdbMerger *pMerger, SDFileSet *pSet) {
  int32_t code = 0;
  STsdb  *pTsdb = pMerger->pTsdb;

  // reader
  code = tsdbDataFReaderOpen(&pMerger->dReader.pReader, pTsdb, pSet);
  if (code) goto _err;

  code = tsdbReadBlockIdx(pMerger->dReader.pReader, pMerger->dReader.aBlockIdx);
  if (code) goto _err;

  pMerger->dReader.merger.pNode = NULL;
  tRBTreeCreate(&pMerger->dReader.merger.rbt, tRowInfoCmprFn);
  for (int8_t iLast = 0; iLast < pSet->nLastF; iLast++) {
    SRBTreeNode *pNode = (SRBTreeNode *)taosMemoryCalloc(1, sizeof(*pNode) + sizeof(SLDataIter));
    if (pNode == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }

    SLDataIter *pIter = (SLDataIter *)pNode->payload;
    pIter->pReader = pMerger->dReader.pReader;
    pIter->iLast = iLast;

    pIter->aBlockL = taosArrayInit(0, sizeof(SBlockL));
    if (pIter->aBlockL == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    code = tBlockDataCreate(&pIter->bData);
    if (code) goto _err;

    code = tsdbReadBlockL(pMerger->dReader.pReader, iLast, pIter->aBlockL);
    if (code) goto _err;

    if (taosArrayGetSize(pIter->aBlockL) == 0) continue;
    pIter->iBlockL = 0;

    SBlockL *pBlockL = (SBlockL *)taosArrayGet(pIter->aBlockL, 0);
    code = tsdbReadLastBlockEx(pMerger->dReader.pReader, iLast, pBlockL, &pIter->bData);
    if (code) goto _err;

    pIter->iRow = 0;
    pIter->rowInfo.suid = pIter->bData.suid;
    pIter->rowInfo.uid = pIter->bData.uid ? pIter->bData.uid : pIter->bData.aUid[0];
    pIter->rowInfo.row = tsdbRowFromBlockData(&pIter->bData, 0);

    pNode = tRBTreePut(&pMerger->dReader.merger.rbt, pNode);
    ASSERT(pNode);

    pMerger->dReader.aLDataiter[iLast] = pIter;
  }

  // writer
  SHeadFile fHead = {.commitID = pMerger->commitID};
  SDataFile fData = *pSet->pDataF;
  SSmaFile  fSma = *pSet->pSmaF;
  SLastFile fLast = {.commitID = pMerger->commitID};
  SDFileSet wSet = {.diskId = pSet->diskId,
                    .fid = pSet->fid,
                    .nLastF = 1,
                    .pHeadF = &fHead,
                    .pDataF = &fData,
                    .pSmaF = &fSma,
                    .aLastF[0] = &fLast};
  code = tsdbDataFWriterOpen(&pMerger->dWriter.pWriter, pTsdb, &wSet);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d tsdb merge file data start failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbMergeFileDataEnd(STsdbMerger *pMerger) {
  int32_t code = 0;
  STsdb  *pTsdb = pMerger->pTsdb;

  // write aBlockIdx
  code = tsdbWriteBlockIdx(pMerger->dWriter.pWriter, pMerger->dWriter.aBlockIdx);
  if (code) goto _err;

  // write aBlockL
  code = tsdbWriteBlockL(pMerger->dWriter.pWriter, pMerger->dWriter.aBlockL);
  if (code) goto _err;

  // update file header
  code = tsdbUpdateDFileSetHeader(pMerger->dWriter.pWriter);
  if (code) goto _err;

  // upsert SDFileSet
  code = tsdbFSUpsertFSet(&pMerger->fs, &pMerger->dWriter.pWriter->wSet);
  if (code) goto _err;

  // close and sync
  code = tsdbDataFWriterClose(&pMerger->dWriter.pWriter, 1);
  if (code) goto _err;

  if (pMerger->dReader.pReader) {
    code = tsdbDataFReaderClose(&pMerger->dReader.pReader);
    if (code) goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb merge file data end failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbMergeFileData(STsdbMerger *pMerger, SDFileSet *pSet) {
  int32_t code = 0;
  STsdb  *pTsdb = pMerger->pTsdb;

  // start
  code = tsdbMergeFileDataStart(pMerger, pSet);
  if (code) goto _err;

  // impl
  SRowInfo *pInfo;
  TABLEID   id = {0};
  while (true) {
    code = tDataMergeNext(&pMerger->dReader.merger, &pInfo);
    if (code) goto _err;

    if (pInfo == NULL) {
      if (pMerger->dWriter.bData.nRow > 0) {
        // TODO
      }

      if (pMerger->dWriter.bDatal.nRow > 0) {
        // TODO
      }

      break;
    }

    if (id.suid != pInfo->suid || id.uid != pInfo->uid) {
      while (true) {
        // move commit the head data
      }

      // prepare to commit next
    }

    code = tBlockDataAppendRow(&pMerger->dWriter.bData, &pInfo->row, NULL, pInfo->uid);
    if (code) goto _err;

    if (pMerger->dWriter.bData.nRow >= pMerger->maxRow * 4 / 5) {
      // code = tsdbCommitDataBlock();
      if (code) goto _err;
    }
  }

  // end
  code = tsdbMergeFileDataEnd(pMerger);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d tsdb merge file data failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbStartMerge(STsdbMerger *pMerger, STsdb *pTsdb) {
  int32_t code = 0;

  pMerger->pTsdb = pTsdb;
  pMerger->maxLast = TSDB_DEFAULT_LAST_FILE;
  pMerger->commitID = ++pTsdb->pVnode->state.commitID;
  code = tsdbFSCopy(pTsdb, &pMerger->fs);
  if (code) goto _exit;

  // reader
  pMerger->dReader.aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pMerger->dReader.aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  // for (int8_t iLast = 0; iLast < TSDB_MAX_LAST_FILE; iLast++) {
  //   pMerger->dReader.aBlockL[iLast] = taosArrayInit(0, sizeof(SBlockL));
  //   if (pMerger->dReader.aBlockL[iLast] == NULL) {
  //     code = TSDB_CODE_OUT_OF_MEMORY;
  //     goto _exit;
  //   }
  // }

  // writer
  pMerger->dWriter.aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pMerger->dWriter.aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  pMerger->dWriter.aBlockL = taosArrayInit(0, sizeof(SBlockL));
  if (pMerger->dWriter.aBlockL == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

_exit:
  return code;
}

static int32_t tsdbEndMerge(STsdbMerger *pMerger) {
  int32_t code = 0;
  STsdb  *pTsdb = pMerger->pTsdb;

  code = tsdbFSCommit1(pTsdb, &pMerger->fs);
  if (code) goto _err;

  taosThreadRwlockWrlock(&pTsdb->rwLock);
  code = tsdbFSCommit2(pTsdb, &pMerger->fs);
  if (code) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    goto _err;
  }
  taosThreadRwlockUnlock(&pTsdb->rwLock);

  // writer
  taosArrayDestroy(pMerger->dWriter.aBlockL);
  taosArrayDestroy(pMerger->dWriter.aBlockIdx);

  // reader
  // for (int8_t iLast = 0; iLast < TSDB_MAX_LAST_FILE; iLast++) {
  //   taosArrayDestroy(pMerger->dReader.aBlockL[iLast]);
  // }
  taosArrayDestroy(pMerger->dReader.aBlockIdx);
  tsdbFSDestroy(&pMerger->fs);

  return code;

_err:
  tsdbError("vgId:%d, tsdb end merge failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbMerge(STsdb *pTsdb) {
  int32_t     code = 0;
  STsdbMerger merger = {0};

  code = tsdbStartMerge(&merger, pTsdb);
  if (code) goto _err;

  for (int32_t iSet = 0; iSet < taosArrayGetSize(merger.fs.aDFileSet); iSet++) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(merger.fs.aDFileSet, iSet);
    if (pSet->nLastF < merger.maxLast) continue;

    code = tsdbMergeFileData(&merger, pSet);
    if (code) goto _err;
  }

  code = tsdbEndMerge(&merger);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d tsdb merge failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}