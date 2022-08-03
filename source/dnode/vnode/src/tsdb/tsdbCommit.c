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
  int64_t suid;
  int64_t uid;
  TSDBROW row;
} SRowInfo;

typedef struct {
  STsdb *pTsdb;
  /* commit data */
  int64_t commitID;
  int32_t minutes;
  int8_t  precision;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  STsdbFS fs;
  // --------------
  TSKEY   nextKey;  // reset by each table commit
  int32_t commitFid;
  TSKEY   minKey;
  TSKEY   maxKey;
  // commit file data
  struct {
    SDataFReader *pReader;
    // data
    SArray    *aBlockIdx;  // SArray<SBlockIdx>
    int32_t    iBlockIdx;
    SBlockIdx *pBlockIdx;
    SMapData   mBlock;  // SMapData<SBlock>
    SBlockData bData;
    // last
    SArray    *aBlockL;  // SArray<SBlockL>
    int32_t    iBlockL;
    SBlockL   *pBlockL;
    SBlockData bDatal;
    int32_t    iRow;
    SRowInfo  *pRowInfo;
    SRowInfo   rowInfo;
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
    code = tsdbDelFReaderOpen(&pCommitter->pDelFReader, pDelFileR, pTsdb, NULL);
    if (code) goto _err;

    code = tsdbReadDelIdx(pCommitter->pDelFReader, pCommitter->aDelIdx, NULL);
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

    code = tsdbReadDelData(pCommitter->pDelFReader, pDelIdx, pCommitter->aDelData, NULL);
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
  code = tsdbWriteDelData(pCommitter->pDelFWriter, pCommitter->aDelData, NULL, &delIdx);
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

  code = tsdbWriteDelIdx(pCommitter->pDelFWriter, pCommitter->aDelIdxN, NULL);
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

static int32_t tsdbCommitNextLastRow(SCommitter *pCommitter) {
  int32_t code = 0;

  ASSERT(pCommitter->dReader.pReader);
  ASSERT(pCommitter->dReader.pRowInfo);

  pCommitter->dReader.iRow++;
  if (pCommitter->dReader.iRow < pCommitter->dReader.bDatal.nRow) {
    pCommitter->dReader.pRowInfo->uid = pCommitter->dReader.bData.aUid[pCommitter->dReader.iRow];
    pCommitter->dReader.pRowInfo->row = tsdbRowFromBlockData(&pCommitter->dReader.bDatal, pCommitter->dReader.iRow);
  } else {
    pCommitter->dReader.iBlockL++;
    if (pCommitter->dReader.iBlockL < taosArrayGetSize(pCommitter->dReader.aBlockL)) {
      pCommitter->dReader.pBlockL = (SBlockL *)taosArrayGet(pCommitter->dReader.aBlockL, pCommitter->dReader.iBlockL);
      code = tsdbReadLastBlock(pCommitter->dReader.pReader, pCommitter->dReader.pBlockL, &pCommitter->dReader.bDatal,
                               NULL, NULL);
      if (code) goto _exit;

      pCommitter->dReader.iRow = 0;
      pCommitter->dReader.pRowInfo->suid = pCommitter->dReader.pBlockL->suid;
      pCommitter->dReader.pRowInfo->uid = pCommitter->dReader.bData.aUid[pCommitter->dReader.iRow];
      pCommitter->dReader.pRowInfo->row = tsdbRowFromBlockData(&pCommitter->dReader.bDatal, pCommitter->dReader.iRow);
    } else {
      pCommitter->dReader.pRowInfo = NULL;
    }
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
    code = tsdbReadBlockIdx(pCommitter->dReader.pReader, pCommitter->dReader.aBlockIdx, NULL);
    if (code) goto _err;

    pCommitter->dReader.iBlockIdx = 0;
    if (pCommitter->dReader.iBlockIdx < taosArrayGetSize(pCommitter->dReader.aBlockIdx)) {
      pCommitter->dReader.pBlockIdx =
          (SBlockIdx *)taosArrayGet(pCommitter->dReader.aBlockIdx, pCommitter->dReader.iBlockIdx);

      code =
          tsdbReadBlock(pCommitter->dReader.pReader, pCommitter->dReader.pBlockIdx, &pCommitter->dReader.mBlock, NULL);
      if (code) goto _err;
    } else {
      pCommitter->dReader.pBlockIdx = NULL;
    }

    // last
    code = tsdbReadBlockL(pCommitter->dReader.pReader, pCommitter->dReader.aBlockL, NULL);
    if (code) goto _err;

    pCommitter->dReader.iBlockL = -1;
    pCommitter->dReader.bDatal.nRow = 0;
    pCommitter->dReader.iRow = -1;
    pCommitter->dReader.pRowInfo = &pCommitter->dReader.rowInfo;
    code = tsdbCommitNextLastRow(pCommitter);
    if (code) goto _err;
  } else {
    pCommitter->dReader.pBlockIdx = NULL;
    pCommitter->dReader.pRowInfo = NULL;
  }

  // Writer
  SHeadFile fHead;
  SDataFile fData;
  SLastFile fLast;
  SSmaFile  fSma;
  SDFileSet wSet = {.pHeadF = &fHead, .pDataF = &fData, .pLastF = &fLast, .pSmaF = &fSma};
  if (pRSet) {
    wSet.diskId = pRSet->diskId;
    wSet.fid = pCommitter->commitFid;
    fHead = (SHeadFile){.commitID = pCommitter->commitID, .offset = 0, .size = 0, .loffset = 0};
    fData = *pRSet->pDataF;
    fLast = (SLastFile){.commitID = pCommitter->commitID, .size = 0};
    fSma = *pRSet->pSmaF;
  } else {
    SDiskID did = {0};

    tfsAllocDisk(pTsdb->pVnode->pTfs, 0, &did);

    tfsMkdirRecurAt(pTsdb->pVnode->pTfs, pTsdb->path, did);

    wSet.diskId = did;
    wSet.fid = pCommitter->commitFid;
    fHead = (SHeadFile){.commitID = pCommitter->commitID, .offset = 0, .size = 0, .loffset = 0};
    fData = (SDataFile){.commitID = pCommitter->commitID, .size = 0};
    fLast = (SLastFile){.commitID = pCommitter->commitID, .size = 0};
    fSma = (SSmaFile){.commitID = pCommitter->commitID, .size = 0};
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

static int32_t tsdbCommitterUpdateTableSchema(SCommitter *pCommitter, int64_t suid, int64_t uid, int32_t sver) {
  int32_t code = 0;

  if (pCommitter->skmTable.pTSchema) {
    if (pCommitter->skmTable.suid == suid) {
      if (suid == 0) {
        if (pCommitter->skmTable.uid == uid && sver == pCommitter->skmTable.pTSchema->version) goto _exit;
      } else {
        if (sver == pCommitter->skmTable.pTSchema->version) goto _exit;
      }
    }
  }

  pCommitter->skmTable.suid = suid;
  pCommitter->skmTable.uid = uid;
  tTSchemaDestroy(pCommitter->skmTable.pTSchema);
  code = metaGetTbTSchemaEx(pCommitter->pTsdb->pVnode->pMeta, suid, uid, sver, &pCommitter->skmTable.pTSchema);
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

static int32_t tsdbCommitBlockData(SCommitter *pCommitter, SBlockData *pBlockData, SBlock *pBlock, SBlockIdx *pBlockIdx,
                                   int8_t toDataOnly) {
  int32_t code = 0;

  if (pBlock->nSubBlock == 0) {
    if (!toDataOnly && pBlockData->nRow < pCommitter->minRow) {
      pBlock->last = 1;
    } else {
      pBlock->last = 0;
    }
  }

  code =
      tsdbWriteBlockData(pCommitter->dWriter.pWriter, pBlockData, NULL, NULL, pBlockIdx, pBlock, pCommitter->cmprAlg);
  if (code) goto _err;

  code = tMapDataPutItem(&pCommitter->dWriter.mBlock, pBlock, tPutBlock);
  if (code) goto _err;

  return code;

_err:
  return code;
}

static int32_t tsdbCommitDataBlock(SCommitter *pCommitter) {
  int32_t code = 0;
  SBlock  block;

  ASSERT(pCommitter->dWriter.bData.nRow > 0);

  code = tsdbWriteDataBlock(pCommitter->dWriter.pWriter, &pCommitter->dWriter.bData, &block, NULL, NULL,
                            pCommitter->cmprAlg);
  if (code) goto _exit;

  code = tMapDataPutItem(&pCommitter->dWriter.mBlock, &block, tPutBlock);
  if (code) goto _exit;

  tBlockDataClearData(&pCommitter->dWriter.bData);

_exit:
  return code;
}

static int32_t tsdbCommitLastBlock(SCommitter *pCommitter) {
  int32_t code = 0;
  SBlockL blockL;

  ASSERT(pCommitter->dWriter.bDatal.nRow > 0);

  code = tsdbWriteLastBlock(pCommitter->dWriter.pWriter, &pCommitter->dWriter.bDatal, &blockL, NULL, NULL,
                            pCommitter->cmprAlg);
  if (code) goto _exit;

  if (taosArrayPush(pCommitter->dWriter.aBlockL, &blockL) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  tBlockDataClearData(&pCommitter->dWriter.bDatal);

_exit:
  return code;
}

static int32_t tsdbMergeTableData(SCommitter *pCommitter, STbDataIter *pIter, SBlock *pBlockMerge, TSDBKEY toKey,
                                  int8_t toDataOnly) {
  int32_t     code = 0;
  SBlockIdx  *pBlockIdx = &(SBlockIdx){.suid = pIter->pTbData->suid, .uid = pIter->pTbData->uid};
  SBlockData *pBlockDataMerge = &pCommitter->dReader.bData;
  SBlockData *pBlockData = &pCommitter->dWriter.bData;
  SBlock      block;
  SBlock     *pBlock = &block;
  TSDBROW    *pRow1;
  TSDBROW     row2;
  TSDBROW    *pRow2 = &row2;

  // read SBlockData
  code = tsdbReadBlockData(pCommitter->dReader.pReader, pBlockIdx, pBlockMerge, pBlockDataMerge, NULL, NULL);
  if (code) goto _err;

  code = tBlockDataSetSchema(pBlockData, pCommitter->skmTable.pTSchema);
  if (code) goto _err;

  // loop to merge
  pRow1 = tsdbTbDataIterGet(pIter);
  *pRow2 = tsdbRowFromBlockData(pBlockDataMerge, 0);
  ASSERT(pRow1 && tsdbKeyCmprFn(&TSDBROW_KEY(pRow1), &toKey) < 0);
  ASSERT(tsdbKeyCmprFn(&TSDBROW_KEY(pRow2), &toKey) < 0);
  code = tsdbCommitterUpdateRowSchema(pCommitter, pBlockIdx->suid, pBlockIdx->uid, TSDBROW_SVERSION(pRow1));
  if (code) goto _err;

  tBlockReset(pBlock);
  tBlockDataClearData(pBlockData);
  while (true) {
    if (pRow1 == NULL && pRow2 == NULL) {
      if (pBlockData->nRow == 0) {
        break;
      } else {
        goto _write_block;
      }
    }

    if (pRow1 && pRow2) {
      int32_t c = tsdbRowCmprFn(pRow1, pRow2);
      if (c < 0) {
        goto _append_mem_row;
      } else if (c > 0) {
        goto _append_block_row;
      } else {
        ASSERT(0);
      }
    } else if (pRow1) {
      goto _append_mem_row;
    } else {
      goto _append_block_row;
    }

  _append_mem_row:
    code = tBlockDataAppendRow(pBlockData, pRow1, pCommitter->skmRow.pTSchema);
    if (code) goto _err;

    tsdbTbDataIterNext(pIter);
    pRow1 = tsdbTbDataIterGet(pIter);
    if (pRow1) {
      if (tsdbKeyCmprFn(&TSDBROW_KEY(pRow1), &toKey) < 0) {
        code = tsdbCommitterUpdateRowSchema(pCommitter, pBlockIdx->suid, pBlockIdx->uid, TSDBROW_SVERSION(pRow1));
        if (code) goto _err;
      } else {
        pRow1 = NULL;
      }
    }

    if (pBlockData->nRow >= pCommitter->maxRow * 4 / 5) {
      goto _write_block;
    } else {
      continue;
    }

  _append_block_row:
    code = tBlockDataAppendRow(pBlockData, pRow2, NULL);
    if (code) goto _err;

    if (pRow2->iRow + 1 < pBlockDataMerge->nRow) {
      *pRow2 = tsdbRowFromBlockData(pBlockDataMerge, pRow2->iRow + 1);
    } else {
      pRow2 = NULL;
    }

    if (pBlockData->nRow >= pCommitter->maxRow * 4 / 5) {
      goto _write_block;
    } else {
      continue;
    }

  _write_block:
    code = tsdbCommitBlockData(pCommitter, pBlockData, pBlock, pBlockIdx, toDataOnly);
    if (code) goto _err;

    tBlockReset(pBlock);
    tBlockDataClearData(pBlockData);
  }

  return code;

_err:
  tsdbError("vgId:%d, tsdb merge block and mem failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableMemData(SCommitter *pCommitter, STbDataIter *pIter, TSDBKEY toKey, int8_t toDataOnly) {
  int32_t code = 0;
#if 0
  TSDBROW    *pRow;
  SBlock      block;
  SBlock     *pBlock = &block;
  SBlockData *pBlockData = &pCommitter->dWriter.bData;
  int64_t     suid = pIter->pTbData->suid;
  int64_t     uid = pIter->pTbData->uid;

  code = tBlockDataSetSchema(pBlockData, pCommitter->skmTable.pTSchema);
  if (code) goto _err;

  tBlockReset(pBlock);
  tBlockDataClearData(pBlockData);
  pRow = tsdbTbDataIterGet(pIter);
  ASSERT(pRow && tsdbKeyCmprFn(&TSDBROW_KEY(pRow), &toKey) < 0);
  while (true) {
    if (pRow == NULL) {
      if (pBlockData->nRow > 0) {
        goto _write_block;
      } else {
        break;
      }
    }

    // update schema
    code = tsdbCommitterUpdateRowSchema(pCommitter, suid, uid, TSDBROW_SVERSION(pRow));
    if (code) goto _err;

    // append
    code = tBlockDataAppendRow(pBlockData, pRow, pCommitter->skmRow.pTSchema);
    if (code) goto _err;

    tsdbTbDataIterNext(pIter);
    pRow = tsdbTbDataIterGet(pIter);
    // if (pRow && tsdbKeyCmprFn(&TSDBROW_KEY(pRow), &toKey) >= 0) pRow = NULL;
    // crash on CI, use the block following
    if (pRow) {
      TSDBKEY tmpKey = TSDBROW_KEY(pRow);
      if (tsdbKeyCmprFn(&tmpKey, &toKey) >= 0) {
        pRow = NULL;
      }
    }

    if (pBlockData->nRow >= pCommitter->maxRow * 4 / 5) goto _write_block;
    continue;

  _write_block:
    code = tsdbCommitBlockData(pCommitter, pBlockData, pBlock, &(SBlockIdx){.suid = suid, .uid = uid}, toDataOnly);
    if (code) goto _err;

    tBlockReset(pBlock);
    tBlockDataClearData(pBlockData);
  }

#endif
  return code;

_err:
  tsdbError("vgId:%d, tsdb commit table mem data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableDiskData(SCommitter *pCommitter, SBlock *pBlock, SBlockIdx *pBlockIdx) {
  int32_t code = 0;
  SBlock  block;

  if (pBlock->last) {
    code = tsdbReadBlockData(pCommitter->dReader.pReader, pBlockIdx, pBlock, &pCommitter->dReader.bData, NULL, NULL);
    if (code) goto _err;

    tBlockReset(&block);
    code = tsdbCommitBlockData(pCommitter, &pCommitter->dReader.bData, &block, pBlockIdx, 0);
    if (code) goto _err;
  } else {
    code = tMapDataPutItem(&pCommitter->dWriter.mBlock, pBlock, tPutBlock);
    if (code) goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d, tsdb commit table disk data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

// static int32_t tsdbCommitTableDataEnd(SCommitter *pCommitter, int64_t suid, int64_t uid) {
//   int32_t    code = 0;
//   SBlockIdx  blockIdx = {.suid = suid, .uid = uid};
//   SBlockIdx *pBlockIdx = &blockIdx;

//   code = tsdbWriteBlock(pCommitter->dWriter.pWriter, &pCommitter->dWriter.mBlock, NULL, pBlockIdx);
//   if (code) goto _err;

//   if (taosArrayPush(pCommitter->dWriter.aBlockIdx, pBlockIdx) == NULL) {
//     code = TSDB_CODE_OUT_OF_MEMORY;
//     goto _err;
//   }

//   return code;

// _err:
//   tsdbError("vgId:%d, commit table data end failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
//   return code;
// }

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
  SBlockData *pBlockData = &pCommitter->dWriter.bData;
  SBlockIdx  *pBlockIdx = &(SBlockIdx){.suid = pIter->pTbData->suid, .uid = pIter->pTbData->uid};
  SBlock      block;
  TSDBROW    *pRow;

  code = tBlockDataSetSchema(pBlockData, pCommitter->skmTable.pTSchema);
  if (code) goto _err;

  pRow = tsdbTbDataIterGet(pIter);
  code = tsdbCommitterUpdateRowSchema(pCommitter, pBlockIdx->suid, pBlockIdx->uid, TSDBROW_SVERSION(pRow));
  if (code) goto _err;
  while (true) {
    if (pRow == NULL) break;
    code = tBlockDataAppendRow(pBlockData, pRow, pCommitter->skmRow.pTSchema);
    if (code) goto _err;

    tsdbTbDataIterNext(pIter);
    pRow = tsdbTbDataIterGet(pIter);
    if (pRow) {
      TSDBKEY key = TSDBROW_KEY(pRow);
      int32_t c = tBlockCmprFn(&(SBlock){.minKey = key, .maxKey = key}, pBlock);

      if (c == 0) {
        code =
            tsdbCommitterUpdateRowSchema(pCommitter, pIter->pTbData->suid, pIter->pTbData->uid, TSDBROW_SVERSION(pRow));
        if (code) goto _err;
      } else if (c > 0) {
        pRow = NULL;
      } else {
        ASSERT(0);
      }
    }
  }

  block = *pBlock;
  code = tsdbCommitBlockData(pCommitter, pBlockData, &block, pBlockIdx, 0);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d, tsdb merge as subblock failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbMergeCommitLast(SCommitter *pCommitter, STbDataIter *pIter, int32_t *nRow) {
  int32_t   code = 0;
  STbData  *pTbData = pIter->pTbData;
  TSDBROW  *pRow = tsdbTbDataIterGet(pIter);
  SRowInfo *pRowInfo = pCommitter->dReader.pRowInfo;

  while (pRow && pRowInfo) {
    int32_t c = tsdbRowCmprFn(pRow, &pRowInfo->row);
    if (c < 0) {
      code = tBlockDataAppendRow(&pCommitter->dWriter.bData, pRow, NULL);
      if (code) goto _err;

      tsdbTbDataIterNext(pIter);
      pRow = tsdbTbDataIterGet(pIter);
      if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) {
        pRow = NULL;
      }
    } else if (c > 0) {
      code = tBlockDataAppendRow(&pCommitter->dWriter.bData, &pRowInfo->row, NULL);
      if (code) goto _err;

      pCommitter->dReader.iRow++;
      if (pCommitter->dReader.iRow < pCommitter->dReader.bDatal.nRow) {
        // todo
      } else {
        pCommitter->dReader.iBlockL++;
        if (pCommitter->dReader.iBlockL < taosArrayGetSize(pCommitter->dReader.aBlockL)) {
          // todo
        } else {
          // todo
        }
      }

      pRowInfo = pCommitter->dReader.pRowInfo;
      if (pRowInfo && (pRowInfo->suid != pTbData->suid || pRowInfo->uid != pTbData->uid)) {
        pRowInfo = NULL;
      }
    } else {
      ASSERT(0);
    }

    (*nRow)--;
    ASSERT(*nRow >= 0);
    if (pCommitter->dWriter.bData.nRow >= pCommitter->maxRow * 4 / 5) goto _write_block_data;
  }

  while (pRow) {
    code = tBlockDataAppendRow(&pCommitter->dWriter.bData, pRow, NULL);
    if (code) goto _err;

    tsdbTbDataIterNext(pIter);
    pRow = tsdbTbDataIterGet(pIter);
    if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) {
      pRow = NULL;
    }

    (*nRow)--;
    ASSERT(*nRow >= 0);
    if (pCommitter->dWriter.bData.nRow >= pCommitter->maxRow * 4 / 5) goto _write_block_data;
  }

  while (pRowInfo) {
    code = tBlockDataAppendRow(&pCommitter->dWriter.bData, &pRowInfo->row, NULL);
    if (code) goto _err;

    pCommitter->dReader.iRow++;
    if (pCommitter->dReader.iRow < pCommitter->dReader.bDatal.nRow) {
      // todo
    } else {
      pCommitter->dReader.iBlockL++;
      if (pCommitter->dReader.iBlockL < taosArrayGetSize(pCommitter->dReader.aBlockL)) {
        // todo
      } else {
        // todo
      }
    }

    pRowInfo = pCommitter->dReader.pRowInfo;
    if (pRowInfo && (pRowInfo->suid != pTbData->suid || pRowInfo->uid != pTbData->uid)) {
      pRowInfo = NULL;
    }

    (*nRow)--;
    ASSERT(*nRow >= 0);
    if (pCommitter->dWriter.bData.nRow >= pCommitter->maxRow * 4 / 5) goto _write_block_data;
  }

  SBlock block;
_write_block_data:
  return code;

_err:
  return code;
}

static int32_t tsdbCommitTableData(SCommitter *pCommitter, STbData *pTbData) {
  int32_t code = 0;

  ASSERT(pCommitter->dReader.pBlockIdx == NULL || tTABLEIDCmprFn(pCommitter->dReader.pBlockIdx, pTbData) >= 0);
  ASSERT(pCommitter->dReader.pRowInfo == NULL || tTABLEIDCmprFn(pCommitter->dReader.pRowInfo, pTbData) >= 0);

  // end last if need
  if (pTbData->suid == 0 || pTbData->suid != 0 /*todo*/) {
    if (pCommitter->dWriter.bDatal.nRow > 0) {
      // TODO: code = tsdbCommitBlockDataL(pCommitter);
      if (code) goto _err;
    }
  }

  // merge commit table data
  STbDataIter  iter = {0};
  STbDataIter *pIter = &iter;
  TSDBROW     *pRow;

  tsdbTbDataIterOpen(pTbData, &(TSDBKEY){.ts = pCommitter->minKey, .version = VERSION_MIN}, 0, pIter);
  pRow = tsdbTbDataIterGet(pIter);
  if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) {
    pRow = NULL;
  }

  if (pRow == NULL) goto _exit;

  SBlockIdx *pBlockIdx = NULL;
  int32_t    iBlock = 0;
  SBlock     block;
  SBlock    *pBlock = &block;

  if (pCommitter->dReader.pBlockIdx && tTABLEIDCmprFn(pTbData, pCommitter->dReader.pBlockIdx) == 0) {
    pBlockIdx = pCommitter->dReader.pBlockIdx;
  }

  if (pBlockIdx && iBlock < pCommitter->dReader.mBlock.nItem) {
    tMapDataGetItemByIdx(&pCommitter->dReader.mBlock, iBlock, pBlock, tGetBlock);
  } else {
    pBlock = NULL;
  }

  tMapDataReset(&pCommitter->dWriter.mBlock);
  while (pBlock && pRow) {
    int32_t c = tBlockCmprFn(pBlock, &(SBlock){.minKey = TSDBROW_KEY(pRow), .maxKey = TSDBROW_KEY(pRow)});
    if (c < 0) {  // disk
      code = tMapDataPutItem(&pCommitter->dWriter.mBlock, pBlock, tPutBlock);
      if (code) goto _err;

      // next
      iBlock++;
      if (iBlock < pCommitter->dReader.mBlock.nItem) {
        tMapDataGetItemByIdx(&pCommitter->dReader.mBlock, iBlock, pBlock, tGetBlock);
      } else {
        pBlock = NULL;
      }
    } else if (c < 0) {  // memory
      code = tsdbCommitTableMemData(pCommitter, pIter, pBlock->minKey, 1);
      if (code) goto _err;

      // next
      pRow = tsdbTbDataIterGet(pIter);
      if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) {
        pRow = NULL;
      }
    } else {  // merge
      int32_t nOvlp = tsdbGetNumOfRowsLessThan(pIter, pBlock->maxKey);

      ASSERT(nOvlp > 0);

      if (pBlock->nRow + nOvlp <= pCommitter->maxRow && pBlock->nSubBlock < TSDB_MAX_SUBBLOCKS) {
        code = tsdbMergeAsSubBlock(pCommitter, pIter, pBlock);
        if (code) goto _err;
      } else {
        // code = tsdbMergeTableData(pCommitter, pIter, pBlock, NULL, 1);
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

  // merge with last
  int32_t nRowLeft = tsdbGetNumOfRowsLessThan(pIter, (TSDBKEY){.ts = pCommitter->maxKey + 1, .version = VERSION_MIN});
  if (pCommitter->dReader.pRowInfo) {
    for (int32_t iRow = pCommitter->dReader.iRow; iRow < pCommitter->dReader.bDatal.nRow; iRow++) {
      int64_t uid = pCommitter->dReader.bDatal.aUid[iRow];
      if (uid == pTbData->uid) {
        nRowLeft++;
      }
    }
  }

  while (nRowLeft) {
    code = tsdbMergeCommitLast(pCommitter, pIter, &nRowLeft);
    if (code) goto _err;
  }

  // end
  if (pCommitter->dWriter.mBlock.nItem > 0) {
    SBlockIdx blockIdx = {.suid = pTbData->suid, .uid = pTbData->uid};
    code = tsdbWriteBlock(pCommitter->dWriter.pWriter, &pCommitter->dWriter.mBlock, NULL, &blockIdx);
    if (code) goto _err;

    if (taosArrayPush(pCommitter->dWriter.aBlockIdx, &blockIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

_exit:
  pRow = tsdbTbDataIterGet(pIter);
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

  // write aBlockL
  code = tsdbWriteBlockL(pCommitter->dWriter.pWriter, pCommitter->dWriter.aBlockL, NULL);
  if (code) goto _err;

  // write aBlockIdx
  code = tsdbWriteBlockIdx(pCommitter->dWriter.pWriter, pCommitter->dWriter.aBlockIdx, NULL);
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

  // data
  while (true) {
    if (pCommitter->dReader.pBlockIdx == NULL || tTABLEIDCmprFn(pCommitter->dReader.pBlockIdx, &toTable) >= 0) break;

    SBlockIdx blockIdx = *pCommitter->dReader.pBlockIdx;
    code = tsdbWriteBlock(pCommitter->dWriter.pWriter, &pCommitter->dReader.mBlock, NULL, &blockIdx);
    if (code) goto _err;

    if (taosArrayPush(pCommitter->dWriter.aBlockIdx, &blockIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }

    pCommitter->dReader.iBlockIdx++;
    if (pCommitter->dReader.iBlockIdx < taosArrayGetSize(pCommitter->dReader.aBlockIdx)) {
      pCommitter->dReader.pBlockIdx =
          (SBlockIdx *)taosArrayGet(pCommitter->dReader.aBlockIdx, pCommitter->dReader.iBlockIdx);

      code =
          tsdbReadBlock(pCommitter->dReader.pReader, pCommitter->dReader.pBlockIdx, &pCommitter->dReader.mBlock, NULL);
      if (code) goto _err;
    } else {
      pCommitter->dReader.pBlockIdx = NULL;
    }
  }

  // last
  while (true) {
    if (pCommitter->dReader.pRowInfo == NULL || tTABLEIDCmprFn(pCommitter->dReader.pRowInfo, &toTable) >= 0) break;

    // commit if not same schema
    if (pCommitter->dWriter.bDatal.nRow > 0) {
      if (pCommitter->dWriter.bDatal.suid != pCommitter->dReader.pRowInfo->suid ||
          pCommitter->dWriter.bDatal.suid == 0) {
        code = tsdbCommitLastBlock(pCommitter);
        if (code) goto _err;
      }
    }

    if (pCommitter->dWriter.bDatal.nRow == 0) {
      code = tsdbCommitterUpdateTableSchema(pCommitter, pCommitter->dReader.pRowInfo->suid,
                                            pCommitter->dReader.pRowInfo->suid, 1 /*TODO*/);
      if (code) goto _err;

      pCommitter->dWriter.bDatal.suid = pCommitter->dReader.pRowInfo->suid;
      code = tBlockDataSetSchema(&pCommitter->dWriter.bDatal, pCommitter->skmTable.pTSchema);
      if (code) goto _err;
    }

    // check if it can make sure that one table data in one block
    int64_t uid = pCommitter->dReader.pRowInfo->uid;
    int32_t nRow = 0;
    for (int32_t iRow = pCommitter->dReader.iRow;
         (iRow < pCommitter->dReader.bDatal.nRow) && (pCommitter->dReader.bDatal.aUid[iRow] == uid); iRow++) {
      nRow++;
    }

    ASSERT(nRow > 0 && nRow < pCommitter->minRow);

    if (pCommitter->dWriter.bDatal.nRow + nRow > pCommitter->maxRow) {
      ASSERT(pCommitter->dWriter.bDatal.nRow > 0);

      code = tsdbCommitLastBlock(pCommitter);
      if (code) goto _err;
    }

    while (nRow > 0) {
      code = tBlockDataAppendRow(&pCommitter->dWriter.bDatal, &pCommitter->dReader.pRowInfo->row, NULL);
      if (code) goto _err;

      code = tsdbCommitNextLastRow(pCommitter);
      if (code) goto _err;

      nRow--;
    }
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
  for (int32_t iTbData = 0; iTbData < taosArrayGetSize(pMemTable->aTbData); iTbData++) {
    STbData *pTbData = (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData);

    // move commit until current (suid, uid)
    code = tsdbMoveCommitData(pCommitter, *(TABLEID *)pTbData);
    if (code) goto _err;

    // commit current table data
    code = tsdbCommitTableData(pCommitter, pTbData);
    if (code) goto _err;
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

  code = tBlockDataInit(&pCommitter->dReader.bData);
  if (code) goto _exit;

  pCommitter->dReader.aBlockL = taosArrayInit(0, sizeof(SBlockL));
  if (pCommitter->dReader.aBlockL == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  code = tBlockDataInit(&pCommitter->dReader.bDatal);
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

  code = tBlockDataInit(&pCommitter->dWriter.bData);
  if (code) goto _exit;

  code = tBlockDataInit(&pCommitter->dWriter.bDatal);
  if (code) goto _exit;

_exit:
  return code;
}

static void tsdbCommitDataEnd(SCommitter *pCommitter) {
  // Reader
  taosArrayDestroy(pCommitter->dReader.aBlockIdx);
  tMapDataClear(&pCommitter->dReader.mBlock);
  tBlockDataClear(&pCommitter->dReader.bData, 1);
  taosArrayDestroy(pCommitter->dReader.aBlockL);
  tBlockDataClear(&pCommitter->dReader.bDatal, 1);

  // Writer
  taosArrayDestroy(pCommitter->dWriter.aBlockIdx);
  taosArrayDestroy(pCommitter->dWriter.aBlockL);
  tMapDataClear(&pCommitter->dWriter.mBlock);
  tBlockDataClear(&pCommitter->dWriter.bData, 1);
  tBlockDataClear(&pCommitter->dWriter.bDatal, 1);
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
  tsdbDebug("vgId:%d, commit data done, nRow:%" PRId64, TD_VID(pTsdb->pVnode), pMemTable->nRow);
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
  int32_t  nTbData = taosArrayGetSize(pMemTable->aTbData);
  STbData *pTbData;
  SDelIdx *pDelIdx;

  ASSERT(nTbData > 0);

  pTbData = (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData);
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
    pTbData = (iTbData < nTbData) ? (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData) : NULL;
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
    pTbData = (iTbData < nTbData) ? (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData) : NULL;
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

  tsdbInfo("vgId:%d, tsdb end commit", TD_VID(pTsdb->pVnode));
  return code;

_err:
  tsdbError("vgId:%d, tsdb end commit failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}
