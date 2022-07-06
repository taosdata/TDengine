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
  /* commit data */
  int64_t commitID;
  int32_t minutes;
  int8_t  precision;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  // --------------
  TSKEY   nextKey;  // reset by each table commit
  int32_t commitFid;
  TSKEY   minKey;
  TSKEY   maxKey;
  // commit file data
  SDataFReader *pReader;
  SArray       *aBlockIdx;  // SArray<SBlockIdx>
  SMapData      oBlockMap;  // SMapData<SBlock>, read from reader
  SBlockData    oBlockData;
  SDataFWriter *pWriter;
  SArray       *aBlockIdxN;  // SArray<SBlockIdx>
  SMapData      nBlockMap;   // SMapData<SBlock>
  SBlockData    nBlockData;
  SSkmInfo      skmTable;
  SSkmInfo      skmRow;
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

  code = tsdbMemTableCreate(pTsdb, &pTsdb->mem);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d tsdb begin failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

int32_t tsdbCommit(STsdb *pTsdb) {
  if (!pTsdb) return 0;

  int32_t    code = 0;
  SCommitter commith;
  SMemTable *pMemTable = pTsdb->mem;

  // check
  if (pMemTable->nRow == 0 && pMemTable->nDel == 0) {
    // TODO: lock?
    pTsdb->mem = NULL;
    tsdbMemTableDestroy(pMemTable);
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

  code = tsdbCommitCache(&commith);
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

  SDelFile *pDelFileR = pTsdb->fs->nState->pDelFile;
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
  tsdbDebug("vgId:%d commit del start", TD_VID(pTsdb->pVnode));
  return code;

_err:
  tsdbError("vgId:%d commit del start failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableDel(SCommitter *pCommitter, STbData *pTbData, SDelIdx *pDelIdx) {
  int32_t   code = 0;
  SDelData *pDelData;
  tb_uid_t  suid;
  tb_uid_t  uid;

  taosArrayClear(pCommitter->aDelData);

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
  if (taosArrayPush(pCommitter->aDelIdx, &delIdx) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

_exit:
  return code;

_err:
  tsdbError("vgId:%d commit table del failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitDelEnd(SCommitter *pCommitter) {
  int32_t code = 0;
  STsdb  *pTsdb = pCommitter->pTsdb;

  code = tsdbWriteDelIdx(pCommitter->pDelFWriter, pCommitter->aDelIdxN, NULL);
  if (code) goto _err;

  code = tsdbUpdateDelFileHdr(pCommitter->pDelFWriter);
  if (code) goto _err;

  code = tsdbFSStateUpsertDelFile(pTsdb->fs->nState, &pCommitter->pDelFWriter->fDel);
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
  tsdbError("vgId:%d commit del end failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitFileDataStart(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SDFileSet *pRSet = NULL;
  SDFileSet  wSet;

  // memory
  pCommitter->nextKey = TSKEY_MAX;

  // old
  taosArrayClear(pCommitter->aBlockIdx);
  tMapDataReset(&pCommitter->oBlockMap);
  tBlockDataReset(&pCommitter->oBlockData);
  pRSet = tsdbFSStateGetDFileSet(pTsdb->fs->nState, pCommitter->commitFid);
  if (pRSet) {
    code = tsdbDataFReaderOpen(&pCommitter->pReader, pTsdb, pRSet);
    if (code) goto _err;

    code = tsdbReadBlockIdx(pCommitter->pReader, pCommitter->aBlockIdx, NULL);
    if (code) goto _err;
  }

  // new
  taosArrayClear(pCommitter->aBlockIdxN);
  tMapDataReset(&pCommitter->nBlockMap);
  tBlockDataReset(&pCommitter->nBlockData);
  if (pRSet) {
    wSet = (SDFileSet){.diskId = pRSet->diskId,
                       .fid = pCommitter->commitFid,
                       .fHead = {.commitID = pCommitter->commitID, .offset = 0, .size = 0},
                       .fData = pRSet->fData,
                       .fLast = {.commitID = pCommitter->commitID, .size = 0},
                       .fSma = pRSet->fSma};
  } else {
    STfs   *pTfs = pTsdb->pVnode->pTfs;
    SDiskID did = {.level = 0, .id = 0};

    // TODO: alloc a new disk
    // tfsAllocDisk(pTfs, 0, &did);

    // create the directory
    tfsMkdirRecurAt(pTfs, pTsdb->path, did);

    wSet = (SDFileSet){.diskId = did,
                       .fid = pCommitter->commitFid,
                       .fHead = {.commitID = pCommitter->commitID, .offset = 0, .size = 0},
                       .fData = {.commitID = pCommitter->commitID, .size = 0},
                       .fLast = {.commitID = pCommitter->commitID, .size = 0},
                       .fSma = {.commitID = pCommitter->commitID, .size = 0}};
  }
  code = tsdbDataFWriterOpen(&pCommitter->pWriter, pTsdb, &wSet);
  if (code) goto _err;

_exit:
  return code;

_err:
  tsdbError("vgId:%d commit file data start failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
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

  code = tsdbWriteBlockData(pCommitter->pWriter, pBlockData, NULL, NULL, pBlockIdx, pBlock, pCommitter->cmprAlg);
  if (code) goto _err;

  code = tMapDataPutItem(&pCommitter->nBlockMap, pBlock, tPutBlock);
  if (code) goto _err;

  return code;

_err:
  return code;
}

static int32_t tsdbMergeTableData(SCommitter *pCommitter, STbDataIter *pIter, SBlock *pBlockMerge, TSDBKEY toKey,
                                  int8_t toDataOnly) {
  int32_t     code = 0;
  SBlockIdx  *pBlockIdx = &(SBlockIdx){.suid = pIter->pTbData->suid, .uid = pIter->pTbData->uid};
  SBlockData *pBlockDataMerge = &pCommitter->oBlockData;
  SBlockData *pBlockData = &pCommitter->nBlockData;
  SBlock      block;
  SBlock     *pBlock = &block;
  TSDBROW    *pRow1;
  TSDBROW     row2;
  TSDBROW    *pRow2 = &row2;

  // read SBlockData
  code = tsdbReadBlockData(pCommitter->pReader, pBlockIdx, pBlockMerge, pBlockDataMerge, NULL, NULL);
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
  tsdbError("vgId:%d tsdb merge block and mem failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableMemData(SCommitter *pCommitter, STbDataIter *pIter, TSDBKEY toKey, int8_t toDataOnly) {
  int32_t     code = 0;
  TSDBROW    *pRow;
  SBlock      block;
  SBlock     *pBlock = &block;
  SBlockData *pBlockData = &pCommitter->nBlockData;
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
      TSDBKEY key = TSDBROW_KEY(pRow);
      if (tsdbKeyCmprFn(&TSDBROW_KEY(pRow), &toKey) >= 0) {
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

  return code;

_err:
  tsdbError("vgId:%d tsdb commit table mem data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableDiskData(SCommitter *pCommitter, SBlock *pBlock, SBlockIdx *pBlockIdx) {
  int32_t code = 0;
  SBlock  block;

  if (pBlock->last) {
    code = tsdbReadBlockData(pCommitter->pReader, pBlockIdx, pBlock, &pCommitter->oBlockData, NULL, NULL);
    if (code) goto _err;

    tBlockReset(&block);
    code = tsdbCommitBlockData(pCommitter, &pCommitter->oBlockData, &block, pBlockIdx, 0);
    if (code) goto _err;
  } else {
    code = tMapDataPutItem(&pCommitter->nBlockMap, pBlock, tPutBlock);
    if (code) goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb commit table disk data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableDataEnd(SCommitter *pCommitter, int64_t suid, int64_t uid) {
  int32_t    code = 0;
  SBlockIdx  blockIdx = {.suid = suid, .uid = uid};
  SBlockIdx *pBlockIdx = &blockIdx;

  code = tsdbWriteBlock(pCommitter->pWriter, &pCommitter->nBlockMap, NULL, pBlockIdx);
  if (code) goto _err;

  if (taosArrayPush(pCommitter->aBlockIdxN, pBlockIdx) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d commit table data end failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbGetOvlpNRow(STbDataIter *pIter, SBlock *pBlock) {
  int32_t     nRow = 0;
  TSDBROW    *pRow;
  TSDBKEY     key;
  int32_t     c = 0;
  STbDataIter iter = *pIter;

  iter.pRow = NULL;
  while (true) {
    pRow = tsdbTbDataIterGet(pIter);

    if (pRow == NULL) break;
    key = TSDBROW_KEY(pRow);

    c = tBlockCmprFn(&(SBlock){.maxKey = key, .minKey = key}, pBlock);
    if (c == 0) {
      nRow++;
      tsdbTbDataIterNext(pIter);
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
  SBlockData *pBlockData = &pCommitter->nBlockData;
  SBlockIdx  *pBlockIdx = &(SBlockIdx){.suid = pIter->pTbData->suid, .uid = pIter->pTbData->uid};
  SBlock      block;
  TSDBROW    *pRow;

  code = tBlockDataSetSchema(pBlockData, pCommitter->skmTable.pTSchema);
  if (code) goto _err;

  pRow = tsdbTbDataIterGet(pIter);
  code = tsdbCommitterUpdateRowSchema(pCommitter, pBlockIdx->suid, pBlockIdx->uid, TSDBROW_SVERSION(pRow));
  if (code) goto _err;
  while (true) {
    if (pRow) break;
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
  tsdbError("vgId:%d tsdb merge as subblock failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableData(SCommitter *pCommitter, STbData *pTbData, SBlockIdx *pBlockIdx) {
  int32_t      code = 0;
  STbDataIter  iter = {0};
  STbDataIter *pIter = &iter;
  TSDBROW     *pRow;
  int32_t      iBlock;
  int32_t      nBlock;
  int64_t      suid;
  int64_t      uid;

  if (pTbData) {
    tsdbTbDataIterOpen(pTbData, &(TSDBKEY){.ts = pCommitter->minKey, .version = VERSION_MIN}, 0, pIter);
    pRow = tsdbTbDataIterGet(pIter);
    if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) pRow = NULL;

    suid = pTbData->suid;
    uid = pTbData->uid;
  } else {
    pIter = NULL;
    pRow = NULL;
  }

  if (pBlockIdx) {
    code = tsdbReadBlock(pCommitter->pReader, pBlockIdx, &pCommitter->oBlockMap, NULL);
    if (code) goto _err;

    nBlock = pCommitter->oBlockMap.nItem;
    ASSERT(nBlock > 0);

    suid = pBlockIdx->suid;
    uid = pBlockIdx->uid;
  } else {
    nBlock = 0;
  }

  if (pRow == NULL && nBlock == 0) goto _exit;

  // start ===========
  tMapDataReset(&pCommitter->nBlockMap);
  SBlock  block;
  SBlock *pBlock = &block;

  iBlock = 0;
  if (iBlock < nBlock) {
    tMapDataGetItemByIdx(&pCommitter->oBlockMap, iBlock, pBlock, tGetBlock);
  } else {
    pBlock = NULL;
  }

  if (pRow) {
    code = tsdbCommitterUpdateTableSchema(pCommitter, pTbData->suid, pTbData->uid, pTbData->maxSkmVer);
    if (code) goto _err;
  }

  // merge ===========
  while (true) {
    if (pRow == NULL && pBlock == NULL) break;

    if (pRow && pBlock) {
      if (pBlock->last) {
        code = tsdbMergeTableData(pCommitter, pIter, pBlock,
                                  (TSDBKEY){.ts = pCommitter->maxKey + 1, .version = VERSION_MIN}, 0);
        if (code) goto _err;

        pRow = tsdbTbDataIterGet(pIter);
        if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) pRow = NULL;
        iBlock++;
        if (iBlock < nBlock) {
          tMapDataGetItemByIdx(&pCommitter->oBlockMap, iBlock, pBlock, tGetBlock);
        } else {
          pBlock = NULL;
        }

        ASSERT(pRow == NULL && pBlock == NULL);
      } else {
        int32_t c = tBlockCmprFn(&(SBlock){.maxKey = TSDBROW_KEY(pRow), .minKey = TSDBROW_KEY(pRow)}, pBlock);
        if (c > 0) {
          // only disk data
          code = tsdbCommitTableDiskData(pCommitter, pBlock, pBlockIdx);
          if (code) goto _err;

          iBlock++;
          if (iBlock < nBlock) {
            tMapDataGetItemByIdx(&pCommitter->oBlockMap, iBlock, pBlock, tGetBlock);
          } else {
            pBlock = NULL;
          }
        } else if (c < 0) {
          // only memory data
          code = tsdbCommitTableMemData(pCommitter, pIter, pBlock->minKey, 1);
          if (code) goto _err;

          pRow = tsdbTbDataIterGet(pIter);
          if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) pRow = NULL;
        } else {
          // merge memory and disk
          int32_t nOvlp = tsdbGetOvlpNRow(pIter, pBlock);
          ASSERT(nOvlp);
          if (pBlock->nRow + nOvlp <= pCommitter->maxRow && pBlock->nSubBlock < TSDB_MAX_SUBBLOCKS) {
            code = tsdbMergeAsSubBlock(pCommitter, pIter, pBlock);
            if (code) goto _err;
          } else {
            TSDBKEY toKey = {.ts = pCommitter->maxKey + 1, .version = VERSION_MIN};
            int8_t  toDataOnly = 0;

            if (iBlock < nBlock - 1) {
              toDataOnly = 1;

              SBlock nextBlock = {0};
              tBlockReset(&nextBlock);
              tMapDataGetItemByIdx(&pCommitter->oBlockMap, iBlock + 1, &nextBlock, tGetBlock);
              toKey = nextBlock.minKey;
            }

            code = tsdbMergeTableData(pCommitter, pIter, pBlock, toKey, toDataOnly);
            if (code) goto _err;
          }

          pRow = tsdbTbDataIterGet(pIter);
          if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) pRow = NULL;
          iBlock++;
          if (iBlock < nBlock) {
            tMapDataGetItemByIdx(&pCommitter->oBlockMap, iBlock, pBlock, tGetBlock);
          } else {
            pBlock = NULL;
          }
        }
      }
    } else if (pBlock) {
      code = tsdbCommitTableDiskData(pCommitter, pBlock, pBlockIdx);
      if (code) goto _err;

      iBlock++;
      if (iBlock < nBlock) {
        tMapDataGetItemByIdx(&pCommitter->oBlockMap, iBlock, pBlock, tGetBlock);
      } else {
        pBlock = NULL;
      }
    } else {
      code =
          tsdbCommitTableMemData(pCommitter, pIter, (TSDBKEY){.ts = pCommitter->maxKey + 1, .version = VERSION_MIN}, 0);
      if (code) goto _err;

      pRow = tsdbTbDataIterGet(pIter);
      if (pRow && TSDBROW_TS(pRow) > pCommitter->maxKey) pRow = NULL;
      ASSERT(pRow == NULL);
    }
  }

  // end =====================
  code = tsdbCommitTableDataEnd(pCommitter, suid, uid);
  if (code) goto _err;

_exit:
  if (pIter) {
    pRow = tsdbTbDataIterGet(pIter);
    if (pRow) pCommitter->nextKey = TMIN(pCommitter->nextKey, TSDBROW_TS(pRow));
  }
  return code;

_err:
  tsdbError("vgId:%d tsdb commit table data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitFileDataEnd(SCommitter *pCommitter) {
  int32_t code = 0;

  // write blockIdx
  code = tsdbWriteBlockIdx(pCommitter->pWriter, pCommitter->aBlockIdxN, NULL);
  if (code) goto _err;

  // update file header
  code = tsdbUpdateDFileSetHeader(pCommitter->pWriter);
  if (code) goto _err;

  // upsert SDFileSet
  code = tsdbFSStateUpsertDFileSet(pCommitter->pTsdb->fs->nState, tsdbDataFWriterGetWSet(pCommitter->pWriter));
  if (code) goto _err;

  // close and sync
  code = tsdbDataFWriterClose(&pCommitter->pWriter, 1);
  if (code) goto _err;

  if (pCommitter->pReader) {
    code = tsdbDataFReaderClose(&pCommitter->pReader);
    goto _err;
  }

_exit:
  return code;

_err:
  tsdbError("vgId:%d commit file data end failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
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
  int32_t    iTbData = 0;
  int32_t    nTbData = taosArrayGetSize(pMemTable->aTbData);
  int32_t    iBlockIdx = 0;
  int32_t    nBlockIdx = taosArrayGetSize(pCommitter->aBlockIdx);
  STbData   *pTbData;
  SBlockIdx *pBlockIdx;

  ASSERT(nTbData > 0);

  pTbData = (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData);
  pBlockIdx = (iBlockIdx < nBlockIdx) ? (SBlockIdx *)taosArrayGet(pCommitter->aBlockIdx, iBlockIdx) : NULL;
  while (pTbData || pBlockIdx) {
    if (pTbData && pBlockIdx) {
      int32_t c = tTABLEIDCmprFn(pTbData, pBlockIdx);

      if (c == 0) {
        goto _commit_table_mem_and_disk;
      } else if (c < 0) {
        goto _commit_table_mem_data;
      } else {
        goto _commit_table_disk_data;
      }
    } else if (pBlockIdx) {
      goto _commit_table_disk_data;
    } else {
      goto _commit_table_mem_data;
    }

  _commit_table_mem_data:
    code = tsdbCommitTableData(pCommitter, pTbData, NULL);
    if (code) goto _err;

    iTbData++;
    pTbData = (iTbData < nTbData) ? (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData) : NULL;
    continue;

  _commit_table_disk_data:
    code = tsdbCommitTableData(pCommitter, NULL, pBlockIdx);
    if (code) goto _err;

    iBlockIdx++;
    pBlockIdx = (iBlockIdx < nBlockIdx) ? (SBlockIdx *)taosArrayGet(pCommitter->aBlockIdx, iBlockIdx) : NULL;
    continue;

  _commit_table_mem_and_disk:
    code = tsdbCommitTableData(pCommitter, pTbData, pBlockIdx);
    if (code) goto _err;

    iBlockIdx++;
    pBlockIdx = (iBlockIdx < nBlockIdx) ? (SBlockIdx *)taosArrayGet(pCommitter->aBlockIdx, iBlockIdx) : NULL;
    iTbData++;
    pTbData = (iTbData < nTbData) ? (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData) : NULL;
    continue;
  }

  // commit file data end
  code = tsdbCommitFileDataEnd(pCommitter);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d commit file data failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  tsdbDataFReaderClose(&pCommitter->pReader);
  tsdbDataFWriterClose(&pCommitter->pWriter, 0);
  return code;
}

// ----------------------------------------------------------------------------
static int32_t tsdbStartCommit(STsdb *pTsdb, SCommitter *pCommitter) {
  int32_t code = 0;

  memset(pCommitter, 0, sizeof(*pCommitter));
  ASSERT(pTsdb->mem && pTsdb->imem == NULL);

  // lock();
  pTsdb->imem = pTsdb->mem;
  pTsdb->mem = NULL;
  // unlock();

  pCommitter->pTsdb = pTsdb;
  pCommitter->commitID = pTsdb->pVnode->state.commitID;
  pCommitter->minutes = pTsdb->keepCfg.days;
  pCommitter->precision = pTsdb->keepCfg.precision;
  pCommitter->minRow = pTsdb->pVnode->config.tsdbCfg.minRows;
  pCommitter->maxRow = pTsdb->pVnode->config.tsdbCfg.maxRows;
  pCommitter->cmprAlg = pTsdb->pVnode->config.tsdbCfg.compression;

  code = tsdbFSBegin(pTsdb->fs);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d tsdb start commit failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitDataStart(SCommitter *pCommitter) {
  int32_t code = 0;

  pCommitter->aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pCommitter->aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  pCommitter->aBlockIdxN = taosArrayInit(0, sizeof(SBlockIdx));
  if (pCommitter->aBlockIdxN == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  code = tBlockDataInit(&pCommitter->oBlockData);
  if (code) goto _exit;

  code = tBlockDataInit(&pCommitter->nBlockData);
  if (code) goto _exit;

_exit:
  return code;
}

static void tsdbCommitDataEnd(SCommitter *pCommitter) {
  taosArrayDestroy(pCommitter->aBlockIdx);
  tMapDataClear(&pCommitter->oBlockMap);
  tBlockDataClear(&pCommitter->oBlockData);
  taosArrayDestroy(pCommitter->aBlockIdxN);
  tMapDataClear(&pCommitter->nBlockMap);
  tBlockDataClear(&pCommitter->nBlockData);
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
    pCommitter->commitFid = tsdbKeyFid(pCommitter->nextKey, pCommitter->minutes, pCommitter->precision);
    tsdbFidKeyRange(pCommitter->commitFid, pCommitter->minutes, pCommitter->precision, &pCommitter->minKey,
                    &pCommitter->maxKey);
    code = tsdbCommitFileData(pCommitter);
    if (code) goto _err;
  }

  // end ====================
  tsdbCommitDataEnd(pCommitter);

_exit:
  tsdbDebug("vgId:%d commit data done, nRow:%" PRId64, TD_VID(pTsdb->pVnode), pMemTable->nRow);
  return code;

_err:
  tsdbCommitDataEnd(pCommitter);
  tsdbError("vgId:%d commit data failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
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
  tsdbDebug("vgId:%d commit del done, nDel:%" PRId64, TD_VID(pTsdb->pVnode), pMemTable->nDel);
  return code;

_err:
  tsdbError("vgId:%d commit del failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitCache(SCommitter *pCommitter) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t tsdbEndCommit(SCommitter *pCommitter, int32_t eno) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  if (eno == 0) {
    code = tsdbFSCommit(pTsdb->fs);
  } else {
    code = tsdbFSRollback(pTsdb->fs);
  }

  tsdbMemTableDestroy(pMemTable);
  pTsdb->imem = NULL;

  tsdbInfo("vgId:%d tsdb end commit", TD_VID(pTsdb->pVnode));
  return code;

_err:
  tsdbError("vgId:%d tsdb end commit failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}
