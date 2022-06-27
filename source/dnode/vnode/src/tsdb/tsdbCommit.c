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
  SMapData      oBlockIdxMap;  // SMapData<SBlockIdx>, read from reader
  SMapData      oBlockMap;     // SMapData<SBlock>, read from reader
  SBlock        oBlock;
  SBlockData    oBlockData;
  SDataFWriter *pWriter;
  SMapData      nBlockIdxMap;  // SMapData<SBlockIdx>, build by committer
  SMapData      nBlockMap;     // SMapData<SBlock>
  SBlock        nBlock;
  SBlockData    nBlockData;
  int64_t       suid;
  int64_t       uid;
  STSchema     *pTSchema;
  /* commit del */
  SDelFReader *pDelFReader;
  SMapData     oDelIdxMap;   // SMapData<SDelIdx>, old
  SMapData     oDelDataMap;  // SMapData<SDelData>, old
  SDelFWriter *pDelFWriter;
  SMapData     nDelIdxMap;   // SMapData<SDelIdx>, new
  SMapData     nDelDataMap;  // SMapData<SDelData>, new
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
  SDelFile  *pDelFileR = NULL;  // TODO
  SDelFile  *pDelFileW = NULL;  // TODO

  tMapDataReset(&pCommitter->oDelIdxMap);
  tMapDataReset(&pCommitter->nDelIdxMap);

  // load old
  if (pDelFileR) {
    code = tsdbDelFReaderOpen(&pCommitter->pDelFReader, pDelFileR, pTsdb, NULL);
    if (code) goto _err;

    code = tsdbReadDelIdx(pCommitter->pDelFReader, &pCommitter->oDelIdxMap, NULL);
    if (code) goto _err;
  }

  // prepare new
  code = tsdbDelFWriterOpen(&pCommitter->pDelFWriter, pDelFileW, pTsdb);
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
  SDelData *pDelData = &(SDelData){};
  tb_uid_t  suid;
  tb_uid_t  uid;
  SDelIdx   delIdx;  // TODO

  // check no del data, just return
  if (pTbData && pTbData->pHead == NULL) {
    pTbData = NULL;
  }
  if (pTbData == NULL && pDelIdx == NULL) goto _exit;

  // prepare
  if (pTbData) {
    delIdx.suid = pTbData->suid;
    delIdx.uid = pTbData->uid;
  } else {
    delIdx.suid = pDelIdx->suid;
    delIdx.uid = pDelIdx->uid;
  }
  delIdx.minKey = TSKEY_MAX;
  delIdx.maxKey = TSKEY_MIN;
  delIdx.minVersion = INT64_MAX;
  delIdx.maxVersion = INT64_MIN;

  // start
  tMapDataReset(&pCommitter->oDelDataMap);
  tMapDataReset(&pCommitter->nDelDataMap);

  if (pDelIdx) {
    code = tsdbReadDelData(pCommitter->pDelFReader, pDelIdx, &pCommitter->oDelDataMap, NULL);
    if (code) goto _err;
  }

  // disk
  for (int32_t iDelData = 0; iDelData < pCommitter->oDelDataMap.nItem; iDelData++) {
    code = tMapDataGetItemByIdx(&pCommitter->oDelDataMap, iDelData, pDelData, tGetDelData);
    if (code) goto _err;

    code = tMapDataPutItem(&pCommitter->nDelDataMap, pDelData, tPutDelData);
    if (code) goto _err;

    if (delIdx.minKey > pDelData->sKey) delIdx.minKey = pDelData->sKey;
    if (delIdx.maxKey < pDelData->eKey) delIdx.maxKey = pDelData->eKey;
    if (delIdx.minVersion > pDelData->version) delIdx.minVersion = pDelData->version;
    if (delIdx.maxVersion < pDelData->version) delIdx.maxVersion = pDelData->version;
  }

  // memory
  pDelData = pTbData ? pTbData->pHead : NULL;
  for (; pDelData; pDelData = pDelData->pNext) {
    code = tMapDataPutItem(&pCommitter->nDelDataMap, pDelData, tPutDelData);
    if (code) goto _err;

    if (delIdx.minKey > pDelData->sKey) delIdx.minKey = pDelData->sKey;
    if (delIdx.maxKey < pDelData->eKey) delIdx.maxKey = pDelData->eKey;
    if (delIdx.minVersion > pDelData->version) delIdx.minVersion = pDelData->version;
    if (delIdx.maxVersion < pDelData->version) delIdx.maxVersion = pDelData->version;
  }

  ASSERT(pCommitter->nDelDataMap.nItem > 0);

  // write
  code = tsdbWriteDelData(pCommitter->pDelFWriter, &pCommitter->nDelDataMap, NULL, &delIdx);
  if (code) goto _err;

  // put delIdx
  code = tMapDataPutItem(&pCommitter->nDelIdxMap, &delIdx, tPutDelIdx);
  if (code) goto _err;

_exit:
  return code;

_err:
  tsdbError("vgId:%d commit table del failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitDelImpl(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;
  int32_t    iDelIdx = 0;
  int32_t    nDelIdx = pCommitter->oDelIdxMap.nItem;
  int32_t    iTbData = 0;
  int32_t    nTbData = taosArrayGetSize(pMemTable->aTbData);
  STbData   *pTbData;
  SDelIdx   *pDelIdx;
  SDelIdx    delIdx;
  int32_t    c;

  ASSERT(nTbData > 0);

  pTbData = (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData);
  if (iDelIdx < nDelIdx) {
    code = tMapDataGetItemByIdx(&pCommitter->oDelIdxMap, iDelIdx, &delIdx, tGetDelIdx);
    if (code) goto _err;
    pDelIdx = &delIdx;
  } else {
    pDelIdx = NULL;
  }

  while (true) {
    if (pTbData == NULL && pDelIdx == NULL) break;

    if (pTbData && pDelIdx) {
      c = tTABLEIDCmprFn(pTbData, pDelIdx);
      if (c == 0) {
        goto _commit_mem_and_disk_del;
      } else if (c < 0) {
        goto _commit_mem_del;
      } else {
        goto _commit_disk_del;
      }
    } else {
      if (pTbData) goto _commit_mem_del;
      if (pDelIdx) goto _commit_disk_del;
    }

  _commit_mem_del:
    code = tsdbCommitTableDel(pCommitter, pTbData, NULL);
    if (code) goto _err;
    iTbData++;
    if (iTbData < nTbData) {
      pTbData = (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData);
    } else {
      pTbData = NULL;
    }
    continue;

  _commit_disk_del:
    code = tsdbCommitTableDel(pCommitter, NULL, pDelIdx);
    if (code) goto _err;
    iDelIdx++;
    if (iDelIdx < nDelIdx) {
      code = tMapDataGetItemByIdx(&pCommitter->oDelIdxMap, iDelIdx, &delIdx, tGetDelIdx);
      if (code) goto _err;
      pDelIdx = &delIdx;
    } else {
      pDelIdx = NULL;
    }
    continue;

  _commit_mem_and_disk_del:
    code = tsdbCommitTableDel(pCommitter, pTbData, pDelIdx);
    if (code) goto _err;
    iTbData++;
    iDelIdx++;
    if (iTbData < nTbData) {
      pTbData = (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData);
    } else {
      pTbData = NULL;
    }
    if (iDelIdx < nDelIdx) {
      code = tMapDataGetItemByIdx(&pCommitter->oDelIdxMap, iDelIdx, &delIdx, tGetDelIdx);
      if (code) goto _err;
      pDelIdx = &delIdx;
    } else {
      pDelIdx = NULL;
    }
    continue;
  }

  return code;

_err:
  tsdbError("vgId:%d commit del impl failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitDelEnd(SCommitter *pCommitter) {
  int32_t code = 0;

  code = tsdbWriteDelIdx(pCommitter->pDelFWriter, &pCommitter->nDelIdxMap, NULL);
  if (code) goto _err;

  code = tsdbUpdateDelFileHdr(pCommitter->pDelFWriter, NULL);
  if (code) goto _err;

  code = tsdbDelFWriterClose(pCommitter->pDelFWriter, 1);
  if (code) goto _err;

  if (pCommitter->pDelFReader) {
    code = tsdbDelFReaderClose(pCommitter->pDelFReader);
    if (code) goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d commit del end failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

// static int32_t tsdbGetOverlapRowNumber(STbDataIter *pIter, SBlock *pBlock) {
//   int32_t     nRow = 0;
//   TSDBROW    *pRow;
//   TSDBKEY     key;
//   int32_t     c = 0;
//   STbDataIter iter = *pIter;

//   iter.pRow = NULL;
//   while (true) {
//     pRow = tsdbTbDataIterGet(pIter);

//     if (pRow == NULL) break;
//     key = tsdbRowKey(pRow);

//     c = tBlockCmprFn(&(SBlock){.info.maxKey = key, .info.minKey = key}, pBlock);
//     if (c == 0) {
//       nRow++;
//     } else if (c > 0) {
//       break;
//     } else {
//       ASSERT(0);
//     }
//   }

//   return nRow;
// }

static int32_t tsdbCommitFileDataStart(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SDFileSet *pRSet = NULL;
  SDFileSet  wSet;

  // memory
  pCommitter->nextKey = TSKEY_MAX;

  // old
  tMapDataReset(&pCommitter->oBlockIdxMap);
  tMapDataReset(&pCommitter->oBlockMap);
  tBlockReset(&pCommitter->oBlock);
  tBlockDataReset(&pCommitter->oBlockData);
  pRSet = tsdbFSStateGetDFileSet(pTsdb->fs->nState, pCommitter->commitFid);
  if (pRSet) {
    code = tsdbDataFReaderOpen(&pCommitter->pReader, pTsdb, pRSet);
    if (code) goto _err;

    code = tsdbReadBlockIdx(pCommitter->pReader, &pCommitter->oBlockIdxMap, NULL);
    if (code) goto _err;
  }

  // new
  tMapDataReset(&pCommitter->nBlockIdxMap);
  tMapDataReset(&pCommitter->nBlockMap);
  tBlockReset(&pCommitter->nBlock);
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

static int32_t tsdbCommitterUpdateSchema(SCommitter *pCommitter, int64_t suid, int64_t uid, int32_t sver) {
  int32_t code = 0;

  if (pCommitter->pTSchema) {
    if (pCommitter->suid == suid) {
      if (suid == 0) {
        if (pCommitter->uid == uid && sver == pCommitter->pTSchema->version) goto _exit;
      } else {
        if (sver == pCommitter->pTSchema->version) goto _exit;
      }
    }
  }

_update_schema:
  pCommitter->suid = suid;
  pCommitter->uid = uid;
  tTSchemaDestroy(pCommitter->pTSchema);
  pCommitter->pTSchema = metaGetTbTSchema(pCommitter->pTsdb->pVnode->pMeta, uid, sver);
  if (pCommitter->pTSchema == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }

_exit:
  return code;
}

static int32_t tsdbCommitMemoryData(SCommitter *pCommitter, STbData *pTbData) {
  int32_t      code = 0;
  STsdb       *pTsdb = pCommitter->pTsdb;
  STbDataIter *pIter = &(STbDataIter){0};
  TSDBKEY      key = {.ts = pCommitter->minKey, .version = VERSION_MIN};
  TSDBROW      row;
  TSDBROW     *pRow;

  // create iter
  tsdbTbDataIterOpen(pTbData, &key, 0, pIter);
  pRow = tsdbTbDataIterGet(pIter);

  if (pRow == NULL || TSDBROW_TS(pRow) > pCommitter->maxKey) goto _exit;

  // main loop
  SBlockIdx  *pBlockIdx = &(SBlockIdx){.suid = pTbData->suid, .uid = pTbData->uid};
  SMapData   *mBlock = &pCommitter->nBlockMap;
  SBlock     *pBlock = &pCommitter->nBlock;
  SBlockData *pBlockData = &pCommitter->nBlockData;
  TSKEY       lastTS;

  tBlockIdxReset(pBlockIdx);
  tMapDataReset(mBlock);
  tBlockReset(pBlock);
  tBlockDataReset(pBlockData);
  lastTS = TSKEY_MIN;
  while (1) {
    if (pRow == NULL || TSDBROW_TS(pRow) > pCommitter->maxKey) {
      if (pBlockData->nRow > 0) {
        goto _write_block;
      } else {
        break;
      }
    }

    // update schema
    code = tsdbCommitterUpdateSchema(pCommitter, pTbData->suid, pTbData->uid, TSDBROW_SVERSION(pRow));
    if (code) goto _err;

    // append
    code = tBlockDataAppendRow(pBlockData, pRow, pCommitter->pTSchema);
    if (code) goto _err;

    // update
    pBlock->minVersion = TMIN(pBlock->minVersion, TSDBROW_VERSION(pRow));
    pBlock->maxVersion = TMAX(pBlock->maxVersion, TSDBROW_VERSION(pRow));
    pBlock->nRow++;
    if (TSDBROW_TS(pRow) == lastTS) pBlock->hasDup = 1;
    lastTS = TSDBROW_TS(pRow);

    // next
    tsdbTbDataIterNext(pIter);
    pRow = tsdbTbDataIterGet(pIter);

    // check
    if (pBlockData->nRow >= pCommitter->maxRow * 4 / 5) goto _write_block;
    continue;

  _write_block:
    row = tBlockDataFirstRow(pBlockData);
    if (tsdbKeyCmprFn(&pBlock->minKey, &TSDBROW_KEY(&row)) > 0) pBlock->minKey = TSDBROW_KEY(&row);
    row = tBlockDataLastRow(pBlockData);
    if (tsdbKeyCmprFn(&pBlock->maxKey, &TSDBROW_KEY(&row)) < 0) pBlock->maxKey = TSDBROW_KEY(&row);
    pBlock->last = pBlockData->nRow < pCommitter->minRow ? 1 : 0;
    code = tsdbWriteBlockData(pCommitter->pWriter, pBlockData, NULL, NULL, pBlockIdx, pBlock, pCommitter->cmprAlg);
    if (code) goto _err;

    // Design SMA and write SMA to file

    // SBlockIdx
    code = tMapDataPutItem(mBlock, pBlock, tPutBlock);
    if (code) goto _err;
    pBlockIdx->minKey = TMIN(pBlockIdx->minKey, pBlock->minKey.ts);
    pBlockIdx->maxKey = TMAX(pBlockIdx->maxKey, pBlock->maxKey.ts);
    pBlockIdx->minVersion = TMIN(pBlockIdx->minVersion, pBlock->minVersion);
    pBlockIdx->maxVersion = TMAX(pBlockIdx->maxVersion, pBlock->maxVersion);

    tBlockReset(pBlock);
    tBlockDataReset(pBlockData);
    lastTS = TSKEY_MIN;
  }

  // write block
  code = tsdbWriteBlock(pCommitter->pWriter, mBlock, NULL, pBlockIdx);
  if (code) goto _err;

  code = tMapDataPutItem(&pCommitter->nBlockIdxMap, pBlockIdx, tPutBlockIdx);
  if (code) goto _err;

_exit:
  if (pRow) pCommitter->nextKey = TMIN(pCommitter->nextKey, TSDBROW_TS(pRow));
  return code;

_err:
  tsdbError("vgId:%d tsdb commit memory data failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitDiskData(SCommitter *pCommitter, SBlockIdx *oBlockIdx) {
  int32_t     code = 0;
  SMapData   *mBlockO = &pCommitter->oBlockMap;
  SBlock     *pBlockO = &pCommitter->oBlock;
  SMapData   *mBlockN = &pCommitter->nBlockMap;
  SBlock     *pBlockN = &pCommitter->nBlock;
  SBlockIdx  *pBlockIdx = &(SBlockIdx){0};
  SBlockData *pBlockDataO = &pCommitter->oBlockData;

  // read
  code = tsdbReadBlock(pCommitter->pReader, oBlockIdx, mBlockO, NULL);
  if (code) goto _err;

  // loop to add to new
  tMapDataReset(mBlockN);
  for (int32_t iBlock = 0; iBlock < mBlockO->nItem; iBlock++) {
    tMapDataGetItemByIdx(mBlockO, iBlock, pBlockO, tGetBlock);

    if (pBlockO->last) {
      ASSERT(iBlock == mBlockO->nItem - 1);
      code = tsdbReadBlockData(pCommitter->pReader, oBlockIdx, pBlockO, pBlockDataO, NULL, NULL);
      if (code) goto _err;

      tBlockReset(pBlockN);
      pBlockN->minKey = pBlockO->minKey;
      pBlockN->maxKey = pBlockO->maxKey;
      pBlockN->minVersion = pBlockO->minVersion;
      pBlockN->maxVersion = pBlockO->maxVersion;
      pBlockN->nRow = pBlockO->nRow;
      pBlockN->last = pBlockO->last;
      pBlockN->hasDup = pBlockO->hasDup;
      code = tsdbWriteBlockData(pCommitter->pWriter, pBlockDataO, NULL, NULL, pBlockIdx, pBlockN, pCommitter->cmprAlg);
      if (code) goto _err;

      code = tMapDataPutItem(mBlockN, pBlockN, tPutBlock);
      if (code) goto _err;
    } else {
      code = tMapDataPutItem(mBlockN, pBlockO, tPutBlock);
      if (code) goto _err;
    }
  }

  // SBlock
  *pBlockIdx = *oBlockIdx;
  code = tsdbWriteBlock(pCommitter->pWriter, mBlockN, NULL, pBlockIdx);
  if (code) goto _err;

  // SBlockIdx
  code = tMapDataPutItem(&pCommitter->nBlockIdxMap, pBlockIdx, tPutBlockIdx);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d tsdb commit disk data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbMergeTableData(SCommitter *pCommitter, STbDataIter *pIter, SBlock *pBlockMerge, TSDBKEY toKey,
                                  int8_t toDataOnly) {
  int32_t     code = 0;
  SBlockIdx  *pBlockIdx = &(SBlockIdx){.suid = pIter->pTbData->suid, .uid = pIter->pTbData->uid};
  SBlockData *pBlockDataMerge = &pCommitter->oBlockData;
  SBlockData *pBlockData = &pCommitter->nBlockData;
  SBlock     *pBlock = &pCommitter->nBlock;
  TSDBROW    *pRow1;
  TSDBROW     row2;
  TSDBROW    *pRow2 = &row2;
  TSDBROW     row;
  TSDBROW    *pRow = &row;
  int32_t     c = 0;
  TSKEY       lastKey;

  // read SBlockData
  code = tsdbReadBlockData(pCommitter->pReader, pBlockIdx, pBlockMerge, pBlockDataMerge, NULL, NULL);
  if (code) goto _err;

  // loop to merge
  pRow1 = tsdbTbDataIterGet(pIter);
  *pRow2 = tsdbRowFromBlockData(pBlockDataMerge, 0);
  ASSERT(tsdbKeyCmprFn(&TSDBROW_KEY(pRow1), &toKey) < 0);
  ASSERT(tsdbKeyCmprFn(&TSDBROW_KEY(pRow2), &toKey) < 0);
  code = tsdbCommitterUpdateSchema(pCommitter, pBlockIdx->suid, pBlockIdx->uid, TSDBROW_SVERSION(pRow1));
  if (code) goto _err;

  lastKey = TSKEY_MIN;
  tBlockReset(pBlock);
  tBlockDataReset(pBlockData);
  while (true) {
    if (pRow1 == NULL && pRow2 == NULL) {
      if (pBlockData->nRow == 0) {
        break;
      } else {
        goto _write_block;
      }
    }

    if (pRow1 && pRow2) {
      if (tsdbRowCmprFn(pRow1, pRow2) < 0) {
        *pRow = *pRow1;

        tsdbTbDataIterNext(pIter);
        pRow1 = tsdbTbDataIterGet(pIter);

        if (pRow1) {
          if (tsdbKeyCmprFn(&TSDBROW_KEY(pRow1), &toKey) < 0) {
            code = tsdbCommitterUpdateSchema(pCommitter, pBlockIdx->suid, pBlockIdx->uid, TSDBROW_VERSION(pRow1));
            if (code) goto _err;
          } else {
            pRow1 = NULL;
          }
        }
      } else if (tsdbRowCmprFn(pRow1, pRow2) < 0) {
        *pRow = *pRow2;

        if (pRow2->iRow + 1 < pBlockDataMerge->nRow) {
          *pRow2 = tsdbRowFromBlockData(pBlockDataMerge, pRow2->iRow + 1);
        } else {
          pRow2 = NULL;
        }
      } else {
        ASSERT(0);
      }
    } else if (pRow1) {
      *pRow = *pRow1;

      tsdbTbDataIterNext(pIter);
      pRow1 = tsdbTbDataIterGet(pIter);
      if (pRow1) {
        if (tsdbKeyCmprFn(&TSDBROW_KEY(pRow1), &toKey) < 0) {
          code = tsdbCommitterUpdateSchema(pCommitter, pBlockIdx->suid, pBlockIdx->uid, TSDBROW_VERSION(pRow1));
          if (code) goto _err;
        } else {
          pRow1 = NULL;
        }
      }
    } else {
      *pRow = *pRow2;

      if (pRow2->iRow + 1 < pBlockDataMerge->nRow) {
        *pRow2 = tsdbRowFromBlockData(pBlockDataMerge, pRow2->iRow + 1);
      } else {
        pRow2 = NULL;
      }
    }

    code = tBlockDataAppendRow(pBlockData, &row, pCommitter->pTSchema);
    if (code) goto _err;

    pBlock->minVersion = TMIN(pBlock->minVersion, TSDBROW_VERSION(pRow));
    pBlock->maxVersion = TMAX(pBlock->maxVersion, TSDBROW_VERSION(pRow));
    pBlock->nRow++;
    if (lastKey == TSDBROW_TS(pRow)) {
      pBlock->hasDup = 1;
    } else {
      lastKey = TSDBROW_TS(pRow);
    }

    if (pBlockData->nRow >= pCommitter->maxRow * 4 / 5) goto _write_block;
    continue;

  _write_block:
    if (!toDataOnly && pBlockData->nRow < pCommitter->minRow) {
      pBlock->last = 1;
    } else {
      pBlock->last = 0;
    }

    code = tsdbWriteBlockData(pCommitter->pWriter, pBlockData, NULL, NULL, pBlockIdx, pBlock, pCommitter->cmprAlg);
    if (code) goto _err;

    code = tMapDataPutItem(&pCommitter->nBlockMap, pBlock, tPutBlock);
    if (code) goto _err;

    lastKey = TSKEY_MIN;
    tBlockReset(pBlock);
    tBlockDataReset(pBlockData);
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb merge block and mem failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableMemData(SCommitter *pCommitter, STbDataIter *pIter, TSDBKEY toKey, int8_t toDataOnly) {
  int32_t     code = 0;
  TSDBROW    *pRow;
  SBlock     *pBlock = &pCommitter->nBlock;
  SBlockData *pBlockData = &pCommitter->nBlockData;
  TSKEY       lastKey = TSKEY_MIN;
  int64_t     suid = pIter->pTbData->suid;
  int64_t     uid = pIter->pTbData->uid;

  tBlockReset(pBlock);
  tBlockDataReset(pBlockData);
  pRow = tsdbTbDataIterGet(pIter);
  while (true) {
    if (pRow == NULL || tsdbKeyCmprFn(&TSDBROW_KEY(pRow), &toKey) >= 0) {
      if (pBlockData->nRow > 0) {
        goto _write_block;
      } else {
        break;
      }
    }

    // update schema
    code = tsdbCommitterUpdateSchema(pCommitter, pIter->pTbData->suid, pIter->pTbData->uid, TSDBROW_SVERSION(pRow));
    if (code) goto _err;

    // append
    code = tBlockDataAppendRow(pBlockData, pRow, pCommitter->pTSchema);
    if (code) goto _err;

    // update
    pBlock->minVersion = TMIN(pBlock->minVersion, TSDBROW_VERSION(pRow));
    pBlock->maxVersion = TMIN(pBlock->maxVersion, TSDBROW_VERSION(pRow));
    pBlock->nRow++;
    if (TSDBROW_TS(pRow) == lastKey) {
      pBlock->hasDup = 1;
    } else {
      lastKey = TSDBROW_TS(pRow);
    }

    tsdbTbDataIterNext(pIter);
    pRow = tsdbTbDataIterGet(pIter);

    if (pBlockData->nRow >= pCommitter->maxRow * 4 / 5) goto _write_block;
    continue;

  _write_block:
    if (!toDataOnly && pBlockData->nRow < pCommitter->minKey) {
      pBlock->last = 1;
    } else {
      pBlock->last = 0;
    }

    code = tsdbWriteBlockData(pCommitter->pWriter, pBlockData, NULL, NULL, &(SBlockIdx){.suid = suid, .uid = uid},
                              pBlock, pCommitter->cmprAlg);
    if (code) goto _err;

    code = tMapDataPutItem(&pCommitter->nBlockMap, pBlock, tPutBlock);
    if (code) goto _err;

    tBlockReset(pBlock);
    tBlockDataReset(pBlockData);
    lastKey = TSKEY_MIN;
  }

  return code;

_err:
  tsdbError("vgId:%d tsdb commit table mem data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableDiskData(SCommitter *pCommitter, SBlock *pBlock, SBlockIdx *pBlockIdx) {
  int32_t code = 0;

  if (pBlock->last) {
    code = tsdbReadBlockData(pCommitter->pReader, pBlockIdx, pBlock, &pCommitter->oBlockData, NULL, NULL);
    if (code) goto _err;

    tBlockReset(&pCommitter->nBlock);
    pCommitter->nBlock.minKey = pBlock->minKey;
    pCommitter->nBlock.maxKey = pBlock->maxKey;
    pCommitter->nBlock.minVersion = pBlock->minVersion;
    pCommitter->nBlock.nRow = pBlock->nRow;
    pCommitter->nBlock.last = pBlock->last;
    pCommitter->nBlock.hasDup = pBlock->hasDup;
    code = tsdbWriteBlockData(pCommitter->pWriter, &pCommitter->oBlockData, NULL, NULL, pBlockIdx, &pCommitter->nBlock,
                              pCommitter->cmprAlg);
    if (code) goto _err;

    code = tMapDataPutItem(&pCommitter->nBlockMap, &pCommitter->nBlock, tPutBlock);
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

static int32_t tsdbMergeMemDisk(SCommitter *pCommitter, STbData *pTbData, SBlockIdx *oBlockIdx) {
  int32_t code = 0;
  //   STbDataIter *pIter = &(STbDataIter){0};
  //   TSDBROW     *pRow;

  //   // create iter
  //   tsdbTbDataIterOpen(pTbData, &(TSDBKEY){.ts = pCommitter->minKey, .version = VERSION_MIN}, 0, pIter);
  //   pRow == tsdbTbDataIterGet(pIter);
  //   if (pRow == NULL || TSDBROW_TS(pRow) > pCommitter->maxKey) {
  //     code = tsdbCommitDiskData(pCommitter, oBlockIdx);
  //     if (code) {
  //       goto _err;
  //     } else {
  //       goto _exit;
  //     }
  //   }

  //   // start ==================
  //   // read
  //   code = tsdbReadBlock(pCommitter->pReader, oBlockIdx, &pCommitter->oBlockMap, NULL);
  //   if (code) goto _err;

  //   // loop to merge
  //   // SBlockData *pBlockData = &pCommitter->nBlockData;
  //   int32_t iBlock = 0;
  //   int32_t nBlock = pCommitter->oBlockMap.nItem;
  //   // SBlock     *pBlockO = &pCommitter->oBlock;
  //   SBlock *pBlock;
  //   int32_t c;

  //   // merge ===================
  //   while (true) {
  //     if ((pRow == NULL || TSDBROW_TS(pRow) > pCommitter->maxKey) && pBlock == NULL) break;

  //     if ((pRow && TSDBROW_TS(pRow) <= pCommitter->maxKey) && pBlock) {
  //       if (pBlock->last) {
  //         // merge memory data and disk data to write to .data/.last (todo)
  //         code = tsdbMergeTableData(pCommitter, pIter, oBlockIdx, pBlock,
  //                                   (TSDBKEY){.ts = pCommitter->maxKey + 1, .version = VERSION_MIN}, 0);
  //         if (code) goto _err;

  //         pRow = tsdbTbDataIterGet(pIter);
  //         iBlock++;
  //       } else {
  //         c = tBlockCmprFn(&(SBlock){}, pBlock);

  //         if (c < 0) {
  //           // commit memory data until pBlock->minKey (not included) only to .data file (todo)
  //           code = tsdbCommitTableMemData(pCommitter, pIter, pBlock->minKey, 1);
  //           if (code) goto _err;

  //           pRow = tsdbTbDataIterGet(pIter);
  //         } else if (c > 0) {
  //           // just move the block (todo)
  //           // code = tsdbCommitTableDiskData(pCommitter, pBlock);
  //           if (code) goto _err;

  //           iBlock++;
  //           // TODO
  //         } else {
  //           int64_t nOvlp = 0;  // = tsdbOvlpRows();
  //           if (nOvlp + pBlock->nRow <= pCommitter->maxRow) {
  //             // add as a subblock
  //           } else {
  //             if (iBlock == nBlock - 1) {
  //               // merge memory data and disk data to .data/.last file
  //               code = tsdbMergeTableData(pCommitter, pIter, oBlockIdx, pBlock,
  //                                         (TSDBKEY){.ts = pCommitter->maxKey + 1, .version = VERSION_MIN}, 0);
  //               if (code) goto _err;
  //             } else {
  //               // merge memory data and disk data to .data file only until pBlock[1].
  //               code = tsdbMergeTableData(pCommitter, pIter, oBlockIdx, pBlock, (TSDBKEY){0} /*TODO*/, 1);
  //             }
  //           }

  //           pRow = tsdbTbDataIterGet(pIter);
  //           iBlock++;
  //         }
  //       }
  //     } else if (pBlock) {
  //       // code = tsdbCommitTableDiskData(pCommitter, pBlock);
  //       if (code) goto _err;

  //       iBlock++;
  //       // next block
  //     } else {
  //       // commit only memory data until (pCommitter->maxKey, VERSION_MAX)
  //       code =
  //           tsdbCommitTableMemData(pCommitter, pIter, (TSDBKEY){.ts = pCommitter->maxKey + 1, .version =
  //           VERSION_MIN}, 0);
  //       if (code) goto _err;

  //       pRow = tsdbTbDataIterGet(pIter);
  //     }
  //   }

  //   // end =====================
  //   // SBlock
  //   // code = tsdbWriteBlock(pCommitter->pWriter, &pCommitter->nBlockMap, NULL, pBlockIdx);
  //   // if (code) goto _err;

  //   // // SBlockIdx
  //   // code = tMapDataPutItem(&pCommitter->nBlockIdxMap, pBlockIdx, tPutBlockIdx);
  //   // if (code) goto _err;

  // _exit:
  //   pRow = tsdbTbDataIterGet(pIter);
  //   if (pRow) {
  //     pCommitter->nextKey = TMIN(pCommitter->nextKey, TSDBROW_TS(pRow));
  //   }
  return code;

  // _err:
  //   tsdbError("vgId:%d tsdb merge mem disk data failed since %s", TD_VID(pCommitter->pTsdb->pVnode),
  //   tstrerror(code)); return code;
}

static int32_t tsdbCommitTableDataEnd(SCommitter *pCommitter, int64_t suid, int64_t uid) {
  int32_t    code = 0;
  SBlockIdx *pBlockIdx = &(SBlockIdx){.suid = suid, .uid = uid};

  code = tsdbWriteBlock(pCommitter->pWriter, &pCommitter->nBlockMap, NULL, pBlockIdx);
  if (code) goto _err;

  code = tMapDataPutItem(&pCommitter->nBlockIdxMap, pBlockIdx, tPutBlockIdx);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d commit table data end failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableData(SCommitter *pCommitter, STbData *pTbData, SBlockIdx *pBlockIdx) {
  int32_t      code = 0;
  STbDataIter *pIter = &(STbDataIter){0};
  TSDBROW     *pRow;
  int32_t      iBlock = 0;
  int32_t      nBlock;
  int64_t      suid;
  int64_t      uid;

  if (pTbData) {
    tsdbTbDataIterOpen(pTbData, &(TSDBKEY){.ts = pCommitter->minKey, .version = VERSION_MIN}, 0, pIter);
    pRow = tsdbTbDataIterGet(pIter);
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

  if ((pRow == NULL || TSDBROW_TS(pRow) > pCommitter->maxKey) && nBlock == 0) goto _exit;

  // start ===========
  tMapDataReset(&pCommitter->nBlockMap);
  SBlock *pBlock = &pCommitter->oBlock;

  if (iBlock < nBlock) {
    tMapDataGetItemByIdx(&pCommitter->oBlockMap, iBlock, pBlock, tGetBlock);
  } else {
    pBlock = NULL;
  }

  // merge ===========
  while (true) {
    if (((pRow == NULL) || TSDBROW_TS(pRow) > pCommitter->maxKey) && pBlock == NULL) break;

    if (pRow && TSDBROW_TS(pRow) <= pCommitter->maxKey && pBlock) {
      if (pBlock->last) {
        code = tsdbMergeTableData(pCommitter, pIter, pBlock,
                                  (TSDBKEY){.ts = pCommitter->maxKey + 1, .version = VERSION_MIN}, 0);
        if (code) goto _err;

        pRow = tsdbTbDataIterGet(pIter);
        iBlock++;
        if (iBlock < nBlock) {
          tMapDataGetItemByIdx(&pCommitter->oBlockMap, iBlock, pBlock, tGetBlock);
        } else {
          pBlock = NULL;
        }
      } else {
        int32_t c = tBlockCmprFn(&(SBlock){.maxKey = TSDBROW_KEY(pRow), .minKey = TSDBROW_KEY(pRow)}, pBlock);
        if (c > 0) {
          code = tsdbCommitTableDiskData(pCommitter, pBlock, pBlockIdx);
          if (code) goto _err;

          iBlock++;
          if (iBlock < nBlock) {
            tMapDataGetItemByIdx(&pCommitter->oBlockMap, iBlock, pBlock, tGetBlock);
          } else {
            pBlock = NULL;
          }
        } else if (c < 0) {
          code = tsdbCommitTableMemData(pCommitter, pIter, pBlock->minKey, 1);
          if (code) goto _err;

          pRow = tsdbTbDataIterGet(pIter);
        } else {
          int64_t nOvlp = 0;  // (todo)
          if (pBlock->nRow + nOvlp <= pCommitter->maxRow && pBlock->nSubBlock < TSDB_MAX_SUBBLOCKS) {
            // add as a subblock
          } else {
            if (iBlock == nBlock - 1) {
              code = tsdbMergeTableData(pCommitter, pIter, pBlock,
                                        (TSDBKEY){.ts = pCommitter->maxKey + 1, .version = VERSION_MIN}, 0);

              if (code) goto _err;
            } else {
              // code = tsdbMergeTableData(pCommitter, pIter, pBlock, pBlock[1].minKey, 1);
              if (code) goto _err;
            }
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
      ASSERT(pRow == NULL || TSDBROW_TS(pRow) > pCommitter->maxKey);
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

static int32_t tsdbCommitFileDataImpl(SCommitter *pCommitter) {
  int32_t    code = 0;
  int32_t    c;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;
  int32_t    iTbData = 0;
  int32_t    nTbData = taosArrayGetSize(pMemTable->aTbData);
  int32_t    iBlockIdx = 0;
  int32_t    nBlockIdx = pCommitter->oBlockIdxMap.nItem;
  STbData   *pTbData;
  SBlockIdx *pBlockIdx = &(SBlockIdx){0};

  ASSERT(nTbData > 0);

  pTbData = (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData);
  if (iBlockIdx < nBlockIdx) {
    tMapDataGetItemByIdx(&pCommitter->oBlockIdxMap, iBlockIdx, pBlockIdx, tGetBlockIdx);
  } else {
    pBlockIdx = NULL;
  }

  // merge
  while (pTbData && pBlockIdx) {
    c = tTABLEIDCmprFn(pTbData, pBlockIdx);

    if (c == 0) {
      // merge commit
      code = tsdbMergeMemDisk(pCommitter, pTbData, pBlockIdx);
      if (code) goto _err;

      iTbData++;
      iBlockIdx++;
      if (iTbData < nTbData) {
        pTbData = (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData);
      } else {
        pTbData = NULL;
      }
      if (iBlockIdx < nBlockIdx) {
        tMapDataGetItemByIdx(&pCommitter->oBlockIdxMap, iBlockIdx, pBlockIdx, tGetBlockIdx);
      } else {
        pBlockIdx = NULL;
      }
    } else if (c < 0) {
      // commit memory data
      code = tsdbCommitMemoryData(pCommitter, pTbData);
      if (code) goto _err;

      iTbData++;
      if (iTbData < nTbData) {
        pTbData = (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData);
      } else {
        pTbData = NULL;
      }
    } else {
      // commit disk data
      code = tsdbCommitDiskData(pCommitter, pBlockIdx);
      if (code) goto _err;

      iBlockIdx++;
      if (iBlockIdx < nBlockIdx) {
        tMapDataGetItemByIdx(&pCommitter->oBlockIdxMap, iBlockIdx, pBlockIdx, tGetBlockIdx);
      } else {
        pBlockIdx = NULL;
      }
    }
  }

  // disk
  while (pBlockIdx) {
    // commit disk data
    code = tsdbCommitDiskData(pCommitter, pBlockIdx);
    if (code) goto _err;

    iBlockIdx++;
    if (iBlockIdx < nBlockIdx) {
      tMapDataGetItemByIdx(&pCommitter->oBlockIdxMap, iBlockIdx, pBlockIdx, tGetBlockIdx);
    } else {
      pBlockIdx = NULL;
    }
  }

  // memory
  while (pTbData) {
    // commit memory data
    code = tsdbCommitMemoryData(pCommitter, pTbData);
    if (code) goto _err;

    iTbData++;
    if (iTbData < nTbData) {
      pTbData = (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData);
    } else {
      pTbData = NULL;
    }
  }

  return code;

_err:
  tsdbError("vgId:%d commit file data impl failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitFileDataEnd(SCommitter *pCommitter) {
  int32_t code = 0;

  // write blockIdx
  code = tsdbWriteBlockIdx(pCommitter->pWriter, &pCommitter->nBlockIdxMap, NULL);
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
  int32_t code = 0;

  // commit file data start
  code = tsdbCommitFileDataStart(pCommitter);
  if (code) goto _err;

  // commit file data impl
  code = tsdbCommitFileDataImpl(pCommitter);
  if (code) goto _err;

  // commit file data end
  code = tsdbCommitFileDataEnd(pCommitter);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d commit file data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
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

  pCommitter->pReader = NULL;
  pCommitter->oBlockIdxMap = tMapDataInit();
  pCommitter->oBlockMap = tMapDataInit();
  pCommitter->oBlock = tBlockInit();
  pCommitter->pWriter = NULL;
  pCommitter->nBlockIdxMap = tMapDataInit();
  pCommitter->nBlockMap = tMapDataInit();
  pCommitter->nBlock = tBlockInit();
  code = tBlockDataInit(&pCommitter->oBlockData);
  if (code) goto _exit;
  code = tBlockDataInit(&pCommitter->nBlockData);
  if (code) {
    tBlockDataClear(&pCommitter->oBlockData);
    goto _exit;
  }

_exit:
  return code;
}

static void tsdbCommitDataEnd(SCommitter *pCommitter) {
  // tMapDataClear(&pCommitter->oBlockIdxMap);
  // tMapDataClear(&pCommitter->oBlockMap);
  // tBlockClear(&pCommitter->oBlock);
  // tBlockDataClear(&pCommitter->oBlockData);
  // tMapDataClear(&pCommitter->nBlockIdxMap);
  // tMapDataClear(&pCommitter->nBlockMap);
  // tBlockClear(&pCommitter->nBlock);
  // tBlockDataClear(&pCommitter->nBlockData);
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
  code = tsdbCommitDelImpl(pCommitter);
  if (code) {
    goto _err;
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
