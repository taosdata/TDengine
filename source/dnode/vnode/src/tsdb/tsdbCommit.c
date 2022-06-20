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
  int32_t minutes;
  int8_t  precision;
  int32_t minRow;
  int32_t maxRow;
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
  SDelData *pDelData;
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

#define ROW_END(pRow, maxKey) (((pRow) == NULL) || ((pRow)->pTSRow->ts > (maxKey)))

static int32_t tsdbCommitMemoryData(SCommitter *pCommitter, SBlockIdx *pBlockIdx, STbDataIter *pIter, TSDBKEY eKey,
                                    bool toDataOnly) {
  int32_t  code = 0;
  TSDBROW *pRow;
  SBlock   block = tBlockInit();

  while (true) {
    pRow = tsdbTbDataIterGet(pIter);

    if (pRow == NULL || tsdbKeyCmprFn(&(TSDBKEY){.ts = pRow->pTSRow->ts, .version = pRow->version}, &eKey) > 0) {
      if (pCommitter->nBlockData.nRow == 0) {
        break;
      } else {
        goto _write_block_data;
      }
    }

    code = tBlockDataAppendRow(&pCommitter->nBlockData, pRow, NULL /*TODO*/);
    if (code) goto _err;

    if (pCommitter->nBlockData.nRow < pCommitter->maxRow * 4 / 5) {
      continue;
    }

  _write_block_data:
    if (!toDataOnly && pCommitter->nBlockData.nRow < pCommitter->minKey) {
      block.last = 1;
    } else {
      block.last = 0;
    }

    code = tsdbWriteBlockData(pCommitter->pWriter, &pCommitter->nBlockData, NULL, NULL, pBlockIdx, &block);
    if (code) goto _err;

    code = tMapDataPutItem(&pCommitter->nBlockMap, &block, tPutBlock);
    if (code) goto _err;

    tBlockReset(&block);
    tBlockDataReset(&pCommitter->nBlockData);
  }

  return code;

_err:
  tsdbError("vgId:%d commit memory data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbGetOverlapRowNumber(STbDataIter *pIter, SBlock *pBlock) {
  int32_t     nRow = 0;
  TSDBROW    *pRow;
  TSDBKEY     key;
  int32_t     c = 0;
  STbDataIter iter = *pIter;

  iter.pRow = NULL;
  while (true) {
    pRow = tsdbTbDataIterGet(pIter);

    if (pRow == NULL) break;
    key = tsdbRowKey(pRow);

    c = tBlockCmprFn(&(SBlock){.info.maxKey = key, .info.minKey = key}, pBlock);
    if (c == 0) {
      nRow++;
    } else if (c > 0) {
      break;
    } else {
      ASSERT(0);
    }
  }

  return nRow;
}

static int32_t tsdbMergeCommitImpl(SCommitter *pCommitter, SBlockIdx *pBlockIdx, STbDataIter *pIter, SBlock *pBlock,
                                   int8_t toDataOnly) {
  int32_t  code = 0;
  int32_t  iRow = 0;
  int32_t  nRow = 0;
  int32_t  c;
  TSDBROW *pRow;
  SBlock   block = tBlockInit();
  TSDBKEY  key1;
  TSDBKEY  key2;

  tBlockDataReset(&pCommitter->nBlockData);

  // load last and merge until {pCommitter->maxKey, INT64_MAX}
  code = tsdbReadBlockData(pCommitter->pReader, pBlockIdx, pBlock, &pCommitter->oBlockData, NULL, 0, NULL, NULL);
  if (code) goto _err;

  iRow = 0;
  nRow = pCommitter->oBlockData.nRow;
  pRow = tsdbTbDataIterGet(pIter);

  while (true) {
    if ((pRow == NULL || pRow->pTSRow->ts > pCommitter->maxKey) && (iRow >= nRow)) {
      if (pCommitter->nBlockData.nRow > 0) {
        goto _write_block_data;
      } else {
        break;
      }
    }

    // TODO

  _write_block_data:
    block.last = pCommitter->nBlockData.nRow < pCommitter->minRow ? 1 : 0;
    code = tsdbWriteBlockData(pCommitter->pWriter, &pCommitter->nBlockData, NULL, NULL, pBlockIdx, &block);
    if (code) goto _err;

    code = tMapDataPutItem(&pCommitter->nBlockMap, &block, tPutBlock);
    if (code) goto _err;
  }

  tBlockReset(&block);
  tBlockDataReset(&pCommitter->nBlockData);

  return code;

_err:
  tsdbError("vgId:%d merge commit impl failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbMergeCommit(SCommitter *pCommitter, SBlockIdx *pBlockIdx, STbDataIter *pIter, SBlock *pBlock,
                               int8_t isLastBlock) {
  int32_t    code = 0;
  TSDBROW   *pRow;
  SBlock     block = tBlockInit();
  SBlockData nBlockData;
  TSDBKEY    key;
  int32_t    c;

  if (pBlock == NULL) {
    key.ts = pCommitter->maxKey;
    key.version = INT64_MAX;
    code = tsdbCommitMemoryData(pCommitter, pBlockIdx, pIter, key, 0);
    if (code) goto _err;
  } else if (pBlock->last) {
    // merge
    code = tsdbMergeCommitImpl(pCommitter, pBlockIdx, pIter, pBlock, 0);
    if (code) goto _err;
  } else {
    // memory
    key.ts = pBlock->info.minKey.ts;
    key.version = pBlock->info.minKey.version - 1;
    code = tsdbCommitMemoryData(pCommitter, pBlockIdx, pIter, key, 1);
    if (code) goto _err;

    // merge or move block
    pRow = tsdbTbDataIterGet(pIter);
    key.ts = pRow->pTSRow->ts;
    key.version = pRow->version;

    c = tBlockCmprFn(&(SBlock){.info.maxKey = key, .info.minKey = key}, pBlock);
    if (c > 0) {
      // move block
      code = tMapDataPutItem(&pCommitter->nBlockMap, pBlock, tPutBlock);
      if (code) goto _err;
    } else if (c == 0) {
      int32_t nOverlap = tsdbGetOverlapRowNumber(pIter, pBlock);

      if (pBlock->nRow + nOverlap > pCommitter->maxRow || pBlock->nSubBlock == TSDB_MAX_SUBBLOCKS) {
        code = tsdbMergeCommitImpl(pCommitter, pBlockIdx, pIter, pBlock, 1);
        if (code) goto _err;
      } else {
        // add as a subblock
      }
    } else {
      ASSERT(0);
    }
  }

  return code;

_err:
  tsdbError("vgId:%d merge commit failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableData(SCommitter *pCommitter, STbData *pTbData, SBlockIdx *pBlockIdx) {
  int32_t      code = 0;
  STbDataIter  iter;
  STbDataIter *pIter = &iter;
  TSDBROW     *pRow;
  int64_t      suid;
  int64_t      uid;
  SBlockIdx    blockIdx;

  // create iter
  if (pTbData) {
    suid = pTbData->suid;
    uid = pTbData->uid;
    tsdbTbDataIterOpen(pTbData, &(TSDBKEY){.ts = pCommitter->minKey, .version = 0}, 0, pIter);
  } else {
    suid = pBlockIdx->suid;
    uid = pBlockIdx->uid;
    pIter = NULL;
  }

  // check
  pRow = tsdbTbDataIterGet(pIter);
  if (ROW_END(pRow, pCommitter->maxKey) && pBlockIdx == NULL) goto _exit;

  // start ================================
  tMapDataReset(&pCommitter->oBlockMap);
  tBlockReset(&pCommitter->oBlock);
  tBlockDataReset(&pCommitter->oBlockData);
  if (pBlockIdx) {
    code = tsdbReadBlock(pCommitter->pReader, pBlockIdx, &pCommitter->oBlockMap, NULL);
    if (code) goto _err;
  }

  blockIdx = tBlockIdxInit(suid, uid);
  tMapDataReset(&pCommitter->nBlockMap);
  tBlockReset(&pCommitter->nBlock);
  tBlockDataReset(&pCommitter->nBlockData);

  // impl ===============================
  int32_t iBlock = 0;
  int32_t nBlock = pCommitter->oBlockMap.nItem;

  // merge
  pRow = tsdbTbDataIterGet(pIter);
  while (!ROW_END(pRow, pCommitter->maxKey) && iBlock < nBlock) {
    tMapDataGetItemByIdx(&pCommitter->oBlockMap, iBlock, &pCommitter->oBlock, tGetBlock);
    code = tsdbMergeCommit(pCommitter, &blockIdx, pIter, &pCommitter->oBlock, iBlock == (nBlock - 1));
    if (code) goto _err;

    pRow = tsdbTbDataIterGet(pIter);
    iBlock++;
  }

  // mem
  pRow = tsdbTbDataIterGet(pIter);
  while (!ROW_END(pRow, pCommitter->maxKey)) {
    code = tsdbMergeCommit(pCommitter, &blockIdx, pIter, NULL, 0);
    if (code) goto _err;

    pRow = tsdbTbDataIterGet(pIter);
  }

  // disk
  while (iBlock < nBlock) {
    tMapDataGetItemByIdx(&pCommitter->oBlockMap, iBlock, &pCommitter->oBlock, tGetBlock);

    code = tsdbMergeCommit(pCommitter, &blockIdx, NULL, &pCommitter->oBlock, 0);
    if (code) goto _err;

    iBlock++;
  }

  // end ===============================
  code = tsdbWriteBlock(pCommitter->pWriter, &pCommitter->nBlockMap, NULL, &blockIdx);
  if (code) goto _err;

  code = tMapDataPutItem(&pCommitter->nBlockIdxMap, &blockIdx, tPutBlockIdx);
  if (code) goto _err;

_exit:
  pRow = tsdbTbDataIterGet(pIter);
  if (pRow) {
    ASSERT(pRow->pTSRow->ts > pCommitter->maxKey);
    if (pCommitter->nextKey > pRow->pTSRow->ts) {
      pCommitter->nextKey = pRow->pTSRow->ts;
    }
  }

  return code;

_err:
  tsdbError("vgId:%d commit Table data failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitFileDataStart(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SDFileSet *pRSet = NULL;  // TODO
  SDFileSet *pWSet = NULL;  // TODO

  // memory
  pCommitter->nextKey = TSKEY_MAX;

  // old
  tMapDataReset(&pCommitter->oBlockIdxMap);
  tMapDataReset(&pCommitter->oBlockMap);
  tBlockReset(&pCommitter->oBlock);
  tBlockDataReset(&pCommitter->oBlockData);
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
  code = tsdbDataFWriterOpen(&pCommitter->pWriter, pTsdb, pWSet);
  if (code) goto _err;

_exit:
  return code;

_err:
  tsdbError("vgId:%d commit file data start failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
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
  SBlockIdx  blockIdx;
  SBlockIdx *pBlockIdx = &blockIdx;

  ASSERT(nTbData > 0);

  pTbData = (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData);
  if (iBlockIdx < nBlockIdx) {
    tMapDataGetItemByIdx(&pCommitter->oBlockIdxMap, iBlockIdx, pBlockIdx, tGetBlockIdx);
  } else {
    pBlockIdx = NULL;
  }

  while (true) {
    if (pTbData == NULL && pBlockIdx == NULL) break;

    if (pTbData && pBlockIdx) {
      c = tTABLEIDCmprFn(pTbData, pBlockIdx);

      if (c == 0) {
        goto _commit_mem_and_disk_data;
      } else if (c < 0) {
        goto _commit_mem_data;
      } else {
        goto _commit_disk_data;
      }
    } else if (pTbData) {
      goto _commit_mem_data;
    } else {
      goto _commit_disk_data;
    }

  _commit_mem_data:
    code = tsdbCommitTableData(pCommitter, pTbData, NULL);
    if (code) goto _err;

    iTbData++;
    if (iTbData < nTbData) {
      pTbData = (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData);
    } else {
      pTbData = NULL;
    }
    continue;

  _commit_disk_data:
    code = tsdbCommitTableData(pCommitter, NULL, pBlockIdx);
    if (code) goto _err;

    iBlockIdx++;
    if (iBlockIdx < nBlockIdx) {
      tMapDataGetItemByIdx(&pCommitter->oBlockIdxMap, iBlockIdx, pBlockIdx, tGetBlockIdx);
    } else {
      pBlockIdx = NULL;
    }
    continue;

  _commit_mem_and_disk_data:
    code = tsdbCommitTableData(pCommitter, pTbData, pBlockIdx);
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
    continue;
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
  code = tsdbUpdateDFileSetHeader(pCommitter->pWriter, NULL);
  if (code) goto _err;

  // close and sync
  code = tsdbDataFWriterClose(pCommitter->pWriter, 1);
  if (code) goto _err;

  if (pCommitter->pReader) {
    code = tsdbDataFReaderClose(pCommitter->pReader);
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
  pCommitter->minutes = pTsdb->keepCfg.days;
  pCommitter->precision = pTsdb->keepCfg.precision;
  pCommitter->minRow = pTsdb->pVnode->config.tsdbCfg.minRows;
  pCommitter->maxRow = pTsdb->pVnode->config.tsdbCfg.maxRows;

  return code;
}

static int32_t tsdbCommitData(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  // check
  if (pMemTable->nRow == 0) goto _exit;

  // loop
  pCommitter->nextKey = pMemTable->info.minKey.ts;
  while (pCommitter->nextKey < TSKEY_MAX) {
    pCommitter->commitFid = tsdbKeyFid(pCommitter->nextKey, pCommitter->minutes, pCommitter->precision);
    tsdbFidKeyRange(pCommitter->commitFid, pCommitter->minutes, pCommitter->precision, &pCommitter->minKey,
                    &pCommitter->maxKey);
    code = tsdbCommitFileData(pCommitter);
    if (code) goto _err;
  }

_exit:
  tsdbDebug("vgId:%d commit data done, nRow:%" PRId64, TD_VID(pTsdb->pVnode), pMemTable->nRow);
  return code;

_err:
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
  int32_t code = 0;
  // TODO
  return code;
}
