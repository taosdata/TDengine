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
  STsdb   *pTsdb;
  uint8_t *pBuf1;
  uint8_t *pBuf2;
  uint8_t *pBuf3;
  uint8_t *pBuf4;
  uint8_t *pBuf5;
  /* commit data */
  int32_t minutes;
  int8_t  precision;
  int32_t minRow;
  int32_t maxRow;
  // commit file data
  TSKEY         nextKey;
  int32_t       commitFid;
  TSKEY         minKey;
  TSKEY         maxKey;
  SDataFReader *pReader;
  SMapData      oBlockIdx;  // SMapData<SBlockIdx>, read from reader
  SDataFWriter *pWriter;
  SMapData      nBlockIdx;  // SMapData<SBlockIdx>, build by committer
  // commit table data
  STbDataIter   iter;
  STbDataIter  *pIter;
  SBlockIdx    *pBlockIdx;
  SMapData      oBlock;
  SMapData      nBlock;
  SColDataBlock oColDataBlock;
  SColDataBlock nColDataBlock;
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
  if (code) {
    goto _err;
  }

  return code;

_err:
  return code;
}

int32_t tsdbCommit(STsdb *pTsdb) {
  if (!pTsdb) return 0;

  int32_t    code = 0;
  SCommitter commith;
  SMemTable *pMemTable = pTsdb->mem;

  // check
  if (pMemTable->nRow == 0 && pMemTable->nDel == 0) {  // TODO
    pTsdb->mem = NULL;
    tsdbMemTableDestroy(pMemTable);
    goto _exit;
  }

  // start commit
  code = tsdbStartCommit(pTsdb, &commith);
  if (code) {
    goto _err;
  }

  // commit impl
  code = tsdbCommitData(&commith);
  if (code) {
    goto _err;
  }

  code = tsdbCommitDel(&commith);
  if (code) {
    goto _err;
  }

  code = tsdbCommitCache(&commith);
  if (code) {
    goto _err;
  }

  // end commit
  code = tsdbEndCommit(&commith, 0);
  if (code) {
    goto _err;
  }

_exit:
  return code;

_err:
  tsdbEndCommit(&commith, code);
  tsdbError("vgId:%d, failed to commit since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbStartCommit(STsdb *pTsdb, SCommitter *pCommitter) {
  int32_t code = 0;

  memset(pCommitter, 0, sizeof(*pCommitter));
  ASSERT(pTsdb->mem && pTsdb->imem == NULL);
  // lock();
  pTsdb->imem = pTsdb->mem;
  pTsdb->mem = NULL;
  // unlock();

  pCommitter->pTsdb = pTsdb;

  return code;
}

static int32_t tsdbCommitData(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  // check
  if (pMemTable->nRow == 0) {
    goto _exit;
  }

  // loop
  pCommitter->nextKey = pMemTable->minKey.ts;
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

static int32_t tsdbCommitFileDataStart(SCommitter *pCommitter);
static int32_t tsdbCommitFileDataImpl(SCommitter *pCommitter);
static int32_t tsdbCommitFileDataEnd(SCommitter *pCommitter);

static int32_t tsdbCommitFileData(SCommitter *pCommitter) {
  int32_t code = 0;

  // commit file data start
  code = tsdbCommitFileDataStart(pCommitter);
  if (code) {
    goto _err;
  }

  // commit file data impl
  code = tsdbCommitFileDataImpl(pCommitter);
  if (code) {
    goto _err;
  }

  // commit file data end
  code = tsdbCommitFileDataEnd(pCommitter);
  if (code) {
    goto _err;
  }

  return code;

_err:
  return code;
}

static int32_t tsdbCommitFileDataStart(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SDFileSet *pRSet = NULL;  // TODO
  SDFileSet *pWSet = NULL;  // TODO

  // memory
  tMapDataReset(&pCommitter->oBlockIdx);
  tMapDataReset(&pCommitter->nBlockIdx);

  // load old
  if (pRSet) {
    code = tsdbDFileSetReaderOpen(&pCommitter->pReader, pTsdb, pRSet);
    if (code) goto _err;

    code = tsdbReadBlockIdx(pCommitter->pReader, &pCommitter->oBlockIdx);
    if (code) goto _err;
  }

  // create new
  code = tsdbDFileSetWriterOpen(&pCommitter->pWriter, pTsdb, pWSet);
  if (code) goto _err;

_exit:
  return code;

_err:
  tsdbError("vgId:%d commit file data start failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableData(SCommitter *pCommitter);

static int32_t tsdbCommitFileDataImpl(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;
  int32_t    iTbData = 0;
  int32_t    nTbData = taosArrayGetSize(pMemTable->aTbData);
  int32_t    iBlockIdx = 0;
  int32_t    nBlockIdx = pCommitter->oBlockIdx.nItem;
  STbData   *pTbData;
  SBlockIdx *pBlockIdx;
  SBlockIdx  blockIdx;
  int32_t    c;

  while (iTbData < nTbData || iBlockIdx < nBlockIdx) {
    pTbData = NULL;
    pBlockIdx = NULL;
    if (iTbData < nTbData) {
      pTbData = (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData);
    }
    if (iBlockIdx < nBlockIdx) {
      tMapDataGetItemByIdx(&pCommitter->oBlockIdx, iBlockIdx, &blockIdx, NULL /* TODO */);
      pBlockIdx = &blockIdx;
    }

    if (pTbData && pBlockIdx) {
      c = tTABLEIDCmprFn(pTbData, pBlockIdx);

      if (c == 0) {
        iTbData++;
        iBlockIdx++;
      } else if (c < 0) {
        iTbData++;
        pBlockIdx = NULL;
      } else {
        iBlockIdx++;
        pTbData = NULL;
      }
    } else {
      if (pTbData) {
        iBlockIdx++;
      }
      if (pBlockIdx) {
        iTbData++;
      }
    }

    if (pTbData &&
        !tsdbTbDataIterOpen(pTbData, &(TSDBKEY){.ts = pCommitter->minKey, .version = 0}, 0, &pCommitter->iter)) {
      pTbData = NULL;
    }

    if (pTbData == NULL && pBlockIdx == NULL) continue;

    pCommitter->pTbData = pTbData;
    pCommitter->pBlockIdx = pBlockIdx;

    code = tsdbCommitTableData(pCommitter);
    if (code) goto _err;
  }

  return code;

_err:
  return code;
}

static int32_t tsdbCommitFileDataEnd(SCommitter *pCommitter) {
  int32_t code = 0;

  code = tsdbWriteBlockIdx(pCommitter->pWriter, pCommitter->nBlockIdx, NULL);
  if (code) goto _err;

  code = tsdbUpdateDFileSetHeader(pCommitter->pWriter, NULL);
  if (code) goto _err;

  code = tsdbDFileSetWriterClose(pCommitter->pWriter, 1);
  if (code) goto _err;

  if (pCommitter->pReader) {
    code = tsdbDFileSetReaderClose(pCommitter->pReader);
    goto _err;
  }

_exit:
  return code;

_err:
  return code;
}

static int32_t tsdbCommitTableDataStart(SCommitter *pCommitter);
static int32_t tsdbCommitTableDataImpl(SCommitter *pCommitter);
static int32_t tsdbCommitTableDataEnd(SCommitter *pCommitter);

static int32_t tsdbCommitTableData(SCommitter *pCommitter) {
  int32_t code = 0;

  // start
  code = tsdbCommitTableDataStart(pCommitter);
  if (code) {
    goto _err;
  }

  // impl
  code = tsdbCommitTableDataImpl(pCommitter);
  if (code) {
    goto _err;
  }

  // end
  code = tsdbCommitTableDataEnd(pCommitter);
  if (code) {
    goto _err;
  }

_exit:
  return code;

_err:
  return code;
}

static int32_t tsdbCommitTableDataStart(SCommitter *pCommitter) {
  int32_t code = 0;

  // old
  tMapDataReset(&pCommitter->oBlock);
  if (pCommitter->pBlockIdx) {
    code = tsdbReadBlock(pCommitter->pReader, &pCommitter->oBlock, NULL);
    if (code) goto _err;
  }

  // new
  tMapDataReset(&pCommitter->nBlock);

_err:
  return code;
}

static int32_t tsdbCommitTableDataImpl(SCommitter *pCommitter) {
  int32_t      code = 0;
  STsdb       *pTsdb = pCommitter->pTsdb;
  STbDataIter *pIter = NULL;
  int32_t      iBlock = 0;
  int32_t      nBlock = pCommitter->nBlock.nItem;
  SBlock      *pBlock;
  SBlock       block;
  TSDBROW     *pRow;
  TSDBROW      row;
  int32_t      iRow = 0;
  STSchema    *pTSchema = NULL;

  if (pCommitter->pTbData) {
    code = tsdbTbDataIterCreate(pCommitter->pTbData, &(TSDBKEY){.ts = pCommitter->minKey, .version = 0}, 0, &pIter);
    if (code) goto _err;
  }

  if (iBlock < nBlock) {
    pBlock = &block;
  } else {
    pBlock = NULL;
  }

  tsdbTbDataIterGet(pIter, pRow);

  // loop to merge memory data and disk data
  for (; pBlock == NULL || (pRow && pRow->pTSRow->ts <= pCommitter->maxKey);) {
    if (pRow == NULL || pRow->pTSRow->ts > pCommitter->maxKey) {
      // only has block data, then move to new index file
    } else if (0) {
      // only commit memory data
    } else {
      // merge memory and block data
    }
  }

  tsdbTbDataIterDestroy(pIter);
  return code;

_err:
  tsdbError("vgId:%d commit table data impl failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  tsdbTbDataIterDestroy(pIter);
  return code;
}

static int32_t tsdbCommitTableDataEnd(SCommitter *pCommitter) {
  int32_t code = 0;
  // TODO
  return code;
}

static int32_t tsdbCommitTableDel(SCommitter *pCommitter, STbData *pTbData, SDelIdx *pDelIdx) {
  int32_t      code = 0;
  SDelData     delData;
  SDelOp      *pDelOp;
  tb_uid_t     suid;
  tb_uid_t     uid;
  SDelIdx      delIdx;  // TODO
  SDelDataInfo info;    // TODO

  // check no del data, just return
  if (pTbData && pTbData->pHead == NULL) {
    pTbData = NULL;
  }
  if (pTbData == NULL && pDelIdx == NULL) goto _exit;

  // prepare
  if (pTbData) {
    info.suid = pTbData->suid;
    info.uid = pTbData->uid;
  } else {
    info.suid = pDelIdx->suid;
    info.uid = pDelIdx->uid;
  }
  delIdx.suid = info.suid;
  delIdx.uid = info.uid;
  delIdx.minKey = TSKEY_MAX;
  delIdx.maxKey = TSKEY_MIN;
  delIdx.minVersion = INT64_MAX;
  delIdx.maxVersion = -1;

  // start
  tMapDataReset(&pCommitter->oDelDataMap);
  tMapDataReset(&pCommitter->nDelDataMap);

  if (pDelIdx) {
    code = tsdbReadDelData(pCommitter->pDelFReader, pDelIdx, &pCommitter->oDelDataMap, NULL);
    if (code) goto _err;
  }

  // disk
  for (int32_t iDelData = 0; iDelData < pCommitter->oDelDataMap.nItem; iDelData++) {
    code = tMapDataGetItemByIdx(&pCommitter->oDelDataMap, iDelData, &delData, tGetDelData);
    if (code) goto _err;

    code = tMapDataPutItem(&pCommitter->nDelDataMap, &delData, tPutDelData);
    if (code) goto _err;

    if (delIdx.minKey > delData.sKey) delIdx.minKey = delData.sKey;
    if (delIdx.maxKey < delData.eKey) delIdx.maxKey = delData.eKey;
    if (delIdx.minVersion > delData.version) delIdx.minVersion = delData.version;
    if (delIdx.maxVersion < delData.version) delIdx.maxVersion = delData.version;
  }

  // memory
  pDelOp = pTbData ? pTbData->pHead : NULL;
  for (; pDelOp; pDelOp = pDelOp->pNext) {
    delData.version = pDelOp->version;
    delData.sKey = pDelOp->sKey;
    delData.eKey = pDelOp->eKey;

    code = tMapDataPutItem(&pCommitter->nDelDataMap, &delData, tPutDelData);
    if (code) goto _err;

    if (delIdx.minKey > delData.sKey) delIdx.minKey = delData.sKey;
    if (delIdx.maxKey < delData.eKey) delIdx.maxKey = delData.eKey;
    if (delIdx.minVersion > delData.version) delIdx.minVersion = delData.version;
    if (delIdx.maxVersion < delData.version) delIdx.maxVersion = delData.version;
  }

  ASSERT(pCommitter->nDelDataMap.nItem > 0);

  // write
  code = tsdbWriteDelData(pCommitter->pDelFWriter, &info, &pCommitter->nDelDataMap, NULL, &delIdx.offset, &delIdx.size);
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