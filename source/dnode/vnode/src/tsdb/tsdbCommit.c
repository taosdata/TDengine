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

typedef struct SCommitter SCommitter;

struct SCommitter {
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
  TSKEY   nextCommitKey;
  // commit file data
  int32_t          commitFid;
  TSKEY            minKey;
  TSKEY            maxKey;
  SDFileSetReader *pReader;
  SDFileSetWriter *pWriter;
  SMapData         oBlockIdx;
  SMapData         nBlockIdx;
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
  SDelFWriter *pDelFWriter;
  SDelIdx      delIdxOld;
  SDelIdx      delIdxNew;
  STbData     *pTbData;
  SDelIdxItem *pDelIdxItem;
  SDelData     delDataOld;
  SDelData     delDataNew;
  SDelIdxItem  delIdxItem;
  /* commit cache */
};

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
  int32_t    code = 0;
  SCommitter commith = {0};
  int        fid;

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

  return code;

_err:
  tsdbError("vgId:%d, failed to commit since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbStartCommit(STsdb *pTsdb, SCommitter *pCommitter) {
  int32_t code = 0;

  ASSERT(pTsdb->mem && pTsdb->imem == NULL);
  // lock();
  pTsdb->imem = pTsdb->mem;
  pTsdb->mem = NULL;
  // unlock();

  pCommitter->pTsdb = pTsdb;

  return code;
}

static int32_t tsdbCommitDataStart(SCommitter *pCommitter);
static int32_t tsdbCommitDataImpl(SCommitter *pCommitter);
static int32_t tsdbCommitDataEnd(SCommitter *pCommitter);

static int32_t tsdbCommitData(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  // no data, just return
  if (pMemTable->nRow == 0) {
    goto _exit;
  }

  // start
  code = tsdbCommitDataStart(pCommitter);
  if (code) {
    goto _err;
  }

  // commit
  code = tsdbCommitDataImpl(pCommitter);
  if (code) {
    goto _err;
  }

  // end
  code = tsdbCommitDataEnd(pCommitter);
  if (code) {
    goto _err;
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

  // load old
  pCommitter->delIdxOld = (SDelIdx){0};
  if (pDelFileR) {
    code = tsdbDelFReaderOpen(&pCommitter->pDelFReader, pDelFileR, pTsdb, NULL);
    if (code) {
      goto _err;
    }

    code = tsdbReadDelIdx(pCommitter->pDelFReader, &pCommitter->delIdxOld, &pCommitter->pBuf1);
    if (code) {
      goto _err;
    }
  }

  // prepare new
  pCommitter->delIdxNew = (SDelIdx){0};
  code = tsdbDelFWriterOpen(&pCommitter->pDelFWriter, pDelFileW, pTsdb);
  if (code) {
    goto _err;
  }

_exit:
  tsdbDebug("vgId:%d commit del start", TD_VID(pTsdb->pVnode));
  return code;

_err:
  tsdbError("vgId:%d commit del start failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableDel(SCommitter *pCommitter);

static int32_t tsdbCommitDelImpl(SCommitter *pCommitter) {
  int32_t      code = 0;
  STsdb       *pTsdb = pCommitter->pTsdb;
  SMemTable   *pMemTable = pTsdb->imem;
  int32_t      c;
  int32_t      iTbData = 0;
  int32_t      nTbData = taosArrayGetSize(pMemTable->aTbData);
  int32_t      iDelIdxItem = 0;
  int32_t      nDelIdxItem = pCommitter->delIdxOld.offset.nOffset;
  STbData     *pTbData = NULL;
  SDelIdxItem *pDelIdxItem = NULL;
  SDelIdxItem  item;

  while (iTbData < nTbData || iDelIdxItem < nDelIdxItem) {
    pTbData = NULL;
    pDelIdxItem = NULL;
    if (iTbData < nTbData) {
      pTbData = (STbData *)taosArrayGetP(pMemTable->aTbData, iTbData);
    }
    if (iDelIdxItem < nDelIdxItem) {
      tDelIdxGetItemByIdx(&pCommitter->delIdxOld, &item, iDelIdxItem);
      pDelIdxItem = &item;
    }

    if (pTbData && pDelIdxItem) {
      c = tTABLEIDCmprFn(pTbData, pDelIdxItem);
      if (c == 0) {
        iTbData++;
        iDelIdxItem++;
      } else if (c < 0) {
        iTbData++;
        pDelIdxItem = NULL;
      } else {
        iDelIdxItem++;
        pTbData = NULL;
      }
    } else {
      if (pTbData) {
        iTbData++;
      }
      if (pDelIdxItem) {
        iDelIdxItem++;
      }
    }

    if (pTbData && pTbData->pHead == NULL) {
      pTbData = NULL;
    }

    if (pTbData == NULL && pDelIdxItem == NULL) continue;

    // do merge
    pCommitter->pTbData = pTbData;
    pCommitter->pDelIdxItem = pDelIdxItem;
    code = tsdbCommitTableDel(pCommitter);
    if (code) goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d commit del impl failed since %s", TD_VID(pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitDelEnd(SCommitter *pCommitter) {
  int32_t code = 0;

  code = tsdbWriteDelIdx(pCommitter->pDelFWriter, &pCommitter->delIdxNew, NULL);
  if (code) {
    goto _err;
  }

  code = tsdbUpdateDelFileHdr(pCommitter->pDelFWriter, NULL);
  if (code) {
    goto _err;
  }

  code = tsdbDelFWriterClose(pCommitter->pDelFWriter, 1);
  if (code) {
    goto _err;
  }

  if (pCommitter->pDelFReader) {
    code = tsdbDelFReaderClose(pCommitter->pDelFReader);
    if (code) goto _err;
  }

  tDelDataClear(&pCommitter->delDataNew);
  tDelIdxClear(&pCommitter->delIdxNew);

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

static int32_t tsdbCommitDataStart(SCommitter *pCommitter) {
  int32_t    code = 0;
  STsdb     *pTsdb = pCommitter->pTsdb;
  SMemTable *pMemTable = pTsdb->imem;

  pCommitter->nextCommitKey = pMemTable->minKey.ts;

  return code;
}

static int32_t tsdbCommitFileData(SCommitter *pCommitter);

static int32_t tsdbCommitDataImpl(SCommitter *pCommitter) {
  int32_t code = 0;

  while (pCommitter->nextCommitKey < TSKEY_MAX) {
    pCommitter->commitFid = tsdbKeyFid(pCommitter->nextCommitKey, pCommitter->minutes, pCommitter->precision);
    code = tsdbCommitFileData(pCommitter);
    if (code) goto _err;
  }

_exit:
  return code;

_err:
  return code;
}

static int32_t tsdbCommitDataEnd(SCommitter *pCommitter) {
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

  // load old
  pCommitter->oBlockIdx.nItem = 0;
  pCommitter->oBlockIdx.flag = 0;
  pCommitter->oBlockIdx.nData = 0;
  if (pRSet) {
    code = tsdbDFileSetReaderOpen(&pCommitter->pReader, pTsdb, pRSet);
    if (code) goto _err;

    code = tsdbReadBlockIdx(pCommitter->pReader, &pCommitter->oBlockIdx);
    if (code) goto _err;
  }

  // create new
  pCommitter->nBlockIdx.nItem = 0;
  pCommitter->nBlockIdx.flag = 0;
  pCommitter->nBlockIdx.nData = 0;
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

static int32_t tsdbCommitTableDelStart(SCommitter *pCommitter) {
  int32_t  code = 0;
  tb_uid_t suid;
  tb_uid_t uid;

  if (pCommitter->pTbData) {
    suid = pCommitter->pTbData->suid;
    uid = pCommitter->pTbData->uid;
  }

  // load old
  pCommitter->delDataOld = (SDelData){0};
  if (pCommitter->pDelIdxItem) {
    suid = pCommitter->pDelIdxItem->suid;
    uid = pCommitter->pDelIdxItem->uid;
    code =
        tsdbReadDelData(pCommitter->pDelFReader, pCommitter->pDelIdxItem, &pCommitter->delDataOld, &pCommitter->pBuf5);
    if (code) goto _err;
  }

  // prepare new
  pCommitter->delDataNew.suid = suid;
  pCommitter->delDataNew.uid = uid;
  pCommitter->delDataNew.offset.flag = 0;
  pCommitter->delDataNew.offset.nOffset = 0;
  pCommitter->delDataNew.nData = 0;
  pCommitter->delIdxItem = (SDelIdxItem){
      .suid = suid,
      .uid = uid,
      .minKey = TSKEY_MAX,
      .maxKey = TSKEY_MIN,
      .minVersion = INT64_MAX,
      .maxVersion = INT64_MIN,
      .offset = -1,
      .size = -1,
  };

  return code;

_err:
  tsdbError("vgId:%d commit table del start failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableDelImpl(SCommitter *pCommitter) {
  int32_t      code = 0;
  SDelDataItem item;

  // old
  if (pCommitter->pDelIdxItem) {
    for (int32_t iDelIdxItem = 0; iDelIdxItem < pCommitter->delDataOld.offset.nOffset; iDelIdxItem++) {
      code = tDelDataGetItemByIdx(&pCommitter->delDataOld, &item, iDelIdxItem);
      if (code) goto _err;

      code = tDelDataPutItem(&pCommitter->delDataNew, &item);
      if (code) goto _err;

      // update index
      if (item.version < pCommitter->delIdxItem.minVersion) pCommitter->delIdxItem.minVersion = item.version;
      if (item.version > pCommitter->delIdxItem.maxVersion) pCommitter->delIdxItem.maxVersion = item.version;
      if (item.sKey < pCommitter->delIdxItem.minKey) pCommitter->delIdxItem.minKey = item.sKey;
      if (item.eKey > pCommitter->delIdxItem.maxKey) pCommitter->delIdxItem.maxKey = item.eKey;
    }
  }

  // new
  if (pCommitter->pTbData) {
    for (SDelOp *pDelOp = pCommitter->pTbData->pHead; pDelOp; pDelOp = pDelOp->pNext) {
      item = (SDelDataItem){.version = pDelOp->version, .sKey = pDelOp->sKey, .eKey = pDelOp->eKey};

      code = tDelDataPutItem(&pCommitter->delDataNew, &item);
      if (code) goto _err;

      // update index
      if (item.version < pCommitter->delIdxItem.minVersion) pCommitter->delIdxItem.minVersion = item.version;
      if (item.version > pCommitter->delIdxItem.maxVersion) pCommitter->delIdxItem.maxVersion = item.version;
      if (item.sKey < pCommitter->delIdxItem.minKey) pCommitter->delIdxItem.minKey = item.sKey;
      if (item.eKey > pCommitter->delIdxItem.maxKey) pCommitter->delIdxItem.maxKey = item.eKey;
    }
  }

  return code;

_err:
  return code;
}

static int32_t tsdbCommitTableDelEnd(SCommitter *pCommitter) {
  int32_t code = 0;

  // write table del data
  code = tsdbWriteDelData(pCommitter->pDelFWriter, &pCommitter->delDataNew, NULL, &pCommitter->delIdxItem.offset,
                          &pCommitter->delIdxItem.size);
  if (code) goto _err;

  // add SDelIdxItem
  code = tDelIdxPutItem(&pCommitter->delIdxNew, &pCommitter->delIdxItem);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d commit table del end failed since %s", TD_VID(pCommitter->pTsdb->pVnode), tstrerror(code));
  return code;
}

static int32_t tsdbCommitTableDel(SCommitter *pCommitter) {
  int32_t code = 0;

  // start
  code = tsdbCommitTableDelStart(pCommitter);
  if (code) goto _err;

  // impl
  code = tsdbCommitTableDelImpl(pCommitter);
  if (code) goto _err;

  // end
  code = tsdbCommitTableDelEnd(pCommitter);
  if (code) goto _err;

  return code;

_err:
  return code;
}