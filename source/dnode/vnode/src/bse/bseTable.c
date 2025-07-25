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

#include "bseTable.h"
#include "bse.h"
#include "bseCache.h"
#include "bseSnapshot.h"
#include "bseTableMgt.h"
#include "osMemPool.h"
#include "vnodeInt.h"

// table footer func
static int32_t footerEncode(STableFooter *pFooter, char *buf);
static int32_t footerDecode(STableFooter *pFooter, char *buf);

// block handle func
static int32_t blkHandleEncode(SBlkHandle *pHandle, char *buf);
static int32_t blkHandleDecode(SBlkHandle *pHandle, char *buf);

// table meta func
static int32_t metaBlockEncode(SMetaBlock *pMeta, char *buf);
static int32_t metaBlockDecode(SMetaBlock *pMeta, char *buf);

static int32_t metaBlockAdd(SBlock *p, SMetaBlock *pMeta);
static int32_t metaBlockGet(SBlock *p, SMetaBlock *pMeta);

// table footer func
static int32_t footerEncode(STableFooter *pFooter, char *buf);
static int32_t footerDecode(STableFooter *pFooter, char *buf);

// block func
static int32_t blockCreate(int32_t cap, SBlock **pBlock);
static void    blockDestroy(SBlock *pBlock);
static int32_t blockPut(SBlock *pBlock, int64_t seq, uint8_t *value, int32_t len);
static int32_t blockAppendBatch(SBlock *p, uint8_t *value, int32_t len);
static int32_t blockEsimateSize(SBlock *pBlock, int32_t extra);
static void    blockClear(SBlock *pBlock);
static int32_t blockSeek(SBlock *p, int64_t seq, uint8_t **pValue, int32_t *len);
static int8_t  blockGetType(SBlock *p);

static int32_t blockSeekMeta(SBlock *p, int64_t seq, SMetaBlock *pMeta);
static int32_t blockGetAllMeta(SBlock *p, SArray *pResult);
static int32_t metaBlockAddIndex(SBlock *p, SBlkHandle *pInfo);

int32_t tableMetaWriterInit(SBTableMeta *pMeta, char *name, SBtableMetaWriter **ppWriter);
int32_t tableMetaWriterCommit(SBtableMetaWriter *pMeta);
void    tableMetaWriterClose(SBtableMetaWriter *p);
int32_t tableMetaWriteAppendRawBlock(SBtableMetaWriter *pMeta, SBlockWrapper *pBlock, SBlkHandle *pBlkHandle);

int32_t tableMetaReaderInit(SBTableMeta *pMeta, char *name, SBtableMetaReader **ppReader);
void    tableMetaReaderClose(SBtableMetaReader *p);
int32_t tableMetaReaderLoadIndex(SBtableMetaReader *p);

int32_t tableMetaOpenFile(SBtableMetaWriter *pMeta, int8_t read, char *name);

int32_t tableMetaReaderOpenIter(SBtableMetaReader *pReader, SBtableMetaReaderIter **pIter);
int32_t tableMetaReaderIterNext(SBtableMetaReaderIter *pIter, SBlockWrapper *pDataWrapper, SBlkHandle *dstHandle);
void    tableMetaReaderIterClose(SBtableMetaReaderIter *p);

// STable builder func
static int32_t tableBuilderGetBlockSize(STableBuilder *p);
static int32_t tableBuilderLoadBlock(STableBuilder *p, SBlkHandle *pHandle, SBlockWrapper *pBlkWrapper);
static int32_t tableBuilderSeek(STableBuilder *p, SBlkHandle *pHandle, int64_t seq, uint8_t **pValue, int32_t *len);
static void    tableBuilderUpdateBlockRange(STableBuilder *p, SBlockItemInfo *pInfo);
static void    tableBuildUpdateTableRange(STableBuilder *p, SBlockItemInfo *pInfo);

// STable pReaderMgt func

static int32_t tableReaderInitMeta(STableReader *p, SBlock *pBlock);

static int32_t tableReaderLoadRawBlock(STableReader *p, SBlkHandle *pHandle, SBlockWrapper *pBlkWrapper);
static int32_t tableReaderLoadRawMeta(STableReader *p, SBlkHandle *pHandle, SBlockWrapper *blkWrapper);
static int32_t tableReaderLoadRawMetaIndex(STableReader *p, SBlockWrapper *blkWrapper);
static int32_t tableReaderLoadRawFooter(STableReader *p, SBlockWrapper *blkWrapper);

static int32_t tableOpenFile(char *name, int8_t read, TdFilePtr *pFile, int64_t *size);
static int32_t tableFlushBlock(TdFilePtr pFile, SBlkHandle *pHandle, SBlockWrapper *pBlk, int32_t *nWrite);
static int32_t tableLoadBlock(TdFilePtr pFile, SBlkHandle *pHandle, SBlockWrapper *pBlk);
static int32_t tableLoadRawBlock(TdFilePtr pFile, SBlkHandle *pHandle, SBlockWrapper *pBlk, int8_t checkSum);

/*---block formate----*/
//---datatype--|---len---|--data---|--rawdatasize---|--compressType---|---checksum---|
//- int8_t   |  int32_t | uint8_t[] |    int32_t     |      int8_t     |    TSCKSUM|
#define BLOCK_ROW_SIZE_OFFSET(p)      (sizeof(SBlock) + (p)->len)
#define BLOCK_ROW_SIZE(p)             BLOCK_ROW_SIZE_OFFSET(p)
#define BLOCK_COMPRESS_TYPE_OFFSET(p) (BLOCK_ROW_SIZE_OFFSET(p) + sizeof(int32_t))
#define BLOCK_CHECKSUM_OFFSET(p)      (BLOCK_COMPRESS_TYPE_OFFSET(p) + sizeof(int8_t))
#define BLOCK_TOTAL_SIZE(p)           (BLOCK_CHECKSUM_OFFSET(p) + sizeof(TSCKSUM))

#define BLOCK_SET_ROW_SIZE(p, size) *(int32_t *)((char *)(p) + BLOCK_ROW_SIZE_OFFSET(p)) = (size)
#define BLOCK_GET_ROW_SIZE(p)       *(int32_t *)((char *)(p) + BLOCK_ROW_SIZE_OFFSET(p))

#define BLOCK_SET_COMPRESS_TYPE(p, type) *(int8_t *)((char *)(p) + BLOCK_COMPRESS_TYPE_OFFSET(p)) = (type)
#define BLOCK_GET_COMPRESS_TYPE(p)       *(int8_t *)((char *)(p) + BLOCK_COMPRESS_TYPE_OFFSET(p))

#define BLOCK_TAIL_LEN (sizeof(int32_t) + sizeof(int8_t) + sizeof(TSCKSUM))

#define COMREPSS_DATA_SET_TYPE_AND_RAWLEN(p, len, type, rawLen) \
  do {                                                          \
    *(int32_t *)((char *)(p) + len) = (rawLen);                 \
    *(int8_t *)((char *)(p) + len + sizeof(int32_t)) = (type);  \
  } while (0);
#define COMPRESS_DATA_GET_TYPE_AND_RAWLEN(p, len, type, rawLen)                 \
  do {                                                                          \
    (rawLen) = *(int32_t *)((char *)(p) + len - BLOCK_TAIL_LEN);                \
    (type) = *(int8_t *)((char *)(p) + len - BLOCK_TAIL_LEN + sizeof(int32_t)); \
  } while (0);

int32_t tableBuilderSeek(STableBuilder *p, SBlkHandle *pHandle, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlockWrapper blockWrapper = {0};

  code = tableBuilderLoadBlock(p, pHandle, &blockWrapper);
  TSDB_CHECK_CODE(code, lino, _error);

  code = blockSeek(blockWrapper.data, seq, pValue, len);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to seek data from table builder at lino %d ince %s", lino, tstrerror(code));
  }
  blockWrapperCleanup(&blockWrapper);
  return code;
}

int32_t tableBuilderLoadBlock(STableBuilder *p, SBlkHandle *pHandle, SBlockWrapper *pBlkWrapper) {
  int32_t code = 0;
  int32_t lino = 0;
  code = blockWrapperInit(pBlkWrapper, pHandle->size);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableLoadBlock(p->pDataFile, pHandle, pBlkWrapper);
_error:
  if (code != 0) {
    bseError("failed to load block from table builder at lino %d since %s", lino, tstrerror(code));
  }
  return code;
}

int32_t tableBuilderOpen(int64_t ts, STableBuilder **pBuilder, SBse *pBse) {
  int32_t code = 0;
  int32_t lino = 0;

  char name[TSDB_FILENAME_LEN] = {0};
  char path[TSDB_FILENAME_LEN] = {0};
  bseBuildDataName(ts, name);
  bseBuildFullName(pBse, name, path);

  STableBuilder *p = taosMemoryCalloc(1, sizeof(STableBuilder));
  if (p == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }
  p->timestamp = ts;
  memcpy(p->name, name, strlen(name));

  code = bseMemTableCreate(&p->pMemTable, BSE_BLOCK_SIZE(pBse));
  p->blockCap = BSE_BLOCK_SIZE(pBse);

  p->compressType = BSE_COMPRESS_TYPE(pBse);
  TSDB_CHECK_CODE(code, lino, _error);

  seqRangeReset(&p->tableRange);
  seqRangeReset(&p->blockRange);

  p->pBse = pBse;
  code = tableOpenFile(path, 0, &p->pDataFile, &p->offset);

  *pBuilder = p;
  p->pMemTable->pTableBuilder = p;

_error:
  if (code != 0) {
    (void)tableBuilderClose(p, 0);
    bseError("failed to open table builder at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

int32_t tableBuilderGetMetaBlock(STableBuilder *p, SArray **pMetaBlock) {
  return bseMemTablGetMetaBlock(p->pImmuMemTable, pMetaBlock);
}

int32_t tableBuilderAddMeta(STableBuilder *p, SBlkHandle *pHandle, int8_t immu) {
  int32_t         code = 0;
  int32_t         lino = 0;
  STableMemTable *pMemTable = immu ? p->pImmuMemTable : p->pMemTable;
  code = bseMemTableRef(pMemTable);
  TAOS_CHECK_GOTO(code, &lino, _error);

  code = bseMemTablePush(pMemTable, pHandle);
  TAOS_CHECK_GOTO(code, &lino, _error);

  seqRangeReset(&pMemTable->range);
_error:
  bseMemTableUnRef(pMemTable);
  return code;
}
int32_t tableBuilderSetBlockInfo(STableMemTable *pMemTable) {
  int32_t        code = 0;
  SBlockWrapper *pWp = &pMemTable->pBlockWrapper;
  SBlock        *pBlock = (SBlock *)pWp->data;

  pBlock->offset = pBlock->len;
  memcpy(pBlock->data + pBlock->len, pWp->kvBuffer, pWp->kvSize);
  pBlock->len += pWp->kvSize;
  pBlock->version = BSE_DATA_VER;

  return code;
}
int32_t tableBuilderFlush(STableBuilder *p, int8_t type, int8_t immutable) {
  int32_t code = 0;
  int32_t lino = 0;

  STableMemTable *pMemTable = immutable ? p->pImmuMemTable : p->pMemTable;
  if (p == NULL) return code;

  SBlockWrapper wrapper = {0};
  code = bseMemTableRef(pMemTable);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableBuilderSetBlockInfo(pMemTable);
  TSDB_CHECK_CODE(code, lino, _error);

  SBlock *pBlk = pMemTable->pBlockWrapper.data;
  if (pBlk->len == 0) {
    bseDebug("no data to flush for table %s", p->name);
    bseMemTableUnRef(pMemTable);
    return 0;
  }

  int8_t compressType = BSE_COMPRESS_TYPE(p->pBse);

  uint8_t *pWrite = (uint8_t *)pBlk;
  int32_t  len = BLOCK_TOTAL_SIZE(pBlk);

  pBlk->type = type;

  BLOCK_SET_COMPRESS_TYPE(pBlk, compressType);
  BLOCK_SET_ROW_SIZE(pBlk, BLOCK_ROW_SIZE(pBlk));

  if (compressType != kNoCompres) {
    code = blockWrapperInit(&wrapper, len + 16);
    TSDB_CHECK_CODE(code, lino, _error);

    int32_t compressSize = wrapper.cap;
    code = bseCompressData(compressType, pWrite, BLOCK_ROW_SIZE(pBlk), wrapper.data, &compressSize);
    if (code != 0) {
      bseWarn("failed to compress data since %s, not set compress", tstrerror(TSDB_CODE_THIRDPARTY_ERROR));

      blockWrapperCleanup(&wrapper);
      BLOCK_SET_COMPRESS_TYPE(pBlk, kNoCompres);
      BLOCK_SET_ROW_SIZE(pBlk, BLOCK_ROW_SIZE(pBlk));
    } else {
      int32_t rawSize = BLOCK_ROW_SIZE(pBlk);
      COMREPSS_DATA_SET_TYPE_AND_RAWLEN(wrapper.data, compressSize, compressType, rawSize);
      len = compressSize + BLOCK_TAIL_LEN;

      pWrite = (uint8_t *)wrapper.data;
    }
  }

  code = taosCalcChecksumAppend(0, (uint8_t *)pWrite, len);
  TSDB_CHECK_CODE(code, lino, _error);

  SBlkHandle handle = {.size = len, .offset = p->offset, .range = pMemTable->range};

  bseDebug("bse flush at offset %" PRId64 " len: %d, block range sseq:%" PRId64 ", eseq:%" PRId64 "", p->offset, len,
           handle.range.sseq, handle.range.eseq);

  int64_t n = taosLSeekFile(p->pDataFile, handle.offset, SEEK_SET);
  if (n < 0) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  int64_t nwrite = taosWriteFile(p->pDataFile, (uint8_t *)pWrite, len);
  if (nwrite != len) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _error);
  }
  p->offset += len;

  code = tableBuilderAddMeta(p, &handle, immutable);

_error:
  if (code != 0) {
    bseError("failed to flush table builder at line %d since %s", lino, tstrerror(code));
  }

  if (pMemTable != NULL) {
    blockWrapperClear(&pMemTable->pBlockWrapper);
    bseMemTableUnRef(pMemTable);
  }
  blockWrapperCleanup(&wrapper);
  return code;
}

void tableBuildUpdateTableRange(STableBuilder *p, SBlockItemInfo *pInfo) {
  SSeqRange range = {.sseq = pInfo->seq, .eseq = pInfo->seq};
  seqRangeUpdate(&p->tableRange, &range);
}

void tableBuilderUpdateBlockRange(STableBuilder *p, SBlockItemInfo *pInfo) {
  SSeqRange range = {.sseq = pInfo->seq, .eseq = pInfo->seq};
  seqRangeUpdate(&p->blockRange, &range);
}
void memtableUpdateBlockRange(STableMemTable *p, SBlockItemInfo *pInfo) {
  SSeqRange range = {.sseq = pInfo->seq, .eseq = pInfo->seq};
  seqRangeUpdate(&p->range, &range);
}

// table block data
// data1 data2 data3 data4 k1v1 k2v2, k3,v3 compresss size raw_size
//|seq len value|seq len value| seq len value| seq len value|
int32_t tableBuilderPut(STableBuilder *p, SBseBatch *pBatch) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t len = 0, offset = 0;

  code = bseMemTableRef(p->pMemTable);
  if (code != 0) {
    return code;
  }

  SBlockWrapper *pBlockWrapper = &p->pMemTable->pBlockWrapper;

  for (int32_t i = 0; i < taosArrayGetSize(pBatch->pSeq);) {
    SBlockItemInfo *pInfo = taosArrayGet(pBatch->pSeq, i);
    if (i == 0 || i == taosArrayGetSize(pBatch->pSeq) - 1) {
      tableBuildUpdateTableRange(p, pInfo);
      memtableUpdateBlockRange(p->pMemTable, pInfo);
    }

    if (atomic_load_8(&p->hasImmuMemTable) ||
        (blockWrapperSize(pBlockWrapper, len + pInfo->size) < tableBuilderGetBlockSize(p))) {
      i++;
      len += pInfo->size;
      tableBuilderUpdateBlockRange(p, pInfo);
      memtableUpdateBlockRange(p->pMemTable, pInfo);
      code = blockWrapperPushMeta(pBlockWrapper, pInfo->seq, NULL, pInfo->size);

      bseTrace("start to insert  bse table builder mem %p, idx %d", p->pMemTable, i);
      continue;
    } else {
      if (len > 0) {
        offset += blockAppendBatch(pBlockWrapper->data, pBatch->buf + offset, len);
      }
      bseTrace("start to flush bse table builder mem %p", p->pMemTable);
      code = tableBuilderFlush(p, BSE_TABLE_DATA_TYPE, 0);
      TSDB_CHECK_CODE(code, lino, _error);
      len = 0;

      code = blockWrapperPushMeta(pBlockWrapper, pInfo->seq, NULL, pInfo->size);
      TSDB_CHECK_CODE(code, lino, _error);
    }
  }

  if (offset < pBatch->len) {
    int32_t size = pBatch->len - offset;
    if (size > 0) {
      code = blockWrapperResize(pBlockWrapper,
                                size + BLOCK_TOTAL_SIZE((SBlock *)(pBlockWrapper->data)) + pBlockWrapper->kvSize);
      TSDB_CHECK_CODE(code, lino, _error);
    }

    if (blockAppendBatch(pBlockWrapper->data, pBatch->buf + offset, size) != size) {
      code = TSDB_CODE_INVALID_PARA;
    }
  }
_error:
  if (code != 0) {
    bseError("failed to append batch since %s", tstrerror(code));
  }

  bseMemTableUnRef(p->pMemTable);
  return code;
}

int32_t tableBuilderTruncFile(STableBuilder *p, int64_t size) {
  int32_t code = 0;
  int32_t lino = 0;

  if (p->pDataFile == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  code = taosFtruncateFile(p->pDataFile, size);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to truncate file since %s", tstrerror(code));
  }
  return code;
}

int32_t compareFunc(const void *pLeft, const void *pRight) {
  SBlkHandle *p1 = (SBlkHandle *)pLeft;
  SBlkHandle *p2 = (SBlkHandle *)pRight;
  if (p1->range.sseq > p2->range.sseq) {
    return 1;
  } else if (p1->range.sseq < p2->range.sseq) {
    return -1;
  }
  return 0;
}
int32_t findTargetBlock(SArray *pMetaHandle, int64_t seq) {
  SBlkHandle handle = {.range = {.sseq = seq, .eseq = seq}};
  return taosArraySearchIdx(pMetaHandle, &handle, compareFunc, TD_LE);
}

int32_t findInMemtable(STableMemTable *p, int64_t seq, uint8_t **value, int32_t *len) {
  int32_t code = 0;
  if (p == NULL) {
    return TSDB_CODE_NOT_FOUND;
  }

  SBlkHandle *pHandle = NULL;
  code = bseMemTableRef(p);

  if (taosArrayGetSize(p->pMetaHandle) > 0) {
    pHandle = taosArrayGetLast(p->pMetaHandle);
    if (!seqRangeIsGreater(&pHandle->range, seq)) {
      int32_t idx = findTargetBlock(p->pMetaHandle, seq);
      if (idx < 0) {
        return TSDB_CODE_NOT_FOUND;
      }
      pHandle = taosArrayGet(p->pMetaHandle, idx);
      code = tableBuilderSeek(p->pTableBuilder, pHandle, seq, value, len);
    }
  } else {
    code = blockWrapperSeek(&p->pBlockWrapper, seq, value, len);
  }
_error:
  bseMemTableUnRef(p);
  return code;
}
int32_t tableBuilderGet(STableBuilder *p, int64_t seq, uint8_t **value, int32_t *len) {
  int32_t code = 0;
  if (p == NULL) {
    return TSDB_CODE_NOT_FOUND;
  }

  code = findInMemtable(p->pMemTable, seq, value, len);
  if (code != 0) {
    code = findInMemtable(p->pImmuMemTable, seq, value, len);
  }
  return code;
}

static void updateTableRange(SBTableMeta *pTableMeta, SArray *pMetaBlock) {
  if (pMetaBlock == NULL) {
    return;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pMetaBlock); i++) {
    SMetaBlock *pMeta = taosArrayGet(pMetaBlock, i);
    seqRangeUpdate(&pTableMeta->range, &pMeta->range);
  }
}
static int32_t tableBuilderClearImmuMemTable(STableBuilder *p) {
  int32_t code = 0;
  taosThreadRwlockWrlock(&p->pBse->rwlock);
  atomic_store_8(&p->hasImmuMemTable, 0);
  bseMemTableUnRef(p->pImmuMemTable);
  p->pImmuMemTable = NULL;

  taosThreadRwlockUnlock(&p->pBse->rwlock);
  return code;
}
static int32_t tableBuildeSwapMemTable(STableBuilder *p) {
  int32_t code = 0;
  taosThreadRwlockWrlock(&p->pBse->rwlock);
  p->pImmuMemTable = p->pMemTable;
  p->pMemTable = NULL;

  atomic_store_8(&p->hasImmuMemTable, 1);

  taosThreadRwlockUnlock(&p->pBse->rwlock);
  return code;
}

int32_t tableBuilderCommit(STableBuilder *p, SBseLiveFileInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  STableCommitInfo commitInfo = {0};
  SArray          *pMetaBlock = NULL;
  if (p == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  code = tableBuilderFlush(p, BSE_TABLE_DATA_TYPE, 1);
  TSDB_CHECK_CODE(code, lino, _error);

  code = taosFsyncFile(p->pDataFile);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableBuilderGetMetaBlock(p, &pMetaBlock);
  TSDB_CHECK_CODE(code, lino, _error);

  if (taosArrayGetSize(pMetaBlock) == 0) {
    bseDebug("no meta block to commit for table %s", p->name);
    return code;
  }

  tableBuilderClearImmuMemTable(p);

  code = tableMetaCommit(p->pTableMeta, pMetaBlock);
  TSDB_CHECK_CODE(code, lino, _error);

  updateTableRange(p->pTableMeta, pMetaBlock);

  pInfo->level = 0;
  pInfo->range = p->pTableMeta->range;
  pInfo->timestamp = p->timestamp;
  pInfo->size = p->offset;

_error:
  if (code != 0) {
    bseError("failed to commit table builder at line %d since %s ", lino, tstrerror(code));
  } else {
    bseInfo("succ to commit table %s", p->name);
  }
  taosArrayDestroy(pMetaBlock);
  return code;
}

int32_t tableBuilderGetBlockSize(STableBuilder *p) { return p->blockCap; }

void tableBuilderClose(STableBuilder *p, int8_t commited) {
  if (p == NULL) {
    return;
  }
  int32_t code = 0;

  bseMemTableUnRef(p->pMemTable);
  bseMemTableUnRef(p->pImmuMemTable);

  taosCloseFile(&p->pDataFile);
  taosMemoryFree(p);
}

static void addSnapshotMetaToBlock(SBlockWrapper *pBlkWrapper, SSeqRange range, int8_t fileType, int8_t blockType,
                                   int64_t timestamp) {
  SBseSnapMeta *pSnapMeta = pBlkWrapper->data;
  pSnapMeta->range = range;
  pSnapMeta->fileType = fileType;
  pSnapMeta->blockType = blockType;
  pSnapMeta->timestamp = timestamp;
  return;
}
static void updateSnapshotMeta(SBlockWrapper *pBlkWrapper, SSeqRange range, int8_t fileType, int8_t blockType,
                               int64_t timestamp) {
  SBseSnapMeta *pSnapMeta = (SBseSnapMeta *)pBlkWrapper->data;
  pSnapMeta->timestamp = timestamp;
  return;
}
int32_t tableReaderLoadRawBlock(STableReader *p, SBlkHandle *pHandle, SBlockWrapper *blkWrapper) {
  int32_t code = 0;
  int32_t lino = 0;

  code = blockWrapperResize(blkWrapper, pHandle->size + sizeof(SBseSnapMeta));
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableLoadRawBlock(p->pDataFile, pHandle, blkWrapper, 1);
  TSDB_CHECK_CODE(code, lino, _error);

  addSnapshotMetaToBlock(blkWrapper, p->range, BSE_TABLE_SNAP, BSE_TABLE_DATA_TYPE, p->timestamp);

_error:
  if (code != 0) {
    bseError("table reader failed to load block at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

int32_t tableReaderLoadRawMeta(STableReader *p, SBlkHandle *pHandle, SBlockWrapper *blkWrapper) {
  int32_t code = 0;
  int32_t lino = 0;

  SBtableMetaReader *pReader = p->pMetaReader;

  code = blockWrapperResize(blkWrapper, pHandle->size + sizeof(SBseSnapMeta));
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableLoadRawBlock(pReader->pFile, pHandle, blkWrapper, 1);
  TSDB_CHECK_CODE(code, lino, _error);

  addSnapshotMetaToBlock(blkWrapper, p->range, BSE_TABLE_META_SNAP, BSE_TABLE_META_TYPE, p->timestamp);
_error:
  if (code != 0) {
    bseError("failed to load raw meta from table pReaderMgt at line %d lino since %s", lino, tstrerror(code));
  }
  return code;
}
int32_t tableReaderLoadRawMetaIndex(STableReader *p, SBlockWrapper *blkWrapper) {
  int32_t code = 0;
  int32_t lino = 0;

  SBtableMetaReader *pReader = p->pMetaReader;
  SBlkHandle        *pHandle = p->pMetaReader->footer.metaHandle;

  code = blockWrapperResize(blkWrapper, pHandle->size + sizeof(SBseSnapMeta));
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableLoadRawBlock(pReader->pFile, pHandle, blkWrapper, 1);
  TSDB_CHECK_CODE(code, lino, _error);

  addSnapshotMetaToBlock(blkWrapper, p->range, BSE_TABLE_META_SNAP, BSE_TABLE_META_INDEX_TYPE, p->timestamp);
_error:
  if (code != 0) {
    bseError("failed to load raw meta from table pReaderMgt at line %d lino since %s", lino, tstrerror(code));
  }
  return code;
}

int32_t tableReaderLoadRawFooter(STableReader *p, SBlockWrapper *blkWrapper) {
  int32_t code = 0;
  int32_t lino = 0;
  char    buf[kEncodeLen] = {0};

  SBtableMetaReader *pReader = p->pMetaReader;
  code = footerEncode(&pReader->footer, buf);
  int32_t len = sizeof(buf);

  int64_t n = taosLSeekFile(pReader->pFile, -kEncodeLen, SEEK_END);
  if (n < 0) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  if (taosReadFile(pReader->pFile, buf, sizeof(buf)) != len) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _error);
  }

  code = blockWrapperResize(blkWrapper, len + sizeof(SBseSnapMeta));
  TSDB_CHECK_CODE(code, lino, _error);

  memcpy((uint8_t *)blkWrapper->data + sizeof(SBseSnapMeta), buf, sizeof(buf));
  blkWrapper->size = len + sizeof(SBseSnapMeta);

  addSnapshotMetaToBlock(blkWrapper, p->range, BSE_TABLE_META_SNAP, BSE_TABLE_FOOTER_TYPE, p->timestamp);
_error:
  if (code != 0) {
    bseError("failed to load raw footer from table pReaderMgt at lino %d since %s", lino, tstrerror(code));
  }
  return code;
}

int32_t tableReaderOpen(int64_t timestamp, STableReader **pReader, void *pReaderMgt) {
  char data[TSDB_FILENAME_LEN] = {0};
  char meta[TSDB_FILENAME_LEN] = {0};

  char dataPath[TSDB_FILENAME_LEN] = {0};

  int32_t code = 0;
  int32_t lino = 0;
  int64_t size = 0;

  STableReaderMgt *pMgt = (STableReaderMgt *)pReaderMgt;
  if (pMgt == NULL) {
    return TSDB_CODE_INVALID_CFG;
  }

  SSubTableMgt *pMeta = pMgt->pMgt;

  STableReader *p = taosMemCalloc(1, sizeof(STableReader));
  if (p == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }
  p->timestamp = timestamp;
  p->blockCap = 1024;
  p->pReaderMgt = pReaderMgt;
  bseBuildDataName(timestamp, data);
  memcpy(p->name, data, strlen(data));

  bseBuildFullName(pMgt->pBse, data, dataPath);
  code = tableOpenFile(dataPath, 1, &p->pDataFile, &p->fileSize);
  TSDB_CHECK_CODE(code, lino, _error);

  code = blockWrapperInit(&p->blockWrapper, 1024);
  TSDB_CHECK_CODE(code, lino, _error);

  bseBuildMetaName(timestamp, meta);
  code = tableMetaReaderInit(pMeta->pTableMetaMgt->pTableMeta, meta, &p->pMetaReader);
  TSDB_CHECK_CODE(code, lino, _error);

  *pReader = p;

_error:
  if (code != 0) {
    tableReaderClose(p);
    bseError("failed to open table pReaderMgt at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

void tableReaderShouldPutToCache(STableReader *p, int8_t cache) { p->putInCache = cache; }

int32_t tableReaderGet(STableReader *p, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t    lino = 0;
  int32_t    code = 0;
  SMetaBlock block = {0};

  STableReaderMgt   *pMgt = (STableReaderMgt *)p->pReaderMgt;
  SBtableMetaReader *pMeta = p->pMetaReader;

  code = tableMetaReaderLoadBlockMeta(pMeta, seq, &block);
  TSDB_CHECK_CODE(code, lino, _error);

  SBlockWrapper wrapper = {0};
  SBlkHandle    blkhandle = {.offset = block.offset, .size = block.size, .range = block.range};

  SCacheItem *pItem = NULL;
  code = blockCacheGet(pMgt->pBlockCache, &blkhandle.range, (void **)&pItem);
  if (code != 0) {
    code = blockWrapperInit(&wrapper, block.size + 16);
    TSDB_CHECK_CODE(code, lino, _error);

    bseDebug("block size:%" PRId64 ", offset:%" PRId64 ", [sseq:%" PRId64 ", eseq:%" PRId64 "]", block.size,
             block.offset, block.range.sseq, block.range.eseq);

    code = tableLoadBlock(p->pDataFile, &blkhandle, &wrapper);
    if (code != 0) {
      blockWrapperCleanup(&wrapper);
      TSDB_CHECK_CODE(code, lino, _error);
    }

    SBlock *pBlock = wrapper.data;
    code = blockCachePut(pMgt->pBlockCache, &block.range, pBlock);
    TSDB_CHECK_CODE(code, lino, _error);

  } else {
    wrapper.data = pItem->pItem;
    wrapper.pCachItem = pItem;
  }

  code = blockSeek(wrapper.data, seq, pValue, len);
  TSDB_CHECK_CODE(code, lino, _error);

  if (wrapper.pCachItem != NULL) {
    bseCacheUnrefItem(wrapper.pCachItem);
  }
  blockWrapperClearMeta(&wrapper);

_error:
  if (code != 0) {
    bseError("failed to get table reader data at line %d since %s", lino, tstrerror(code));
  }
  return code;
}
int32_t tableReaderGetMeta(STableReader *p, SArray **pMeta) {
  int32_t code = 0;
  int32_t lino = 0;

  SArray *pMetaHandle = taosArrayInit(128, sizeof(SBlkHandle));
  if (pMetaHandle == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  code = tableMetaReaderLoadAllDataHandle(p->pMetaReader, pMetaHandle);
  TSDB_CHECK_CODE(code, lino, _error);

  *pMeta = pMetaHandle;

_error:
  if (code != 0) {
    bseError("failed to get table reader meta at lino %d since %s", lino, tstrerror(code));
  }
  return code;
}

void tableReaderClose(STableReader *p) {
  if (p == NULL) return;
  int32_t code = 0;

  taosArrayDestroy(p->pMetaHandle);

  taosCloseFile(&p->pDataFile);
  tableMetaReaderClose(p->pMetaReader);
  blockWrapperCleanup(&p->blockWrapper);

  taosMemoryFree(p);
}

int32_t blockCreate(int32_t cap, SBlock **p) {
  int32_t code = 0;
  SBlock *t = taosMemCalloc(1, cap);
  if (t == NULL) {
    return terrno;
  }
  *p = t;
  return code;
}

int32_t blockEsimateSize(SBlock *p, int32_t extra) { return BLOCK_TOTAL_SIZE(p) + extra; }

int32_t blockWrapperSize(SBlockWrapper *p, int32_t extra) {
  if (p == NULL || p->data == NULL) {
    return 0;
  }

  return p->kvSize + blockEsimateSize(p->data, extra) + 12;
}
int32_t blockAppendBatch(SBlock *p, uint8_t *value, int32_t len) {
  int32_t  code = 0;
  int32_t  offset = 0;
  uint8_t *data = (uint8_t *)p->data + p->len;
  memcpy(data, value, len);
  p->len += len;
  return len;
}
int32_t blockPut(SBlock *p, int64_t seq, uint8_t *value, int32_t len) {
  int32_t  code = 0;
  uint8_t *data = (uint8_t *)p->data + p->len;

  int32_t offset = taosEncodeVariantI64((void **)&data, seq);
  offset += taosEncodeVariantI32((void **)&data, len);
  offset += taosEncodeBinary((void **)&data, value, len);
  p->len += len;
  return offset;
}
void blockClear(SBlock *p) {
  p->len = 0;
  p->type = 0;
  p->data[0] = 0;
}

int32_t blockSeek(SBlock *p, int64_t seq, uint8_t **pValue, int32_t *len) {
  int8_t  found = 0;
  int32_t code = 0;
  int32_t offset = 0;

  uint8_t *p1 = (uint8_t *)p->data + p->offset;
  uint8_t *p2 = p1;
  while (p2 - p1 < p->len) {
    int64_t k;
    int32_t v;
    p2 = taosDecodeVariantI64(p2, &k);
    p2 = taosDecodeVariantI32(p2, &v);
    if (seq == k) {
      *len = v;
      found = 1;

      *pValue = taosMemoryCalloc(1, v);
      memcpy(*pValue, (uint8_t *)p->data + offset, v);
      break;
    }
    offset += v;
  }
  if (found == 0) {
    code = TSDB_CODE_NOT_FOUND;
  }
  return code;
}

int32_t blockWrapperSeek(SBlockWrapper *p, int64_t tgt, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  if (p == NULL || p->data == NULL) {
    return TSDB_CODE_NOT_FOUND;
  }
  int32_t  offset = 0;
  uint8_t *p1 = p->kvBuffer;
  uint8_t *p2 = p1;
  while ((p2 - p1) < p->kvSize) {
    int64_t seq = 0;
    int32_t vlen = 0;
    p2 = taosDecodeVariantI64(p2, &seq);
    p2 = taosDecodeVariantI32(p2, &vlen);

    if (seq == tgt) {
      *len = vlen;
      *pValue = taosMemoryCalloc(1, vlen);
      if (*pValue == NULL) {
        return terrno;
      }
      uint8_t *pdata = (uint8_t *)p->data + offset;
      memcpy(*pValue, pdata, vlen);
      return 0;
    }
  }
  return code;
}

int8_t blockGetType(SBlock *p) { return p->type; }
void   blockDestroy(SBlock *pBlock) { taosMemoryFree(pBlock); }

int32_t metaBlockAddIndex(SBlock *p, SBlkHandle *pInfo) {
  int32_t  code = 0;
  uint8_t *data = (uint8_t *)p->data + p->len;
  int32_t  offset = blkHandleEncode(pInfo, (char *)data);
  p->len += offset;
  return offset;
}

int32_t blkHandleEncode(SBlkHandle *pHandle, char *buf) {
  char   *p = buf;
  int32_t tlen = 0;
  tlen += taosEncodeVariantU64((void **)&p, pHandle->offset);
  tlen += taosEncodeVariantU64((void **)&p, pHandle->size);
  tlen += taosEncodeVariantI64((void **)&p, pHandle->range.sseq);
  tlen += taosEncodeVariantI64((void **)&p, pHandle->range.eseq);
  return tlen;
}
int32_t blkHandleDecode(SBlkHandle *pHandle, char *buf) {
  char *p = buf;
  p = taosDecodeVariantU64(p, &pHandle->offset);
  p = taosDecodeVariantU64(p, &pHandle->size);
  p = taosDecodeVariantI64(p, &pHandle->range.sseq);
  p = taosDecodeVariantI64(p, &pHandle->range.eseq);
  return p - buf;
}

// | meta handle | index handle | padding | magic number high | magic number low |
int32_t footerEncode(STableFooter *pFooter, char *buf) {
  char   *p = buf;
  int32_t len = 0;
  len += blkHandleEncode(pFooter->metaHandle, p + len);
  len += blkHandleEncode(pFooter->indexHandle, p + len);

  p = buf + kEncodeLen - 8;
  taosEncodeFixedU32((void **)&p, kMagicNum);
  taosEncodeFixedU32((void **)&p, kMagicNum);
  return 0;
}
int32_t footerDecode(STableFooter *pFooter, char *buf) {
  int32_t  code = 0;
  char    *p = buf;
  char    *mp = buf + kEncodeLen - 8;
  uint32_t ml, mh;

  taosDecodeFixedU32(mp, &ml);
  taosDecodeFixedU32(mp + 4, &mh);
  if (ml != kMagicNum || mh != kMagicNum) {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  int32_t len = blkHandleDecode(pFooter->metaHandle, buf);
  if (len < 0) {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  len = blkHandleDecode(pFooter->indexHandle, buf + len);
  if (len < 0) {
    return TSDB_CODE_FILE_CORRUPTED;
  }
  return code;
}

int32_t blockSeekMeta(SBlock *pBlock, int64_t seq, SMetaBlock *pMeta) {
  int32_t  code = 0;
  int32_t  len = 0;
  uint8_t *p = (uint8_t *)pBlock->data;

  while (len < pBlock->len) {
    SMetaBlock meta = {0};
    int32_t    offset = metaBlockDecode(&meta, (char *)p);
    if (seqRangeContains(&meta.range, seq)) {
      memcpy(pMeta, &meta, sizeof(SMetaBlock));
      return 0;
    }
    len += offset;
    p += offset;
  }
  return TSDB_CODE_NOT_FOUND;
}
int32_t blockGetAllMeta(SBlock *pBlock, SArray *pMeta) {
  int32_t  code = 0;
  int32_t  len = 0;
  uint8_t *p = (uint8_t *)pBlock->data;

  while (len < pBlock->len) {
    SMetaBlock meta = {0};
    int32_t    offset = metaBlockDecode(&meta, (char *)p);
    if (taosArrayPush(pMeta, &meta) == NULL) {
      return terrno;
    }
    len += offset;
    p += offset;
  }

  return code;
}

int32_t metaBlockEncode(SMetaBlock *pMeta, char *buf) {
  char   *p = buf;
  int32_t len = 0;
  len += taosEncodeFixedI8((void **)&p, pMeta->type);
  len += taosEncodeFixedI8((void **)&p, pMeta->version);
  len += taosEncodeFixedI16((void **)&p, pMeta->reserve);
  len += taosEncodeVariantI64((void **)&p, pMeta->offset);
  len += taosEncodeVariantI64((void **)&p, pMeta->size);
  len += taosEncodeVariantI64((void **)&p, pMeta->range.sseq);
  len += taosEncodeVariantI64((void **)&p, pMeta->range.eseq);
  return len;
}
int32_t metaBlockDecode(SMetaBlock *pMeta, char *buf) {
  char   *p = buf;
  int32_t len = 0;
  p = taosDecodeFixedI8(p, &pMeta->type);
  p = taosDecodeFixedI8(p, &pMeta->version);
  p = taosDecodeFixedI16(p, &pMeta->reserve);
  p = taosDecodeVariantI64(p, &pMeta->offset);
  p = taosDecodeVariantI64(p, &pMeta->size);
  p = taosDecodeVariantI64(p, &pMeta->range.sseq);
  p = taosDecodeVariantI64(p, &pMeta->range.eseq);
  return p - buf;
}

int32_t metaBlockAdd(SBlock *p, SMetaBlock *pBlk) {
  int32_t  code = 0;
  uint8_t *data = (uint8_t *)p->data + p->len;
  int32_t  offset = metaBlockEncode(pBlk, (char *)data);
  p->len += offset;
  return offset;
}
int32_t metaBlockGet(SBlock *p, SMetaBlock *pBlk) {
  int32_t  code = 0;
  uint8_t *data = (uint8_t *)p->data + p->len;
  int32_t  offset = metaBlockDecode(pBlk, (char *)data);
  p->len += offset;
  return offset;
}

int32_t tableFlushBlock(TdFilePtr pFile, SBlkHandle *pHandle, SBlockWrapper *pBlkW, int32_t *nWrite) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlock *pBlk = pBlkW->data;
  if (pBlk->len == 0) {
    return 0;
  }
  pBlk->version = BSE_META_VER;
  int8_t compressType = kNoCompres;

  SBlockWrapper wrapper = {0};

  uint8_t *pWrite = (uint8_t *)pBlk;
  int32_t  len = BLOCK_TOTAL_SIZE(pBlk);

  BLOCK_SET_COMPRESS_TYPE(pBlk, compressType);
  BLOCK_SET_ROW_SIZE(pBlk, BLOCK_ROW_SIZE(pBlk));

  if (compressType != kNoCompres) {
    code = blockWrapperInit(&wrapper, len + 4);
    TSDB_CHECK_CODE(code, lino, _error);

    int32_t compressSize = wrapper.cap;
    code = bseCompressData(compressType, pWrite, BLOCK_ROW_SIZE(pBlk), wrapper.data, &compressSize);
    if (code != 0) {
      bseWarn("failed to compress data since %s, not set compress", tstrerror(TSDB_CODE_THIRDPARTY_ERROR));

      blockWrapperCleanup(&wrapper);
      BLOCK_SET_COMPRESS_TYPE(pBlk, kNoCompres);
      BLOCK_SET_ROW_SIZE(pBlk, BLOCK_ROW_SIZE(pBlk));
    } else {
      int32_t rawSize = BLOCK_ROW_SIZE(pBlk);
      COMREPSS_DATA_SET_TYPE_AND_RAWLEN(wrapper.data, compressSize, compressType, rawSize);
      len = compressSize + BLOCK_TAIL_LEN;

      pWrite = (uint8_t *)wrapper.data;
    }
  }

  code = taosCalcChecksumAppend(0, (uint8_t *)pWrite, len);
  TSDB_CHECK_CODE(code, lino, _error);

  int64_t n = taosLSeekFile(pFile, pHandle->offset, SEEK_SET);
  if (n < 0) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  int32_t nwrite = taosWriteFile(pFile, (uint8_t *)pWrite, len);
  if (nwrite != len) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _error);
  }
  *nWrite = nwrite;
  blockWrapperCleanup(&wrapper);
_error:
  if (code != 0) {
    bseError("failed to flush table builder at line %d since %s", lino, tstrerror(code));
  } else {
    bseDebug("flush at offset %" PRId64 ", size %d", pHandle->offset, len);
  }
  return code;
}
int32_t tableLoadBlock(TdFilePtr pFile, SBlkHandle *pHandle, SBlockWrapper *pBlkW) {
  int32_t code = 0;
  int32_t lino = 0;

  code = blockWrapperResize(pBlkW, pHandle->size + 16);
  TSDB_CHECK_CODE(code, lino, _error);

  SBlock  *pBlk = pBlkW->data;
  uint8_t *pRead = (uint8_t *)pBlk;

  SBlockWrapper pHelp = {0};

  int64_t n = taosLSeekFile(pFile, pHandle->offset, SEEK_SET);
  if (n < 0) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  int32_t nr = taosReadFile(pFile, pRead, pHandle->size);
  if (nr != pHandle->size) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }

  if (taosCheckChecksumWhole((uint8_t *)pRead, pHandle->size) != 1) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }
  uint8_t compressType = 0;
  int32_t rawSize = 0;

  COMPRESS_DATA_GET_TYPE_AND_RAWLEN(pRead, pHandle->size, compressType, rawSize);

  if (compressType != kNoCompres) {
    code = blockWrapperInit(&pHelp, rawSize);
    TSDB_CHECK_CODE(code, lino, _error);

    int32_t unCompressSize = pHelp.cap;
    code = bseDecompressData(compressType, pRead, pHandle->size - BLOCK_TAIL_LEN, pHelp.data, &unCompressSize);
    if (code != 0) {
      TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
    }

    SBlock *p = pHelp.data;
    if (BLOCK_ROW_SIZE_OFFSET(p) != unCompressSize) {
      TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
    }
    blockWrapperCleanup(pBlkW);

    blockWrapperTransfer(pBlkW, &pHelp);

  } else {
    if (pBlk->len != (pHandle->size - BLOCK_TAIL_LEN - sizeof(SBlock))) {
      TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
    }
  }
_error:
  if (code != 0) {
    bseError("failed to load block at lino %d since %s, read at offset %" PRId64 ", size:%" PRId64 "", lino,
             tstrerror(code), pHandle->offset, pHandle->size);
  } else {
    bseDebug("read at offset %" PRId64 ", size %" PRId64 "", pHandle->offset, pHandle->size);
  }

  blockWrapperCleanup(&pHelp);
  return code;
}
int32_t tableLoadRawBlock(TdFilePtr pFile, SBlkHandle *pHandle, SBlockWrapper *pBlkW, int8_t checkSum) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlock  *pBlk = pBlkW->data;
  uint8_t *pRead = (uint8_t *)pBlk + sizeof(SBseSnapMeta);

  int64_t n = taosLSeekFile(pFile, pHandle->offset, SEEK_SET);
  if (n < 0) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  int32_t nr = taosReadFile(pFile, pRead, pHandle->size);
  if (nr != pHandle->size) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }

  if (checkSum) {
    if (taosCheckChecksumWhole((uint8_t *)pRead, pHandle->size) != 1) {
      TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
    }
  }

  pBlkW->size = pHandle->size + sizeof(SBseSnapMeta);
_error:
  if (code != 0) {
    bseError("failed to load block at lino %d since %s", lino, tstrerror(code));
  }
  return code;
}

int8_t seqRangeContains(SSeqRange *p, int64_t seq) { return seq >= p->sseq && seq <= p->eseq; }

void seqRangeReset(SSeqRange *p) {
  p->sseq = -1;
  p->eseq = -1;
}

int8_t seqRangeIsGreater(SSeqRange *p, int64_t seq) { return seq > p->eseq; }

void seqRangeUpdate(SSeqRange *dst, SSeqRange *src) {
  if (dst->sseq == -1) {
    dst->sseq = src->sseq;
  }
  dst->eseq = src->eseq;
}

int32_t blockWrapperInit(SBlockWrapper *p, int32_t cap) {
  int32_t code = 0;
  int32_t lino = 0;
  p->data = taosMemoryCalloc(1, cap);
  if (p->data == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  p->kvSize = 0;
  p->kvCap = 128;
  p->kvBuffer = taosMemoryCalloc(1, p->kvCap);
  if (p->kvBuffer == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  SBlock *block = (SBlock *)p->data;
  block->offset = 0;
  block->version = 0;
  p->cap = cap;
_error:
  if (code != 0) {
    blockWrapperCleanup(p);
  }
  return code;
}
int32_t blockWrapperPushMeta(SBlockWrapper *p, int64_t seq, uint8_t *value, int32_t len) {
  int32_t code = 0;
  if ((p->kvSize + 12) > p->kvCap) {
    if (p->kvCap == 0) {
      p->kvCap = 128;
    } else {
      p->kvCap *= 2;
    }

    void *data = taosMemoryRealloc(p->kvBuffer, p->kvCap);
    if (data == NULL) {
      return terrno;
    }
    p->kvBuffer = data;
  }
  uint8_t *data = (uint8_t *)p->kvBuffer + p->kvSize;
  p->kvSize += taosEncodeVariantI64((void **)&data, seq);
  p->kvSize += taosEncodeVariantI32((void **)&data, len);
  return code;
}

void blockWrapperClearMeta(SBlockWrapper *p) {
  if (p->kvBuffer != NULL) {
    taosMemoryFree(p->kvBuffer);
  }
  p->kvSize = 0;
  p->kvCap = 0;
}

void blockWrapperCleanup(SBlockWrapper *p) {
  if (p->data != NULL) {
    taosMemoryFree(p->data);
    p->data = NULL;
  }
  p->kvSize = 0;
  taosMemoryFreeClear(p->kvBuffer);
  p->cap = 0;
}

void blockWrapperTransfer(SBlockWrapper *dst, SBlockWrapper *src) {
  if (dst == NULL || src == NULL) {
    return;
  }
  dst->data = src->data;
  dst->cap = src->cap;

  dst->kvBuffer = src->kvBuffer;
  dst->kvSize = src->kvSize;
  dst->kvCap = src->kvCap;

  src->kvBuffer = NULL;
  src->kvSize = 0;
  src->kvCap = 0;

  src->data = NULL;
  src->cap = 0;
}

int32_t blockWrapperResize(SBlockWrapper *p, int32_t newCap) {
  if (p->cap < newCap) {
    int32_t cap = p->cap;
    if (cap == 0) cap = 1024;
    while (cap < newCap) {
      cap = cap * 2;
    }
    void *data = taosMemoryRealloc(p->data, cap);
    if (data == NULL) {
      return terrno;
    }
    p->data = data;
    p->cap = cap;
  }
  return 0;
}

void blockWrapperClear(SBlockWrapper *p) {
  if (p->data == NULL) {
    return;
  }
  SBlock *block = (SBlock *)p->data;
  p->kvSize = 0;
  p->size = 0;
  blockClear(block);
}

void blockWrapperSetType(SBlockWrapper *p, int8_t type) {
  SBlock *block = (SBlock *)p->data;
  block->type = type;
}

int32_t tableReaderIterInit(int64_t timestamp, int8_t type, STableReaderIter **ppIter, SBse *pBse) {
  int32_t    code = 0;
  int32_t    lino = 0;
  STableMgt *pTableMgt = pBse->pTableMgt;

  STableReaderIter *p = taosMemCalloc(1, sizeof(STableReaderIter));
  if (p == NULL) {
    return terrno;
  }

  p->timestamp = timestamp;
  SSubTableMgt *retentionMgt = NULL;

  code = createSubTableMgt(timestamp, 1, pBse->pTableMgt, &retentionMgt);
  TSDB_CHECK_CODE(code, lino, _error);

  p->pSubMgt = retentionMgt;

  code = tableReaderOpen(timestamp, &p->pTableReader, retentionMgt->pReaderMgt);
  TSDB_CHECK_CODE(code, lino, _error);

  tableReaderShouldPutToCache(p->pTableReader, 0);

  p->blockIndex = 0;
  p->blockType = type;

  if (p->blockType == BSE_TABLE_DATA_TYPE) {
    code = tableReaderGetMeta(p->pTableReader, &p->pMetaHandle);
    TSDB_CHECK_CODE(code, lino, _error);

  } else if (p->blockType == BSE_TABLE_META_TYPE) {
    p->pMetaHandle = taosArrayInit(8, sizeof(SBlkHandle));
    if (p->pMetaHandle == NULL) {
      TSDB_CHECK_CODE(code = terrno, lino, _error);
    }
    code = tableMetaReaderLoadMetaHandle(p->pTableReader->pMetaReader, p->pMetaHandle);
  } else {
    p->isOver = 1;
  }
  *ppIter = p;

_error:
  if (code != 0) {
    bseError("failed to init table reader iter since %s", tstrerror(code));
    tableReaderIterDestroy(p);
  }
  return code;
}

int32_t tableReaderIterNext(STableReaderIter *pIter, uint8_t **pValue, int32_t *len) {
  int32_t      code = 0;
  int32_t      lino = 0;
  SBseSnapMeta snapMeta = {0};
  snapMeta.range.sseq = -1;
  snapMeta.range.eseq = -1;
  snapMeta.timestamp = pIter->timestamp;
  snapMeta.fileType = pIter->fileType;
  snapMeta.blockType = pIter->blockType;

  if (pIter->blockType == BSE_TABLE_DATA_TYPE) {
    SBlkHandle *pHandle = NULL;
    if (pIter->blockIndex >= taosArrayGetSize(pIter->pMetaHandle)) {
      taosArrayDestroy(pIter->pMetaHandle);
      pIter->pMetaHandle = NULL;
      pIter->blockIndex = 0;
      pIter->isOver = 1;
      return 0;
    } else {
      pHandle = taosArrayGet(pIter->pMetaHandle, pIter->blockIndex);
      bseDebug("file type %d, block type: %d,block index %d, offset %" PRId64 ", size %" PRId64 ", range [%" PRId64
               ", %" PRId64 "]",
               pIter->fileType, pIter->blockType, pIter->blockIndex, pHandle->offset, pHandle->size,
               pHandle->range.sseq, pHandle->range.eseq);
      code = tableReaderLoadRawBlock(pIter->pTableReader, pHandle, &pIter->blockWrapper);
      TSDB_CHECK_CODE(code, lino, _error);

      pIter->blockIndex++;
    }

  } else if (pIter->blockType == BSE_TABLE_META_TYPE) {
    SBlkHandle *pHandle = NULL;
    if (pIter->blockIndex >= taosArrayGetSize(pIter->pMetaHandle)) {
      taosArrayDestroy(pIter->pMetaHandle);
      pIter->pMetaHandle = NULL;
      pIter->blockIndex = 0;
      pIter->blockType = BSE_TABLE_META_INDEX_TYPE;
    } else {
      pHandle = taosArrayGet(pIter->pMetaHandle, pIter->blockIndex);

      bseDebug("file type %d, block type: %d,block index %d, offset %" PRId64 ", size %" PRId64 ", range [%" PRId64
               ", %" PRId64 "]",
               pIter->fileType, pIter->blockType, pIter->blockIndex, pHandle->offset, pHandle->size,
               pHandle->range.sseq, pHandle->range.eseq);
      code = tableReaderLoadRawMeta(pIter->pTableReader, pHandle, &pIter->blockWrapper);
      TSDB_CHECK_CODE(code, lino, _error);
      pIter->blockIndex++;
    }
  }

  if (pIter->blockType == BSE_TABLE_META_INDEX_TYPE) {
    code = tableReaderLoadRawMetaIndex(pIter->pTableReader, &pIter->blockWrapper);
    TSDB_CHECK_CODE(code, lino, _error);

    pIter->blockType = BSE_TABLE_FOOTER_TYPE;
  } else if (pIter->blockType == BSE_TABLE_FOOTER_TYPE) {
    code = tableReaderLoadRawFooter(pIter->pTableReader, &pIter->blockWrapper);
    TSDB_CHECK_CODE(code, lino, _error);

    pIter->blockType = BSE_TABLE_END_TYPE;
  } else if (pIter->blockType == BSE_TABLE_END_TYPE) {
    pIter->isOver = 1;
  }

_error:
  if (code != 0) {
    bseError("failed to load block since %s", tstrerror(code));
    pIter->isOver = 1;
  }
  SSeqRange range = {0};
  if (pIter->blockWrapper.data != NULL) {
    updateSnapshotMeta(&pIter->blockWrapper, range, pIter->fileType, pIter->blockType, snapMeta.timestamp);
    *pValue = pIter->blockWrapper.data;
    *len = pIter->blockWrapper.size;
  }
  return code;
}

int8_t tableReaderIterValid(STableReaderIter *pIter) { return pIter->isOver == 0; }

int32_t bseReadCurrentSnap(SBse *pBse, uint8_t **pValue, int32_t *len) {
  int32_t   code = 0;
  char      path[128] = {0};
  int32_t   lino = 0;
  TdFilePtr fd = NULL;
  int64_t   sz = 0;
  char      name[TSDB_FILENAME_LEN] = {0};

  uint8_t *pCurrent = NULL;

  bseBuildCurrentName(pBse, name);
  if (taosCheckExistFile(name) == 0) {
    bseInfo("vgId:%d, no current meta file found, skip recover", BSE_VGID(pBse));
    return 0;
  }
  code = taosStatFile(name, &sz, NULL, NULL);
  TSDB_CHECK_CODE(code, lino, _error);

  fd = taosOpenFile(name, TD_FILE_READ);
  if (fd == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }
  pCurrent = (uint8_t *)taosMemoryCalloc(1, sizeof(SBseSnapMeta) + sz);
  if (pCurrent == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  int64_t nread = taosReadFile(fd, pCurrent + sizeof(SBseSnapMeta), sz);
  if (nread != sz) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }
  taosCloseFile(&fd);

  SBseSnapMeta *pMeta = (SBseSnapMeta *)(pCurrent);
  pMeta->fileType = BSE_CURRENT_SNAP;

  *pValue = pCurrent;

  *len = sz + sizeof(SBseSnapMeta);
_error:
  if (code != 0) {
    bseError("vgId:%d, failed to read current at line %d since %s", BSE_VGID(pBse), lino, tstrerror(code));
    taosCloseFile(&fd);
    taosMemoryFree(pCurrent);
  }
  return code;
}

void tableReaderIterDestroy(STableReaderIter *pIter) {
  if (pIter == NULL) return;

  taosArrayDestroy(pIter->pMetaHandle);
  tableReaderClose(pIter->pTableReader);
  blockWrapperCleanup(&pIter->blockWrapper);
  destroySubTableMgt(pIter->pSubMgt);
  taosMemoryFree(pIter);
}

int32_t blockWithMetaInit(SBlock *pBlock, SBlockWithMeta **pMeta) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlockWithMeta *p = taosMemCalloc(1, sizeof(SBlockWithMeta));
  if (p == NULL) {
    return terrno;
  }
  p->pBlock = pBlock;
  p->pMeta = taosArrayInit(8, sizeof(SBlockIndexMeta));
  if (p->pMeta == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  uint8_t *p1 = (uint8_t *)pBlock->data;
  uint8_t *p2 = (uint8_t *)p1;
  while (p2 - p1 < pBlock->len) {
    int64_t         k;
    int32_t         vlen = 0;
    SBlockIndexMeta meta = {0};
    int32_t         offset = 0;
    p2 = taosDecodeVariantI64((void **)p2, &k);
    offset = p2 - p1;
    p2 = taosDecodeVariantI32((void **)p2, &vlen);

    meta.seq = k;
    meta.offset = offset;
    if (taosArrayPush(p->pMeta, &meta) == NULL) {
      TSDB_CHECK_CODE(code = terrno, lino, _error);
    }
    p2 += vlen;
  }

  *pMeta = p;
_error:
  if (code != 0) {
    bseError("failed to init block with meta since %s", tstrerror(code));
    blockWithMetaCleanup(p);
  }
  return code;
}

int32_t blockWithMetaCleanup(SBlockWithMeta *p) {
  if (p == NULL) return 0;
  taosArrayDestroy(p->pMeta);
  taosMemoryFree(p);
  return 0;
}

int comprareFunc(const void *pLeft, const void *pRight) {
  SBlockIndexMeta *p1 = (SBlockIndexMeta *)pLeft;
  SBlockIndexMeta *p2 = (SBlockIndexMeta *)pRight;
  if (p1->seq > p2->seq) {
    return 1;
  } else if (p1->seq < p2->seq) {
    return -1;
  }
  return 0;
}

int32_t blockWithMetaSeek(SBlockWithMeta *p, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t         code = 0;
  SBlockIndexMeta key = {.seq = seq, .offset = 0};
  int32_t         idx = taosArraySearchIdx(p->pMeta, &seq, comprareFunc, TD_EQ);
  if (idx < 0) {
    return TSDB_CODE_NOT_FOUND;
  }
  SBlockIndexMeta *pMeta = taosArrayGet(p->pMeta, idx);
  if (pMeta == NULL) {
    return TSDB_CODE_NOT_FOUND;
  }

  uint8_t *data = (uint8_t *)p->pBlock->data + pMeta->offset;

  data = taosDecodeVariantI32((void *)data, len);
  if (*len <= 0) {
    return TSDB_CODE_NOT_FOUND;
  }
  *pValue = taosMemCalloc(1, *len);
  if (*pValue == NULL) {
    return terrno;
  }
  memcpy(*pValue, data, *len);

  return code;
}

int32_t tableMetaOpen(char *name, SBTableMeta **pMeta, void *pMetaMgt) {
  int32_t code = 0;
  int32_t lino = 0;

  SBTableMeta *p = taosMemCalloc(1, sizeof(SBTableMeta));
  if (p == NULL) {
    TSDB_CHECK_CODE(code, lino, _error);
  }

  if (name != NULL) {
    memcpy(p->name, name, strlen(name) + 1);
  }
  p->pBse = ((STableMetaMgt *)pMetaMgt)->pBse;

  p->blockCap = BSE_BLOCK_SIZE((SBse *)p->pBse);

  *pMeta = p;
_error:
  if (code != 0) {
    bseError("failed to open table meta %s at line %d since %s", name, lino, tstrerror(code));
    tableMetaClose(p);
  }

  return code;
}

int32_t tableMetaCommit(SBTableMeta *pMeta, SArray *pBlock) {
  int32_t                code = 0;
  int32_t                lino = 0;
  SBtableMetaWriter     *pWriter = NULL;
  SBtableMetaReader     *pReader = NULL;
  SBtableMetaReaderIter *pIter = NULL;

  char tempMetaName[TSDB_FILENAME_LEN] = {0};
  char metaName[TSDB_FILENAME_LEN] = {0};

  char tempMetaPath[TSDB_FILENAME_LEN] = {0};
  char metaPath[TSDB_FILENAME_LEN] = {0};

  bseBuildTempMetaName(pMeta->timestamp, tempMetaName);
  bseBuildMetaName(pMeta->timestamp, metaName);

  code = tableMetaWriterInit(pMeta, tempMetaName, &pWriter);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableMetaReaderInit(pMeta, metaName, &pReader);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableMetaReaderOpenIter(pReader, &pIter);
  TSDB_CHECK_CODE(code, lino, _error);

  while (!pIter->isOver) {
    SBlkHandle    blkHandle = {0};
    SBlockWrapper wrapper;

    code = tableMetaReaderIterNext(pIter, &wrapper, &blkHandle);
    TSDB_CHECK_CODE(code, lino, _error);

    if (pIter->isOver) {
      break;
    }

    blockWrapperSetType(&wrapper, BSE_TABLE_META_TYPE);

    code = tableMetaWriteAppendRawBlock(pWriter, &wrapper, &blkHandle);
    TSDB_CHECK_CODE(code, lino, _error);

    seqRangeUpdate(&pMeta->range, &blkHandle.range);
  }

  code = tableMetaWriterAppendBlock(pWriter, pBlock);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableMetaWriterCommit(pWriter);
  TSDB_CHECK_CODE(code, lino, _error);

  tableMetaWriterClose(pWriter);
  tableMetaReaderClose(pReader);

  pWriter = NULL;
  pReader = NULL;

  bseBuildFullName(pMeta->pBse, tempMetaName, tempMetaPath);
  bseBuildFullName(pMeta->pBse, metaName, metaPath);

  code = taosRenameFile(tempMetaPath, metaPath);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to commit table meta %s at line %d since %s", pMeta->name, lino, tstrerror(code));
  }
  tableMetaReaderIterClose(pIter);
  tableMetaWriterClose(pWriter);
  tableMetaReaderClose(pReader);

  return code;
}
int32_t tableMetaWriterAppendBlock(SBtableMetaWriter *pMeta, SArray *pBlock) {
  int32_t code = 0;
  if (taosArrayAddAll(pMeta->pBlock, pBlock) == NULL) {
    return terrno;
  }
  return code;
}

int32_t tableMetaWriterFlushBlock(SBtableMetaWriter *pMeta) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SSeqRange range = {.sseq = -1, .eseq = -1};

  int64_t offset = 0;
  int32_t nWrite = 0;
  int32_t size = pMeta->blockCap;

  blockWrapperClear(&pMeta->blockWrapper);
  code = blockWrapperResize(&pMeta->blockWrapper, size);
  TSDB_CHECK_CODE(code, lino, _error);

  for (int32_t i = 0; i < taosArrayGetSize(pMeta->pBlock); i++) {
    SMetaBlock *pBlk = taosArrayGet(pMeta->pBlock, i);
    if (blockEsimateSize(pMeta->blockWrapper.data, sizeof(SMetaBlock)) >= pMeta->blockCap) {
      SBlkHandle handle = {.offset = pMeta->offset, .size = offset, .range = range};

      blockWrapperSetType(&pMeta->blockWrapper, BSE_TABLE_META_TYPE);

      code = tableFlushBlock(pMeta->pFile, &handle, &pMeta->blockWrapper, &nWrite);
      TSDB_CHECK_CODE(code, lino, _error);

      pMeta->offset += nWrite;
      handle.size = nWrite;

      blockWrapperClear(&pMeta->blockWrapper);
      code = blockWrapperResize(&pMeta->blockWrapper, size);
      TSDB_CHECK_CODE(code, lino, _error);

      if (taosArrayPush(pMeta->pBlkHandle, &handle) == NULL) {
        TSDB_CHECK_CODE(code = terrno, lino, _error);
      }
      range.sseq = -1;
      offset = 0;
    }

    offset += metaBlockAdd(pMeta->blockWrapper.data, pBlk);

    if (range.sseq == -1) {
      range.sseq = pBlk->range.sseq;
    }
    range.eseq = pBlk->range.eseq;
  }
  if (offset == 0) {
    return 0;
  }

  blockWrapperSetType(&pMeta->blockWrapper, BSE_TABLE_META_TYPE);

  SBlkHandle handle = {.offset = pMeta->offset, .size = offset, .range = range};
  code = tableFlushBlock(pMeta->pFile, &handle, &pMeta->blockWrapper, &nWrite);
  TSDB_CHECK_CODE(code, lino, _error);

  pMeta->offset += nWrite;
  handle.size = nWrite;

  if (taosArrayPush(pMeta->pBlkHandle, &handle) == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }
_error:
  if (code != 0) {
    bseError("failed to flush table meta %s at line %d since %s", pMeta->name, 0, tstrerror(code));
    tableMetaWriterClose(pMeta);
  }
  return code;
}

int32_t tableMetaWriterFlushIndex(SBtableMetaWriter *pMeta) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t nWrite = 0;
  int64_t lastOffset = pMeta->offset;
  int32_t blkHandleSize = 0;

  int32_t extra = 8;
  int32_t size = taosArrayGetSize(pMeta->pBlkHandle) * sizeof(SBlkHandle);

  SSeqRange range = {-1, -1};

  blockWrapperClear(&pMeta->blockWrapper);
  code = blockWrapperResize(&pMeta->blockWrapper, size + extra);
  TSDB_CHECK_CODE(code, lino, _error);

  for (int32_t i = 0; i < taosArrayGetSize(pMeta->pBlkHandle); i++) {
    SBlkHandle *pHandle = taosArrayGet(pMeta->pBlkHandle, i);
    if (pHandle == NULL) {
      TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
    }
    blkHandleSize += metaBlockAddIndex(pMeta->blockWrapper.data, pHandle);

    seqRangeUpdate(&range, &pHandle->range);
  }

  blockWrapperSetType(&pMeta->blockWrapper, BSE_TABLE_META_INDEX_TYPE);

  SBlkHandle handle = {.offset = lastOffset, .size = blkHandleSize, .range = range};
  code = tableFlushBlock(pMeta->pFile, &handle, &pMeta->blockWrapper, &nWrite);
  TSDB_CHECK_CODE(code, lino, _error);

  SBlkHandle metaHandle = {.offset = pMeta->offset, .size = nWrite, .range = range};
  SBlkHandle indexHandle = {.offset = pMeta->offset + nWrite, .size = 0, .range = range};
  pMeta->offset += nWrite;

  memcpy(pMeta->footer.metaHandle, &metaHandle, sizeof(SBlkHandle));
  memcpy(pMeta->footer.indexHandle, &metaHandle, sizeof(SBlkHandle));
_error:
  if (code != 0) {
    bseError("failed to build table meta index at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

int32_t tableMetaWriterFlushFooter(SBtableMetaWriter *p) {
  char buf[kEncodeLen] = {0};

  int32_t code = 0;
  int32_t lino = 0;

  code = footerEncode(&p->footer, buf);
  TSDB_CHECK_CODE(code, lino, _error);

  p->offset += sizeof(buf);

  int32_t nwrite = taosWriteFile(p->pFile, buf, sizeof(buf));
  if (nwrite != sizeof(buf)) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _error);
  }

_error:
  if (code != 0) {
    bseError("failed to add footer to table builder at line %d since %s", lino, tstrerror(code));
  }
  return code;
}
int32_t tableMetaWriterCommit(SBtableMetaWriter *pMeta) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tableMetaWriterFlushBlock(pMeta);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableMetaWriterFlushIndex(pMeta);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableMetaWriterFlushFooter(pMeta);
  TSDB_CHECK_CODE(code, lino, _error);
_error:
  if (code != 0) {
    bseError("failed to commit table meta %s at line %d since %s", pMeta->name, lino, tstrerror(code));
    tableMetaWriterClose(pMeta);
  }
  return code;
}
int32_t tableMetaWriteAppendRawBlock(SBtableMetaWriter *pMeta, SBlockWrapper *pBlock, SBlkHandle *pBlkHandle) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t nwrite = 0;
  code = tableFlushBlock(pMeta->pFile, pBlkHandle, pBlock, &nwrite);
  TSDB_CHECK_CODE(code, lino, _error);

  SBlkHandle handle = {.offset = pMeta->offset, .size = nwrite, .range = pBlkHandle->range};
  if (taosArrayPush(pMeta->pBlkHandle, &handle) == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }
  pMeta->offset += nwrite;
_error:
  if (code != 0) {
    bseError("failed to append block to table meta %s at line %d since %s", pMeta->name, lino, tstrerror(code));
    tableMetaWriterClose(pMeta);
  }
  return code;
}

int32_t tableMetaReaderLoadFooter(SBtableMetaReader *pMeta) {
  int32_t code = 0;
  int32_t lino = 0;
  char    footer[kEncodeLen] = {0};

  if (pMeta->pFile == NULL) {
    return 0;
  }
  int64_t n = taosLSeekFile(pMeta->pFile, -kEncodeLen, SEEK_END);
  if (n < 0) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  if (taosReadFile(pMeta->pFile, footer, kEncodeLen) != kEncodeLen) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _error);
  }

  code = footerDecode(&pMeta->footer, footer);
  TSDB_CHECK_CODE(code, lino, _error);
_error:
  if (code != 0) {
    bseError("failed to load table meta footer %s at line %d since %s", pMeta->name, lino, tstrerror(code));
  }
  return code;
}

int32_t tableOpenFile(char *name, int8_t read, TdFilePtr *pFile, int64_t *size) {
  int32_t lino = 0;
  int32_t code = 0;
  int32_t opt = 0;

  TdFilePtr p = NULL;
  if (read) {
    opt = TD_FILE_READ;
  } else {
    opt = TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_APPEND;
  }

  if (!taosCheckExistFile(name)) {
    if (read) {
      return 0;
    }

    p = taosOpenFile(name, opt);
    if (p == NULL) {
      TSDB_CHECK_CODE(code = terrno, lino, _error);
    }

    *pFile = p;
    return code;
  }

  code = taosStatFile(name, size, NULL, NULL);
  TSDB_CHECK_CODE(code, lino, _error);
  if (*size <= 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_NOT_FOUND, lino, _error);
  }

  p = taosOpenFile(name, opt);
  if (p == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }
  *pFile = p;

_error:
  if (code != 0) {
    bseError("failed to open table meta %s at line %d since %s", name, lino, tstrerror(code));
  }
  return code;
}
int32_t tableMetaOpenFile(SBtableMetaWriter *pMeta, int8_t read, char *name) {
  int32_t code = 0;
  int64_t size = 0;
  int32_t lino = 0;

  code = tableOpenFile(name, read, &pMeta->pFile, &size);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to open table meta %s at line %d since %s", pMeta->name, lino, tstrerror(code));
  }

  return code;
}

int32_t tableMetaReaderLoad(SBtableMetaReader *pMeta) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tableMetaOpenFile(pMeta, 1, pMeta->name);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableMetaReaderLoadFooter(pMeta);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableMetaReaderLoadIndex(pMeta);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to load table meta %s at line %d since %s", pMeta->name, lino, tstrerror(code));
  }
  return code;
}

void tableMetaClose(SBTableMeta *p) {
  if (p == NULL) return;
  taosMemoryFree(p);
}

int32_t tableMetaWriterInit(SBTableMeta *pMeta, char *name, SBtableMetaWriter **ppWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  char path[TSDB_FILENAME_LEN] = {0};
  bseBuildFullName(pMeta->pBse, name, path);

  SBtableMetaWriter *p = taosMemCalloc(1, sizeof(SBtableMetaWriter));
  if (p == NULL) {
    return terrno;
  }
  p->pTableMeta = pMeta;

  p->blockCap = pMeta->blockCap;

  p->pBlkHandle = taosArrayInit(128, sizeof(SBlkHandle));
  if (p->pBlkHandle == NULL) {
    TSDB_CHECK_CODE(code, lino, _error);
  }

  p->pBlock = taosArrayInit(128, sizeof(SMetaBlock));
  if (p->pBlock == NULL) {
    TSDB_CHECK_CODE(code, lino, _error);
  }

  code = blockWrapperInit(&p->blockWrapper, 1024);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableMetaOpenFile(p, 0, path);
  TSDB_CHECK_CODE(code, lino, _error);

  *ppWriter = p;
_error:
  if (code != 0) {
    bseError("failed to init table meta writer %s at line %d since %s", pMeta->name, lino, tstrerror(code));
    tableMetaWriterClose(p);
  }
  return code;
}

void tableMetaWriterClose(SBtableMetaWriter *p) {
  if (p == NULL) return;
  taosCloseFile(&p->pFile);
  taosArrayDestroy(p->pBlkHandle);
  taosArrayDestroy(p->pBlock);
  blockWrapperCleanup(&p->blockWrapper);
  taosMemoryFree(p);
}

int32_t tableMetaReaderInit(SBTableMeta *pMeta, char *name, SBtableMetaReader **ppReader) {
  int32_t code = 0;
  int32_t lino = 0;
  char    path[TSDB_FILENAME_LEN] = {0};
  bseBuildFullName(pMeta->pBse, name, path);

  SBtableMetaReader *p = taosMemCalloc(1, sizeof(SBtableMetaReader));
  if (p == NULL) {
    return terrno;
  }
  memcpy(p->name, path, sizeof(path));
  p->pTableMeta = pMeta;

  p->pBlkHandle = taosArrayInit(128, sizeof(SBlkHandle));
  if (p->pBlkHandle == NULL) {
    TSDB_CHECK_CODE(code, lino, _error);
  }

  code = blockWrapperInit(&p->blockWrapper, 1024);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableMetaReaderLoad(p);
  TSDB_CHECK_CODE(code, lino, _error);

  *ppReader = p;
_error:
  if (code != 0) {
    bseError("failed to init table meta reader %s at line %d since %s", pMeta->name, lino, tstrerror(code));
    tableMetaReaderClose(p);
  }
  return code;
}

void tableMetaReaderClose(SBtableMetaReader *p) {
  if (p == NULL) return;
  taosCloseFile(&p->pFile);
  taosArrayDestroy(p->pBlkHandle);
  blockWrapperCleanup(&p->blockWrapper);
  taosMemoryFree(p);
}
int32_t tableMetaReaderLoadBlockMeta(SBtableMetaReader *p, int64_t seq, SMetaBlock *pMetaBlock) {
  int32_t            code = 0;
  int32_t            lino = 0;
  SBtableMetaReader *pMeta = p;
  SSeqRange          range = {.sseq = seq, .eseq = seq};

  SBlkHandle  handle = {.range = range};
  int32_t     index = taosArraySearchIdx(p->pBlkHandle, &handle, compareFunc, TD_LE);
  SBlkHandle *pHandle = taosArrayGet(p->pBlkHandle, index);
  if (pHandle == NULL) {
    return TSDB_CODE_NOT_FOUND;
  }

  code = tableLoadBlock(p->pFile, pHandle, &p->blockWrapper);
  TSDB_CHECK_CODE(code, lino, _error);

  code = blockSeekMeta(p->blockWrapper.data, seq, pMetaBlock);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  return code;
}
int32_t tableMetaReaderLoadAllDataHandle(SBtableMetaReader *p, SArray *dataHandle) {
  int32_t lino = 0;
  int32_t code = 0;
  SArray *pMeta = taosArrayInit(8, sizeof(SMetaBlock));
  for (int32_t i = 0; i < taosArrayGetSize(p->pBlkHandle); i++) {
    SBlkHandle *pHandle = taosArrayGet(p->pBlkHandle, i);
    if (pHandle == NULL) {
      TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
    }
    code = tableLoadBlock(p->pFile, pHandle, &p->blockWrapper);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = blockGetAllMeta(p->blockWrapper.data, pMeta);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (taosArrayGetSize(pMeta) == 0) {
      TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
    }
    for (int32_t j = 0; j < taosArrayGetSize(pMeta); j++) {
      SMetaBlock *pBlk = taosArrayGet(pMeta, j);
      SBlkHandle  handle = {.offset = pBlk->offset, .size = pBlk->size, .range = pBlk->range};
      if (taosArrayPush(dataHandle, &handle) == NULL) {
        TSDB_CHECK_CODE(code = terrno, lino, _exit);
      }
    }
  }
_exit:
  taosArrayDestroy(pMeta);
  return code;
}

int32_t tableMetaReaderLoadMetaHandle(SBtableMetaReader *p, SArray *pMetaHandle) {
  int32_t code = 0;
  int32_t lino = 0;

  if (taosArrayGetSize(p->pBlkHandle) == 0) {
    return TSDB_CODE_NOT_FOUND;
  }

  for (int32_t i = 0; i < taosArrayGetSize(p->pBlkHandle); i++) {
    SBlkHandle *pHandle = taosArrayGet(p->pBlkHandle, i);
    if (pHandle == NULL) {
      return TSDB_CODE_FILE_CORRUPTED;
    }
    if (taosArrayPush(pMetaHandle, pHandle) == NULL) {
      TSDB_CHECK_CODE(code = terrno, lino, _error);
    }
  }
_error:
  return code;
}

int32_t tableMetaReaderLoadIndex(SBtableMetaReader *p) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t offset = 0;

  if (p->pFile == NULL) {
    return 0;
  }
  SBtableMetaReader *pMeta = p;
  p->blockWrapper.type = BSE_TABLE_META_TYPE;

  code = tableLoadBlock(pMeta->pFile, pMeta->footer.metaHandle, &p->blockWrapper);
  TSDB_CHECK_CODE(code, lino, _error);

  if (blockGetType(p->blockWrapper.data) != BSE_TABLE_META_INDEX_TYPE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }

  SBlock  *pBlk = (SBlock *)p->blockWrapper.data;
  uint8_t *data = (uint8_t *)pBlk->data;
  do {
    SBlkHandle handle = {0};
    offset += blkHandleDecode(&handle, (char *)data + offset);
    if (taosArrayPush(pMeta->pBlkHandle, &handle) == NULL) {
      TSDB_CHECK_CODE(terrno, lino, _error);
    }
  } while (offset < pBlk->len);

_error:
  if (code != 0) {
    bseError("failed to load table meta blk handle %s at line %d since %s", pMeta->name, lino, tstrerror(code));
  }
  return code;
}

int32_t tableMetaReaderOpenIter(SBtableMetaReader *pReader, SBtableMetaReaderIter **pIter) {
  int32_t code = 0;
  int32_t lino = 0;

  SBtableMetaReaderIter *p = taosMemCalloc(1, sizeof(SBtableMetaReaderIter));
  if (p == NULL) {
    return terrno;
  }
  p->pReader = pReader;

  code = blockWrapperInit(&p->pBlockWrapper, 1024);
  if (code != 0) {
    return code;
  }

  *pIter = p;
  if (taosArrayGetSize(pReader->pBlkHandle) == 0) {
    p->isOver = 1;
    return 0;
  }

  return 0;
}

int32_t tableMetaReaderIterNext(SBtableMetaReaderIter *pIter, SBlockWrapper *pDataWrapper, SBlkHandle *dstHandle) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pIter->blkIdx >= taosArrayGetSize(pIter->pReader->pBlkHandle)) {
    pIter->isOver = 1;
    return 0;
  }

  SBlkHandle *pHandle = taosArrayGet(pIter->pReader->pBlkHandle, pIter->blkIdx);
  if (pHandle == NULL) {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  SBlockWrapper *pWrapper = &pIter->pBlockWrapper;
  code = blockWrapperResize(pWrapper, pHandle->size);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableLoadBlock(pIter->pReader->pFile, pHandle, pWrapper);
  TSDB_CHECK_CODE(code, lino, _error);

  pIter->blkIdx++;

  if (blockGetType(pWrapper->data) != BSE_TABLE_META_TYPE) {
    pIter->isOver = 1;
    return 0;
  }

  *pDataWrapper = *pWrapper;
  *dstHandle = *pHandle;

_error:
  if (code != 0) {
    bseError("failed to load table meta blk handle %s at line %d since %s", pIter->pReader->name, lino,
             tstrerror(code));
    pIter->pReader = NULL;
  }
  return code;
}

void tableMetaReaderIterClose(SBtableMetaReaderIter *p) {
  if (p == NULL) return;
  blockWrapperCleanup(&p->pBlockWrapper);
  taosMemoryFree(p);
}

int32_t bseMemTableCreate(STableMemTable **pMemTable, int32_t cap) {
  int32_t code = 0;
  int32_t lino = 0;

  STableMemTable *p = taosMemoryCalloc(1, sizeof(STableMemTable));
  if (p == NULL) {
    return terrno;
  }

  p->pMetaHandle = taosArrayInit(8, sizeof(SBlkHandle));
  if (p->pMetaHandle == NULL) {
    code = terrno;
    TAOS_CHECK_GOTO(code, &lino, _error);
  }

  code = blockWrapperInit(&p->pBlockWrapper, cap);
  TAOS_CHECK_GOTO(code, &lino, _error);

  seqRangeReset(&p->range);
  p->ref = 1;
  bseTrace("create mem table %p", p);

_error:
  if (code != 0) {
    bseMemTableDestroy(p);
  }
  *pMemTable = p;

  return code;
}

int32_t bseMemTableRef(STableMemTable *pMemTable) {
  int32_t code = 0;
  if (pMemTable == NULL) {
    return TSDB_CODE_INVALID_CFG;
  }
  SBse *pBse = (SBse *)pMemTable->pBse;
  bseTrace("ref mem table %p", pMemTable);
  int32_t nRef = atomic_fetch_add_32(&pMemTable->ref, 1);
  if (nRef <= 0) {
    bseError("vgId:%d, memtable ref count is invalid, ref:%d", BSE_VGID(pBse), nRef);
    return TSDB_CODE_INVALID_CFG;
  }
  return code;
}

void bseMemTableUnRef(STableMemTable *pMemTable) {
  int32_t code = 0;

  bseTrace("unref mem table %p", pMemTable);
  if (pMemTable == NULL) {
    return;
  }
  if (atomic_sub_fetch_32(&pMemTable->ref, 1) == 0) {
    bseMemTableDestroy(pMemTable);
    bseTrace("destroy mem table %p", pMemTable);
  }
}
void bseMemTableDestroy(STableMemTable *pMemTable) {
  if (pMemTable == NULL) return;
  taosArrayDestroy(pMemTable->pMetaHandle);
  blockWrapperCleanup(&pMemTable->pBlockWrapper);
  taosMemoryFree(pMemTable);
}
int32_t bseMemTablePush(STableMemTable *pMemTable, void *pHandle) {
  int32_t code = 0;
  if (pMemTable == NULL || pHandle == NULL) {
    code = TSDB_CODE_INVALID_PARA;
    return code;
  }
  if (taosArrayPush(pMemTable->pMetaHandle, pHandle) == NULL) {
    code = terrno;
    bseError("Failed to push handle to memtable since %s", tstrerror(code));
    return code;
  }
  return code;
}
int32_t bseMemTablGetMetaBlock(STableMemTable *p, SArray **pMetaBlock) {
  int32_t code = 0;
  SArray *pBlock = taosArrayInit(8, sizeof(SMetaBlock));
  if (pBlock == NULL) {
    return terrno;
  }
  for (int32_t i = 0; i < taosArrayGetSize(p->pMetaHandle); i++) {
    SBlkHandle *handle = taosArrayGet(p->pMetaHandle, i);
    SMetaBlock  block = {.type = BSE_TABLE_META_TYPE,
                         .version = BSE_DATA_VER,
                         .range = handle->range,
                         .offset = handle->offset,
                         .size = handle->size};

    if (taosArrayPush(pBlock, &block) == NULL) {
      taosArrayDestroy(pBlock);
      return terrno;
    }
  }
  *pMetaBlock = pBlock;
  return code;
}