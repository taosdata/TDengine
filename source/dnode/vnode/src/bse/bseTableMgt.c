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

#include "bseTableMgt.h"
#include "bse.h"
#include "bseTable.h"
#include "bseUtil.h"
#include "thash.h"

static int32_t tableReaderMgtInit(STableReaderMgt *pReader, SBse *pBse, int64_t retention);
static void    tableReaderMgtSetRetion(STableReaderMgt *pReader, int64_t retention);
static int32_t tableReaderMgtSeek(STableReaderMgt *pReaderMgt, int64_t seq, uint8_t **pValue, int32_t *len);
static int32_t tableReaderMgtClear(STableReaderMgt *pReader);
static void    tableReaderMgtDestroy(STableReaderMgt *pReader);

static int32_t tableBuilderMgtInit(STableBuilderMgt *pMgt, SBse *pBse, int64_t retention);
static void    tableBuilderMgtSetRetion(STableBuilderMgt *pMgt, int64_t retention);
static int32_t tableBuilderMgtGetBuilder(STableBuilderMgt *pMgt, int64_t seq, STableBuilder **p);
static int32_t tableBuilderMgtCommit(STableBuilderMgt *pMgt, SBseLiveFileInfo *pInfo);
static int32_t tableBuilderMgtSeek(STableBuilderMgt *pMgt, int64_t seq, uint8_t **pValue, int32_t *len);
static int32_t tableBuilderMgtPutBatch(STableBuilderMgt *pMgt, SBseBatch *pBatch);
static int32_t tableBuilderMgtClear(STableBuilderMgt *pMgt);
static void    tableBuilderMgtDestroy(STableBuilderMgt *pMgt);

static int32_t tableBuilderMgtRecoverTable(STableBuilderMgt *pMgt, int64_t seq, STableBuilder **pBuilder, int64_t size);

static int32_t tableMetaMgtInit(STableMetaMgt *pMgt, SBse *pBse, int64_t retention);
static void    tableMetaMgtDestroy(STableMetaMgt *pMgt);

static void tableReaderFree(void *pReader);

static void blockFree(void *pBlock);

int32_t bseTableMgtCreate(SBse *pBse, void **pMgt) {
  int32_t code = 0;
  int32_t lino = 0;

  STableMgt *p = taosMemoryCalloc(1, sizeof(STableMgt));
  if (p == NULL) {
    return terrno;
  }
  p->pBse = pBse;
  p->pHashObj = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (p->pHashObj == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  *pMgt = p;
_error:
  if (code != 0) {
    if (p != NULL)
      bseError("vgId:%d failed to open table pBuilderMgt at line %d since %s", BSE_GET_VGID((SBse *)p->pBse), lino,
               tstrerror(code));
    bseTableMgtCleanup(p);
  }
  return code;
}

int32_t bseTableMgtSetLastRetentionTs(STableMgt *pMgt, int64_t retention) {
  if (pMgt == NULL) return 0;

  pMgt->retionTs = retention;
  return 0;
}

int32_t bseTableMgtCreateCache(STableMgt *pMgt) {
  int32_t code = 0;
  int32_t lino = 0;

  SCacheMgt *pCacheMgt = taosMemCalloc(1, sizeof(SCacheMgt));
  if (pCacheMgt == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }
  taosThreadRwlockInit(&pCacheMgt->mutex, NULL);

  code = blockCacheOpen(48, blockFree, &pCacheMgt->pBlockCache);

_error:
  return code;
}

int32_t createSubTableMgt(int64_t retenTs, int32_t readOnly, STableMgt *pMgt, SSubTableMgt **pSubMgt) {
  int32_t code = 0;
  int32_t lino = 0;

  SSubTableMgt *p = taosMemCalloc(1, sizeof(SSubTableMgt));
  if (p == NULL) {
    code = terrno;
    TSDB_CHECK_CODE(terrno, lino, _error);
  }

  if (!readOnly) {
    code = tableBuilderMgtInit(p->pBuilderMgt, pMgt->pBse, retenTs);
    TSDB_CHECK_CODE(code, lino, _error);

    p->pBuilderMgt->pMgt = p;
  }

  code = tableReaderMgtInit(p->pReaderMgt, pMgt->pBse, retenTs);
  TSDB_CHECK_CODE(code, lino, _error);

  p->pReaderMgt->pMgt = p;

  code = tableMetaMgtInit(p->pTableMetaMgt, pMgt->pBse, retenTs);
  TSDB_CHECK_CODE(code, lino, _error);

  p->pTableMetaMgt->pMgt = p;

  *pSubMgt = p;
_error:
  if (code != 0) {
    bseError("failed to create sub table mgt at line %d since %s", lino, tstrerror(code));
    destroySubTableMgt(p);
  }
  return code;
}
void destroySubTableMgt(SSubTableMgt *p) {
  if (p != NULL) {
    tableBuilderMgtDestroy(p->pBuilderMgt);
    tableReaderMgtDestroy(p->pReaderMgt);
    tableMetaMgtDestroy(p->pTableMetaMgt);
  }
  taosMemoryFree(p);
}
int32_t bseTableMgtGet(STableMgt *pMgt, int64_t seq, uint8_t **pValue, int32_t *len) {
  if (pMgt == NULL) return 0;

  int32_t code = 0;
  int32_t lino = 0;
  int32_t       readOnly = 1;
  SSubTableMgt *pSubMgt = NULL;

  SBse *pBse = pMgt->pBse;

  int64_t retenTs = 0;
  code = bseGetRetentionTsBySeq(pMgt->pBse, seq, &retenTs);
  TSDB_CHECK_CODE(code, lino, _error);

  if (retenTs > 0) {
    SSubTableMgt **ppSubMgt = taosHashGet(pMgt->pHashObj, &retenTs, sizeof(retenTs));
    if (ppSubMgt == NULL || *ppSubMgt == NULL) {
      code = createSubTableMgt(retenTs, 0, pMgt, &pSubMgt);
      TSDB_CHECK_CODE(code, lino, _error);

      code = taosHashPut(pMgt->pHashObj, &retenTs, sizeof(retenTs), &pSubMgt, sizeof(SSubTableMgt *));
      TSDB_CHECK_CODE(code, lino, _error);

    } else {
      pSubMgt = *ppSubMgt;
    }
  } else {
    pSubMgt = pMgt->pCurrTableMgt;
    if (pSubMgt == NULL) {
      return code;
    }
    readOnly = 0;
  }

  if (readOnly) {
    code = tableReaderMgtSeek(pSubMgt->pReaderMgt, seq, pValue, len);
    TSDB_CHECK_CODE(code, lino, _error);
  } else {
    code = tableBuilderMgtSeek(pSubMgt->pBuilderMgt, seq, pValue, len);
    if (code == TSDB_CODE_OUT_OF_RANGE) {
      code = tableReaderMgtSeek(pSubMgt->pReaderMgt, seq, pValue, len);
      TSDB_CHECK_CODE(code, lino, _error);
    }
  }
_error:
  if (code != 0) {
    bseError("failed to get table at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

int32_t bseTableMgtRecoverTable(STableMgt *pMgt, SBseLiveFileInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  if (pMgt == NULL) return 0;

  SSubTableMgt *pSubMgt = NULL;

  code = createSubTableMgt(pInfo->retentionTs, 0, pMgt, &pSubMgt);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableBuilderMgtRecoverTable(pSubMgt->pBuilderMgt, 0, NULL, pInfo->size);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to recover table at line %d since %s", lino, tstrerror(code));
  }
  destroySubTableMgt(pSubMgt);
  return 0;
}

int32_t bseTableMgtCleanup(void *pMgt) {
  if (pMgt == NULL) return 0;

  STableMgt *p = (STableMgt *)pMgt;

  void *pIter = taosHashIterate(p->pHashObj, NULL);
  while (pIter) {
    SSubTableMgt **ppSubMgt = pIter;
    destroySubTableMgt(*ppSubMgt);
    pIter = taosHashIterate(p->pHashObj, pIter);
  }

  destroySubTableMgt(p->pCurrTableMgt);

  taosHashCleanup(p->pHashObj);
  taosMemoryFree(p);
  return 0;
}

int32_t bseTableMgtAppend(STableMgt *pMgt, SBseBatch *pBatch) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t retionTs = 0;

  SBse         *pBse = pMgt->pBse;
  SSubTableMgt *pSubMgt = pMgt->pCurrTableMgt;

  if (pSubMgt == NULL) {
    if (pMgt->retionTs != 0) {
      retionTs = pMgt->retionTs;
    } else {
      retionTs = taosGetTimestampSec();
    }

    code = createSubTableMgt(retionTs, 0, pMgt, &pMgt->pCurrTableMgt);
    TSDB_CHECK_CODE(code, lino, _error);
    pSubMgt = pMgt->pCurrTableMgt;
  }

  code = tableBuilderMgtPutBatch(pSubMgt->pBuilderMgt, pBatch);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to append table at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

int32_t bseTableMgtGetLiveFileSet(STableMgt *pMgt, SArray **pList) {
  int32_t code = 0;
  return code;
}

int32_t bseTableMgtCommit(STableMgt *pMgt, SBseLiveFileInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  int8_t  flushIdx = -1;

  SSubTableMgt *pSubMgt = pMgt->pCurrTableMgt;
  if (pSubMgt == NULL) {
    bseInfo("nothing to commit table");
    return code;
  }

  code = tableBuilderMgtCommit(pSubMgt->pBuilderMgt, pInfo);
  TSDB_CHECK_CODE(code, lino, _error);
_error:
  if (code != 0) {
    bseError("failed to commit table at line %d since %s", lino, tstrerror(code));
  } else {
    bseInfo("succ to commit table");
  }
  return code;
}

int32_t bseTableMgtUpdateLiveFileSet(STableMgt *pMgt, SArray *pLiveFileSet) {
  int32_t code = 0;
  return code;
}

int32_t bseTableMgtSetBlockCacheSize(STableMgt *pMgt, int32_t cap) {
  int32_t code = 0;
  return code;
  // return blockCacheResize(pMgt->pReaderMgt->pBlockCache, cap);
}

int32_t bseTableMgtSetTableCacheSize(STableMgt *pMgt, int32_t cap) {
  int32_t code = 0;
  return code;
  // return tableCacheResize(pMgt->pReaderMgt->pTableCache, cap);
}

int32_t bseTableMgtClear(STableMgt *pMgt) {
  int32_t code = 0;
  int32_t lino = 0;
  if (pMgt == NULL) return 0;

  destroySubTableMgt(pMgt->pCurrTableMgt);

  void *pIter = taosHashIterate(pMgt->pHashObj, NULL);
  while (pIter) {
    SSubTableMgt **ppSubMgt = pIter;
    destroySubTableMgt(*ppSubMgt);
    pIter = taosHashIterate(pMgt->pHashObj, pIter);
  }
  taosHashClear(pMgt->pHashObj);

_error:
  if (code != 0) {
    bseError("failed to clear table at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

void tableReaderFree(void *pReader) {
  STableReader *p = (STableReader *)pReader;
  if (p != NULL) {
    tableReaderClose(p);
  }
}
void blockFree(void *pBlock) { taosMemoryFree(pBlock); }

int32_t tableReaderMgtInit(STableReaderMgt *pReader, SBse *pBse, int64_t retention) {
  int32_t code = 0;
  int32_t lino = 0;

  taosThreadRwlockInit(&pReader->mutex, NULL);

  code = blockCacheOpen(48, blockFree, &pReader->pBlockCache);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableCacheOpen(32, tableReaderFree, &pReader->pTableCache);
  TSDB_CHECK_CODE(code, lino, _error);

  pReader->pBse = pBse;
  pReader->retenTs = retention;

_error:
  if (code != 0) {
    bseError("failed to init table pReaderMgt mgt at line %d since %s", lino, tstrerror(code));
  }
  return code;
}
void tableReaderMgtSetRetion(STableReaderMgt *pReader, int64_t retention) { pReader->retenTs = retention; }

int32_t tableReaderMgtClear(STableReaderMgt *pReader) {
  int32_t code = 0;

  taosThreadRwlockWrlock(&pReader->mutex);

  (void)(tableCacheClear(pReader->pTableCache));

  (void)(blockCacheClear(pReader->pBlockCache));
  taosThreadRwlockUnlock(&pReader->mutex);

  return code;
}

void tableReaderMgtDestroy(STableReaderMgt *pReader) {
  tableCacheClose(pReader->pTableCache);
  blockCacheClose(pReader->pBlockCache);
  taosThreadRwlockDestroy(&pReader->mutex);
}

int32_t tableReaderMgtSeek(STableReaderMgt *pReaderMgt, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  int32_t lino = 0;

  STableReader    *pReader = NULL;

  code = tableReaderOpen(pReaderMgt->retenTs, &pReader, pReaderMgt);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableReaderGet(pReader, seq, pValue, len);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to seek table pReaderMgt at line %d since %s", lino, tstrerror(code));
  }

  tableReaderClose(pReader);
  return code;
}

int32_t tableBuilderMgtInit(STableBuilderMgt *pMgt, SBse *pBse, int64_t retention) {
  int32_t code = 0;
  int32_t lino = 0;

  taosThreadMutexInit(&pMgt->mutex, NULL);
  pMgt->pBse = pBse;

  for (int32_t i = 0; i < 2; i++) {
    pMgt->p[i] = NULL;
  }
  pMgt->inUse = 0;
  pMgt->retenTs = retention;
  return code;
}

int32_t tableBuilderMgtClear(STableBuilderMgt *pMgt) {
  int32_t code = 0;
  int32_t lino = 0;

  taosThreadMutexLock(&pMgt->mutex);
  for (int32_t i = 0; i < 2; i++) {
    if (pMgt->p[i] != NULL) {
      tableBuilderClose(pMgt->p[i], 0);
      pMgt->p[i] = NULL;
      pMgt->inUse = 0;
    }
  }
  taosThreadMutexUnlock(&pMgt->mutex);
  return code;
}

int32_t tableBuilderMgtPutBatch(STableBuilderMgt *pMgt, SBseBatch *pBatch) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t seq = pBatch->startSeq;
  taosThreadMutexLock(&pMgt->mutex);
  STableBuilder *p = pMgt->p[pMgt->inUse];
  taosThreadMutexUnlock(&pMgt->mutex);

  if (p == NULL) {
    code = tableBuilderMgtGetBuilder(pMgt, seq, &p);
    TSDB_CHECK_CODE(code, lino, _error);
  }

  code = tableBuilderPut(p, pBatch);
  TSDB_CHECK_CODE(code, lino, _error);
_error:
  if (code != 0) {
    bseError("failed to put batch to table builder at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

int32_t tableBuilderMgtSeek(STableBuilderMgt *pMgt, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t        code = 0;
  int32_t        lino = 0;
  STableBuilder *pBuilder = NULL;

  taosThreadMutexLock(&pMgt->mutex);
  int8_t inUse = pMgt->inUse;
  pBuilder = pMgt->p[inUse];
  taosThreadMutexUnlock(&pMgt->mutex);

  if (pBuilder && seqRangeContains(&pBuilder->tableRange, seq)) {
    code = tableBuilderGet(pBuilder, seq, pValue, len);
  } else {
    code = TSDB_CODE_OUT_OF_RANGE;  //  continue to read from reader
  }
  return code;
}

int32_t tableBuilderMgtGetBuilder(STableBuilderMgt *pMgt, int64_t seq, STableBuilder **pBuilder) {
  int32_t code = 0;
  int32_t lino = 0;

  SBse *pBse = pMgt->pBse;

  STableBuilder *p = NULL;

  code = tableBuilderOpen(pMgt->retenTs, &p, pBse);
  TSDB_CHECK_CODE(code, lino, _error);

  p->pTableMeta = pMgt->pMgt->pTableMetaMgt->pTableMeta;

  p->pBse = pMgt->pBse;
  pMgt->p[pMgt->inUse] = p;

  *pBuilder = p;

_error:
  if (code != 0) {
    bseError("failed to open table builder at line %d since %s", __LINE__, tstrerror(code));
  }

  return code;
}

int32_t tableBuilderMgtRecoverTable(STableBuilderMgt *pMgt, int64_t seq, STableBuilder **pBuilder, int64_t size) {
  int32_t        code = 0;
  int32_t        lino = 0;
  STableBuilder *pTable = NULL;

  code = tableBuilderMgtGetBuilder(pMgt, seq, &pTable);
  TSDB_CHECK_CODE(code, lino, _error);

  if (pTable->offset > size) {
    code = tableBuilderTruncFile(pTable, size);
    TSDB_CHECK_CODE(code, lino, _error);
  }
_error:
  if (code != 0) {
    bseError("failed to open table builder at line %d since %s", lino, tstrerror(code));
  }

  return code;
}
int32_t tableBuilderMgtCommit(STableBuilderMgt *pMgt, SBseLiveFileInfo *pInfo) {
  int32_t        code = 0;
  int32_t        lino = 0;
  int8_t         flushIdx = -1;
  STableBuilder *pBuilder = NULL;

  taosThreadMutexLock(&pMgt->mutex);
  pBuilder = pMgt->p[pMgt->inUse];

  taosThreadMutexUnlock(&pMgt->mutex);
  if (pBuilder != NULL) {
    code = tableBuilderCommit(pBuilder, pInfo);
    TSDB_CHECK_CODE(code, lino, _error);
  }
_error:
  if (code != 0) {
    bseError("failed to commit table builder at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

void tableBuilderMgtDestroy(STableBuilderMgt *pMgt) {
  for (int32_t i = 0; i < 2; i++) {
    if (pMgt->p[i] != NULL) {
      tableBuilderClose(pMgt->p[i], 0);
    }
  }
  taosThreadMutexDestroy(&pMgt->mutex);
}

int32_t tableMetaMgtInit(STableMetaMgt *pMgt, SBse *pBse, int64_t retention) {
  int32_t code = 0;
  int32_t lino = 0;
  pMgt->pBse = pBse;

  code = tableMetaOpen(NULL, &pMgt->pTableMeta, pMgt);
  TSDB_CHECK_CODE(code, lino, _error);

  pMgt->retenTs = retention;
  pMgt->pTableMeta->retentionTs = retention;
  pMgt->pTableMeta->pBse = pBse;

_error:
  if (code != 0) {
    bseError("failed to init table meta mgt at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

static void tableMetaMgtDestroy(STableMetaMgt *pMgt) {
  if (pMgt->pTableMeta != NULL) {
    tableMetaClose(pMgt->pTableMeta);
    pMgt->pTableMeta = NULL;
  }
}
