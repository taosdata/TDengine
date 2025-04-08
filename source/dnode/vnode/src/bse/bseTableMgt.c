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

static int32_t tableReaderMgtInit(STableReaderMgt *pReader, SBse *pBse);
static void    tableReaderMgtCleanup(STableReaderMgt *pReader);
static int32_t tableReaderMgtSeek(STableReaderMgt *pReaderMgt, int64_t seq, uint8_t **pValue, int32_t *len);
static int32_t tableReaderMgtAddLiveFile(STableReaderMgt *pReader, SBseLiveFileInfo *pInfo);
static int32_t tableReaderMgtRemveLiveFile(STableReaderMgt *pReader, SBseLiveFileInfo *pInfo);
static int32_t tableReaderMgtAddLiveFileSet(STableReaderMgt *pReader, SArray *pFileSet);
static int32_t tableReadMgtGetAllLiveFileSet(STableReaderMgt *pReader, SArray **pList);

static int32_t tableBuilderMgtInitialize(STableBuilderMgt *pMgt, SBse *pBse);
static int32_t tableBuilderMgtGetBuilder(STableBuilderMgt *pMgt, int64_t seq, STableBuilder **p);
static void    tableBuilderMgtDestroy(STableBuilderMgt *pMgt);
static int32_t tableBuilderMgtCommit(STableBuilderMgt *pMgt, SBseLiveFileInfo *pInfo, int8_t *commited);
static int32_t tableBuilderMgtSeek(STableBuilderMgt *pMgt, int64_t seq, uint8_t **pValue, int32_t *len);
int32_t        tableBuilderMgtPutBatch(STableBuilderMgt *pMgt, SBseBatch *pBatch);

static void tableReadeFree(void *pReader);
static void blockWrapperFree(void *pBlockWrapper);

int32_t bseTableMgtCreate(SBse *pBse, void **pMgt) {
  int32_t code = 0;
  int32_t lino = 0;

  STableMgt *p = taosMemoryCalloc(1, sizeof(STableMgt));
  if (p == NULL) {
    return terrno;
  }
  p->pBse = pBse;

  code = tableBuilderMgtInitialize(p->pBuilderMgt, pBse);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableReaderMgtInit(p->pReaderMgt, pBse);
  TSDB_CHECK_CODE(code, lino, _error);

  *pMgt = p;
_error:
  if (code != 0) {
    if (p != NULL)
      bseError("vgId:%d failed to open table pBuilderMgt since %s at line %d", BSE_GET_VGID((SBse *)p->pBse),
               tstrerror(code), lino);
    bseTableMgtCleanup(p);
  }
  return code;
}

int32_t bseTableMgtGet(STableMgt *pMgt, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tableBuilderMgtSeek(pMgt->pBuilderMgt, seq, pValue, len);
  if (code == TSDB_CODE_OUT_OF_RANGE) {
    code = tableReaderMgtSeek(pMgt->pReaderMgt, seq, pValue, len);
    TSDB_CHECK_CODE(code, lino, _error);
  }
_error:
  if (code != 0) {
    bseError("failed to get table at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

int32_t bseTableMgtCleanup(void *pMgt) {
  if (pMgt == NULL) return 0;

  STableMgt *p = (STableMgt *)pMgt;
  tableBuilderMgtDestroy(p->pBuilderMgt);
  tableReaderMgtCleanup(p->pReaderMgt);
  taosMemoryFree(p);
  return 0;
}

int32_t bseTableMgtAppend(STableMgt *pMgt, SBseBatch *pBatch) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tableBuilderMgtPutBatch(pMgt->pBuilderMgt, pBatch);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to append table at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

int32_t bseTableMgtGetLiveFileSet(STableMgt *pMgt, SArray **pList) {
  return tableReadMgtGetAllLiveFileSet(pMgt->pReaderMgt, pList);
}

int32_t bseTableMgtCommit(STableMgt *pMgt, SArray **pLiveFileList) {
  int32_t code = 0;
  int32_t lino = 0;
  int8_t  flushIdx = -1;
  int8_t  commited = 0;

  SBseLiveFileInfo info = {0};

  code = tableBuilderMgtCommit(pMgt->pBuilderMgt, &info, &commited);
  TSDB_CHECK_CODE(code, lino, _error);

  if (commited == 1) {
    code = tableReaderMgtAddLiveFile(pMgt->pReaderMgt, &info);
    TSDB_CHECK_CODE(code, lino, _error);
  }

  code = bseTableMgtGetLiveFileSet(pMgt, pLiveFileList);

_error:
  if (code != 0) {
    bseError("failed to commit table at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

int32_t bseTableMgtUpdateLiveFileSet(STableMgt *pMgt, SArray *pLiveFileSet) {
  return tableReaderMgtAddLiveFileSet(pMgt->pReaderMgt, pLiveFileSet);
}

static void tableReadeFree(void *pReader) {
  STableReader *p = (STableReader *)pReader;
  if (p != NULL) {
    tableReaderClose(p);
  }
}
static void blockFree(void *pBlock) { taosMemoryFree(pBlock); }
static void blockWithMetaFree(void *pBlock) {
  SBlockWithMeta *p = (SBlockWithMeta *)pBlock;
  blockWithMetaCleanup(p);
}

int32_t tableReaderMgtInit(STableReaderMgt *pReader, SBse *pBse) {
  int32_t code = 0;
  int32_t lino = 0;

  taosThreadRwlockInit(&pReader->mutex, NULL);

  pReader->pFileList = taosArrayInit(128, sizeof(SBseLiveFileInfo));
  if (pReader->pFileList == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }
  int32_t cap = BSE_GET_BLOCK_SIZE(pBse);

  code = blockCacheOpen(48, blockFree, &pReader->pBlockCache);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableCacheOpen(32, tableReadeFree, &pReader->pTableCache);
  TSDB_CHECK_CODE(code, lino, _error);

  pReader->pBse = pBse;

_error:
  if (code != 0) {
    bseError("failed to init table pReaderMgt mgt since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

void tableReaderMgtCleanup(STableReaderMgt *pReader) {
  taosArrayDestroy(pReader->pFileList);
  tableCacheClose(pReader->pTableCache);
  blockCacheClose(pReader->pBlockCache);
  taosThreadRwlockDestroy(&pReader->mutex);
}

int32_t compareFileInfoFunc(const void *a, const void *b) {
  SBseLiveFileInfo *p1 = (SBseLiveFileInfo *)a;
  SBseLiveFileInfo *p2 = (SBseLiveFileInfo *)b;

  if (p1->sseq < p2->sseq) {
    return -1;
  } else if (p1->sseq > p2->sseq) {
    return 1;
  }
  return 0;
}
static int32_t findTargetTable(SArray *pFileList, int64_t seq) {
  SBseLiveFileInfo target = {.sseq = seq, .eseq = seq};
  return taosArraySearchIdx(pFileList, &target, compareFileInfoFunc, TD_LE);
}
int32_t tableReaderMgtSeek(STableReaderMgt *pReaderMgt, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  int32_t lino = 0;

  SBseLiveFileInfo info = {0};
  STableReader    *pReader = NULL;
  taosThreadRwlockRdlock(&pReaderMgt->mutex);
  int32_t idx = findTargetTable(pReaderMgt->pFileList, seq);
  if (idx < 0 || idx >= taosArrayGetSize(pReaderMgt->pFileList)) {
    taosThreadRwlockUnlock(&pReaderMgt->mutex);
    TSDB_CHECK_CODE(code = TSDB_CODE_NOT_FOUND, lino, _error);
  }

  SBseLiveFileInfo *pInfo = taosArrayGet(pReaderMgt->pFileList, idx);
  memcpy(&info, pInfo, sizeof(SBseLiveFileInfo));
  taosThreadRwlockUnlock(&pReaderMgt->mutex);

  SSeqRange range = {.sseq = info.sseq, .eseq = info.eseq};
  if (inSeqRange(&range, seq)) {
    STableReader *pReader = NULL;
    code = tableCacheGet(pReaderMgt->pTableCache, &range, &pReader);
    if (code != 0) {
      char name[TSDB_FILENAME_LEN] = {0};
      bseBuildFullName((SBse *)pReaderMgt->pBse, info.name, name);

      code = tableReaderOpen(name, &pReader, pReaderMgt);
      TSDB_CHECK_CODE(code, lino, _error);

      code = tableCachePut(pReaderMgt->pTableCache, &range, pReader);
      if (code != 0) {
        bseError("failed to put table reader to cache since %s at line %d", tstrerror(code), lino);
        TSDB_CHECK_CODE(code, lino, _error);
      }
    }
    code = tableReaderGet(pReader, seq, pValue, len);
    TSDB_CHECK_CODE(code, lino, _error);
  }
_error:
  if (code != 0) {
    bseError("failed to seek table pReaderMgt since %s at line %d", tstrerror(code), lino);
  }
  if (pReader != NULL) {
    tableReaderClose(pReader);
  }
  return code;
}

static int32_t tableReaderMgtAddLiveFile(STableReaderMgt *pReader, SBseLiveFileInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  taosThreadRwlockWrlock(&pReader->mutex);
  if (taosArrayPush(pReader->pFileList, pInfo) == NULL) {
    code = terrno;
  }
  taosThreadRwlockUnlock(&pReader->mutex);
  return code;
}

static int32_t tableReaderMgtRemveLiveFile(STableReaderMgt *pReader, SBseLiveFileInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  taosThreadRwlockWrlock(&pReader->mutex);
  for (int32_t i = 0; i < taosArrayGetSize(pReader->pFileList); i++) {
    SBseLiveFileInfo *p = taosArrayGet(pReader->pFileList, i);
    if (strcmp(p->name, pInfo->name) == 0) {
      taosArrayRemove(pReader->pFileList, i);
      break;
    }
  }
  taosThreadRwlockUnlock(&pReader->mutex);
  return code;
}

static int32_t tableReaderMgtAddLiveFileSet(STableReaderMgt *pReader, SArray *pFileSet) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t lastSeq = 0;

  taosThreadRwlockWrlock(&pReader->mutex);
  if (taosArrayAddAll(pReader->pFileList, pFileSet) == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }
_error:
  if (code != 0) {
    bseError("failed to recover table pReaderMgt since %s at line %d", tstrerror(code), lino);
  }
  taosThreadRwlockUnlock(&pReader->mutex);
  return code;
}
static int32_t tableReadMgtGetAllLiveFileSet(STableReaderMgt *pReader, SArray **pList) {
  int32_t code = 0;
  int32_t lino = 0;

  SArray *res = taosArrayInit(128, sizeof(SBseLiveFileInfo));
  if (res == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  taosThreadRwlockRdlock(&pReader->mutex);
  if (taosArrayAddAll(res, pReader->pFileList) == NULL) {
    taosThreadRwlockUnlock(&pReader->mutex);
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  taosThreadRwlockUnlock(&pReader->mutex);
_error:
  if (code != 0) {
    bseError("failed to get live file list at line %d since %s", lino, tstrerror(code));
  }
  *pList = res;
  return code;
}

int32_t tableBuilderMgtInitialize(STableBuilderMgt *pMgt, SBse *pBse) {
  int32_t code = 0;
  int32_t lino = 0;

  taosThreadMutexInit(&pMgt->mutex, NULL);
  pMgt->pBse = pBse;

  for (int32_t i = 0; i < 2; i++) {
    pMgt->p[i] = NULL;
  }
  pMgt->inUse = 0;
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

  code = tableBuilderPutBatch(p, pBatch);
  TSDB_CHECK_CODE(code, lino, _error);
_error:
  if (code != 0) {
    bseError("failed to put batch to table builder at line %d since %s", lino, tstrerror(code));
  }
  return code;
}
static int32_t tableBuilderMgtSeek(STableBuilderMgt *pMgt, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t        code = 0;
  int32_t        lino = 0;
  STableBuilder *pBuilder = NULL;

  taosThreadMutexLock(&pMgt->mutex);
  int8_t inUse = pMgt->inUse;
  pBuilder = pMgt->p[inUse];
  taosThreadMutexUnlock(&pMgt->mutex);

  if (pBuilder && inSeqRange(&pBuilder->tableRange, seq)) {
    code = tableBuilderGet(pBuilder, seq, pValue, len);
  } else {
    code = TSDB_CODE_OUT_OF_RANGE;  //  continue to read from reader
  }
  return code;
}

int32_t tableBuilderMgtGetBuilder(STableBuilderMgt *pMgt, int64_t seq, STableBuilder **pBuilder) {
  int32_t code = 0;
  char    path[TSDB_FILENAME_LEN] = {0};

  SBse *pBse = pMgt->pBse;
  bseBuildDataName(pMgt->pBse, seq, path);

  STableBuilder *p = NULL;
  code = tableBuilderOpen(path, &p, pBse);
  if (code != 0) {
    return code;
  }

  p->pBse = pMgt->pBse;
  pMgt->p[pMgt->inUse] = p;

  *pBuilder = p;
  return code;
}

int32_t tableBuilderMgtCommit(STableBuilderMgt *pMgt, SBseLiveFileInfo *pInfo, int8_t *commited) {
  int32_t        code = 0;
  int32_t        lino = 0;
  int8_t         flushIdx = -1;
  STableBuilder *pBuilder = NULL;

  taosThreadMutexLock(&pMgt->mutex);

  flushIdx = pMgt->inUse;
  pMgt->inUse = 1 - pMgt->inUse;
  pBuilder = pMgt->p[flushIdx];

  taosThreadMutexUnlock(&pMgt->mutex);
  if (pBuilder != NULL) {
    code = tableBuilderCommit(pBuilder, pInfo);
    TSDB_CHECK_CODE(code, lino, _error);

    *commited = 1;
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