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

static int32_t initTableCache(int32_t cap, STableCacheMgt **pMgt) {
  int32_t code = 0;

  STableCacheMgt *p = taosMemoryCalloc(1, sizeof(STableCacheMgt));
  if (p == NULL) {
    return terrno;
  }
  p->cap = cap;

  *pMgt = p;
  return code;
}

static int32_t initBlockCache(int32_t cap, SBlockCacheMgt **pMgt) {
  int32_t code = 0;

  SBlockCacheMgt *p = taosMemoryCalloc(1, sizeof(SBlockCacheMgt));
  if (p == NULL) {
    return terrno;
  }

  p->cap = cap;
  *pMgt = p;

  return code;
}

static void destroyTableCache(STableCacheMgt *p) { taosMemFree(p); }
static void destroyBlockCache(SBlockCacheMgt *p) { taosMemFree(p); }

int32_t bseTableMgtInit(SBse *pBse, void **pMgt) {
  int32_t code = 0;
  int32_t lino = 0;

  STableMgt *p = taosMemoryCalloc(1, sizeof(STableMgt));
  if (p == NULL) {
    return terrno;
  }
  memcpy(p->path, pBse->path, sizeof(p->path));

  code = taosThreadMutexInit(&p->mutex, NULL);
  TSDB_CHECK_CODE(code, lino, _error);

  p->pFileList = taosArrayInit(128, sizeof(STableLiveFileInfo));
  if (p->pFileList == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }

  code = initBlockCache(4096 * 2, &p->pBatchCache);
  TSDB_CHECK_CODE(code, lino, _error);

  code = initTableCache(4096 * 2, &p->pTableCache);
  TSDB_CHECK_CODE(code, lino, _error);

  p->pBse = pBse;

  *pMgt = p;
_error:
  if (code != 0) {
    if (p != NULL)
      bseError("vgId:%d failed to open table manager since %s at line %d", BSE_VGID((SBse *)p->pBse), tstrerror(code),
               lino);
    bseTableMgtCleanup(p);
  }
  return code;
}

int32_t bseTableMgtCleanup(void *pMgt) {
  if (pMgt == NULL) return 0;
  STableMgt *p = (STableMgt *)pMgt;

  taosArrayDestroy(p->pFileList);
  destroyBlockCache(p->pBatchCache);
  destroyTableCache(p->pTableCache);
  taosMemFree(p);
  return 0;
}

int32_t bseTableMgtGet(STableMgt *p, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  int32_t lino = 0;

  STableMgt *pMgt = (STableMgt *)p;
  taosThreadMutexLock(&pMgt->mutex);

  STableBuilderMgt *pBuilderMgt = pMgt->manager;

  int8_t     inUse = pBuilderMgt->inUse;
  SSeqRange *range = &pBuilderMgt->range[inUse];

  if (seq >= range->sseq) {
    STableBuilder *pBuilder = pBuilderMgt->p[inUse];
    code = tableBuildGet(pBuilder, seq, pValue, len);
    goto _error;
  }

  for (int32_t i = 0; i < taosArrayGetSize(p->pFileList); i++) {
    STableLiveFileInfo *pInfo = taosArrayGet(p->pFileList, i);
    if (pInfo->sseq <= seq && pInfo->eseq >= seq) {
      STableReader reader;
      code = tableReadOpen(pInfo->name, &reader);
      TSDB_CHECK_CODE(code, lino, _error);

      code = tableReadGet(&reader, seq, pValue, len);
      TSDB_CHECK_CODE(code, lino, _error);

      code = tableReadClose(&reader);
      TSDB_CHECK_CODE(code, lino, _error);
      break;
    }
  }

_error:
  if (code != 0) {
    bseError("failed to get table at line %d since %s", code, terrno);
  }
  taosThreadMutexUnlock(&pMgt->mutex);
  return code;
}

int32_t bseTableMgtAddLiveFile(STableMgt *pMgt, STableLiveFileInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  taosThreadMutexLock(&pMgt->mutex);
  if (taosArrayPush(pMgt->pFileList, pInfo) == NULL) {
    code = terrno;
  }
_error:
  if (code != 0) {
    bseError("failed to update live file at line %d since %s", lino, tstrerror(code));
  }
  taosThreadMutexUnlock(&pMgt->mutex);
  return code;
}
int32_t bseTableMgtRemoveLiveFile(STableMgt *pMgt, STableLiveFileInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  taosThreadMutexLock(&pMgt->mutex);

  for (int32_t i = 0; i < taosArrayGetSize(pMgt->pFileList); i++) {
    STableLiveFileInfo *p = taosArrayGet(pMgt->pFileList, i);
    if (strcmp(p->name, pInfo->name) == 0) {
      taosArrayRemove(pMgt->pFileList, i);
      break;
    }
  }

_error:
  if (code != 0) {
    bseError("failed to update live file at line %d since %s", lino, tstrerror(code));
  }
  taosThreadMutexUnlock(&pMgt->mutex);
  return code;
}

int32_t bseTableMgtCommit(STableMgt *pMgt) {
  int32_t code = 0;
  int32_t lino = 0;

  STableBuilderMgt *pBuilderMgt = pMgt->manager;

  int8_t flushIdx = -1;
  taosThreadMutexLock(&pMgt->mutex);
  flushIdx = pBuilderMgt->inUse;
  pBuilderMgt->inUse = 1 - pBuilderMgt->inUse;
  taosThreadMutexUnlock(&pMgt->mutex);

  if (flushIdx == -1) {
    return 0;
  }
  STableBuilder     *pBuilder = pBuilderMgt->p[flushIdx];
  STableLiveFileInfo info = {0};

  code = tableBuildCommit(pBuilder, &info);
  TSDB_CHECK_CODE(code, lino, _error);

  code = bseTableMgtAddLiveFile(pMgt, &info);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to commit table at line %d since %s", lino, tstrerror(code));
  }
  return code;
}