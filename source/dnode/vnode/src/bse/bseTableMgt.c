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

static int32_t getTableBuildFromManage(STableBuilderMgt *pMgt, STableBuilder **p);
static void    tableBuildManageDestroy(STableBuilderMgt *pMgt);

static int32_t tableReaderMgtInit(STableMgt *pMgt) {
  int32_t code = 0;
  int32_t lino = 0;

  STableReaderMgt *pReader = (STableReaderMgt *)pMgt->reader;
  taosThreadMutexInit(&pReader->mutex, NULL);

  pReader->pFileList = taosArrayInit(128, sizeof(SBseLiveFileInfo));
  if (pReader->pFileList == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  code = blockCacheOpen(4096 * 2, &pReader->pBatchCache);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableCacheOpen(4096 * 2, &pReader->pTableCache);
  TSDB_CHECK_CODE(code, lino, _error);

  pReader->pBse = pMgt->pBse;

_error:
  if (code != 0) {
    bseError("failed to init table reader mgt since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

static void tableReaderMgtDestroy(STableReaderMgt *pReader) {
  taosArrayDestroy(pReader->pFileList);
  tableCacheClose(pReader->pTableCache);
  blockCacheClose(pReader->pBatchCache);
  taosThreadMutexDestroy(&pReader->mutex);
}

static int32_t tableReaderMgtSeek(STableReaderMgt *pReaderMgt, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  int32_t lino = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pReaderMgt->pFileList); i++) {
    SBseLiveFileInfo *pInfo = taosArrayGet(pReaderMgt->pFileList, i);
    if (pInfo->sseq <= seq && pInfo->eseq >= seq) {
      STableReader *pReader = NULL;
      char          name[TSDB_FILENAME_LEN] = {0};

      bseBuildFullName((SBse *)pReaderMgt->pBse, pInfo->name, name);
      code = tableReadOpen(name, &pReader);
      TSDB_CHECK_CODE(code, lino, _error);

      code = tableReadGet(pReader, seq, pValue, len);
      TSDB_CHECK_CODE(code, lino, _error);

      code = tableReadClose(pReader);
      TSDB_CHECK_CODE(code, lino, _error);
      break;
    }
  }
_error:
  if (code != 0) {
    bseError("failed to seek table reader since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

static int32_t tableReaderMgtAddLiveFile(STableReaderMgt *pReader, SBseLiveFileInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  if (taosArrayPush(pReader->pFileList, pInfo) == NULL) {
    code = terrno;
  }
  return code;
}

static int32_t tableReaderMgtRemveLiveFile(STableReaderMgt *pReader, SBseLiveFileInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pReader->pFileList); i++) {
    SBseLiveFileInfo *p = taosArrayGet(pReader->pFileList, i);
    if (strcmp(p->name, pInfo->name) == 0) {
      taosArrayRemove(pReader->pFileList, i);
      break;
    }
  }
  return code;
}

static int32_t tableReaderMgtRecover(STableReaderMgt *pReader) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t lastSeq = 0;

  SBse           *pBse = pReader->pBse;
  SBseCommitInfo *pInfo = &pBse->commitInfo;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pFileList); i++) {
    SBseLiveFileInfo *p = taosArrayGet(pInfo->pFileList, i);
    code = tableReaderMgtAddLiveFile(pReader, p);
    TSDB_CHECK_CODE(code, lino, _error);
  }

  SBseLiveFileInfo *pLastFile = taosArrayGetLast(pReader->pFileList);
  if (pLastFile != NULL) {
    lastSeq = pLastFile->eseq;
  }

  pBse->seq = lastSeq + 1;
_error:
  if (code != 0) {
    bseError("failed to recover table reader since %s at line %d", tstrerror(code), lino);
  }
  return code;
}
int32_t bseTableMgtCreate(SBse *pBse, void **pMgt) {
  int32_t code = 0;
  int32_t lino = 0;

  STableMgt *p = taosMemoryCalloc(1, sizeof(STableMgt));
  if (p == NULL) {
    return terrno;
  }
  memcpy(p->path, pBse->path, sizeof(p->path));

  code = taosThreadMutexInit(&p->mutex, NULL);
  TSDB_CHECK_CODE(code, lino, _error);

  // // p->pFileList = taosArrayInit(128, sizeof(SBseLiveFileInfo));
  // if (p->pFileList == NULL) {
  //   TSDB_CHECK_CODE(terrno, lino, _error);
  // }

  // // code = blockCacheOpen(4096 * 2, &p->pBatchCache);
  // // TSDB_CHECK_CODE(code, lino, _error);

  // // code = tableCacheOpen(4096 * 2, &p->pTableCache);
  // // TSDB_CHECK_CODE(code, lino, _error);

  p->pBse = pBse;
  p->manager->pBse = pBse;

  tableReaderMgtInit(p);

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

  taosThreadMutexDestroy(&p->mutex);

  tableBuildManageDestroy(p->manager);

  tableReaderMgtDestroy(p->reader);
  taosMemFree(p);
  return 0;
}

int32_t bseTableMgtGet(STableMgt *p, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  int32_t lino = 0;

  STableMgt *pMgt = (STableMgt *)p;
  taosThreadMutexLock(&pMgt->mutex);

  STableBuilderMgt *pBuilderMgt = pMgt->manager;

  int8_t inUse = pBuilderMgt->inUse;

  STableBuilder *pBuilder = pBuilderMgt->p[inUse];
  if (pBuilder && inSeqRange(&pBuilder->range, seq)) {
    code = tableBuildGet(pBuilder, seq, pValue, len);
    goto _error;
  }

  code = tableReaderMgtSeek(pMgt->reader, seq, pValue, len);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to get table at line %d since %s", lino, tstrerror(code));
  }
  taosThreadMutexUnlock(&pMgt->mutex);
  return code;
}

int32_t bseTableMgtAddLiveFile(STableMgt *pMgt, SBseLiveFileInfo *pInfo) {
  int32_t code = 0;
  return tableReaderMgtAddLiveFile(pMgt->reader, pInfo);
}
int32_t bseTableMgtRemoveLiveFile(STableMgt *pMgt, SBseLiveFileInfo *pInfo) {
  int32_t code = 0;
  return tableReaderMgtRemveLiveFile(pMgt->reader, pInfo);
}

int32_t bseTableMgtAppend(STableMgt *pMgt, SBseBatch *pBatch) {
  int32_t code = 0;
  int32_t lino = 0;

  taosThreadMutexLock(&pMgt->mutex);
  STableBuilderMgt *pBuilderMgt = pMgt->manager;

  STableBuilder *p = pBuilderMgt->p[pBuilderMgt->inUse];
  if (p == NULL) {
    code = getTableBuildFromManage(pBuilderMgt, &p);
    TSDB_CHECK_CODE(code, lino, _error);
  }

  code = tableBuildPutBatch(p, pBatch);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to append table at line %d since %s", lino, tstrerror(code));
  }
  taosThreadMutexUnlock(&pMgt->mutex);
  return code;
}

int32_t bseTableMgtGetLiveFileList(STableMgt *pMgt, SArray **pList) {
  int32_t code = 0;
  int32_t lino = 0;

  SArray *res = taosArrayInit(128, sizeof(SBseLiveFileInfo));
  taosThreadMutexLock(&pMgt->mutex);
  STableReaderMgt *pReaderMgt = pMgt->reader;
  for (int32_t i = 0; i < taosArrayGetSize(pReaderMgt->pFileList); i++) {
    SBseLiveFileInfo *p = taosArrayGet(pReaderMgt->pFileList, i);
    if (taosArrayPush(res, p) == NULL) {
      code = terrno;
      break;
    }
  }
  taosThreadMutexUnlock(&pMgt->mutex);

  if (code != 0) {
    bseError("failed to get live file list at line %d since %s", lino, tstrerror(code));
  }

  *pList = res;
  return code;
}

int32_t bseTableMgtCommit(STableMgt *pMgt) {
  int32_t code = 0;
  int32_t lino = 0;
  int8_t  flushIdx = -1;

  STableBuilderMgt *pBuilderMgt = pMgt->manager;
  STableBuilder    *pBuilder = NULL;

  taosThreadMutexLock(&pMgt->mutex);
  flushIdx = pBuilderMgt->inUse;
  pBuilderMgt->inUse = 1 - pBuilderMgt->inUse;
  taosThreadMutexUnlock(&pMgt->mutex);

  if (flushIdx == -1) {
    return 0;
  }

  pBuilder = pBuilderMgt->p[flushIdx];
  if (pBuilder == NULL) {
    return code;
  }

  SBseLiveFileInfo info = {0};

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

int32_t bseTableMgtRecover(SBse *pBse, STableMgt *pMgt) { return tableReaderMgtRecover(pMgt->reader); }

int32_t getTableBuildFromManage(STableBuilderMgt *pMgt, STableBuilder **pBuilder) {
  int32_t code = 0;
  char    path[TSDB_FILENAME_LEN] = {0};

  SBse *pBse = pMgt->pBse;
  bseBuildDataName(pMgt->pBse, pBse->seq, path);

  STableBuilder *p = NULL;
  code = tableBuildOpen(path, &p);

  p->bse = pMgt->pBse;
  pMgt->p[pMgt->inUse] = p;

  *pBuilder = p;
  return code;
}
void tableBuildManageDestroy(STableBuilderMgt *pMgt) {
  for (int32_t i = 0; i < 2; i++) {
    if (pMgt->p[i] != NULL) {
      tableBuildClose(pMgt->p[i], 0);
    }
  }
}