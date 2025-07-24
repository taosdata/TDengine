
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
#ifndef _TD_BSE_TABLE_MANAGER_H_
#define _TD_BSE_TABLE_MANAGER_H_

#include "bse.h"
#include "bseCache.h"
#include "bseTable.h"
#include "bseUtil.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SSubTableMgt SSubTableMgt;

typedef struct {
  STableBuilder *p;
  int8_t inited;

  TdThreadMutex mutex;
  int64_t       timestamp;
  SSubTableMgt *pMgt;

  SBse *pBse;
} STableBuilderMgt;

typedef struct {
  STableCache   *pTableCache;
  SBlockCache   *pBlockCache;
  TdThreadRwlock mutex;
  SBse          *pBse;

  SSubTableMgt *pMgt;
  int64_t       timestamp;
} STableReaderMgt;

typedef struct {
  char      path[TSDB_FILENAME_LEN];
  SBse     *pBse;

  SBTableMeta *pTableMeta;
  int64_t      timestamp;

  SSubTableMgt *pMgt;
} STableMetaMgt;

typedef struct STableMgt STableMgt;

struct SSubTableMgt {
  STableMgt       *pTableMgt;
  STableBuilderMgt pBuilderMgt[1];
  STableReaderMgt  pReaderMgt[1];
  STableMetaMgt    pTableMetaMgt[1];
};

typedef struct {
  SBlockCache   *pBlockCache;
  STableCache   *pTableCache;
  TdThreadRwlock mutex;
} SCacheMgt;

struct STableMgt {
  void         *pBse;
  SSubTableMgt *pCurrTableMgt;
  int64_t       timestamp;
  SHashObj     *pHashObj;
  SCacheMgt    *pCacheMgt;
};

int32_t bseTableMgtCreate(SBse *pBse, void **pMgt);
int32_t bseTableMgtSetLastTableId(STableMgt *pMgt, int64_t retention);

int32_t bseTableMgtCreateCache(STableMgt *pMgt);

int32_t bseTableMgtGet(STableMgt *p, int64_t seq, uint8_t **pValue, int32_t *len);

int32_t bseTableMgtCleanup(void *p);

int32_t bseTableMgtCommit(STableMgt *pMgt, SBseLiveFileInfo *pInfo);

int32_t bseTableMgtUpdateLiveFileSet(STableMgt *pMgt, SArray *pLiveFileList);

int32_t bseTableMgtAppend(STableMgt *pMgt, SBseBatch *pBatch);

int32_t bseTableMgtGetLiveFileSet(STableMgt *pMgt, SArray **pList);

int32_t bseTableMgtClear(STableMgt *pMgt);

int32_t bseTableMgtSetBlockCacheSize(STableMgt *pMgt, int32_t cap);

int32_t bseTableMgtSetTableCacheSize(STableMgt *pMgt, int32_t cap);

int32_t blockWithMetaInit(SBlock *pBlock, SBlockWithMeta **pMeta);

int32_t blockWithMetaCleanup(SBlockWithMeta *p);

int32_t blockWithMetaSeek(SBlockWithMeta *p, int64_t seq, uint8_t **pValue, int32_t *len);

int32_t bseTableMgtRecoverTable(STableMgt *pMgt, SBseLiveFileInfo *pInfo);

int32_t createSubTableMgt(int64_t timestamp, int32_t readOnly, STableMgt *pMgt, SSubTableMgt **pSubMgt);
void    destroySubTableMgt(SSubTableMgt *p);
#ifdef __cplusplus
}
#endif

#endif