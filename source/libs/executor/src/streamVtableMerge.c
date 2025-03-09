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

#include "streamVtableMerge.h"

#include "query.h"

typedef struct SVMBufPageInfo {
  int32_t pageId;
  int64_t deadlineTime;
} SVMBufPageInfo;

typedef struct SStreamVtableMergeSource {
  SList*       pageIdList;
  SSDataBlock* pBlock;
  int32_t      rowIndex;

  int64_t fetchUs;
  int64_t fetchNum;
} SStreamVtableMergeSource;

typedef struct SStreamVtableMergeHandle {
  int32_t        nSrcTbls;
  SHashObj*      pSources;
  SDiskbasedBuf* pBuf;
} SStreamVtableMergeHandle;

int32_t streamVtableMergeAddBlock(SStreamVtableMergeHandle* pHandle, SSDataBlock* pSourceBlock, const char* idstr) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  int64_t                   pTbUid = 0;
  void*                     px = 0;
  SStreamVtableMergeSource* pSource = NULL;

  QUERY_CHECK_NULL(pHandle, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pSourceBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pTbUid = pSourceBlock->info.id.uid;
  px = taosHashGet(pHandle->pSources, &pTbUid, sizeof(int64_t));

  if (px == NULL) {
    if (taosHashGetSize(pHandle->pSources) >= pHandle->nSrcTbls) {
      qError("Number of source tables exceeded the limit %d, table uid: %" PRId64, pHandle->nSrcTbls, pTbUid);
      code = TSDB_CODE_INTERNAL_ERROR;
      QUERY_CHECK_CODE(code, lino, _end);
    }
    // try to allocate a new source
    pSource = taosMemoryCalloc(1, sizeof(SStreamVtableMergeSource));
    QUERY_CHECK_NULL(pSource, code, lino, _end, terrno);
    code = taosHashPut(pHandle->pSources, &pTbUid, sizeof(int64_t), &pSource, POINTER_BYTES);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(pSource);
      pSource = NULL;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  } else {
    pSource = *(SStreamVtableMergeSource**)px;
    QUERY_CHECK_NULL(pSource, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  return code;
}
