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

#include "tstreamFileState.h"

#include "tlist.h"
#include "tsimplehash.h"


struct SStreamFileState {
  SList* rowBuffLru;
  SList* rowBuffFree;
  SSHashObj* rowMap;
  uint32_t rowSize;
  uint32_t pageSize;
  uint64_t maxNumOfPages;
  uint64_t preVersion;
  uint64_t version;
  uint64_t  fileSize;
  TdFilePtr pRowFile;
  char*     filePath;
};

typedef struct SStatePage {
  int32_t pageId;
  int8_t  refCount;
  int64_t fileOffset;
  void*   buff;
} SStatePage;


SStreamFileState* createStreamFileState(uint64_t memSize, uint32_t rowSize, const char* path) {
  if (memSize == 0 || rowSize == 0 || strlen(path) == 0) {
    goto _error;
  }

  SStreamFileState* pFileState = taosMemoryCalloc(1, sizeof(SStreamFileState));
  if (!pFileState) {
    goto _error;
  }
  pFileState->rowBuffLru = tdListNew(POINTER_BYTES);
  pFileState->rowBuffFree = tdListNew(POINTER_BYTES);
  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pFileState->rowMap = tSimpleHashInit(1024, hashFn);
  if (!pFileState->rowBuffLru || !pFileState->rowBuffFree || !pFileState->rowMap) {
    goto _error;
  }
  pFileState->rowSize = rowSize;
  pFileState->pageSize = rowSize * 4;
  pFileState->maxNumOfPages = (uint64_t)(memSize / pFileState->pageSize);
  pFileState->preVersion = 0;
  pFileState->version = 1;
  pFileState->pRowFile = NULL;
  pFileState->filePath = taosStrdup(path);
  if (!pFileState->filePath) {
    goto _error;
  }


_error:
  destroyStreamFileState(pFileState);
  return NULL;
}

void destroyStreamFileState(SStreamFileState* pFileState) {
  tdListFree(pFileState->rowBuffLru);
  tdListFree(pFileState->rowBuffFree);
  tSimpleHashCleanup(pFileState->rowMap);
  taosMemoryFreeClear(pFileState->filePath);
  if (!pFileState->pRowFile) {
    taosCloseFile(&pFileState->pRowFile);
  }
}

SRowBuffPos* getNewRowPos(SStreamFileState* pFileState, void* key, int32_t keyLen) {
  return NULL;
}

void* getRowBuffByPos(SStreamFileState* pFileState, SRowBuffPos* pPos) {
  if (pPos->pPage->buff) {
    return pPos->pRow;
  }
  // todo(liuyao) swap
}

// void* getRowBuff(SStreamFileState* pFileState, void* pPos) {
//   return NULL;
// }

void releaseRowBuf(SStreamFileState* pFileState, void* buff) {

}

void clearExpiredRow(SStreamFileState* pFileState, TSKEY ts) {

}

SStreamFileState* getSnapshot(SStreamFileState* pFileState) {
  return NULL;
}

int32_t flushSnapshot(SStreamFileState* pFileState) {
  return 0;
}

int32_t recoverSnapshot(SStreamFileState* pFileState) {
  return 0;
}