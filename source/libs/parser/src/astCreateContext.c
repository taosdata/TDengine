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

#include "ttoken.h"
#include "astCreateContext.h"

typedef struct SResourceEntry {
  EResourceType type;
  void* res;
} SResourceEntry;

int32_t createAstCreateContext(SParseContext* pQueryCxt, SAstCreateContext* pCxt) {
  pCxt->pQueryCxt = pQueryCxt;
  pCxt->notSupport = false;
  pCxt->valid = true;
  pCxt->pRootNode = NULL;
  pCxt->pResourceHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
  if (NULL == pCxt->pResourceHash) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t destroyAstCreateContext(SAstCreateContext* pCxt) {
  SResourceEntry* item = taosHashIterate(pCxt->pResourceHash, NULL);
  while (item) {
    switch (item->type) {
      case AST_CXT_RESOURCE_NODE:
        nodesDestroyNode(item->res);
        break;
      case AST_CXT_RESOURCE_NODE_LIST:
        nodesDestroyList(item->res);
        break;
      default:
        tfree(item->res);
    }
    item = taosHashIterate(pCxt->pResourceHash, item);
  }
}

void* acquireRaii(SAstCreateContext* pCxt, EResourceType type, void* p) {
  if (NULL == p) {
    return NULL;
  }
  SResourceEntry entry = { .type = type, .res = p };
  taosHashPut(pCxt->pResourceHash, &p, POINTER_BYTES, &entry, sizeof(SResourceEntry));
  return p;
}

void* releaseRaii(SAstCreateContext* pCxt, void* p) {
  if (NULL == p) {
    return NULL;
  }
  taosHashRemove(pCxt->pResourceHash, &p, POINTER_BYTES);
  return p;
}
