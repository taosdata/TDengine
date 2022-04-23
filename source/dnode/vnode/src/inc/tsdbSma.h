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

#ifndef _TD_VNODE_TSDB_SMA_H_
#define _TD_VNODE_TSDB_SMA_H_

#include "os.h"
#include "thash.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef int32_t (*__tb_ddl_fn_t)(void *ahandle, void **result, void *p1, void *p2);

struct STbDdlHandle {
  void         *ahandle;
  void         *result;
  __tb_ddl_fn_t fp;
};

typedef struct {
  tb_uid_t  suid;
  SArray   *tbUids;
  SHashObj *uidHash;
} STbUidStore;

static FORCE_INLINE int32_t tsdbUidStoreInit(STbUidStore **pStore) {
  ASSERT(*pStore == NULL);
  *pStore = taosMemoryCalloc(1, sizeof(STbUidStore));
  if (*pStore == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tsdbUidStorePut(STbUidStore *pStore, tb_uid_t suid, tb_uid_t uid);
void   *tsdbUidStoreFree(STbUidStore *pStore);

int32_t tsdbRegisterRSma(STsdb *pTsdb, SMeta *pMeta, SVCreateTbReq *pReq);
int32_t tsdbFetchTbUidList(void *pTsdb, void **result, void *suid, void *uid);
int32_t tsdbUpdateTbUidList(STsdb *pTsdb, STbUidStore *pUidStore);
int32_t tsdbTriggerRSma(STsdb *pTsdb, SMeta *pMeta, const void *pMsg, int32_t inputType);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_TSDB_SMA_H_*/