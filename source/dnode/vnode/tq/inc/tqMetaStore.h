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

#ifndef _TQ_META_STORE_H_
#define _TQ_META_STORE_H_

#include "os.h"
#include "tq.h"

#ifdef __cplusplus
extern "C" {
#endif


TqMetaStore*  tqStoreOpen(const char* path,
    TqSerializeFun   pSerializer,
    TqDeserializeFun pDeserializer,
    TqDeleteFun      pDeleter,
    int32_t          tqConfigFlag
  );
int32_t       tqStoreClose(TqMetaStore*);
//int32_t       tqStoreDelete(TqMetaStore*);
//int32_t       tqStoreCommitAll(TqMetaStore*);
int32_t       tqStorePersist(TqMetaStore*);
//clean deleted idx and data from persistent file
int32_t       tqStoreCompact(TqMetaStore*);

void*   tqHandleGet(TqMetaStore*, int64_t key);
//make it unpersist
void*   tqHandleTouchGet(TqMetaStore*, int64_t key);
int32_t tqHandleMovePut(TqMetaStore*, int64_t key, void* value);
int32_t tqHandleCopyPut(TqMetaStore*, int64_t key, void* value, size_t vsize);
//delete committed kv pair
//notice that a delete action still needs to be committed
int32_t tqHandleDel(TqMetaStore*, int64_t key);
int32_t tqHandleCommit(TqMetaStore*, int64_t key);
int32_t tqHandleAbort(TqMetaStore*, int64_t key);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _TQ_META_STORE_H_ */
