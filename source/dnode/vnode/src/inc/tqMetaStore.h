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
#include "tqInt.h"

#ifdef __cplusplus
extern "C" {
#endif

STqMetaStore* tqStoreOpen(STQ* pTq, const char* path, FTqSerialize pSerializer, FTqDeserialize pDeserializer,
                          FTqDelete pDeleter, int32_t tqConfigFlag);
int32_t       tqStoreClose(STqMetaStore*);
// int32_t       tqStoreDelete(TqMetaStore*);
// int32_t       tqStoreCommitAll(TqMetaStore*);
int32_t tqStorePersist(STqMetaStore*);
// clean deleted idx and data from persistent file
int32_t tqStoreCompact(STqMetaStore*);

void* tqHandleGet(STqMetaStore*, int64_t key);
// make it unpersist
void*   tqHandleTouchGet(STqMetaStore*, int64_t key);
int32_t tqHandleMovePut(STqMetaStore*, int64_t key, void* value);
int32_t tqHandleCopyPut(STqMetaStore*, int64_t key, void* value, size_t vsize);
// delete committed kv pair
// notice that a delete action still needs to be committed
int32_t tqHandleDel(STqMetaStore*, int64_t key);
int32_t tqHandlePurge(STqMetaStore*, int64_t key);
int32_t tqHandleCommit(STqMetaStore*, int64_t key);
int32_t tqHandleAbort(STqMetaStore*, int64_t key);

#ifdef __cplusplus
}
#endif

#endif /* ifndef _TQ_META_STORE_H_ */
