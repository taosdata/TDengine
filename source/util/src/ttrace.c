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
#include "ttrace.h"
#include "taos.h"
#include "thash.h"
#include "tuuid.h"

// clang-format off
//static TdThreadOnce init = PTHREAD_ONCE_INIT;
//static void *              ids = NULL;
//static TdThreadMutex       mtx;
//
//void traceInit() {
//  ids = taosHashInit(4096, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
//  taosThreadMutexInit(&mtx, NULL);
//}
//void traceCreateEnv() {
//  taosThreadOnce(&init, traceInit);
//}
//void traceDestroyEnv() {
//  taosThreadMutexDestroy(&mtx);
//  taosHashCleanup(ids);
//}
//
//STraceId traceInitId(STraceSubId *h, STraceSubId *l) {
//  STraceId id = *h; 
//  id = ((id << 32) & 0xFFFFFFFF) |  ((*l) & 0xFFFFFFFF);  
//  return id;
//}
//void traceId2Str(STraceId *id, char *buf) {
//  int32_t f = (*id >> 32) & 0xFFFFFFFF;
//  int32_t s = (*id) & 0xFFFFFFFF;
//  sprintf(buf, "%d:%d", f, s); 
//}
//
//void traceSetSubId(STraceId *id, STraceSubId *subId) {
//  int32_t parent = ((*id >> 32) & 0xFFFFFFFF); 
//  taosThreadMutexLock(&mtx);    
//  taosHashPut(ids, subId, sizeof(*subId), &parent, sizeof(parent));
//  taosThreadMutexUnlock(&mtx);    
//}
//
//STraceSubId traceGetParentId(STraceId *id) {
//  int32_t parent = ((*id >> 32) & 0xFFFFFFFF);   
//  taosThreadMutexLock(&mtx);    
//  STraceSubId *p = taosHashGet(ids, (void *)&parent, sizeof(parent));
//  parent = *p;
//  taosThreadMutexUnlock(&mtx);    
//
//  return parent;  
//}
//
//STraceSubId  traceGenSubId() {
//  return tGenIdPI32();
//}
//void    traceSetRootId(STraceId *traceid, int64_t rootId) {
//  traceId->rootId = rootId;
//}
//int64_t traceGetRootId(STraceId *traceId);
//
//void    traceSetMsgId(STraceId *traceid, int64_t msgId);
//int64_t traceGetMsgId(STraceId *traceid);
//
//char *trace2Str(STraceId *id);
//
//void traceSetSubId(STraceId *id, int32_t *subId);
// clang-format on
