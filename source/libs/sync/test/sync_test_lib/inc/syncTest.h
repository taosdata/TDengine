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

#ifndef _TD_LIBS_SYNC_TEST_H
#define _TD_LIBS_SYNC_TEST_H

#ifdef __cplusplus
extern "C" {
#endif

#include "syncInt.h"

#include "tref.h"
#include "wal.h"

#include "tref.h"
#include "syncEnv.h"
#include "syncIO.h"
#include "syncIndexMgr.h"
#include "syncInt.h"
#include "syncMessage.h"
#include "syncRaftCfg.h"
#include "syncRaftEntry.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncRespMgr.h"
#include "syncSnapshot.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"

extern void addEpIntoEpSet(SEpSet* pEpSet, const char* fqdn, uint16_t port);

cJSON* syncEntry2Json(const SSyncRaftEntry* pEntry);
char*  syncEntry2Str(const SSyncRaftEntry* pEntry);
void   syncEntryPrint(const SSyncRaftEntry* pObj);
void   syncEntryPrint2(char* s, const SSyncRaftEntry* pObj);
void   syncEntryLog(const SSyncRaftEntry* pObj);
void   syncEntryLog2(char* s, const SSyncRaftEntry* pObj);

cJSON* raftCache2Json(SRaftEntryHashCache* pObj);
char*  raftCache2Str(SRaftEntryHashCache* pObj);
void   raftCachePrint(SRaftEntryHashCache* pObj);
void   raftCachePrint2(char* s, SRaftEntryHashCache* pObj);
void   raftCacheLog(SRaftEntryHashCache* pObj);
void   raftCacheLog2(char* s, SRaftEntryHashCache* pObj);

cJSON* raftEntryCache2Json(SRaftEntryCache* pObj);
char*  raftEntryCache2Str(SRaftEntryCache* pObj);
void   raftEntryCachePrint(SRaftEntryCache* pObj);
void   raftEntryCachePrint2(char* s, SRaftEntryCache* pObj);
void   raftEntryCacheLog(SRaftEntryCache* pObj);
void   raftEntryCacheLog2(char* s, SRaftEntryCache* pObj);

cJSON* syncAppendEntriesBatch2Json(const SyncAppendEntriesBatch* pMsg);
char*  syncAppendEntriesBatch2Str(const SyncAppendEntriesBatch* pMsg);
void   syncAppendEntriesBatchPrint(const SyncAppendEntriesBatch* pMsg);
void   syncAppendEntriesBatchPrint2(char* s, const SyncAppendEntriesBatch* pMsg);
void   syncAppendEntriesBatchLog(const SyncAppendEntriesBatch* pMsg);
void   syncAppendEntriesBatchLog2(char* s, const SyncAppendEntriesBatch* pMsg);

cJSON* logStore2Json(SSyncLogStore* pLogStore);
char*  logStore2Str(SSyncLogStore* pLogStore);
cJSON* logStoreSimple2Json(SSyncLogStore* pLogStore);
char*  logStoreSimple2Str(SSyncLogStore* pLogStore);
void   logStorePrint(SSyncLogStore* pLogStore);
void   logStorePrint2(char* s, SSyncLogStore* pLogStore);
void   logStoreLog(SSyncLogStore* pLogStore);
void   logStoreLog2(char* s, SSyncLogStore* pLogStore);
void   logStoreSimplePrint(SSyncLogStore* pLogStore);
void   logStoreSimplePrint2(char* s, SSyncLogStore* pLogStore);
void   logStoreSimpleLog(SSyncLogStore* pLogStore);
void   logStoreSimpleLog2(char* s, SSyncLogStore* pLogStore);

cJSON* syncNode2Json(const SSyncNode* pSyncNode);
char*  syncNode2Str(const SSyncNode* pSyncNode);

cJSON* voteGranted2Json(SVotesGranted* pVotesGranted);
char*  voteGranted2Str(SVotesGranted* pVotesGranted);
cJSON* votesRespond2Json(SVotesRespond* pVotesRespond);
char*  votesRespond2Str(SVotesRespond* pVotesRespond);

cJSON* syncUtilNodeInfo2Json(const SNodeInfo* p);
char*  syncUtilRaftId2Str(const SRaftId* p);

cJSON* snapshotSender2Json(SSyncSnapshotSender* pSender);
char*  snapshotSender2Str(SSyncSnapshotSender* pSender);
cJSON* snapshotReceiver2Json(SSyncSnapshotReceiver* pReceiver);
char*  snapshotReceiver2Str(SSyncSnapshotReceiver* pReceiver);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_RAFT_ENTRY_H*/
