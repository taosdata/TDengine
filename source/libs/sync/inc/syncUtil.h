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

#ifndef _TD_LIBS_SYNC_UTIL_H
#define _TD_LIBS_SYNC_UTIL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "syncInt.h"

uint64_t syncUtilAddr2U64(const char* host, uint16_t port);
void     syncUtilU642Addr(uint64_t u64, char* host, int64_t len, uint16_t* port);
void     syncUtilnodeInfo2EpSet(const SNodeInfo* pInfo, SEpSet* pEpSet);
void     syncUtilraftId2EpSet(const SRaftId* raftId, SEpSet* pEpSet);
bool     syncUtilnodeInfo2raftId(const SNodeInfo* pInfo, SyncGroupId vgId, SRaftId* raftId);
bool     syncUtilSameId(const SRaftId* pId1, const SRaftId* pId2);
bool     syncUtilEmptyId(const SRaftId* pId);

int32_t     syncUtilElectRandomMS(int32_t min, int32_t max);
int32_t     syncUtilQuorum(int32_t replicaNum);
cJSON*      syncUtilNodeInfo2Json(const SNodeInfo* p);
cJSON*      syncUtilRaftId2Json(const SRaftId* p);
char*       syncUtilRaftId2Str(const SRaftId* p);
const char* syncStr(ESyncState state);
char*       syncUtilPrintBin(char* ptr, uint32_t len);
char*       syncUtilPrintBin2(char* ptr, uint32_t len);
void        syncUtilMsgHtoN(void* msg);
void        syncUtilMsgNtoH(void* msg);
bool        syncUtilUserPreCommit(tmsg_t msgType);
bool        syncUtilUserCommit(tmsg_t msgType);
bool        syncUtilUserRollback(tmsg_t msgType);

void syncPrintNodeLog(const char* flags, ELogLevel level, int32_t dflag, SSyncNode* pNode, const char* format, ...);
void syncPrintSnapshotSenderLog(const char* flags, ELogLevel level, int32_t dflag, SSyncSnapshotSender* pSender,
                                const char* format, ...);
void syncPrintSnapshotReceiverLog(const char* flags, ELogLevel level, int32_t dflag, SSyncSnapshotReceiver* pReceiver,
                                  const char* format, ...);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_UTIL_H*/
