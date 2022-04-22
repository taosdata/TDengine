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

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "syncInt.h"
#include "syncMessage.h"
#include "taosdef.h"

// ---- encode / decode
uint64_t syncUtilAddr2U64(const char* host, uint16_t port);
void     syncUtilU642Addr(uint64_t u64, char* host, size_t len, uint16_t* port);
void     syncUtilnodeInfo2EpSet(const SNodeInfo* pNodeInfo, SEpSet* pEpSet);
void     syncUtilraftId2EpSet(const SRaftId* raftId, SEpSet* pEpSet);
void     syncUtilnodeInfo2raftId(const SNodeInfo* pNodeInfo, SyncGroupId vgId, SRaftId* raftId);
bool     syncUtilSameId(const SRaftId* pId1, const SRaftId* pId2);
bool     syncUtilEmptyId(const SRaftId* pId);

// ---- SSyncBuffer ----
void syncUtilbufBuild(SSyncBuffer* syncBuf, size_t len);
void syncUtilbufDestroy(SSyncBuffer* syncBuf);
void syncUtilbufCopy(const SSyncBuffer* src, SSyncBuffer* dest);
void syncUtilbufCopyDeep(const SSyncBuffer* src, SSyncBuffer* dest);

// ---- misc ----
int32_t     syncUtilRand(int32_t max);
int32_t     syncUtilElectRandomMS(int32_t min, int32_t max);
int32_t     syncUtilQuorum(int32_t replicaNum);
cJSON*      syncUtilNodeInfo2Json(const SNodeInfo* p);
cJSON*      syncUtilRaftId2Json(const SRaftId* p);
char*       syncUtilRaftId2Str(const SRaftId* p);
const char* syncUtilState2String(ESyncState state);
bool        syncUtilCanPrint(char c);
char*       syncUtilprintBin(char* ptr, uint32_t len);
char*       syncUtilprintBin2(char* ptr, uint32_t len);
SyncIndex   syncUtilMinIndex(SyncIndex a, SyncIndex b);
SyncIndex   syncUtilMaxIndex(SyncIndex a, SyncIndex b);
void        syncUtilMsgHtoN(void* msg);
void        syncUtilMsgNtoH(void* msg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_UTIL_H*/
