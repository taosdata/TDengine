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

#ifndef _TD_LIBS_SYNC_RAFT_STORE_H
#define _TD_LIBS_SYNC_RAFT_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "syncInt.h"

#define RAFT_STORE_BLOCK_SIZE 512
#define RAFT_STORE_PATH_LEN   (TSDB_FILENAME_LEN * 2)
#define EMPTY_RAFT_ID         ((SRaftId){.addr = 0, .vgId = 0})

int32_t raftStoreOpen(SSyncNode *pNode);
void    raftStoreClose(SSyncNode *pNode);

bool raftStoreHasVoted(SSyncNode *pNode);
void raftStoreVote(SSyncNode *pNode, SRaftId *pRaftId);
void raftStoreClearVote(SSyncNode *pNode);
void raftStoreNextTerm(SSyncNode *pNode);
void raftStoreSetTerm(SSyncNode *pNode, SyncTerm term);
SyncTerm raftStoreGetTerm(SSyncNode *pNode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_RAFT_STORE_H*/
