/*
 * Copyright (c) 2019 TAOS Data, Inc. <cli@taosdata.com>
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

#ifndef _TD_LIBS_SYNC_RAFT_H
#define _TD_LIBS_SYNC_RAFT_H

#include "sync.h"
#include "raft_message.h"

typedef struct SSyncRaft {
  // owner sync node
  SSyncNode* pNode;

  SSyncInfo info;

} SSyncRaft;

int32_t syncRaftStart(SSyncRaft* pRaft, const SSyncInfo* pInfo);
int32_t syncRaftStep(SSyncRaft* pRaft, const RaftMessage* pMsg);
int32_t syncRaftTick(SSyncRaft* pRaft);

#endif /* _TD_LIBS_SYNC_RAFT_H */