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

#include "syncInt.h"
#include "raft.h"
#include "raft_log.h"
#include "sync_raft_impl.h"
#include "raft_message.h"

int syncRaftHandleAppendEntriesMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  const RaftMsg_Append_Entries *appendEntries = &(pMsg->appendEntries);
  
  SNodeInfo* pNode = syncRaftGetNodeById(pRaft, pMsg->from);
  if (pNode == NULL) {
    return 0;
  }

  SSyncMessage* pRespMsg = syncNewEmptyAppendRespMsg(pRaft->selfGroupId, pRaft->selfId, pRaft->term);
  if (pRespMsg == NULL) {
    return 0;
  }

  RaftMsg_Append_Resp *appendResp = &(pRespMsg->appendResp);
  // ignore committed logs
  if (syncRaftLogIsCommitted(pRaft->log, appendEntries->index)) {
    appendResp->index = pRaft->log->commitIndex;
    goto out;
  }

  syncInfo("[%d:%d] recv append from %d index %" PRId64"",
      pRaft->selfGroupId, pRaft->selfId, pMsg->from, appendEntries->index); 

out:
  pRaft->io.send(pRespMsg, pNode);
  return 0;
}