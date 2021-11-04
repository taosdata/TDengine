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
#include "raft_message.h"

int syncRaftHandleElectionMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  if (pRaft->state == TAOS_SYNC_ROLE_LEADER) {
    syncDebug("%d ignoring RAFT_MSG_INTERNAL_ELECTION because already leader", pRaft->selfId);
    return 0;
  }

  // if there is pending uncommitted config,cannot start election
  if (syncRaftLogNumOfPendingConf(pRaft->log) > 0 && syncRaftHasUnappliedLog(pRaft->log)) {
    syncWarn("[%d:%d] cannot syncRaftStartElection at term %" PRId64 " since there are still pending configuration changes to apply",
      pRaft->selfGroupId, pRaft->selfId, pRaft->term);
    return 0;
  }

  syncInfo("[%d:%d] is starting a new election at term %" PRId64 "", pRaft->selfGroupId, pRaft->selfId, pRaft->term);

  if (pRaft->preVote) {
    syncRaftStartElection(pRaft, SYNC_RAFT_CAMPAIGN_PRE_ELECTION);
  } else {
    syncRaftStartElection(pRaft, SYNC_RAFT_CAMPAIGN_ELECTION);
  }

  return 0;
}
