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

#ifndef _TD_LIBS_SYNC_TYPE_H
#define _TD_LIBS_SYNC_TYPE_H

typedef int32_t SyncTime;

typedef struct SSyncRaft SSyncRaft;

typedef struct SSyncRaftLog SSyncRaftLog;

#ifndef MIN
#define MIN(x, y) (((x) < (y)) ? (x) : (y))
#endif

#ifndef MAX
#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#endif

typedef enum {
  SYNC_RAFT_CAMPAIGN_PRE_ELECTION = 0,
  SYNC_RAFT_CAMPAIGN_ELECTION = 1,
  SYNC_RAFT_CAMPAIGN_TRANSFER = 3,
} SyncRaftElectionType;

typedef enum {
  SYNC_RAFT_VOTE_RESP_UNKNOWN = 0,
  SYNC_RAFT_VOTE_RESP_GRANT   = 1,
  SYNC_RAFT_VOTE_RESP_REJECT  = 2,
} SyncRaftVoteRespType;

#endif  /* _TD_LIBS_SYNC_TYPE_H */
