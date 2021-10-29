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

#ifndef _TD_LIBS_SYNC_RAFT_MESSAGE_H
#define _TD_LIBS_SYNC_RAFT_MESSAGE_H

#include "sync.h"

/** 
 * below define message type which handled by Raft node thread
 * internal message, which communicate in threads, start with RAFT_MSG_INTERNAL_*,
 * internal message use pointer only, need not to be decode/encode
 * outter message start with RAFT_MSG_*, need to implement its decode/encode functions
 **/
typedef enum RaftMessageType {
  // client propose a cmd
  RAFT_MSG_INTERNAL_PROP = 1,

  RAFT_MSG_APPEND,
  RAFT_MSG_APPEND_RESP,

  RAFT_MSG_VOTE,
  RAFT_MSG_VOTE_RESP,

  RAFT_MSG_PRE_VOTE,
  RAFT_MSG_PRE_VOTE_RESP,

} RaftMessageType;

typedef struct RaftMsgInternal_Prop {
  const SSyncBuffer *pBuf;
  bool isWeak;
  void* pData;
} RaftMsgInternal_Prop;

typedef struct RaftMessage {
  RaftMessageType msgType;
  SSyncTerm term;
  SyncNodeId from;
  SyncNodeId to;

  union {
    RaftMsgInternal_Prop propose;
  };
} RaftMessage;

static FORCE_INLINE RaftMessage* syncInitPropMsg(RaftMessage* pMsg, const SSyncBuffer* pBuf, void* pData, bool isWeak) {
  *pMsg = (RaftMessage) {
    .msgType = RAFT_MSG_INTERNAL_PROP,
    .propose = (RaftMsgInternal_Prop) {
      .isWeak = isWeak,
      .pBuf = pBuf,
      .pData = pData,
    },
  };

  return pMsg;
}

static FORCE_INLINE bool syncIsInternalMsg(const RaftMessage* pMsg) {
  return pMsg->msgType == RAFT_MSG_INTERNAL_PROP;
}

#endif  /* _TD_LIBS_SYNC_RAFT_MESSAGE_H */