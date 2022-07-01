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

#ifndef _TD_LIBS_SYNC_APPEND_ENTRIES_REPLY_H
#define _TD_LIBS_SYNC_APPEND_ENTRIES_REPLY_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "syncInt.h"
#include "syncMessage.h"
#include "taosdef.h"

// TLA+ Spec
// HandleAppendEntriesResponse(i, j, m) ==
//    /\ m.mterm = currentTerm[i]
//    /\ \/ /\ m.msuccess \* successful
//          /\ nextIndex'  = [nextIndex  EXCEPT ![i][j] = m.mmatchIndex + 1]
//          /\ matchIndex' = [matchIndex EXCEPT ![i][j] = m.mmatchIndex]
//       \/ /\ \lnot m.msuccess \* not successful
//          /\ nextIndex' = [nextIndex EXCEPT ![i][j] =
//                               Max({nextIndex[i][j] - 1, 1})]
//          /\ UNCHANGED <<matchIndex>>
//    /\ Discard(m)
//    /\ UNCHANGED <<serverVars, candidateVars, logVars, elections>>
//
int32_t syncNodeOnAppendEntriesReplyCb(SSyncNode* ths, SyncAppendEntriesReply* pMsg);
int32_t syncNodeOnAppendEntriesReplySnapshotCb(SSyncNode* ths, SyncAppendEntriesReply* pMsg);
int32_t syncNodeOnAppendEntriesReplySnapshot2Cb(SSyncNode* ths, SyncAppendEntriesReply* pMsg);

typedef struct SReaderParam {
  SyncIndex start;
  SyncIndex end;
} SReaderParam;

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_APPEND_ENTRIES_REPLY_H*/
