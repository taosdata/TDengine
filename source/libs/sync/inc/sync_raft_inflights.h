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
 * along with this program. If not, see <http: *www.gnu.org/licenses/>.
 */

#ifndef TD_SYNC_RAFT_INFLIGHTS_H
#define TD_SYNC_RAFT_INFLIGHTS_H

#include "sync.h"

/**
 * SSyncRaftInflights limits the number of MsgApp (represented by the largest index
 * contained within) sent to followers but not yet acknowledged by them. Callers
 * use syncRaftInflightFull() to check whether more messages can be sent, 
 * call syncRaftInflightAdd() whenever they are sending a new append, 
 * and release "quota" via FreeLE() whenever an ack is received.
**/
typedef struct SSyncRaftInflights {
  /* the starting index in the buffer */
  int start;

  /* number of inflights in the buffer */
  int count;

  /* the size of the buffer */
  int size;

	/** 
   * buffer contains the index of the last entry
	 * inside one message.
   **/
  SyncIndex* buffer;
} SSyncRaftInflights;

SSyncRaftInflights* syncRaftOpenInflights(int size);
void syncRaftCloseInflights(SSyncRaftInflights*);

static FORCE_INLINE void syncRaftInflightReset(SSyncRaftInflights* inflights) {  
  inflights->count = 0;
  inflights->start = 0;
}

static FORCE_INLINE bool syncRaftInflightFull(SSyncRaftInflights* inflights) {
  return inflights->count == inflights->size;
}

/**
 * syncRaftInflightAdd notifies the Inflights that a new message with the given index is being
 * dispatched. syncRaftInflightFull() must be called prior to syncRaftInflightAdd() 
 * to verify that there is room for one more message, 
 * and consecutive calls to add syncRaftInflightAdd() must provide a
 * monotonic sequence of indexes.
 **/
void syncRaftInflightAdd(SSyncRaftInflights* inflights, SyncIndex inflightIndex);

/**
 * syncRaftInflightFreeLE frees the inflights smaller or equal to the given `to` flight.
 **/
void syncRaftInflightFreeLE(SSyncRaftInflights* inflights, SyncIndex toIndex);

/** 
 * syncRaftInflightFreeFirstOne releases the first inflight. 
 * This is a no-op if nothing is inflight.
 **/
void syncRaftInflightFreeFirstOne(SSyncRaftInflights* inflights);

#endif /* TD_SYNC_RAFT_INFLIGHTS_H */