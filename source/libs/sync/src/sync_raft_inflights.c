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

#include "sync_raft_inflights.h"

SSyncRaftInflights* syncRaftOpenInflights(int size) {
  SSyncRaftInflights* inflights = (SSyncRaftInflights*)malloc(sizeof(SSyncRaftInflights));
  if (inflights == NULL) {
    return NULL;
  }
  SyncIndex* buffer = (SyncIndex*)malloc(sizeof(SyncIndex) * size);
  if (buffer == NULL) {
    free(inflights);
    return NULL;
  }
  *inflights = (SSyncRaftInflights) {
    .buffer = buffer,
    .count  = 0,
    .size   = 0,
    .start  = 0,
  };

  return inflights;
}

void syncRaftCloseInflights(SSyncRaftInflights* inflights) {
  free(inflights->buffer);
  free(inflights);
}

/**
 * syncRaftInflightAdd notifies the Inflights that a new message with the given index is being
 * dispatched. syncRaftInflightFull() must be called prior to syncRaftInflightAdd() 
 * to verify that there is room for one more message, 
 * and consecutive calls to add syncRaftInflightAdd() must provide a
 * monotonic sequence of indexes.
 **/
void syncRaftInflightAdd(SSyncRaftInflights* inflights, SyncIndex inflightIndex) {
  assert(!syncRaftInflightFull(inflights));

  int next = inflights->start + inflights->count;
  int size = inflights->size;
  /* is next wrapped around buffer? */
  if (next >= size) {
    next -= size;
  }

  inflights->buffer[next] = inflightIndex;
  inflights->count++;
}

/**
 * syncRaftInflightFreeLE frees the inflights smaller or equal to the given `to` flight.
 **/
void syncRaftInflightFreeLE(SSyncRaftInflights* inflights, SyncIndex toIndex) {
  if (inflights->count == 0 || toIndex < inflights->buffer[inflights->start]) {
    /* out of the left side of the window */
    return;
  }

  int i, idx;
  for (i = 0, idx = inflights->start; i < inflights->count; i++) {
    if (toIndex < inflights->buffer[idx]) { // found the first large inflight
      break;
    }

    // increase index and maybe rotate
    int size = inflights->size;
    idx++;
    if (idx >= size) {
      idx -= size;
    }
  }

  // free i inflights and set new start index
  inflights->count -= i;
  inflights->start  = idx;
  assert(inflights->count >= 0);
  if (inflights->count == 0) {
		// inflights is empty, reset the start index so that we don't grow the
		// buffer unnecessarily.    
    inflights->start = 0;
  }
}

/** 
 * syncRaftInflightFreeFirstOne releases the first inflight. 
 * This is a no-op if nothing is inflight.
 **/
void syncRaftInflightFreeFirstOne(SSyncRaftInflights* inflights) {
  syncRaftInflightFreeLE(inflights, inflights->buffer[inflights->start]);
}
