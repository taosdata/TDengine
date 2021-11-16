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

#include "sync_raft_config_change.h"
#include "sync_raft_restore.h"
#include "sync_raft_progress_tracker.h"

static int toConfChangeSingle(const SSyncConfigState* cs, SSyncConfChangeSingleArray* out, SSyncConfChangeSingleArray* in);

// syncRaftRestoreConfig takes a Changer (which must represent an empty configuration), and
// runs a sequence of changes enacting the configuration described in the
// ConfState.
//
// TODO(tbg) it's silly that this takes a Changer. Unravel this by making sure
// the Changer only needs a ProgressMap (not a whole Tracker) at which point
// this can just take LastIndex and MaxInflight directly instead and cook up
// the results from that alone.
int syncRaftRestoreConfig(SSyncRaftChanger* changer, const SSyncConfigState* cs) {
  SSyncConfChangeSingleArray outgoing;
  SSyncConfChangeSingleArray incoming;
  SSyncConfChangeSingleArray css;
  SSyncRaftProgressTracker* tracker = changer->tracker;
  SSyncRaftProgressTrackerConfig* config = &tracker->config;
  SSyncRaftProgressMap* progressMap = &tracker->progressMap;
  int i, ret;

  ret = toConfChangeSingle(cs, &outgoing, &incoming);
  if (ret != 0) {
    goto out;
  }

  if (outgoing.n == 0) {
    // No outgoing config, so just apply the incoming changes one by one.
    for (i = 0; i < incoming.n; ++i) {
      css = (SSyncConfChangeSingleArray) {
        .n = 1,
        .changes = &incoming.changes[i],
      };
      ret = syncRaftChangerSimpleConfig(changer, &css, config, progressMap);
      if (ret != 0) {
        goto out;
      }
    }
  } else {
		// The ConfState describes a joint configuration.
		//
		// First, apply all of the changes of the outgoing config one by one, so
		// that it temporarily becomes the incoming active config. For example,
		// if the config is (1 2 3)&(2 3 4), this will establish (2 3 4)&().
    for (i = 0; i < outgoing.n; ++i) {
      css = (SSyncConfChangeSingleArray) {
        .n = 1,
        .changes = &outgoing.changes[i],
      };
      ret = syncRaftChangerSimpleConfig(changer, &css, config, progressMap);
      if (ret != 0) {
        goto out;
      }
    }

    ret = syncRaftChangerEnterJoint(changer, cs->autoLeave, &incoming, config, progressMap);
    if (ret != 0) {
      goto out;
    }
  }

out:
  if (incoming.n != 0) free(incoming.changes);
  if (outgoing.n != 0) free(outgoing.changes);
  return ret;
}

// toConfChangeSingle translates a conf state into 1) a slice of operations creating
// first the config that will become the outgoing one, and then the incoming one, and
// b) another slice that, when applied to the config resulted from 1), represents the
// ConfState.
static int toConfChangeSingle(const SSyncConfigState* cs, SSyncConfChangeSingleArray* out, SSyncConfChangeSingleArray* in) {
  int i;

  out->n = in->n = 0;

  out->n = cs->votersOutgoing.replica;
  out->changes = (SSyncConfChangeSingle*)malloc(sizeof(SSyncConfChangeSingle) * out->n);
  if (out->changes == NULL) {
    out->n = 0;
    return -1;
  }
  in->n = cs->votersOutgoing.replica + cs->voters.replica + cs->learners.replica + cs->learnersNext.replica;
  out->changes = (SSyncConfChangeSingle*)malloc(sizeof(SSyncConfChangeSingle) * in->n);
  if (in->changes == NULL) {
    in->n = 0;
    return -1;
  }

	// Example to follow along this code:
	// voters=(1 2 3) learners=(5) outgoing=(1 2 4 6) learners_next=(4)
	//
	// This means that before entering the joint config, the configuration
	// had voters (1 2 4 6) and perhaps some learners that are already gone.
	// The new set of voters is (1 2 3), i.e. (1 2) were kept around, and (4 6)
	// are no longer voters; however 4 is poised to become a learner upon leaving
	// the joint state.
	// We can't tell whether 5 was a learner before entering the joint config,
	// but it doesn't matter (we'll pretend that it wasn't).
	//
	// The code below will construct
	// outgoing = add 1; add 2; add 4; add 6
	// incoming = remove 1; remove 2; remove 4; remove 6
	//            add 1;    add 2;    add 3;
	//            add-learner 5;
	//            add-learner 4;
	//
	// So, when starting with an empty config, after applying 'outgoing' we have
	//
	//   quorum=(1 2 4 6)
	//
	// From which we enter a joint state via 'incoming'
	//
	//   quorum=(1 2 3)&&(1 2 4 6) learners=(5) learners_next=(4)
	//
	// as desired.

  for (i = 0; i < cs->votersOutgoing.replica; ++i) {
    // If there are outgoing voters, first add them one by one so that the
		// (non-joint) config has them all.
    out->changes[i] = (SSyncConfChangeSingle) {
      .type = SYNC_RAFT_Conf_AddNode,
      .nodeId = cs->votersOutgoing.nodeId[i],
    };
  }

	// We're done constructing the outgoing slice, now on to the incoming one
	// (which will apply on top of the config created by the outgoing slice).

	// First, we'll remove all of the outgoing voters.
  int j = 0;
  for (i = 0; i < cs->votersOutgoing.replica; ++i) {
    in->changes[j] = (SSyncConfChangeSingle) {
      .type = SYNC_RAFT_Conf_RemoveNode,
      .nodeId = cs->votersOutgoing.nodeId[i],
    };
    j += 1;
  }
  // Then we'll add the incoming voters and learners.
  for (i = 0; i < cs->voters.replica; ++i) {
    in->changes[j] = (SSyncConfChangeSingle) {
      .type = SYNC_RAFT_Conf_AddNode,
      .nodeId = cs->voters.nodeId[i],
    };
    j += 1;
  }
  for (i = 0; i < cs->learners.replica; ++i) {
    in->changes[j] = (SSyncConfChangeSingle) {
      .type = SYNC_RAFT_Conf_AddLearnerNode,
      .nodeId = cs->learners.nodeId[i],
    };
    j += 1;
  }
	// Same for LearnersNext; these are nodes we want to be learners but which
	// are currently voters in the outgoing config.
  for (i = 0; i < cs->learnersNext.replica; ++i) {
    in->changes[j] = (SSyncConfChangeSingle) {
      .type = SYNC_RAFT_Conf_AddLearnerNode,
      .nodeId = cs->learnersNext.nodeId[i],
    };
    j += 1;
  }
  return 0;
}