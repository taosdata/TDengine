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

#ifndef TD_SYNC_RAFT_RESTORE_H
#define TD_SYNC_RAFT_RESTORE_H

#include "sync_type.h"
#include "sync_raft_proto.h"

// syncRaftRestoreConfig takes a Changer (which must represent an empty configuration), and
// runs a sequence of changes enacting the configuration described in the
// ConfState.
//
// TODO(tbg) it's silly that this takes a Changer. Unravel this by making sure
// the Changer only needs a ProgressMap (not a whole Tracker) at which point
// this can just take LastIndex and MaxInflight directly instead and cook up
// the results from that alone.
int syncRaftRestoreConfig(SSyncRaftChanger* changer, const SSyncConfigState* cs);

#endif /* TD_SYNC_RAFT_RESTORE_H */
