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

#ifndef TD_SYNC_RAFT_CONFIG_CHANGE_H
#define TD_SYNC_RAFT_CONFIG_CHANGE_H

#include "sync_type.h"
#include "sync_raft_proto.h"

/**
 * Changer facilitates configuration changes. It exposes methods to handle
 * simple and joint consensus while performing the proper validation that allows
 * refusing invalid configuration changes before they affect the active
 * configuration.
 **/
struct SSyncRaftChanger {
  SSyncRaftProgressTracker* tracker;
  SyncIndex lastIndex;  
};

typedef int (*configChangeFp)(SSyncRaftChanger* changer, const SSyncConfChangeSingleArray* css,
                            SSyncRaftProgressTrackerConfig* config, SSyncRaftProgressMap* progressMap);

int syncRaftChangerSimpleConfig(SSyncRaftChanger* changer, const SSyncConfChangeSingleArray* css,
                            SSyncRaftProgressTrackerConfig* config, SSyncRaftProgressMap* progressMap);

int syncRaftChangerEnterJoint(SSyncRaftChanger* changer, bool autoLeave, const SSyncConfChangeSingleArray* css,
                            SSyncRaftProgressTrackerConfig* config, SSyncRaftProgressMap* progressMap);

#endif /* TD_SYNC_RAFT_CONFIG_CHANGE_H */
