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

#include "sdbint.h"

char* sdbStatusStr[] = {"offline", "unsynced", "syncing", "serving", "null"};

char* sdbRoleStr[] = {"unauthed", "undecided", "master", "slave", "null"};

#ifndef CLUSTER

/*
 * Lite Version sync request is always successful
 */
int sdbForwardDbReqToPeer(SSdbTable *pTable, char type, char *data, int dataLen) {
  return 0;
}

/*
 * Lite Version does not need to initialize peers
 */
int sdbInitPeers(char *directory) {
  return 0;
}

/*
 * Lite Version does not need to cleanup peers
 */
void sdbCleanUpPeers(){}

#endif