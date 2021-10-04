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

#include "mnodeInt.h"

struct Mnode *mnodeCreateInstance(SMnodePara para) {
  return NULL;
}

void mnodeDropInstance(struct Mnode *vnode) {}

int32_t mnodeDeploy(struct Mnode *mnode, struct SMInfos *minfos) { return 0; }

void mnodeUnDeploy(struct Mnode *mnode) {}

bool mnodeIsServing(struct Mnode *mnode) { return false; }

int32_t mnodeGetStatistics(struct Mnode *mnode, SMnodeStat *stat) { return 0; }

int32_t mnodeRetriveAuth(struct Mnode *mnode, char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  return 0;
}

void mnodeProcessMsg(struct Mnode *mnode, SRpcMsg *rpcMsg) {}
