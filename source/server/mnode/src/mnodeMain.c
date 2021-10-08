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

int32_t mnodeInit(SMnodePara para) { return 0; }

void mnodeCleanup() {}

int32_t mnodeDeploy(struct SMInfos *minfos) { return 0; }

void mnodeUnDeploy() {}

bool mnodeIsServing() { return false; }

int32_t mnodeGetStatistics(SMnodeStat *stat) { return 0; }

int32_t mnodeRetriveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey) { return 0; }

void mnodeProcessMsg(SRpcMsg *rpcMsg) {}
