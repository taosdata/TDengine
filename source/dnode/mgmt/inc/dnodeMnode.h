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

#ifndef _TD_DNODE_MNODE_H_
#define _TD_DNODE_MNODE_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "dnodeInt.h"

int32_t dnodeInitMnode();
void    dnodeCleanupMnode();
int32_t dnodeGetUserAuthFromMnode(char *user, char *spi, char *encrypt, char *secret, char *ckey);

void dnodeProcessMnodeMgmtMsg(SRpcMsg *pMsg, SEpSet *pEpSet);
void dnodeProcessMnodeReadMsg(SRpcMsg *pMsg, SEpSet *pEpSet);
void dnodeProcessMnodeWriteMsg(SRpcMsg *pMsg, SEpSet *pEpSet);
void dnodeProcessMnodeSyncMsg(SRpcMsg *pMsg, SEpSet *pEpSet);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_MNODE_H_*/