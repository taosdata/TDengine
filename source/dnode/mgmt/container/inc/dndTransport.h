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

#ifndef _TD_DND_TRANSPORT_H_
#define _TD_DND_TRANSPORT_H_

#include "dndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t dndInitServer(SDnode *pDnode);
void    dndCleanupServer(SDnode *pDnode);
int32_t dndInitClient(SDnode *pDnode);
void    dndCleanupClient(SDnode *pDnode);
int32_t dndInitMsgHandle(SDnode *pDnode);

int32_t dndSendReqToMnode(void *wrapper, SRpcMsg *pMsg);
int32_t dndSendReqToDnode(void *wrapper, SEpSet *pEpSet, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_TRANSPORT_H_*/
