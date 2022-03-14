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

#ifndef _TD_DND_MNODE_INT_H_
#define _TD_DND_MNODE_INT_H_

#include "dndInt.h"

#ifdef __cplusplus
extern "C" {
#endif


// interface
SMgmtFp mmGetMgmtFp();

int32_t mmInit(SDnode *pDnode);
void    mmCleanup(SDnode *pDnode);
int32_t mmProcessCreateMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t mmProcessAlterMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);
int32_t mmProcessDropMnodeReq(SDnode *pDnode, SRpcMsg *pRpcMsg);

int32_t mmGetUserAuth(SMgmtWrapper *pWrapper, char *user, char *spi, char *encrypt, char *secret, char *ckey);
int32_t mmGetMonitorInfo(SDnode *pDnode, SMonClusterInfo *pClusterInfo, SMonVgroupInfo *pVgroupInfo,
                         SMonGrantInfo *pGrantInfo);


#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_MNODE_INT_H_*/