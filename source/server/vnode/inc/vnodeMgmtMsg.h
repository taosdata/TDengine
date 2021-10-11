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

#ifndef _TD_VNODE_MGMT_MSG_H_
#define _TD_VNODE_MGMT_MSG_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "vnodeInt.h"

int32_t vnodeProcessCreateVnodeMsg(SRpcMsg *rpcMsg);
int32_t vnodeProcessAlterVnodeMsg(SRpcMsg *rpcMsg);
int32_t vnodeProcessSyncVnodeMsg(SRpcMsg *rpcMsg);
int32_t vnodeProcessCompactVnodeMsg(SRpcMsg *rpcMsg);
int32_t vnodeProcessDropVnodeMsg(SRpcMsg *rpcMsg);
int32_t vnodeProcessAlterStreamReq(SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_MGMT_H_*/
