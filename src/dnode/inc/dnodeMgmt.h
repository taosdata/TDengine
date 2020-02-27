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

#ifndef TDENGINE_DNODE_MGMT_H
#define TDENGINE_DNODE_MGMT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

int32_t dnodeInitMgmt();
void dnodeInitMgmtIp();

void dnodeProcessMsgFromMgmt(int8_t msgType, void *pCont, int32_t contLen, void *pConn, int32_t code);
void dnodeSendMsgToMnode(int8_t msgType, void *pCont, int32_t contLen);
void dnodeSendRspToMnode(void *pConn, int8_t msgType, int32_t code, void *pCont, int32_t contLen);

void dnodeSendVpeerCfgMsg(int32_t vnode);
void dnodeSendMeterCfgMsg(int32_t vnode, int32_t sid);



#ifdef __cplusplus
}
#endif

#endif
