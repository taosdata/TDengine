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

#ifndef _TD_MNODE_H_
#define _TD_MNODE_H_

#ifdef __cplusplus
extern "C" {
#endif

struct SRpcMsg;

/**
 * Deploy Mnode instances in Dnode.
 * 
 * @return Error Code.
 */
int32_t mnodeDeploy();

/**
 * Delete the Mnode instance deployed in Dnode.
 */
void mnodeUnDeploy();

/**
 * Start Mnode service.
 * 
 * @return Error Code.
 */
int32_t mnodeStart();

/**
 * Stop Mnode service.
 */
int32_t mnodeStop();

/**
 * Interface for processing messages.
 * 
 * @param pMsg Message to be processed.
 * @return Error code
 */
int32_t mnodeProcessMsg(SRpcMsg *pMsg);

/**
 * Whether the Mnode is in service.
 * 
 * @return Server status.
 */
bool mnodeIsServing();

#ifdef __cplusplus
}
#endif

#endif /*_TD_MNODE_H_*/