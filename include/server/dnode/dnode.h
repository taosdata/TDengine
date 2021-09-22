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

#ifndef _TD_DNODE_H_
#define _TD_DNODE_H_

#ifdef __cplusplus
extern "C" {
#endif

struct SRpcMsg;
struct SRpcEpSet;

/**
 * Initialize and start the dnode module
 * 
 * @return Error Code
 */
int32_t dnodeInit();

/**
 * Stop and cleanup dnode module
 */
void dnodeCleanup();

/**
 * Send messages to other dnodes, such as create vnode message.
 *
 * @param epSet The endpoint list of the dnodes.
 * @param rpcMsg Message to be sent.
 */
void dnodeSendMsgToDnode(SRpcEpSet *epSet, SRpcMsg *rpcMsg);

/**
 * Send messages to mnode, such as config message.
 *
 * @param rpcMsg Message to be sent.
 */
void dnodeSendMsgToMnode(SRpcMsg *rpcMsg);

/**
 * Get the corresponding endpoint information from dnodeId.
 *
 * @param dnodeId
 * @param ep The endpoint of dnode
 * @param fqdn The fqdn of dnode
 * @param port The port of dnode
 */
void dnodeGetDnodeEp(int32_t dnodeId, char *ep, char *fqdn, uint16_t *port);

/**
 * Report to dnode the start-up steps of other modules
 *
 * @param name Name of the start-up phase.
 * @param desc Description of the start-up phase.
 */
void dnodeReportStartup(char *name, char *desc);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_H_*/