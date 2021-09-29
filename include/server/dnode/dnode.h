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

/**
 * Initialize and start the dnode module
 *
 * @return Instance of dnode module
 */
struct Dnode *dnodeCreateInstance();

/**
 * Stop and cleanup dnode module
 *
 * @param dnode Instance of dnode module
 */
void dnodeCleanupInstance(struct Dnode *dnode);

/**
 * Send messages to other dnodes, such as create vnode message.
 *
 * @param dnode The instance of Dnode module
 * @param epSet The endpoint list of the dnodes.
 * @param rpcMsg Message to be sent.
 */
void dnodeSendMsgToDnode(struct Dnode *dnode, struct SRpcEpSet *epSet, struct SRpcMsg *rpcMsg);

/**
 * Send messages to mnode, such as config message.
 *
 * @param dnode The instance of dnode module
 * @param rpcMsg Message to be sent.
 */
void dnodeSendMsgToMnode(struct Dnode *dnode, struct SRpcMsg *rpcMsg);

/**
 * Send redirect message to dnode or shell.
 *
 * @param dnode The instance of dnode module
 * @param rpcMsg Message to be sent.
 * @param forShell Used to identify whether to send to shell or dnode.
 */
void dnodeSendRedirectMsg(struct Dnode *dnode, struct SRpcMsg *rpcMsg, bool forShell);

/**
 * Get the corresponding endpoint information from dnodeId.
 *
 * @param dnode The instance of Dnode module
 * @param dnodeId The id ot dnode
 * @param ep The endpoint of dnode
 * @param fqdn The fqdn of dnode
 * @param port The port of dnode
 */
void dnodeGetDnodeEp(struct Dnode *dnode, int32_t dnodeId, char *ep, char *fqdn, uint16_t *port);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_H_*/
