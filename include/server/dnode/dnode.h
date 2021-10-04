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

struct SRpcEpSet;
struct SRpcMsg;
struct Dnode;

/**
 * Initialize and start the dnode module.
 *
 * @return Instance of dnode module.
 */
struct Dnode *dnodeCreateInstance();

/**
 * Stop and cleanup dnode module.
 *
 * @param dnode, instance of dnode module.
 */
void dnodeDropInstance(struct Dnode *dnode);

/**
 * Send messages to other dnodes, such as create vnode message.
 *
 * @param dnode, the instance of Dnode module.
 * @param epSet, the endpoint list of the dnodes.
 * @param rpcMsg, message to be sent.
 */
void dnodeSendMsgToDnode(struct Dnode *dnode, struct SRpcEpSet *epSet, struct SRpcMsg *rpcMsg);

/**
 * Send messages to mnode, such as config message.
 *
 * @param dnode, the instance of dnode module.
 * @param rpcMsg, message to be sent.
 */
void dnodeSendMsgToMnode(struct Dnode *dnode, struct SRpcMsg *rpcMsg);

/**
 * Send redirect message to dnode or shell.
 *
 * @param dnode, the instance of dnode module.
 * @param rpcMsg, message to be sent.
 * @param forShell, used to identify whether to send to shell or dnode.
 */
void dnodeSendRedirectMsg(struct Dnode *dnode, struct SRpcMsg *rpcMsg, bool forShell);

/**
 * Get the corresponding endpoint information from dnodeId.
 *
 * @param dnode, the instance of dnode module.
 * @param dnodeId, the id ot dnode.
 * @param ep, the endpoint of dnode.
 * @param fqdn, the fqdn of dnode.
 * @param port, the port of dnode.
 */
void dnodeGetDnodeEp(struct Dnode *dnode, int32_t dnodeId, char *ep, char *fqdn, uint16_t *port);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_H_*/
