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

#ifndef _TD_VNODE_H_
#define _TD_VNODE_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  /**
   * Send messages to other dnodes, such as create vnode message.
   *
   * @param epSet, the endpoint list of dnodes.
   * @param rpcMsg, message to be sent.
   */
  void (*SendMsgToDnode)(struct SRpcEpSet *epSet, struct SRpcMsg *rpcMsg);

  /**
   * Send messages to mnode, such as config message.
   *
   * @param rpcMsg, message to be sent.
   */
  void (*SendMsgToMnode)(struct SRpcMsg *rpcMsg);

  /**
   * Get the corresponding endpoint information from dnodeId.
   *
   * @param dnodeId, the id ot dnode.
   * @param ep, the endpoint of dnode.
   * @param fqdn, the fqdn of dnode.
   * @param port, the port of dnode.
   */
  void (*GetDnodeEp)(int32_t dnodeId, char *ep, char *fqdn, uint16_t *port);

} SVnodeFp;

typedef struct {
  SVnodeFp fp;
} SVnodePara;

/**
 * Start initialize vnode module.
 *
 * @param para, initialization parameters.
 * @return Error code.
 */
int32_t vnodeInit(SVnodePara para);

/**
 * Cleanup vnode module.
 */
void vnodeCleanup();

typedef struct {
  int32_t unused;
} SVnodeStat;

/**
 * Get the statistical information of vnode.
 *
 * @param stat, statistical information.
 * @return Error Code.
 */
int32_t vnodeGetStatistics(SVnodeStat *stat);

/**
 * Get the status of all vnodes.
 *
 * @param status, status msg.
 */
void vnodeGetStatus(struct SStatusMsg *status);

/**
 * Set access permissions for all vnodes.
 *
 * @param access, access permissions of vnodes.
 * @param numOfVnodes, the size of vnodes.
 */
void vnodeSetAccess(struct SVgroupAccess *access, int32_t numOfVnodes);

/**
 * Interface for processing messages.
 *
 * @param msg, message to be processed.
 */
void vnodeProcessMsg(SRpcMsg *msg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_H_*/
