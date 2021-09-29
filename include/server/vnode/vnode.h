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

struct Dnode;

typedef struct {
  /**
   * Send messages to other dnodes, such as create vnode message.
   *
   * @param dnode, the instance of dnode module.
   * @param epSet, the endpoint list of dnodes.
   * @param rpcMsg, message to be sent.
   */
  void (*SendMsgToDnode)(struct Dnode *dnode, struct SRpcEpSet *epSet, struct SRpcMsg *rpcMsg);

  /**
   * Send messages to mnode, such as config message.
   *
   * @param dnode, the instance of dnode module.
   * @param rpcMsg, message to be sent.
   */
  void (*SendMsgToMnode)(struct Dnode *dnode, struct SRpcMsg *rpcMsg);

  /**
   * Get the corresponding endpoint information from dnodeId.
   *
   * @param dnode, the instance of dnode module.
   * @param dnodeId, the id ot dnode.
   * @param ep, the endpoint of dnode.
   * @param fqdn, the fqdn of dnode.
   * @param port, the port of dnode.
   */
  void (*GetDnodeEp)(struct Dnode *dnode, int32_t dnodeId, char *ep, char *fqdn, uint16_t *port);

} SVnodeFp;

typedef struct {
  struct Dnode *dnode;
  SVnodeFp      fp;
} SVnodePara;

/**
 * Start initialize vnode module.
 *
 * @param para, initialization parameters.
 * @return Instance of vnode module.
 */
struct Vnode *vnodeCreateInstance(SVnodePara para);

/**
 * Cleanup vnode module.
 *
 * @param vnode, instance of vnode module.
 */
void vnodeCleanupInstance(struct Vnode *vnode);

typedef struct {
  int32_t unused;
} SVnodeStat;

/**
 * Get the statistical information of vnode.
 *
 * @param vnode, instance of vnode module.
 * @param sta, statistical information.
 * @return Error Code.
 */
int32_t vnodeGetStatistics(struct Vnode *vnode, SVnodeStat *stat);

/**
 * Interface for processing messages.
 *
 * @param vnode, instance of vnode module.
 * @param msg, message to be processed.
 */
void vnodeProcessMsg(struct Vnode *vnode, SRpcMsg *msg);

/**
 * Get the status of all vnodes.
 *
 * @param vnode, instance of vnode module.
 * @param status, status msg.
 */
void vnodeGetStatus(struct Vnode *vnode, struct SStatusMsg *status);

/**
 * Set access permissions for all vnodes.
 *
 * @param vnode, instance of vnode module.
 * @param access, access permissions of vnodes.
 * @param numOfVnodes, the size of vnodes.
 */
void vnodeSetAccess(struct Vnode *vnode, struct SVgroupAccess *access, int32_t numOfVnodes);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_H_*/
