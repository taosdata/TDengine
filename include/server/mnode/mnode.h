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

struct Dnode;

typedef struct {
  /**
   * Send messages to other dnodes, such as create vnode message.
   *
   * @param dnode, the instance of dnode module.
   * @param epSet, the endpoint list of the dnodes.
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
   * Send redirect message to dnode or shell.
   *
   * @param dnode, the instance of dnode module.
   * @param rpcMsg, message to be sent.
   * @param forShell, used to identify whether to send to shell or dnode.
   */
  void (*SendRedirectMsg)(struct Dnode *dnode, struct SRpcMsg *rpcMsg, bool forShell);

  /**
   * Get the corresponding endpoint information from dnodeId.
   *
   * @param dnode, the instance of dDnode module.
   * @param dnodeId, the id ot dnode.
   * @param ep, the endpoint of dnode.
   * @param fqdn, the fqdn of dnode.
   * @param port, the port of dnode.
   */
  void (*GetDnodeEp)(struct Dnode *dnode, int32_t dnodeId, char *ep, char *fqdn, uint16_t *port);

} SMnodeFp;

typedef struct {
  struct Dnode *dnode;
  SMnodeFp      fp;
  char          clusterId[TSDB_CLUSTER_ID_LEN];
  int32_t       dnodeId;
} SMnodePara;

/**
 * Initialize and start mnode module.
 *
 * @param para, initialization parameters.
 * @return Instance of mnode module.
 */
struct Mnode *mnodeCreateInstance(SMnodePara para);

/**
 * Stop and cleanup mnode module.
 *
 * @param mnode, instance of mnode module.
 */
void mnodeCleanupInstance(struct Mnode *vnode);

/**
 * Deploy mnode instances in dnode.
 *
 * @param mnode, instance of mnode module.
 * @param minfos, server information used to deploy the mnode instance.
 * @return Error Code.
 */
int32_t mnodeDeploy(struct Mnode *mnode, struct SMInfos *minfos);

/**
 * Delete the mnode instance deployed in dnode.
 *
 * @param mnode, instance of mnode module.
 */
void mnodeUnDeploy(struct Mnode *mnode);

/**
 * Interface for processing messages.
 *
 * @param mnode, instance of mnode module.
 * @param rpcMsg, message to be processed.
 * @return Error code.
 */
void mnodeProcessMsg(struct Mnode *mnode, SRpcMsg *rpcMsg);

/**
 * Whether the mnode is in service.
 *
 * @param mnode, instance of mnode module.
 * @return Server status.
 */
bool mnodeIsServing(struct Mnode *mnode);

typedef struct {
  int64_t numOfDnode;
  int64_t numOfMnode;
  int64_t numOfVgroup;
  int64_t numOfDatabase;
  int64_t numOfSuperTable;
  int64_t numOfChildTable;
  int64_t numOfColumn;
  int64_t totalPoints;
  int64_t totalStorage;
  int64_t compStorage;
} SMnodeStat;

/**
 * Get the statistical information of Mnode.
 *
 * @param mnode, instance of mnode module.
 * @param stat, statistical information.
 * @return Error Code.
 */
int32_t mnodeGetStatistics(struct Mnode *mnode, SMnodeStat *stat);

/**
 * Get the statistical information of Mnode.
 *
 * @param mnode, instance of mnode module.
 * @param user, username.
 * @param spi,  security parameter index.
 * @param encrypt, encrypt algorithm.
 * @param secret, key for authentication.
 * @param ckey, ciphering key.
 * @return Error Code.
 */
int32_t mnodeRetriveAuth(struct Mnode *mnode, char *user, char *spi, char *encrypt, char *secret, char *ckey);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MNODE_H_*/
