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

typedef enum { MN_STATUS_UNINIT = 0, MN_STATUS_INIT = 1, MN_STATUS_READY = 2, MN_STATUS_CLOSING = 3 } EMnStatus;

typedef struct {
  /**
   * Send messages to other dnodes, such as create vnode message.
   *
   * @param epSet, the endpoint list of the dnodes.
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
   * Send redirect message to dnode or shell.
   *
   * @param rpcMsg, message to be sent.
   * @param forShell, used to identify whether to send to shell or dnode.
   */
  void (*SendRedirectMsg)(struct SRpcMsg *rpcMsg, bool forShell);

} SMnodeFp;

typedef struct {
  SMnodeFp fp;
  char     clusterId[TSDB_CLUSTER_ID_LEN];
  int32_t  dnodeId;
} SMnodePara;

/**
 * Initialize and start mnode module.
 *
 * @param para, initialization parameters.
 * @return Error code.
 */
int32_t mnodeInit(SMnodePara para);

/**
 * Stop and cleanup mnode module.
 */
void mnodeCleanup();

/**
 * Deploy mnode instances in dnode.
 *
 * @return Error Code.
 */
int32_t mnodeDeploy();

/**
 * Delete the mnode instance deployed in dnode.
 */
void mnodeUnDeploy();

/**
 * Whether the mnode is in service.
 *
 * @return Server status.
 */
EMnStatus mnodeIsServing();

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
 * @param stat, statistical information.
 * @return Error Code.
 */
int32_t mnodeGetStatistics(SMnodeStat *stat);

/**
 * Get the auth information of Mnode.
 *
 * @param user, username.
 * @param spi,  security parameter index.
 * @param encrypt, encrypt algorithm.
 * @param secret, key for authentication.
 * @param ckey, ciphering key.
 * @return Error Code.
 */
int32_t mnodeRetriveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey);

/**
 * Interface for processing messages.
 *
 * @param rpcMsg, message to be processed.
 * @return Error code.
 */
void mnodeProcessMsg(SRpcMsg *rpcMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MNODE_H_*/
