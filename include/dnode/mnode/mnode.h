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

#ifndef _TD_MND_H_
#define _TD_MND_H_

#include "monitor.h"
#include "tmsg.h"
#include "tmsgcb.h"
#include "trpc.h"
#include "sync.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------ TYPES EXPOSED ------------------------ */
typedef struct SMnode SMnode;

typedef struct {
  int32_t  dnodeId;
  bool     deploy;
  int8_t   selfIndex;
  int8_t   numOfReplicas;
  int8_t   numOfTotalReplicas;
  SReplica replicas[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];
  int32_t  nodeRoles[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];
  SMsgCb   msgCb;
  int64_t  lastIndex;
} SMnodeOpt;

/* ------------------------ SMnode ------------------------ */
/**
 * @brief Open a mnode.
 *
 * @param path Path of the mnode.
 * @param pOption Option of the mnode.
 * @return SMnode* The mnode object.
 */
SMnode *mndOpen(const char *path, const SMnodeOpt *pOption);

/**
 * @brief Close a mnode.
 *
 * @param pMnode The mnode object to close.
 */
void mndClose(SMnode *pMnode);
void mndPreClose(SMnode *pMnode);

/**
 * @brief Start mnode
 *
 * @param pMnode The mnode object.
 */
int32_t mndStart(SMnode *pMnode);

/**
 * @brief Stop mnode
 *
 * @param pMnode The mnode object.
 */
void mndStop(SMnode *pMnode);

int32_t mndIsCatchUp(SMnode *pMnode);
ESyncRole mndGetRole(SMnode *pMnode);

/**
 * @brief Get mnode monitor info.
 *
 * @param pMnode The mnode object.
 * @param pCluster
 * @param pVgroup
 * @param pStbInfo
 * @param pGrant
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t mndGetMonitorInfo(SMnode *pMnode, SMonClusterInfo *pClusterInfo, SMonVgroupInfo *pVgroupInfo,
                          SMonStbInfo *pStbInfo, SMonGrantInfo *pGrantInfo);

/**
 * @brief Get mnode loads for status msg.
 *
 * @param pMnode The mnode object.
 * @param pLoad
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t mndGetLoad(SMnode *pMnode, SMnodeLoad *pLoad);

/**
 * @brief Process the rpc, sync request.
 *
 * @param pMsg The request msg.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t mndProcessRpcMsg(SRpcMsg *pMsg);
int32_t mndProcessSyncMsg(SRpcMsg *pMsg);
int32_t mndPreProcessQueryMsg(SRpcMsg *pMsg);
void    mndPostProcessQueryMsg(SRpcMsg *pMsg);

/**
 * @brief Generate machine code
 */
void mndGenerateMachineCode();

void mndDumpSdb();

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_H_*/
