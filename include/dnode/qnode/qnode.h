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

#ifndef _TD_QNODE_H_
#define _TD_QNODE_H_

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------ TYPES EXPOSED ------------------------ */
typedef struct SDnode SDnode;
typedef struct SQnode SQnode;
typedef int32_t (*SendReqToDnodeFp)(SDnode *pDnode, struct SEpSet *epSet, struct SRpcMsg *pMsg);
typedef int32_t (*SendReqToMnodeFp)(SDnode *pDnode, struct SRpcMsg *pMsg);
typedef void (*SendRedirectRspFp)(SDnode *pDnode, struct SRpcMsg *pMsg);

typedef struct {
  int64_t numOfStartTask;
  int64_t numOfStopTask;
  int64_t numOfRecvedFetch;
  int64_t numOfSentHb;
  int64_t numOfSentFetch;
  int64_t numOfTaskInQueue;
  int64_t numOfFetchInQueue;
  int64_t numOfErrors;
} SQnodeLoad;

typedef struct {
  int32_t           sver;
  int32_t           dnodeId;
  int64_t           clusterId;
  SDnode           *pDnode;
  SendReqToDnodeFp  sendReqFp;
  SendReqToMnodeFp  sendReqToMnodeFp;
  SendRedirectRspFp sendRedirectRspFp;
} SQnodeOpt;

/* ------------------------ SQnode ------------------------ */
/**
 * @brief Start one Qnode in Dnode.
 *
 * @param pOption Option of the qnode.
 * @return SQnode* The qnode object.
 */
SQnode *qndOpen(const SQnodeOpt *pOption);

/**
 * @brief Stop Qnode in Dnode.
 *
 * @param pQnode The qnode object to close.
 */
void qndClose(SQnode *pQnode);

/**
 * @brief Get the statistical information of Qnode
 *
 * @param pQnode The qnode object.
 * @param pLoad Statistics of the qnode.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t qndGetLoad(SQnode *pQnode, SQnodeLoad *pLoad);

/**
 * @brief Process a query or fetch message.
 *
 * @param pQnode The qnode object.
 * @param pMsg The request message
 * @param pRsp The response message
 * @return int32_t 0 for success, -1 for failure
 */
int32_t qndProcessMsg(SQnode *pQnode, SRpcMsg *pMsg, SRpcMsg **pRsp);

#ifdef __cplusplus
}
#endif

#endif /*_TD_QNODE_H_*/