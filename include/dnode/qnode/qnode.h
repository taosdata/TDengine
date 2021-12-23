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
#include "trpc.h"

typedef struct {
  uint64_t numOfStartTask;
  uint64_t numOfStopTask;
  uint64_t numOfRecvedFetch;
  uint64_t numOfSentHb;
  uint64_t numOfSentFetch;
  uint64_t numOfTaskInQueue;
  uint64_t numOfFetchInQueue;
  uint64_t numOfErrors;
} SQnodeStat;


/**
 * Start one Qnode in Dnode.
 * @return Error Code.
 */
int32_t qnodeStart();

/**
 * Stop Qnode in Dnode.
 *
 * @param qnodeId Qnode ID to stop, -1 for all Qnodes.
 */
void qnodeStop(int64_t qnodeId);

 
/**
 * Get the statistical information of Qnode
 *
 * @param qnodeId Qnode ID to get statistics, -1 for all 
 * @param stat Statistical information.
 * @return Error Code.
 */
int32_t qnodeGetStatistics(int64_t qnodeId, SQnodeStat *stat);

/**
 * Interface for processing Qnode messages.
 * 
 * @param pMsg Message to be processed.
 * @return Error code
 */
void qnodeProcessReq(SRpcMsg *pMsg);



#ifdef __cplusplus
}
#endif

#endif /*_TD_QNODE_H_*/