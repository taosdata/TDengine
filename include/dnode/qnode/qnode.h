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

/* start Task msg */
typedef struct {
  uint32_t  schedulerIp;
  uint16_t  schedulerPort;
  int64_t   taskId;
  int64_t   queryId;
  uint32_t  srcIp;
  uint16_t  srcPort;
} SQnodeStartTaskMsg;

/* stop Task msg */
typedef struct {
  int64_t   taskId;
} SQnodeStopTaskMsg;

/* start/stop Task msg response */
typedef struct {
  int64_t   taskId;
  int32_t code;
} SQnodeTaskRespMsg;

/* Task status msg */
typedef struct {
  int64_t   taskId;
  int32_t   status;
  int64_t   queryId;
} SQnodeTaskStatusMsg;

/* Qnode/Scheduler heartbeat msg */
typedef struct {
  int32_t status;
  int32_t load;
  
} SQnodeHeartbeatMsg;

/* Qnode sent/received msg */
typedef struct {
  int8_t   msgType;
  int32_t  msgLen;
  char     msg[];
} SQnodeMsg;


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