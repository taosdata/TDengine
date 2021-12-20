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

#ifndef _TD_QWORKER_INT_H_
#define _TD_QWORKER_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#define QWORKER_DEFAULT_SCHEDULER_NUMBER 10000
#define QWORKER_DEFAULT_RES_CACHE_NUMBER 10000

typedef struct SQWorkerMgmt {
  SQWorkerCfg cfg;
  SHashObj *scheduleHash;    //key: schedulerId, value: 
  SHashObj *resHash;       //key: queryId+taskId, value:
} SQWorkerMgmt;


#ifdef __cplusplus
}
#endif

#endif /*_TD_QWORKER_INT_H_*/
