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

struct SRpcMsg;

/**
 * Start Initialize Vnode module.
 *
 * @return Error Code.
 */
int32_t vnodeInit();
 
/**
 * Cleanup Vnode module.
 */
void vnodeCleanup();
 
typedef struct {
  int64_t queryMsgCount;
  int64_t writeMsgCount;
} SVnodeStat;
 
/**
 * Get the statistical information of Vnode
 *
 * @param stat Statistical information.
 * @return Error Code.
 */
int32_t vnodeGetStatistics(SVnodeStat *stat);

/** 
 * Interface for processing messages.
 *
 * @param pMsg Message to be processed.
 * @return Error code
 */
int32_t vnodeProcessMsg(SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_H_*/