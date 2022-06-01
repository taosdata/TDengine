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

#include "tmsgcb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------ TYPES EXPOSED ------------------------ */
typedef struct SQnode SQnode;

typedef struct {
  SMsgCb msgCb;
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
 */
int32_t qndProcessQueryMsg(SQnode *pQnode, int64_t ts, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_QNODE_H_*/
