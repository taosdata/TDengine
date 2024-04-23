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

#ifndef TDENGINE_COMMAND_H
#define TDENGINE_COMMAND_H

#include "cmdnodes.h"
#include "plannodes.h"
#include "tmsg.h"

typedef struct SExplainCtx SExplainCtx;

int32_t qExecCommand(int64_t* pConnId, bool sysInfoUser, SNode *pStmt, SRetrieveTableRsp **pRsp, int8_t biMode);

int32_t qExecStaticExplain(SQueryPlan *pDag, SRetrieveTableRsp **pRsp);
int32_t qExecExplainBegin(SQueryPlan *pDag, SExplainCtx **pCtx, int64_t startTs);
int32_t qExecExplainEnd(SExplainCtx *pCtx, SRetrieveTableRsp **pRsp);
int32_t qExplainUpdateExecInfo(SExplainCtx *pCtx, SExplainRsp *pRspMsg, int32_t groupId, SRetrieveTableRsp **pRsp);
void    qExplainFreeCtx(SExplainCtx *pCtx);

#endif
