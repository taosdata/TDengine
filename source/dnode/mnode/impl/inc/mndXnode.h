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

#ifndef _TD_MND_XNODE_H_
#define _TD_MND_XNODE_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  XNODE_SHOW_OUTPUT_PLAINTEXT = 0,
  XNODE_SHOW_OUTPUT_MASKED = 1,
  XNODE_SHOW_OUTPUT_ENCRYPTED = 2,
} EXnodeShowOutputMode;

int32_t mndXnodeResolveShowMode(int32_t protection, int8_t requestMode, int8_t *effectiveMode);
bool    mndXnodeShowFilterMatches(int8_t filterType, int64_t filterValue, int32_t taskId, int32_t jobTaskId);
int32_t mndXnodeMaskDsnForShow(const char *dsn, int32_t dsnLen, char **masked, int32_t *maskedLen);
int32_t mndXnodeProjectJobConfigForShow(SShowObj *pShow, const char *config, int32_t configLen, char **projected,
                                        int32_t *projectedLen);
int32_t mndXnodeFinishShowProtection(SShowObj *pShow);
void    mndXnodeCleanupShowProtection(SShowObj *pShow);

#define TSDB_XNODE_VER_NUMBER   1

int32_t mndInitXnode(SMnode* pMnode);
void    mndCleanupXnode(SMnode* pMnode);
void    mndRetrieveTasks(SMnode* pMnode, SArray* pFc, SArray* pAd);
void    mndXnodeHandleBecomeLeader(SMnode* pMnode);
void    mndXnodeHandleBecomeNotLeader();
int32_t mndSetXnodeBlobColumn(SColumnInfoData* pColInfo, int32_t row, const char* data,
                              int32_t storedLen);
int32_t mndGetXnodeShowStatus(SXnodeObj* pObj, char* status, int32_t statusLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_ANODE_H_*/
