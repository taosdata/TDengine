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

#ifndef TDENGINE_MODULE_MPEER_SCORE_H
#define TDENGINE_MODULE_MPEER_SCORE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include "mnode.h"

/*
* Interface functions
*/

int32_t balanceGetScoresMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn);
int32_t balanceRetrieveScores(SShowObj *pShow, char *data, int32_t rows, void *pConn);

/*
 * Internal definitions
 */

void balanceInitDnodeList();
void balanceMakeDnodeList();
void balanceReleaseDnodeList();

void  balanceCalcSystemScore();
float balanceTryCalcDnodeScore(SDnodeObj *pDnode, int32_t extraVnode);

extern int32_t     tsBalanceDnodeListSize;
extern SDnodeObj **tsBalanceDnodeList;

#ifdef __cplusplus
}
#endif

#endif
