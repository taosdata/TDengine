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

#ifndef TDENGINE_DNODE_STEP_H
#define TDENGINE_DNODE_STEP_H

#ifdef __cplusplus
extern "C" {
#endif
#include "dnode.h"

int32_t dnodeStepInit(SStep *pSteps, int32_t stepSize);
void    dnodeStepCleanup(SStep *pSteps, int32_t stepSize);
void    dnodeReportStep(char *name, char *desc, int8_t finished);
void    dnodeSendStartupStep(SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif
