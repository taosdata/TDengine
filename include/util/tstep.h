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

#ifndef _TD_UTIL_STEP_H_
#define _TD_UTIL_STEP_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SSteps SSteps;
typedef int32_t (*InitFp)();
typedef void (*CleanupFp)();
typedef void (*ReportFp)(char *name, char *desc);

SSteps *taosStepInit(int32_t maxsize, ReportFp fp);
int32_t taosStepExec(SSteps *steps);
void    taosStepCleanup(SSteps *steps);
int32_t taosStepAdd(SSteps *steps, char *name, InitFp initFp, CleanupFp cleanupFp);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_STEP_H_*/
