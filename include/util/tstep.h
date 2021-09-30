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

typedef int32_t (*FnInitObj)(void *parent, void **self);
typedef void (*FnCleanupObj)(void **self);
typedef void (*FnReportProgress)(void *parent, const char *name, const char *desc);

typedef struct SStepObj {
  const char *     name;
  void *           parent;
  void **          self;
  FnInitObj        initFp;
  FnCleanupObj     cleanupFp;
  FnReportProgress reportFp;
} SStepObj;

typedef struct SSteps {
  int32_t   cursize;
  int32_t   maxsize;
  SStepObj *steps;
} SSteps;

SSteps *taosStepInit(int32_t stepsize);
int32_t taosStepAdd(SSteps *steps, SStepObj *step);
int32_t taosStepExec(SSteps *steps);
void    taosStepCleanup(SSteps *steps);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_STEP_H_*/
