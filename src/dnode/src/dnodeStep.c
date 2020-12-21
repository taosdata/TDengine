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

#define _DEFAULT_SOURCE
#include "os.h"
#include "dnodeStep.h"

static SStartupStep tsStartupStep;

void dnodeReportStep(char *name, char *desc, int8_t finished) {
  tstrncpy(tsStartupStep.name, name, sizeof(tsStartupStep.name));
  tstrncpy(tsStartupStep.desc, desc, sizeof(tsStartupStep.desc));
  tsStartupStep.finished = finished;
}

void dnodeSendStartupStep(SRpcMsg *pMsg) {
  dInfo("nettest msg is received, cont:%s", (char *)pMsg->pCont);

  SStartupStep *pStep = rpcMallocCont(sizeof(SStartupStep));
  memcpy(pStep, &tsStartupStep, sizeof(SStartupStep));

  dDebug("startup msg is sent, step:%s desc:%s finished:%d", pStep->name, pStep->desc, pStep->finished);

  SRpcMsg rpcRsp = {.handle = pMsg->handle, .pCont = pStep, .contLen = sizeof(SStartupStep)};
  rpcSendResponse(&rpcRsp);
  rpcFreeCont(pMsg->pCont);
}

void taosStepCleanupImp(SStep *pSteps, int32_t stepId) {
  for (int32_t step = stepId; step >= 0; step--) {
    SStep *pStep = pSteps + step;
    dDebug("step:%s will cleanup", pStep->name);
    if (pStep->cleanupFp != NULL) {
      (*pStep->cleanupFp)();
    }
  }
}

int32_t dnodeStepInit(SStep *pSteps, int32_t stepSize) {
  for (int32_t step = 0; step < stepSize; step++) {
    SStep *pStep = pSteps + step;
    if (pStep->initFp == NULL) continue;

    dnodeReportStep(pStep->name, "Start initialization", 0);

    int32_t code = (*pStep->initFp)();
    if (code != 0) {
      dDebug("step:%s will cleanup", pStep->name);
      taosStepCleanupImp(pSteps, step);
      return code;
    }
    dInfo("step:%s is initialized", pStep->name);

    dnodeReportStep(pStep->name, "Initialization complete", 0);
  }

  return 0;
}

void dnodeStepCleanup(SStep *pSteps, int32_t stepSize) { 
  return taosStepCleanupImp(pSteps, stepSize - 1);
}
