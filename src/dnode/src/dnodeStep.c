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
#include "taosmsg.h"
#include "dnodeInt.h"

static SStartupStep tsStartupStep;

void dnodeReportStep(char *name, char *desc, int8_t finished) {
  tstrncpy(tsStartupStep.name, name, sizeof(tsStartupStep.name));
  tstrncpy(tsStartupStep.desc, desc, sizeof(tsStartupStep.desc));
  tsStartupStep.finished = finished;
}

void dnodeSendStartupStep(SRpcMsg *pMsg) {
  dInfo("nettest msg is received, cont:%s", (char *)pMsg->pCont);

  SStartupStep *pStep = rpcMallocCont(sizeof(SStartupStep));
#if 1
  memcpy(pStep, &tsStartupStep, sizeof(SStartupStep));
#else
  static int32_t step = 0;
  sprintf(pStep->name, "module:%d", step++);
  sprintf(pStep->desc, "step:%d", step++);
  if (step > 10) pStep->finished = 1;
#endif

  SRpcMsg rpcRsp = {.handle = pMsg->handle, .pCont = pStep, .contLen = sizeof(SStartupStep)};
  rpcSendResponse(&rpcRsp);
  rpcFreeCont(pMsg->pCont);
}
