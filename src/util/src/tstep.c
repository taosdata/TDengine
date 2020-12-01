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
#include "tstep.h"
#include "tulog.h"
#include "taoserror.h"

void taosStepCleanupImp(SStep *pSteps, int32_t stepId) {
  for (int32_t step = stepId; step >= 0; step--) {
    SStep *pStep = pSteps + step;
    uDebug("step:%s will cleanup", pStep->name);
    if (pStep->cleanupFp != NULL) {
      (*pStep->cleanupFp)();
    }
  }
}

int32_t taosStepInit(SStep *pSteps, int32_t stepSize) {
  for (int32_t step = 0; step < stepSize; step++) {
    SStep *pStep = pSteps + step;
    if (pStep->initFp == NULL) continue;

    int32_t code = (*pStep->initFp)();
    if (code != 0) {
      uDebug("step:%s will init", pStep->name);
      taosStepCleanupImp(pSteps, step);
      return code;
    }
  }

  return 0;
}

void taosStepCleanup(SStep *pSteps, int32_t stepSize) { 
  return taosStepCleanupImp(pSteps, stepSize - 1);
}
