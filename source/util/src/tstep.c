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
#include "tulog.h"
#include "tstep.h"

SSteps *taosStepInit(int32_t maxsize) {
  SSteps *steps = calloc(1, sizeof(SSteps));
  if (steps == NULL) return NULL;

  steps->maxsize = maxsize;
  steps->cursize = 0;
  steps->steps = calloc(maxsize, sizeof(SStepObj));

  return steps;
}

int32_t taosStepAdd(SSteps *steps, SStepObj *step) {
  if (steps == NULL) return - 1;

  if (steps->cursize >= steps->maxsize) {
    uError("failed to add step since up to the maxsize");
    return -1;
  }

  steps->steps[steps->cursize++] = *step;
  return 0;
}

static void taosStepCleanupImp(SSteps *steps, int32_t pos) {
  for (int32_t s = pos; s >= 0; s--) {
    SStepObj *step = steps->steps + s;
    uDebug("step:%s will cleanup", step->name);
    if (step->cleanupFp != NULL) {
      (*step->cleanupFp)(step->self);
    }
  }
}

int32_t taosStepExec(SSteps *steps) {
  if (steps == NULL) return -1;

  for (int32_t s = 0; s < steps->cursize; s++) {
    SStepObj *step = steps->steps + s;
    if (step->initFp == NULL) continue;

    if (step->reportFp != NULL) {
      (*step->reportFp)(step->parent, step->name, "start initialize");
    }

    int32_t code = (*step->initFp)(step->parent, step->self);
    if (code != 0) {
      uDebug("step:%s will cleanup", step->name);
      taosStepCleanupImp(steps, s);
      return code;
    }

    uInfo("step:%s is initialized", step->name);

    if (step->reportFp != NULL) {
      (*step->reportFp)(step->parent, step->name, "initialize completed");
    }
  }

  return 0;
}

void taosStepCleanup(SSteps *steps) {
  if (steps == NULL) return;
  taosStepCleanupImp(steps, steps->cursize - 1);
}