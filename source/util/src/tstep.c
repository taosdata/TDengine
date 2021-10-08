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
#include "ulog.h"
#include "tstep.h"

typedef struct SStepObj {
  char *    name;
  void **   self;
  InitFp    initFp;
  CleanupFp cleanupFp;
} SStep;

typedef struct SSteps {
  int32_t cursize;
  int32_t maxsize;
  SStep * steps;
  ReportFp reportFp;
} SSteps;

SSteps *taosStepInit(int32_t maxsize, ReportFp fp) {
  SSteps *steps = calloc(1, sizeof(SSteps));
  if (steps == NULL) return NULL;

  steps->maxsize = maxsize;
  steps->cursize = 0;
  steps->steps = calloc(maxsize, sizeof(SStep));
  steps->reportFp = fp;

  return steps;
}

int32_t taosStepAdd(struct SSteps *steps, char *name, void **obj, InitFp initFp, CleanupFp cleanupFp) {
  if (steps == NULL) return -1;
  if (steps->cursize >= steps->maxsize) {
    uError("failed to add step since up to the maxsize");
    return -1;
  }

  SStep step = {.name = name, .self = obj, .initFp = initFp, .cleanupFp = cleanupFp};
  steps->steps[steps->cursize++] = step;
  return 0;
}

static void taosStepCleanupImp(SSteps *steps, int32_t pos) {
  for (int32_t s = pos; s >= 0; s--) {
    SStep *step = steps->steps + s;
    uDebug("step:%s will cleanup", step->name);
    if (step->cleanupFp != NULL) {
      (*step->cleanupFp)(step->self);
    }
  }
}

int32_t taosStepExec(SSteps *steps) {
  if (steps == NULL) return -1;

  for (int32_t s = 0; s < steps->cursize; s++) {
    SStep *step = steps->steps + s;
    if (step->initFp == NULL) continue;

    if (steps->reportFp != NULL) {
      (*steps->reportFp)(step->name, "start initialize");
    }

    int32_t code = (*step->initFp)(step->self);
    if (code != 0) {
      uDebug("step:%s will cleanup", step->name);
      taosStepCleanupImp(steps, s);
      return code;
    }

    uInfo("step:%s is initialized", step->name);

    if (steps->reportFp != NULL) {
      (*steps->reportFp)(step->name, "initialize completed");
    }
  }

  return 0;
}

void taosStepCleanup(SSteps *steps) {
  if (steps == NULL) return;
  taosStepCleanupImp(steps, steps->cursize - 1);
}