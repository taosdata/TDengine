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

#include "cos.h"
#include "vnd.h"

static volatile int32_t VINIT = 0;
static TdThreadOnce   jsonTemplateInit = PTHREAD_ONCE_INIT;  // initialize only once
SVAsync* vnodeAsyncHandle[2];
int32_t jsonTemplateRef = 0;

static void jsonTemplateFree(void* handle) {
  SArray *tmp = (SArray *)handle;
  for(int i = 0; i < taosArrayGetSize(tmp); i++){
    avro_schema_decref(taosArrayGet(tmp, i));
  }
  taosArrayDestroy(tmp);
}

static void jsonTemplateMgmtInit(void) {
  jsonTemplateRef = taosOpenRef(10000, jsonTemplateFree);
}

int vnodeInit(int nthreads) {
  int32_t init;

  init = atomic_val_compare_exchange_32(&VINIT, 0, 1);
  if (init) {
    return 0;
  }

  // vnode-commit
  vnodeAsyncInit(&vnodeAsyncHandle[0], "vnode-commit");
  vnodeAsyncSetWorkers(vnodeAsyncHandle[0], nthreads);

  // vnode-merge
  vnodeAsyncInit(&vnodeAsyncHandle[1], "vnode-merge");
  vnodeAsyncSetWorkers(vnodeAsyncHandle[1], nthreads);

  if (walInit() < 0) {
    return -1;
  }

  taosThreadOnce(&jsonTemplateInit, jsonTemplateMgmtInit);

  return 0;
}

void vnodeCleanup() {
  int32_t init = atomic_val_compare_exchange_32(&VINIT, 1, 0);
  if (init == 0) return;

  // set stop
  vnodeAsyncDestroy(&vnodeAsyncHandle[0]);
  vnodeAsyncDestroy(&vnodeAsyncHandle[1]);

  walCleanUp();
  smaCleanUp();
}
