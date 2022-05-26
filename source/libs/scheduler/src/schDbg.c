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

#include "query.h"
#include "schedulerInt.h"

tsem_t schdRspSem;

void schdExecCallback(SQueryResult* pResult, void* param, int32_t code) {
  if (code) {
    pResult->code = code;
  }
  
  *(SQueryResult*)param = *pResult;

  taosMemoryFree(pResult);

  tsem_post(&schdRspSem);
}

void schdFetchCallback(void* pResult, void* param, int32_t code) {
  SSchdFetchParam* fParam = (SSchdFetchParam*)param;

  *fParam->pData = pResult;
  *fParam->code = code;

  tsem_post(&schdRspSem);
}


