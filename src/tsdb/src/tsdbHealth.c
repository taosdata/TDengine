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

#include "os.h"
#include "taosmsg.h"
#include "tarray.h"
#include "query.h"
#include "tglobal.h"
#include "tlist.h"
#include "tsdbint.h"
#include "tsdbBuffer.h"
#include "tsdbLog.h"
#include "tsdbHealth.h"
#include "ttimer.h"


// return malloc new block count 
int32_t tsdbInsertNewBlock(STsdbRepo * pRepo) {
  STsdbBufPool *pPool = pRepo->pPool;
  int32_t cnt = 0;

  if(tsdbIdleMemEnough() && tsdbAllowNewBlock(pRepo)) {
    STsdbBufBlock *pBufBlock = tsdbNewBufBlock(pPool->bufBlockSize);
    if (pBufBlock) {
        if (tdListAppend(pPool->bufBlockList, (void *)(&pBufBlock)) < 0) {
          // append error
          tsdbFreeBufBlock(pBufBlock);
        } else {
          pPool->nElasticBlocks ++;
          cnt ++ ; 
          printf(" elastic block add one ok. current blocks=%d \n", pPool->nElasticBlocks);
        }
    }
 } 
 return cnt;
}

// switch anther thread to run
void cbKillQueryFree(void* param1, void* param2) {
  STsdbRepo* pRepo =  (STsdbRepo*)param1;
  // vnode
  if(pRepo->appH.notifyStatus) {
    pRepo->appH.notifyStatus(pRepo->appH.appH, TSDB_STATUS_COMMIT_NOBLOCK, TSDB_CODE_SUCCESS);
  }
}

// return true do free , false do nothing
bool tsdbUrgeQueryFree(STsdbRepo * pRepo) {
  // 1 start timer
  if(pRepo->tmrCtrl == NULL){
    pRepo->tmrCtrl = taosTmrInit(0, 0, 0, "REPO");
  }

  tmr_h hTimer = taosTmrStart(cbKillQueryFree, 1, pRepo, pRepo->tmrCtrl);
  return hTimer != NULL;
}

bool tsdbIdleMemEnough() {
  // TODO config to taos.cfg
  int32_t lowestRate = 5;  // below 10% idle memory, return not enough memory
  float  memoryUsedMB = 0;
  float  memoryAvailMB;

  if (!taosGetSysMemory(&memoryUsedMB)) {
    tsdbWarn("tsdbHealth get memory error, return false.");
    return true;
  }

  if(memoryUsedMB > tsTotalMemoryMB || tsTotalMemoryMB == 0) {
    tsdbWarn("tsdbHealth used memory(%d MB) large total memory(%d MB), return false.", (int)memoryUsedMB, (int)tsTotalMemoryMB);
    return true;
  }

  memoryAvailMB = (float)tsTotalMemoryMB - memoryUsedMB;
  int32_t rate = (int32_t)(memoryAvailMB/tsTotalMemoryMB * 100);
  if(rate < lowestRate){
    tsdbWarn("tsdbHealth real rate :%d less than lowest rate:%d, so return false.", rate, lowestRate);
    return false;
  }

  return true;
}

bool tsdbAllowNewBlock(STsdbRepo* pRepo) {
  //TODO config to taos.cfg
  int32_t nMaxElastic = 1;
  STsdbBufPool* pPool = pRepo->pPool;
  printf("tsdbAllowNewBlock nElasticBlock(%d) MaxElasticBlocks(%d)\n", pPool->nElasticBlocks, nMaxElastic);
  if(pPool->nElasticBlocks >= nMaxElastic) {
    tsdbWarn("tsdbAllowNewBlock return fasle. nElasticBlock(%d) >= MaxElasticBlocks(%d)", pPool->nElasticBlocks, nMaxElastic);
    return false;
  }
  return true;
}

bool tsdbNoProblem(STsdbRepo* pRepo) {
  if(!tsdbIdleMemEnough()) 
     return false;
  if(listNEles(pRepo->pPool->bufBlockList) == 0) 
     return false;
  return true;
}