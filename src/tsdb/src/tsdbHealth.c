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
#include "tsdbint.h"
#include "tsdbBuffer.h"
#include "tsdbLog.h"
#include "tsdbHealth.h"
#include "ttimer.h"

#include "../../vnode/inc/vnodeInt.h"
// get qmgmt
void* vnodeGetqMgmt(void* pVnode){
  if(pVnode == NULL) return NULL;
  return ((SVnodeObj*)pVnode)->qMgmt;
}

// return malloc new block count 
int32_t tsdbInsertNewBlock(STsdbRepo * pRepo) {
  STsdbBufPool *pPool = pRepo->pPool;
  int32_t cnt = 0;

  if(enoughIdleMemory() && allowNewBlock(pRepo)) {
    STsdbBufBlock *pBufBlock = tsdbNewBufBlock(pPool->bufBlockSize);
    if (pBufBlock) {
      if (tsdbLockRepo(pRepo) >= 0) {
        if (tdListAppend(pPool->bufBlockList, (void *)(&pBufBlock)) < 0) {
          // append error
          tsdbFreeBufBlock(pBufBlock);
        } else {
          pPool->nRecycleBlocks ++;
          cnt ++ ; 
        }
        tsdbUnlockRepo(pRepo);  
      }
    }
 } 
 return cnt;
}

// switch anther thread to run
void cbKillQueryFree(void* param1, void* param2) {
  STsdbRepo* pRepo =  (STsdbRepo*)param1;
  int32_t longMs = 2000; // TODO config to taos.cfg
  
  // vnode
  void* vnodeObj = pRepo->appH.appH;
  if(vnodeObj == NULL) return ;

  // qMgmt
  void* qMgmt = vnodeGetqMgmt(vnodeObj);
  if(qMgmt == NULL) return ;

  // qid top list
  SArray *qids = (SArray*)qObtainLongQuery(qMgmt, longMs);
  if(qids == NULL) return ;

  // kill Query
  size_t cnt = taosArrayGetSize(qids);
  int64_t qId = 0;
  for(size_t i=0; i < cnt; i++) {
    qId = *(int64_t*)taosArrayGetP(qids, i);
    qKillQueryByQId(qMgmt, qId, 100, 50); // wait 50*100 ms 
    // notify wait
    pthread_cond_signal(&pRepo->pPool->poolNotEmpty);
    // check break condition 
    if(enoughIdleMemory() && allowNewBlock(pRepo)) {
      break;
    }
  }

  // free qids
  taosArrayDestroyEx(qids, free);
}

// return true do free , false do nothing
bool tsdbUrgeQueryFree(STsdbRepo * pRepo) {
  // 1 start timer
  tmr_h hTimer = taosTmrStart(cbKillQueryFree, 1, pRepo, NULL);
  return hTimer != NULL;
}

bool enoughIdleMemory(){
  // TODO config to taos.cfg
  int32_t lowestRate = 20;  // below 20% idle memory, return not enough memory
  float  memoryUsedMB = 0;
  float  memoryAvailMB;

  if (true != taosGetSysMemory(&memoryUsedMB)) {
    tsdbWarn("tsdbHealth get memory error, return false.");
    return false;
  }

  if(memoryUsedMB > tsTotalMemoryMB || tsTotalMemoryMB == 0) {
    tsdbWarn("tsdbHealth used memory(%d MB) large total memory(%d MB), return false.", (int)memoryUsedMB, (int)tsTotalMemoryMB);
    return false;
  }

  memoryAvailMB = (float)tsTotalMemoryMB - memoryUsedMB;
  int32_t rate = (int32_t)(memoryAvailMB/tsTotalMemoryMB * 100);
  if(rate < lowestRate){
    tsdbWarn("tsdbHealth real rate :%d less than lowest rate:%d, so return false.", rate, lowestRate);
    return false;
  }

  return true;
}

bool allowNewBlock(STsdbRepo* pRepo){
  //TODO config to taos.cfg
  int32_t nElasticBlocks = 10;
  STsdbBufPool* pPool = pRepo->pPool;
  int32_t nOverBlocks = pPool->nBufBlocks - pRepo->config.totalBlocks;
  if(nOverBlocks > nElasticBlocks) {
    tsdbWarn("tsdbHealth allowNewBlock forbid. nOverBlocks(%d) > nElasticBlocks(%d)", nOverBlocks, nElasticBlocks);
    return false;
  }

  return true;
}
