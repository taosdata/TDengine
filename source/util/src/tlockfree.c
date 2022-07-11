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
#include "tlockfree.h"

#define TD_RWLATCH_WRITE_FLAG 0x40000000
#define TD_RWLATCH_REENTRANT_FLAG 0x4000000000000000

void taosInitRWLatch(SRWLatch *pLatch) { *pLatch = 0; }
void taosInitReentrantRWLatch(SRWLatch *pLatch) { *pLatch = TD_RWLATCH_REENTRANT_FLAG; }

void taosWLockLatch(SRWLatch *pLatch) {
  SRWLatch oLatch, nLatch;
  int32_t  nLoops = 0;

  // Set write flag
  while (1) {
    oLatch = atomic_load_64(pLatch);
    if (oLatch & TD_RWLATCH_WRITE_FLAG) {
      if (oLatch & TD_RWLATCH_REENTRANT_FLAG) {
        nLatch = (((oLatch >> 32) + 1) << 32) | (oLatch & 0xFFFFFFFF);
        if (atomic_val_compare_exchange_64(pLatch, oLatch, nLatch) == oLatch) break;

        continue;
      }
      nLoops++;
      if (nLoops > 1000) {
        sched_yield();
        nLoops = 0;
      }
      continue;
    }

    nLatch = oLatch | TD_RWLATCH_WRITE_FLAG;
    if (atomic_val_compare_exchange_64(pLatch, oLatch, nLatch) == oLatch) break;
  }

  // wait for all reads end
  nLoops = 0;
  while (1) {
    oLatch = atomic_load_64(pLatch);
    if (0 == (oLatch & 0xFFFFFFF)) break;
    nLoops++;
    if (nLoops > 1000) {
      sched_yield();
      nLoops = 0;
    }
  }
}

// no reentrant
int32_t taosWTryLockLatch(SRWLatch *pLatch) {
  SRWLatch oLatch, nLatch;
  oLatch = atomic_load_64(pLatch);
  if (oLatch << 2) {
    return -1;
  }

  nLatch = oLatch | TD_RWLATCH_WRITE_FLAG;
  if (atomic_val_compare_exchange_64(pLatch, oLatch, nLatch) == oLatch) {
    return 0;
  }

  return -1;
}

void taosWUnLockLatch(SRWLatch *pLatch) { 
  SRWLatch oLatch, nLatch, wLatch;

  while (1) {  
    oLatch = atomic_load_64(pLatch);
    
    if (0 == (oLatch & TD_RWLATCH_REENTRANT_FLAG)) {
      atomic_store_64(pLatch, 0); 
      break;
    }

    wLatch = ((oLatch << 2) >> 34);
    if (wLatch) {
      nLatch = ((--wLatch) << 32) | TD_RWLATCH_REENTRANT_FLAG | TD_RWLATCH_WRITE_FLAG;
    } else {
      nLatch = TD_RWLATCH_REENTRANT_FLAG;
    }

    if (atomic_val_compare_exchange_64(pLatch, oLatch, nLatch) == oLatch) break;
  }
}

void taosRLockLatch(SRWLatch *pLatch) {
  SRWLatch oLatch, nLatch;
  int32_t  nLoops = 0;

  while (1) {
    oLatch = atomic_load_64(pLatch);
    if (oLatch & TD_RWLATCH_WRITE_FLAG) {
      nLoops++;
      if (nLoops > 1000) {
        sched_yield();
        nLoops = 0;
      }
      continue;
    }

    nLatch = oLatch + 1;
    if (atomic_val_compare_exchange_64(pLatch, oLatch, nLatch) == oLatch) break;
  }
}

void taosRUnLockLatch(SRWLatch *pLatch) { atomic_fetch_sub_64(pLatch, 1); }
