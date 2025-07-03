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

void taosInitRWLatch(SRWLatch *pLatch) { *pLatch = 0; }

void taosWLockLatch(SRWLatch *pLatch) {
  SRWLatch oLatch, nLatch;
  int32_t  nLoops = 0;

  // Set write flag
  while (1) {
    oLatch = atomic_load_32(pLatch);
    if (oLatch & TD_RWLATCH_WRITE_FLAG) {
      nLoops++;
      if (nLoops > 1000) {
        (void)sched_yield();
        nLoops = 0;
      }
      continue;
    }

    nLatch = oLatch | TD_RWLATCH_WRITE_FLAG;
    if (atomic_val_compare_exchange_32(pLatch, oLatch, nLatch) == oLatch) break;
  }

  // wait for all reads end
  nLoops = 0;
  while (1) {
    oLatch = atomic_load_32(pLatch);
    if (oLatch == TD_RWLATCH_WRITE_FLAG) break;
    nLoops++;
    if (nLoops > 1000) {
      (void)sched_yield();
      nLoops = 0;
    }
  }
}

// no reentrant
int32_t taosWTryLockLatch(SRWLatch *pLatch) {
  SRWLatch oLatch, nLatch;
  oLatch = atomic_load_32(pLatch);
  if (oLatch) {
    return -1;
  }

  nLatch = oLatch | TD_RWLATCH_WRITE_FLAG;
  if (atomic_val_compare_exchange_32(pLatch, oLatch, nLatch) == oLatch) {
    return 0;
  }

  return -1;
}

int32_t taosWTryForceLockLatch(SRWLatch *pLatch) {
  SRWLatch oLatch, nLatch;

  while (1) {
    oLatch = atomic_load_32(pLatch);
    if (oLatch) {
      nLatch = oLatch | TD_RWLATCH_WRITE_FLAG;
      if (atomic_val_compare_exchange_32(pLatch, oLatch, nLatch) == oLatch) {
        return -1;
      }
      
      continue;
    } else {
      nLatch = oLatch | TD_RWLATCH_WRITE_FLAG;
      if (atomic_val_compare_exchange_32(pLatch, oLatch, nLatch) == oLatch) {
        return 0;
      }
      
      continue;
    }
  }

  return -1;
}

bool taosIsOnlyWLocked(SRWLatch *pLatch) {
  return TD_RWLATCH_WRITE_FLAG == atomic_load_32(pLatch);
}

bool taosHasRWWFlag(SRWLatch *pLatch) {
  return TD_RWLATCH_WRITE_FLAG & atomic_load_32(pLatch);
}

void taosWUnLockLatch(SRWLatch *pLatch) { atomic_store_32(pLatch, 0); }

void taosRLockLatch(SRWLatch *pLatch) {
  SRWLatch oLatch, nLatch;
  int32_t  nLoops = 0;

  while (1) {
    oLatch = atomic_load_32(pLatch);
    if (oLatch & TD_RWLATCH_WRITE_FLAG) {
      nLoops++;
      if (nLoops > 1000) {
        (void)sched_yield();
        nLoops = 0;
      }
      continue;
    }

    nLatch = oLatch + 1;
    if (atomic_val_compare_exchange_32(pLatch, oLatch, nLatch) == oLatch) break;
  }
}

// no reentrant
int32_t taosRTryLockLatch(SRWLatch *pLatch) {
  SRWLatch oLatch, nLatch;
  int32_t  nLoops = 0;

  while (1) {
    oLatch = atomic_load_32(pLatch);
    if (oLatch & TD_RWLATCH_WRITE_FLAG) {
      return -1;
    }

    nLatch = oLatch + 1;
    if (atomic_val_compare_exchange_32(pLatch, oLatch, nLatch) == oLatch) {
      break;
    }
  }

  return 0;
}

int32_t taosRUnLockLatch(SRWLatch *pLatch) { return atomic_sub_fetch_32(pLatch, 1); }
