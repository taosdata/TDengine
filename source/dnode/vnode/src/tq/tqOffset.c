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

#include "tqOffset.h"

enum ETqOffsetPersist {
  TQ_OFFSET_PERSIST__LAZY = 1,
  TQ_OFFSET_PERSIST__EAGER,
};

struct STqOffsetCfg {
  int8_t persistPolicy;
};

struct STqOffsetStore {
  STqOffsetCfg cfg;
  SHashObj*    pHash;  // SHashObj<subscribeKey, offset>
};

STqOffsetStore* STqOffsetOpen(STqOffsetCfg* pCfg) {
  STqOffsetStore* pStore = taosMemoryMalloc(sizeof(STqOffsetStore));
  if (pStore == NULL) {
    return NULL;
  }
  memcpy(&pStore->cfg, pCfg, sizeof(STqOffsetCfg));
  pStore->pHash = taosHashInit(64, MurmurHash3_32, true, HASH_NO_LOCK);
  return pStore;
}

