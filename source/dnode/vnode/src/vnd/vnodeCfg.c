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

#include "vnodeInt.h"

const SVnodeCfg vnodeCfgDefault = {.vgId = -1,
                                   .dbname = "",
                                   .dbId = 0,
                                   .wsize = 96 * 1024 * 1024,
                                   .ssize = 1 * 1024 * 1024,
                                   .lsize = 1024,
                                   .isHeapAllocator = false,
                                   .ttl = 0,
                                   .keep = 0,
                                   .streamMode = 0,
                                   .isWeak = 0,
                                   .tsdbCfg = {.precision = TWO_STAGE_COMP,
                                               .update = 0,
                                               .compression = 2,
                                               .days = 10,
                                               .minRows = 100,
                                               .maxRows = 4096,
                                               .keep2 = 3650,
                                               .keep0 = 3650,
                                               .keep1 = 3650},
                                   .walCfg = {.level = TAOS_WAL_WRITE},
                                   .hashBegin = 0,
                                   .hashEnd = 0,
                                   .hashMethod = 0};

int vnodeCheckCfg(const SVnodeCfg *pCfg) {
  // TODO
  return 0;
}

int vnodeEncodeConfig(const void *pObj, SJson *pJson) {
  const SVnodeCfg *pCfg = (SVnodeCfg *)pObj;

  if (tjsonAddIntegerToObject(pJson, "vgId", pCfg->vgId) < 0) return -1;
  if (tjsonAddStringToObject(pJson, "dbname", pCfg->dbname) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "dbId", pCfg->dbId) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "wsize", pCfg->wsize) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "ssize", pCfg->ssize) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "lsize", pCfg->lsize) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "isHeap", pCfg->isHeapAllocator) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "ttl", pCfg->ttl) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "keep", pCfg->keep) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "streamMode", pCfg->streamMode) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "isWeak", pCfg->isWeak) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "precision", pCfg->tsdbCfg.precision) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "update", pCfg->tsdbCfg.update) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "compression", pCfg->tsdbCfg.compression) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "daysPerFile", pCfg->tsdbCfg.days) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "minRows", pCfg->tsdbCfg.minRows) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "maxRows", pCfg->tsdbCfg.maxRows) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "keep0", pCfg->tsdbCfg.keep0) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "keep1", pCfg->tsdbCfg.keep1) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "keep2", pCfg->tsdbCfg.keep2) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "lruCacheSize", pCfg->tsdbCfg.lruCacheSize) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "wal.vgId", pCfg->walCfg.vgId) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "wal.fsyncPeriod", pCfg->walCfg.fsyncPeriod) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "wal.retentionPeriod", pCfg->walCfg.retentionPeriod) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "wal.rollPeriod", pCfg->walCfg.rollPeriod) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "wal.retentionSize", pCfg->walCfg.retentionSize) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "wal.segSize", pCfg->walCfg.segSize) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "wal.level", pCfg->walCfg.level) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "hashBegin", pCfg->hashBegin) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "hashEnd", pCfg->hashEnd) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "hashMethod", pCfg->hashMethod) < 0) return -1;

  return 0;
}

int vnodeDecodeConfig(const SJson *pJson, void *pObj) {
  SVnodeCfg *pCfg = (SVnodeCfg *)pObj;

  if (tjsonGetNumberValue(pJson, "vgId", pCfg->vgId) < 0) return -1;
  if (tjsonGetStringValue(pJson, "dbname", pCfg->dbname) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "dbId", pCfg->dbId) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "wsize", pCfg->wsize) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "ssize", pCfg->ssize) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "lsize", pCfg->lsize) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "isHeap", pCfg->isHeapAllocator) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "ttl", pCfg->ttl) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "keep", pCfg->keep) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "streamMode", pCfg->streamMode) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "isWeak", pCfg->isWeak) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "precision", pCfg->tsdbCfg.precision) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "update", pCfg->tsdbCfg.update) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "compression", pCfg->tsdbCfg.compression) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "daysPerFile", pCfg->tsdbCfg.days) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "minRows", pCfg->tsdbCfg.minRows) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "maxRows", pCfg->tsdbCfg.maxRows) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "keep0", pCfg->tsdbCfg.keep0) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "keep1", pCfg->tsdbCfg.keep1) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "keep2", pCfg->tsdbCfg.keep2) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "lruCacheSize", pCfg->tsdbCfg.lruCacheSize) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "wal.vgId", pCfg->walCfg.vgId) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "wal.fsyncPeriod", pCfg->walCfg.fsyncPeriod) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "wal.retentionPeriod", pCfg->walCfg.retentionPeriod) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "wal.rollPeriod", pCfg->walCfg.rollPeriod) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "wal.retentionSize", pCfg->walCfg.retentionSize) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "wal.segSize", pCfg->walCfg.segSize) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "wal.level", pCfg->walCfg.level) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "hashBegin", pCfg->hashBegin) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "hashEnd", pCfg->hashEnd) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "hashMethod", pCfg->hashMethod) < 0) return -1;

  return 0;
}

int vnodeValidateTableHash(SVnodeCfg *pVnodeOptions, char *tableFName) {
  uint32_t hashValue = 0;

  switch (pVnodeOptions->hashMethod) {
    default:
      hashValue = MurmurHash3_32(tableFName, strlen(tableFName));
      break;
  }

    // TODO OPEN THIS !!!!!!!
#if 0
  if (hashValue < pVnodeOptions->hashBegin || hashValue > pVnodeOptions->hashEnd) {
    terrno = TSDB_CODE_VND_HASH_MISMATCH;
    return TSDB_CODE_VND_HASH_MISMATCH;
  }
#endif

  return TSDB_CODE_SUCCESS;
}
