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

#include "tutil.h"
#include "vnd.h"

const SVnodeCfg vnodeCfgDefault = {.vgId = -1,
                                   .dbname = "",
                                   .dbId = 0,
                                   .szPage = 4096,
                                   .szCache = 256,
                                   .cacheLast = 3,
                                   .cacheLastSize = 8,
                                   .szBuf = 96 * 1024 * 1024,
                                   .isHeap = false,
                                   .isWeak = 0,
                                   .tsdbCfg = {.precision = TSDB_TIME_PRECISION_MILLI,
                                               .update = 1,
                                               .compression = 2,
                                               .slLevel = 5,
                                               .days = 14400,
                                               .minRows = 100,
                                               .maxRows = 4096,
                                               .keep2 = 5256000,
                                               .keep0 = 5256000,
                                               .keep1 = 5256000,
                                               .keepTimeOffset = TSDB_DEFAULT_KEEP_TIME_OFFSET},
                                   .walCfg =
                                       {
                                           .vgId = -1,
                                           .fsyncPeriod = 0,
                                           .retentionPeriod = -1,
                                           .rollPeriod = 0,
                                           .segSize = 0,
                                           .retentionSize = -1,
                                           .level = TAOS_WAL_WRITE,
                                       },
                                   .hashBegin = 0,
                                   .hashEnd = 0,
                                   .hashMethod = 0,
                                   .sttTrigger = TSDB_DEFAULT_SST_TRIGGER,
                                   .tsdbPageSize = TSDB_DEFAULT_PAGE_SIZE};

int vnodeCheckCfg(const SVnodeCfg *pCfg) {
  // TODO
  return 0;
}

const char *vnodeRoleToStr(ESyncRole role) {
  switch (role) {
    case TAOS_SYNC_ROLE_VOTER:
      return "true";
    case TAOS_SYNC_ROLE_LEARNER:
      return "false";
    default:
      return "unknown";
  }
}

const ESyncRole vnodeStrToRole(char *str) {
  if (strcmp(str, "true") == 0) {
    return TAOS_SYNC_ROLE_VOTER;
  }
  if (strcmp(str, "false") == 0) {
    return TAOS_SYNC_ROLE_LEARNER;
  }

  return TAOS_SYNC_ROLE_ERROR;
}

int vnodeEncodeConfig(const void *pObj, SJson *pJson) {
  const SVnodeCfg *pCfg = (SVnodeCfg *)pObj;

  if (tjsonAddIntegerToObject(pJson, "vgId", pCfg->vgId) < 0) return -1;
  if (tjsonAddStringToObject(pJson, "dbname", pCfg->dbname) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "dbId", pCfg->dbId) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "szPage", pCfg->szPage) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "szCache", pCfg->szCache) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "cacheLast", pCfg->cacheLast) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "cacheLastSize", pCfg->cacheLastSize) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "szBuf", pCfg->szBuf) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "isHeap", pCfg->isHeap) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "isWeak", pCfg->isWeak) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "isTsma", pCfg->isTsma) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "isRsma", pCfg->isRsma) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "precision", pCfg->tsdbCfg.precision) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "update", pCfg->tsdbCfg.update) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "compression", pCfg->tsdbCfg.compression) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "slLevel", pCfg->tsdbCfg.slLevel) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "daysPerFile", pCfg->tsdbCfg.days) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "minRows", pCfg->tsdbCfg.minRows) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "maxRows", pCfg->tsdbCfg.maxRows) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "keep0", pCfg->tsdbCfg.keep0) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "keep1", pCfg->tsdbCfg.keep1) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "keep2", pCfg->tsdbCfg.keep2) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "keepTimeOffset", pCfg->tsdbCfg.keepTimeOffset) < 0) return -1;
  if (pCfg->tsdbCfg.retentions[0].keep > 0) {
    int32_t nRetention = 1;
    if (pCfg->tsdbCfg.retentions[1].freq > 0) {
      ++nRetention;
      if (pCfg->tsdbCfg.retentions[2].freq > 0) {
        ++nRetention;
      }
    }
    SJson *pNodeRetentions = tjsonCreateArray();
    tjsonAddItemToObject(pJson, "retentions", pNodeRetentions);
    for (int32_t i = 0; i < nRetention; ++i) {
      SJson            *pNodeRetention = tjsonCreateObject();
      const SRetention *pRetention = pCfg->tsdbCfg.retentions + i;
      tjsonAddIntegerToObject(pNodeRetention, "freq", pRetention->freq);
      tjsonAddIntegerToObject(pNodeRetention, "freqUnit", pRetention->freqUnit);
      tjsonAddIntegerToObject(pNodeRetention, "keep", pRetention->keep);
      tjsonAddIntegerToObject(pNodeRetention, "keepUnit", pRetention->keepUnit);
      tjsonAddItemToArray(pNodeRetentions, pNodeRetention);
    }
  }
  if (tjsonAddIntegerToObject(pJson, "wal.vgId", pCfg->walCfg.vgId) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "wal.fsyncPeriod", pCfg->walCfg.fsyncPeriod) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "wal.retentionPeriod", pCfg->walCfg.retentionPeriod) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "wal.rollPeriod", pCfg->walCfg.rollPeriod) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "wal.retentionSize", pCfg->walCfg.retentionSize) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "wal.segSize", pCfg->walCfg.segSize) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "wal.level", pCfg->walCfg.level) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "sstTrigger", pCfg->sttTrigger) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "hashBegin", pCfg->hashBegin) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "hashEnd", pCfg->hashEnd) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "hashChange", pCfg->hashChange) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "hashMethod", pCfg->hashMethod) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "hashPrefix", pCfg->hashPrefix) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "hashSuffix", pCfg->hashSuffix) < 0) return -1;

  if (tjsonAddIntegerToObject(pJson, "syncCfg.replicaNum", pCfg->syncCfg.replicaNum) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "syncCfg.myIndex", pCfg->syncCfg.myIndex) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "syncCfg.changeVersion", pCfg->syncCfg.changeVersion) < 0) return -1;

  if (tjsonAddIntegerToObject(pJson, "vndStats.stables", pCfg->vndStats.numOfSTables) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "vndStats.ctables", pCfg->vndStats.numOfCTables) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "vndStats.ntables", pCfg->vndStats.numOfNTables) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "vndStats.timeseries", pCfg->vndStats.numOfTimeSeries) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "vndStats.ntimeseries", pCfg->vndStats.numOfNTimeSeries) < 0) return -1;

  SJson *nodeInfo = tjsonCreateArray();
  if (nodeInfo == NULL) return -1;
  if (tjsonAddItemToObject(pJson, "syncCfg.nodeInfo", nodeInfo) < 0) return -1;
  vDebug("vgId:%d, encode config, replicas:%d totalReplicas:%d selfIndex:%d changeVersion:%d",
        pCfg->vgId, pCfg->syncCfg.replicaNum,
         pCfg->syncCfg.totalReplicaNum, pCfg->syncCfg.myIndex, pCfg->syncCfg.changeVersion);
  for (int i = 0; i < pCfg->syncCfg.totalReplicaNum; ++i) {
    SJson     *info = tjsonCreateObject();
    SNodeInfo *pNode = (SNodeInfo *)&pCfg->syncCfg.nodeInfo[i];
    if (info == NULL) return -1;
    if (tjsonAddIntegerToObject(info, "nodePort", pNode->nodePort) < 0) return -1;
    if (tjsonAddStringToObject(info, "nodeFqdn", pNode->nodeFqdn) < 0) return -1;
    if (tjsonAddIntegerToObject(info, "nodeId", pNode->nodeId) < 0) return -1;
    if (tjsonAddIntegerToObject(info, "clusterId", pNode->clusterId) < 0) return -1;
    if (tjsonAddStringToObject(info, "isReplica", vnodeRoleToStr(pNode->nodeRole)) < 0) return -1;
    if (tjsonAddItemToArray(nodeInfo, info) < 0) return -1;
    vDebug("vgId:%d, encode config, replica:%d ep:%s:%u dnode:%d", pCfg->vgId, i, pNode->nodeFqdn, pNode->nodePort,
           pNode->nodeId);
  }

  return 0;
}

int vnodeDecodeConfig(const SJson *pJson, void *pObj) {
  SVnodeCfg *pCfg = (SVnodeCfg *)pObj;

  int32_t code;
  tjsonGetNumberValue(pJson, "vgId", pCfg->vgId, code);
  if (code < 0) return -1;
  if (tjsonGetStringValue(pJson, "dbname", pCfg->dbname) < 0) return -1;
  tjsonGetNumberValue(pJson, "dbId", pCfg->dbId, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "szPage", pCfg->szPage, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "szCache", pCfg->szCache, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "cacheLast", pCfg->cacheLast, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "cacheLastSize", pCfg->cacheLastSize, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "szBuf", pCfg->szBuf, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "isHeap", pCfg->isHeap, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "isWeak", pCfg->isWeak, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "isTsma", pCfg->isTsma, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "isRsma", pCfg->isRsma, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "precision", pCfg->tsdbCfg.precision, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "update", pCfg->tsdbCfg.update, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "compression", pCfg->tsdbCfg.compression, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "slLevel", pCfg->tsdbCfg.slLevel, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "daysPerFile", pCfg->tsdbCfg.days, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "minRows", pCfg->tsdbCfg.minRows, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "maxRows", pCfg->tsdbCfg.maxRows, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "keep0", pCfg->tsdbCfg.keep0, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "keep1", pCfg->tsdbCfg.keep1, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "keep2", pCfg->tsdbCfg.keep2, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "keepTimeOffset", pCfg->tsdbCfg.keepTimeOffset, code);
  if (code < 0) return -1;
  SJson  *pNodeRetentions = tjsonGetObjectItem(pJson, "retentions");
  int32_t nRetention = tjsonGetArraySize(pNodeRetentions);
  if (nRetention > TSDB_RETENTION_MAX) {
    nRetention = TSDB_RETENTION_MAX;
  }
  for (int32_t i = 0; i < nRetention; ++i) {
    SJson *pNodeRetention = tjsonGetArrayItem(pNodeRetentions, i);
    ASSERT(pNodeRetention != NULL);
    tjsonGetNumberValue(pNodeRetention, "freq", (pCfg->tsdbCfg.retentions)[i].freq, code);
    tjsonGetNumberValue(pNodeRetention, "freqUnit", (pCfg->tsdbCfg.retentions)[i].freqUnit, code);
    tjsonGetNumberValue(pNodeRetention, "keep", (pCfg->tsdbCfg.retentions)[i].keep, code);
    tjsonGetNumberValue(pNodeRetention, "keepUnit", (pCfg->tsdbCfg.retentions)[i].keepUnit, code);
  }
  tjsonGetNumberValue(pJson, "wal.vgId", pCfg->walCfg.vgId, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "wal.fsyncPeriod", pCfg->walCfg.fsyncPeriod, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "wal.retentionPeriod", pCfg->walCfg.retentionPeriod, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "wal.rollPeriod", pCfg->walCfg.rollPeriod, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "wal.retentionSize", pCfg->walCfg.retentionSize, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "wal.segSize", pCfg->walCfg.segSize, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "wal.level", pCfg->walCfg.level, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "sstTrigger", pCfg->sttTrigger, code);
  if (code < 0) pCfg->sttTrigger = TSDB_DEFAULT_SST_TRIGGER;
  tjsonGetNumberValue(pJson, "hashBegin", pCfg->hashBegin, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "hashEnd", pCfg->hashEnd, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "hashChange", pCfg->hashChange, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "hashMethod", pCfg->hashMethod, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "hashPrefix", pCfg->hashPrefix, code);
  if (code < 0) pCfg->hashPrefix = TSDB_DEFAULT_HASH_PREFIX;
  tjsonGetNumberValue(pJson, "hashSuffix", pCfg->hashSuffix, code);
  if (code < 0) pCfg->hashSuffix = TSDB_DEFAULT_HASH_SUFFIX;

  tjsonGetNumberValue(pJson, "syncCfg.replicaNum", pCfg->syncCfg.replicaNum, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "syncCfg.myIndex", pCfg->syncCfg.myIndex, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "syncCfg.changeVersion", pCfg->syncCfg.changeVersion, code);
  if (code < 0) return -1;

  tjsonGetNumberValue(pJson, "vndStats.stables", pCfg->vndStats.numOfSTables, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "vndStats.ctables", pCfg->vndStats.numOfCTables, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "vndStats.ntables", pCfg->vndStats.numOfNTables, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "vndStats.timeseries", pCfg->vndStats.numOfTimeSeries, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "vndStats.ntimeseries", pCfg->vndStats.numOfNTimeSeries, code);
  if (code < 0) return -1;

  SJson *nodeInfo = tjsonGetObjectItem(pJson, "syncCfg.nodeInfo");
  int    arraySize = tjsonGetArraySize(nodeInfo);
  pCfg->syncCfg.totalReplicaNum = arraySize;

  vDebug("vgId:%d, decode config, replicas:%d totalReplicas:%d selfIndex:%d", pCfg->vgId, pCfg->syncCfg.replicaNum,
         pCfg->syncCfg.totalReplicaNum, pCfg->syncCfg.myIndex);
  for (int i = 0; i < arraySize; ++i) {
    SJson     *info = tjsonGetArrayItem(nodeInfo, i);
    SNodeInfo *pNode = &pCfg->syncCfg.nodeInfo[i];
    if (info == NULL) return -1;
    tjsonGetNumberValue(info, "nodePort", pNode->nodePort, code);
    if (code < 0) return -1;
    tjsonGetStringValue(info, "nodeFqdn", pNode->nodeFqdn);
    tjsonGetNumberValue(info, "nodeId", pNode->nodeId, code);
    if (code < 0) return -1;
    tjsonGetNumberValue(info, "clusterId", pNode->clusterId, code);
    if (code < 0) return -1;
    char role[10] = {0};
    code = tjsonGetStringValue(info, "isReplica", role);
    if (code < 0) return -1;
    if (strlen(role) != 0) {
      pNode->nodeRole = vnodeStrToRole(role);
    } else {
      pNode->nodeRole = TAOS_SYNC_ROLE_VOTER;
    }
    vDebug("vgId:%d, decode config, replica:%d ep:%s:%u dnode:%d", pCfg->vgId, i, pNode->nodeFqdn, pNode->nodePort,
           pNode->nodeId);
  }

  tjsonGetNumberValue(pJson, "tsdbPageSize", pCfg->tsdbPageSize, code);
  if (code < 0 || pCfg->tsdbPageSize < TSDB_MIN_PAGESIZE_PER_VNODE * 1024) {
    pCfg->tsdbPageSize = TSDB_DEFAULT_TSDB_PAGESIZE * 1024;
  }

  return 0;
}

int vnodeValidateTableHash(SVnode *pVnode, char *tableFName) {
  uint32_t hashValue = 0;

  switch (pVnode->config.hashMethod) {
    default:
      hashValue = taosGetTbHashVal(tableFName, strlen(tableFName), pVnode->config.hashMethod, pVnode->config.hashPrefix,
                                   pVnode->config.hashSuffix);
      break;
  }

  if (hashValue < pVnode->config.hashBegin || hashValue > pVnode->config.hashEnd) {
    terrno = TSDB_CODE_VND_HASH_MISMATCH;
    return -1;
  }

  return 0;
}
