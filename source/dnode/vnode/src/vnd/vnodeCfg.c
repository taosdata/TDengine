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

#include "tglobal.h"
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
                                           .clearFiles = 0,
                                       },
                                   .hashBegin = 0,
                                   .hashEnd = 0,
                                   .hashMethod = 0,
                                   .sttTrigger = TSDB_DEFAULT_SST_TRIGGER,
                                   .s3ChunkSize = TSDB_DEFAULT_S3_CHUNK_SIZE,
                                   .s3KeepLocal = TSDB_DEFAULT_S3_KEEP_LOCAL,
                                   .s3Compact = TSDB_DEFAULT_S3_COMPACT,
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

  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "vgId", pCfg->vgId));
  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, "dbname", pCfg->dbname));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "dbId", pCfg->dbId));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "szPage", pCfg->szPage));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "szCache", pCfg->szCache));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "cacheLast", pCfg->cacheLast));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "cacheLastSize", pCfg->cacheLastSize));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "szBuf", pCfg->szBuf));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "isHeap", pCfg->isHeap));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "isWeak", pCfg->isWeak));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "isTsma", pCfg->isTsma));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "isRsma", pCfg->isRsma));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "precision", pCfg->tsdbCfg.precision));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "update", pCfg->tsdbCfg.update));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "compression", pCfg->tsdbCfg.compression));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "slLevel", pCfg->tsdbCfg.slLevel));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "daysPerFile", pCfg->tsdbCfg.days));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "minRows", pCfg->tsdbCfg.minRows));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "maxRows", pCfg->tsdbCfg.maxRows));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "keep0", pCfg->tsdbCfg.keep0));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "keep1", pCfg->tsdbCfg.keep1));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "keep2", pCfg->tsdbCfg.keep2));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "keepTimeOffset", pCfg->tsdbCfg.keepTimeOffset));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "s3ChunkSize", pCfg->s3ChunkSize));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "s3KeepLocal", pCfg->s3KeepLocal));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "s3Compact", pCfg->s3Compact));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "tsdbPageSize", pCfg->tsdbPageSize));
  if (pCfg->tsdbCfg.retentions[0].keep > 0) {
    int32_t nRetention = 1;
    if (pCfg->tsdbCfg.retentions[1].freq > 0) {
      ++nRetention;
      if (pCfg->tsdbCfg.retentions[2].freq > 0) {
        ++nRetention;
      }
    }
    SJson *pNodeRetentions = tjsonCreateArray();
    if (pNodeRetentions == NULL) {
      return terrno;
    }
    TAOS_CHECK_RETURN(tjsonAddItemToObject(pJson, "retentions", pNodeRetentions));
    for (int32_t i = 0; i < nRetention; ++i) {
      SJson            *pNodeRetention = tjsonCreateObject();
      const SRetention *pRetention = pCfg->tsdbCfg.retentions + i;
      TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pNodeRetention, "freq", pRetention->freq));
      TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pNodeRetention, "freqUnit", pRetention->freqUnit));
      TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pNodeRetention, "keep", pRetention->keep));
      TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pNodeRetention, "keepUnit", pRetention->keepUnit));
      TAOS_CHECK_RETURN(tjsonAddItemToArray(pNodeRetentions, pNodeRetention));
    }
  }
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "tsdb.encryptAlgorithm", pCfg->tsdbCfg.encryptAlgorithm));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "wal.vgId", pCfg->walCfg.vgId));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "wal.fsyncPeriod", pCfg->walCfg.fsyncPeriod));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "wal.retentionPeriod", pCfg->walCfg.retentionPeriod));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "wal.rollPeriod", pCfg->walCfg.rollPeriod));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "wal.retentionSize", pCfg->walCfg.retentionSize));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "wal.segSize", pCfg->walCfg.segSize));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "wal.level", pCfg->walCfg.level));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "wal.clearFiles", pCfg->walCfg.clearFiles));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "wal.encryptAlgorithm", pCfg->walCfg.encryptAlgorithm));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "tdbEncryptAlgorithm", pCfg->tdbEncryptAlgorithm));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "sstTrigger", pCfg->sttTrigger));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "hashBegin", pCfg->hashBegin));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "hashEnd", pCfg->hashEnd));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "hashChange", pCfg->hashChange));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "hashMethod", pCfg->hashMethod));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "hashPrefix", pCfg->hashPrefix));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "hashSuffix", pCfg->hashSuffix));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "syncCfg.replicaNum", pCfg->syncCfg.replicaNum));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "syncCfg.myIndex", pCfg->syncCfg.myIndex));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "syncCfg.changeVersion", pCfg->syncCfg.changeVersion));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "vndStats.stables", pCfg->vndStats.numOfSTables));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "vndStats.ctables", pCfg->vndStats.numOfCTables));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "vndStats.ntables", pCfg->vndStats.numOfNTables));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "vndStats.timeseries", pCfg->vndStats.numOfTimeSeries));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "vndStats.ntimeseries", pCfg->vndStats.numOfNTimeSeries));

  SJson *nodeInfo = tjsonCreateArray();
  if (nodeInfo == NULL) {
    return terrno;
  }
  TAOS_CHECK_RETURN(tjsonAddItemToObject(pJson, "syncCfg.nodeInfo", nodeInfo));
  vDebug("vgId:%d, encode config, replicas:%d totalReplicas:%d selfIndex:%d changeVersion:%d", pCfg->vgId,
         pCfg->syncCfg.replicaNum, pCfg->syncCfg.totalReplicaNum, pCfg->syncCfg.myIndex, pCfg->syncCfg.changeVersion);
  for (int i = 0; i < pCfg->syncCfg.totalReplicaNum; ++i) {
    SJson *info = tjsonCreateObject();
    if (info == NULL) {
      return terrno;
    }

    SNodeInfo *pNode = (SNodeInfo *)&pCfg->syncCfg.nodeInfo[i];
    TAOS_CHECK_RETURN(tjsonAddIntegerToObject(info, "nodePort", pNode->nodePort));
    TAOS_CHECK_RETURN(tjsonAddStringToObject(info, "nodeFqdn", pNode->nodeFqdn));
    TAOS_CHECK_RETURN(tjsonAddIntegerToObject(info, "nodeId", pNode->nodeId));
    TAOS_CHECK_RETURN(tjsonAddIntegerToObject(info, "clusterId", pNode->clusterId));
    TAOS_CHECK_RETURN(tjsonAddStringToObject(info, "isReplica", vnodeRoleToStr(pNode->nodeRole)));
    TAOS_CHECK_RETURN(tjsonAddItemToArray(nodeInfo, info));
    vDebug("vgId:%d, encode config, replica:%d ep:%s:%u dnode:%d", pCfg->vgId, i, pNode->nodeFqdn, pNode->nodePort,
           pNode->nodeId);
  }

  return 0;
}

int vnodeDecodeConfig(const SJson *pJson, void *pObj) {
  SVnodeCfg *pCfg = (SVnodeCfg *)pObj;

  int32_t code;
  tjsonGetNumberValue(pJson, "vgId", pCfg->vgId, code);
  if (code) return code;
  if ((code = tjsonGetStringValue(pJson, "dbname", pCfg->dbname))) return code;
  tjsonGetNumberValue(pJson, "dbId", pCfg->dbId, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "szPage", pCfg->szPage, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "szCache", pCfg->szCache, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "cacheLast", pCfg->cacheLast, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "cacheLastSize", pCfg->cacheLastSize, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "szBuf", pCfg->szBuf, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "isHeap", pCfg->isHeap, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "isWeak", pCfg->isWeak, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "isTsma", pCfg->isTsma, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "isRsma", pCfg->isRsma, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "precision", pCfg->tsdbCfg.precision, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "update", pCfg->tsdbCfg.update, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "compression", pCfg->tsdbCfg.compression, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "slLevel", pCfg->tsdbCfg.slLevel, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "daysPerFile", pCfg->tsdbCfg.days, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "minRows", pCfg->tsdbCfg.minRows, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "maxRows", pCfg->tsdbCfg.maxRows, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "keep0", pCfg->tsdbCfg.keep0, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "keep1", pCfg->tsdbCfg.keep1, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "keep2", pCfg->tsdbCfg.keep2, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "keepTimeOffset", pCfg->tsdbCfg.keepTimeOffset, code);
  if (code) return code;
  SJson  *pNodeRetentions = tjsonGetObjectItem(pJson, "retentions");
  int32_t nRetention = tjsonGetArraySize(pNodeRetentions);
  if (nRetention > TSDB_RETENTION_MAX) {
    nRetention = TSDB_RETENTION_MAX;
  }
  for (int32_t i = 0; i < nRetention; ++i) {
    SJson *pNodeRetention = tjsonGetArrayItem(pNodeRetentions, i);
    tjsonGetNumberValue(pNodeRetention, "freq", (pCfg->tsdbCfg.retentions)[i].freq, code);
    if (code) return code;
    tjsonGetNumberValue(pNodeRetention, "freqUnit", (pCfg->tsdbCfg.retentions)[i].freqUnit, code);
    if (code) return code;
    tjsonGetNumberValue(pNodeRetention, "keep", (pCfg->tsdbCfg.retentions)[i].keep, code);
    if (code) return code;
    tjsonGetNumberValue(pNodeRetention, "keepUnit", (pCfg->tsdbCfg.retentions)[i].keepUnit, code);
    if (code) return code;
  }
  tjsonGetNumberValue(pJson, "tsdb.encryptAlgorithm", pCfg->tsdbCfg.encryptAlgorithm, code);
  if (code) return code;
#if defined(TD_ENTERPRISE)
  if (pCfg->tsdbCfg.encryptAlgorithm == DND_CA_SM4) {
    if (tsEncryptKey[0] == 0) {
      return terrno = TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
    } else {
      strncpy(pCfg->tsdbCfg.encryptKey, tsEncryptKey, ENCRYPT_KEY_LEN);
    }
  }
#endif
  tjsonGetNumberValue(pJson, "wal.vgId", pCfg->walCfg.vgId, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "wal.fsyncPeriod", pCfg->walCfg.fsyncPeriod, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "wal.retentionPeriod", pCfg->walCfg.retentionPeriod, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "wal.rollPeriod", pCfg->walCfg.rollPeriod, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "wal.retentionSize", pCfg->walCfg.retentionSize, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "wal.segSize", pCfg->walCfg.segSize, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "wal.level", pCfg->walCfg.level, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "wal.clearFiles", pCfg->walCfg.clearFiles, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "wal.encryptAlgorithm", pCfg->walCfg.encryptAlgorithm, code);
  if (code) return code;
#if defined(TD_ENTERPRISE)
  if (pCfg->walCfg.encryptAlgorithm == DND_CA_SM4) {
    if (tsEncryptKey[0] == 0) {
      return terrno = TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
    } else {
      strncpy(pCfg->walCfg.encryptKey, tsEncryptKey, ENCRYPT_KEY_LEN);
    }
  }
#endif
  tjsonGetNumberValue(pJson, "tdbEncryptAlgorithm", pCfg->tdbEncryptAlgorithm, code);
  if (code) return code;
#if defined(TD_ENTERPRISE)
  if (pCfg->tdbEncryptAlgorithm == DND_CA_SM4) {
    if (tsEncryptKey[0] == 0) {
      return terrno = TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
    } else {
      strncpy(pCfg->tdbEncryptKey, tsEncryptKey, ENCRYPT_KEY_LEN);
    }
  }
#endif
  tjsonGetNumberValue(pJson, "sstTrigger", pCfg->sttTrigger, code);
  if (code < 0) pCfg->sttTrigger = TSDB_DEFAULT_SST_TRIGGER;
  tjsonGetNumberValue(pJson, "hashBegin", pCfg->hashBegin, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "hashEnd", pCfg->hashEnd, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "hashChange", pCfg->hashChange, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "hashMethod", pCfg->hashMethod, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "hashPrefix", pCfg->hashPrefix, code);
  if (code < 0) pCfg->hashPrefix = TSDB_DEFAULT_HASH_PREFIX;
  tjsonGetNumberValue(pJson, "hashSuffix", pCfg->hashSuffix, code);
  if (code < 0) pCfg->hashSuffix = TSDB_DEFAULT_HASH_SUFFIX;

  tjsonGetNumberValue(pJson, "syncCfg.replicaNum", pCfg->syncCfg.replicaNum, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "syncCfg.myIndex", pCfg->syncCfg.myIndex, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "syncCfg.changeVersion", pCfg->syncCfg.changeVersion, code);
  if (code) return code;

  tjsonGetNumberValue(pJson, "vndStats.stables", pCfg->vndStats.numOfSTables, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "vndStats.ctables", pCfg->vndStats.numOfCTables, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "vndStats.ntables", pCfg->vndStats.numOfNTables, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "vndStats.timeseries", pCfg->vndStats.numOfTimeSeries, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "vndStats.ntimeseries", pCfg->vndStats.numOfNTimeSeries, code);
  if (code) return code;

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
    if (code) return code;
    code = tjsonGetStringValue(info, "nodeFqdn", pNode->nodeFqdn);
    tjsonGetNumberValue(info, "nodeId", pNode->nodeId, code);
    if (code) return code;
    tjsonGetNumberValue(info, "clusterId", pNode->clusterId, code);
    if (code) return code;
    char role[10] = {0};
    code = tjsonGetStringValue(info, "isReplica", role);
    if (code) return code;
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

  tjsonGetNumberValue(pJson, "s3ChunkSize", pCfg->s3ChunkSize, code);
  if (code < 0 || pCfg->s3ChunkSize < TSDB_MIN_S3_CHUNK_SIZE) {
    pCfg->s3ChunkSize = TSDB_DEFAULT_S3_CHUNK_SIZE;
  }
  tjsonGetNumberValue(pJson, "s3KeepLocal", pCfg->s3KeepLocal, code);
  if (code < 0 || pCfg->s3KeepLocal < TSDB_MIN_S3_KEEP_LOCAL) {
    pCfg->s3KeepLocal = TSDB_DEFAULT_S3_KEEP_LOCAL;
  }
  tjsonGetNumberValue(pJson, "s3Compact", pCfg->s3Compact, code);
  if (code < 0) {
    pCfg->s3Compact = TSDB_DEFAULT_S3_COMPACT;
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
    return terrno = TSDB_CODE_VND_HASH_MISMATCH;
  }

  return 0;
}
