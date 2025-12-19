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

#define ALLOW_FORBID_FUNC
#include <arpa/inet.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include "auth.h"
#include "cJSON.h"
#include "dbRest.h"
#include "grant.h"
#include "mndGrant.h"
#include "os.h"
#include "taoserror.h"
#include "tbase64.h"
#include "tchecksum.h"
#include "tdef.h"
#include "tdes.h"
#include "tglobal.h"
#include "thash.h"
#include "tjson.h"
#include "tlog.h"
#include "trpc.h"
#include "tthread.h"
#include "ttime.h"

extern SGrantStatus gStatus;
extern int32_t      grantUniqGenActiveCode(SGrantUniqObj *grant);
extern void         tDestroyGrantUniqObj(SGrantUniqObj *obj);

SHashObj *gAuthQuotaHash = NULL;

static TdThread      gAuthQuotaRefreshThread;
static volatile bool gAuthQuotaRefreshStop = false;
static SMnode       *gAuthServerMnode = NULL;

#define AUTH_QUOTA_REFRESH_INTERVAL_SEC 60

static bool quotaItemEqual(const SAuthQuotaItem *a, const SAuthQuotaItem *b) {
  if (a == NULL && b == NULL) return true;
  if (a == NULL || b == NULL) return false;
  return (a->expireDate == b->expireDate) && (a->limitQuantity == b->limitQuantity) && (a->limitSpeed == b->limitSpeed);
}

static bool checkIsUpdated(const SAuthQuota *pQuota, const SAuthReqData *pAuthReqData) {
  if (!pQuota || !pAuthReqData) {
    return false;
  }

  const char *authUsage = pAuthReqData->auth_usage;
  if (!authUsage || strlen(authUsage) == 0) {
    uWarn("auth_usage is empty, treat as no update needed");
    return false;
  }

  int64_t limitTimeSeries = 0;
  int64_t limitDnodes = 0;
  int32_t limitCpuCores = 0;
  int32_t limitVnodes = 0;
  int64_t limitStorageSize = 0;

  char tmpBuf[1024] = {0};
  tstrncpy(tmpBuf, authUsage, sizeof(tmpBuf));

  char *saveptr = NULL;
  char *token = strtok_r(tmpBuf, ",", &saveptr);

  while (token != NULL) {
    while (*token == ' ') token++;

    if (strncmp(token, "timeseries:", 11) == 0) {
      char *values = token + 11;
      char *slash = strchr(values, '/');
      if (slash) {
        char *limitStr = slash + 1;
        if (strncmp(limitStr, "un", 2) == 0) {
          limitTimeSeries = GRANT_UNIQ_UNLIMITED;
        } else {
          limitTimeSeries = atoll(limitStr);
        }
      }
    } else if (strncmp(token, "dnodes:", 7) == 0) {
      char *values = token + 7;
      char *slash = strchr(values, '/');
      if (slash) {
        char *limitStr = slash + 1;
        if (strncmp(limitStr, "un", 2) == 0) {
          limitDnodes = GRANT_UNIQ_UNLIMITED;
        } else {
          limitDnodes = atoll(limitStr);
        }
      }
    } else if (strncmp(token, "cpucores:", 9) == 0) {
      char *values = token + 9;
      char *slash = strchr(values, '/');
      if (slash) {
        char *limitStr = slash + 1;
        if (strncmp(limitStr, "un", 2) == 0) {
          limitCpuCores = GRANT_UNIQ_UNLIMITED;
        } else {
          limitCpuCores = atoi(limitStr);
        }
      }
    } else if (strncmp(token, "vnodes:", 7) == 0) {
      char *values = token + 7;
      char *slash = strchr(values, '/');
      if (slash) {
        char *limitStr = slash + 1;
        if (strncmp(limitStr, "un", 2) == 0) {
          limitVnodes = GRANT_UNIQ_UNLIMITED;
        } else {
          limitVnodes = atoi(limitStr);
        }
      }
    } else if (strncmp(token, "storage:", 8) == 0) {
      char *values = token + 8;
      char *slash = strchr(values, '/');
      if (slash) {
        char *limitStr = slash + 1;
        if (strncmp(limitStr, "un", 2) == 0) {
          limitStorageSize = GRANT_UNIQ_UNLIMITED;
        } else {
          limitStorageSize = atoll(limitStr);
        }
      }
    }

    token = strtok_r(NULL, ",", &saveptr);
  }

  bool isUpdated = false;

  if (pQuota->limitTimeSeries != limitTimeSeries) {
    uDebug("timeseries limit changed: %" PRIi64 " -> %" PRIi64, pQuota->limitTimeSeries, limitTimeSeries);
    isUpdated = true;
  }

  if (pQuota->limitDnodes != limitDnodes) {
    uDebug("dnodes limit changed: %d -> %d", (int)pQuota->limitDnodes, (int)limitDnodes);
    isUpdated = true;
  }

  if (pQuota->limitCpuCores != limitCpuCores) {
    uDebug("cpucores limit changed: %d -> %d", pQuota->limitCpuCores, limitCpuCores);
    isUpdated = true;
  }

  if (pQuota->limitVnodes != limitVnodes) {
    uDebug("vnodes limit changed: %d -> %d", pQuota->limitVnodes, limitVnodes);
    isUpdated = true;
  }

  if (pQuota->limitStorageSize != limitStorageSize) {
    uDebug("storage limit changed: %" PRIi64 " -> %" PRIi64, pQuota->limitStorageSize, limitStorageSize);
    isUpdated = true;
  }

  return isUpdated;
}

static bool authQuotaEqual(const SAuthQuota *a, const SAuthQuota *b) {
  if (a == NULL && b == NULL) return true;
  if (a == NULL || b == NULL) return false;

  if (a->enable != b->enable) return false;
  if (a->service != b->service) return false;
  if (a->expireDays != b->expireDays) return false;

  if (a->limitTimeSeries != b->limitTimeSeries) return false;
  if (a->limitCpuCores != b->limitCpuCores) return false;
  if (a->limitDnodes != b->limitDnodes) return false;
  if (a->limitVnodes != b->limitVnodes) return false;
  if (a->limitStorageSize != b->limitStorageSize) return false;

  if (!quotaItemEqual(&a->stream, &b->stream)) return false;
  if (!quotaItemEqual(&a->subscription, &b->subscription)) return false;
  if (!quotaItemEqual(&a->view, &b->view)) return false;

  if (a->audit != b->audit) return false;
  if (a->storage != b->storage) return false;
  if (a->dataSync != b->dataSync) return false;
  if (a->backupRestore != b->backupRestore) return false;
  if (a->sharedStorage != b->sharedStorage) return false;
  if (a->activeActive != b->activeActive) return false;
  if (a->dualReplica != b->dualReplica) return false;
  if (a->dbEncrypt != b->dbEncrypt) return false;

  if (!quotaItemEqual(&a->tdgpt, &b->tdgpt)) return false;
  if (!quotaItemEqual(&a->mount, &b->mount)) return false;

  if (!quotaItemEqual(&a->opc_da, &b->opc_da)) return false;
  if (!quotaItemEqual(&a->opc_ua, &b->opc_ua)) return false;
  if (!quotaItemEqual(&a->pi, &b->pi)) return false;
  if (!quotaItemEqual(&a->kafka, &b->kafka)) return false;
  if (!quotaItemEqual(&a->influxdb, &b->influxdb)) return false;
  if (!quotaItemEqual(&a->mqtt, &b->mqtt)) return false;
  if (!quotaItemEqual(&a->avevahistorian, &b->avevahistorian)) return false;
  if (!quotaItemEqual(&a->opentsdb, &b->opentsdb)) return false;
  if (!quotaItemEqual(&a->td2_6, &b->td2_6)) return false;
  if (!quotaItemEqual(&a->td3_0, &b->td3_0)) return false;
  if (!quotaItemEqual(&a->mysql, &b->mysql)) return false;
  if (!quotaItemEqual(&a->postgres, &b->postgres)) return false;
  if (!quotaItemEqual(&a->oracle, &b->oracle)) return false;
  if (!quotaItemEqual(&a->mssql, &b->mssql)) return false;
  if (!quotaItemEqual(&a->mongodb, &b->mongodb)) return false;
  if (!quotaItemEqual(&a->csv, &b->csv)) return false;
  if (!quotaItemEqual(&a->sparkplugb, &b->sparkplugb)) return false;
  if (!quotaItemEqual(&a->orc, &b->orc)) return false;
  if (!quotaItemEqual(&a->kinghist, &b->kinghist)) return false;
  if (!quotaItemEqual(&a->pulsar, &b->pulsar)) return false;

  // IDMP
  if (a->idmpExpireDays != b->idmpExpireDays) return false;
  if (a->idmpLimitTsAttributes != b->idmpLimitTsAttributes) return false;
  if (a->idmpLimitNonTsAttributes != b->idmpLimitNonTsAttributes) return false;
  if (a->idmpLimitElements != b->idmpLimitElements) return false;
  if (a->idmpLimitServers != b->idmpLimitServers) return false;
  if (a->idmpLimitCpuCores != b->idmpLimitCpuCores) return false;
  if (a->idmpLimitUsers != b->idmpLimitUsers) return false;
  if (a->idmpVersionCtrl != b->idmpVersionCtrl) return false;
  if (a->idmpDataForecast != b->idmpDataForecast) return false;
  if (a->idmpDataDetect != b->idmpDataDetect) return false;
  if (a->idmpDataQuality != b->idmpDataQuality) return false;
  if (a->idmpAiChatGen != b->idmpAiChatGen) return false;

  if ((a->extensionArray == NULL) != (b->extensionArray == NULL)) return false;
  if ((a->idmpExtensionArray == NULL) != (b->idmpExtensionArray == NULL)) return false;

  return true;
}

static int32_t checkAuthServer() {
  // TODO：remove
  if (!gStatus.checkAuthServer) {
    // if (!tsAuthServer) {        //only for debug
    uError("not an auth server, cannot process auth request");
    return TSDB_CODE_GRANT_INVALID_AUTH_SERVER;
  }

  if (gStatus.expired) {
    uError("auth server grant is expired, cannot process auth request");
    return TSDB_CODE_GRANT_EXPIRED;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkAuthQuotaExpireLimits(const char *clusterId, SAuthQuota *pAuthQuota) {
  if (!pAuthQuota) return TSDB_CODE_INVALID_PARA;

  if (!gAuthQuotaHash) {
    return TSDB_CODE_INVALID_PARA;
  }

  // only check basic expire days
  int32_t basicExpireDays = grantGetBasicExpireDays(true);
  if (basicExpireDays < pAuthQuota->expireDays) {
    uError("cluster %s quota expireDays check failed: expireDays=%d exceeds basicExpireDays=%d",
           clusterId ? clusterId : "unknown", pAuthQuota->expireDays, basicExpireDays);
    return TSDB_CODE_GRANT_QUOTA_OUT_OF_RANGE;
  }

  // 定义宏：检查子项的过期日期是否超出基础过期日期
  // 规则：如果 expireDays != UNLIMITED，则所有子项的 expireDate 必须 <= expireDays
#define CHECK_EXPIRE_DATE(field, name)                                                                            \
  do {                                                                                                            \
    if (pAuthQuota->expireDays != GRANT_UNIQ_UNLIMITED) {                                                         \
      if (pAuthQuota->field.expireDate == GRANT_UNIQ_UNLIMITED) {                                                 \
        uError("cluster %s quota " name " expireDate check failed: expireDays=%d, " name ".expireDate=UNLIMITED", \
               clusterId ? clusterId : "unknown", pAuthQuota->expireDays);                                        \
        return TSDB_CODE_GRANT_QUOTA_OUT_OF_RANGE;                                                                \
      }                                                                                                           \
      if (pAuthQuota->expireDays < pAuthQuota->field.expireDate) {                                                \
        uError("cluster %s quota " name " expireDate check failed: expireDays=%d < " name ".expireDate=%d",       \
               clusterId ? clusterId : "unknown", pAuthQuota->expireDays, pAuthQuota->field.expireDate);          \
        return TSDB_CODE_GRANT_QUOTA_OUT_OF_RANGE;                                                                \
      }                                                                                                           \
    }                                                                                                             \
  } while (0)

  CHECK_EXPIRE_DATE(stream, "stream");
  CHECK_EXPIRE_DATE(subscription, "subscription");
  CHECK_EXPIRE_DATE(view, "view");
  CHECK_EXPIRE_DATE(tdgpt, "tdgpt");
  CHECK_EXPIRE_DATE(mount, "mount");
  CHECK_EXPIRE_DATE(opc_da, "opc_da");
  CHECK_EXPIRE_DATE(opc_ua, "opc_ua");
  CHECK_EXPIRE_DATE(pi, "pi");
  CHECK_EXPIRE_DATE(kafka, "kafka");
  CHECK_EXPIRE_DATE(influxdb, "influxdb");
  CHECK_EXPIRE_DATE(mqtt, "mqtt");
  CHECK_EXPIRE_DATE(avevahistorian, "avevahistorian");
  CHECK_EXPIRE_DATE(opentsdb, "opentsdb");
  CHECK_EXPIRE_DATE(td2_6, "td2_6");
  CHECK_EXPIRE_DATE(td3_0, "td3_0");
  CHECK_EXPIRE_DATE(mysql, "mysql");
  CHECK_EXPIRE_DATE(postgres, "postgres");
  CHECK_EXPIRE_DATE(oracle, "oracle");
  CHECK_EXPIRE_DATE(mssql, "mssql");
  CHECK_EXPIRE_DATE(mongodb, "mongodb");
  CHECK_EXPIRE_DATE(csv, "csv");
  CHECK_EXPIRE_DATE(sparkplugb, "sparkplugb");
  CHECK_EXPIRE_DATE(orc, "orc");
  CHECK_EXPIRE_DATE(kinghist, "kinghist");
  CHECK_EXPIRE_DATE(pulsar, "pulsar");

#undef CHECK_EXPIRE_DATE

  // IMDP
  // int32_t idmpBasicExpireDays = grantGetBasicExpireDays(false);
  // if (idmpBasicExpireDays < pAuthQuota->idmpExpireDays) {
  //   return TSDB_CODE_GRANT_QUOTA_OUT_OF_RANGE;
  // }

  // if (pAuthQuota->idmpExpireDays != GRANT_UNIQ_UNLIMITED) {
  //   if (pAuthQuota->idmpLimitTsAttributes == GRANT_UNIQ_UNLIMITED ||
  //       pAuthQuota->idmpExpireDays < pAuthQuota->idmpLimitTsAttributes) {
  //     return TSDB_CODE_GRANT_QUOTA_OUT_OF_RANGE;
  //   }
  //   if (pAuthQuota->idmpLimitNonTsAttributes == GRANT_UNIQ_UNLIMITED ||
  //       pAuthQuota->idmpExpireDays < pAuthQuota->idmpLimitNonTsAttributes) {
  //     return TSDB_CODE_GRANT_QUOTA_OUT_OF_RANGE;
  //   }
  //   if (pAuthQuota->idmpLimitElements == GRANT_UNIQ_UNLIMITED ||
  //       pAuthQuota->idmpExpireDays < pAuthQuota->idmpLimitElements) {
  //     return TSDB_CODE_GRANT_QUOTA_OUT_OF_RANGE;
  //   }
  //   if (pAuthQuota->idmpLimitServers == GRANT_UNIQ_UNLIMITED ||
  //       pAuthQuota->idmpExpireDays < pAuthQuota->idmpLimitServers) {
  //     return TSDB_CODE_GRANT_QUOTA_OUT_OF_RANGE;
  //   }
  //   if (pAuthQuota->idmpLimitCpuCores == GRANT_UNIQ_UNLIMITED ||
  //       pAuthQuota->idmpExpireDays < pAuthQuota->idmpLimitCpuCores) {
  //     return TSDB_CODE_GRANT_QUOTA_OUT_OF_RANGE;
  //   }
  //   if (pAuthQuota->idmpLimitUsers == GRANT_UNIQ_UNLIMITED || pAuthQuota->idmpExpireDays <
  //   pAuthQuota->idmpLimitUsers) {
  //     return TSDB_CODE_GRANT_QUOTA_OUT_OF_RANGE;
  //   }
  //   if (pAuthQuota->idmpVersionCtrl == GRANT_UNIQ_UNLIMITED ||
  //       pAuthQuota->idmpExpireDays < pAuthQuota->idmpVersionCtrl) {
  //     return TSDB_CODE_GRANT_QUOTA_OUT_OF_RANGE;
  //   }
  // }

  return TSDB_CODE_SUCCESS;
}

static int32_t checkAuthQuotaNumLimits(const char *clusterId, size_t clusterIdLen, SAuthQuota *pAuthQuota) {
  if (!gAuthQuotaHash) {
    return TSDB_CODE_INVALID_PARA;
  }
  // basic resource limits
  int64_t     totalTimeSeries = 0;
  int64_t     totalCpuCores = 0;
  int64_t     totalDnodes = 0;
  int64_t     totalVnodes = 0;
  int64_t     totalStorageSize = 0;
  SAuthQuota *pQuota = NULL;

  // 定义宏：累加资源限制
#define ACCUMULATE_RESOURCE_LIMIT(field, total)             \
  do {                                                      \
    if (pQuota->field > 0 && total != GRANT_UNIQ_UNLIMITED) \
      total += pQuota->field;                               \
    else if (pQuota->field == GRANT_UNIQ_UNLIMITED)         \
      total = GRANT_UNIQ_UNLIMITED;                         \
  } while (0)

  void *pIter = taosHashIterate(gAuthQuotaHash, NULL);
  while (pIter != NULL) {
    size_t keyLen = 0;
    char  *key = (char *)taosHashGetKey(pIter, &keyLen);

    if (keyLen == clusterIdLen && memcmp(key, clusterId, clusterIdLen) == 0) {
      pIter = taosHashIterate(gAuthQuotaHash, pIter);
      continue;
    }

    pQuota = (SAuthQuota *)pIter;
    if (pQuota->enable == false) {
      pIter = taosHashIterate(gAuthQuotaHash, pIter);
      continue;
    }

    ACCUMULATE_RESOURCE_LIMIT(limitTimeSeries, totalTimeSeries);
    ACCUMULATE_RESOURCE_LIMIT(limitStorageSize, totalStorageSize);
    ACCUMULATE_RESOURCE_LIMIT(limitCpuCores, totalCpuCores);
    ACCUMULATE_RESOURCE_LIMIT(limitDnodes, totalDnodes);
    ACCUMULATE_RESOURCE_LIMIT(limitVnodes, totalVnodes);

    pIter = taosHashIterate(gAuthQuotaHash, pIter);
  }

  // add current request quota (reuse pQuota for the macro)
  pQuota = pAuthQuota;
  ACCUMULATE_RESOURCE_LIMIT(limitTimeSeries, totalTimeSeries);
  ACCUMULATE_RESOURCE_LIMIT(limitCpuCores, totalCpuCores);
  ACCUMULATE_RESOURCE_LIMIT(limitDnodes, totalDnodes);
  ACCUMULATE_RESOURCE_LIMIT(limitVnodes, totalVnodes);
  ACCUMULATE_RESOURCE_LIMIT(limitStorageSize, totalStorageSize);

#undef ACCUMULATE_RESOURCE_LIMIT

  // 定义宏：检查服务器限制并打印详细错误日志
#define CHECK_SERVER_LIMIT(field, total, name)                                                               \
  do {                                                                                                       \
    if (gStatus.field != GRANT_UNIQ_UNLIMITED && (total > gStatus.field || total == GRANT_UNIQ_UNLIMITED)) { \
      if (total == GRANT_UNIQ_UNLIMITED) {                                                                   \
        uError("cluster %s " name " quota exceeded: total=UNLIMITED, server limit=%" PRIi64,                 \
               clusterId ? clusterId : "unknown", (int64_t)gStatus.field);                                   \
      } else {                                                                                               \
        uError("cluster %s " name " quota exceeded: total=%" PRIi64 ", server limit=%" PRIi64,               \
               clusterId ? clusterId : "unknown", (int64_t)total, (int64_t)gStatus.field);                   \
      }                                                                                                      \
      return TSDB_CODE_GRANT_QUOTA_OUT_OF_RANGE;                                                             \
    }                                                                                                        \
  } while (0)

  // compare with server gStatus
  CHECK_SERVER_LIMIT(limitTimeSeries, totalTimeSeries, "limitTimeSeries");
  CHECK_SERVER_LIMIT(limitCpuCores, totalCpuCores, "limitCpuCores");
  CHECK_SERVER_LIMIT(limitDnodes, totalDnodes, "limitDnodes");
  CHECK_SERVER_LIMIT(limitVnodes, totalVnodes, "limitVnodes");
  CHECK_SERVER_LIMIT(limitStorageSize, totalStorageSize, "limitStorageSize");

#undef CHECK_SERVER_LIMIT

  return TSDB_CODE_SUCCESS;
}

static bool checkAuthClientsLimit(SAuthQuota *pAuthQuota) {
  if (!pAuthQuota || !gAuthQuotaHash) return false;

  if (gStatus.limitAuthClients == GRANT_UNIQ_UNLIMITED) {
    return true;
  }

  int16_t     totalAuthClients = 0;
  SAuthQuota *pQuota = NULL;
  void       *pIter = taosHashIterate(gAuthQuotaHash, NULL);
  while (pIter != NULL) {
    size_t keyLen = 0;
    char  *key = (char *)taosHashGetKey(pIter, &keyLen);

    pQuota = (SAuthQuota *)pIter;
    if (pQuota->enable == true) {
      totalAuthClients++;
    }

    pIter = taosHashIterate(gAuthQuotaHash, pIter);
  }

  if (totalAuthClients > gStatus.limitAuthClients) {
    uError("auth clients limit exceeded: totalAuthClients=%d, limitAuthClients=%d", totalAuthClients,
           gStatus.limitAuthClients);
    return false;
  }

  return true;
}

static int32_t convertAuthQuotaToGrantUniqObj(SAuthQuota *pAuthQuota, SGrantUniqObj *pGrantObj) {
  if (!pAuthQuota || !pGrantObj) return TSDB_CODE_INVALID_PARA;

  int32_t code = 0;

  int32_t nowDays = (int32_t)(taosGetTimestampMs() / 86400000);
  int32_t grantDays = nowDays + MAX_AUTH_DAY;

  pGrantObj->officialVersion = 1;

  // basic expireDays
#define SET_EXPIRE_DAYS(srcField, dstIndex)                                                                    \
  do {                                                                                                         \
    if (pAuthQuota->srcField != QUOTA_UNDEFINED) {                                                             \
      if (pAuthQuota->srcField == GRANT_UNIQ_UNLIMITED) {                                                      \
        pGrantObj->expireDays[dstIndex] = grantDays;                                                           \
      } else {                                                                                                 \
        pGrantObj->expireDays[dstIndex] = pAuthQuota->srcField > grantDays ? grantDays : pAuthQuota->srcField; \
      }                                                                                                        \
    }                                                                                                          \
  } while (0)

// basic limit
#define SET_LIMIT_FIELD(srcField, dstField)        \
  do {                                             \
    if (pAuthQuota->srcField != QUOTA_UNDEFINED) { \
      pGrantObj->dstField = pAuthQuota->srcField;  \
    }                                              \
  } while (0)

// quota item (expireDate + limitQuantity)
#define SET_QUOTA_ITEM(itemField, expireIndex, limitField)                                               \
  do {                                                                                                   \
    if (pAuthQuota->itemField.expireDate != QUOTA_UNDEFINED) {                                           \
      if (pAuthQuota->itemField.expireDate == GRANT_UNIQ_UNLIMITED) {                                    \
        pGrantObj->expireDays[expireIndex] = grantDays;                                                  \
      } else {                                                                                           \
        pGrantObj->expireDays[expireIndex] =                                                             \
            pAuthQuota->itemField.expireDate > grantDays ? grantDays : pAuthQuota->itemField.expireDate; \
      }                                                                                                  \
      if (pAuthQuota->itemField.limitQuantity != QUOTA_UNDEFINED) {                                      \
        pGrantObj->limitField = pAuthQuota->itemField.limitQuantity;                                     \
      }                                                                                                  \
    }                                                                                                    \
  } while (0)

  SET_EXPIRE_DAYS(service, GRANT_OPT_SERVICE);
  SET_EXPIRE_DAYS(expireDays, GRANT_OPT_BASIC);
  SET_LIMIT_FIELD(limitTimeSeries, limitTimeSeries);
  SET_LIMIT_FIELD(limitCpuCores, limitCpuCores);
  SET_LIMIT_FIELD(limitDnodes, limitDnodes);
  SET_LIMIT_FIELD(limitVnodes, limitVnodes);
  SET_LIMIT_FIELD(limitStorageSize, limitStorageSize);

  SET_QUOTA_ITEM(stream, GRANT_OPT_STREAM, limitStreams);
  SET_QUOTA_ITEM(subscription, GRANT_OPT_SUBSCRIPTION, limitSubscriptions);
  SET_QUOTA_ITEM(view, GRANT_OPT_VIEW, limitViews);

  SET_EXPIRE_DAYS(audit, GRANT_OPT_AUDIT);
  SET_EXPIRE_DAYS(storage, GRANT_OPT_STORAGE);
  SET_EXPIRE_DAYS(backupRestore, GRANT_OPT_DATA_BAK_RST);

#undef SET_EXPIRE_DAYS
#undef SET_LIMIT_FIELD
#undef SET_QUOTA_ITEM

// addDynamicGrantItem expireDay
#define ADD_DYNAMIC_ITEM_SIMPLE(srcField, itemName, name)                                          \
  do {                                                                                             \
    if (pAuthQuota->srcField != QUOTA_UNDEFINED) {                                                 \
      code = addDynamicGrantItem(pGrantObj, itemName, pAuthQuota->srcField, GRANT_UNIQ_UNDEFINED); \
      if (code != TSDB_CODE_SUCCESS) {                                                             \
        uError("failed to add dynamic grant item " name ", errMsg:%s", tstrerror(code));           \
        return code;                                                                               \
      }                                                                                            \
    }                                                                                              \
  } while (0)

// addDynamicGrantItem expireDay,num
#define ADD_DYNAMIC_ITEM_FROM_QUOTA(itemField, itemName, name)                           \
  do {                                                                                   \
    if (pAuthQuota->itemField.expireDate != QUOTA_UNDEFINED) {                           \
      code = addDynamicGrantItem(pGrantObj, itemName, pAuthQuota->itemField.expireDate,  \
                                 pAuthQuota->itemField.limitQuantity);                   \
      if (code != TSDB_CODE_SUCCESS) {                                                   \
        uError("failed to add dynamic grant item " name ", errMsg:%s", tstrerror(code)); \
        return code;                                                                     \
      }                                                                                  \
    }                                                                                    \
  } while (0)

// addDynamicGrantItem2
#define ADD_DYNAMIC_ITEM2(itemField, itemName, name)                                                      \
  do {                                                                                                    \
    if (pAuthQuota->itemField.expireDate != QUOTA_UNDEFINED) {                                            \
      code = addDynamicGrantItem2(pGrantObj, itemName, pAuthQuota->itemField.expireDate,                  \
                                  pAuthQuota->itemField.limitQuantity, pAuthQuota->itemField.limitSpeed); \
      if (code != TSDB_CODE_SUCCESS) {                                                                    \
        uError("failed to add dynamic grant item2 " name ", errMsg:%s", tstrerror(code));                 \
        return code;                                                                                      \
      }                                                                                                   \
    }                                                                                                     \
  } while (0)

  // expireday
  ADD_DYNAMIC_ITEM_SIMPLE(dataSync, "data_sync", "data_sync");
  ADD_DYNAMIC_ITEM_SIMPLE(sharedStorage, "object_storage", "object_storage");
  ADD_DYNAMIC_ITEM_SIMPLE(activeActive, "active_active", "active_active");
  ADD_DYNAMIC_ITEM_SIMPLE(dualReplica, "dual_replica", "dual_replica");
  ADD_DYNAMIC_ITEM_SIMPLE(dbEncrypt, "db_encryption", "db_encryption");

  // expireday,num
  ADD_DYNAMIC_ITEM_FROM_QUOTA(tdgpt, "tdgpt", "tdgpt");
  // ADD_DYNAMIC_ITEM_FROM_QUOTA(mount, "mount", "mount");

  // expireday,num,speed
  ADD_DYNAMIC_ITEM2(opc_da, "opc_da", "opc_da");
  ADD_DYNAMIC_ITEM2(opc_ua, "opc_ua", "opc_ua");
  ADD_DYNAMIC_ITEM2(pi, "pi", "pi");
  ADD_DYNAMIC_ITEM2(kafka, "kafka", "kafka");
  ADD_DYNAMIC_ITEM2(influxdb, "influxdb", "influxdb");
  ADD_DYNAMIC_ITEM2(mqtt, "mqtt", "mqtt");
  ADD_DYNAMIC_ITEM2(avevahistorian, "avevahistorian", "avevahistorian");
  ADD_DYNAMIC_ITEM2(opentsdb, "opentsdb", "opentsdb");
  ADD_DYNAMIC_ITEM2(td2_6, "td2.6", "td2.6");
  ADD_DYNAMIC_ITEM2(td3_0, "td3.0", "td3.0");
  ADD_DYNAMIC_ITEM2(mysql, "mysql", "mysql");
  ADD_DYNAMIC_ITEM2(postgres, "postgres", "postgres");
  ADD_DYNAMIC_ITEM2(oracle, "oracle", "oracle");
  ADD_DYNAMIC_ITEM2(mssql, "mssql", "mssql");
  ADD_DYNAMIC_ITEM2(mongodb, "mongodb", "mongodb");
  ADD_DYNAMIC_ITEM2(csv, "csv", "csv");
  ADD_DYNAMIC_ITEM2(sparkplugb, "sparkplugb", "sparkplugb");
  ADD_DYNAMIC_ITEM2(orc, "orc", "orc");
  ADD_DYNAMIC_ITEM2(kinghist, "kinghist", "kinghist");
  ADD_DYNAMIC_ITEM2(pulsar, "pulsar", "pulsar");

#undef ADD_DYNAMIC_ITEM_SIMPLE
#undef ADD_DYNAMIC_ITEM_FROM_QUOTA
#undef ADD_DYNAMIC_ITEM2

// IDMP expireDays
#define SET_IDMP_EXPIRE_DAYS(srcField, dstIndex)                                                                   \
  do {                                                                                                             \
    if (pAuthQuota->srcField != QUOTA_UNDEFINED) {                                                                 \
      if (pAuthQuota->srcField == GRANT_UNIQ_UNLIMITED) {                                                          \
        pGrantObj->idmpExpireDays[dstIndex] = grantDays;                                                           \
      } else {                                                                                                     \
        pGrantObj->idmpExpireDays[dstIndex] = pAuthQuota->srcField > grantDays ? grantDays : pAuthQuota->srcField; \
      }                                                                                                            \
      pGrantObj->flags |= GRANT_ACTIVE_FLG_IDMP_ASSIGNED;                                                          \
    }                                                                                                              \
  } while (0)

// IDMP limit
#define SET_IDMP_LIMIT_FIELD(srcField, dstField)          \
  do {                                                    \
    if (pAuthQuota->srcField != QUOTA_UNDEFINED) {        \
      pGrantObj->dstField = pAuthQuota->srcField;         \
      pGrantObj->flags |= GRANT_ACTIVE_FLG_IDMP_ASSIGNED; \
    }                                                     \
  } while (0)

  SET_IDMP_EXPIRE_DAYS(idmpExpireDays, GRANT_OPT_IDMP_BASIC);
  SET_IDMP_LIMIT_FIELD(idmpLimitTsAttributes, idmpLimitTsAttributes);
  SET_IDMP_LIMIT_FIELD(idmpLimitNonTsAttributes, idmpLimitNonTsAttributes);
  SET_IDMP_LIMIT_FIELD(idmpLimitElements, idmpLimitElements);
  SET_IDMP_LIMIT_FIELD(idmpLimitServers, idmpLimitServers);
  SET_IDMP_LIMIT_FIELD(idmpLimitCpuCores, idmpLimitCpuCores);
  SET_IDMP_LIMIT_FIELD(idmpLimitUsers, idmpLimitUsers);
  SET_IDMP_EXPIRE_DAYS(idmpVersionCtrl, GRANT_OPT_IDMP_VERSION_CTRL);
  SET_IDMP_EXPIRE_DAYS(idmpDataForecast, GRANT_OPT_IDMP_DATA_FORECAST);
  SET_IDMP_EXPIRE_DAYS(idmpDataDetect, GRANT_OPT_IDMP_DATA_DETECT);
  SET_IDMP_EXPIRE_DAYS(idmpDataQuality, GRANT_OPT_IDMP_DATA_QUALITY);
  SET_IDMP_EXPIRE_DAYS(idmpAiChatGen, GRANT_OPT_IDMP_AI_CHAT_GEN);

#undef SET_IDMP_EXPIRE_DAYS
#undef SET_IDMP_LIMIT_FIELD

  // extension
  if (pAuthQuota->extensionArray != NULL) {
    for (int i = 0; i < taosArrayGetSize(pAuthQuota->extensionArray); i++) {
      SAuthQuotaExItem *exitem = (SAuthQuotaExItem *)taosArrayGet(pAuthQuota->extensionArray, i);
      code = addDynamicGrantItemEx(pGrantObj, exitem->name, exitem->item.expireDate, exitem->item.limitQuantity);
      if (code != TSDB_CODE_SUCCESS) {
        uError("failed to add dynamic grant item '%s', errMsg:%s", exitem->name, tstrerror(code));
        return code;
      }
    }
    taosArrayDestroy(pAuthQuota->extensionArray);
  }
  if (pAuthQuota->idmpExtensionArray != NULL) {
    for (int i = 0; i < taosArrayGetSize(pAuthQuota->idmpExtensionArray); i++) {
      SAuthQuotaExItem *exitem = (SAuthQuotaExItem *)taosArrayGet(pAuthQuota->idmpExtensionArray, i);
      code = addDynamicGrantItemEx(pGrantObj, exitem->name, exitem->item.expireDate, exitem->item.limitQuantity);
      if (code != TSDB_CODE_SUCCESS) {
        uError("failed to add dynamic grant item '%s', errMsg:%s", exitem->name, tstrerror(code));
        return code;
      }
    }
    taosArrayDestroy(pAuthQuota->idmpExtensionArray);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t parseAuthRequest(SRpcMsg *pReq, char *clusterId, int clusterIdLen, SAuthReqData *pAuthReqData) {
  if (!pReq->pCont || pReq->contLen <= 0) {
    uError("invalid auth request, empty content");
    return TSDB_CODE_INVALID_MSG;
  }

  char   *pDecrypted = NULL;
  int32_t decryptedLen = 0;
  int32_t code = decryptAuthMessage((char *)pReq->pCont, pReq->contLen - 1, &pDecrypted, &decryptedLen);
  if (code != TSDB_CODE_SUCCESS || !pDecrypted) {
    uError("failed to decrypt auth request, code:%d", code);
    return code;
  }

  uInfo("receive auth request, encrypted length: %d, decrypted length: %d", pReq->contLen, decryptedLen);

  SJson *pReqJson = tjsonParse(pDecrypted);
  if (!pReqJson) {
    uError("failed to parse auth request JSON after decryption");
    taosMemoryFree(pDecrypted);
    return TSDB_CODE_INVALID_JSON_FORMAT;
  }
  taosMemoryFree(pDecrypted);

  // clusterId
  code = tjsonGetStringValue(pReqJson, "clusterId", clusterId);
  if (code != 0 || clusterId[0] == 0) {
    tjsonDelete(pReqJson);
    uError("clusterId not found in auth request");
    return TSDB_CODE_INVALID_MSG;
  }

  if (pAuthReqData) {
    memset(pAuthReqData, 0, sizeof(SAuthReqData));

    tjsonGetBigIntValue(pReqJson, "ts", &pAuthReqData->ts);
    tjsonGetStringValue(pReqJson, "auth_time", pAuthReqData->auth_time);
    tjsonGetStringValue(pReqJson, "auth_status", pAuthReqData->auth_status);
    tjsonGetStringValue(pReqJson, "auth_usage", pAuthReqData->auth_usage);
    tjsonGetIntValue(pReqJson, "auth_code", &pAuthReqData->auth_code);

    int32_t authUpdatedInt = 0;
    tjsonGetIntValue(pReqJson, "auth_updated", &authUpdatedInt);
    pAuthReqData->auth_updated = (authUpdatedInt != 0);

    tjsonGetStringValue(pReqJson, "machine_code", pAuthReqData->machine_code);
    tjsonGetStringValue(pReqJson, "fqdn", pAuthReqData->fqdn);
    tjsonGetStringValue(pReqJson, "first_ep", pAuthReqData->first_ep);
    tjsonGetBigIntValue(pReqJson, "create_time", &pAuthReqData->create_time);
    tjsonGetBigIntValue(pReqJson, "boot_time", &pAuthReqData->boot_time);
    tjsonGetIntValue(pReqJson, "authReqInterval", &pAuthReqData->authReqInterval);
    tjsonGetIntValue(pReqJson, "expireDays", &pAuthReqData->expireDays);

    uDebug("success parse authRequest, clusterId:%s, auth_status:%s, auth_updated:%d, machine_code:%s, first_ep:%s",
           clusterId, pAuthReqData->auth_status, pAuthReqData->auth_updated,
           pAuthReqData->machine_code[0] ? pAuthReqData->machine_code : "empty",
           pAuthReqData->first_ep[0] ? pAuthReqData->first_ep : "empty");
  }

  tjsonDelete(pReqJson);
  return TSDB_CODE_SUCCESS;
}

// static bool checkAndExtendGrant(SAuthReqData *pAuthReqData, SGrantUniqObj *pGrantObj) {
//   if (!pAuthReqData || !pGrantObj) {
//     return false;
//   }

//   int32_t nowDays = (int32_t)(taosGetTimestampMs() / 86400000);
//   int32_t clientExpireDays = pAuthReqData->expireDays;

//   if (clientExpireDays == GRANT_UNIQ_UNLIMITED) {
//     uInfo("clientExpireDays is unlimited, no update needed");
//     return false;
//   }

//   int32_t authReqIntervalDays = (pAuthReqData->authReqInterval + 86399) / 86400;  // 向上取整
//   int32_t threshold15Days = nowDays + MAX_AUTH_DAY / 2;
//   int32_t thresholdReqInterval = nowDays + authReqIntervalDays;

//   int32_t threshold = (threshold15Days > thresholdReqInterval) ? threshold15Days : thresholdReqInterval;

//   if (clientExpireDays == GRANT_UNIQ_UNDEFINED || clientExpireDays < threshold) {
//     pAuthReqData->auth_updated = 1;
//     pGrantObj->expireDays[GRANT_OPT_BASIC] = clientExpireDays + MAX_AUTH_DAY > pGrantObj->expireDays[GRANT_OPT_BASIC]
//                                                  ? clientExpireDays + MAX_AUTH_DAY
//                                                  : pGrantObj->expireDays[GRANT_OPT_BASIC];

//     return true;
//   }
//   return false;
// }

static int32_t generateActiveCode(const char *clusterId, SGrantUniqObj *pGrantObj, char **pActiveCode) {
  int32_t code = grantUniqGenActiveCode(pGrantObj);
  if (code != TSDB_CODE_SUCCESS) {
    uError("failed to generate active code for cluster %s", clusterId);
    return code;
  }

  *pActiveCode = pGrantObj->active;
  uDebug("generated active code for cluster %s", clusterId);
  return 0;
}

// static int32_t revokeGrant(SGrantUniqObj *pGrantObj, SAuthQuota *pQuota, int32_t clientExpireDays) {
//   if (!pGrantObj || !pQuota) {
//     return TSDB_CODE_GRANT_DISABLED;
//   }

//   int64_t nowSec = taosGetTimestampSec();
//   int32_t todayDays = (int32_t)(nowSec / 86400);

//   // client already expired, no need to revoke
//   if (clientExpireDays < todayDays + 1) {
//     if (gAuthQuotaHash != NULL) {
//       SAuthQuota *pQuota =
//           (SAuthQuota *)taosHashGet(gAuthQuotaHash, pGrantObj->clusterId, strlen(pGrantObj->clusterId));
//       if (pQuota != NULL) {
//         pQuota->enable = false;
//       }
//     }
//     return TSDB_CODE_GRANT_DISABLED;
//   }
//   pGrantObj->expireDays[GRANT_OPT_BASIC] = todayDays + 1;
//   uInfo("revoke grant, default to tomorrow (today=%d, expire=%d)", todayDays,
//   pGrantObj->expireDays[GRANT_OPT_BASIC]);

//   return TSDB_CODE_SUCCESS;
// }

static int32_t buildAuthErrorResponse(int32_t errCode, const char *errMsg, char **pRspContent, int32_t *pRspLen) {
  SJson *pRspJson = tjsonCreateObject();
  if (!pRspJson) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  tjsonAddIntegerToObject(pRspJson, "code", errCode);
  tjsonAddStringToObject(pRspJson, "message", errMsg ? errMsg : "");

  *pRspContent = tjsonToString(pRspJson);
  tjsonDelete(pRspJson);

  if (!*pRspContent) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *pRspLen = strlen(*pRspContent) + 1;  // +1 for null terminator
  return 0;
}

static int32_t buildAuthResponse(const char *clusterId, const char *activeCode, char **pRspContent, int32_t *pRspLen) {
  SJson *pRspJson = tjsonCreateObject();
  if (!pRspJson) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  tjsonAddIntegerToObject(pRspJson, "code", TSDB_CODE_SUCCESS);
  tjsonAddStringToObject(pRspJson, "message", "auth success");
  tjsonAddStringToObject(pRspJson, "activeCode", activeCode ? activeCode : "");

  SJson *pCheckInfo = tjsonCreateObject();
  if (pCheckInfo) {
    char checkStr[256];
    snprintf(checkStr, sizeof(checkStr), "rsp_%s_%" PRId64, clusterId, taosGetTimestampMs());
    TSCKSUM checksum = taosCalcChecksum(0, (const uint8_t *)checkStr, strlen(checkStr));

    tjsonAddIntegerToObject(pCheckInfo, "checksum", checksum);
    tjsonAddStringToObject(pCheckInfo, "checkStr", checkStr);
    tjsonAddItemToObject(pRspJson, "checkInfo", pCheckInfo);
  }

  *pRspContent = tjsonToString(pRspJson);
  tjsonDelete(pRspJson);

  if (!*pRspContent) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  *pRspLen = strlen(*pRspContent) + 1;  // +1 for null terminator
  return 0;
}

// authServer process
static int32_t mndRetrieveAuthReq(SRpcMsg *pReq) {
  int32_t       code = 0;
  int32_t       lino = 0;
  char          clusterId[GRANT_CLUSTER_ID_LEN + 1] = {0};
  SGrantUniqObj grantObj = {0};
  SAuthReqData  authReqData = {0};
  char         *pRspCont = NULL;
  int32_t       rspLen = 0;
  const char   *errMsg = NULL;
  bool          updateTable = true;
  int32_t       nowDays = (int32_t)(taosGetTimestampMs() / 86400000);

  code = checkAuthServer();
  if (code != TSDB_CODE_SUCCESS) {
    updateTable = false;
    errMsg = "check authServer failed";
    TAOS_CHECK_EXIT(code);
  }

  code = parseAuthRequest(pReq, clusterId, sizeof(clusterId), &authReqData);
  if (code != TSDB_CODE_SUCCESS) {
    // parse failed , don't update table
    updateTable = false;
    errMsg = "failed to parse auth request";
    TAOS_CHECK_EXIT(code);
  }

  // get from hash table
  if (gAuthQuotaHash == NULL) {
    updateTable = false;
    errMsg = "auth quota hash is not initialized";
    TAOS_CHECK_EXIT(TSDB_CODE_INTERNAL_ERROR);
  }

  SAuthQuota *pQuota = (SAuthQuota *)taosHashGet(gAuthQuotaHash, clusterId, sizeof(clusterId));
  if (pQuota == NULL) {
    updateTable = false;
    errMsg = "auth quota is not found";
    TAOS_CHECK_EXIT(TSDB_CODE_GRANT_DISABLED);
  }

  // check auth request timestamp
  if (authReqData.ts < taosGetTimestampMs() - 60000) {  // 1 minute
    errMsg = "auth request too old";
    TAOS_CHECK_EXIT(TSDB_CODE_GRANT_RESTFUL_TIMEOUT);
  }

  if (!authReqData.auth_updated) {
    authReqData.auth_updated = checkIsUpdated(pQuota, &authReqData);
  }

  char *activeCode = NULL;
  grantObjInit(&grantObj, 1);
  tstrncpy(grantObj.clusterId, clusterId, sizeof(grantObj.clusterId));
  grantObj.distribute = taosGetTimestampMs() / 1000;

  if (!pQuota->enable) {
    // revoke grant if client expired
    // if (authReqData.expireDays > nowDays + 1) {
    //   grantObj.expireDays[GRANT_OPT_BASIC] = nowDays + 1;
    //   code = generateActiveCode(clusterId, &grantObj, &activeCode);
    //   if (code != 0) {
    //     errMsg = "failed to generate active code";
    //     TAOS_CHECK_EXIT(code);
    //   }
    // } else {
    // already expired, no need to update
    authReqData.auth_updated = false;
    errMsg = "cluster enables set to false";
    TAOS_CHECK_EXIT(TSDB_CODE_GRANT_DISABLED);
    // }
  }

  if (authReqData.auth_updated) {
    // check auth clients limit
    if (!checkAuthClientsLimit(pQuota)) {
      errMsg = "auth clients limit exceeded";
      TAOS_CHECK_EXIT(TSDB_CODE_GRANT_QUOTA_OUT_OF_RANGE);
    }

    // check expiredDays
    code = checkAuthQuotaExpireLimits(clusterId, pQuota);
    if (code != TSDB_CODE_SUCCESS) {
      errMsg = "failed to check auth quota expireDays limits";
      TAOS_CHECK_EXIT(code);
    }
    // check num and speed limits
    code = checkAuthQuotaNumLimits(clusterId, sizeof(clusterId), pQuota);
    if (code != TSDB_CODE_SUCCESS) {
      errMsg = "failed to check auth quota num and speed limits";
      TAOS_CHECK_EXIT(code);
    }

    code = convertAuthQuotaToGrantUniqObj(pQuota, &grantObj);
    if (code != TSDB_CODE_SUCCESS) {
      errMsg = "failed to convert authQuota to grantObj";
      TAOS_CHECK_EXIT(code);
    }

    code = generateActiveCode(clusterId, &grantObj, &activeCode);
    if (code != 0) {
      errMsg = "failed to generate active code";
      TAOS_CHECK_EXIT(code);
    }
  }

  code = buildAuthResponse(clusterId, activeCode, &pRspCont, &rspLen);
  if (code != TSDB_CODE_SUCCESS) {
    errMsg = "failed to build auth response";
    TAOS_CHECK_EXIT(code);
  }

  char   *pEncrypted = NULL;
  int32_t encryptedLen = 0;
  code = encryptAuthMessage(pRspCont, rspLen - 1, &pEncrypted, &encryptedLen);
  if (code != TSDB_CODE_SUCCESS || !pEncrypted) {
    errMsg = "failed to encrypt auth response";
    TAOS_CHECK_EXIT(code);
  }

  uInfo("auth response built, plain length: %d, encrypted length: %d", rspLen, encryptedLen);

  pReq->info.rsp = rpcMallocCont(encryptedLen + 1);
  if (!pReq->info.rsp) {
    taosMemoryFree(pEncrypted);
    errMsg = "out of memory";
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  memcpy(pReq->info.rsp, pEncrypted, encryptedLen);
  ((char *)pReq->info.rsp)[encryptedLen] = '\0';
  pReq->info.rspLen = encryptedLen + 1;
  taosMemoryFree(pEncrypted);

  uInfo("auth request processed successfully, clusterId:%s", clusterId);

_exit:
  tDestroyGrantUniqObj(&grantObj);
  if (pRspCont) taosMemoryFree(pRspCont);

  if (code != TSDB_CODE_SUCCESS) {
    authReqData.auth_updated = 0;
    authReqData.auth_code = code;

    uError("failed to process auth request at line %d, clusterId:%s, code:%d, %s", lino,
           clusterId[0] ? clusterId : "unknown", code, errMsg ? errMsg : tstrerror(code));

    char   *errRspCont = NULL;
    int32_t errRspLen = 0;
    if (buildAuthErrorResponse(code, errMsg, &errRspCont, &errRspLen) == 0) {
      pReq->info.rsp = rpcMallocCont(errRspLen);
      if (pReq->info.rsp) {
        memcpy(pReq->info.rsp, errRspCont, errRspLen);
        pReq->info.rspLen = errRspLen;
      }
      taosMemoryFree(errRspCont);
    } else {
      pReq->info.rsp = NULL;
      pReq->info.rspLen = 0;
    }
  }
  if (updateTable) updateAuthServer(clusterId, &authReqData);

  pReq->info.hasEpSet = 0;
  TAOS_RETURN(code);
}

static void *authQuotaRefreshThread(void *param) {
  setThreadName("auth-quota-refresh");
  uInfo("auth quota refresh thread started, interval:%ds", AUTH_QUOTA_REFRESH_INTERVAL_SEC);

  while (!gAuthQuotaRefreshStop) {
    if (!gAuthServerMnode) {
      continue;
    }

    int32_t code = queryAuthServerAll();
    if (code == TSDB_CODE_SUCCESS) {
      uDebug("auth quota batch refresh completed successfully");
    } else if (code == TSDB_CODE_GRANT_RESTFUL_TIMEOUT) {
      uWarn("auth quota batch refresh timeout, will retry next interval");
    } else {
      uError("failed to batch refresh auth quota, code:%d(%s)", code, tstrerror(code));
    }
    taosSsleep(AUTH_QUOTA_REFRESH_INTERVAL_SEC);
  }

  uInfo("auth quota refresh thread stopped");
  return NULL;
}

int32_t initAuthServer(SMnode *pMnode) {
  int32_t code = 0;
  mndSetMsgHandle(pMnode, TDMT_MND_AUTH_CHECK, mndRetrieveAuthReq);

  if (gAuthQuotaHash == NULL) {
    gAuthQuotaHash = taosHashInit(128, MurmurHash3_32, true, HASH_ENTRY_LOCK);
    if (gAuthQuotaHash == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  gAuthServerMnode = pMnode;

  gAuthQuotaRefreshStop = false;
  TdThreadAttr attr;
  taosThreadAttrInit(&attr);
  code = taosThreadCreate(&gAuthQuotaRefreshThread, &attr, authQuotaRefreshThread, NULL);
  taosThreadAttrDestroy(&attr);

  return code;
}

void cleanupAuthServer() {
  if (gAuthQuotaRefreshThread != 0) {
    gAuthQuotaRefreshStop = true;
    taosThreadJoin(gAuthQuotaRefreshThread, NULL);
    uInfo("auth quota refresh thread stopped");
  }

  if (gAuthQuotaHash != NULL) {
    taosHashCleanup(gAuthQuotaHash);
    gAuthQuotaHash = NULL;
    uInfo("auth quota hash cleaned up");
  }

  gAuthServerMnode = NULL;
}