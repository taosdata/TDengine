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

#include "dbRest.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include "cJSON.h"
#include "grant.h"
#include "os.h"
#include "taoserror.h"
#include "tbase64.h"
#include "tjson.h"
#include "tlog.h"
#include "ttime.h"

#define REST_TIMEOUT_SEC 5
#define REST_HOST "127.0.0.1"
#define REST_PORT 6041
#define REST_USER "authroot"
#define REST_PASS "auth125!"

extern SHashObj *gAuthQuotaHash;

typedef struct SUpdateAuthTask {
  char         clusterId[GRANT_CLUSTER_ID_LEN + 1];
  SAuthReqData authReqData;
} SUpdateAuthTask;

static int32_t httpPost(const char *host, uint16_t port, const char *req, int reqLen, char **pResp, ssize_t *pRespLen) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    uError("auth http post failed to create socket, error:%s", strerror(errno));
    return TSDB_CODE_GRANT_RESTFUL_ERROR;
  }

  struct timeval tv = {REST_TIMEOUT_SEC, 0};
  if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
    uError("auth http post failed to set SO_RCVTIMEO, error:%s", strerror(errno));
    close(fd);
    return TSDB_CODE_GRANT_RESTFUL_ERROR;
  }
  if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
    uError("auth http post failed to set SO_SNDTIMEO, error:%s", strerror(errno));
    close(fd);
    return TSDB_CODE_GRANT_RESTFUL_ERROR;
  }

  struct sockaddr_in addr = {0};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
    uError("auth http post failed to convert host %s to address, error:%s", host, strerror(errno));
    close(fd);
    return TSDB_CODE_GRANT_RESTFUL_ERROR;
  }

  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    int err = errno;
    close(fd);
    if (err == ETIMEDOUT || err == EAGAIN || err == EINPROGRESS) {
      uError("auth http post connection timeout to %s:%u (timeout:%ds)", host, port, REST_TIMEOUT_SEC);
      return TSDB_CODE_GRANT_RESTFUL_TIMEOUT;
    } else {
      uError("auth http post failed to connect to %s:%u, error:%s", host, port, strerror(err));
      return TSDB_CODE_GRANT_RESTFUL_ERROR;
    }
  }

  ssize_t sent = 0;
  while (sent < reqLen) {
    ssize_t n = send(fd, req + sent, reqLen - sent, 0);
    if (n < 0) {
      int err = errno;
      close(fd);
      if (err == ETIMEDOUT || err == EAGAIN) {
        uError("auth http post send timeout to %s:%u (timeout:%ds)", host, port, REST_TIMEOUT_SEC);
        return TSDB_CODE_GRANT_RESTFUL_TIMEOUT;
      } else {
        uError("auth http post failed to send to %s:%u, error:%s", host, port, strerror(err));
        return TSDB_CODE_GRANT_RESTFUL_ERROR;
      }
    }
    sent += n;
  }

  char *buf = taosMemoryMalloc(1024 * 1024);
  if (!buf) {
    close(fd);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  ssize_t total = 0, n;
  while (total < 1024 * 1024 - 1) {
    n = recv(fd, buf + total, 1024 * 1024 - total - 1, 0);
    if (n > 0) {
      total += n;
    } else if (n == 0) {
      break;
    } else {
      int err = errno;
      close(fd);
      taosMemoryFree(buf);
      if (err == ETIMEDOUT || err == EAGAIN || err == EWOULDBLOCK) {
        uError("auth http post receive timeout from %s:%u (timeout:%ds)", host, port, REST_TIMEOUT_SEC);
        return TSDB_CODE_GRANT_RESTFUL_TIMEOUT;
      } else {
        uError("auth http post failed to receive from %s:%u, error:%s", host, port, strerror(err));
        return TSDB_CODE_GRANT_RESTFUL_ERROR;
      }
    }
  }
  close(fd);

  if (total <= 0) {
    taosMemoryFree(buf);
    uError("auth http post failed to receive response from %s:%u (received:%zd bytes)", host, port, total);
    return TSDB_CODE_GRANT_RESTFUL_ERROR;
  }

  buf[total] = '\0';
  *pResp = buf;
  *pRespLen = total;
  return 0;
}

static char *trimSpace(char *str) {
  if (!str) return str;

  while (*str && (*str == ' ' || *str == '\t')) str++;

  char *end = str + strlen(str) - 1;
  while (end > str && (*end == ' ' || *end == '\t')) {
    *end = '\0';
    end--;
  }

  return str;
}

static int32_t grantParseExpireDay(const char *expireStr, const char *key, int32_t *pExpireDays) {
  if (expireStr == NULL || strlen(expireStr) == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (0 == strncmp(expireStr, "un", 2)) {
    *pExpireDays = GRANT_UNIQ_UNLIMITED;
    return TSDB_CODE_SUCCESS;
  }

  // check format YYYY-MM-DD
  if (strlen(expireStr) != 10 || expireStr[4] != '-' || expireStr[7] != '-') {
    uError("failed to parse param:%s, invalid expire day:'%s', should be: YYYY-MM-DD", key, expireStr);
    return TSDB_CODE_INVALID_TIMESTAMP;
  }

  struct tm tm;
  memset(&tm, 0, sizeof(struct tm));
  char *str = taosStrpTime(expireStr, "%Y-%m-%d", &tm);
  if (str == NULL) {
    uError("failed to parse param:%s, invalid expire day:'%s', should be: YYYY-MM-DD", key, expireStr);
    return TSDB_CODE_INVALID_TIMESTAMP;
  }

  int64_t expire = ceil(((double)taosMktime(&tm, NULL) + 86400) / 86400);
  if (expire < 0) {
    uError("failed to parse param:%s, invalid expire day:'%s'(%" PRIi64 "), should not be negative", key, expireStr,
           expire);
    return TSDB_CODE_GRANT_QUOTA_OUT_OF_RANGE;
  }

  if (expire > GRANT_UNIQ_MAX_EXPIRE_SECOND / 86400) {
    uError("failed to parse param:%s, expire day:'%s'(%" PRIi64 ") too large, max is %" PRIi64, key, expireStr, expire,
           (int64_t)(GRANT_UNIQ_MAX_EXPIRE_SECOND / 86400));
    return TSDB_CODE_GRANT_QUOTA_OUT_OF_RANGE;
  }

  *pExpireDays = (int32_t)expire;
  return TSDB_CODE_SUCCESS;
}

void initAuthQuota(SAuthQuota *pAuthQuota) {
  if (!pAuthQuota) return;

  memset(pAuthQuota, 0, sizeof(SAuthQuota));

  pAuthQuota->service = QUOTA_UNDEFINED;
  pAuthQuota->expireDays = QUOTA_UNDEFINED;
  pAuthQuota->limitTimeSeries = QUOTA_UNDEFINED;
  pAuthQuota->limitCpuCores = QUOTA_UNDEFINED;
  pAuthQuota->limitDnodes = QUOTA_UNDEFINED;
  pAuthQuota->limitVnodes = QUOTA_UNDEFINED;
  pAuthQuota->limitStorageSize = QUOTA_UNDEFINED;

  pAuthQuota->stream.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->stream.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->stream.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->subscription.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->subscription.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->subscription.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->view.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->view.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->view.limitSpeed = QUOTA_UNDEFINED;

  // 纯日期字段（int32_t 类型）
  pAuthQuota->audit = QUOTA_UNDEFINED;
  pAuthQuota->storage = QUOTA_UNDEFINED;
  pAuthQuota->dataSync = QUOTA_UNDEFINED;
  pAuthQuota->backupRestore = QUOTA_UNDEFINED;
  pAuthQuota->sharedStorage = QUOTA_UNDEFINED;
  pAuthQuota->activeActive = QUOTA_UNDEFINED;
  pAuthQuota->dualReplica = QUOTA_UNDEFINED;
  pAuthQuota->dbEncrypt = QUOTA_UNDEFINED;

  pAuthQuota->tdgpt.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->tdgpt.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->tdgpt.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->mount.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->mount.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->mount.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->opc_da.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->opc_da.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->opc_da.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->opc_ua.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->opc_ua.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->opc_ua.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->pi.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->pi.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->pi.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->kafka.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->kafka.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->kafka.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->influxdb.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->influxdb.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->influxdb.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->mqtt.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->mqtt.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->mqtt.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->avevahistorian.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->avevahistorian.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->avevahistorian.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->opentsdb.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->opentsdb.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->opentsdb.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->td2_6.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->td2_6.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->td2_6.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->td3_0.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->td3_0.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->td3_0.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->mysql.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->mysql.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->mysql.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->postgres.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->postgres.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->postgres.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->oracle.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->oracle.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->oracle.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->mssql.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->mssql.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->mssql.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->mongodb.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->mongodb.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->mongodb.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->csv.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->csv.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->csv.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->sparkplugb.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->sparkplugb.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->sparkplugb.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->orc.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->orc.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->orc.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->kinghist.expireDate = QUOTA_UNDEFINED;
  pAuthQuota->kinghist.limitQuantity = QUOTA_UNDEFINED;
  pAuthQuota->kinghist.limitSpeed = QUOTA_UNDEFINED;

  pAuthQuota->idmpExpireDays = QUOTA_UNDEFINED;
  pAuthQuota->idmpLimitTsAttributes = QUOTA_UNDEFINED;
  pAuthQuota->idmpLimitNonTsAttributes = QUOTA_UNDEFINED;
  pAuthQuota->idmpLimitElements = QUOTA_UNDEFINED;
  pAuthQuota->idmpLimitServers = QUOTA_UNDEFINED;
  pAuthQuota->idmpLimitCpuCores = QUOTA_UNDEFINED;
  pAuthQuota->idmpLimitUsers = QUOTA_UNDEFINED;
  pAuthQuota->idmpVersionCtrl = QUOTA_UNDEFINED;
  pAuthQuota->idmpDataForecast = QUOTA_UNDEFINED;
  pAuthQuota->idmpDataDetect = QUOTA_UNDEFINED;
  pAuthQuota->idmpDataQuality = QUOTA_UNDEFINED;
  pAuthQuota->idmpAiChatGen = QUOTA_UNDEFINED;
}

static int32_t parseQuotaItem(const char *key, const char *value, SAuthQuotaItem *pItem) {
  if (!value || !pItem) return TSDB_CODE_INVALID_PARA;

  memset(pItem, 0, sizeof(SAuthQuotaItem));
  pItem->expireDate = QUOTA_UNDEFINED;
  pItem->limitQuantity = QUOTA_UNDEFINED;
  pItem->limitSpeed = QUOTA_UNDEFINED;

  char valueCopy[256];
  tstrncpy(valueCopy, value, sizeof(valueCopy));

  char *saveptr = NULL;
  char *token = strtok_r(valueCopy, ",", &saveptr);
  int   idx = 0;

  while (token && idx < 3) {
    token = trimSpace(token);
    if (idx == 0) {
      int32_t code = grantParseExpireDay(token, key, &pItem->expireDate);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    } else if (idx == 1) {
      if (strcmp(token, "un") == 0) {
        pItem->limitQuantity = GRANT_UNIQ_UNLIMITED;
      } else {
        pItem->limitQuantity = atoi(token);
      }
    } else if (idx == 2) {
      if (strcmp(token, "un") == 0) {
        pItem->limitSpeed = GRANT_UNIQ_UNLIMITED;
      } else {
        pItem->limitSpeed = atoi(token);
      }
    }

    token = strtok_r(NULL, ",", &saveptr);
    idx++;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t parseAuthQuota(const char *authQuotaStr, SAuthQuota *pAuthQuota) {
  if (!authQuotaStr || !pAuthQuota) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t code = 0;
  initAuthQuota(pAuthQuota);

  char *copy = taosMemoryMalloc(strlen(authQuotaStr) + 1);
  if (!copy) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  strcpy(copy, authQuotaStr);

  char *saveptr1 = NULL;
  char *pair = strtok_r(copy, ";", &saveptr1);

  while (pair) {
    char *colon = strchr(pair, ':');
    if (!colon) {
      pair = strtok_r(NULL, ";", &saveptr1);
      continue;
    }

    *colon = '\0';
    char *key = trimSpace(pair);
    char *value = trimSpace(colon + 1);

    // basic
    if (strcmp(key, "service") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->service);
    } else if (strcmp(key, "expireDays") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->expireDays);
    } else if (strcmp(key, "limitTimeSeries") == 0) {
      if (strcmp(value, "un") == 0) {
        pAuthQuota->limitTimeSeries = GRANT_UNIQ_UNLIMITED;
      } else {
        pAuthQuota->limitTimeSeries = atoll(value);
      }
    } else if (strcmp(key, "limitCpuCores") == 0) {
      if (strcmp(value, "un") == 0) {
        pAuthQuota->limitCpuCores = GRANT_UNIQ_UNLIMITED;
      } else {
        pAuthQuota->limitCpuCores = atoi(value);
      }
    } else if (strcmp(key, "limitDnodes") == 0) {
      if (strcmp(value, "un") == 0) {
        pAuthQuota->limitDnodes = GRANT_UNIQ_UNLIMITED;
      } else {
        pAuthQuota->limitDnodes = (int16_t)atoi(value);
      }
    } else if (strcmp(key, "limitVnodes") == 0) {
      if (strcmp(value, "un") == 0) {
        pAuthQuota->limitVnodes = GRANT_UNIQ_UNLIMITED;
      } else {
        pAuthQuota->limitVnodes = atoi(value);
      }
    } else if (strcmp(key, "limitStorageSize") == 0) {
      if (strcmp(value, "un") == 0) {
        pAuthQuota->limitStorageSize = GRANT_UNIQ_UNLIMITED;
      } else {
        pAuthQuota->limitStorageSize = atoll(value);
      }
    }
    // grants full
    else if (strcmp(key, "stream") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->stream);
    } else if (strcmp(key, "subscription") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->subscription);
    } else if (strcmp(key, "view") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->view);
    } else if (strcmp(key, "audit") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->audit);
    } else if (strcmp(key, "storage") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->storage);
    } else if (strcmp(key, "dataSync") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->dataSync);
    } else if (strcmp(key, "backupRestore") == 0 || strcmp(key, "ackupRestore") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->backupRestore);
    } else if (strcmp(key, "sharedStorage") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->sharedStorage);
    } else if (strcmp(key, "ActiveActive") == 0 || strcmp(key, "activeActive") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->activeActive);
    } else if (strcmp(key, "DualReplica") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->dualReplica);
    } else if (strcmp(key, "dbEncrypt") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->dbEncrypt);
    } else if (strcmp(key, "tdgpt") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->tdgpt);
    } else if (strcmp(key, "mount") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->mount);
    }
    // 数据源连接器
    else if (strcmp(key, "opc_da") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->opc_da);
    } else if (strcmp(key, "opc_ua") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->opc_ua);
    } else if (strcmp(key, "pi") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->pi);
    } else if (strcmp(key, "kafka") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->kafka);
    } else if (strcmp(key, "influxdb") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->influxdb);
    } else if (strcmp(key, "mqtt") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->mqtt);
    } else if (strcmp(key, "avevahistorian") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->avevahistorian);
    } else if (strcmp(key, "opentsdb") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->opentsdb);
    } else if (strcmp(key, "td2.6") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->td2_6);
    } else if (strcmp(key, "td3.0") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->td3_0);
    } else if (strcmp(key, "mysql") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->mysql);
    } else if (strcmp(key, "postgres") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->postgres);
    } else if (strcmp(key, "oracle") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->oracle);
    } else if (strcmp(key, "mssql") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->mssql);
    } else if (strcmp(key, "mongodb") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->mongodb);
    } else if (strcmp(key, "csv") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->csv);
    } else if (strcmp(key, "sparkplugb") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->sparkplugb);
    } else if (strcmp(key, "orc") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->orc);
    } else if (strcmp(key, "kinghist") == 0) {
      code = parseQuotaItem(key, value, &pAuthQuota->kinghist);
    }
    // IDMP
    else if (strcmp(key, "idmpExpireDays") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->idmpExpireDays);
    } else if (strcmp(key, "idmpLimitTsAttributes") == 0) {
      if (strcmp(value, "un") == 0) {
        pAuthQuota->idmpLimitTsAttributes = GRANT_UNIQ_UNLIMITED;
      } else {
        pAuthQuota->idmpLimitTsAttributes = atoll(value);
      }
    } else if (strcmp(key, "idmpLimitNonTsAttributes") == 0) {
      if (strcmp(value, "un") == 0) {
        pAuthQuota->idmpLimitNonTsAttributes = GRANT_UNIQ_UNLIMITED;
      } else {
        pAuthQuota->idmpLimitNonTsAttributes = atoll(value);
      }
    } else if (strcmp(key, "idmpLimitElements") == 0) {
      if (strcmp(value, "un") == 0) {
        pAuthQuota->idmpLimitElements = GRANT_UNIQ_UNLIMITED;
      } else {
        pAuthQuota->idmpLimitElements = atoi(value);
      }
    } else if (strcmp(key, "idmpLimitServers") == 0) {
      if (strcmp(value, "un") == 0) {
        pAuthQuota->idmpLimitServers = GRANT_UNIQ_UNLIMITED;
      } else {
        pAuthQuota->idmpLimitServers = atoi(value);
      }
    } else if (strcmp(key, "idmpLimitCpuCores") == 0) {
      if (strcmp(value, "un") == 0) {
        pAuthQuota->idmpLimitCpuCores = GRANT_UNIQ_UNLIMITED;
      } else {
        pAuthQuota->idmpLimitCpuCores = atoi(value);
      }
    } else if (strcmp(key, "idmpLimitUsers") == 0) {
      if (strcmp(value, "un") == 0) {
        pAuthQuota->idmpLimitUsers = GRANT_UNIQ_UNLIMITED;
      } else {
        pAuthQuota->idmpLimitUsers = atoi(value);
      }
    } else if (strcmp(key, "idmpVersionCtrl") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->idmpVersionCtrl);
    } else if (strcmp(key, "idmpDataForecast") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->idmpDataForecast);
    } else if (strcmp(key, "idmpDataDetect") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->idmpDataDetect);
    } else if (strcmp(key, "idmpDataQuality") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->idmpDataQuality);
    } else if (strcmp(key, "idmpAiChatGen") == 0) {
      code = grantParseExpireDay(value, key, &pAuthQuota->idmpAiChatGen);
    }
    // for future extension
    else if (strstr(key, "idmp") != NULL) {
      SAuthQuotaExItem item;
      parseQuotaItem(key, value, &item.item);
      strncpy(item.name, key, GRANT_ITEM_NAME_LEN);
      if (pAuthQuota->idmpExtensionArray == NULL) {
        pAuthQuota->idmpExtensionArray = taosArrayInit(2, sizeof(SAuthQuotaExItem));
      }
      if (taosArrayPush(pAuthQuota->idmpExtensionArray, &item) == NULL) {
        uError("failed to add idmp extension item to array");
        taosMemoryFree(copy);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    } else {
      SAuthQuotaExItem item;
      parseQuotaItem(key, value, &item.item);
      strncpy(item.name, key, GRANT_ITEM_NAME_LEN);
      if (pAuthQuota->extensionArray == NULL) {
        pAuthQuota->extensionArray = taosArrayInit(2, sizeof(SAuthQuotaExItem));
      }
      if (taosArrayPush(pAuthQuota->extensionArray, &item) == NULL) {
        uError("failed to add extension item to array");
        taosMemoryFree(copy);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }

    pair = strtok_r(NULL, ";", &saveptr1);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(copy);
      return code;
    }
  }

  taosMemoryFree(copy);
  return TSDB_CODE_SUCCESS;
}

// query tbname
int32_t queryTbname(const char *clusterId, char **tbname) {
  int32_t code = 0;
  char   *authEnc = NULL;
  char   *resp = NULL;
  char   *jsonCopy = NULL;
  SJson  *pJson = NULL;

  if (!clusterId || !tbname || !*tbname) {
    return TSDB_CODE_INVALID_PARA;
  }

  char sql[512];
  snprintf(sql, sizeof(sql), "select tbname from auth.grantserver where cluster_id='%s' group by tbname;", clusterId);

  char auth[256];
  snprintf(auth, sizeof(auth), "%s:%s", REST_USER, REST_PASS);
  if (base64_encode((unsigned char *)auth, strlen(auth), &authEnc) != 0) {
    code = TSDB_CODE_FAILED;
    goto _exit;
  }

  char request[4096];
  int  reqLen = snprintf(request, sizeof(request),
                         "POST /rest/sql HTTP/1.1\r\n"
                          "Host: %s:%d\r\n"
                          "Authorization: Basic %s\r\n"
                          "Content-Type: text/plain\r\n"
                          "Content-Length: %zu\r\n"
                          "Connection: close\r\n\r\n%s",
                         REST_HOST, REST_PORT, authEnc, strlen(sql), sql);

  ssize_t respLen;
  code = httpPost(REST_HOST, REST_PORT, request, reqLen, &resp, &respLen);
  if (code != 0) {
    uError("send query request to taosadaptor failed, errorcode: %d, errorMsg: %s", code, tstrerror(code));
    goto _exit;
  }

  char *json = strstr(resp, "\r\n\r\n");
  if (!json) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }
  json += 4;

  char *nl = strchr(json, '\n');
  if (nl && (nl - json) < 10) json = nl + 1;

  jsonCopy = taosMemoryMalloc(strlen(json) + 1);
  if (!jsonCopy) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  strcpy(jsonCopy, json);

  char *end = strstr(jsonCopy, "\r\n0\r\n");
  if (end) *end = '\0';

  pJson = tjsonParse(jsonCopy);
  if (!pJson) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }

  SJson  *pCodeItem = tjsonGetObjectItem(pJson, "code");
  int32_t respCode = pCodeItem ? (int32_t)cJSON_GetNumberValue((cJSON *)pCodeItem) : -1;
  if (respCode != 0) {
    code = respCode;
    goto _exit;
  }

  SJson  *pRowsItem = tjsonGetObjectItem(pJson, "rows");
  int32_t rows = pRowsItem ? (int32_t)cJSON_GetNumberValue((cJSON *)pRowsItem) : 0;
  if (rows != 1) {
    uError("query tbname got %d rows, expected exactly 1", rows);
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  SJson *pData = tjsonGetObjectItem(pJson, "data");
  if (!pData || !cJSON_IsArray((cJSON *)pData) || cJSON_GetArraySize((cJSON *)pData) != 1) {
    uError("query tbname invalid data array");
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }

  SJson *pRow = tjsonGetArrayItem(pData, 0);
  if (!pRow || !cJSON_IsArray((cJSON *)pRow) || cJSON_GetArraySize((cJSON *)pRow) < 1) {
    uError("query tbname invalid row format");
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }

  SJson *pTbname = tjsonGetArrayItem(pRow, 0);
  if (!pTbname || !cJSON_IsString((cJSON *)pTbname)) {
    uError("query tbname missing tbname string");
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }

  const char *tbnameStr = cJSON_GetStringValue((cJSON *)pTbname);
  if (!tbnameStr || tbnameStr[0] == '\0') {
    uError("query tbname empty tbname string");
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }

  tstrncpy(*tbname, tbnameStr, TSDB_TABLE_NAME_LEN + 1);
  uDebug("success get tbname:%s, clusterId:%s", *tbname, clusterId);

_exit:
  if (authEnc) taosMemoryFree(authEnc);
  if (resp) taosMemoryFree(resp);
  if (jsonCopy) taosMemoryFree(jsonCopy);
  if (pJson) tjsonDelete(pJson);

  return code;
}

int32_t queryAuthServerAll() {
  int32_t   code = 0;
  char     *authEnc = NULL;
  char     *resp = NULL;
  char     *jsonCopy = NULL;
  SJson    *pJson = NULL;
  SHashObj *pQueriedClusterHash = NULL;

  char sql[512];
  snprintf(sql, sizeof(sql), "select tags cluster_id,enables,auth_quota from auth.grantserver;");

  char auth[256];
  snprintf(auth, sizeof(auth), "%s:%s", REST_USER, REST_PASS);
  if (base64_encode((unsigned char *)auth, strlen(auth), &authEnc) != 0) {
    code = TSDB_CODE_FAILED;
    goto _exit;
  }

  char request[4096];
  int  reqLen = snprintf(request, sizeof(request),
                         "POST /rest/sql HTTP/1.1\r\n"
                          "Host: %s:%d\r\n"
                          "Authorization: Basic %s\r\n"
                          "Content-Type: text/plain\r\n"
                          "Content-Length: %zu\r\n"
                          "Connection: close\r\n\r\n%s",
                         REST_HOST, REST_PORT, authEnc, strlen(sql), sql);

  ssize_t respLen;
  code = httpPost(REST_HOST, REST_PORT, request, reqLen, &resp, &respLen);
  if (code != 0) {
    uError("send query request to taosadaptor failed, errorcode: %d, errorMsg: %s", code, tstrerror(code));
    goto _exit;
  }

  char *json = strstr(resp, "\r\n\r\n");
  if (!json) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }
  json += 4;

  char *nl = strchr(json, '\n');
  if (nl && (nl - json) < 10) json = nl + 1;

  jsonCopy = taosMemoryMalloc(strlen(json) + 1);
  if (!jsonCopy) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  strcpy(jsonCopy, json);

  char *end = strstr(jsonCopy, "\r\n0\r\n");
  if (end) *end = '\0';

  pJson = tjsonParse(jsonCopy);
  if (!pJson) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }

  SJson  *pCodeItem = tjsonGetObjectItem(pJson, "code");
  int32_t respCode = pCodeItem ? (int32_t)cJSON_GetNumberValue((cJSON *)pCodeItem) : -1;
  if (respCode != 0) {
    code = respCode;
    goto _exit;
  }

  SJson  *pRowsItem = tjsonGetObjectItem(pJson, "rows");
  int32_t rows = pRowsItem ? (int32_t)cJSON_GetNumberValue((cJSON *)pRowsItem) : 0;
  if (rows <= 0) {
    uDebug("no auth quota data returned from database");
    if (gAuthQuotaHash != NULL) {
      taosHashClear(gAuthQuotaHash);
      uInfo("cleared auth quota hash due to empty database result");
    }
    code = TSDB_CODE_SUCCESS;
    goto _exit;
  }

  SJson *pDataArray = tjsonGetObjectItem(pJson, "data");
  if (!pDataArray || !cJSON_IsArray((cJSON *)pDataArray)) {
    uError("invalid data array in auth quota query response");
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }

  int arraySize = cJSON_GetArraySize((cJSON *)pDataArray);
  uDebug("auth quota query returned %d rows", arraySize);

  // query clusterId
  if (gAuthQuotaHash != NULL && arraySize > 0) {
    pQueriedClusterHash =
        taosHashInit(arraySize * 2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
    if (!pQueriedClusterHash) {
      uError("failed to create temporary hash for queried clusterIds");
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
  }

  // 第一步：收集所有查询到的 clusterId 并更新/添加到 hash
  for (int i = 0; i < arraySize; i++) {
    SJson *pRow = tjsonGetArrayItem(pDataArray, i);
    if (!pRow || !cJSON_IsArray((cJSON *)pRow)) {
      uWarn("skip invalid row %d in auth quota data", i);
      continue;
    }

    SJson *pClusterIdItem = tjsonGetArrayItem(pRow, 0);
    SJson *pEnablesItem = tjsonGetArrayItem(pRow, 1);
    SJson *pAuthQuotaStrItem = tjsonGetArrayItem(pRow, 2);

    if (!pClusterIdItem || !pEnablesItem || !pAuthQuotaStrItem) {
      uWarn("skip row %d with missing fields", i);
      continue;
    }

    char        clusterId[GRANT_CLUSTER_ID_LEN + 1] = {0};
    const char *clusterIdStr = cJSON_GetStringValue((cJSON *)pClusterIdItem);
    if (!clusterIdStr || strlen(clusterIdStr) == 0) {
      uError("skip row %d with empty cluster_id", i);
      continue;
    }
    if (strlen(clusterIdStr) > GRANT_CLUSTER_ID_LEN) {
      uError("skip row %d with cluster_id longer than %d", i, GRANT_CLUSTER_ID_LEN);
      continue;
    }
    tstrncpy(clusterId, clusterIdStr, sizeof(clusterId));

    if (pQueriedClusterHash != NULL) {
      int32_t dummy = 1;
      taosHashPut(pQueriedClusterHash, clusterId, sizeof(clusterId), &dummy, sizeof(int32_t));
    }

    bool enables = cJSON_IsTrue((cJSON *)pEnablesItem);

    const char *authQuotaStr = cJSON_GetStringValue((cJSON *)pAuthQuotaStrItem);
    if (!authQuotaStr || strlen(authQuotaStr) == 0) {
      uWarn("skip cluster %s with empty auth_quota", clusterId);
      continue;
    }

    SAuthQuota authQuota = {0};
    initAuthQuota(&authQuota);

    int32_t parseCode = parseAuthQuota(authQuotaStr, &authQuota);
    authQuota.enable = enables;

    if (parseCode != TSDB_CODE_SUCCESS) {
      uError("failed to parse auth_quota for cluster %s, code:%d", clusterId, parseCode);
      continue;
    }

    if (gAuthQuotaHash != NULL) {
      int32_t putCode = taosHashPut(gAuthQuotaHash, clusterId, sizeof(clusterId), &authQuota, sizeof(SAuthQuota));
      if (putCode != TSDB_CODE_SUCCESS && !HASH_NODE_EXIST(putCode)) {
        uError("failed to put auth quota for cluster %s to hash, code:%d", clusterId, putCode);
      } else {
        uDebug("auth quota updated for cluster:%s, enable=%d", clusterId, enables);
      }
    }
  }

  // remove stale clusterId
  if (gAuthQuotaHash != NULL && pQueriedClusterHash != NULL) {
    SArray *pClusterIdsToRemove = taosArrayInit(16, GRANT_CLUSTER_ID_LEN + 1);
    if (pClusterIdsToRemove == NULL) {
      uError("failed to allocate memory for clusterIds to remove");
    } else {
      void *pIter = taosHashIterate(gAuthQuotaHash, NULL);
      while (pIter != NULL) {
        size_t keyLen = 0;
        char  *key = (char *)taosHashGetKey(pIter, &keyLen);

        if (key && keyLen > 0 && keyLen <= GRANT_CLUSTER_ID_LEN + 1) {
          void *pFound = taosHashGet(pQueriedClusterHash, key, keyLen);
          if (pFound == NULL) {
            char clusterId[GRANT_CLUSTER_ID_LEN + 1] = {0};
            tstrncpy(clusterId, key, GRANT_CLUSTER_ID_LEN + 1);
            if (taosArrayPush(pClusterIdsToRemove, clusterId) == NULL) {
              uError("failed to add clusterId to remove array");
              break;
            }
          }
        }

        pIter = taosHashIterate(gAuthQuotaHash, pIter);
      }

      int removeCount = taosArrayGetSize(pClusterIdsToRemove);
      for (int i = 0; i < removeCount; i++) {
        char clusterId[GRANT_CLUSTER_ID_LEN + 1] = {0};
        tstrncpy(clusterId, (char *)taosArrayGet(pClusterIdsToRemove, i), sizeof(clusterId));
        int32_t removeCode = taosHashRemove(gAuthQuotaHash, clusterId, sizeof(clusterId));
        if (removeCode == TSDB_CODE_SUCCESS) {
          uDebug("removed stale auth quota for cluster:%s", clusterId);
        } else {
          uWarn("failed to remove stale auth quota for cluster:%s, code:%d", clusterId, removeCode);
        }
      }

      if (removeCount > 0) {
        uInfo("removed %d stale auth quota entries from hash", removeCount);
      }

      taosArrayDestroy(pClusterIdsToRemove);
    }
  }

  uInfo("auth quota batch load completed, total %d rows processed", arraySize);
  code = TSDB_CODE_SUCCESS;

_exit:
  if (authEnc) taosMemoryFree(authEnc);
  if (resp) taosMemoryFree(resp);
  if (jsonCopy) taosMemoryFree(jsonCopy);
  if (pJson) tjsonDelete(pJson);
  if (pQueriedClusterHash != NULL) {
    taosHashCleanup(pQueriedClusterHash);
  }

  return code;
}

// select db
int32_t queryAuthServer(const char *clusterId, SAuthQuota *authQuota, bool *pEnableIsFalse) {
  int32_t code = 0;
  char   *authEnc = NULL;
  char   *resp = NULL;
  char   *jsonCopy = NULL;
  SJson  *pJson = NULL;

  char sql[512];
  snprintf(sql, sizeof(sql), "select tags enables,auth_quota from auth.grantserver where cluster_id='%s';", clusterId);

  char auth[256];
  snprintf(auth, sizeof(auth), "%s:%s", REST_USER, REST_PASS);
  if (base64_encode((unsigned char *)auth, strlen(auth), &authEnc) != 0) {
    code = TSDB_CODE_FAILED;
    goto _exit;
  }

  char request[4096];
  int  reqLen = snprintf(request, sizeof(request),
                         "POST /rest/sql HTTP/1.1\r\n"
                          "Host: %s:%d\r\n"
                          "Authorization: Basic %s\r\n"
                          "Content-Type: text/plain\r\n"
                          "Content-Length: %zu\r\n"
                          "Connection: close\r\n\r\n%s",
                         REST_HOST, REST_PORT, authEnc, strlen(sql), sql);

  ssize_t respLen;
  code = httpPost(REST_HOST, REST_PORT, request, reqLen, &resp, &respLen);
  if (code != 0) {
    uError("send query request to taosadaptor failed, errorcode: %d, errorMsg: %s", code, tstrerror(code));
    goto _exit;
  }

  char *json = strstr(resp, "\r\n\r\n");
  if (!json) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }
  json += 4;

  char *nl = strchr(json, '\n');
  if (nl && (nl - json) < 10) json = nl + 1;

  jsonCopy = taosMemoryMalloc(strlen(json) + 1);
  if (!jsonCopy) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  strcpy(jsonCopy, json);

  char *end = strstr(jsonCopy, "\r\n0\r\n");
  if (end) *end = '\0';

  pJson = tjsonParse(jsonCopy);
  if (!pJson) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _exit;
  }

  SJson  *pCodeItem = tjsonGetObjectItem(pJson, "code");
  int32_t respCode = pCodeItem ? (int32_t)cJSON_GetNumberValue((cJSON *)pCodeItem) : -1;
  if (respCode != 0) {
    code = respCode;
    goto _exit;
  }

  SJson  *pRowsItem = tjsonGetObjectItem(pJson, "rows");
  int32_t rows = pRowsItem ? (int32_t)cJSON_GetNumberValue((cJSON *)pRowsItem) : 0;
  if (rows <= 0) {
    code = TSDB_CODE_GRANT_DISABLED;
    goto _exit;
  }

  SJson *pData = tjsonGetObjectItem(pJson, "data");
  SJson *pRow = pData ? tjsonGetArrayItem(pData, 0) : NULL;
  if (!pRow) {
    code = TSDB_CODE_GRANT_DISABLED;
    goto _exit;
  }

  // parse enables
  SJson *pEnables = tjsonGetArrayItem(pRow, 0);
  bool   enables = (pEnables && cJSON_IsBool((cJSON *)pEnables)) ? cJSON_IsTrue((cJSON *)pEnables) : true;
  if (!enables) {
    *pEnableIsFalse = true;
    code = TSDB_CODE_GRANT_DISABLED;
    goto _exit;
  } else {
    authQuota->enable = true;
  }

  // parse and check auth_quota
  SJson *pQuota = tjsonGetArrayItem(pRow, 1);
  if (pQuota && cJSON_IsString((cJSON *)pQuota)) {
    const char *quotaStr = cJSON_GetStringValue((cJSON *)pQuota);
    if (quotaStr && strlen(quotaStr) > 0) {
      code = parseAuthQuota(quotaStr, authQuota);
      if (code != 0) {
        goto _exit;
      }
    }
  }

  uDebug("success get auth message, clusterId:%s", clusterId);

_exit:
  if (authEnc) taosMemoryFree(authEnc);
  if (resp) taosMemoryFree(resp);
  if (jsonCopy) taosMemoryFree(jsonCopy);
  if (pJson) tjsonDelete(pJson);

  return code;
}

static void *updateAuthServerThread(void *param) {
  setThreadName("auth-update");
  SUpdateAuthTask *pTask = (SUpdateAuthTask *)param;
  if (!pTask) {
    uError("updateAuthServerThread: null task parameter");
    return NULL;
  }

  const char   *clusterId = pTask->clusterId;
  SAuthReqData *pAuthReqData = &pTask->authReqData;

  char *tbname = taosMemoryMalloc(TSDB_TABLE_NAME_LEN + 1);
  if (!tbname) {
    uError("failed to allocate memory for tbname in updateAuthServerThread");
    taosMemoryFree(pTask);
    return NULL;
  }

  int32_t code = queryTbname(clusterId, &tbname);
  if (code != 0) {
    uError("query tbname failed in updateAuthServerThread, errorcode: %d, errorMsg: %s", code, tstrerror(code));
    taosMemoryFree(tbname);
    taosMemoryFree(pTask);
    return NULL;
  }

  char sql[32768];
  int  sqlLen =
      snprintf(sql, sizeof(sql),
               "insert into auth.%s (ts, auth_time, auth_status, auth_usage, auth_code, auth_updated, machine_code, "
               "fqdn, first_ep, create_time, boot_time) values (%" PRId64
               ", '%s', '%s', '%s', %d, %d, '%s', '%s', '%s', %" PRId64 ", %" PRId64 ");",
               tbname, pAuthReqData->ts, pAuthReqData->auth_time, pAuthReqData->auth_status, pAuthReqData->auth_usage,
               pAuthReqData->auth_code, (int)pAuthReqData->auth_updated, pAuthReqData->machine_code, pAuthReqData->fqdn,
               pAuthReqData->first_ep, pAuthReqData->create_time, pAuthReqData->boot_time);

  if (sqlLen >= sizeof(sql)) {
    uError("SQL statement too long, truncated: %d >= %zu", sqlLen, sizeof(sql));
  }

  char auth[256], *authEnc = NULL;
  snprintf(auth, sizeof(auth), "%s:%s", REST_USER, REST_PASS);
  if (base64_encode((unsigned char *)auth, strlen(auth), &authEnc) != 0) {
    uError("failed to encode auth string in updateAuthServerThread");
    taosMemoryFree(tbname);
    taosMemoryFree(pTask);
    return NULL;
  }

  char request[40960];
  int  reqLen = snprintf(request, sizeof(request),
                         "POST /rest/sql HTTP/1.1\r\n"
                          "Host: %s:%d\r\n"
                          "Authorization: Basic %s\r\n"
                          "Content-Type: text/plain\r\n"
                          "Content-Length: %zu\r\n"
                          "Connection: close\r\n\r\n%s",
                         REST_HOST, REST_PORT, authEnc, strlen(sql), sql);
  taosMemoryFree(authEnc);

  if (reqLen >= sizeof(request)) {
    uError("HTTP request too long, truncated: %d >= %zu", reqLen, sizeof(request));
    taosMemoryFree(tbname);
    taosMemoryFree(pTask);
    return NULL;
  }

  char   *resp = NULL;
  ssize_t respLen;
  code = httpPost(REST_HOST, REST_PORT, request, reqLen, &resp, &respLen);
  if (code != 0) {
    uError("send insert request to taosadaptor failed in updateAuthServerThread, errorcode: %d, errorMsg: %s", code,
           tstrerror(code));
  } else {
    uDebug("auth server update completed successfully for cluster:%s", clusterId);
  }

  taosMemoryFree(tbname);
  taosMemoryFree(resp);
  taosMemoryFree(pTask);
  return NULL;
}

void updateAuthServer(const char *clusterId, SAuthReqData *pAuthReqData) {
  if (!clusterId || !pAuthReqData) {
    uError("invalid parameters for updateAuthServer");
    return;
  }

  SUpdateAuthTask *pTask = (SUpdateAuthTask *)taosMemoryCalloc(1, sizeof(SUpdateAuthTask));
  if (!pTask) {
    uError("failed to allocate memory for update auth task");
    return;
  }

  tstrncpy(pTask->clusterId, clusterId, sizeof(pTask->clusterId));
  memcpy(&pTask->authReqData, pAuthReqData, sizeof(SAuthReqData));

  TdThread     thread;
  TdThreadAttr attr;
  taosThreadAttrInit(&attr);
  taosThreadAttrSetDetachState(&attr, PTHREAD_CREATE_DETACHED);

  int32_t code = taosThreadCreate(&thread, &attr, updateAuthServerThread, pTask);
  taosThreadAttrDestroy(&attr);

  if (code != 0) {
    uError("failed to create update auth server thread, code:%d", code);
    taosMemoryFree(pTask);
  } else {
    uDebug("update auth server task submitted asynchronously for cluster:%s", clusterId);
  }
}