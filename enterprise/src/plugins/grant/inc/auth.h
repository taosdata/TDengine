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

#ifndef TD_AUTH_H
#define TD_AUTH_H

#include <stdint.h>
#include "machine.h"
#include "mndDef.h"
#include "mnode.h"
#include "taoserror.h"
#include "tjson.h"
#include "tmsgcb.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_AUTH_DAY 30
typedef struct {
  int64_t ts;
  char    auth_time[25];
  char    auth_status[13];
  int32_t auth_code;
  char    auth_usage[4097];
  bool    auth_updated;
  char    machine_code[7553];
  char    fqdn[19201];
  char    first_ep[257];
  int64_t create_time;
  int64_t boot_time;
  int32_t authReqInterval;
  int32_t expireDays;
} SAuthReqData;

#ifdef AUTH_SERVER
#define QUOTA_UNDEFINED -100

typedef struct SAuthQuotaItem {
  int32_t expireDate;
  int32_t limitQuantity;
  int32_t limitSpeed;
} SAuthQuotaItem;

typedef struct SAuthQuotaExItem {
  char           name[GRANT_ITEM_NAME_LEN];
  SAuthQuotaItem item;
} SAuthQuotaExItem;

typedef struct SAuthQuota {
  bool    enable;      // enabled:true
  int32_t service;     // service:2025-12-31
  int32_t expireDays;  // expireDays:2025-12-31

  int64_t limitTimeSeries;   // limitTimeSeries:-1
  int32_t limitCpuCores;     // limitCpuCores:-1
  int16_t limitDnodes;       // limitDnodes:-1
  int32_t limitVnodes;       // limitVnodes:-1
  int64_t limitStorageSize;  // limitStorageSize:-1

  SAuthQuotaItem stream;         // stream:2025-12-31,-1
  SAuthQuotaItem subscription;   // subscription:2025-12-31,-1
  SAuthQuotaItem view;           // view:2025-12-31,-1
  int32_t        audit;          // audit:2025-12-31
  int32_t        storage;        // storage:2025-12-31
  int32_t        dataSync;       // dataSync:2025-12-31
  int32_t        backupRestore;  // backupRestore:2025-12-31
  int32_t        sharedStorage;  // sharedStorage:2025-12-31
  int32_t        activeActive;   // ActiveActive:2025-12-31
  int32_t        dualReplica;    // DualReplica:2025-12-31
  int32_t        dbEncrypt;      // dbEncrypt:2025-12-31

  SAuthQuotaItem tdgpt;  // tdgpt:2025-12-31,-1
  SAuthQuotaItem mount;  // mount:2025-12-31,-1

  SAuthQuotaItem opc_da;          // opc_da:2025-12-31,100,1000
  SAuthQuotaItem opc_ua;          // opc_ua:2025-12-31,100,1000
  SAuthQuotaItem pi;              // pi:2025-12-31,100,1000
  SAuthQuotaItem kafka;           // kafka:2025-12-31,100,1000
  SAuthQuotaItem influxdb;        // influxdb:2025-12-31,100,1000
  SAuthQuotaItem mqtt;            // mqtt:2025-12-31,100,1000
  SAuthQuotaItem avevahistorian;  // avevahistorian:2025-12-31,100,1000
  SAuthQuotaItem opentsdb;        // opentsdb:2025-12-31,100,1000
  SAuthQuotaItem td2_6;           // td2.6:2025-12-31,100,1000
  SAuthQuotaItem td3_0;           // td3.0:2025-12-31,100,1000
  SAuthQuotaItem mysql;           // mysql:2025-12-31,100,1000
  SAuthQuotaItem postgres;        // postgres:2025-12-31,100,1000
  SAuthQuotaItem oracle;          // oracle:2025-12-31,100,1000
  SAuthQuotaItem mssql;           // mssql:2025-12-31,100,1000
  SAuthQuotaItem mongodb;         // mongodb:2025-12-31,100,1000
  SAuthQuotaItem csv;             // csv:2025-12-31,100,1000
  SAuthQuotaItem sparkplugb;      // sparkplugb:2025-12-31,100,1000
  SAuthQuotaItem orc;             // orc:2025-12-31,100,1000
  SAuthQuotaItem kinghist;        // kinghist:2025-12-31,100,1000
  SAuthQuotaItem pulsar;          // pulsar:2025-12-31,100,1000
  SArray        *extensionArray;  // extension:2025-12-31,100,1000

  // IDMP
  int32_t idmpExpireDays;            // idmpExpireDays:2025-12-31
  int64_t idmpLimitTsAttributes;     // idmpLimitTsAttributes:-1
  int64_t idmpLimitNonTsAttributes;  // idmpLimitNonTsAttributes:-1
  int32_t idmpLimitElements;         // idmpLimitElements:-1
  int32_t idmpLimitServers;          // idmpLimitServers:-1
  int32_t idmpLimitCpuCores;         // idmpLimitCpuCores:-1
  int32_t idmpLimitUsers;            // idmpLimitUsers:-1
  int32_t idmpVersionCtrl;           // idmpVersionCtrl:2025-12-31
  int32_t idmpDataForecast;          // idmpDataForecast:2025-12-31
  int32_t idmpDataDetect;            // idmpDataDetect:2025-12-31
  int32_t idmpDataQuality;           // idmpDataQuality:2025-12-31
  int32_t idmpAiChatGen;             // idmpAiChatGen:2025-12-31
  SArray *idmpExtensionArray;        // idmpExtension:2025-12-31,100,1000
} SAuthQuota;

void    initAuthQuota(SAuthQuota *pAuthQuota);
int32_t parseAuthQuota(const char *authQuotaStr, SAuthQuota *pAuthQuota);

extern const char *gGrantState[GRANT_STATE_MAX];

int32_t initAuthServer(SMnode *pMnode);
void    cleanupAuthServer();
#endif

int32_t mndAuthReqDataToJson(SAuthReqData *pData, SJson *pJson);

int32_t initAuthClient(SMnode *pMnode);
void    cleanupAuthClient();

int32_t encryptAuthMessage(const char *pPlainText, int32_t plainLen, char **ppCipherText, int32_t *pCipherLen);
int32_t decryptAuthMessage(const char *pCipherText, int32_t cipherLen, char **ppPlainText, int32_t *pPlainLen);

#ifdef __cplusplus
}
#endif

#endif  // TD_AUTH_H
