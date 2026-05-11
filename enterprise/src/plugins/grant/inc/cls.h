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

#ifndef TD_CLS_H
#define TD_CLS_H

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

#define MAX_CLS_EXPIRE_DAYS 15
typedef struct {
  char id[33];
  char machine_code[TSDB_MACHINE_ID_LEN + 1];
  char ep[TSDB_EP_LEN];
} SClsReqInstance;

typedef struct {
  char    key[TSDB_FQDN_LEN + 1];
  int64_t value;
} SClsGrant;

typedef struct {
  int64_t ts;
  char    auth_time[25];
  char    auth_status[13];
  int32_t auth_code;
  SClsGrant *pGrantUsage;
  bool    auth_updated;
  SClsReqInstance *pInstance;
  char    first_ep[257];
  int64_t create_time;
  int64_t boot_time;
  int32_t authReqInterval;
  int32_t expireDays;
} SClsReqData;

typedef struct {
  int32_t  id;
  int32_t  clsRespLen;
  char*    clsResp;
  bool     isValid;
  int32_t  extendLen;
  char*    extend;
  int64_t  updateTime;
  SRWLatch lock;
} SGrantClsObj;

int32_t mndClsReqDataToJson(SClsReqData *pData, SJson *pJson);

int32_t initClsClient(SMnode *pMnode);
void    cleanupClsClient();

#ifdef GRANT_TEST_HELPER
int32_t clsTestParseExpireToDays(const char *expire, int32_t capDays, int32_t *pExpireDays);
int32_t clsTestVerifyPayloadSignature(const uint8_t *pPayload, int32_t payloadLen, const char *signatureBase64);
int32_t clsTestConvertClsGrantsToGrantUniqObj(const char *validUntil, SJson *pGrantsJson, SGrantUniqObj *pGrantObj);
int32_t clsTestBuildGracePeriodValidUntil(char *buf, int32_t bufLen);
void    clsTestCleanupGrantObj(SGrantUniqObj *pGrantObj);
#endif

int32_t mndProcessClsRspGrant(SMnode *pMnode, char *pCont, int32_t contLen, bool useGracePeriod);


#ifdef __cplusplus
}
#endif

#endif  // TD_CLS_H
