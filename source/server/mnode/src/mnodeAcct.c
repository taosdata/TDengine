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
#include "os.h"
#include "mnodeSdb.h"

static void mnodeCreateDefaultAcct() {
  int32_t code = TSDB_CODE_SUCCESS;

  SAcctObj acctObj = {0};
  tstrncpy(acctObj.acct, TSDB_DEFAULT_USER, TSDB_USER_LEN);
  acctObj.cfg = (SAcctCfg){.maxUsers = 128,
                           .maxDbs = 128,
                           .maxTimeSeries = INT32_MAX,
                           .maxStreams = 1000,
                           .maxStorage = INT64_MAX,
                           .accessState = TSDB_VN_ALL_ACCCESS};
  acctObj.acctId = 1;
  acctObj.createdTime = taosGetTimestampMs();
  acctObj.updateTime = taosGetTimestampMs();

  sdbInsertRow(MN_SDB_ACCT, &acctObj);
}

int32_t mnodeEncodeAcct(SAcctObj *pAcct, char *buf, int32_t maxLen) {
  int32_t len = 0;

  len += snprintf(buf + len, maxLen - len, "{\"type\":%d, ", MN_SDB_ACCT);
  len += snprintf(buf + len, maxLen - len, "\"acct\":\"%s\", ", pAcct->acct);
  len += snprintf(buf + len, maxLen - len, "\"acctId\":\"%d\", ", pAcct->acctId);
  len += snprintf(buf + len, maxLen - len, "\"maxUsers\":\"%d\", ", pAcct->cfg.maxUsers);
  len += snprintf(buf + len, maxLen - len, "\"maxDbs\":\"%d\", ", pAcct->cfg.maxDbs);
  len += snprintf(buf + len, maxLen - len, "\"maxTimeSeries\":\"%d\", ", pAcct->cfg.maxTimeSeries);
  len += snprintf(buf + len, maxLen - len, "\"maxStreams\":\"%d\", ", pAcct->cfg.maxStreams);
  len += snprintf(buf + len, maxLen - len, "\"maxStorage\":\"%" PRIu64 "\", ", pAcct->cfg.maxStorage);
  len += snprintf(buf + len, maxLen - len, "\"accessState\":\"%d\", ", pAcct->cfg.accessState);
  len += snprintf(buf + len, maxLen - len, "\"createdTime\":\"%" PRIu64 "\", ", pAcct->createdTime);
  len += snprintf(buf + len, maxLen - len, "\"updateTime\":\"%" PRIu64 "\"}\n", pAcct->updateTime);

  return len;
}

SAcctObj *mnodeDecodeAcct(cJSON *root) {
  int32_t   code = -1;
  SAcctObj *pAcct = calloc(1, sizeof(SAcctObj));

  cJSON *acct = cJSON_GetObjectItem(root, "acct");
  if (!acct || acct->type != cJSON_String) {
    mError("failed to parse acct since acct not found");
    goto DECODE_ACCT_OVER;
  }
  tstrncpy(pAcct->acct, acct->valuestring, TSDB_USER_LEN);

  cJSON *acctId = cJSON_GetObjectItem(root, "acctId");
  if (!acctId || acctId->type != cJSON_String) {
    mError("acct:%s, failed to parse since acctId not found", pAcct->acct);
    goto DECODE_ACCT_OVER;
  }
  pAcct->acctId = atol(acctId->valuestring);

  cJSON *maxUsers = cJSON_GetObjectItem(root, "maxUsers");
  if (!maxUsers || maxUsers->type != cJSON_String) {
    mError("acct:%s, failed to parse since maxUsers not found", pAcct->acct);
    goto DECODE_ACCT_OVER;
  }
  pAcct->cfg.maxUsers = atol(maxUsers->valuestring);

  cJSON *maxDbs = cJSON_GetObjectItem(root, "maxDbs");
  if (!maxDbs || maxDbs->type != cJSON_String) {
    mError("acct:%s, failed to parse since maxDbs not found", pAcct->acct);
    goto DECODE_ACCT_OVER;
  }
  pAcct->cfg.maxDbs = atol(maxDbs->valuestring);

  cJSON *maxTimeSeries = cJSON_GetObjectItem(root, "maxTimeSeries");
  if (!maxTimeSeries || maxTimeSeries->type != cJSON_String) {
    mError("acct:%s, failed to parse since maxTimeSeries not found", pAcct->acct);
    goto DECODE_ACCT_OVER;
  }
  pAcct->cfg.maxTimeSeries = atol(maxTimeSeries->valuestring);

  cJSON *maxStreams = cJSON_GetObjectItem(root, "maxStreams");
  if (!maxStreams || maxStreams->type != cJSON_String) {
    mError("acct:%s, failed to parse since maxStreams not found", pAcct->acct);
    goto DECODE_ACCT_OVER;
  }
  pAcct->cfg.maxStreams = atol(maxStreams->valuestring);

  cJSON *maxStorage = cJSON_GetObjectItem(root, "maxStorage");
  if (!maxStorage || maxStorage->type != cJSON_String) {
    mError("acct:%s, failed to parse since maxStorage not found", pAcct->acct);
    goto DECODE_ACCT_OVER;
  }
  pAcct->cfg.maxStorage = atoll(maxStorage->valuestring);

  cJSON *accessState = cJSON_GetObjectItem(root, "accessState");
  if (!accessState || accessState->type != cJSON_String) {
    mError("acct:%s, failed to parse since accessState not found", pAcct->acct);
    goto DECODE_ACCT_OVER;
  }
  pAcct->cfg.accessState = atol(accessState->valuestring);

  cJSON *createdTime = cJSON_GetObjectItem(root, "createdTime");
  if (!createdTime || createdTime->type != cJSON_String) {
    mError("acct:%s, failed to parse since createdTime not found", pAcct->acct);
    goto DECODE_ACCT_OVER;
  }
  pAcct->createdTime = atol(createdTime->valuestring);

  cJSON *updateTime = cJSON_GetObjectItem(root, "updateTime");
  if (!updateTime || updateTime->type != cJSON_String) {
    mError("acct:%s, failed to parse since updateTime not found", pAcct->acct);
    goto DECODE_ACCT_OVER;
  }
  pAcct->updateTime = atol(updateTime->valuestring);

  code = 0;
  mTrace("acct:%s, parse success", pAcct->acct);

DECODE_ACCT_OVER:
  if (code != 0) {
    free(pAcct);
    pAcct = NULL;
  }
  return pAcct;
}

int32_t mnodeInitAcct() {
  sdbSetFp(MN_SDB_ACCT, MN_KEY_BINARY, mnodeCreateDefaultAcct, (SdbEncodeFp)mnodeEncodeAcct,
           (SdbDecodeFp)(mnodeDecodeAcct), sizeof(SAcctObj));

  return 0;
}

void mnodeCleanupAcct() {}
