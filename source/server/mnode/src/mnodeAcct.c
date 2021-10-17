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
  int32_t    code = TSDB_CODE_SUCCESS;

  SAcctObj acctObj = {0};
  tstrncpy(acctObj.acct, TSDB_DEFAULT_USER, TSDB_USER_LEN);
  acctObj.cfg = (SAcctCfg){.maxUsers = 128,
                           .maxDbs = 128,
                           .maxTimeSeries = INT32_MAX,
                           .maxConnections = 1024,
                           .maxStreams = 1000,
                           .maxPointsPerSecond = 10000000,
                           .maxStorage = INT64_MAX,
                           .maxQueryTime = INT64_MAX,
                           .maxInbound = 0,
                           .maxOutbound = 0,
                           .accessState = TSDB_VN_ALL_ACCCESS};
  acctObj.acctId = 1;
  acctObj.createdTime = taosGetTimestampMs();

  sdbInsertRow(MN_SDB_ACCT, &acctObj);
}

int32_t mnodeEncodeAcct(SAcctObj *pAcct, char *buf, int32_t maxLen) {
  int32_t len = 0;

  len += snprintf(buf + len, maxLen - len, "{\"type\":%d, ", MN_SDB_ACCT);
  len += snprintf(buf + len, maxLen - len, "\"acctId\":\"%d\", ", pAcct->acctId);
  len += snprintf(buf + len, maxLen - len, "\"maxUsers\":\"%d\", ", pAcct->cfg.maxUsers);
  len += snprintf(buf + len, maxLen - len, "\"maxDbs\":\"%d\", ", pAcct->cfg.maxDbs);
  len += snprintf(buf + len, maxLen - len, "\"maxTimeSeries\":\"%d\", ", pAcct->cfg.maxTimeSeries);
  len += snprintf(buf + len, maxLen - len, "\"maxConnections\":\"%d\", ", pAcct->cfg.maxConnections);
  len += snprintf(buf + len, maxLen - len, "\"maxStreams\":\"%d\", ", pAcct->cfg.maxStreams);
  len += snprintf(buf + len, maxLen - len, "\"maxPointsPerSecond\":\"%d\", ", pAcct->cfg.maxPointsPerSecond);
  len += snprintf(buf + len, maxLen - len, "\"maxUsers\":\"%" PRIu64 "\", ", pAcct->cfg.maxStorage);
  len += snprintf(buf + len, maxLen - len, "\"maxQueryTime\":\"%" PRIu64 "\", ", pAcct->cfg.maxQueryTime);
  len += snprintf(buf + len, maxLen - len, "\"maxInbound\"\":%" PRIu64 "\", ", pAcct->cfg.maxInbound);
  len += snprintf(buf + len, maxLen - len, "\"maxOutbound\":\"%" PRIu64 "\", ", pAcct->cfg.maxOutbound);
  len += snprintf(buf + len, maxLen - len, "\"accessState\":\"%d\", ", pAcct->cfg.accessState);
  len += snprintf(buf + len, maxLen - len, "\"createdTime\":\"%" PRIu64 "\", ", pAcct->createdTime);
  len += snprintf(buf + len, maxLen - len, "\"updateTime\":\"%" PRIu64 "\"}\n", pAcct->updateTime);

  return len;
}

SAcctObj *mnodeDecodeAcct(cJSON *root) {
  SAcctObj *pAcct = calloc(1, sizeof(SAcctObj));
  return pAcct;
}

int32_t mnodeInitAcct() {
  sdbSetFp(MN_SDB_ACCT, MN_KEY_BINARY, mnodeCreateDefaultAcct, (SdbEncodeFp)mnodeEncodeAcct,
           (SdbDecodeFp)(mnodeDecodeAcct), sizeof(SAcctObj));

  return 0;
}

void mnodeCleanupAcct() {}
