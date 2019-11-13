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
#include <arpa/inet.h>
#include <sys/time.h>
#include <math.h>
#include <float.h>

#include "mgmt.h"
#include "mgmtUtil.h"
#include "textbuffer.h"
#include "tschemautil.h"
#include "tsqlfunction.h"
#include "vnodeTagMgmt.h"

extern int cksumsize;

uint64_t mgmtGetCheckSum(FILE* fp, int offset) {
  uint64_t checksum = 0;
  uint64_t data;
  int      bytes;

  while (1) {
    data = 0;
    bytes = fread(&data, sizeof(data), 1, fp);

    if (bytes != sizeof(data)) break;

    checksum += data;
  }

  return checksum;
}

bool mgmtMeterCreateFromMetric(STabObj* pMeterObj) { return pMeterObj->meterType == TSDB_METER_MTABLE; }

bool mgmtIsMetric(STabObj* pMeterObj) { return pMeterObj->meterType == TSDB_METER_METRIC; }

bool mgmtIsNormalMeter(STabObj* pMeterObj) { return !mgmtIsMetric(pMeterObj); }

/**
 * TODO: the tag offset value should be kept in memory to avoid dynamically calculating the value
 *
 * @param pMeter
 * @param col
 * @param pTagColSchema
 * @return
 */
char* mgmtMeterGetTag(STabObj* pMeter, int32_t col, SSchema* pTagColSchema) {
  if (!mgmtMeterCreateFromMetric(pMeter)) {
    return NULL;
  }

  STabObj* pMetric = mgmtGetMeter(pMeter->pTagData);
  int32_t  offset = mgmtGetTagsLength(pMetric, col) + TSDB_METER_ID_LEN;
  assert(offset > 0);

  if (pTagColSchema != NULL) {
    *pTagColSchema = ((SSchema*)pMetric->schema)[pMetric->numOfColumns + col];
  }

  return (pMeter->pTagData + offset);
}

int32_t mgmtGetTagsLength(STabObj* pMetric, int32_t col) {  // length before column col
  assert(mgmtIsMetric(pMetric) && col >= 0);

  int32_t len = 0;
  int32_t tagColumnIndexOffset = pMetric->numOfColumns;

  for (int32_t i = 0; i < pMetric->numOfTags && i < col; ++i) {
    len += ((SSchema*)pMetric->schema)[tagColumnIndexOffset + i].bytes;
  }

  return len;
}

bool mgmtCheckIsMonitorDB(char *db, char *monitordb) {
  char dbName[TSDB_DB_NAME_LEN + 1] = {0};
  extractDBName(db, dbName);

  size_t len = strlen(dbName);
  return (strncasecmp(dbName, monitordb, len) == 0 && len == strlen(monitordb));
}
