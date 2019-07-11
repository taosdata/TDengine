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

#include <arpa/inet.h>
#include <sys/time.h>

#include "mgmt.h"

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
