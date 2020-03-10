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
#include "mgmtAcct.h"

int32_t (*mgmtCheckUserGrantFp)() = NULL;
int32_t (*mgmtCheckDbGrantFp)() = NULL;
void    (*mgmtAddTimeSeriesFp)(uint32_t timeSeriesNum) = NULL;
void    (*mgmtRestoreTimeSeriesFp)(uint32_t timeSeriesNum) = NULL;
int32_t (*mgmtCheckTimeSeriesFp)(uint32_t timeseries) = NULL;
bool    (*mgmtCheckExpiredFp)() = NULL;

int32_t mgmtCheckUserGrant() {
  if (mgmtCheckUserGrantFp) {
    return (*mgmtCheckUserGrantFp)();
  } else {
    return 0;
  }
}

int32_t mgmtCheckDbGrant() {
  if (mgmtCheckDbGrantFp) {
    return (*mgmtCheckDbGrantFp)();
  } else {
    return 0;
  }
}

void mgmtAddTimeSeries(SAcctObj *pAcct, uint32_t timeSeriesNum) {
  pAcct->acctInfo.numOfTimeSeries += timeSeriesNum;
  if (mgmtAddTimeSeriesFp) {
    (*mgmtAddTimeSeriesFp)(timeSeriesNum);
  }
}

void mgmtRestoreTimeSeries(SAcctObj *pAcct, uint32_t timeSeriesNum) {
  pAcct->acctInfo.numOfTimeSeries -= timeSeriesNum;
  if (mgmtRestoreTimeSeriesFp) {
    (*mgmtRestoreTimeSeriesFp)(timeSeriesNum);
  }
}

int32_t mgmtCheckTimeSeries(uint32_t timeseries) {
  if (mgmtCheckTimeSeriesFp) {
    return (*mgmtCheckTimeSeriesFp)(timeseries);
  } else {
    return 0;
  }
}

bool mgmtCheckExpired() {
  if (mgmtCheckExpiredFp) {
    return mgmtCheckExpiredFp();
  } else {
    return false;
  }
}
