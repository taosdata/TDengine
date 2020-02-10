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
#include "mnode.h"
#include "mgmtAcct.h"
#include "mgmtGrant.h"

int32_t mgmtCheckUserGrantImp() { return 0; }
int32_t (*mgmtCheckUserGrant)() = mgmtCheckUserGrantImp;

int32_t mgmtCheckDbGrantImp() { return 0; }
int32_t (*mgmtCheckDbGrant)() = mgmtCheckDbGrantImp;

void mgmtAddTimeSeriesImp(uint32_t timeSeriesNum) {}
void (*mgmtAddTimeSeries)(uint32_t timeSeriesNum) = mgmtAddTimeSeriesImp;

void mgmtRestoreTimeSeriesImp(uint32_t timeSeriesNum) {}
void (*mgmtRestoreTimeSeries)(uint32_t timeSeriesNum) = mgmtRestoreTimeSeriesImp;

int32_t mgmtCheckTimeSeriesImp(uint32_t timeseries) { return 0; }
int32_t (*mgmtCheckTimeSeries)(uint32_t timeseries) = mgmtCheckTimeSeriesImp;

bool mgmtCheckExpiredImp() { return false; }
bool (*mgmtCheckExpired)() = mgmtCheckExpiredImp;