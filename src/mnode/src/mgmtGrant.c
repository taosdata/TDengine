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
#ifndef _GRANT
#include "os.h"
#include "mgmtGrant.h"

bool mgmtCheckExpired() { return false; }
void mgmtParseParameterKFp() {}
void mgmtSendMsgToMaster() {}
void mgmtSetCurStorage(uint64_t storage) {}
void mgmtAddTimeSeries(SAcctObj *pAcct, uint32_t timeSeriesNum) {}
void mgmtRestoreTimeSeries(SAcctObj *pAcct, uint32_t timeseries) {}
int32_t mgmtCheckTimeSeries(uint32_t timeseries) { return TSDB_CODE_SUCCESS; }
int32_t mgmtCheckUserGrant() { return TSDB_CODE_SUCCESS; }
int32_t mgmtCheckDbGrant() { return TSDB_CODE_SUCCESS; }
int32_t mgmtCheckDnodeGrant() { return TSDB_CODE_SUCCESS; }
int32_t mgmtCheckAccts() { return TSDB_CODE_SUCCESS; }

#endif