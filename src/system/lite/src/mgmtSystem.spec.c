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

#include "dnodeSystem.h"
#include "mgmt.h"

extern void *mgmtTmr;
extern void *mgmtStatusTimer;

void mgmtProcessDnodeStatus(void *handle, void *tmrId);

int mgmtInitSystem() { return mgmtStartSystem(); }

int32_t mgmtStartCheckMgmtRunning() { return 0; }

void mgmtDoStatistic(void *handle, void *tmrId) {}

void mgmtStartMgmtTimer() { taosTmrReset(mgmtProcessDnodeStatus, 500, NULL, mgmtTmr, &mgmtStatusTimer); }

void mgmtStopSystem() {}

void mgmtCleanUpRedirect() {}

bool grantCheckExpired() { return false; }

int grantGetGrantsMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) { return TSDB_CODE_OPS_NOT_SUPPORT; }

int grantRetrieveGrants(SShowObj *pShow, char *data, int rows, SConnObj *pConn) { return 0; }

void grantRestoreTimeSeries(uint32_t timeseries) {}