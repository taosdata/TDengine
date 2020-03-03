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
#include "grant.h"

char* grantGetMachineSerials();

void grantAddTimeSeries(uint32_t timeseries);
void grantRestoreTimeSeries(uint32_t timeseries);
void grantUpdate(void *pMsg);

bool grantCheckExpired();
int32_t grantCheckUsers();
int32_t grantCheckDatabases();
int32_t grantCheckTimeSeries(uint32_t timeseries);
int32_t grantGetGrantsMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn);
int32_t grantRetrieveGrants(SShowObj *pShow, char *data, int32_t rows, void *pConn);

static void grantParseParameterK() {
  char *key = grantGetMachineSerials();
  if (key != NULL) {
    fprintf(stdout, "machine code: %s \n", key);
  } else {
    fprintf(stderr, "should generate machine code under root authority!\n");
  }
  exit(EXIT_SUCCESS);
}

void grantInit() {
  mgmtCheckUserGrantFp    = grantCheckUsers;
  mgmtCheckDbGrantFp      = grantCheckDatabases;
  mgmtAddTimeSeriesFp     = grantAddTimeSeries;
  mgmtRestoreTimeSeriesFp = grantRestoreTimeSeries;
  mgmtCheckTimeSeriesFp   = grantCheckTimeSeries;
  mgmtCheckExpiredFp      = grantCheckExpired;
  mgmtGetGrantsMetaFp     = grantGetGrantsMeta;
  mgmtRetrieveGrantsFp    = grantRetrieveGrants;
  dnodeParseParameterKFp  = grantParseParameterK;
  mgmtUpdateGrantInfoFp   = grantUpdate;
}

