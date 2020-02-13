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

#ifndef TBASE_MNODE_TABLE_H
#define TBASE_MNODE_TABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "mnode.h"


typedef struct {
  ETableType type;
  void*      obj;
} STableObj;

int      mgmtInitMeters();
STableObj mgmtGetTable(char *tableId);

STabObj *mgmtGetTableInfo(char *src, char *tags[]);
int      mgmtRetrieveMetricMeta(SConnObj *pConn, char **pStart, SSuperTableMetaMsg *pInfo);
int      mgmtCreateTable(SDbObj *pDb, SCreateTableMsg *pCreate);
int      mgmtDropTable(SDbObj *pDb, char *meterId, int ignore);
int      mgmtAlterTable(SDbObj *pDb, SAlterTableMsg *pAlter);
int      mgmtGetTableMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn);
int      mgmtRetrieveTables(SShowObj *pShow, char *data, int rows, SConnObj *pConn);
void     mgmtCleanUpMeters();
SSchema *mgmtGetTableSchema(STabObj *pTable);  // get schema for a meter

int mgmtAddMeterIntoMetric(STabObj *pMetric, STabObj *pTable);
int mgmtRemoveMeterFromMetric(STabObj *pMetric, STabObj *pTable);
int mgmtGetSuperTableMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn);
int mgmtRetrieveSuperTables(SShowObj *pShow, char *data, int rows, SConnObj *pConn);



#ifdef __cplusplus
}
#endif

#endif
