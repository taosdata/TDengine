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

#ifndef _TD_AUDIT_INT_H_
#define _TD_AUDIT_INT_H_

#include "audit.h"
#include "taos.h"
#include "tarray.h"

typedef struct {
  SAuditCfg       cfg;
  SArray *records;
  TdThreadMutex   recordLock;
  TdThreadMutex   connLock;
  TAOS           *conn;
  int32_t         dnodeId;
  TdThreadRwlock  infoLock;
  char            auditDB[TSDB_DB_FNAME_LEN];
  char            auditToken[TSDB_TOKEN_LEN];
  char            connDb[TSDB_DB_FNAME_LEN];
  char            connToken[TSDB_TOKEN_LEN];
  char            connUser[TSDB_USER_LEN];
  bool            tableReady;
} SAudit;

#endif /*_TD_AUDIT_INT_H_*/
