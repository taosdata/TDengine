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

#ifndef _TD_AUDIT_H_
#define _TD_AUDIT_H_

#include "tarray.h"
#include "tdef.h"
#include "tlog.h"
#include "tmsg.h"
#include "tjson.h"
#include "tmsgcb.h"
#include "trpc.h"
#include "mnode.h"

#ifdef __cplusplus
extern "C" {
#endif

#define AUDIT_DETAIL_MAX 65472

typedef struct {
  const char *server;
  uint16_t    port;
  bool        comp;
} SAuditCfg;

int32_t auditInit(const SAuditCfg *pCfg);
void    auditSend(SJson *pJson);
void    auditRecord(SRpcMsg *pReq, int64_t clusterId, char *operation, char *target1, char *target2, 
                    char *detail, int32_t len);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MONITOR_H_*/
