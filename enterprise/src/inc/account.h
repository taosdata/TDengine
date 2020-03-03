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

#ifndef TDENGINE_PLUGIN_ACCOUNT_H
#define TDENGINE_PLUGIN_ACCOUNT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include "mnode.h"

void acctInit();

extern int32_t (*mgmtInitAcctsFp)();
extern void (*mgmtCleanUpAcctsFp)();

extern int32_t (*mgmtCreateAcctFp)(char *name, char *pass, SAcctCfg *pCfg);
extern int32_t (*mgmtDropAcctFp)(char *name);
extern int32_t (*mgmtAlterAcctFp)(char *name, char *pass, SAcctCfg *pCfg);
extern int32_t (*mgmtGetAcctMetaFp)(STableMeta *pMeta, SShowObj *pShow, void *pConn);
extern int32_t (*mgmtRetrieveAcctsFp)(SShowObj *pShow, char *data, int32_t rows, void *pConn);
extern SAcctObj *(*mgmtGetAcctFp)(char *acctName);

extern int32_t (*mgmtCheckUserLimitFp)(SAcctObj *pAcct);
extern int32_t (*mgmtCheckDbLimitFp)(SAcctObj *pAcct);
extern int32_t (*mgmtCheckTimeSeriesLimitFp)(SAcctObj *pAcct, int32_t numOfTimeSeries);

#ifdef __cplusplus
}
#endif

#endif
