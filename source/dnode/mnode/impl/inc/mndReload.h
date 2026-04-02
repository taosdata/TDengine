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

#ifndef _TD_MND_RELOAD_H_
#define _TD_MND_RELOAD_H_

#include "mndInt.h"
#include "tmsg.h"

int32_t mndInitReload(SMnode* pMnode);
void    mndCleanupReload(SMnode* pMnode);

int32_t mndReloadLastCache(SMnode* pMnode, SRpcMsg* pReq, int8_t cacheType, int8_t scopeType,
                            const char* dbName, const char* tableName, const char* colName,
                            int64_t* pReloadUid);
int32_t mndShowReloads(SMnode* pMnode, SRpcMsg* pReq, SRetrieveTableRsp** ppRsp);
int32_t mndShowReload(SMnode* pMnode, SRpcMsg* pReq, int64_t reloadUid, SRetrieveTableRsp** ppRsp);
int32_t mndDropReload(SMnode* pMnode, SRpcMsg* pReq, int64_t reloadUid);

#endif
