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

#ifndef TD_GRANT_H
#define TD_GRANT_H

#include <stdint.h>
#include "machine.h"
#include "mndDef.h"
#include "mnode.h"
#include "taoserror.h"
#include "tjson.h"
#include "tmsgcb.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndCollectClusterInfo(SMnode *pMnode, SAuthReqData *pAuthReqData);

void grantSetClusterId(SMnode *pMnode, char *pClusterId);

void    grantObjInit(SGrantUniqObj *pObj, bool official);
void    grantRetrieveGrantInfo(SMnode *pMnode);
int32_t grantSecondsToString(int64_t seconds, char *ts);

int32_t addDynamicGrantItem(SGrantUniqObj *pGrantObj, const char *itemName, int32_t expire, int64_t number);

int32_t addDynamicGrantItem2(SGrantUniqObj *pGrantObj, const char *itemName, int32_t expire, int32_t number,
                             int32_t speed);

int32_t addDynamicGrantItemEx(SGrantUniqObj *pGrantObj, const char *itemName, int32_t expire, int32_t number);

int32_t grantGetBasicExpireDays(bool basic);
#ifdef __cplusplus
}
#endif

#endif  // TD_GRANT_H
