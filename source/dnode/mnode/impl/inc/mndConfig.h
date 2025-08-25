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

#ifndef _TD_MND_CONFIG_H_
#define _TD_MND_CONFIG_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif
int32_t mndInitConfig(SMnode *pMnode);

SSdbRaw       *mnCfgActionEncode(SConfigObj *pCfg);
SSdbRow       *mndCfgActionDecode(SSdbRaw *pRaw);
static int32_t mndCfgActionInsert(SSdb *pSdb, SConfigObj *obj);
static int32_t mndCfgActionDelete(SSdb *pSdb, SConfigObj *obj);
static int32_t mndCfgActionUpdate(SSdb *pSdb, SConfigObj *oldItem, SConfigObj *newObj);
static int32_t mndCfgActionDeploy(SMnode *pMnode);
static int32_t mndCfgActionAfterRestored(SMnode *pMnode);

static int32_t mndProcessConfigReq(SRpcMsg *pReq);
int32_t        mndSetDeleteConfigCommitLogs(STrans *pTrans, SConfigObj *item);
#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_ARBGROUP_H_*/
