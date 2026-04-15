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

#ifndef _TD_MND_SECURITY_POLICY_H_
#define _TD_MND_SECURITY_POLICY_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitSecurityPolicy(SMnode *pMnode);
void    mndCleanupSecurityPolicy(SMnode *pMnode);

// Accessors
int32_t mndGetClusterSoDMode(SMnode *pMnode);
int32_t mndGetClusterMacActive(SMnode *pMnode);

// SoD enforcement
int32_t mndProcessEnforceSod(SMnode *pMnode);
void    mndSodTransStop(SMnode *pMnode, void *param, int32_t paramLen);
void    mndSodGrantRoleStop(SMnode *pMnode, void *param, int32_t paramLen);

// Config handlers (called from mndProcessConfigClusterReq in mndCluster.c)
int32_t mndProcessConfigSoDReq(SMnode *pMnode, SRpcMsg *pReq, SMCfgClusterReq *pCfg);
int32_t mndProcessConfigMacReq(SMnode *pMnode, SRpcMsg *pReq, SMCfgClusterReq *pCfg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_SECURITY_POLICY_H_*/
