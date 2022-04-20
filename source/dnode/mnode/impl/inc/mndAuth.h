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

#ifndef _TD_MND_AUTH_H_
#define _TD_MND_AUTH_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitAuth(SMnode *pMnode);
void    mndCleanupAuth(SMnode *pMnode);

int32_t mndCheckCreateUserAuth(SUserObj *pOperUser);
int32_t mndCheckAlterUserAuth(SUserObj *pOperUser, SUserObj *pUser, SDbObj *pDb, SAlterUserReq *pAlter);
int32_t mndCheckDropUserAuth(SUserObj *pOperUser);

int32_t mndCheckNodeAuth(SUserObj *pOperUser);
int32_t mndCheckFuncAuth(SUserObj *pOperUser);

int32_t mndCheckCreateDbAuth(SUserObj *pOperUser);
int32_t mndCheckAlterDropCompactDbAuth(SUserObj *pOperUser, SDbObj *pDb);
int32_t mndCheckUseDbAuth(SUserObj *pOperUser, SDbObj *pDb);

int32_t mndCheckWriteAuth(SUserObj *pOperUser, SDbObj *pDb);
int32_t mndCheckReadAuth(SUserObj *pOperUser, SDbObj *pDb);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_AUTH_H_*/
