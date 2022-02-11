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

#ifndef _TD_MND_DATABASE_H_
#define _TD_MND_DATABASE_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitDb(SMnode *pMnode);
void    mndCleanupDb(SMnode *pMnode);
SDbObj *mndAcquireDb(SMnode *pMnode, char *db);
void    mndReleaseDb(SMnode *pMnode, SDbObj *pDb);
int32_t mndValidateDBInfo(SMnode *pMnode, SDbVgVersion *dbs, int32_t num, void **rsp, int32_t *rspLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_DATABASE_H_*/
