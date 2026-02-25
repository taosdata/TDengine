/*
 * Copyright (c) 2024 TAOS Data, Inc.
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

#ifndef _TD_MND_INSTANCE_H_
#define _TD_MND_INSTANCE_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitInstance(SMnode *pMnode);
void    mndCleanupInstance(SMnode *pMnode);

int32_t       mndInstanceUpsert(SMnode *pMnode, const char *id, const char *type, const char *desc, int64_t regTime,
                                int32_t expire);
int32_t       mndInstanceRemove(SMnode *pMnode, const char *id);
SInstanceObj *mndInstanceAcquire(SMnode *pMnode, const char *id);
void          mndInstanceRelease(SMnode *pMnode, SInstanceObj *pInstance);

#ifdef __cplusplus
}
#endif

#endif /* _TD_MND_INSTANCE_H_ */

