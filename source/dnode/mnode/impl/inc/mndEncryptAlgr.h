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

#ifndef _TD_MND_ENCRYPT_ALGR_H_
#define _TD_MND_ENCRYPT_ALGR_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif
#define ENCRYPT_ALGR_TYPE__SYMMETRIC_CIPHERS 1

#define ENCRYPT_ALGR_SOURCE_BUILTIN 1


int32_t mndInitEncryptAlgr(SMnode *pMnode);
void    mndCleanupEncryptAlgr(SMnode *pMnode);
SEncryptAlgrObj *mndAcquireEncryptAlgrById(SMnode *pMnode, int64_t id);
SEncryptAlgrObj *mndAcquireEncryptAlgrByAId(SMnode *pMnode, char* algorithm_id);
void mndReleaseEncryptAlgr(SMnode *pMnode, SEncryptAlgrObj *pObj);
void mndGetEncryptOsslAlgrNameById(SMnode *pMnode, int64_t id, char* out);
#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_ENCRYPT_ALGR_H_*/