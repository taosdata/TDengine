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

#ifndef _TD_MND_AUDIT_H_
#define _TD_MND_AUDIT_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

// ---------------------------------------------------------------------------
// mndInitAudit / mndCleanupAudit
//
// Called during mnode startup / shutdown.  mndInitAudit registers the
// direct-write audit flush callback so that audit records are written
// directly into the local cluster's audit database instead of being sent
// to taoskeeper via HTTP.
// ---------------------------------------------------------------------------
int32_t mndInitAudit(SMnode *pMnode);
void    mndCleanupAudit(SMnode *pMnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_AUDIT_H_*/
