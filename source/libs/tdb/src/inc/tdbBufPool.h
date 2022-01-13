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

#ifndef _TD_TDB_BUF_POOL_H_
#define _TD_TDB_BUF_POOL_H_

#include "tdbPage.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct STkvBufPool STkvBufPool;

int       tbpOpen(STkvBufPool **ppTkvBufPool);
int       tbpClose(STkvBufPool *pTkvBufPool);
STdbPage *tbpNewPage(STkvBufPool *pTkvBufPool);
int       tbpDelPage(STkvBufPool *pTkvBufPool);
STdbPage *tbpFetchPage(STkvBufPool *pTkvBufPool, pgid_t pgid);
int       tbpUnpinPage(STkvBufPool *pTkvBufPool, pgid_t pgid);
void      tbpFlushPages(STkvBufPool *pTkvBufPool);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TDB_BUF_POOL_H_*/