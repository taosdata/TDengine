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

#ifndef _TD_MND_STREAM_H_
#define _TD_MND_STREAM_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitStream(SMnode *pMnode);
void    mndCleanupStream(SMnode *pMnode);

SStreamObj *mndAcquireStream(SMnode *pMnode, char *streamName);
void        mndReleaseStream(SMnode *pMnode, SStreamObj *pStream);

SSdbRaw *mndStreamActionEncode(SStreamObj *pStream);
SSdbRow *mndStreamActionDecode(SSdbRaw *pRaw);

int32_t mndAddStreamToTrans(SMnode *pMnode, SStreamObj *pStream, const char *ast, STrans *pTrans);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_STREAM_H_*/
