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

#ifndef _TD_MND_OFFSET_H_
#define _TD_MND_OFFSET_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitOffset(SMnode *pMnode);
void    mndCleanupOffset(SMnode *pMnode);

SMqOffsetObj *mndAcquireOffset(SMnode *pMnode, const char *key);
void          mndReleaseOffset(SMnode *pMnode, SMqOffsetObj *pOffset);

SSdbRaw *mndOffsetActionEncode(SMqOffsetObj *pOffset);
SSdbRow *mndOffsetActionDecode(SSdbRaw *pRaw);

int32_t mndCreateOffsets(STrans *pTrans, const char *cgroup, const char *topicName, const SArray *vgs);

static FORCE_INLINE int32_t mndMakePartitionKey(char *key, const char *cgroup, const char *topicName, int32_t vgId) {
  return snprintf(key, TSDB_PARTITION_KEY_LEN, "%d:%s:%s", vgId, cgroup, topicName);
}

int32_t mndDropOffsetByDB(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);
int32_t mndDropOffsetByTopic(SMnode *pMnode, STrans *pTrans, const char *topic);
int32_t mndDropOffsetBySubKey(SMnode *pMnode, STrans *pTrans, const char *subKey);

bool mndOffsetFromTopic(SMqOffsetObj *pOffset, const char *topic);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_OFFSET_H_*/
