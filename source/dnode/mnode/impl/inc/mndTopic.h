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

#ifndef _TD_MND_TOPIC_H_
#define _TD_MND_TOPIC_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitTopic(SMnode *pMnode);
void    mndCleanupTopic(SMnode *pMnode);

int32_t      mndAcquireTopic(SMnode *pMnode, const char *topicName, SMqTopicObj **pTopic);
void         mndReleaseTopic(SMnode *pMnode, SMqTopicObj *pTopic);
int32_t      mndDropTopicByDB(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);
bool         mndTopicExistsForDb(SMnode *pMnode, SDbObj *pDb);
void         mndTopicGetShowName(const char* fullTopic, char* topic);
bool         checkTopic(SArray *topics, char *topicName);

int32_t mndGetNumOfTopics(SMnode *pMnode, char *dbName, int32_t *pNumOfTopics);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_TOPIC_H_*/
