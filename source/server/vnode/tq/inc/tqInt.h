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

#ifndef _TD_TQ_INT_H_
#define _TD_TQ_INT_H_

#include "tq.h"


#ifdef __cplusplus
extern "C" {
#endif

//create persistent storage for meta info such as consuming offset
//return value > 0: cgId
//return value <= 0: error code
int tqCreateTCGroup(STQ*, const char* topic, int cgId, tqGroupHandle** handle);
//create ring buffer in memory and load consuming offset
//int tqOpenTCGroup(STQ*, const char* topic, int cgId);
//destroy ring buffer and persist consuming offset
//int tqCloseTCGroup(STQ*, const char* topic, int cgId);
//delete persistent storage for meta info
int tqDropTCGroup(STQ*, const char* topic, int cgId);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TQ_INT_H_*/
