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

typedef enum {
  MND_OPER_CONNECT = 1,
  MND_OPER_CREATE_USER,
  MND_OPER_DROP_USER,
  MND_OPER_ALTER_USER,
  MND_OPER_CREATE_BNODE,
  MND_OPER_DROP_BNODE,
  MND_OPER_CREATE_DNODE,
  MND_OPER_DROP_DNODE,
  MND_OPER_CONFIG_DNODE,
  MND_OPER_CREATE_MNODE,
  MND_OPER_DROP_MNODE,
  MND_OPER_CREATE_QNODE,
  MND_OPER_DROP_QNODE,
  MND_OPER_CREATE_SNODE,
  MND_OPER_DROP_SNODE,
  MND_OPER_REDISTRIBUTE_VGROUP,
  MND_OPER_MERGE_VGROUP,
  MND_OPER_SPLIT_VGROUP,
  MND_OPER_BALANCE_VGROUP,
  MND_OPER_CREATE_FUNC,
  MND_OPER_DROP_FUNC,
  MND_OPER_KILL_TRANS,
  MND_OPER_CREATE_DB,
  MND_OPER_ALTER_DB,
  MND_OPER_DROP_DB,
  MND_OPER_COMPACT_DB,
  MND_OPER_USE_DB,
  MND_OPER_WRITE_DB,
  MND_OPER_READ_DB,
} EOperType;

int32_t mndInitAuth(SMnode *pMnode);
void    mndCleanupAuth(SMnode *pMnode);

int32_t mndCheckOperAuth(SMnode *pMnode, const char *user, EOperType operType);
int32_t mndCheckDbAuth(SMnode *pMnode, const char *user, EOperType operType, SDbObj *pDb);
int32_t mndCheckShowAuth(SMnode *pMnode, const char *user, int32_t showType);
int32_t mndCheckAlterUserAuth(SUserObj *pOperUser, SUserObj *pUser, SAlterUserReq *pAlter);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_AUTH_H_*/
