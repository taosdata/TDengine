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

#ifndef _TD_VNODE_READ_MSG_H_
#define _TD_VNODE_READ_MSG_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "vnodeInt.h"

typedef struct SReadMsg {
  int32_t code;
  int32_t contLen;
  int8_t  qtype;
  int8_t  msgType;
  SVnode *pVnode;
  SVnRsp rspRet;
  void *  rpcHandle;
  void *  rpcAhandle;
  void *  qhandle;
  char    pCont[];
} SReadMsg;

int32_t vnodeProcessQueryMsg(SVnode *pVnode, SReadMsg *pRead);
int32_t vnodeProcessFetchMsg(SVnode *pVnode, SReadMsg *pRead);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_READ_MSG_H_*/
