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

#ifndef _TD_VNODE_WRITE_MSG_H_
#define _TD_VNODE_WRITE_MSG_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "vnodeInt.h"
 
int32_t vnodeProcessSubmitReq(SVnode *pVnode, SSubmitReq *pReq, SSubmitRsp *pRsp);
int32_t vnodeProcessCreateTableReq(SVnode *pVnode, SCreateTableReq *pReq, SCreateTableRsp *pRsp);
int32_t vnodeProcessDropTableReq(SVnode *pVnode, SDropTableReq *pReq, SDropTableRsp *pRsp);
int32_t vnodeProcessAlterTableReq(SVnode *pVnode, SAlterTableReq *pReq, SAlterTableRsp *pRsp);
int32_t vnodeProcessDropStableReq(SVnode *pVnode, SDropStableReq *pReq, SDropStableRsp *pRsp);
int32_t vnodeProcessUpdateTagValReq(SVnode *pVnode, SUpdateTagValReq *pReq, SUpdateTagValRsp *pRsp);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_WRITE_MSG_H_*/