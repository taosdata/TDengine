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

#ifndef TDENGINE_VNODE_STATUS_H
#define TDENGINE_VNODE_STATUS_H

#ifdef __cplusplus
extern "C" {
#endif
#include "vnodeInt.h"

typedef enum _VN_STATUS {
  TAOS_VN_STATUS_INIT = 0,
  TAOS_VN_STATUS_READY = 1,
  TAOS_VN_STATUS_CLOSING = 2,
  TAOS_VN_STATUS_UPDATING = 3,
  TAOS_VN_STATUS_RESET = 4,
} EVnodeStatus;

bool vnodeSetInitStatus(SVnodeObj* pVnode);
bool vnodeSetReadyStatus(SVnodeObj* pVnode);
bool vnodeSetClosingStatus(SVnodeObj* pVnode);
bool vnodeSetUpdatingStatus(SVnodeObj* pVnode);
bool vnodeSetResetStatus(SVnodeObj* pVnode);

bool vnodeInInitStatus(SVnodeObj* pVnode);
bool vnodeInReadyStatus(SVnodeObj* pVnode);
bool vnodeInReadyOrUpdatingStatus(SVnodeObj* pVnode);
bool vnodeInClosingStatus(SVnodeObj* pVnode);
bool vnodeInResetStatus(SVnodeObj* pVnode);

#ifdef __cplusplus
}
#endif

#endif