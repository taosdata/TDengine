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

#ifndef TDENGINE_PLUGIN_MPEER_H
#define TDENGINE_PLUGIN_MPEER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "mnode.h"

void mpeerInit();

extern int32_t (*mpeerInitMnodesFp)(char *directory);
extern void    (*mpeerCleanUpMnodesFp)();

extern int32_t (*mpeerAddMnodeFp)(uint32_t privateIp, uint32_t publicIp);
extern int32_t (*mpeerRemoveMnodeFp)(uint32_t privateIp);

extern int32_t (*mpeerForwardRequestFp)(char type, void *cont, int32_t contLen);

extern void    (*sdbWorkAsMasterCallback)();

extern int32_t (*mpeerGetMnodesNumFp)();
extern void *  (*mpeerGetNextMnodeFp)(SShowObj *pShow, SSdbPeer **pMnode);

#ifdef __cplusplus
}
#endif

#endif
