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

#ifndef TD_TDISK_H
#define TD_TDISK_H

#include "taosdef.h"
#include "hash.h"
#include "hash.h"
#include "taoserror.h"
#include "tglobal.h"

#ifdef __cplusplus
extern "C" {
#endif

int  tdInitTiers(SDiskCfg *pDiskCfg, int ndisk);
void tdDestroyTiers();
int  tdUpdateDiskInfos();
void tdGetPrimaryPath(char *dst);

#ifdef __cplusplus
}
#endif

#endif
