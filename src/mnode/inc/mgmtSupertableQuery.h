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

#ifndef TBASE_MNODE_SUPER_TABLE_QUERY_H
#define TBASE_MNODE_SUPER_TABLE_QUERY_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "mnode.h"
#include "tast.h"

int32_t mgmtRetrieveMetersFromSuperTable(SSuperTableMetaMsg* pInfo, int32_t tableIndex, tQueryResultset* pRes);
int32_t mgmtDoJoin(SSuperTableMetaMsg* pSuperTableMetaMsg, tQueryResultset* pRes);
void    mgmtReorganizeMetersInMetricMeta(SSuperTableMetaMsg* pInfo, int32_t index, tQueryResultset* pRes);


#endif
