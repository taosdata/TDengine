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

#ifndef TDENGINE_DNODE_WRITE_H
#define TDENGINE_DNODE_WRITE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>
#include "taosdef.h"
#include "taosmsg.h"

/*
 * Write data based on dnode, the detail result can be fetched from rsponse
 *   pSubmit:  Data to be written
 *   pConn:    Communication handle
 *   callback: Pass the write result through a callback function, possibly in a different thread space
 *             rsp: will not be freed by callback function
 */
void dnodeWriteData(SShellSubmitMsg *pSubmit, void *pConn, void (*callback)(SShellSubmitRspMsg *rsp, void *pConn));

/*
 * Create table with specified configuration and open it
 * if table already exist, update its schema and tag
 */
int32_t dnodeCreateTable(SDCreateTableMsg *pTable);

/*
 * Remove table from local repository
 */
int32_t dnodeDropTable(SDRemoveTableMsg *pTable);

/*
 * Create stream
 * if stream already exist, update it
 */
int32_t dnodeCreateStream(SAlterStreamMsg *stream);

/*
 * Remove all child tables of supertable from local repository
 */
int32_t dnodeDropSuperTable(uint64_t stableUid);

#ifdef __cplusplus
}
#endif

#endif
