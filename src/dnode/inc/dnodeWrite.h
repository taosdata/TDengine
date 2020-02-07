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

/*
 * Check if table already exists
 */
int32_t dnodeCheckTableExist(int vid, int sid, int64_t uid);

/*
 * Create table with specified configuration and open it
 */
int32_t dnodeCreateTable(int vid, int sid, SMeterObj *table);

/*
 * Modify table configuration information
 */
int32_t dnodeAlterTable(int vid, SMeterObj *table);

/*
 * Remove table from local repository
 */
int32_t dnodeDropTable(int vid, int sid, int64_t uid);

/*
 * Write data based on dnode
 */
int32_t dnodeWriteData(SShellSubmitMsg *msg);


#ifdef __cplusplus
}
#endif

#endif
