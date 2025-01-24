/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __WRAPDB_H_
#define __WRAPDB_H_

int32_t executeSql(TAOS* taos, char* sql);

int32_t queryCnt(TAOS* taos, char* sql, int64_t * pVal);

int32_t queryTS(TAOS* taos, char* sql, int64_t* pVal);

#endif