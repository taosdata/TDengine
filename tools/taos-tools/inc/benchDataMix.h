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

#ifndef __BENCHDATAMIX_H_
#define __BENCHDATAMIX_H_


uint32_t dataGenByField(Field* fd, char* pstr, uint32_t len, char* prefix, int64_t *k);

// data generate by calc ts 
uint32_t dataGenByCalcTs(Field* fd, char* pstr, uint32_t len, int64_t ts);

#endif
