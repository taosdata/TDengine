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

#ifndef __BENCHINSERTMIX_H_
#define __BENCHINSERTMIX_H_


#define ABS_DIFF(a, b) (a > b ? a - b : b - a)

#define FULL_DISORDER(stb) (stb->disRatio == 100)

// insert data to db->stb with info
bool insertDataMix(threadInfo* info, SDataBase* db, SSuperTable* stb);

#endif
