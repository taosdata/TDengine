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

#ifndef _TD_DNODE_H_
#define _TD_DNODE_H_

#include "tdef.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Initialize the dnode
 *
 * @return int32_t 0 for success and -1 for failure
 */
int32_t dmInit();

/**
 * @brief Cleanup the dnode
 */
void dmCleanup();

/**
 * @brief Run dnode.
 */
int32_t dmRun();

/**
 * @brief Stop dnode.
 */
void dmStop();

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_H_*/
