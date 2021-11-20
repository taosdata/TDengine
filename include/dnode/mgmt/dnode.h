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

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------ TYPES EXPOSED ------------------------ */
typedef struct SDnode SDnode;

/* ------------------------ SDnode ------------------------ */
/**
 * @brief Initialize and start the dnode.
 *
 * @param cfgPath Config file path.
 * @return SDnode* The dnode object.
 */
SDnode *dnodeInit(const char *cfgPath);

/**
 * @brief Stop and cleanup dnode.
 *
 * @param pDnode The dnode object to close.
 */
void dnodeCleanup(SDnode *pDnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_H_*/
