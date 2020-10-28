
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

#ifndef TDENGINE_TREF_H
#define TDENGINE_TREF_H

#ifdef __cplusplus
extern "C" {
#endif

int  taosOpenRef(int max, void (*fp)(void *)); // return refId which will be used by other APIs
void taosCloseRef(int refId); 
int  taosListRef();  // return the number of references in system
int  taosAddRef(int refId, void *p);
int  taosAcquireRef(int refId, void *p);
void taosReleaseRef(int refId, void *p);

#define taosRemoveRef taosReleaseRef


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TREF_H
