
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

// open an instance, return refId which will be used by other APIs
int  taosOpenRef(int max, void (*fp)(void *)); 

// close the Ref instance
void taosCloseRef(int refId); 

// add ref, p is the pointer to resource or pointer ID
int  taosAddRef(int refId, void *p);
#define taosRemoveRef taosReleaseRef

// acquire ref, p is the pointer to resource or pointer ID
int  taosAcquireRef(int refId, void *p);

// release ref, p is the pointer to resource or pinter ID
void taosReleaseRef(int refId, void *p);

// return the first if p is null, otherwise return the next after p
void *taosIterateRef(int refId, void *p);

// return the number of references in system
int  taosListRef();  

/* sample code to iterate the refs 

void demoIterateRefs(int refId) {

  void *p = taosIterateRef(refId, NULL);
  while (p) {

    // process P

    p = taosIterateRef(refId, p);
  }
}

*/

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TREF_H
