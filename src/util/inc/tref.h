
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

// open a reference set, max is the mod used by hash, fp is the pointer to free resource function
// return rsetId which will be used by other APIs. On error, -1 is returned, and terrno is set appropriately
int taosOpenRef(int max, void (*fp)(void *)); 

// close the reference set, refId is the return value by taosOpenRef
// return 0 if success. On error, -1 is returned, and terrno is set appropriately
int taosCloseRef(int refId); 

// add ref, p is the pointer to resource or pointer ID
// return Reference ID(rid) allocated. On error, -1 is returned, and terrno is set appropriately
int64_t taosAddRef(int refId, void *p);   

// remove ref, rid is the reference ID returned by taosAddRef
// return 0 if success. On error, -1 is returned, and terrno is set appropriately
int taosRemoveRef(int rsetId, int64_t rid);

// acquire ref, rid is the reference ID returned by taosAddRef
// return the resource p. On error, NULL is returned, and terrno is set appropriately
void *taosAcquireRef(int rsetId, int64_t rid);

// release ref, rid is the reference ID returned by taosAddRef
// return 0 if success. On error, -1 is returned, and terrno is set appropriately
int taosReleaseRef(int rsetId, int64_t rid);

// return the first reference if rid is 0, otherwise return the next after current reference. 
// if return value is NULL, it means list is over(if terrno is set, it means error happens)
void *taosIterateRef(int rsetId, int64_t rid);

// return the number of references in system
int  taosListRef();  

/* sample code to iterate the refs 

void demoIterateRefs(int rsetId) {

  void *p = taosIterateRef(refId, 0);
  while (p) {
    // process P
    
    // get the rid from p

    p = taosIterateRef(rsetId, rid);
  }
}

*/

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TREF_H
