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

#include "index_cache.h"

static int32_t compareKey(const void *l, const void *r) {
  char *lp = (char *)l;
  char *rp = (char *)r;

  // skip total len
  int32_t ll, rl;  // len    
  memcpy(&ll, lp, sizeof(int32_t));
  memcpy(&rl, rp, sizeof(int32_t));
  lp += sizeof(int32_t); 
  rp += sizeof(int32_t);
  
  // compare field id
  int32_t lf, rf; // field id
  memcpy(&lf, lp, sizeof(lf));
  memcpy(&rf, rp, sizeof(rf));
  if (lf != rf) {
    return lf < rf ? -1: 1;
  }
  lp += sizeof(lf);
  rp += sizeof(rf);

  // compare field value 
  int32_t lfl, rfl;
  memcpy(&lfl, lp, sizeof(lfl)); 
  memcpy(&rfl, rp, sizeof(rfl)); 
  lp += sizeof(lfl);
  rp += sizeof(rfl);
  
  //refator later
  int32_t i, j;
  for (i = 0, j = 0; i < lfl && j < rfl; i++, j++) {
    if (lp[i] == rp[j]) { continue; }
    else { return lp[i] < rp[j] ? -1 : 1;}
  }
  if (i < lfl) { return  1;}
  else if (j < rfl) { return -1; }
  lp += lfl;
  rp += rfl; 

  // compare version
  int32_t lv, rv;
  memcpy(&lv, lp, sizeof(lv));
  memcpy(&rv, rp, sizeof(rv));
  if (lv != rv) {
    return lv > rv ? -1 : 1; 
  }  
  lp += sizeof(lv);
  rp += sizeof(rv);

  
  return 0;  
  
} 
IndexCache *indexCacheCreate() {
  IndexCache *cache = calloc(1, sizeof(IndexCache));
  return cache;
}

void indexCacheDestroy(IndexCache *cache) {
  free(cache);
}

int indexCachePut(IndexCache *cache, int32_t fieldId, const char *fieldValue,  int32_t fvlen, uint64_t uid, int8_t operType) {
  if (cache == NULL) { return -1;} 
  int32_t version = T_REF_INC(cache);     

  int32_t total = sizeof(int32_t) + sizeof(fieldId) + 4 + fvlen  + sizeof(version) + sizeof(uid) + sizeof(operType); 

  char *buf = calloc(1, total); 
  char *p   = buf;

  memcpy(buf, &total, sizeof(total)); 
  total += total;

  memcpy(buf, &fieldId, sizeof(fieldId));   
  buf += sizeof(fieldId);

  memcpy(buf, &fvlen, sizeof(fvlen));
  buf += sizeof(fvlen);
  memcpy(buf, fieldValue, fvlen); 
  buf += fvlen;

  memcpy(buf, &version, sizeof(version));
  buf += sizeof(version);

  memcpy(buf, &uid, sizeof(uid));  
  buf += sizeof(uid);

  memcpy(buf, &operType, sizeof(operType));
  buf += sizeof(operType); 
 

}
int indexCacheSearch(IndexCache *cache, SIndexMultiTermQuery *query, SArray *result) {
  
  return 0;  
}





