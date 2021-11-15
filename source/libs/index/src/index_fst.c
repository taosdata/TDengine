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

#include "index_fst.h"

// fst node function 
FstNode *fstNodeCreate(int64_t version, ComiledAddr addr, uint8_t *data) {
  FstNode *n = (FstNode *)malloc(sizeof(FstNode)); 
  if (n == NULL) { return NULL; }

  if (addr == EMPTY_ADDRESS) {
     n->date = NULL;
     n->version = version;
     n->state   = EmptyFinal;  
     n->start   = EMPTY_ADDRESS;
     n->end     = EMPTY_ADDRESS;  
     n->isFinal = true; 
     n->nTrans  = 0;
     n->sizes   = 0;  
     n->finalOutpu = 0;  
     return n;
   } 
   uint8_t v = (data[addr] & 0b1100000) >> 6;
   if (v == 0b11) {
      
   } else if (v == 0b10) {
    
   } else {

   } 
   
  
}



