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
#include "index_fst_node.h"

FstBuilderNode *fstBuilderNodeDefault() {
  FstBuilderNode *bn = malloc(sizeof(FstBuilderNode));
  bn->isFinal     = false; 
  bn->finalOutput = 0; 
  bn->trans       = NULL; 
  return bn;
}

// not destroy src, User's bussiness  
void fstBuilderNodeCloneFrom(FstBuilderNode *dst, FstBuilderNode *src) {
  if (dst == NULL || src == NULL) { return; } 

  dst->isFinal     = src->isFinal;
  dst->finalOutput = src->finalOutput ;
  dst->trans       = src->trans;    

  src->trans = NULL;
}

